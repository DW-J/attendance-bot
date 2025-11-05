import os, re, pytz, datetime as dt
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import gspread
from google.oauth2.service_account import Credentials
import json
import unicodedata as ud

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(os.environ["SHEET_ID"])
logs = sh.worksheet("logs")
app = App(token=os.environ["SLACK_BOT_TOKEN"])
KST = pytz.timezone("Asia/Seoul")

ADMIN_ID_SET = {s.strip() for s in (os.getenv("ADMIN_IDS") or "").split(",") if s.strip()}
ADMIN_EMAIL_SET = {e.strip().lower() for e in (os.getenv("ADMIN_EMAILS") or "").split(",") if e.strip()}

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# 캐시
user_cache = {}
user_email_cache = {}

def parse_date(s):
    try:
        y,m,d = map(int, s.split("-")); return dt.date(y,m,d)
    except Exception: return None
    
def logs_usage_since(user_key: str, since_date=None):
    ws = sh.worksheet("logs")
    vals = ws.get_all_values()
    if not vals: return 0.0
    head = [h.strip().lower() for h in vals[0]]
    i_user = head.index("user_id") if "user_id" in head else head.index("user_key")
    i_type = head.index("type"); i_date = head.index("date")
    used = 0.0
    for r in vals[1:]:
        uk = (r[i_user] if i_user < len(r) else "").strip()
        if uk.lower() != (user_key or "").strip().lower(): continue
        t = (r[i_type] if i_type < len(r) else "").strip().lower()
        d = (r[i_date] if i_date < len(r) else "").strip()
        dd = parse_date(d)
        if since_date and (not dd or dd < since_date):  # 기준일 전 기록은 제외
            continue
        if t == "annual": used += 1.0
        elif t == "halfday": used += 0.5
    return used

def get_balance_row_map():
    ws = sh.worksheet("balances")
    vals = ws.get_all_values()
    if not vals: return ws, {}, []
    head = [h.strip().lower() for h in vals[0]]
    idx = {h:i for i,h in enumerate(head)}
    rows = vals
    pos = {}  # user_key(lower) -> rownum(1-based)
    for rn, r in enumerate(rows[1:], start=2):
        uk = (r[idx.get("user_key",0)] if idx.get("user_key",0) < len(r) else "").strip().lower()
        if uk: pos[uk] = rn
    return ws, idx, rows, pos

def effective_left_for(user_key: str):
    ws, idx, rows, pos = get_balance_row_map()
    rn = pos.get((user_key or "").strip().lower())
    if not rn: return None  # balances에 행이 없음
    row = rows[rn-1]
    def get(name, default=""):
        i = idx.get(name); return (row[i] if i is not None and i < len(row) else default)
    # 우선 override 기반
    o_left = get("override_left","")
    o_from = get("override_from","")
    if o_left:
        base = float(o_left)
        since = parse_date(o_from) if DATE_RE.match(o_from or "") else None
        used = logs_usage_since(user_key, since_date=since)
        return max(0.0, base - used)
    # 없으면 기존 total-집계 방식(연도 기준)
    total = float(get("annual_total","0") or 0)
    # 올해 사용 합산
    year = dt.datetime.now(KST).year
    used = 0.0
    ws_logs = sh.worksheet("logs")
    vals = ws_logs.get_all_values()
    head = [h.strip().lower() for h in vals[0]]
    i_user = head.index("user_id") if "user_id" in head else head.index("user_key")
    i_type = head.index("type"); i_date = head.index("date")
    for r in vals[1:]:
        uk = (r[i_user] if i_user < len(r) else "").strip()
        if uk.lower() != (user_key or "").strip().lower(): continue
        t = (r[i_type] if i_type < len(r) else "").strip().lower()
        dd = parse_date((r[i_date] if i_date < len(r) else "").strip())
        if not dd or dd.year != year: continue
        used += 1.0 if t=="annual" else (0.5 if t=="halfday" else 0.0)
    return max(0.0, total - used)

def build_override_view(initial_left:str="", initial_date:str=""):
    return {
      "type":"modal","callback_id":"override_submit",
      "title":{"type":"plain_text","text":"잔여 기준선 설정"},
      "submit":{"type":"plain_text","text":"저장"},
      "close":{"type":"plain_text","text":"취소"},
      "blocks":[
        {"type":"input","block_id":"target_b","label":{"type":"plain_text","text":"대상자"},
         "element":{"type":"users_select","action_id":"target"}},
        {"type":"input","block_id":"left_b","label":{"type":"plain_text","text":"기준선 잔여일수(예: 12.5)"},
         "element":{"type":"plain_text_input","action_id":"left"}},
        {"type":"input","block_id":"from_b","label":{"type":"plain_text","text":"기준 적용 시작일"},
         "element":{"type":"datepicker","action_id":"from_date"}},
        {"type":"input","block_id":"note_b","optional":True,"label":{"type":"plain_text","text":"비고"},
         "element":{"type":"plain_text_input","action_id":"note"}}
      ]}

def recompute_balances(target_year=None):
    year = target_year or dt.datetime.now(KST).year
    ws_logs = sh.worksheet("logs")
    vals = ws_logs.get_all_values()  # A:H
    if not vals: return
    head = [h.strip().lower() for h in vals[0]]
    get = lambda col: head.index(col)

    i_user_key = get("user_id") if "user_id" in head else get("user_key")
    i_user_name = get("user_name")
    i_type = get("type")
    i_date = get("date")

    used = {}   # user_key -> {'name':..., 'annual':x, 'half':y}
    for r in vals[1:]:
        if len(r) <= max(i_user_key,i_user_name,i_type,i_date): continue
        ukey = (r[i_user_key] or "").strip()
        if not ukey: continue
        t = (r[i_type] or "").strip().lower()
        d = parse_date((r[i_date] or "").strip())
        # 연차/반차만 집계. 날짜 없으면 올해로 간주하지 않음
        if t not in ("annual","halfday"): 
            continue
        if not d or d.year != year:
            continue
        if ukey not in used:
            used[ukey] = {"name": (r[i_user_name] or "").strip(), "annual":0.0, "half":0.0}
        if t == "annual": used[ukey]["annual"] += 1.0
        elif t == "halfday": used[ukey]["half"] += 0.5

    ws_bal = sh.worksheet("balances")
    bal_vals = ws_bal.get_all_values()
    if not bal_vals:
        ws_bal.append_row(["user_key","user_name","annual_total","annual_used","annual_left","half_used","notes"])
        bal_vals = ws_bal.get_all_values()
    bhead = [h.strip().lower() for h in bal_vals[0]]
    # 보장 헤더
    need = ["user_key","user_name","annual_total","annual_used","annual_left","half_used","notes"]
    for n in need:
        if n not in bhead: raise RuntimeError("balances 헤더 불일치: " + ",".join(need))

    # 기존 인덱스
    idx_map = { (row[0] or "").strip().lower(): i for i,row in enumerate(bal_vals[1:], start=2) }  # user_key -> rownum

    updates = []
    for ukey, agg in used.items():
        rownum = idx_map.get(ukey.lower())
        # annual_total이 없으면 0으로 가정
        if rownum:
            total = bal_vals[rownum-1][2] if len(bal_vals[rownum-1])>2 else "0"
        else:
            total = "0"
        try:
            total_f = float(total)
        except:
            total_f = 0.0
        annual_used = agg["annual"]
        half_used = agg["half"]
        annual_left = max(0.0, total_f - (annual_used + half_used))
        row = [ukey, agg["name"], str(total_f), annual_used, annual_left, half_used, ""]
        if rownum:
            updates.append((rownum, row))
        else:
            ws_bal.append_row(row)
            idx_map[ukey.lower()] = ws_bal.row_count

    # 벌크 업데이트
    for rownum, row in updates:
        ws_bal.update(f"A{rownum}:G{rownum}", [row], value_input_option="USER_ENTERED")

def date_to_iso_week_kst(date_str: str) -> str:
    y, m, d = map(int, date_str.split("-"))
    day = dt.datetime(y, m, d, tzinfo=KST).date()
    Y, W, _ = day.isocalendar()
    return f"{Y}-W{W:02d}"

def available_weeks_for_user(ukey: str):
    ws = get_ws("schedule_weekly")
    vals = ws.get_all_values()
    if not vals:
        return []
    header = [h.strip() for h in vals[0]]
    try:
        wi = header.index("week"); ui = header.index("user_key")
    except ValueError:
        return []
    out = set()
    for r in vals[1:]:
        wk = r[wi].strip() if wi < len(r) else ""
        uk = r[ui].strip() if ui < len(r) else ""
        if uk.lower() == (ukey or "").strip().lower():
            out.add(wk)
    return sorted(out)

def get_ws(name: str):
    try:
        return sh.worksheet(name)
    except Exception:
        raise RuntimeError(f"시트 '{name}'를 찾을 수 없습니다.")
    
def sheet_rows_as_dicts(ws, header_row=1):
    vals = ws.get_all_values()
    if len(vals) < header_row:
        return []
    headers = [h.strip() for h in vals[header_row-1]]
    rows = []
    for r in vals[header_row:]:
        row = {}
        for i, h in enumerate(headers):
            if not h:
                continue
            row[h] = r[i] if i < len(r) else ""
        rows.append(row)
    return rows

def find_balance_row_for(user_key: str):
    ws = get_ws("balances")
    rows = sheet_rows_as_dicts(ws)
    # user_key 완전일치 1순위
    for r in rows:
        if (r.get("user_key") or "").strip().lower() == (user_key or "").strip().lower():
            return r
    # 이메일/ID 혼용 대비: 끝 공백 제거 후 비교
    for r in rows:
        if (r.get("user_key") or "").strip() == (user_key or "").strip():
            return r
    return None

ISO_WEEK_RE = re.compile(r"^\d{4}-W\d{2}$")

def current_iso_week_kst() -> str:
    today = dt.datetime.now(KST).date()
    y, w, _ = today.isocalendar()
    return f"{y}-W{w:02d}"

def find_schedule_for(week: str, user_key: str):
    ws = get_ws("schedule_weekly")
    rows = sheet_rows_as_dicts(ws)
    # week, user_key 모두 일치하는 첫 행
    for r in rows:
        if (r.get("week")==week) and ((r.get("user_key") or "").strip().lower()==(user_key or "").strip().lower()):
            return r
    # user_key 느슨 비교
    for r in rows:
        if (r.get("week")==week) and ((r.get("user_key") or "").strip()==(user_key or "").strip()):
            return r
    return None

def safe_user_key(client, slack_user_id: str) -> str:
    """이메일 우선. 실패 시 Slack ID."""
    try:
        info = client.users_info(user=slack_user_id)
        email = info["user"]["profile"].get("email")
        return email or slack_user_id
    except Exception:
        return slack_user_id

def safe_user_name(client, slack_user_id: str) -> str:
    try:
        info = client.users_info(user=slack_user_id)
        p = info["user"]["profile"]
        return p.get("display_name") or p.get("real_name") or slack_user_id
    except Exception:
        return slack_user_id

def reply_error(respond, msg="오류가 발생했습니다. 잠시 후 다시 시도하세요."):
    try:
        respond(msg)
    except Exception:
        pass

def resolve_user_name(client, user_id):
    try:
        info = client.users_info(user=user_id)
        p = info["user"]["profile"]
        return p.get("display_name") or p.get("real_name") or user_id
    except Exception:
        return user_id
    
def resolve_user_email(client, user_id: str) -> str | None:
    try:
        info = client.users_info(user=user_id)  # users:read.email 필요
        return info["user"]["profile"].get("email")
    except Exception:
        return None
    
def append_log(user_key, user_name, type_, note="", date_str="", by_user=None):
    # user_key, by_user에는 이제 이메일 또는 ID가 들어옴
    now = dt.datetime.now(KST).isoformat(timespec="seconds")
    row = [now, user_key, user_name or "", type_, note, date_str, "auto", by_user or user_key]
    logs.append_rows([row], value_input_option="USER_ENTERED", table_range="A1:H1")

def is_admin(user_id: str, client=None) -> bool:
    # 1) ID 화이트리스트
    if user_id in ADMIN_ID_SET:
        return True
    # 2) 이메일 화이트리스트
    if client and ADMIN_EMAIL_SET:
        try:
            info = client.users_info(user=user_id)
            email = (info["user"]["profile"].get("email") or "").lower()
            return email in ADMIN_EMAIL_SET
        except Exception:
            return False
    return False

# ---------- 에러 핸들러 데코레이터 : 슬래시 커맨드에서 예외 발생 시 통일 메시지. ----------
def slash_guard(fn):
    def _w(ack, body, respond, *args, **kwargs):
        try:
            return fn(ack, body, respond, *args, **kwargs)
        except Exception as e:
            reply_error(respond, f"처리 중 오류: {e}")
    return _w


# ---------- 뷰 생성기 ----------
def build_attendance_view(selected_action=None, preserved=None):
    action_options = [
        {"text": {"type": "plain_text", "text": "연차"},  "value": "annual"},
        {"text": {"type": "plain_text", "text": "반차"},  "value": "halfday"},
    ]
    blocks = [
        {
            "type": "input",
            "block_id": "action_b",
            "dispatch_action": True,  # 여기
            "label": {"type": "plain_text", "text": "항목"},
            "element": {
                "type": "static_select",
                "action_id": "action",
                "placeholder": {"type": "plain_text", "text": "선택하세요"},
                "options": action_options,
                **({"initial_option": next((o for o in action_options if o["value"] == selected_action), None)}
                   if selected_action else {})
            },
        },
        {
            "type": "input",
            "block_id": "date_b",
            "optional": True,
            "label": {"type": "plain_text", "text": "날짜(연차/반차)"},
            "element": {"type": "datepicker", "action_id": "date"},
        },
        {
            "type": "input",
            "block_id": "note_b",
            "optional": True,
            "label": {"type": "plain_text", "text": "메모"},
            "element": {"type": "plain_text_input", "action_id": "note"},
        },
    ]

    if selected_action == "halfday":
        blocks.insert(2, {
            "type": "input",
            "block_id": "half_b",
            "label": {"type": "plain_text", "text": "반차 구분"},
            "element": {
                "type": "radio_buttons",
                "action_id": "half_period",
                "options": [
                    {"text": {"type": "plain_text", "text": "오전(AM)"}, "value": "am"},
                    {"text": {"type": "plain_text", "text": "오후(PM)"}, "value": "pm"},
                ],
            },
        })

    return {
        "type": "modal",
        "callback_id": "attendance_submit",
        "title": {"type": "plain_text", "text": "근태 등록"},
        "submit": {"type": "plain_text", "text": "저장"},
        "close": {"type": "plain_text", "text": "취소"},
        "private_metadata": json.dumps(preserved or {}),
        "blocks": blocks,
    }

    # 반차일 때만 오전/오후 라디오 추가
    if selected_action == "halfday":
        blocks.insert(
            2,
            {
                "type": "input",
                "block_id": "half_b",
                "label": {"type": "plain_text", "text": "반차 구분"},
                "element": {
                    "type": "radio_buttons",
                    "action_id": "half_period",
                    "options": [
                        {"text": {"type": "plain_text", "text": "오전(AM)"}, "value": "am"},
                        {"text": {"type": "plain_text", "text": "오후(PM)"}, "value": "pm"},
                    ],
                },
            },
        )

    return {
        "type": "modal",
        "callback_id": "attendance_submit",
        "title": {"type": "plain_text", "text": "근태 등록"},
        "submit": {"type": "plain_text", "text": "저장"},
        "close": {"type": "plain_text", "text": "취소"},
        "private_metadata": json.dumps(preserved or {}),
        "blocks": blocks,
    }
    
# --- 모달 뷰 빌더(관리자용) ---
def build_admin_view():
    return {
        "type": "modal",
        "callback_id": "admin_attendance_submit",
        "title": {"type": "plain_text", "text": "관리자 근태 입력"},
        "submit": {"type": "plain_text", "text": "저장"},
        "close": {"type": "plain_text", "text": "취소"},
        "blocks": [
            {   # 대상자 선택
                "type": "input",
                "block_id": "target_b",
                "label": {"type": "plain_text", "text": "대상자"},
                "element": {"type": "users_select", "action_id": "target"}
            },
            {   # 항목
                "type": "input",
                "block_id": "action_b",
                "label": {"type": "plain_text", "text": "항목"},
                "element": {
                    "type": "static_select",
                    "action_id": "action",
                    "placeholder": {"type": "plain_text", "text": "선택"},
                    "options": [
                        {"text": {"type": "plain_text", "text": "출근"},   "value": "checkin"},
                        {"text": {"type": "plain_text", "text": "퇴근"},   "value": "checkout"},
                        {"text": {"type": "plain_text", "text": "연차"},   "value": "annual"},
                        {"text": {"type": "plain_text", "text": "반차"},   "value": "halfday"},
                    ]
                }
            },
            {   # 반차 구분(선택사항)
                "type": "input",
                "block_id": "half_b",
                "optional": True,
                "label": {"type": "plain_text", "text": "반차 구분"},
                "element": {
                    "type": "radio_buttons",
                    "action_id": "half_period",
                    "options": [
                        {"text": {"type": "plain_text", "text": "오전(AM)"}, "value": "am"},
                        {"text": {"type": "plain_text", "text": "오후(PM)"}, "value": "pm"},
                    ],
                },
            },
            {   # 일자
                "type": "input",
                "block_id": "date_b",
                "optional": True,
                "label": {"type": "plain_text", "text": "날짜(연차/반차 필수)"},
                "element": {"type": "datepicker", "action_id": "date"}
            },
            {   # 메모
                "type": "input",
                "block_id": "note_b",
                "optional": True,
                "label": {"type": "plain_text", "text": "메모"},
                "element": {"type": "plain_text_input", "action_id": "note"}
            },
        ]
    }
  
def disp_width(s: str) -> int:
    w = 0
    for ch in s or "":
        e = ud.east_asian_width(ch)
        w += 2 if e in ("W","F") else 1
    return w

def pad_right(s: str, target: int) -> str:
    w = disp_width(s)
    return s + " " * max(0, target - w)  
  
def render_week_table(week: str, r: dict) -> str:
    cols = [("월", r.get("Mon", "-") or "-"),
            ("화", r.get("Tue", "-") or "-"),
            ("수", r.get("Wed", "-") or "-"),
            ("목", r.get("Thu", "-") or "-"),
            ("금", r.get("Fri", "-") or "-"),
            ("토", r.get("Sat", "-") or "-"),
            ("일", r.get("Sun", "-") or "-")]

    # 각 칸 최소폭 2. 헤더/값 중 큰 표시폭으로 고정
    widths = []
    for k, v in cols:
        widths.append(max(2, disp_width(k), disp_width(v)))
    header = " ".join(pad_right(k, w) for (k, _), w in zip(cols, widths))
    values = " ".join(pad_right(v, w) for (_, v), w in zip(cols, widths))

    return f"*{week} 주간 스케줄*\n```{header}\n{values}```"
  
@app.event("app_mention")
def on_mention(body, say):
    say("`/근태` 또는 `출근`/`퇴근` 키워드를 사용하세요.")
    
@app.command("/출근")
def 출근_cmd(ack, body, respond, client):
    ack()
    uid = body["user_id"]
    email = resolve_user_email(client, uid)    # ← 이메일
    uname = resolve_user_name(client, uid)     # 선택사항
    append_log(email, uname, "checkin", by_user=email)
    respond(f"{uname} 출근 등록 완료")

@app.command("/퇴근")
def 퇴근_cmd(ack, body, respond, client):
    ack()
    uid = body["user_id"]
    email = resolve_user_email(client, uid)
    uname = resolve_user_name(client, uid)
    append_log(email, uname, "checkout", by_user=email)
    respond(f"{uname} 퇴근 등록 완료")


@app.message(re.compile(r"^\s*(출근|퇴근)\s*$"))
def on_keyword(message, say, context):
    t = "출근" if context["matches"][0] == "출근" else "퇴근"
    append_log(message["user"], "", t)
    say(f"{context['matches'][0]} 등록 완료")

# ---------- /근태: 모달 열기 ----------
@app.command("/근태")
def 근태_modal(ack, body, client):
    ack()
    view = build_attendance_view(
        selected_action=None,
        preserved={"channel_id": body.get("channel_id")}
    )
    client.views_open(trigger_id=body["trigger_id"], view=view)
    
# --- 커맨드: /근태관리 ---
# 호출부
@app.command("/근태관리")
def admin_modal(ack, body, client, respond):
    ack()
    if not is_admin(body["user_id"], client):   # 이제 OK
        respond("권한 없음.")
        return
    client.views_open(trigger_id=body["trigger_id"], view=build_admin_view())

# ---------- 선택 변경 시: 모달 업데이트 ----------
@app.block_action("action")
def on_action_change(ack, body, client):
    ack()
    selected = body["actions"][0]["selected_option"]["value"]  # "annual" or "halfday"
    view_id = body["view"]["id"]
    # 기존 private_metadata 유지
    meta = body["view"].get("private_metadata")
    new_view = build_attendance_view(selected_action=selected, preserved=json.loads(meta or "{}"))
    client.views_update(view_id=view_id, view=new_view)

# ---------- 제출 처리 ----------
@app.view("attendance_submit")
def 근태_submit(ack, body, view, client):
    vals = view["state"]["values"]
    action = vals["action_b"]["action"]["selected_option"]["value"]  # "annual" | "halfday"
    selected_date = vals.get("date_b", {}).get("date", {}).get("selected_date")  # YYYY-MM-DD or None
    note = (vals.get("note_b", {}).get("note", {}).get("value") or "").strip()
    user_id = body["user"]["id"]

    # 검증
    errors = {}
    if action in ("annual", "halfday") and not selected_date:
        errors["date_b"] = "날짜를 선택하세요."

    half_period = None
    if action == "halfday":
        hp = vals.get("half_b", {}).get("half_period", {}).get("selected_option")
        if not hp:
            errors["half_b"] = "반차는 오전/오후 선택이 필요합니다."
        else:
            half_period = hp["value"]  # "am" | "pm"

    if errors:
        ack(response_action="errors", errors=errors)
        return

    # 통과
    ack()
    uid = body["user"]["id"]
    email = resolve_user_email(client, uid)
    uname = resolve_user_name(client, uid)

    # 사용자명 조회(스코프 없으면 ID로 대체)
    try:
        info = client.users_info(user=user_id)
        prof = info["user"]["profile"]
        user_name = prof.get("display_name") or prof.get("real_name") or user_id
    except Exception:
        user_name = user_id

    # 로그 기록 규칙
    # type: "annual" | "halfday"
    # note: 반차면 "(오전)" 또는 "(오후)" 접미사 부여
    if action == "halfday" and half_period:
        note = f"{note} (오전)" if half_period == "am" else f"{note} (오후)"

    append_log(email, uname, action, note=note, date_str=(selected_date or ""), by_user=email)

    # 에페메럴 알림
    meta = json.loads(view.get("private_metadata") or "{}")
    ch = meta.get("channel_id")
    if ch:
        if action == "annual":
            txt = f"연차 {selected_date} 등록 완료"
        elif action == "halfday":
            txt = f"반차 {selected_date} 등록 완료"
        else:
            txt = "처리 완료"
        client.chat_postEphemeral(channel=ch, user=user_id, text=txt)
        
# --- 제출: admin_attendance_submit ---
@app.view("admin_attendance_submit")
def admin_submit(ack, body, view, client):
    vals = view["state"]["values"]
    target_uid = vals["target_b"]["target"]["selected_user"]
    action = vals["action_b"]["action"]["selected_option"]["value"]  # checkin|checkout|annual|halfday
    date_str = (vals.get("date_b", {}).get("date", {}) or {}).get("selected_date")  # YYYY-MM-DD or None
    note = (vals.get("note_b", {}).get("note", {}).get("value") or "").strip()
    half = vals.get("half_b", {}).get("half_period", {}).get("selected_option")
    if action == "halfday":
        if not half:
            ack(response_action="errors", errors={"half_b": "반차는 오전/오후 선택이 필요합니다."})
            return
        note = f"{note} (오전)" if half["value"] == "am" else f"{note} (오후)"
    if action in ("annual", "halfday") and not date_str:
        ack(response_action="errors", errors={"date_b": "연차/반차는 날짜가 필요합니다."})
        return

    ack()

    # 이메일/이름
    target_email = resolve_user_email(client, target_uid)
    target_name = resolve_user_name(client, target_uid)
    admin_email = resolve_user_email(client, body["user"]["id"])

    # 기록: user_key=대상자, by=관리자
    append_log(target_email, target_name, action, note=note, date_str=(date_str or ""), by_user=admin_email)

    # 알림(에페메럴은 커맨드 채널로 보낼 수 있음)
    try:
        channel = body.get("channel", {}).get("id")  # slash로 호출 시 없을 수 있음
        if channel:
            client.chat_postMessage(channel=channel, text=f"[관리자 대리입력] {target_name} {action} 등록 완료")
    except Exception:
        pass

@app.command("/잔여")
def 잔여_cmd(ack, body, respond, client):
    ack()
    uid = body["user_id"]
    ukey = safe_user_key(client, uid)
    row = None
    eff = effective_left_for(ukey)
    try:
        row = find_balance_row_for(ukey)
    except Exception as e:
        reply_error(respond, f"balances 시트 조회 실패: {e}")
        return

    if eff is None:
        respond("balances에 사용자 행이 없습니다. 관리자에게 문의하세요.")
        return

    total = row.get("annual_total") or "0"
    used  = row.get("annual_used")  or "0"
    left  = row.get("annual_left")  or "0"
    half  = row.get("half_used")    or "0"
    uname = row.get("user_name")    or safe_user_name(client, uid)

    blocks = [
        {"type": "header", "text": {"type":"plain_text","text":"연차 잔여 요약"}},
        {"type": "section","fields":[
            {"type":"mrkdwn","text":f"*이름*\n{uname}"},
            {"type":"mrkdwn","text":f"*식별자*\n{ukey}"},
            {"type":"mrkdwn","text":f"*총 연차*\n{total}"},
            {"type":"mrkdwn","text":f"*사용*\n{used}"},
            {"type":"mrkdwn","text":f"*잔여*\n*{left}*"},
            {"type":"mrkdwn","text":f"*반차 사용*\n{half}"},
        ]},
        {"type":"context","elements":[{"type":"mrkdwn","text":"balances 시트 기준"}]}
    ]
    respond(blocks=blocks, text="잔여 요약")

@app.command("/스케줄")
def 스케줄_cmd(ack, body, respond, client):
    ack()
    text = (body.get("text") or "").strip()
    if ISO_WEEK_RE.match(text):
        week = text
    elif DATE_RE.match(text):
        week = date_to_iso_week_kst(text)
    else:
        week = current_iso_week_kst()

    uid = body["user_id"]
    ukey = safe_user_key(client, uid)
    try:
        r = find_schedule_for(week, ukey)
    except Exception as e:
        reply_error(respond, f"schedule_weekly 시트 조회 실패: {e}")
        return

    if not r:
        sugg = available_weeks_for_user(ukey)
        respond(f"{week} 주차 스케줄 없음. 사용 가능한 주: {', '.join(sugg[:10])}" if sugg else f"{ukey} 행이 없습니다.")
        return

    respond(text=render_week_table(week, r))

@app.command("/잔여갱신")
def 잔여갱신_modal(ack, body, client, respond):
    ack()
    if not is_admin(body["user_id"], client):
        respond("권한 없음.")
        return
    today = dt.datetime.now(KST).date().isoformat()
    client.views_open(trigger_id=body["trigger_id"],
                      view=build_override_view(initial_left="", initial_date=today))
    
@app.view("override_submit")
def 잔여갱신_submit(ack, body, view, client):
    vals = view["state"]["values"]
    target_uid = vals["target_b"]["target"]["selected_user"]
    left_str = (vals["left_b"]["left"]["value"] or "").strip()
    from_date = vals["from_b"]["from_date"].get("selected_date")
    note = (vals.get("note_b",{}).get("note",{}).get("value") or "").strip()
    errors = {}
    try:
        left_val = float(left_str)
        if left_val < 0: errors["left_b"] = "0 이상 입력"
    except Exception:
        errors["left_b"] = "숫자 형식으로 입력"
    if not from_date: errors["from_b"] = "기준 시작일 선택"
    if errors:
        ack(response_action="errors", errors=errors); return
    ack()

    # 키 해석
    ukey = safe_user_key(client, target_uid)
    uname = safe_user_name(client, target_uid)

    # balances upsert
    ws, idx, rows, pos = get_balance_row_map()
    rn = pos.get((ukey or "").strip().lower())
    now_iso = dt.datetime.now(KST).isoformat(timespec="seconds")
    # 보장 헤더
    need = ["user_key","user_name","annual_total","annual_used","annual_left","half_used","override_left","override_from","last_admin_update","notes"]
    if rows:
        head = [h.strip().lower() for h in rows[0]]
        if any(n not in head for n in ["user_key","user_name"]):
            raise RuntimeError("balances 헤더에 user_key/user_name 필요")
    # 새 행
    row = [ukey, uname, "", "", "", "", str(left_val), from_date, now_iso, note]
    if rn:
        ws.update(
            range_name=f"A{rn}:J{rn}",
            values=[row],
            value_input_option="USER_ENTERED",
        )
    else:
        # 헤더가 없다면 추가
        if not rows:
            ws.append_row(["user_key","user_name","annual_total","annual_used","annual_left","half_used","override_left","override_from","last_admin_update","notes"])
        ws.append_row(row)

    # 알림
    try:
        client.chat_postMessage(channel=body["user"]["id"], text=f"[잔여 기준선 설정] {uname}: {left_val}일, 기준일 {from_date}")
    except Exception:
        pass

if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
