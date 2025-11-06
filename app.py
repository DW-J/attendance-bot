import os, re, pytz, datetime as dt
import gspread
import json
import unicodedata as ud
import time, random, threading
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from google.oauth2.service_account import Credentials
from typing import Callable

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(os.environ["SHEET_ID"])
logs = sh.worksheet("logs")
app = App(token=os.environ["SLACK_BOT_TOKEN"])
KST = pytz.timezone("Asia/Seoul")

ADMIN_ID_SET = {s.strip() for s in (os.getenv("ADMIN_IDS") or "").split(",") if s.strip()} # Slack 사용자 ID 화이트리스트
ADMIN_EMAIL_SET = {e.strip().lower() for e in (os.getenv("ADMIN_EMAILS") or "").split(",") if e.strip()} # 이메일 화이트리스트

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$") # YYYY-MM-DD

ISO_WEEK_RE = re.compile(r"^\d{4}-W\d{2}$") # YYYY-Www

DEDUP_WINDOW_SEC = 60          # 동일 사용자/타입/날짜 60초 내 중복 방지
DAILY_UNIQUE = {"checkin","checkout"}  # 하루 1회만 허용하는 타입

# 인플라이트 처리용 잠금 및 집합
_inflight_lock = threading.Lock()
_inflight = set()  # idempotency key 잠금

# 캐시
user_cache = {}
user_email_cache = {}

# --- 날짜 문자열 파싱 ---
def parse_date(s):
    try:
        y,m,d = map(int, s.split("-")); return dt.date(y,m,d)
    except Exception: return None

# --- 오늘 KST 날짜 문자열 ---    
def today_kst_ymd():
    return dt.datetime.now(KST).date().isoformat()

# --- 특정 사용자에 대한 특정 일자 이후 사용 일수 집계 ---    
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

# --- 잔여일수 행 맵 조회 ---
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

def to_float(s, default=0.0):
    try: return float(str(s).strip())
    except: return default

def effective_left_for(user_key: str):
    ws = sh.worksheet("balances")
    vals = ws.get_all_values()
    if not vals: return 0.0
    head = [h.strip().lower() for h in vals[0]]
    col = {h:i for i,h in enumerate(head)}
    rn = None
    for rnum, r in enumerate(vals[1:], start=2):
        uk = (r[col.get("user_key",0)] if col.get("user_key",0) < len(r) else "").strip().lower()
        if uk == (user_key or "").strip().lower():
            row = r; rn = rnum; break
    if not rn: return 0.0

    o_left = row[col["override_left"]] if "override_left" in col and col["override_left"] < len(row) else ""
    o_from = row[col["override_from"]] if "override_from" in col and col["override_from"] < len(row) else ""
    if str(o_left).strip():
        base = to_float(o_left, 0.0)
        since = None
        try:
            y,m,d = map(int, str(o_from).split("-"))
            since = dt.date(y,m,d)
        except: pass
        used = logs_usage_since(user_key, since_date=since)  # 기존 함수 사용
        return max(0.0, base - used)

    # fallback: 연간 계산
    total = to_float(row[col.get("annual_total","")], 0.0)
    year = dt.datetime.now(KST).year
    used = 0.0
    ws_logs = sh.worksheet("logs")
    lvals = ws_logs.get_all_values()
    if lvals:
        lhead = [h.strip().lower() for h in lvals[0]]
        iu = lhead.index("user_key") if "user_key" in lhead else lhead.index("user_id")
        it = lhead.index("type"); idt = lhead.index("date")
        for r in lvals[1:]:
            if iu >= len(r) or it >= len(r) or idt >= len(r): continue
            if (r[iu] or "").strip().lower() != (user_key or "").strip().lower(): continue
            t = (r[it] or "").strip().lower()
            try:
                y,m,d = map(int, (r[idt] or "").split("-"))
                dd = dt.date(y,m,d)
            except:
                continue
            if dd.year != year: continue
            used += 1.0 if t=="annual" else (0.5 if t=="halfday" else 0.0)
    return max(0.0, total - used)


# --- 잔여일수 재정의 모달 뷰 빌더 ---
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

# --- 잔여일수 재계산 ---
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

    # 연차/반차 집계
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

    # 벌크 업데이트 대상 수집
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

# --- 날짜 문자열을 KST ISO 주차 문자열로 변환 ---
def date_to_iso_week_kst(date_str: str) -> str:
    y, m, d = map(int, date_str.split("-"))
    day = dt.datetime(y, m, d, tzinfo=KST).date()
    Y, W, _ = day.isocalendar()
    return f"{Y}-W{W:02d}"

# --- 사용자에 대한 사용 가능한 주차 목록 조회 ---
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

# --- 시트 조회 ---
def get_ws(name: str):
    try:
        return sh.worksheet(name)
    except Exception:
        raise RuntimeError(f"시트 '{name}'를 찾을 수 없습니다.")

# --- 시트 행을 딕셔너리 목록으로 변환 ---    
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

# --- 잔여일수 행 조회 ---
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


# --- 현재 KST ISO 주차 문자열 ---
def current_iso_week_kst() -> str:
    today = dt.datetime.now(KST).date()
    y, w, _ = today.isocalendar()
    return f"{y}-W{w:02d}"

# --- 주간 스케줄 조회 ---
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

# --- 사용자 키 안전 조회 ---
def safe_user_key(client, slack_user_id: str) -> str:
    """이메일 우선. 실패 시 Slack ID."""
    try:
        info = client.users_info(user=slack_user_id)
        email = info["user"]["profile"].get("email")
        return email or slack_user_id
    except Exception:
        return slack_user_id

# --- 사용자 이름 안전 조회 ---
def safe_user_name(client, slack_user_id: str) -> str:
    try:
        info = client.users_info(user=slack_user_id)
        p = info["user"]["profile"]
        return p.get("display_name") or p.get("real_name") or slack_user_id
    except Exception:
        return slack_user_id

# --- 오류 응답 안전 처리 ---
def reply_error(respond, msg="오류가 발생했습니다. 잠시 후 다시 시도하세요."):
    try:
        respond(msg)
    except Exception:
        pass

# --- 사용자 이름 조회 ---
def resolve_user_name(client, user_id):
    try:
        info = client.users_info(user=user_id)
        p = info["user"]["profile"]
        return p.get("display_name") or p.get("real_name") or user_id
    except Exception:
        return user_id

# --- 사용자 이메일 조회 ---    
def resolve_user_email(client, user_id: str) -> str | None:
    try:
        info = client.users_info(user=user_id)  # users:read.email 필요
        return info["user"]["profile"].get("email")
    except Exception:
        return None

# --- 로그 기록 추가 ---    
def append_log(user_key, user_name, type_, note="", date_str="", by_user=None):
    # user_key, by_user에는 이제 이메일 또는 ID가 들어옴
    now = dt.datetime.now(KST).isoformat(timespec="seconds")
    row = [now, user_key, user_name or "", type_, note, date_str, "auto", by_user or user_key]
    logs.append_rows([row], value_input_option="USER_ENTERED", table_range="A1:H1")

# --- 관리자 여부 확인 ---
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

# --- idempotency key 생성 ---
def idemp_key(user_key: str, type_: str, date_str: str) -> str:
    # date_str 없으면 오늘
    ds = date_str or dt.datetime.now(KST).date().isoformat()
    return f"{user_key}|{type_.lower()}|{ds}"

# --- 지수적 백오프 재시도 ---
def with_retry(fn: Callable, *, retries=5, base=0.2, max_sleep=2.0):
    """
    지수적 백오프 재시도. gspread 네트워크/429 방어.
    """
    last = None
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            last = e
            sleep = min(max_sleep, base * (2 ** i) + random.uniform(0, base))
            time.sleep(sleep)
    raise last

# --- 관리자 요청 기록 ---
def record_admin_request(admin_key, target_key, action, date_str, note, result, error=""):
    ws = get_ws("admin_requests")
    now = dt.datetime.now(KST).isoformat(timespec="seconds")
    row = [now, admin_key, target_key, action, date_str or "", note or "", result, error or ""]
    return with_retry(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))

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
    # 반차일 때만 오전/오후 라디오 추가
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

# --- 문자열 표시폭 계산 ---  
def disp_width(s: str) -> int:
    w = 0
    for ch in s or "":
        e = ud.east_asian_width(ch)
        w += 2 if e in ("W","F") else 1
    return w

# --- 우측 패딩 ---
def pad_right(s: str, target: int) -> str:
    w = disp_width(s)
    return s + " " * max(0, target - w)  

# --- 주간 스케줄 테이블 렌더링 ---  
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

# --- 오늘 이미 기록했는지 검사 ---
def already_logged_today(user_key: str, type_: str, date_str: str, note_tag: str | None = None) -> bool:
    ws = logs
    ds = (date_str or today_kst_ymd()).strip()
    vals = ws.get_all_values()
    if not vals: return False
    head = [h.strip().lower() for h in vals[0]]
    idx = {h:i for i,h in enumerate(head)}
    iu = idx.get("user_key") or idx.get("user_id")
    it = idx.get("type"); idate = idx.get("date"); inote = idx.get("note")
    if iu is None or it is None or idate is None: return False

    uk_l = (user_key or "").strip().lower()
    tp_l = (type_ or "").strip().lower()
    for r in vals[1:]:
        uk = (r[iu] if iu < len(r) else "").strip().lower()
        tp = (r[it] if it < len(r) else "").strip().lower()
        ds2 = (r[idate] if idate < len(r) else "").strip()
        if uk != uk_l or tp != tp_l or ds2 != ds:
            continue
        # halfday는 오전/오후까지 동일해야 중복으로 간주
        if tp_l == "halfday" and note_tag:
            note = (r[inote] if inote is not None and inote < len(r) else "")
            if note_tag == "am" and "(오전)" in note:
                return True
            if note_tag == "pm" and "(오후)" in note:
                return True
            continue
        return True
    return False

# --- 중복 처리 방지 및 일일 1회 제한 적용 후 기록 추가 ---
def guard_and_append(user_key, user_name, type_, note="", date_str="", by_user=None, note_tag=None):
    ds = (date_str or today_kst_ymd()).strip()
    key = idemp_key(user_key, type_, ds)
    with _inflight_lock:
        if key in _inflight:
            raise RuntimeError("중복 처리 중입니다. 잠시 후 다시 시도하세요.")
        _inflight.add(key)
    try:
        t = type_.lower()
        if t in DAILY_UNIQUE and already_logged_today(user_key, t, ds):
            raise RuntimeError(f"이미 오늘 {t} 기록이 있습니다.")
        if t == "annual" and already_logged_today(user_key, "annual", ds):
            raise RuntimeError("이미 해당 날짜에 연차 기록이 있습니다.")
        if t == "halfday" and already_logged_today(user_key, "halfday", ds, note_tag=note_tag):
            # 오전/오후 동일 건만 차단
            tag_txt = "오전" if note_tag == "am" else "오후"
            raise RuntimeError(f"이미 해당 날짜 {tag_txt} 반차 기록이 있습니다.")

        def _do():
            return append_log(user_key, user_name, type_, note=note, date_str=ds, by_user=by_user)
        return with_retry(_do)
    finally:
        with _inflight_lock:
            _inflight.discard(key)
    
# --- 예외를 사람이 읽을 수 있는 메시지로 변환 ---            
def human_error(e: Exception) -> str:
    s = str(e)
    if "PERMISSION" in s.upper():
        return "권한 오류. 시트 공유와 스코프를 확인하세요."
    if "429" in s or "Rate Limit" in s:
        return "요청이 많습니다. 잠시 후 재시도하세요."
    if "이미 오늘" in s:
        return s
    if "중복 처리 중" in s:
        return s
    return "처리 중 오류가 발생했습니다."

# ---과거 빈 date 백필
def backfill_dates_from_timestamps():
    ws = logs
    vals = ws.get_all_values()
    if not vals: return
    head = [h.strip().lower() for h in vals[0]]
    idx = {h:i for i,h in enumerate(head)}
    idate = idx.get("date"); its = idx.get("timestamp") or idx.get("ts")
    if idate is None or its is None:
        return
    updates = []
    for rnum, r in enumerate(vals[1:], start=2):
        d = (r[idate] if idate < len(r) else "").strip()
        if not d:
            ts = (r[its] if its < len(r) else "").strip()
            if ts:
                d = ts.split("T", 1)[0]
                updates.append((rnum, d))
    for rnum, d in updates:
        logs.update(
            range_name=f"F{rnum}:F{rnum}",  # date 열 위치가 F
            values=[[d]],
            value_input_option="USER_ENTERED",
        )

# --- 중복 기록 검사 및 메시지 생성 ---
def dup_error_msg_for(action: str, user_key: str, date_str: str, half_period: str | None) -> str | None:
    t = action.lower()
    ds = (date_str or today_kst_ymd()).strip()
    if t in {"checkin","checkout"} and already_logged_today(user_key, t, ds):
        return f"이미 오늘 {t} 기록이 있습니다."
    if t == "annual" and already_logged_today(user_key, "annual", ds):
        return "이미 해당 날짜에 연차 기록이 있습니다."
    if t == "halfday" and already_logged_today(user_key, "halfday", ds, note_tag=half_period):
        tag_txt = "오전" if half_period == "am" else "오후"
        return f"이미 해당 날짜 {tag_txt} 반차 기록이 있습니다."
    return None

# --- balances 시트에 행 삽입 또는 업데이트 ---
def upsert_balances_row(ukey, uname, *, override_left=None, override_from=None, note=""):
    ws = sh.worksheet("balances")
    vals = ws.get_all_values()
    if not vals:
        ws.append_row(["user_key","user_name","annual_total","annual_used","annual_left","half_used",
                       "override_left","override_from","last_admin_update","notes"])
        vals = ws.get_all_values()
    head = [h.strip().lower() for h in vals[0]]
    col = {h:i for i,h in enumerate(head)}  # 0-based
    # 행 찾기
    pos = None
    for rnum, r in enumerate(vals[1:], start=2):
        if (r[0] if r else "").strip().lower() == (ukey or "").strip().lower():
            pos = rnum; break
    # 행이 없으면 append
    if pos is None:
        ws.append_row([""]*10, value_input_option="USER_ENTERED")
        pos = len(vals) + 1  # 새 행 번호

    # 공통 필드
    updates = []
    def set_cell(name, value):
        c = col.get(name); 
        if c is not None:
            rng = f"{chr(ord('A')+c)}{pos}:{chr(ord('A')+c)}{pos}"
            updates.append((rng, [[value]]))

    set_cell("user_key", ukey)
    set_cell("user_name", uname)
    if override_left is not None:
        set_cell("override_left", str(override_left))
    if override_from is not None:
        set_cell("override_from", override_from)
    set_cell("last_admin_update", dt.datetime.now(KST).isoformat(timespec="seconds"))
    if note:
        set_cell("notes", note)

    # 일괄 업데이트
    for rng, v in updates:
        ws.update(range_name=rng, values=v, value_input_option="USER_ENTERED")


# ---------- 이벤트 핸들러 등록 ----------
@app.event("app_home_opened")
def _home_noop(event, logger):
    logger.debug(f"home opened by {event.get('user')}")
  
# ---------- 멘션 및 키워드 처리 ----------
@app.event("app_mention")
def on_mention(body, say):
    say("`/근태` 또는 `출근`/`퇴근` 키워드를 사용하세요.")
    
@app.command("/출근")
def 출근_cmd(ack, body, respond, client):
    ack()
    uid = body["user_id"]
    ukey = safe_user_key(client, uid)
    uname = safe_user_name(client, uid)
    try:
        guard_and_append(ukey, uname, "checkin", by_user=ukey)
        respond(f"{uname} 출근 등록 완료")
    except Exception as e:
        respond(human_error(e))

@app.command("/퇴근")
def 퇴근_cmd(ack, body, respond, client):
    ack()
    uid = body["user_id"]
    ukey = safe_user_key(client, uid)
    uname = safe_user_name(client, uid)
    try:
        guard_and_append(ukey, uname, "checkout", by_user=ukey)
        respond(f"{uname} 퇴근 등록 완료")
    except Exception as e:
        respond(human_error(e))


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
    action = vals["action_b"]["action"]["selected_option"]["value"]  # annual|halfday|checkin|checkout
    selected_date = (vals.get("date_b", {}).get("date", {}) or {}).get("selected_date")
    note = (vals.get("note_b", {}).get("note", {}).get("value") or "").strip()

    half_period = None
    if action == "halfday":
        hp = vals.get("half_b", {}).get("half_period", {}).get("selected_option")
        half_period = hp["value"] if hp else None

    # 사용자 키/이름
    uid = body["user"]["id"]
    ukey = safe_user_key(client, uid)
    uname = safe_user_name(client, uid)

    # 1) 필수값 검증
    errors = {}
    if action in ("annual","halfday") and not selected_date:
        errors["date_b"] = "연차/반차는 날짜가 필요합니다."
    if action == "halfday" and not half_period:
        errors["half_b"] = "반차는 오전/오후 선택이 필요합니다."

    # 2) 중복 사전검증
    if not errors:
        dup = dup_error_msg_for(action, ukey, selected_date or today_kst_ymd(), half_period)
        if dup:
            # 블록 위치를 맞춰 에러 핀
            if action == "annual":
                errors["date_b"] = dup
            elif action == "halfday":
                # 오전/오후 중복이면 half_b, 그 외는 date_b
                errors["half_b" if "반차" in dup else "date_b"] = dup
            else:
                errors["action_b"] = dup

    # 3) 에러 있으면 폼 에러로 응답하고 종료
    if errors:
        ack(response_action="errors", errors=errors)
        return

    # 4) 통과 → ack 후 기록
    ack()

    if action == "halfday":
        if half_period == "am": note = f"{note} (오전)"
        elif half_period == "pm": note = f"{note} (오후)"

    try:
        guard_and_append(
            ukey, uname, action,
            note=note,
            date_str=(selected_date or ""),
            by_user=ukey,
            note_tag=half_period
        )
    except Exception as e:
        # 예외는 DM로 알려줌. 뷰는 이미 닫힘.
        try:
            client.chat_postMessage(channel=uid, text=human_error(e))
        except Exception:
            pass

        
# --- 제출: admin_attendance_submit ---
@app.view("admin_attendance_submit")
def admin_submit(ack, body, view, client):
    vals = view["state"]["values"]
    target_uid = vals["target_b"]["target"]["selected_user"]
    action = vals["action_b"]["action"]["selected_option"]["value"]      # checkin|checkout|annual|halfday
    date_str = (vals.get("date_b", {}).get("date", {}) or {}).get("selected_date")  # YYYY-MM-DD or None
    note = (vals.get("note_b", {}).get("note", {}).get("value") or "").strip()
    half_period = None
    if action == "halfday":
        hp = vals.get("half_b", {}).get("half_period", {}).get("selected_option")
        half_period = hp["value"] if hp else None

    # 기본 필수 검증
    errors = {}
    if action in ("annual","halfday") and not date_str:
        errors["date_b"] = "연차/반차는 날짜가 필요합니다."
    if action == "halfday" and not half_period:
        errors["half_b"] = "반차는 오전/오후 선택이 필요합니다."

    # 사용자 키 준비
    admin_uid = body["user"]["id"]
    admin_key = safe_user_key(client, admin_uid)
    target_key = safe_user_key(client, target_uid)
    target_name = safe_user_name(client, target_uid)

    # 중복 사전검증
    if not errors:
        dup = dup_error_msg_for(action, target_key, date_str or today_kst_ymd(), half_period)
        if dup:
            # 블록 위치에 맞춰 에러 핀 지정
            if action == "annual":
                errors["date_b"] = dup
            elif action == "halfday":
                # 오전/오후 중복이면 half_b, 일반 중복이면 date_b로
                errors["half_b" if "(오전" in dup or "(오후" in dup else "date_b"] = dup
            else:
                errors["action_b"] = dup

    if errors:
        ack(response_action="errors", errors=errors)
        # 감사로그는 실패로 남김
        try:
            record_admin_request(admin_key, target_key, action, date_str or "", note, "fail", "; ".join(errors.values()))
        except Exception:
            pass
        return

    # 통과 → ack 후 실제 기록
    ack()

    if action == "halfday":
        if half_period == "am": note = f"{note} (오전)"
        elif half_period == "pm": note = f"{note} (오후)"

    try:
        guard_and_append(target_key, target_name, action, note=note, date_str=(date_str or ""), by_user=admin_key, note_tag=half_period)
        record_admin_request(admin_key, target_key, action, date_str or "", note, "ok", "")
        client.chat_postMessage(channel=admin_uid, text=f"[관리자 대리입력] {target_name} {action} {date_str or ''} 처리 완료")
    except Exception as e:
        # 여기선 더 이상 raise 금지
        record_admin_request(admin_key, target_key, action, date_str or "", note, "fail", str(e))
        client.chat_postMessage(channel=admin_uid, text=f"[관리자 대리입력 실패] {human_error(e)}")

# ---------- /잔여 커맨드 ----------
@app.command("/잔여")
def 잔여_cmd(ack, body, respond, client):
    ack()
    try:
        uid = body["user_id"]
        ukey = safe_user_key(client, uid)          # 이메일 우선, 실패시 Slack ID
        eff = effective_left_for(ukey)             # override_left/override_from 반영 계산
        if eff is None:
            respond("balances에 사용자 행이 없습니다. 관리자에게 문의하세요.")
        else:
            respond(f"현재 잔여: {eff:g}일")
    except Exception as e:
        respond(f"처리 중 오류: {e}")

# ---------- /스케줄 커맨드 ----------
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

@app.command("/잔여debug")
def 잔여debug(ack, body, respond, client):
    ack()
    uid = body["user_id"]
    ukey = safe_user_key(client, uid)
    ws = sh.worksheet("balances")
    vals = ws.get_all_values()
    head = [h.strip().lower() for h in vals[0]] if vals else []
    row = next((r for r in vals[1:] if (r[0] or "").strip().lower()==ukey.lower()), [])
    resp = {
        "ukey": ukey,
        "override_left": (row[ head.index("override_left") ] if "override_left" in head and row else ""),
        "override_from": (row[ head.index("override_from") ] if "override_from" in head and row else ""),
        "annual_total":  (row[ head.index("annual_total") ] if "annual_total"  in head and row else ""),
        "annual_used":   (row[ head.index("annual_used") ]  if "annual_used"   in head and row else ""),
        "half_used":     (row[ head.index("half_used") ]    if "half_used"     in head and row else ""),
        "effective_left": effective_left_for(ukey),
    }
    respond(f"```{resp}```")


if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
