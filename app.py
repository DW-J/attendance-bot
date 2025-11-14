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
from datetime import timedelta

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(os.environ["SHEET_ID"])
logs = sh.worksheet("logs")
app = App(token=os.environ["SLACK_BOT_TOKEN"])
KST = pytz.timezone("Asia/Seoul")


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$") # YYYY-MM-DD

ISO_WEEK_RE = re.compile(r"^\d{4}-W\d{2}$") # YYYY-Www

DEDUP_WINDOW_SEC = 60          # 동일 사용자/타입/날짜 60초 내 중복 방지
DAILY_UNIQUE = {"checkin","checkout"}  # 하루 1회만 허용하는 타입

DOW_COLS = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

# 캐시
user_cache = {}
user_email_cache = {}

ADMIN_ID_SET = {s.strip() for s in (os.getenv("ADMIN_IDS") or "").split(",") if s.strip()} # Slack 사용자 ID 화이트리스트
ADMIN_EMAIL_SET = {e.strip().lower() for e in (os.getenv("ADMIN_EMAILS") or "").split(",") if e.strip()} # 이메일 화이트리스트

# 과거 날짜 입력 정책
ALLOW_BACKDATE_USER = False   # 사용자는 과거 입력 금지
ALLOW_FUTURE_YEAR_USER = False   # 사용자: 현재 연도 이후 금지

ALLOW_BACKDATE_ADMIN = True   # 관리자는 허용(원하면 False로)
ALLOW_FUTURE_YEAR_ADMIN = True   # 관리자 예외 (원하면 False)

# --- Business day helpers ---
HOLIDAYS_CACHE = None  # set[str] of "YYYY-MM-DD"

REQUIRED_SHEETS = ("logs", "balances", "schedule_weekly", "holidays")

def sheet_exists(name: str) -> bool:
    try:
        get_ws(name)
        return True
    except Exception:
        return False

def require_sheets_or_error(names=REQUIRED_SHEETS) -> str | None:
    missing = [n for n in names if not sheet_exists(n)]
    if missing:
        return "시트가 없습니다: " + ", ".join(missing)
    return None

def this_year() -> int:
    return today_kst_date().year


def year_of(s: str | None) -> int | None:
    d = parse_ymd_safe(s) if s else None
    return d.year if d else None

def spans_future_year(start_s: str, end_s: str) -> bool:
    y1, y2 = year_of(start_s), year_of(end_s)
    if y1 is None or y2 is None:
        return False
    return max(y1, y2) > this_year()

def spans_multiple_years(start_s: str, end_s: str) -> bool:
    y1, y2 = year_of(start_s), year_of(end_s)
    if y1 is None or y2 is None:
        return False
    return y1 != y2

# 표준 타입: checkin / checkout / annual / halfday / off
def normalize_action(v: str | None) -> str | None:
    if not v:
        return None
    s = str(v).strip().lower()
    mapping = {
        "출근": "checkin", "퇴근": "checkout",
        "연차": "annual", "반차": "halfday", "휴무": "off",
        "checkin": "checkin", "checkout": "checkout",
        "annual": "annual", "halfday": "halfday", "off": "off",
    }
    return mapping.get(s)

# --- 관리자 여부 확인 ---
def is_admin(user_id: str, client=None) -> bool:
    # 1) ID 화이트리스트
    if user_id in ADMIN_ID_SET:
        return True
    # 2) 이메일 화이트리스트
    if ADMIN_EMAIL_SET:
        try:
            info = client.users_info(user=user_id)
            email = (info["user"]["profile"].get("email") or "").lower()
            if email in ADMIN_EMAIL_SET:
                return True
        except Exception:
            pass
    return False

# --- 사용자 키/이름 ---
# --- 사용자 키 안전 조회 ---
def safe_user_key(client, slack_user_id: str) -> str:
    """이메일 우선. 실패 시 Slack ID."""
    try:
        info = client.users_info(user=slack_user_id)
        return info["user"]["profile"].get("email") or slack_user_id
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
    
# --- 시간/날짜 ------------------------------------------------------------

# --- 오늘 KST 날짜 문자열 ---    
def today_kst_ymd():
    return dt.datetime.now(KST).date().isoformat()

# --- YYYY-MM-DD 안전 파싱 ---
def parse_ymd_safe(s: str):
    try:
        y, m, d = map(int, s.split("-"))
        return dt.date(y, m, d)
    except Exception:
        return None

# --- 시작일~종료일 날짜 문자열 반복기 ---
def iter_dates(start_s: str, end_s: str):
    start = parse_ymd_safe(start_s)
    end = parse_ymd_safe(end_s)
    if not start or not end:
        return []
    if end < start:
        return []
    cur = start
    one = timedelta(days=1)
    out = []
    while cur <= end:
        out.append(cur.isoformat())
        cur += one
    return out

# --- 날짜 문자열 파싱 ---
def parse_date(s):
    try:
        y,m,d = map(int, s.split("-")); 
        return dt.date(y,m,d)
    except Exception: 
        return None

# --- 워크시트 가져오기 ---    
def get_ws(name: str):
    return sh.worksheet(name)

# --- 지수적 백오프 재시도 ---
def with_retry(fn, retries=5, base=0.2, max_sleep=2.0):
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

# --- 예외를 사람이 읽을 수 있는 메시지로 변환 ---            
def human_error(e: Exception) -> str:
    s = str(e)
    if "이미 오늘" in s or "이미 해당 날짜" in s:
        return s
    if "중복 처리 중" in s:
        return s
    us = s.upper()
    if "PERMISSION" in us or "INSUFFICIENT" in us:
        return "권한 오류. 시트 공유와 API 권한을 확인하세요."
    if "RATE_LIMIT" in us or "429" in us:
        return "요청이 많습니다. 잠시 후 다시 시도하세요."
    return "처리 중 오류가 발생했습니다."

# =========================================================
# logs 기록 / 중복 체크
# =========================================================

# 인플라이트 처리용 잠금 및 집합
_inflight_lock = threading.Lock()
_inflight = set()  # idempotency key 잠금

# --- 로그 기록 추가 ---    
def append_log(user_key, user_name, type_, note="", date_str="", by_user=None):
    ws = get_ws("logs")
    now = dt.datetime.now(KST).isoformat(timespec="seconds")
    row = [
        now,
        user_key,
        user_name or "",
        type_,
        note or "",
        date_str or "",
        "auto",
        by_user or user_key,
    ]
    with_retry(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))

# --- 오늘 이미 기록했는지 검사 ---
def already_logged(user_key: str, type_: str, date_str: str, note_tag: str | None = None, alt_user_key: str | None = None) -> bool:
    ws = get_ws("logs")
    vals = ws.get_all_values()
    if not vals:
        return False

    head = [h.strip().lower() for h in vals[0]]
    idx = {h: i for i, h in enumerate(head)}
    iu = idx.get("user_key") or idx.get("user_id")
    it = idx.get("type")
    idate = idx.get("date")
    inote = idx.get("note")
    if iu is None or it is None or idate is None:
        return False

    # --- 정규화 ---
    want_type = (type_ or "").strip().lower()
    want_date = (date_str or "").strip()
    uk_primary = (user_key or "").strip().lower()
    uk_alt = (alt_user_key or "").strip().lower()
    def is_same_user(cell: str) -> bool:
        v = (cell or "").strip().lower()
        return v == uk_primary or (uk_alt and v == uk_alt)

    for r in vals[1:]:
        if iu >= len(r) or it >= len(r) or idate >= len(r):
            continue
        if not is_same_user(r[iu]):
            continue
        if (r[it] or "").strip().lower() != want_type:
            continue
        if (r[idate] or "").strip() != want_date:
            continue

        # 반차는 오전/오후까지 동일해야 중복
        if want_type == "halfday" and note_tag:
            if inote is None or inote >= len(r):
                continue
            note = r[inote] or ""
            if note_tag == "am" and "(오전)" in note:
                return True
            if note_tag == "pm" and "(오후)" in note:
                return True
            continue

        return True

    return False


# --- idempotency key 생성 ---
def idemp_key(user_key: str, type_: str, date_str: str) -> str:
    # date_str 없으면 오늘
    return f"{(user_key or '').strip().lower()}|{type_.lower()}|{date_str}"

# --- 중복 처리 방지 및 일일 1회 제한 적용 후 기록 추가 ---
def guard_and_append(user_key, user_name, type_, note="", date_str="", by_user=None, note_tag=None,
                     alt_user_key: str | None = None, *, is_admin: bool = False):
    ds = (date_str or today_kst_ymd()).strip()
    t  = (type_ or "").strip().lower()

    # --- 과거 날짜 금지 정책 (최종 가드) ---
    if t in ("annual", "halfday", "off", "checkin", "checkout"):
        if not (is_admin and ALLOW_BACKDATE_ADMIN):
            if is_past_ymd(ds):
                raise RuntimeError("지난 날짜에는 등록할 수 없습니다.")

    key = idemp_key(user_key, t, ds)
    
    with _inflight_lock:
        if key in _inflight:
            raise RuntimeError("중복 처리 중입니다. 잠시 후 다시 시도하세요.")
        _inflight.add(key)

    try:
        # 기본 중복 규칙
        if t in ("checkin", "checkout") and already_logged(user_key, t, ds, alt_user_key=alt_user_key):
            raise RuntimeError(f"이미 오늘 {t} 기록이 있습니다.")
        if t == "annual" and already_logged(user_key, "annual", ds, alt_user_key=alt_user_key):
            raise RuntimeError("이미 해당 날짜에 연차 기록이 있습니다.")
        if t == "halfday" and already_logged(user_key, "halfday", ds, note_tag=note_tag, alt_user_key=alt_user_key):
            tag_txt = "오전" if note_tag == "am" else "오후"
            raise RuntimeError(f"이미 해당 날짜 {tag_txt} 반차 기록이 있습니다.")
        if t == "off" and already_logged(user_key, "off", ds, alt_user_key=alt_user_key):
            raise RuntimeError("이미 해당 날짜에 휴무 기록이 있습니다.")

        # --- 상호배타 규칙 추가 ---
        if t == "halfday":
            # 그 날짜에 '연차'가 이미 있으면 반차 금지
            if already_logged(user_key, "annual", ds, alt_user_key=alt_user_key):
                raise RuntimeError("해당 날짜에 이미 연차가 있어 반차를 등록할 수 없습니다.")
        if t == "annual":
            # 그 날짜에 반차(오전/오후 중 하나라도)가 있으면 연차 금지
            if any_halfday_on_date(user_key, ds, alt_user_key=alt_user_key):
                raise RuntimeError("해당 날짜에 이미 반차가 있어 연차를 등록할 수 없습니다.")

        def _do():
            return append_log(user_key, user_name, t, note=note, date_str=ds, by_user=by_user)

        return with_retry(_do)

    finally:
        with _inflight_lock:
            _inflight.discard(key)


# --- 중복 기록 검사 및 메시지 생성 ---
def dup_error_msg_for(action: str, user_key: str, date_str: str, half_period: str | None, alt_user_key: str | None = None) -> str | None:
    ds = (date_str or today_kst_ymd()).strip()
    t = action.lower()

    if t in {"checkin","checkout"} and already_logged(user_key, t, ds, alt_user_key=alt_user_key):
        return f"이미 오늘 {t} 기록이 있습니다."
    if t == "annual":
        if already_logged(user_key, "annual", ds, alt_user_key=alt_user_key):
            return "이미 해당 날짜에 연차 기록이 있습니다."
        if any_halfday_on_date(user_key, ds, alt_user_key=alt_user_key):
            return "해당 날짜에 이미 반차가 있어 연차를 등록할 수 없습니다."
    if t == "halfday":
        if already_logged(user_key, "halfday", ds, note_tag=half_period, alt_user_key=alt_user_key):
            tag_txt = "오전" if half_period == "am" else "오후"
            return f"이미 해당 날짜 {tag_txt} 반차 기록이 있습니다."
        if already_logged(user_key, "annual", ds, alt_user_key=alt_user_key):
            return "해당 날짜에 이미 연차가 있어 반차를 등록할 수 없습니다."
    if t == "off" and already_logged(user_key, "off", ds, alt_user_key=alt_user_key):
        return "이미 해당 날짜에 휴무 기록이 있습니다."
    return None


# =========================================================
# 관리자 감사 로그
# ========================================================= 

# --- 관리자 요청 기록 ---
def record_admin_request(admin_key, target_key, action, date_str, note, result, error_msg=""):
    ws = get_ws("admin_requests")
    vals = ws.get_all_values()
    if not vals:
        ws.append_row(
            ["ts_iso","admin_key","target_key","action","date","note","result","error"],
            value_input_option="USER_ENTERED",
        )
    now = dt.datetime.now(KST).isoformat(timespec="seconds")
    row = [now, admin_key, target_key, action, date_str or "", note or "", result, error_msg or ""]
    with_retry(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))

# =========================================================
# /근태 : 사용자 모달
# =========================================================

# ---------- 뷰 생성기 ----------
def build_attendance_view(selected_action: str | None = None, preserved: dict | None = None):
    action_options = [
        {"text": {"type": "plain_text", "text": "연차"}, "value": "annual"},
        {"text": {"type": "plain_text", "text": "반차"}, "value": "halfday"},
        {"text": {"type": "plain_text", "text": "휴무"}, "value": "off"},
    ]

    blocks = [
        {
            "type": "input",
            "block_id": "action_b",
            "dispatch_action": True,
            "label": {"type": "plain_text", "text": "항목"},
            "element": {
                "type": "static_select",
                "action_id": "action",
                "placeholder": {"type": "plain_text", "text": "선택하세요"},
                "options": action_options,
                **(
                    {"initial_option": next((o for o in action_options if o["value"] == selected_action), None)}
                    if selected_action else {}
                ),
            },
        },
        {
            "type": "input",
            "block_id": "date_start_b",
            "label": {"type": "plain_text", "text": "시작일"},
            "element": {"type": "datepicker", "action_id": "date_start"},
        },
        {
            "type": "input",
            "block_id": "date_end_b",
            "optional": True,
            "label": {"type": "plain_text", "text": "종료일 (연차/휴무 기간용)"},
            "element": {"type": "datepicker", "action_id": "date_end"},
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
        blocks.insert(
            3,
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
    
# ---------- /근태: 모달 열기 ----------
@app.command("/근태")
def 근태_modal(ack, body, client):
    ack()
    view = build_attendance_view(
        selected_action=None,
        preserved={"channel_id": body.get("channel_id")},
    )
    client.views_open(trigger_id=body["trigger_id"], view=view)   

# ---------- 선택 변경 시: 모달 업데이트 ----------
@app.block_action("action")
def 근태_action_change(ack, body, client):
    ack()
    selected = body["actions"][0]["selected_option"]["value"]
    meta = json.loads(body["view"].get("private_metadata") or "{}")
    new_view = build_attendance_view(selected_action=selected, preserved=meta)
    client.views_update(view_id=body["view"]["id"], view=new_view)
    
# ---------- 제출 처리 ----------
@app.view("attendance_submit")
def 근태_submit(ack, body, view, client, logger):
    acked = False
    def ack_errors(errors: dict):
        nonlocal acked
        if not acked:
            ack(response_action="errors", errors=errors); acked = True
        else:
            try:
                uid = body["user"]["id"]
                client.chat_postEphemeral(channel=uid, user=uid,
                    text="\n".join(f"{k}: {v}" for k, v in errors.items()))
            except Exception:
                pass
    def ack_ok():
        nonlocal acked
        if not acked:
            ack(); acked = True

    try:
        vals = (view.get("state") or {}).get("values") or {}
        errors = {}

        # 1) 파싱
        action_raw = (vals.get("action_b", {}).get("action", {}) or {}).get("selected_option", {}).get("value")
        action = normalize_action(action_raw)
        def get_date(b,a):
            try: return (vals.get(b, {}).get(a, {}) or {}).get("selected_date")
            except: return None
        date_start = get_date("date_start_b","date_start")
        date_end   = get_date("date_end_b","date_end")
        note = (vals.get("note_b", {}).get("note", {}).get("value") or "").strip()
        half_period = None
        if action == "halfday":
            hp = (vals.get("half_b", {}).get("half_period", {}) or {}).get("selected_option")
            if hp: half_period = hp.get("value")  # "am"/"pm"

        # 2) 형식 검증(폼 에러)
        if not action:
            errors["action_b"] = "항목을 선택하세요."
        if action in ("annual","halfday","off") and not date_start:
            errors["date_start_b"] = "시작일을 선택하세요."
        if action in ("annual","off") and date_start and date_end:
            ds, de = parse_ymd_safe(date_start), parse_ymd_safe(date_end)
            if ds and de and de < ds:
                errors["date_end_b"] = "종료일이 시작일보다 앞일 수 없습니다."
        if action == "halfday" and not half_period:
            errors["half_b"] = "반차는 오전/오후 선택이 필요합니다."
        if errors: return ack_errors(errors)

        # 3) 연도/과거일 정책(폼 에러)
        # (여기서 only 연도/과거일만 체크)
        thisy = today_kst_date().year
        if not ALLOW_FUTURE_YEAR_USER:
            y0 = parse_ymd_safe(date_start).year if date_start else None
            y1 = parse_ymd_safe(date_end).year if (date_end or date_start) else y0
            if action == "halfday":
                if y0 and y0 > thisy:
                    errors["date_start_b"] = f"{thisy}년 이후 날짜는 등록할 수 없습니다."
            elif action in ("annual","off"):
                if y0 and y1:
                    if max(y0,y1) > thisy:
                        errors["date_start_b"] = f"{thisy}년 이후 날짜가 포함되어 있습니다."
                    elif y0 != y1:
                        errors["date_start_b"] = "두 해에 걸친 기간은 나눠서 등록하세요."
        if not ALLOW_BACKDATE_USER:
            if action == "halfday":
                if date_start and is_past_ymd(date_start):
                    errors["date_start_b"] = "지난 날짜에는 반차를 등록할 수 없습니다."
            elif action in ("annual","off"):
                if date_start and not date_end and is_past_ymd(date_start):
                    errors["date_start_b"] = "지난 날짜에는 등록할 수 없습니다."
                elif date_start and date_end and contains_past_ymd(date_start, date_end):
                    errors["date_start_b"] = "지난 날짜가 포함되어 있습니다. 오늘 이후로만 선택하세요."
        # 시트 존재
        missing = [n for n in ("logs","balances","schedule_weekly","holidays") if not sheet_exists(n)]
        if missing:
            errors["action_b"] = "시트가 없습니다: " + ", ".join(missing)
        if errors: return ack_errors(errors)

        # 4) 여기서 즉시 ACK (이후 I/O OK)
        ack_ok()

        # 5) 사용자 정보
        uid  = body["user"]["id"]
        ukey = safe_user_key(client, uid)
        uname= safe_user_name(client, uid)

        saved, failed, skips = [], [], []

        if action == "halfday":
            ds = date_start
            note_final = note + (" (오전)" if half_period=="am" else " (오후)")
            try:
                guard_and_append(ukey, uname, "halfday",
                    note=note_final, date_str=ds, by_user=ukey,
                    note_tag=half_period, alt_user_key=uid)
                saved.append(ds)
            except Exception as e:
                failed.append((ds, human_error(e)))

        elif action == "annual":
            # 잔여·스킵 계산은 ACK 이후 수행 → 초과면 에페메럴로만 안내하고 종료
            try:
                savables, skips = resolve_annual_savables(ukey, date_start, date_end or date_start, alt_user_key=uid)
                need_days = len(savables)
                current_left = update_balance_for_user(ukey, uname)
            except Exception as e:
                client.chat_postEphemeral(channel=uid, user=uid, text="잔여/시트 계산 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
                return

            if need_days > current_left:
                msg = f"선택한 기간 중 실제 기록 대상 {need_days}일이 잔여 {current_left:g}일을 초과하여 등록되지 않았습니다."
                if skips:
                    msg += f"\n*스킵됨(사유)*\n" + "\n".join(f"- {d} : {m}" for d, m in skips)
                client.chat_postEphemeral(channel=uid, user=uid, text=msg)
                return

            # 기록
            for ds in savables:
                try:
                    guard_and_append(ukey, uname, "annual",
                        note=note, date_str=ds, by_user=ukey, alt_user_key=uid)
                    saved.append(ds)
                except Exception as e:
                    failed.append((ds, human_error(e)))

        elif action == "off":
            for ds in iter_dates(date_start, date_end or date_start):
                try:
                    guard_and_append(ukey, uname, "off",
                        note=note, date_str=ds, by_user=ukey, alt_user_key=uid)
                    saved.append(ds)
                except Exception as e:
                    failed.append((ds, human_error(e)))
        else:
            ds = date_start or today_kst_ymd()
            try:
                guard_and_append(ukey, uname, action,
                    note=note, date_str=ds, by_user=ukey, alt_user_key=uid)
                saved.append(ds)
            except Exception as e:
                failed.append((ds, human_error(e)))

        # 6) 결과 에페메럴
        try:
            ch = (json.loads(view.get("private_metadata") or "{}").get("channel_id")) or uid
            def section(title, items):
                if not items: return ""
                if isinstance(items[0], tuple):
                    body = "\n".join(f"- {d} : {m}" for d, m in items)
                else:
                    body = "\n".join(f"- {d}" for d in items)
                return f"\n*{title}*\n{body}"
            title = {"annual":"연차 등록 결과","halfday":"반차 등록 결과","off":"휴무 등록 결과"}.get(action,"처리 결과")
            period = f" ({date_start}" + (f" ~ {date_end}" if date_end and date_end!=date_start else "") + ")"
            msg = f"*{title}*{period}" + section("저장됨", saved) + (section("스킵됨(사유)", skips) if action=="annual" else "") + section("실패함(오류)", failed)
            client.chat_postEphemeral(channel=ch, user=uid, text=msg)
        except Exception:
            pass

    except Exception as e:
        logger.exception("attendance_submit fatal")
        try: ack_ok()
        except Exception: pass
        try:
            uid = body["user"]["id"]
            client.chat_postMessage(channel=uid, text=human_error(e))
        except Exception:
            pass


# =========================================================
# /근태관리 : 관리자 대리입력 모달
# =========================================================

# --- 모달 뷰 빌더(관리자용) ---
def build_admin_view(selected_action: str | None = None, preserved: dict | None = None):
    action_options = [
        {"text": {"type": "plain_text", "text": "출근"}, "value": "checkin"},
        {"text": {"type": "plain_text", "text": "퇴근"}, "value": "checkout"},
        {"text": {"type": "plain_text", "text": "연차"}, "value": "annual"},
        {"text": {"type": "plain_text", "text": "반차"}, "value": "halfday"},
        {"text": {"type": "plain_text", "text": "휴무"}, "value": "off"},
    ]

    blocks = [
        {
            "type": "input",
            "block_id": "target_b",
            "label": {"type": "plain_text", "text": "대상자"},
            "element": {"type": "users_select", "action_id": "target"},
        },
        {
            "type": "input",
            "block_id": "action_b",
            "dispatch_action": True,
            "label": {"type": "plain_text", "text": "항목"},
            "element": {
                "type": "static_select",
                "action_id": "action",
                "placeholder": {"type": "plain_text", "text": "선택하세요"},
                "options": action_options,
                **(
                    {"initial_option": next((o for o in action_options if o["value"] == selected_action), None)}
                    if selected_action else {}
                ),
            },
        },
        {
            "type": "input",
            "block_id": "date_start_b",
            "label": {"type": "plain_text", "text": "시작일"},
            "element": {"type": "datepicker", "action_id": "date_start"},
        },
        {
            "type": "input",
            "block_id": "date_end_b",
            "optional": True,
            "label": {"type": "plain_text", "text": "종료일 (연차/휴무/기간 처리용)"},
            "element": {"type": "datepicker", "action_id": "date_end"},
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
        blocks.insert(
            3,
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
        "callback_id": "admin_attendance_submit",
        "title": {"type": "plain_text", "text": "관리자 근태 입력"},
        "submit": {"type": "plain_text", "text": "저장"},
        "close": {"type": "plain_text", "text": "취소"},
        "private_metadata": json.dumps(preserved or {}),
        "blocks": blocks,
    }

# --- 커맨드: /근태관리 ---
# 호출부
@app.command("/근태관리")
def admin_modal(ack, body, client):
    ack()
    client.views_open(
        trigger_id=body["trigger_id"],
        view={
            "type":"modal", "callback_id":"admin_attendance_submit",
            "private_metadata": json.dumps({"channel_id": body.get("channel_id")}),
            "title":{"type":"plain_text","text":"관리자 근태 등록"},
            "submit":{"type":"plain_text","text":"저장"},
            "close":{"type":"plain_text","text":"취소"},
            "blocks":[
                # 대상자 선택 (users_select)
                {"type":"input","block_id":"u_b","label":{"type":"plain_text","text":"대상자"},
                 "element":{"type":"users_select","action_id":"u"}},
                # 액션
                {"type":"input","block_id":"action_b","label":{"type":"plain_text","text":"항목"},
                 "element":{"type":"static_select","action_id":"action","options":[
                    {"text":{"type":"plain_text","text":"연차"},"value":"annual"},
                    {"text":{"type":"plain_text","text":"반차"},"value":"halfday"},
                    {"text":{"type":"plain_text","text":"휴무"},"value":"off"},
                 ]}},
                # 반차 오전/오후
                {"type":"input","block_id":"half_b","optional":True,"label":{"type":"plain_text","text":"반차 구분"},
                 "element":{"type":"radio_buttons","action_id":"half_period","options":[
                   {"text":{"type":"plain_text","text":"오전"},"value":"am"},
                   {"text":{"type":"plain_text","text":"오후"},"value":"pm"},
                 ]}},
                # 기간
                {"type":"input","block_id":"date_start_b","label":{"type":"plain_text","text":"시작일"},
                 "element":{"type":"datepicker","action_id":"date_start"}},
                {"type":"input","block_id":"date_end_b","optional":True,"label":{"type":"plain_text","text":"종료일"},
                 "element":{"type":"datepicker","action_id":"date_end"}},
                # 주말 포함 옵션(결정 2번 참조)
                {"type":"input","block_id":"opt_biz","optional":True,"label":{"type":"plain_text","text":"옵션"},
                 "element":{"type":"checkboxes","action_id":"biz_opt","options":[
                   {"text":{"type":"plain_text","text":"주말/공휴일 포함"},"value":"include_weekends"}
                 ]}},
                {"type":"input","block_id":"note_b","optional":True,"label":{"type":"plain_text","text":"메모"},
                 "element":{"type":"plain_text_input","action_id":"note"}}
            ]}
    )
    
@app.block_action("action")
def admin_action_change(ack, body, client):
    # /근태와 /근태관리 둘 다 이 핸들러를 쓰므로 callback_id로 분기
    ack()
    selected = body["actions"][0]["selected_option"]["value"]
    view = body["view"]
    meta = json.loads(view.get("private_metadata") or "{}")
    if view.get("callback_id") == "admin_attendance_submit":
        new_view = build_admin_view(selected_action=selected, preserved=meta)
    else:
        new_view = build_attendance_view(selected_action=selected, preserved=meta)
    client.views_update(view_id=view["id"], view=new_view)
    
# --- 제출: admin_attendance_submit ---
@app.view("admin_attendance_submit")
def admin_submit(ack, body, view, client, logger):
    acked = False
    def ack_errors(errors):
        nonlocal acked
        if not acked: ack(response_action="errors", errors=errors); acked=True

    # 1) 파싱
    vals = (view.get("state") or {}).get("values") or {}
    get = lambda b,a: (vals.get(b, {}).get(a, {}) or {})
    action = normalize_action(get("action_b","action").get("selected_option",{}).get("value"))
    target_uid = get("u_b","u").get("selected_user")
    date_start = get("date_start_b","date_start").get("selected_date")
    date_end   = get("date_end_b","date_end").get("selected_date")
    note = (get("note_b","note").get("value") or "").strip()
    half_period = None
    if action=="halfday":
        hp = get("half_b","half_period").get("selected_option")
        if hp: half_period = hp.get("value")  # "am"/"pm"
    include_weekends = any(o.get("value")=="include_weekends"
                           for o in (get("opt_biz","biz_opt").get("selected_options") or []))

    # 2) 형식 검증
    errors={}
    if not target_uid: errors["u_b"]="대상자를 선택하세요."
    if not action: errors["action_b"]="항목을 선택하세요."
    if action in ("annual","halfday","off") and not date_start:
        errors["date_start_b"]="시작일을 선택하세요."
    if action in ("annual","off") and date_start and date_end:
        ds,de = parse_ymd_safe(date_start), parse_ymd_safe(date_end)
        if ds and de and de<ds: errors["date_end_b"]="종료일이 시작일보다 앞일 수 없습니다."
    if action=="halfday" and not half_period:
        errors["half_b"]="반차는 오전/오후 선택이 필요합니다."
    if errors: return ack_errors(errors)

    # 3) 연도/과거일 정책(관리자 플래그 적용)
    def _safe_date(s): return parse_ymd_safe(s) if s else None
    def _safe_year(s): d=_safe_date(s); return d.year if d else None
    thisy=today_kst_date().year
    y0=_safe_year(date_start); y1=_safe_year(date_end) or y0
    if not ALLOW_FUTURE_YEAR_ADMIN:
        if action=="halfday" and y0 and y0>thisy: errors["date_start_b"]=f"{thisy}년 이후 날짜는 등록할 수 없습니다."
        elif action in ("annual","off") and y0 and y1:
            if max(y0,y1)>thisy: errors["date_start_b"]=f"{thisy}년 이후 날짜가 포함되어 있습니다."
            elif y0!=y1: errors["date_start_b"]="두 해에 걸친 기간은 나눠서 등록하세요."
    if not ALLOW_BACKDATE_ADMIN:
        d0=_safe_date(date_start); d1=_safe_date(date_end) or d0; today=today_kst_date()
        if action=="halfday" and d0 and d0<today: errors["date_start_b"]="지난 날짜에는 반차를 등록할 수 없습니다."
        elif action in ("annual","off") and d0 and d1:
            if d0==d1 and d0<today: errors["date_start_b"]="지난 날짜에는 등록할 수 없습니다."
            elif d1<today or (d0<today<=d1): errors["date_start_b"]="지난 날짜가 포함되어 있습니다."
    # 시트 존재
    miss=[n for n in ("logs","balances","schedule_weekly","holidays") if not sheet_exists(n)]
    if miss: errors["action_b"]="시트가 없습니다: "+", ".join(miss)
    if errors: return ack_errors(errors)

    # 4) 여기서 ACK (이후 I/O)
    ack()

    # 5) 대상자 키/이름
    target_key = safe_user_key(client, target_uid)
    target_name= safe_user_name(client, target_uid)
    admin_uid  = body["user"]["id"]
    admin_key  = safe_user_key(client, admin_uid)

    saved, failed, skips = [], [], []

    try:
        if action=="halfday":
            ds=date_start
            note_final = note + (" (오전)" if half_period=="am" else " (오후)")
            guard_and_append(target_key, target_name, "halfday",
                             note=note_final, date_str=ds, by_user=admin_key,
                             note_tag=half_period, alt_user_key=target_uid, is_admin=True)
            saved.append(ds)

        elif action=="annual":
            if not date_end: date_end=date_start
            # 주말 포함 옵션 반영
            if include_weekends:
                dates = list(iter_dates(date_start, date_end))
            else:
                # 기존 정책 유지: 사업일만
                # + 반차 충돌/중복/연차중복 체크는 guard에서 처리되며, 실패는 failed에 기록
                dates = [d for d in iter_dates(date_start, date_end) if is_business_day(parse_ymd_safe(d))]

            for ds in dates:
                try:
                    # 상호배타/중복/별칭 규칙 동일 + 관리자 플래그
                    guard_and_append(target_key, target_name, "annual",
                                     note=note, date_str=ds, by_user=admin_key,
                                     alt_user_key=target_uid, is_admin=True)
                    saved.append(ds)
                except Exception as e:
                    failed.append((ds, human_error(e)))

        elif action=="off":
            for ds in iter_dates(date_start, date_end or date_start):
                try:
                    guard_and_append(target_key, target_name, "off",
                                     note=note, date_str=ds, by_user=admin_key,
                                     alt_user_key=target_uid, is_admin=True)
                    saved.append(ds)
                except Exception as e:
                    failed.append((ds, human_error(e)))
    finally:
        # 결과 에페메럴
        ch = (json.loads(view.get("private_metadata") or "{}").get("channel_id")) or admin_uid
        def section(title, items):
            if not items: return ""
            if isinstance(items[0], tuple): body="\n".join(f"- {d} : {m}" for d,m in items)
            else: body="\n".join(f"- {d}" for d in items)
            return f"\n*{title}*\n{body}"
        client.chat_postEphemeral(
            channel=ch, user=admin_uid,
            text=(f"*관리자 {('연차' if action=='annual' else '반차' if action=='halfday' else '휴무')} 등록 결과* "
                  f"({date_start}" + (f" ~ {date_end}" if date_end and date_end!=date_start else "") + ")\n"
                  + section("저장됨", saved) + section("실패함(오류)", failed))
        )

# =========================================================
# balances / 잔여 계산 유틸
# ---------------------------------------------------------
# 시트 스키마 (balances):
# A:user_key  B:user_name  C:annual_total  D:annual_used
# E:annual_left  F:half_used  G:override_left
# H:override_from  I:last_admin_update  J:notes
# =========================================================

# --- 안전한 실수 변환 ---
def to_float(v, default=0.0):
    try:
        s = str(v).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default

def logs_usage_since(user_key: str, since_date: dt.date | None = None, year: int | None = None):
    ws = get_ws("logs")
    vals = ws.get_all_values()
    if not vals:
        return 0.0, 0.0

    head = [h.strip().lower() for h in vals[0]]
    idx = {h: i for i, h in enumerate(head)}
    iu = idx.get("user_key") or idx.get("user_id")
    it = idx.get("type")
    idate = idx.get("date")
    if iu is None or it is None or idate is None:
        return 0.0, 0.0

    target = (user_key or "").strip().lower()
    dates_annual = set()
    half_count = {}

    def ok_date(d: dt.date) -> bool:
        if since_date and d < since_date:
            return False
        if year and d.year != year:
            return False
        # 평일/휴일 필터 (원한다면 사업일만 차감)
        return is_business_day(d)

    # 1차 스캔: 연차 날짜 수집
    for r in vals[1:]:
        if iu >= len(r) or it >= len(r) or idate >= len(r):
            continue
        uk = (r[iu] or "").strip().lower()
        if uk != target:
            continue
        t = (r[it] or "").strip().lower()
        d = parse_ymd_safe((r[idate] or "").strip())
        if not d or not ok_date(d):
            continue
        if t == "annual":
            dates_annual.add(d.isoformat())

    # 2차 스캔: 반차 카운트 (단, 같은 날 연차 있으면 무시)
    for r in vals[1:]:
        if iu >= len(r) or it >= len(r) or idate >= len(r):
            continue
        uk = (r[iu] or "").strip().lower()
        if uk != target:
            continue
        t = (r[it] or "").strip().lower()
        d = parse_ymd_safe((r[idate] or "").strip())
        if not d or not ok_date(d):
            continue
        if t == "halfday" and d.isoformat() not in dates_annual:
            half_count[d.isoformat()] = half_count.get(d.isoformat(), 0) + 1

    annual_used = float(len(dates_annual))
    # 같은 날 반차 2개라도 1.0까지만 인정
    half_used = 0.0
    for _, c in half_count.items():
        half_used += min(1.0, 0.5 * c)

    return annual_used, half_used

def get_or_create_balance_row(ukey: str, uname: str):
    """
    balances에서 user_key 행을 찾고 없으면 생성.
    (rownum, row_values, head_list, col_index_map) 반환.
    """
    ws = get_ws("balances")
    vals = ws.get_all_values()

    # 헤더 없으면 생성
    if not vals:
        ws.append_row(
            ["user_key","user_name","annual_total","annual_used","annual_left",
             "half_used","override_left","override_from","last_admin_update","notes"],
            value_input_option="USER_ENTERED",
        )
        vals = ws.get_all_values()

    head = [h.strip().lower() for h in vals[0]]
    col = {h: i for i, h in enumerate(head)}

    # 필수 컬럼 없으면 그대로 둔다 (호출 전에 시트 맞춰둘 것)
    if "user_key" not in col or "user_name" not in col:
        raise RuntimeError("balances 시트 헤더(user_key,user_name)가 올바르지 않습니다.")

    target = (ukey or "").strip().lower()
    rownum = None
    row = None

    for rn, r in enumerate(vals[1:], start=2):
        key = (r[col["user_key"]] if col["user_key"] < len(r) else "").strip().lower()
        if key == target:
            rownum = rn
            row = r
            break

    if rownum is None:
        # 새 행 스켈레톤
        rownum = len(vals) + 1
        new_row = [""] * len(head)
        new_row[col["user_key"]] = ukey
        new_row[col["user_name"]] = uname
        ws.append_row(new_row, value_input_option="USER_ENTERED")
        vals = ws.get_all_values()
        row = vals[rownum - 1]

    return ws, rownum, row, head, col


def update_balance_for_user(ukey: str, uname: str) -> float:
    """
    1) balances에 해당 user_key 행 생성 또는 로드
    2) override_left가 있으면:
         override_from 이후 logs 사용량만 차감
       없으면:
         annual_total 기준, 해당 연도 logs 사용량 차감
    3) annual_used, half_used, annual_left, last_admin_update 업데이트
    4) 최종 잔여일수 반환
    """
    ws, rownum, row, head, col = get_or_create_balance_row(ukey, uname)

    def get_cell(name: str, default: str = ""):
        i = col.get(name)
        if i is None:
            return default
        return (row[i] if i < len(row) and row[i] != "" else default)

    now = dt.datetime.now(KST)
    now_iso = now.isoformat(timespec="seconds")

    # 관리자 기준선 우선
    o_left_raw = get_cell("override_left", "")
    o_from_raw = get_cell("override_from", "")

    updates = {
        "user_key": ukey,
        "user_name": uname,
        "last_admin_update": now_iso,
    }

    if str(o_left_raw).strip() != "":
        base = to_float(o_left_raw, 0.0)
        since = parse_ymd_safe(str(o_from_raw)) if o_from_raw else None
        au, hu = logs_usage_since(ukey, since_date=since)
        left = max(0.0, base - (au + hu))
        updates["annual_used"] = f"{au:.1f}"
        updates["half_used"] = f"{hu:.1f}"
        updates["annual_left"] = f"{left:.1f}"
        eff_left = left
    else:
        # annual_total 기반
        total = to_float(get_cell("annual_total", "0"), 0.0)
        year = now.year
        au, hu = logs_usage_since(ukey, year=year)
        left = max(0.0, total - (au + hu))
        updates["annual_used"] = f"{au:.1f}"
        updates["half_used"] = f"{hu:.1f}"
        updates["annual_left"] = f"{left:.1f}"
        eff_left = left
        
    
    # 시트 업데이트 (필요 컬럼만)
    def cell_range(col_index: int) -> str:
        # 0-based index -> A,B,C...
        return f"{chr(ord('A') + col_index)}{rownum}:{chr(ord('A') + col_index)}{rownum}"

    def do_updates():
        data = []
        for name, val in updates.items():
            i = col.get(name)
            if i is None:
                continue
            data.append({
                "range": cell_range(i),
                "values": [[val]],
            })
        if data:
            ws.batch_update(data, value_input_option="USER_ENTERED")

    with_retry(do_updates)

    return eff_left

# =========================================================
# /잔여 : 사용자 잔여 조회 + balances 동기화
# =========================================================

# ---------- /잔여 커맨드 ----------
@app.command("/잔여")
def 잔여_cmd(ack, body, respond, client):
    ack()
    try:
        uid = body["user_id"]
        ukey = safe_user_key(client, uid)
        uname = safe_user_name(client, uid)

        # balances 갱신(annual_used/half_used/annual_left 갱신됨)
        left = update_balance_for_user(ukey, uname)

        ws = get_ws("balances")
        vals = ws.get_all_values()
        head = [h.strip().lower() for h in vals[0]] if vals else []
        col = {h: i for i, h in enumerate(head)}

        # 대상 행 찾기
        row = None
        for r in vals[1:]:
            if col.get("user_key", 0) < len(r) and (r[col["user_key"]] or "").strip().lower() == ukey.lower():
                row = r
                break

        # 안전 추출 유틸
        def getc(name, default=""):
            i = col.get(name)
            if i is None or row is None or i >= len(row):
                return default
            return (row[i] or "").strip()

        def f1(x):
            try:
                return f"{float(x):.1f}"
            except Exception:
                return x if x != "" else "-"

        # 값 추출
        annual_total = getc("annual_total", "")
        annual_used  = getc("annual_used", "")
        half_used    = getc("half_used", "")
        annual_left  = getc("annual_left", "")  # update_balance_for_user가 방금 쓴 값
        override_left = getc("override_left", "")
        override_from = getc("override_from", "")

        # 기준 설명
        if override_left != "":
            basis = f"(관리자 기준선 {override_left}일" + (f", 기준일 {override_from}" if override_from else "") + ")"
        elif annual_total != "":
            basis = f"(연차 총 {annual_total}일)"
        else:
            basis = ""

        # 출력
        msg = (
            f"*{uname} 님 잔여 요약*\n"
            f"• 총 연차: {f1(annual_total)}일\n"
            f"• 사용한 연차: {f1(annual_used)}일\n"
            f"• 사용한 반차: {f1(half_used)}일\n"
            f"• 남은 연차: {f1(annual_left if annual_left != '' else left)}일"
            + (f"\n_{basis}_" if basis else "")
        )
        respond(msg)

    except Exception as e:
        respond(f"잔여 계산 오류: {human_error(e)}")

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

# --- 특정 사용자 잔여일수 계산 ---
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

# --- 날짜 문자열을 KST 요일 컬럼명으로 변환 ---
def weekday_col_kst(date_str: str) -> str:
    y, m, d = map(int, date_str.split("-"))
    day = dt.datetime(y, m, d, tzinfo=KST).date()
    return DOW_COLS[day.weekday()]  # Monday=0

# --- 주간 스케줄에 출근 기록 업서트 ---
def upsert_weekly_schedule_checkin(user_key: str, date_str: str):
    ws = get_ws("schedule_weekly")
    vals = ws.get_all_values()
    if not vals:
        ws.append_row(
            ["week","user_key","Mon","Tue","Wed","Thu","Fri","Sat","Sun"],
            value_input_option="USER_ENTERED",
        )
        vals = ws.get_all_values()

    header = [h.strip() for h in vals[0]]
    # 소문자 맵으로 인덱스 관리
    col = {h.lower(): i for i, h in enumerate(header)}

    week = date_to_iso_week_kst(date_str)
    dow = weekday_col_kst(date_str)  # "Mon".."Sun"
    dow_idx = col.get(dow.lower())
    week_idx = col.get("week")
    user_idx = col.get("user_key")
    if dow_idx is None or week_idx is None or user_idx is None:
        return  # 헤더 틀리면 아무것도 안 함

    key_norm = (user_key or "").strip().lower()
    rownum = None
    row = None

    # (week, user_key) 행 찾기
    for rn, r in enumerate(vals[1:], start=2):
        w = (r[week_idx] if week_idx < len(r) else "").strip()
        uk = (r[user_idx] if user_idx < len(r) else "").strip().lower()
        if w == week and uk == key_norm:
            rownum = rn
            row = r
            break

    # 없으면 새 행
    if rownum is None:
        rownum = len(vals) + 1
        row = [""] * len(header)
        row[week_idx] = week
        row[user_idx] = user_key
        ws.append_row(row, value_input_option="USER_ENTERED")
        # append 후 최신값 다시 로드
        vals = ws.get_all_values()
        row = vals[rownum - 1]

    # 해당 요일 셀이 비어있을 때만 '출근' 기록
    cur = row[dow_idx] if dow_idx < len(row) else ""
    if not str(cur).strip():
        col_letter = chr(ord("A") + dow_idx)
        ws.update(
            range_name=f"{col_letter}{rownum}:{col_letter}{rownum}",
            values=[["출근"]],
            value_input_option="USER_ENTERED",
        )


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

# ---------- 에러 핸들러 데코레이터 : 슬래시 커맨드에서 예외 발생 시 통일 메시지. ----------
def slash_guard(fn):
    def _w(ack, body, respond, *args, **kwargs):
        try:
            return fn(ack, body, respond, *args, **kwargs)
        except Exception as e:
            reply_error(respond, f"처리 중 오류: {e}")
    return _w

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
    rows = [
        ("월", r.get("Mon", "-") or "-"),
            ("화", r.get("Tue", "-") or "-"),
            ("수", r.get("Wed", "-") or "-"),
            ("목", r.get("Thu", "-") or "-"),
            ("금", r.get("Fri", "-") or "-"),
            ("토", r.get("Sat", "-") or "-"),
            ("일", r.get("Sun", "-") or "-")
            ]

    # 열 너비 계산 (가독성 정렬)
    w_day = max(2, max(disp_width(k) for k, _ in rows))
    w_val = max(2, max(disp_width(v) for _, v in rows))

    lines = [f"{pad_right(k, w_day)} - {pad_right(v, w_val)}" for k, v in rows]
    body = "\n".join(lines)
    return f"*{week} 주간 스케줄*\n```{body}```"

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

# --- logs 시트에서 사용자 연차/반차 사용량 집계 ---
def calc_usage_from_logs(user_key: str, *, since: dt.date | None = None, year: int | None = None):
    """logs에서 annual/halfday 사용량 합산."""
    vals = logs.get_all_values()
    if not vals:
        return 0.0, 0.0

    head = [h.strip().lower() for h in vals[0]]
    idx = {h: i for i, h in enumerate(head)}
    iu = idx.get("user_key") or idx.get("user_id")
    it = idx.get("type")
    idate = idx.get("date")
    if iu is None or it is None or idate is None:
        return 0.0, 0.0

    target = (user_key or "").strip().lower()
    annual_used = 0.0
    half_used = 0.0

    for r in vals[1:]:
        if iu >= len(r) or it >= len(r) or idate >= len(r):
            continue
        uk = (r[iu] or "").strip().lower()
        if uk != target:
            continue

        t = (r[it] or "").strip().lower()
        d = parse_ymd_safe((r[idate] or "").strip())
        if not d:
            continue

        if since and d < since:
            continue
        if year and d.year != year:
            continue

        if t == "annual":
            annual_used += 1.0
        elif t == "halfday":
            half_used += 0.5

    return annual_used, half_used

# ---------- 이벤트 핸들러 등록 ----------
@app.event("app_home_opened")
def _home_noop(event, logger):
    logger.debug(f"home opened by {event.get('user')}")
  
# ---------- 멘션 및 키워드 처리 ----------
@app.event("app_mention")
def on_mention(body, say):
    say("`/근태` 또는 `출근`/`퇴근` 키워드를 사용하세요.")
    
# ---------- /출근, /퇴근 커맨드 ----------
@app.command("/출근")
def 출근_cmd(ack, body, respond, client):
    ack()
    uid = body["user_id"]
    ukey = safe_user_key(client, uid)
    uname = safe_user_name(client, uid)
    ds = today_kst_ymd()
    try:
        # 중복 / 재시도 / 로그 기록까지 포함
        guard_and_append(ukey, uname, "checkin", date_str=ds, by_user=ukey, alt_user_key=uid)
        # 주간 스케줄 자동 반영 쓰는 경우만
        try:
            upsert_weekly_schedule_checkin(ukey, ds)
        except Exception:
            pass
        respond(f"{uname} 출근 등록 완료")
    except Exception as e:
        respond(human_error(e))


# ---------- /퇴근 커맨드 ----------
@app.command("/퇴근")
def 퇴근_cmd(ack, body, respond, client):
    ack()
    uid = body["user_id"]
    ukey = safe_user_key(client, uid)
    uname = safe_user_name(client, uid)
    ds = today_kst_ymd()
    try:
        guard_and_append(ukey, uname, "checkout", date_str=ds, by_user=ukey, alt_user_key=uid)
        respond(f"{uname} 퇴근 등록 완료")
    except Exception as e:
        respond(human_error(e))

# ---------- 출근/퇴근 키워드 핸들러 ----------
@app.message(re.compile(r"^\s*(출근|퇴근)\s*$"))
def on_keyword(message, say, context):
    t = "출근" if context["matches"][0] == "출근" else "퇴근"
    append_log(message["user"], "", t)
    say(f"{context['matches'][0]} 등록 완료")

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

# ---------- /잔여갱신 커맨드 + 모달 제출 핸들러 ----------
@app.command("/잔여갱신")
def 잔여갱신_modal(ack, body, client, respond):
    ack()
    if not is_admin(body["user_id"], client):
        respond("권한 없음.")
        return
    today = dt.datetime.now(KST).date().isoformat()
    client.views_open(trigger_id=body["trigger_id"],
                      view=build_override_view(initial_left="", initial_date=today))

# --- 잔여일수 재정의 모달 제출 핸들러 ---    
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

# ---------- /잔여debug 커맨드 (개발용) ----------
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

@app.event({"type": "message", "subtype": "channel_join"})
def on_join(body, client):
    ev = body["event"]
    user = ev.get("user")
    channel = ev.get("channel")
    if user and channel:
        client.chat_postMessage(
            channel=channel,
            text=f"<@{user}> 님 환영합니다. `/출근`, `/퇴근`, `/근태`를 사용해보세요."
        )

def load_holidays() -> set[str]:
    """holidays 시트 1열에 YYYY-MM-DD가 있다고 가정."""
    global HOLIDAYS_CACHE
    if HOLIDAYS_CACHE is not None:
        return HOLIDAYS_CACHE
    try:
        ws = get_ws("holidays")
        vals = ws.get_all_values()
        s = set()
        for r in vals[1:] if vals and vals[0] else vals:
            if not r:
                continue
            d = (r[0] or "").strip()
            if parse_ymd_safe(d):
                s.add(d)
        HOLIDAYS_CACHE = s
        return s
    except Exception:
        HOLIDAYS_CACHE = set()
        return HOLIDAYS_CACHE

def is_weekend(d: dt.date) -> bool:
    # 월=0 ... 일=6
    return d.weekday() >= 5

def is_holiday(d: dt.date) -> bool:
    return d.isoformat() in load_holidays()

def is_business_day(d: dt.date) -> bool:
    return (not is_weekend(d)) and (not is_holiday(d))

def iter_business_dates(start_s: str, end_s: str):
    for s in iter_dates(start_s, end_s):
        d = parse_ymd_safe(s)
        if d and is_business_day(d):
            yield s

def logs_usage_since(user_key: str, since_date: dt.date | None = None, year: int | None = None):
    """
    annual: 1.0, halfday: 0.5
    단, 주말/공휴일 기록은 차감하지 않음.
    """
    ws = get_ws("logs")
    vals = ws.get_all_values()
    if not vals:
        return 0.0, 0.0

    head = [h.strip().lower() for h in vals[0]]
    idx = {h: i for i, h in enumerate(head)}
    iu = idx.get("user_key") or idx.get("user_id")
    it = idx.get("type")
    idate = idx.get("date")
    if iu is None or it is None or idate is None:
        return 0.0, 0.0

    target = (user_key or "").strip().lower()
    annual_used = 0.0
    half_used = 0.5 * 0  # 명시

    for r in vals[1:]:
        if iu >= len(r) or it >= len(r) or idate >= len(r):
            continue

        uk = (r[iu] or "").strip().lower()
        if uk != target:
            continue

        t = (r[it] or "").strip().lower()
        d = parse_ymd_safe((r[idate] or "").strip())
        if not d:
            continue

        # 범위/연도 필터
        if since_date and d < since_date:
            continue
        if year and d.year != year:
            continue

        # 주말/공휴일은 차감 제외
        if not is_business_day(d):
            continue

        if t == "annual":
            annual_used += 1.0
        elif t == "halfday":
            half_used += 0.5

    return annual_used, half_used

def any_halfday_on_date(user_key: str, date_str: str, alt_user_key: str | None = None) -> bool:
    ws = get_ws("logs")
    vals = ws.get_all_values()
    if not vals:
        return False
    head = [h.strip().lower() for h in vals[0]]
    idx = {h: i for i, h in enumerate(head)}
    iu = idx.get("user_key") or idx.get("user_id")
    it = idx.get("type")
    idate = idx.get("date")
    if iu is None or it is None or idate is None:
        return False

    uk_primary = (user_key or "").strip().lower()
    uk_alt = (alt_user_key or "").strip().lower()
    want_date = (date_str or "").strip()

    def same_user(v):
        v = (v or "").strip().lower()
        return v == uk_primary or (uk_alt and v == uk_alt)

    for r in vals[1:]:
        if iu >= len(r) or it >= len(r) or idate >= len(r):
            continue
        if not same_user(r[iu]):
            continue
        if (r[it] or "").strip().lower() != "halfday":
            continue
        if (r[idate] or "").strip() != want_date:
            continue
        return True
    return False

def count_halfday_on_date(user_key: str, date_str: str, alt_user_key: str | None = None) -> int:
    ws = get_ws("logs")
    vals = ws.get_all_values()
    if not vals:
        return 0
    head = [h.strip().lower() for h in vals[0]]
    idx = {h: i for i, h in enumerate(head)}
    iu = idx.get("user_key") or idx.get("user_id")
    it = idx.get("type")
    idate = idx.get("date")
    if iu is None or it is None or idate is None:
        return 0

    uk_primary = (user_key or "").strip().lower()
    uk_alt = (alt_user_key or "").strip().lower()
    want_date = (date_str or "").strip()

    def same_user(v):
        v = (v or "").strip().lower()
        return v == uk_primary or (uk_alt and v == uk_alt)

    c = 0
    for r in vals[1:]:
        if iu >= len(r) or it >= len(r) or idate >= len(r):
            continue
        if not same_user(r[iu]):
            continue
        if (r[it] or "").strip().lower() != "halfday":
            continue
        if (r[idate] or "").strip() != want_date:
            continue
        c += 1
    return c

def explain_skip_for_annual(user_key: str, ds: str, *, alt_user_key: str | None = None) -> str | None:
    """
    annual 제출 시, 해당 날짜 ds가 왜 스킵되는지 사유 문자열을 반환.
    사유가 없으면 None (즉, 저장 가능).
    """
    d = parse_ymd_safe(ds)
    if not d:
        return "유효하지 않은 날짜 형식입니다."

    # 주말/공휴일 정책: 평일만 기록한다면 아래 두 줄 유지
    if not is_business_day(d):
        return "주말/공휴일은 연차 기록 대상이 아닙니다."

    # 상호배타: 해당 날짜에 반차가 하나라도 있으면 연차 금지
    if any_halfday_on_date(user_key, ds, alt_user_key=alt_user_key):
        return "해당 날짜에 반차가 있어 연차를 등록할 수 없습니다."

    # 중복: 같은 날짜 연차 이미 있음
    if already_logged(user_key, "annual", ds, alt_user_key=alt_user_key):
        return "이미 해당 날짜에 연차 기록이 있습니다."

    return None  # 저장 가능

# --- 연차 기간 제출 시, 저장 가능한 날짜들과 스킵(사유) 목록 반환 ---
def resolve_annual_savables(user_key: str, start_s: str, end_s: str, *, alt_user_key: str | None = None):
    """
    연차 기간 제출 시, 저장 가능한 날짜들과 스킵(사유) 목록을 돌려준다.
    반환: (savable_dates: list[str], skips: list[tuple[str, str]])
    """
    dates_all = list(iter_dates(start_s, end_s))
    savable, skips = [], []
    for ds in dates_all:
        reason = explain_skip_for_annual(user_key, ds, alt_user_key=alt_user_key)
        if reason:
            skips.append((ds, reason))
        else:
            savable.append(ds)
    return savable, skips

def today_kst_date() -> dt.date:
    return dt.datetime.now(KST).date()

def is_past_ymd(s: str) -> bool:
    d = parse_ymd_safe(s)
    if not d:
        return False
    return d < today_kst_date()

def contains_past_ymd(start_s: str, end_s: str) -> bool:
    ds = parse_ymd_safe(start_s); de = parse_ymd_safe(end_s)
    if not ds or not de:
        return False
    today = today_kst_date()
    # 전부 과거거나, 일부라도 과거 포함
    return de < today or ds < today <= de or ds < today and de >= ds


if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
