import os, re, pytz, datetime as dt
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import gspread
from google.oauth2.service_account import Credentials
import json

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(os.environ["SHEET_ID"])
logs = sh.worksheet("logs")
app = App(token=os.environ["SLACK_BOT_TOKEN"])
KST = pytz.timezone("Asia/Seoul")

user_cache = {}

def resolve_user_name(client, user_id):
    try:
        info = client.users_info(user=user_id)
        p = info["user"]["profile"]
        return p.get("display_name") or p.get("real_name") or user_id
    except Exception:
        return user_id
    
def append_log(user_id, user_name, type_, note="", date_str="", by_user=None):
    now = dt.datetime.now(KST).isoformat(timespec="seconds")
    type_ = (type_ or "").strip()
    note = (note or "").strip()
    date_str = (date_str or "").strip()
    # 휴무/연차는 YYYY-MM-DD 필요. 없으면 오늘 날짜로 보정
    if type_ in ("annual","off") and not date_str:
        date_str = dt.datetime.now(KST).date().isoformat()
        
    row = [now, user_id, user_name, type_, note, date_str, "auto", by_user or user_id]
    
    # A1:H1을 표 헤더 범위로 고정 -> 항상 A열부터 append
    logs.append_rows([row], value_input_option="USER_ENTERED", table_range="A1:H1")

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
  
@app.event("app_mention")
def on_mention(body, say):
    say("`/근태` 또는 `출근`/`퇴근` 키워드를 사용하세요.")
    
@app.command("/출근")
def 출근_cmd(ack, body, respond, client):
    ack()
    uid = body["user_id"]
    uname = resolve_user_name(client, uid)
    append_log(uid, uname, "출근")  # user_name 컬럼 채움
    respond(f"{uname} 출근 등록 완료")

@app.command("/퇴근")
def 퇴근_cmd(ack, body, respond, client):
    ack()
    uid = body["user_id"]
    uname = resolve_user_name(client, uid)
    append_log(uid, uname, "퇴근")
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

    append_log(user_id, user_name, action, note=note, date_str=(selected_date or ""))

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

if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
