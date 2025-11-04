import os, re, pytz, datetime as dt
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import gspread
from google.oauth2.service_account import Credentials

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

@app.command("/근태")
def 근태(ack, body, respond):
    ack()
    text = body.get("text","").strip()
    if text.startswith("연차"):
        _, date = text.split(maxsplit=1)
        append_log(body["user_id"], "", "annual", date_str=date)
        respond(f"연차 {date} 등록")
    elif text.startswith("휴무"):
        _, date = text.split(maxsplit=1)
        append_log(body["user_id"], "", "off", date_str=date)
        respond(f"휴무 {date} 등록")
    elif text in ("출근","퇴근"):
        t = "출근" if text=="출근" else "퇴근"
        append_log(body["user_id"], "", t)
        respond(f"{text} 등록 완료")
    else:
        respond("사용법: `/근태 출근|퇴근` 또는 `/근태 연차 YYYY-MM-DD`")

if __name__ == "__main__":
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
