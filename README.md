D1 기본 뼈대 배치
- Slack 토큰 2개 확보: SLACK_APP_TOKEN, SLACK_BOT_TOKEN. Socket Mode 켠다.
- 워크스페이스에 앱 설치.
- 구글 시트 만들고 서비스계정 이메일을 “편집자”로 공유. 시트 시간대는 Asia/Seoul.
- Slack 앱 생성+권한 설정: commands, app_mentions:read, chat:write, channels:history. Socket Mode 활성화
- 시트 5장 만들기: logs, balances, schedule_weekly, holidays, admin_requests.
- 키워드 “출근/퇴근” 정규식 처리
- 동작 확인: 채널에서 “출근” 입력 → 봇 확인 메시지 + logs에 한 줄 생김
