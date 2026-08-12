# Kafka Heartbeat SMTP Notifier

Kafka heartbeat 메시지를 consume하여 SMTP 이메일로 전달하는 작은 Python 서비스입니다.
메일 발송에 성공한 뒤에만 Kafka offset을 commit하며, 발송 실패 시 같은 메시지를 5초
간격으로 재시도합니다.

## 실행

Python 3.10 이상을 권장합니다.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

`.env`에서 Kafka, SMTP 접속 정보를 수정한 후 실행합니다.

```powershell
python main.py
```

`SMTP_TO`는 쉼표로 여러 주소를 지정할 수 있습니다. heartbeat마다 메일을 보내지
않으려면 `MAIL_MIN_INTERVAL_SECONDS=300`처럼 최소 간격을 설정하세요. 간격 안에
들어온 메시지는 메일 없이 offset이 commit됩니다.

## SMTP 설정 예시

- STARTTLS(일반적으로 port 587): `SMTP_USE_STARTTLS=true`, `SMTP_USE_SSL=false`
- Implicit TLS(일반적으로 port 465): `SMTP_USE_STARTTLS=false`, `SMTP_USE_SSL=true`
- 사내 SMTP가 인증을 요구하지 않으면 `SMTP_USERNAME`과 `SMTP_PASSWORD`를 비웁니다.

`.env`는 Git에서 제외되어 있습니다. 운영 환경에서는 비밀번호를 secret manager나
배포 환경변수로 주입하는 것이 좋습니다.

메일 발송 직후 프로세스가 종료되어 Kafka commit이 완료되지 않으면 동일 메일이 다시
발송될 수 있습니다. 즉, 이 서비스는 유실을 줄이는 at-least-once 방식입니다.

## 이후 백엔드 편입

설정, Kafka record 변환, SMTP 발송, consumer 실행 루프를 분리했습니다. 기존 Python
백엔드에 합칠 때는 `HeartbeatNotifier`를 별도 worker/process로 실행하거나 consumer
루프만 백그라운드 worker에 연결하면 됩니다.
