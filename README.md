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

## Kafka SASL_SSL

기본 설정은 9094 포트의 `SASL_SSL` 접속과 consumer group
`healthcheck-monitor`입니다. 브로커 설정에 맞춰 `KAFKA_SASL_MECHANISM`을
`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` 중 하나로 지정합니다. 사설 CA를
사용하는 환경이면 `KAFKA_SSL_CA_LOCATION`에 PEM 인증서 경로를 지정하세요.

160대가 1분마다 heartbeat를 보내는 정도는 단일 consumer로 충분합니다. 다만 운영
알림은 메시지별 발송보다 장비별 마지막 수신 시각을 저장하고, 일정 시간(예: 3분)이
지나도록 heartbeat가 없는 장비만 장애로 묶어서 알리는 방식을 권장합니다. 현재의
`MAIL_MIN_INTERVAL_SECONDS`는 메일 폭주를 막기 위한 임시 안전장치이며, 장비별 누락
감지는 실제 heartbeat의 장비 ID 필드에 맞춰 추가해야 합니다.

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
