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

테스트는 별도 테스트 패키지 없이 실행할 수 있습니다.

```powershell
python -m unittest discover -v
```

`SMTP_TO`는 쉼표로 여러 주소를 지정할 수 있습니다. 정상 heartbeat는 메일 없이
상태만 갱신합니다. 장비가 `WARN`/`UNKNOWN` 상태로 전환될 때, `UP`으로 복구될 때,
그리고 heartbeat가 일정 시간 들어오지 않을 때만 메일을 발송합니다.

장비별 최신 상태는 기본적으로 `heartbeat_state.db` SQLite 파일에 저장됩니다.
경로는 `SQLITE_PATH`로 변경할 수 있으며 DB 파일과 WAL 파일은 Git에서 제외됩니다.

## Kafka SASL_SSL

기본 설정은 9094 포트의 `SASL_SSL` 접속과 consumer group
`healthcheck-monitor`입니다. 브로커 설정에 맞춰 `KAFKA_SASL_MECHANISM`을
`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` 중 하나로 지정합니다. 사설 CA를
사용하는 환경이면 `KAFKA_SSL_CA_LOCATION`에 PEM 인증서 경로를 지정하세요.

160대가 1분마다 heartbeat를 보내는 정도는 단일 consumer로 충분합니다. 다만 운영
consumer는 Kafka record key와 CloudEvent의 `data.sourceInfo.instanceId`를 장비 ID로
사용합니다. `HEARTBEAT_STALE_AFTER_SECONDS`와 메시지의 heartbeat interval 3배 중
큰 값을 미수신 기준으로 사용합니다. 한 번 이상 수신한 장비는 SQLite에 저장되므로
프로세스를 재시작해도 마지막 수신 시각, 상태, 미수신 알림 여부가 유지됩니다. 아직
한 번도 메시지를 받지 않은 장비까지 감시하려면 별도의 전체 장비 목록이 필요합니다.

SQLite 접근은 `DeviceStateRepository` 경계 뒤에 분리되어 있습니다. 이후 PostgreSQL로
전환할 때 동일 메서드를 구현하는 저장소를 추가하고 consumer에 주입하면 됩니다.

## 컨테이너 이미지

이미지에는 실행 명령 `python main.py`가 포함되어 있으므로 Kubernetes에서 별도의
`command`나 `args`를 지정할 필요가 없습니다.

```powershell
docker build -t kafka-mailing:latest .
docker run --rm --env-file .env -v heartbeat-state:/data kafka-mailing:latest
```

컨테이너는 root가 아닌 UID/GID `10001`로 실행하며 SQLite 기본 경로는
`/data/heartbeat_state.db`입니다.

## Kubernetes

`k8s/`에 ConfigMap, Secret 예제, PVC, Deployment가 있습니다. 일반 설정은
ConfigMap의 `envFrom`, 인증정보는 Secret의 `envFrom`을 통해 환경변수로 주입됩니다.
애플리케이션은 환경변수를 직접 읽으므로 Pod 실행 명령으로 설정을 전달하거나 `.env`
파일을 이미지에 포함할 필요가 없습니다.

1. `k8s/configmap.yaml`의 Kafka/SMTP 주소와 수신자를 수정합니다.
2. `k8s/secret.example.yaml`을 실제 Secret 관리 방식에 맞게 적용합니다. 실제 비밀번호가
   들어간 Secret YAML은 Git에 커밋하지 마세요.
3. `k8s/deployment.yaml`의 `REPLACE_WITH_IMAGE`를 빌드·배포한 이미지로 바꿉니다.
4. 다음 순서로 적용합니다.

```powershell
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.example.yaml
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/deployment.yaml
```

ConfigMap이나 Secret의 환경변수는 실행 중인 컨테이너에 자동 갱신되지 않습니다.
변경 후에는 `kubectl rollout restart deployment/healthcheck-monitor`로 Pod를 다시
시작해야 합니다. SQLite를 사용하므로 Deployment는 `replicas: 1`과 `Recreate`
전략을 유지해야 합니다.

Kafka가 사설 CA를 사용하면 CA PEM을 별도 ConfigMap/Secret volume으로 마운트하고
`KAFKA_SSL_CA_LOCATION`을 해당 파일 경로로 지정해야 합니다. Java producer의 JKS
truststore 파일을 그대로 지정할 수는 없습니다.

프로듀서가 보내는 Kafka value는 CloudEvents Structured JSON입니다. 현재 프로듀서
소스의 `data.heartbeat`와 실제 캡처 샘플의 레거시 오타 `data.hearbeat`를 모두
호환합니다. 잘못된 JSON이나 필수 필드가 없는 메시지는 오류 로그를 남기고 건너뜁니다.

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
