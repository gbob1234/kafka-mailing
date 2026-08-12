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
사용하는 환경이면서 컨테이너 기본 신뢰 저장소가 해당 CA를 신뢰하지 않는 경우에만
`KAFKA_SSL_CA_LOCATION`에 PEM CA bundle 경로를 지정하세요. 별도 경로 없이
SASL_SSL 연결이 성공한다면 이 값은 비워둘 수 있습니다.

160대가 1분마다 heartbeat를 보내는 정도는 단일 consumer로 충분합니다. 다만 운영
consumer는 Kafka record key와 CloudEvent의 `data.sourceInfo.instanceId`를 장비 ID로
사용합니다. `HEARTBEAT_STALE_AFTER_SECONDS`와 메시지의 heartbeat interval 3배 중
큰 값을 미수신 기준으로 사용합니다. 한 번 이상 수신한 장비는 SQLite에 저장되므로
프로세스를 재시작해도 마지막 수신 시각, 상태, 미수신 알림 여부가 유지됩니다. 아직
한 번도 메시지를 받지 않은 장비까지 감시하려면 별도의 전체 장비 목록이 필요합니다.

SQLite 접근은 `DeviceStateRepository` 경계 뒤에 분리되어 있습니다. 이후 PostgreSQL로
전환할 때 동일 메서드를 구현하는 저장소를 추가하고 consumer에 주입하면 됩니다.

## 안전한 알림 처리

SMTP 발송은 Kafka consumer thread에서 실행하지 않습니다. consumer는 필요한 알림을
SQLite의 `notification_queue`에 기록한 뒤 즉시 다음 heartbeat를 처리하고, 별도
worker thread가 큐를 읽어 메일을 발송합니다.

- 활성 상태의 동일 장비·알림·상태는 unique index로 중복 등록되지 않습니다.
- 발송 중 종료된 `SENDING` 작업은 재시작 시 `RETRY`로 복원됩니다.
- SMTP 실패는 지수형 간격으로 재시도하며 최대 간격과 횟수를 제한합니다.
- 최대 횟수를 넘긴 작업은 `DEAD`로 남아 원인과 함께 확인할 수 있습니다.
- consumer lag는 `KAFKA_LAG_LOG_INTERVAL_SECONDS` 주기로 로그에 출력합니다.
- poll 간격이 비정상적으로 길면 `STALE_GUARD_RECOVERY_SECONDS` 동안 미수신
  판정을 보류하여 backlog에 있는 정상 heartbeat를 장애로 오판하지 않게 합니다.

```env
MAIL_QUEUE_POLL_SECONDS=1
MAIL_MAX_RETRY_ATTEMPTS=10
MAIL_RETRY_INITIAL_SECONDS=5
MAIL_RETRY_MAX_SECONDS=300
KAFKA_LAG_LOG_INTERVAL_SECONDS=60
KAFKA_POLL_DELAY_GUARD_SECONDS=10
STALE_GUARD_RECOVERY_SECONDS=30
```

## 컨테이너 이미지

이미지에는 실행 명령 `python main.py`가 포함되어 있으므로 Kubernetes에서 별도의
`command`나 `args`를 지정할 필요가 없습니다.

```powershell
docker build -t kafka-mailing:latest .
docker run --rm --env-file .env -v heartbeat-state:/data kafka-mailing:latest
```

컨테이너는 root가 아닌 UID/GID `10001`로 실행하며 SQLite 기본 경로는
`/data/heartbeat_state.db`입니다.

Dockerfile의 `VOLUME` 선언은 Kubernetes PVC 마운트와 관계가 없고 익명 volume 혼동을
줄이기 위해 제거했습니다. `/data` 디렉터리와 UID 10001 권한은 이미지에 유지됩니다.

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

Deployment의 init container는 PowerStore PVC 마운트 직후 `/data` 소유권을 UID/GID
10001로 맞춘 다음, 실제 애플리케이션 사용자로 SQLite 파일과 테이블을 생성합니다.
권한 또는 스토리지 문제가 있으면 본 컨테이너 대신 init container 단계에서 명확히
실패합니다.

PowerStore block CSI 볼륨이면 기본 `SQLITE_JOURNAL_MODE=WAL`을 사용합니다. NFS로
제공되는 PowerStore 볼륨이면 파일 잠금 호환성을 위해 `DELETE`를 권장합니다. NFS
root-squash 정책으로 `chown`이 거부된다면 export 또는 StorageClass에서 UID/GID
10001 쓰기 권한을 제공해야 하며 Pod 내부 명령만으로 우회할 수 없습니다.

Kafka 인증서가 컨테이너 기본 신뢰 저장소에서 검증되지 않을 때만 CA PEM을 별도
ConfigMap/Secret volume으로 마운트하고 `KAFKA_SSL_CA_LOCATION`을 해당 파일 경로로
지정합니다. Java producer의 JKS truststore 파일을 그대로 지정할 수는 없습니다.
Nginx TLS Secret의 `tls.crt`를 재사용하려면 그 파일에 Kafka 인증서의 발급 CA chain이
PEM 형식으로 포함되어 있어야 합니다. `tls.key`는 Kafka 서버 인증 검증에 사용하지
않습니다.

프로듀서가 보내는 Kafka value는 CloudEvents Structured JSON입니다. 현재 프로듀서
소스의 `data.heartbeat`와 실제 캡처 샘플의 레거시 오타 `data.hearbeat`를 모두
호환합니다. 잘못된 JSON이나 필수 필드가 없는 메시지는 오류 로그를 남기고 건너뜁니다.

## SMTP 연결

메일 서버는 사내 IP 화이트리스트 기반의 plain SMTP relay를 전제로 합니다. 항상
`smtplib.SMTP`로 연결하며 `SMTP_SSL`, STARTTLS, SMTP AUTH를 사용하지 않습니다.
기본 포트는 25이고 `SMTP_HOST`, `SMTP_PORT`, `SMTP_FROM`, `SMTP_TO`만 설정합니다.

Kafka와 SQLite에는 CloudEvent의 UTC 원문을 그대로 유지합니다. 이메일에서는
CloudEvent 시각, heartbeat 생성 시각, 메일러 수신 시각을 KST(UTC+09:00)로 표시하고,
Payload 영역의 `time` 및 `*At` 필드도 이메일 표시용 복사본에서만 `+09:00`으로
변환합니다.

`.env`는 Git에서 제외되어 있습니다. 운영 환경에서는 비밀번호를 secret manager나
배포 환경변수로 주입하는 것이 좋습니다.

메일 발송 직후 프로세스가 종료되어 Kafka commit이 완료되지 않으면 동일 메일이 다시
발송될 수 있습니다. 즉, 이 서비스는 유실을 줄이는 at-least-once 방식입니다.

## 이후 백엔드 편입

설정, Kafka record 변환, SMTP 발송, consumer 실행 루프를 분리했습니다. 기존 Python
백엔드에 합칠 때는 `HeartbeatNotifier`를 별도 worker/process로 실행하거나 consumer
루프만 백그라운드 worker에 연결하면 됩니다.
