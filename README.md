# Kafka Heartbeat SMTP Notifier

Kafka heartbeat 메시지를 consume하여 SMTP 이메일로 전달하는 작은 Python 서비스입니다.
수신 상태와 알림을 PostgreSQL의 한 트랜잭션으로 저장한 뒤 Kafka offset을 commit하며, SMTP 발송 실패 시 설정된
지수형 간격으로 재시도합니다.

## 실행

Python 3.10 이상을 권장합니다.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

`.env`에서 PostgreSQL(`DATABASE_URL`), Kafka, Oracle, SMTP 접속 정보를 수정한 후 실행합니다.

```powershell
python main.py
```

단위 테스트와 선택적인 실제 PostgreSQL 통합 테스트를 제공합니다.

```powershell
pip install -r requirements-dev.txt
python -m unittest discover -v
# 통합 테스트: 운영 DB가 아닌 전용 테스트 DB만 지정하세요.
$env:TEST_DATABASE_URL="postgresql://postgres:TEST_PASSWORD@localhost:5432/test_db"
python -m unittest tests.test_postgres_api -v
```

`SMTP_TO`는 쉼표로 여러 주소를 지정할 수 있습니다. 정상 heartbeat는 메일 없이
상태만 갱신합니다. 장비가 `WARN`/`UNKNOWN` 상태로 전환될 때, `UP`으로 복구될 때,
그리고 heartbeat가 일정 시간 들어오지 않을 때만 메일을 발송합니다.

장비별 최신 상태와 관리 설정, 메일 큐는 PostgreSQL에 저장합니다.
`python -m heartbeat_mailer.postgres`로 테이블을 미리 생성할 수 있으며 앱 기동 시에도
기존 데이터를 보존하면서 테이블 유무를 확인합니다. SQLite는 더 이상 런타임에 사용하지 않습니다.

## Kafka SASL_SSL

기본 설정은 9094 포트의 `SASL_SSL` 접속과 consumer group
`healthcheck-monitor`입니다. 브로커 설정에 맞춰 `KAFKA_SASL_MECHANISM`을
`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` 중 하나로 지정합니다. 사설 CA를
사용하는 환경이면서 컨테이너 기본 신뢰 저장소가 해당 CA를 신뢰하지 않는 경우에만
`KAFKA_SSL_CA_LOCATION`에 PEM CA bundle 경로를 지정하세요. 별도 경로 없이
SASL_SSL 연결이 성공한다면 이 값은 비워둘 수 있습니다.

160대가 1분마다 heartbeat를 보내는 정도는 단일 consumer로 충분합니다. 다만 운영
consumer는 Kafka record key와 CloudEvent의 `data.sourceInfo.instanceId`를 장비 ID로
사용합니다. 모니터링 설정 `HEARTBEAT_STALE_AFTER_SECONDS`(기본 180초)만
미수신 기준으로 사용하며 메시지 interval은 판정에 사용하지 않습니다. 한 번 이상 수신한 장비는 PostgreSQL에 저장되므로
프로세스를 재시작해도 마지막 수신 시각, 상태, 미수신 알림 여부가 유지됩니다. 아직
한 번도 메시지를 받지 않은 장비까지 감시하려면 별도의 전체 장비 목록이 필요합니다.

## Oracle MES 장비 상태 조건

이미지 수집기 경고는 heartbeat의 `device_id`와 Oracle 조회 결과의 `EQP_ID`가 정확히
일치하고 `MAIN_STAT_CD`가 `STAB` 또는 `NECK`일 때만 발송합니다. 이 조건은 비정상
`ALERT`, heartbeat 미수신 `MISSING`, 수신 재개 `RECEIVING`, 보고 상태 복구 `RECOVERY`에 모두 적용됩니다. 최초 경고가
MES 조건으로 억제되었다면 이후 복구 메일도 발송하지 않습니다.

Oracle 조회는 Kafka consumer와 별도 thread에서 기본 30초마다 실행하며 결과를 메모리에
캐시합니다. 따라서 Oracle 응답을 기다리느라 Kafka 소비가 지연되지 않습니다. Oracle
조회가 실패하거나 heartbeat 장비 ID에 해당하는 `EQP_ID`가 없으면 경고 로그를 남기고
해당 장비의 메일 알림을 보류합니다. 실패 전 캐시는 알림 판정에 재사용하지 않습니다.

```env
ORACLE_DSN=oracle.example.com:1521/service-name
ORACLE_USER=replace-me
ORACLE_PASSWORD=replace-me
ORACLE_STATUS_QUERY=select eqp_id, main_stat_cd from REPLACE_WITH_STATUS_TABLE
ORACLE_REFRESH_SECONDS=30
ORACLE_CACHE_MAX_AGE_SECONDS=90
ORACLE_CALL_TIMEOUT_MS=5000
ORACLE_ALERT_STATUS_CODES=STAB,NECK
```

`ORACLE_STATUS_QUERY`에는 반드시 첫 번째 컬럼으로 `EQP_ID`, 두 번째 컬럼으로
`MAIN_STAT_CD`를 반환하는 조회 SQL을 넣어야 합니다. SQL 끝의 세미콜론은 있어도
제거한 뒤 실행합니다. Oracle 계정에는 해당 테이블 또는 View의 `SELECT` 권한만
부여하면 됩니다. `python-oracledb`의 기본 Thin 모드를 사용하므로 컨테이너에 Oracle
Instant Client를 별도로 설치할 필요가 없습니다.

저장소는 `heartbeat_mailer/postgres.py`에 분리되어 있으며 API와 consumer가 동일 DB를 사용합니다.
MES Oracle은 읽기 전용 조회 대상으로 유지하며, 모니터링 DB와 별개입니다.

## 안전한 알림 처리

SMTP 발송은 Kafka consumer thread에서 실행하지 않습니다. consumer는 필요한 알림을
PostgreSQL의 `notification_queue`에 기록한 뒤 다음 heartbeat를 처리하고, 별도
worker thread가 큐를 읽어 메일을 발송합니다.

- 활성 상태의 동일 장비·알림·상태는 unique index로 중복 등록되지 않습니다.
- 발송 중 종료된 `SENDING` 작업은 재시작 시 `RETRY`로 복원됩니다.
- SMTP 실패는 지수형 간격으로 재시도하며 최대 간격과 횟수를 제한합니다.
- 최대 횟수를 넘긴 작업은 `DEAD`로 남아 원인과 함께 확인할 수 있습니다.
- consumer lag는 `KAFKA_LAG_LOG_INTERVAL_SECONDS` 주기로 로그에 출력합니다.
- 정상 운영 중 lag와 poll 지연은 로그만 남기며 미수신 판정을 차단하지 않습니다.
- broker metadata와 fresh watermark 조회가 실패하거나 partition이 할당되지 않으면
  장비별 미수신 알림을 전부 보류합니다. 160대 장비 문제로 확대 해석하지 않습니다.
- 프로세스 시작, Kafka 연결 복구 또는 파티션 재할당 후 첫 성공 조회에서
  파티션별 high watermark를 목표로 저장합니다. 모든 목표까지 처리하면 판정을
  재개하며, 이후 새로 들어온 메시지 때문에 목표를 늘리거나 30초 대기하지 않습니다.
  목표 확인은 health check 주기로 이루어지며 offset 차이는 실제 메시지 개수와 다를 수 있습니다.
- 마지막 수신 시각은 모니터링이 메시지를 소비한 시각입니다. 새 heartbeat 처리 시
  기준을 넘는 수신 공백은 로그만 기록하고 과거 미수신 경고를 소급 생성하지 않습니다.
- 미수신 경고 후 heartbeat가 돌아오면 `RECEIVING`을 보냅니다. 수신 재개는
  메시지 내용의 `UP` 복구와 별개이며, `WARN/DOWN` 보고는 기존 상태 경고로 처리합니다.
- Kafka 연결 문제는 시스템 메일 없이 로그로만 남깁니다. 연결 불가 중 신규 미수신
  경고를 만들지 않으며 이미 큐에 등록된 메일의 발송/재시도는 유지합니다.
- 기존 `STALE_GUARD_RECOVERY_SECONDS`, `KAFKA_RECOVERY_STABILIZATION_SECONDS`는
  더 이상 사용하지 않습니다. 기존 환경변수에 남아 있어도 무시됩니다.

```env
MAIL_QUEUE_POLL_SECONDS=1
MAIL_MAX_RETRY_ATTEMPTS=10
MAIL_RETRY_INITIAL_SECONDS=5
MAIL_RETRY_MAX_SECONDS=300
KAFKA_LAG_LOG_INTERVAL_SECONDS=60
KAFKA_POLL_DELAY_GUARD_SECONDS=10
KAFKA_HEALTH_CHECK_INTERVAL_SECONDS=10
KAFKA_HEALTH_CHECK_TIMEOUT_SECONDS=3
KAFKA_HEALTH_MAX_AGE_SECONDS=30
```

## 컨테이너 이미지

이미지에는 실행 명령 `python main.py`가 포함되어 있으므로 Kubernetes에서 별도의
`command`나 `args`를 지정할 필요가 없습니다.

```powershell
docker build -t kafka-mailing:latest .
docker run --rm --env-file .env kafka-mailing:latest
```

컨테이너는 root가 아닌 UID/GID `10001`로 실행합니다. 앱에는 `/data` 볼륨이 필요 없습니다.
`DATABASE_URL`은 컨테이너에서 접근 가능한 DB 주소여야 합니다. 컨테이너 내부의
`localhost`는 노트북이나 다른 DB 컨테이너가 아닙니다. Podman도 동일 명령으로 빌드할 수 있습니다.

## Kubernetes

`k8s/`에 앱 Deployment/API Service와 별도 PostgreSQL StatefulSet/Service/PVC 템플릿이 있습니다. 일반 설정은
ConfigMap의 `envFrom`, 인증정보는 Secret의 `envFrom`을 통해 환경변수로 주입됩니다.
애플리케이션은 환경변수를 직접 읽으므로 Pod 실행 명령으로 설정을 전달하거나 `.env`
파일을 이미지에 포함할 필요가 없습니다.

1. `k8s/configmap.yaml`의 Kafka/SMTP/Oracle 주소, Oracle 조회 SQL과 수신자를 수정합니다.
2. `k8s/secret.example.yaml`을 실제 Secret 관리 방식에 맞게 적용합니다. 실제 비밀번호가
   들어간 Secret YAML은 Git에 커밋하지 마세요. Oracle 계정과 비밀번호도 Secret으로
   주입합니다.
3. `k8s/postgres-secret.example.yaml`의 DB 비밀번호와 앱 Secret의 `DATABASE_URL`을
   일치시킵니다. URL 비밀번호의 `@`, `:`, `/`, `#` 등 특수문자는 percent-encoding해야 합니다.
   `API_TOKEN`은 24자 이상 임의 문자열로 설정합니다.
4. `k8s/postgres.yaml`의 StorageClass를 환경에 맞게 지정합니다. 기본값은 클러스터의
   default StorageClass이며, PowerStore block CSI 볼륨을 권장합니다.
5. `k8s/deployment.yaml`의 모든 `REPLACE_WITH_IMAGE`를 동일한 빌드 이미지 태그로 바꿉니다.
6. 다음 순서로 적용합니다. 아래 Secret 경로는 예시이며 실제 값은 별도 안전한 파일/관리 도구로 주입합니다.

```powershell
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.example.yaml
kubectl apply -f k8s/postgres-secret.example.yaml
kubectl apply -f k8s/postgres.yaml
kubectl rollout status statefulset/healthcheck-postgres
kubectl apply -f k8s/deployment.yaml
kubectl rollout status deployment/healthcheck-monitor
```

ConfigMap이나 Secret의 환경변수는 실행 중인 컨테이너에 자동 갱신되지 않습니다.
변경 후에는 `kubectl rollout restart deployment/healthcheck-monitor`로 Pod를 다시
시작해야 합니다. PostgreSQL이어도 consumer는 `replicas: 1`과 `Recreate`를 유지해야 합니다.
현재 각 consumer가 전체 장비를 감시하므로 여러 replica를 켜면 파티션 소유권과 경고 판정이
충돌할 수 있습니다. API는 독립 프로세스이므로 나중에 별도 Deployment로 분리할 수 있습니다.

- 앱 Pod: schema 생성 init container + consumer 컨테이너 + API 컨테이너. 데이터 PVC 없음.
- DB Pod: `postgres:17-bookworm` StatefulSet 1개 + 전용 5Gi PVC. 앱 재배포와 독립적입니다.
- 스키마 init은 기존 데이터를 삭제하지 않습니다. DB 연결 실패 시 init 단계에서 재시도합니다.
- 예시의 `POSTGRES_USER`는 DB 초기 관리자입니다. 운영 보안을 강화하려면 스키마 초기화 계정과
  앱의 DML 전용 계정을 분리하고, 네트워크 정책으로 DB/API 접근을 제한하세요.
- DB Secret은 최초 빈 PVC 초기화에 사용됩니다. 이미 생성된 DB의 비밀번호는 Secret만
  바꿔서는 변경되지 않으며 DB 계정 변경 작업과 함께 처리해야 합니다.
- DB Pod 1개는 HA 구성이 아닙니다. PVC는 백업을 대신하지 않으므로 별도 백업 정책이 필요합니다.

### 기존 SQLite 배포에서 전환

이번 전환은 **새 PostgreSQL에 재등록하는 방식**입니다. SQLite 데이터를 자동 이관하지 않습니다.
기존 consumer를 중지하고 PostgreSQL을 준비한 뒤 새 이미지를 배포하세요. 기존 consumer와
새 consumer를 동시에 실행하지 마세요. Kafka group은 기존 `healthcheck-monitor`를 유지합니다.

새 heartbeat를 소비한 수집기는 다시 등록되지만, 그때부터 메시지를 보내지 않는 수집기는
DB에 없으므로 감시 대상이 되지 않습니다. 기존 알림 이력/미발송 큐도 자동 이관되지 않습니다.
이 정보까지 보존해야 한다면 전환 전에 데이터 이관이 필요합니다.
기존 SQLite DB와 PVC는 보관하세요. `k8s/pvc.yaml`은 구버전 참고용이며 새 배포에는 적용하지 않습니다.
`SQLITE_PATH`, `SQLITE_JOURNAL_MODE` 환경변수는 더 이상 사용하지 않습니다.

## 수집기 관리 API

API는 Kafka·SMTP·Oracle 설정 없이 `DATABASE_URL`, `API_TOKEN`,
`HEARTBEAT_STALE_AFTER_SECONDS`만으로 실행할 수 있습니다.

```powershell
python -m uvicorn heartbeat_mailer.api:app --host 127.0.0.1 --port 8000
# Kubernetes 내부 API를 로컬에서 확인할 때
kubectl port-forward service/healthcheck-monitor-api 8000:8000
```

`http://127.0.0.1:8000/docs`에서 Swagger UI의 Authorize에 API_TOKEN을 입력해 시험할 수 있습니다.
데이터/변경 API는 `Authorization: Bearer <API_TOKEN>` 인증이 필수입니다.
Service는 내부 ClusterIP이며 외부 노출 시 반드시 TLS와 접근 제어를 추가하세요.

| 메서드 | 경로 | 기능 |
| --- | --- | --- |
| GET | `/api/v1/devices?limit=100&offset=0&include_deleted=false` | 목록/페이지 조회 |
| GET | `/api/v1/devices/{device_id}` | 수신 상태와 관리 설정 조회 |
| PATCH | `/api/v1/devices/{device_id}/monitoring` | `{"enabled":false,"reason":"계획 작업"}`로 감시 제외, true로 재개 |
| DELETE | `/api/v1/devices/{device_id}?reason=retired` | 논리 삭제, 자동 재등록 차단 |
| POST | `/api/v1/devices/{device_id}/restore` | 삭제 복원. 감시는 제외 상태로 유지 |
| GET | `/health/live`, `/health/ready` | API 생존/DB 연결 확인. 인증 불필요 |

감시 제외 상태에서도 최신 heartbeat는 저장됩니다. 삭제 상태에서는 상태 갱신도 중지하고
삭제 표시를 보존합니다. 복원 뒤에는 PATCH로 감시를 명시적으로 재개해야 합니다.
감시 재개 시 새 180초 유예를 주지 않으므로 마지막 수신이 이미 기준을 넘었다면 Kafka/MES
조건을 충족하는 다음 점검에서 경고할 수 있습니다.

감시 제외·삭제·복원 시 미발송 큐와 알림 등록 플래그를 정리합니다. API 성공 이후에는 해당
취소 작업이 새로 발송되지 않습니다. 이미 SMTP 전송 중인 메일은 취소할 수 없어 API가 해당
발송 종료를 기다릴 수 있습니다. DB 잠금 제한 시간을 넘기면 503이므로 상태 조회 후 재시도하세요.
SMTP 발송 잠금과 소비 상태 저장 잠금은 분리했습니다.

조회 응답의 `receptionStatus`는 마지막 소비 시각과 현재 시각의 차이를 보여주는 참고 상태입니다.
Kafka 복구 대기나 MES 조건까지 포함한 발송 여부는 아니며 `missingAlertRegistered`도 발송 완료가
아닌 큐 등록 여부입니다. API 시각은 UTC ISO 8601, 이메일 시각은 KST입니다.
API health는 consumer/Kafka/Oracle 전체 상태를 검사하는 엔드포인트가 아닙니다.

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

Kafka와 PostgreSQL에는 CloudEvent의 UTC 원문을 그대로 유지합니다. 이메일에서는
수집기 보고 시각과 모니터링 수신 시각을 KST로 표시합니다. 현업용 HTML 메일은
Outlook 호환성을 위해 테이블 레이아웃과 인라인 CSS를 사용하며 수집기 상태, 대상 장비
식별정보와 판단 시각만 간결하게 제공합니다. 원본 Payload와 Kafka topic/partition/offset
정보는 PostgreSQL과 로그에는 유지하지만 메일 본문에는 포함하지 않습니다.

`.env`는 Git에서 제외되어 있습니다. 운영 환경에서는 비밀번호를 secret manager나
배포 환경변수로 주입하는 것이 좋습니다.

메일 발송 직후 프로세스가 종료되어 큐의 SENT 상태가 DB에 commit되지 않으면 동일 메일이 다시
발송될 수 있습니다. 즉, 이 서비스는 유실을 줄이는 at-least-once 방식입니다.

## 이후 백엔드 편입

설정, Kafka record 변환, SMTP 발송, consumer 실행 루프를 분리했습니다. 기존 Python
백엔드에 합칠 때는 `HeartbeatNotifier`를 별도 worker/process로 실행하거나 consumer
루프만 백그라운드 worker에 연결하면 됩니다.
