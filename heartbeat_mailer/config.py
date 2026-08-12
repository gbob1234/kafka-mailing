from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv


def _required(name: str) -> str:
    """필수 환경변수를 읽고 앞뒤 공백을 제거한다.

    입력:
        name: 읽을 환경변수 이름.
    반환:
        비어 있지 않은 환경변수 문자열.
    예외:
        ValueError: 환경변수가 없거나 빈 문자열인 경우.
    """
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Required environment variable is missing: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    """Kafka, SMTP, 알림 및 SQLite 실행 설정을 보관하는 불변 객체."""
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    kafka_auto_offset_reset: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    kafka_ssl_ca_location: str
    smtp_host: str
    smtp_port: int
    smtp_from: str
    smtp_to: tuple[str, ...]
    mail_subject_prefix: str
    heartbeat_stale_after_seconds: float
    sqlite_path: str
    mail_queue_poll_seconds: float
    mail_max_retry_attempts: int
    mail_retry_initial_seconds: float
    mail_retry_max_seconds: float
    kafka_lag_log_interval_seconds: float
    kafka_poll_delay_guard_seconds: float
    stale_guard_recovery_seconds: float

    @classmethod
    def from_env(cls) -> "Settings":
        """환경변수와 ``.env`` 파일에서 전체 설정을 생성하고 검증한다.

        입력:
            없음. 프로세스 환경변수와 작업 경로의 ``.env``를 사용한다.
        반환:
            유효성 검사를 통과한 ``Settings`` 객체.
        예외:
            ValueError: 필수값 누락, 숫자 변환 실패 또는 서로 충돌하는
                설정이 발견된 경우.
        """
        load_dotenv()
        smtp_to = tuple(
            address.strip()
            for address in _required("SMTP_TO").split(",")
            if address.strip()
        )
        settings = cls(
            kafka_bootstrap_servers=_required("KAFKA_BOOTSTRAP_SERVERS"),
            kafka_topic=_required("KAFKA_TOPIC"),
            kafka_group_id=os.getenv(
                "KAFKA_GROUP_ID", "healthcheck-monitor"
            ).strip(),
            kafka_auto_offset_reset=os.getenv(
                "KAFKA_AUTO_OFFSET_RESET", "latest"
            ).strip(),
            kafka_security_protocol=os.getenv(
                "KAFKA_SECURITY_PROTOCOL", "SASL_SSL"
            ).strip(),
            kafka_sasl_mechanism=os.getenv(
                "KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"
            ).strip(),
            kafka_sasl_username=_required("KAFKA_SASL_USERNAME"),
            kafka_sasl_password=_required("KAFKA_SASL_PASSWORD"),
            kafka_ssl_ca_location=os.getenv(
                "KAFKA_SSL_CA_LOCATION", ""
            ).strip(),
            smtp_host=_required("SMTP_HOST"),
            smtp_port=int(os.getenv("SMTP_PORT", "25")),
            smtp_from=_required("SMTP_FROM"),
            smtp_to=smtp_to,
            mail_subject_prefix=os.getenv(
                "MAIL_SUBJECT_PREFIX", "[Kafka Heartbeat]"
            ).strip(),
            heartbeat_stale_after_seconds=float(
                os.getenv("HEARTBEAT_STALE_AFTER_SECONDS", "180")
            ),
            sqlite_path=os.getenv(
                "SQLITE_PATH", "heartbeat_state.db"
            ).strip(),
            mail_queue_poll_seconds=float(
                os.getenv("MAIL_QUEUE_POLL_SECONDS", "1")
            ),
            mail_max_retry_attempts=int(
                os.getenv("MAIL_MAX_RETRY_ATTEMPTS", "10")
            ),
            mail_retry_initial_seconds=float(
                os.getenv("MAIL_RETRY_INITIAL_SECONDS", "5")
            ),
            mail_retry_max_seconds=float(
                os.getenv("MAIL_RETRY_MAX_SECONDS", "300")
            ),
            kafka_lag_log_interval_seconds=float(
                os.getenv("KAFKA_LAG_LOG_INTERVAL_SECONDS", "60")
            ),
            kafka_poll_delay_guard_seconds=float(
                os.getenv("KAFKA_POLL_DELAY_GUARD_SECONDS", "10")
            ),
            stale_guard_recovery_seconds=float(
                os.getenv("STALE_GUARD_RECOVERY_SECONDS", "30")
            ),
        )
        if settings.kafka_security_protocol.upper() != "SASL_SSL":
            raise ValueError("KAFKA_SECURITY_PROTOCOL must be SASL_SSL")
        if not settings.smtp_to:
            raise ValueError("SMTP_TO must contain at least one email address")
        if settings.heartbeat_stale_after_seconds <= 0:
            raise ValueError("HEARTBEAT_STALE_AFTER_SECONDS must be positive")
        if not settings.sqlite_path:
            raise ValueError("SQLITE_PATH cannot be empty")
        if settings.mail_queue_poll_seconds <= 0:
            raise ValueError("MAIL_QUEUE_POLL_SECONDS must be positive")
        if settings.mail_max_retry_attempts <= 0:
            raise ValueError("MAIL_MAX_RETRY_ATTEMPTS must be positive")
        if settings.mail_retry_initial_seconds <= 0:
            raise ValueError("MAIL_RETRY_INITIAL_SECONDS must be positive")
        if settings.mail_retry_max_seconds < settings.mail_retry_initial_seconds:
            raise ValueError(
                "MAIL_RETRY_MAX_SECONDS must be greater than or equal to "
                "MAIL_RETRY_INITIAL_SECONDS"
            )
        if settings.kafka_lag_log_interval_seconds <= 0:
            raise ValueError("KAFKA_LAG_LOG_INTERVAL_SECONDS must be positive")
        if settings.kafka_poll_delay_guard_seconds <= 0:
            raise ValueError("KAFKA_POLL_DELAY_GUARD_SECONDS must be positive")
        if settings.stale_guard_recovery_seconds <= 0:
            raise ValueError("STALE_GUARD_RECOVERY_SECONDS must be positive")
        return settings
