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


def _bool(name: str, default: bool) -> bool:
    """환경변수 문자열을 불리언 값으로 변환한다.

    입력:
        name: 읽을 환경변수 이름.
        default: 환경변수가 없을 때 사용할 기본값.
    반환:
        true/false 계열 문자열을 변환한 ``bool`` 값.
    예외:
        ValueError: 지원하지 않는 문자열이 설정된 경우.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


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
    smtp_username: str
    smtp_password: str
    smtp_from: str
    smtp_to: tuple[str, ...]
    smtp_use_starttls: bool
    smtp_use_ssl: bool
    mail_subject_prefix: str
    heartbeat_stale_after_seconds: float
    sqlite_path: str

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
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            smtp_username=os.getenv("SMTP_USERNAME", "").strip(),
            smtp_password=os.getenv("SMTP_PASSWORD", ""),
            smtp_from=_required("SMTP_FROM"),
            smtp_to=smtp_to,
            smtp_use_starttls=_bool("SMTP_USE_STARTTLS", True),
            smtp_use_ssl=_bool("SMTP_USE_SSL", False),
            mail_subject_prefix=os.getenv(
                "MAIL_SUBJECT_PREFIX", "[Kafka Heartbeat]"
            ).strip(),
            heartbeat_stale_after_seconds=float(
                os.getenv("HEARTBEAT_STALE_AFTER_SECONDS", "180")
            ),
            sqlite_path=os.getenv(
                "SQLITE_PATH", "heartbeat_state.db"
            ).strip(),
        )
        if settings.smtp_use_starttls and settings.smtp_use_ssl:
            raise ValueError("SMTP_USE_STARTTLS and SMTP_USE_SSL cannot both be true")
        if settings.kafka_security_protocol.upper() != "SASL_SSL":
            raise ValueError("KAFKA_SECURITY_PROTOCOL must be SASL_SSL")
        if not settings.smtp_to:
            raise ValueError("SMTP_TO must contain at least one email address")
        if settings.heartbeat_stale_after_seconds <= 0:
            raise ValueError("HEARTBEAT_STALE_AFTER_SECONDS must be positive")
        if not settings.sqlite_path:
            raise ValueError("SQLITE_PATH cannot be empty")
        return settings
