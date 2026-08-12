from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Required environment variable is missing: {name}")
    return value


def _bool(name: str, default: bool) -> bool:
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
