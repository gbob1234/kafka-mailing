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
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    smtp_from: str
    smtp_to: tuple[str, ...]
    smtp_use_starttls: bool
    smtp_use_ssl: bool
    mail_subject_prefix: str
    mail_min_interval_seconds: float

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
                "KAFKA_GROUP_ID", "heartbeat-smtp-notifier"
            ).strip(),
            kafka_auto_offset_reset=os.getenv(
                "KAFKA_AUTO_OFFSET_RESET", "latest"
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
            mail_min_interval_seconds=float(
                os.getenv("MAIL_MIN_INTERVAL_SECONDS", "0")
            ),
        )
        if settings.smtp_use_starttls and settings.smtp_use_ssl:
            raise ValueError("SMTP_USE_STARTTLS and SMTP_USE_SSL cannot both be true")
        if not settings.smtp_to:
            raise ValueError("SMTP_TO must contain at least one email address")
        if settings.mail_min_interval_seconds < 0:
            raise ValueError("MAIL_MIN_INTERVAL_SECONDS cannot be negative")
        return settings

