from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any


class InvalidHeartbeat(ValueError):
    """Raised when a Kafka value is not the expected health CloudEvent."""


@dataclass(frozen=True)
class HeartbeatMessage:
    payload: dict[str, Any]
    raw_text: str
    topic: str
    partition: int
    offset: int
    key: str | None
    consumed_at: datetime
    event_id: str
    event_type: str
    event_source: str
    event_time: str
    device_id: str
    system_id: str
    hostname: str
    ip_address: str
    program_name: str
    program_version: str
    status_level: str
    status_code: str
    status_message: str
    sequence: int
    interval_seconds: int
    generated_at: str

    @classmethod
    def from_kafka_record(cls, record: Any) -> "HeartbeatMessage":
        raw_bytes = record.value() or b""
        raw_text = raw_bytes.decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise InvalidHeartbeat("Kafka value is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise InvalidHeartbeat("CloudEvent must be a JSON object")
        if payload.get("specversion") != "1.0":
            raise InvalidHeartbeat("CloudEvent specversion must be 1.0")

        data = _object(payload, "data")
        source_info = _object(data, "sourceInfo")
        status = _object(data, "status")
        # The captured producer message contains the legacy typo `hearbeat`, while
        # the current producer source emits the correct `heartbeat` spelling.
        heartbeat = data.get("heartbeat") or data.get("hearbeat")
        if not isinstance(heartbeat, dict):
            raise InvalidHeartbeat("data.heartbeat is required")

        key_bytes = record.key()
        message = cls(
            payload=payload,
            raw_text=raw_text,
            topic=record.topic(),
            partition=record.partition(),
            offset=record.offset(),
            key=(
                key_bytes.decode("utf-8", errors="replace")
                if key_bytes is not None
                else None
            ),
            consumed_at=datetime.now(timezone.utc),
            event_id=_text(payload, "id"),
            event_type=_text(payload, "type"),
            event_source=_text(payload, "source"),
            event_time=_text(payload, "time"),
            device_id=_text(source_info, "instanceId"),
            system_id=_text(source_info, "systemId"),
            hostname=_text(source_info, "hostname"),
            ip_address=_text(source_info, "ipAddress"),
            program_name=_text(source_info, "programName"),
            program_version=_text(source_info, "programVersion"),
            status_level=_text(status, "level").upper(),
            status_code=_text(status, "code"),
            status_message=_text(status, "message"),
            sequence=_integer(heartbeat, "sequence"),
            interval_seconds=_integer(heartbeat, "interval"),
            generated_at=_text(heartbeat, "generatedAt"),
        )
        if message.status_level not in {"UP", "WARN", "UNKNOWN", "DOWN"}:
            raise InvalidHeartbeat(f"Unsupported status level: {message.status_level}")
        if message.interval_seconds <= 0:
            raise InvalidHeartbeat("heartbeat interval must be greater than zero")
        return message

    def display_name(self) -> str:
        return f"{self.program_name} / {self.hostname} ({self.device_id})"

    def status_signature(self) -> tuple[str, str]:
        return self.status_level, self.status_code

    def pretty_payload(self) -> str:
        return json.dumps(self.payload, ensure_ascii=False, indent=2)


def _object(parent: dict[str, Any], field: str) -> dict[str, Any]:
    value = parent.get(field)
    if not isinstance(value, dict):
        raise InvalidHeartbeat(f"{field} must be a JSON object")
    return value


def _text(parent: dict[str, Any], field: str) -> str:
    value = parent.get(field)
    if value is None or str(value).strip() == "":
        raise InvalidHeartbeat(f"{field} is required")
    return str(value).strip()


def _integer(parent: dict[str, Any], field: str) -> int:
    value = parent.get(field)
    if isinstance(value, bool):
        raise InvalidHeartbeat(f"{field} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidHeartbeat(f"{field} must be an integer") from exc
