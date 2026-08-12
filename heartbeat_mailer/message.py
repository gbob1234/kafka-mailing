from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any


@dataclass(frozen=True)
class HeartbeatMessage:
    payload: Any
    raw_text: str
    topic: str
    partition: int
    offset: int
    key: str | None
    consumed_at: datetime

    @classmethod
    def from_kafka_record(cls, record: Any) -> "HeartbeatMessage":
        raw_bytes = record.value() or b""
        raw_text = raw_bytes.decode("utf-8", errors="replace")
        try:
            payload: Any = json.loads(raw_text)
        except json.JSONDecodeError:
            payload = raw_text

        key_bytes = record.key()
        return cls(
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
        )

    def display_name(self) -> str:
        if isinstance(self.payload, dict):
            for field in ("serviceName", "service", "application", "pipelineId", "id"):
                value = self.payload.get(field)
                if value:
                    return str(value)
        return self.key or f"{self.topic}:{self.partition}"

    def pretty_payload(self) -> str:
        if isinstance(self.payload, (dict, list)):
            return json.dumps(self.payload, ensure_ascii=False, indent=2)
        return str(self.payload)

