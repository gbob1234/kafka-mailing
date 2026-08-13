from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any


class InvalidHeartbeat(ValueError):
    """Kafka value가 기대한 health CloudEvent 형식이 아닐 때 발생하는 예외."""


@dataclass(frozen=True)
class HeartbeatMessage:
    """검증된 CloudEvent와 Kafka record 메타데이터를 담는 불변 객체."""
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
        """Kafka record의 UTF-8 JSON을 검증하여 heartbeat 객체로 변환한다.

        입력:
            record: ``value``, ``key``, ``topic``, ``partition``, ``offset``
                메서드를 제공하는 confluent-kafka Message 호환 객체.
        반환:
            CloudEvents 1.0 필수 필드와 장비 상태를 추출한
            ``HeartbeatMessage`` 객체.
        예외:
            InvalidHeartbeat: JSON 파싱 실패, 필수 필드 누락 또는 지원하지
                않는 상태값이 발견된 경우.
        """
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
        # 현재 생산자는 heartbeat를 사용한다. 이미 발행된 구버전 메시지와 DB 복원을
        # 위해 과거 오타인 hearbeat도 하위 호환으로 허용한다.
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
        """메일 제목에 사용할 사람이 읽기 쉬운 수집기 표시명을 반환한다.

        입력:
            없음. 객체의 프로그램명, 수집기 호스트명, 대상 장비 ID를 사용한다.
        반환:
            ``프로그램 / 수집기 호스트 (대상 장비 ID)`` 형식의 문자열.
        """
        return f"{self.program_name} / {self.hostname} ({self.device_id})"

    def status_signature(self) -> tuple[str, str]:
        """동일 장애의 반복 여부를 판단할 상태 식별값을 반환한다.

        입력:
            없음.
        반환:
            ``(상태 레벨, 상태 코드)`` 튜플.
        """
        return self.status_level, self.status_code

    def pretty_payload(self) -> str:
        """원본 CloudEvent를 이메일용 들여쓰기 JSON 문자열로 변환한다.

        입력:
            없음.
        반환:
            한글을 이스케이프하지 않은 가독성 높은 JSON 문자열.
        """
        return json.dumps(self.payload, ensure_ascii=False, indent=2)


def _object(parent: dict[str, Any], field: str) -> dict[str, Any]:
    """부모 JSON 객체에서 필수 하위 객체를 꺼낸다.

    입력:
        parent: 검색할 JSON 객체.
        field: 하위 객체의 필드명.
    반환:
        해당 필드의 ``dict`` 값.
    예외:
        InvalidHeartbeat: 필드가 없거나 JSON 객체가 아닌 경우.
    """
    value = parent.get(field)
    if not isinstance(value, dict):
        raise InvalidHeartbeat(f"{field} must be a JSON object")
    return value


def _text(parent: dict[str, Any], field: str) -> str:
    """부모 JSON 객체에서 비어 있지 않은 필수 문자열을 읽는다.

    입력:
        parent: 검색할 JSON 객체.
        field: 읽을 필드명.
    반환:
        앞뒤 공백을 제거한 문자열.
    예외:
        InvalidHeartbeat: 값이 없거나 빈 문자열인 경우.
    """
    value = parent.get(field)
    if value is None or str(value).strip() == "":
        raise InvalidHeartbeat(f"{field} is required")
    return str(value).strip()


def _integer(parent: dict[str, Any], field: str) -> int:
    """부모 JSON 객체의 필수 값을 정수로 변환한다.

    입력:
        parent: 검색할 JSON 객체.
        field: 읽을 필드명.
    반환:
        변환된 정수.
    예외:
        InvalidHeartbeat: 불리언이거나 정수로 변환할 수 없는 경우.
    """
    value = parent.get(field)
    if isinstance(value, bool):
        raise InvalidHeartbeat(f"{field} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidHeartbeat(f"{field} must be an integer") from exc
