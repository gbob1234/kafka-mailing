from __future__ import annotations

import json
import unittest

from heartbeat_mailer.message import HeartbeatMessage, InvalidHeartbeat


class FakeRecord:
    """HeartbeatMessage 파서 테스트에 사용하는 Kafka record 대역 객체."""

    def __init__(self, payload: dict, key: str = "DEVICE-001") -> None:
        """테스트 payload와 Kafka key를 UTF-8 bytes로 준비한다.

        입력:
            payload: Kafka value로 직렬화할 JSON 객체.
            key: 장비를 구분할 Kafka record key.
        반환:
            없음.
        """
        self._payload = json.dumps(payload).encode("utf-8")
        self._key = key.encode("utf-8")

    def value(self) -> bytes:
        """테스트 Kafka value를 반환한다.

        입력:
            없음.
        반환:
            JSON으로 직렬화된 UTF-8 bytes.
        """
        return self._payload

    def key(self) -> bytes:
        """테스트 Kafka key를 반환한다.

        입력:
            없음.
        반환:
            UTF-8 장비 ID bytes.
        """
        return self._key

    def topic(self) -> str:
        """테스트 topic 이름을 반환한다.

        입력:
            없음.
        반환:
            고정 topic 문자열.
        """
        return "healthcheck"

    def partition(self) -> int:
        """테스트 partition 번호를 반환한다.

        입력:
            없음.
        반환:
            고정 partition 번호 0.
        """
        return 0

    def offset(self) -> int:
        """테스트 offset을 반환한다.

        입력:
            없음.
        반환:
            고정 offset 1.
        """
        return 1


def cloud_event(heartbeat_field: str = "heartbeat") -> dict:
    """파서 테스트에 사용할 정상 CloudEvents 1.0 객체를 생성한다.

    입력:
        heartbeat_field: heartbeat 객체에 사용할 필드명. 구버전 오타의
            하위 호환 테스트에서는 ``hearbeat``를 전달한다.
    반환:
        장비 정보와 WARN 상태를 포함한 CloudEvent 사전.
    """
    return {
        "specversion": "1.0",
        "id": "event-1",
        "source": "/systems/SYS/devices/DEVICE-001/programs/PRODUCER",
        "type": "com.company.health.status.v1",
        "time": "2026-08-12T02:14:37Z",
        "data": {
            "sourceInfo": {
                "systemId": "SYS",
                "hostname": "host-01",
                "ipAddress": "192.0.2.1",
                "programName": "PRODUCER",
                "programVersion": "1.1",
                "instanceId": "DEVICE-001",
            },
            "status": {
                "level": "WARN",
                "code": "IMAGE_KAFKA_SEND_FAILED",
                "message": "Latest image delivery failed",
            },
            heartbeat_field: {
                "sequence": 7,
                "interval": 60,
                "generatedAt": "2026-08-12T02:14:37Z",
            },
        },
    }


class HeartbeatMessageTest(unittest.TestCase):
    """CloudEvent heartbeat 변환과 유효성 검사를 확인한다."""

    def test_parses_current_heartbeat_field(self) -> None:
        """현재 ``heartbeat`` 필드가 장비 상태로 정상 변환되는지 확인한다."""
        message = HeartbeatMessage.from_kafka_record(FakeRecord(cloud_event()))
        self.assertEqual("DEVICE-001", message.device_id)
        self.assertEqual("WARN", message.status_level)
        self.assertEqual(60, message.interval_seconds)

    def test_parses_legacy_hearbeat_typo(self) -> None:
        """과거 ``hearbeat`` 오타 메시지도 DB 복원용으로 읽는지 확인한다."""
        message = HeartbeatMessage.from_kafka_record(
            FakeRecord(cloud_event("hearbeat"))
        )
        self.assertEqual(7, message.sequence)

    def test_rejects_non_cloud_event(self) -> None:
        """CloudEvents 필수 필드가 없는 JSON을 거부하는지 확인한다."""
        with self.assertRaises(InvalidHeartbeat):
            HeartbeatMessage.from_kafka_record(FakeRecord({"status": "UP"}))


if __name__ == "__main__":
    unittest.main()
