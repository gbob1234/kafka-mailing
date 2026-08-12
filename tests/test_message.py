from __future__ import annotations

import json
import unittest

from heartbeat_mailer.message import HeartbeatMessage, InvalidHeartbeat


class FakeRecord:
    def __init__(self, payload: dict, key: str = "DEVICE-001") -> None:
        self._payload = json.dumps(payload).encode("utf-8")
        self._key = key.encode("utf-8")

    def value(self) -> bytes:
        return self._payload

    def key(self) -> bytes:
        return self._key

    def topic(self) -> str:
        return "healthcheck"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 1


def cloud_event(heartbeat_field: str = "heartbeat") -> dict:
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
    def test_parses_current_heartbeat_field(self) -> None:
        message = HeartbeatMessage.from_kafka_record(FakeRecord(cloud_event()))
        self.assertEqual("DEVICE-001", message.device_id)
        self.assertEqual("WARN", message.status_level)
        self.assertEqual(60, message.interval_seconds)

    def test_parses_legacy_hearbeat_typo(self) -> None:
        message = HeartbeatMessage.from_kafka_record(
            FakeRecord(cloud_event("hearbeat"))
        )
        self.assertEqual(7, message.sequence)

    def test_rejects_non_cloud_event(self) -> None:
        with self.assertRaises(InvalidHeartbeat):
            HeartbeatMessage.from_kafka_record(FakeRecord({"status": "UP"}))


if __name__ == "__main__":
    unittest.main()
