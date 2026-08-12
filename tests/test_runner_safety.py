from __future__ import annotations

import importlib.util
import sys
from types import SimpleNamespace
from types import ModuleType
import time
import unittest

from heartbeat_mailer.message import HeartbeatMessage

if importlib.util.find_spec("confluent_kafka") is None:
    kafka_stub = ModuleType("confluent_kafka")
    kafka_stub.Consumer = object
    kafka_stub.Message = object
    kafka_stub.KafkaException = RuntimeError
    kafka_stub.KafkaError = SimpleNamespace(_PARTITION_EOF=-191)
    sys.modules["confluent_kafka"] = kafka_stub

from heartbeat_mailer.runner import DeviceState, HeartbeatNotifier
from tests.test_message import FakeRecord, cloud_event


class RecordingNotificationRepository:
    """미수신 알림 등록 횟수를 기록하는 저장소 대역."""

    def __init__(self) -> None:
        """빈 호출 목록을 준비한다."""
        self.calls: list[tuple[str, str]] = []

    def enqueue(self, heartbeat, notification_type, detail) -> bool:
        """등록 요청의 장비 ID와 알림 유형을 기록한다."""
        self.calls.append((heartbeat.device_id, notification_type))
        return True


class RecordingStateRepository:
    """미수신 상태 갱신 장비를 기록하는 저장소 대역."""

    def __init__(self) -> None:
        """빈 장비 ID 목록을 준비한다."""
        self.marked: list[str] = []

    def mark_stale(self, device_id: str) -> None:
        """미수신으로 표시된 장비 ID를 기록한다."""
        self.marked.append(device_id)


class RunnerSafetyTest(unittest.TestCase):
    """poll 지연 및 backlog 상황의 미수신 오판 방지 동작을 검증한다."""

    def _notifier(self) -> HeartbeatNotifier:
        """외부 Kafka 연결 없이 안전장치를 시험할 notifier를 만든다."""
        notifier = HeartbeatNotifier.__new__(HeartbeatNotifier)
        notifier._settings = SimpleNamespace(
            kafka_poll_delay_guard_seconds=10.0,
            stale_guard_recovery_seconds=30.0,
            heartbeat_stale_after_seconds=180.0,
        )
        notifier._last_poll_completed_at = time.monotonic()
        notifier._stale_suppressed_until = 0.0
        notifier._last_stale_guard_log_at = 0.0
        notifier._notification_repository = RecordingNotificationRepository()
        notifier._state_repository = RecordingStateRepository()
        heartbeat = HeartbeatMessage.from_kafka_record(
            FakeRecord(cloud_event())
        )
        notifier._devices = {
            heartbeat.device_id: DeviceState(
                heartbeat=heartbeat,
                last_seen_at=time.time() - 500,
                status_signature=heartbeat.status_signature(),
            )
        }
        return notifier

    def test_poll_delay_starts_stale_guard(self) -> None:
        """긴 poll 간격이 미수신 판정 보류 시간을 설정하는지 확인한다."""
        notifier = self._notifier()
        notifier._last_poll_completed_at = time.monotonic() - 20
        notifier._observe_poll_delay()
        self.assertGreater(notifier._stale_suppressed_until, time.monotonic())

    def test_stale_notification_waits_until_guard_expires(self) -> None:
        """보류 중에는 미수신을 등록하지 않고 종료 후에만 등록하는지 확인한다."""
        notifier = self._notifier()
        notifier._stale_suppressed_until = time.monotonic() + 30
        notifier._notify_stale_devices()
        self.assertEqual([], notifier._notification_repository.calls)

        notifier._stale_suppressed_until = 0
        notifier._notify_stale_devices()
        self.assertEqual(
            [("DEVICE-001", "MISSING")],
            notifier._notification_repository.calls,
        )
        self.assertEqual(["DEVICE-001"], notifier._state_repository.marked)


if __name__ == "__main__":
    unittest.main()
