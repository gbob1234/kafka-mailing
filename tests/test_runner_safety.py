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


class FakeTopicMetadata:
    """오류가 없는 Kafka topic metadata 대역."""

    error = None


class FakeMetadata:
    """healthcheck topic을 포함하는 Kafka metadata 대역."""

    def __init__(self) -> None:
        """테스트 topic metadata 사전을 생성한다."""
        self.topics = {"healthcheck": FakeTopicMetadata()}


class FakePosition:
    """consumer position과 partition 정보를 제공하는 대역."""

    def __init__(self, offset: int = 100) -> None:
        """고정 topic, partition과 주어진 offset을 저장한다."""
        self.topic = "healthcheck"
        self.partition = 0
        self.offset = offset


class FakeHealthyConsumer:
    """broker 왕복 확인과 watermark 조회가 성공하는 consumer 대역."""

    def __init__(self, high: int = 100) -> None:
        """테스트에서 반환할 high watermark를 저장한다."""
        self.high = high
        self.position_value = FakePosition()

    def list_topics(self, topic, timeout):
        """정상 topic metadata를 반환한다."""
        return FakeMetadata()

    def assignment(self):
        """할당된 partition 한 개를 반환한다."""
        return [self.position_value]

    def position(self, assignment):
        """할당 partition의 현재 consumer position을 반환한다."""
        return [self.position_value]

    def get_watermark_offsets(self, position, timeout, cached):
        """low=0과 설정된 high watermark를 반환한다."""
        return 0, self.high


class FakeBrokenConsumer(FakeHealthyConsumer):
    """broker metadata 요청이 실패하는 consumer 대역."""

    def list_topics(self, topic, timeout):
        """Kafka broker 연결 실패를 흉내 낸다."""
        raise RuntimeError("broker unavailable")


class RunnerSafetyTest(unittest.TestCase):
    """poll 지연 및 backlog 상황의 미수신 오판 방지 동작을 검증한다."""

    def _notifier(self) -> HeartbeatNotifier:
        """외부 Kafka 연결 없이 안전장치를 시험할 notifier를 만든다."""
        notifier = HeartbeatNotifier.__new__(HeartbeatNotifier)
        notifier._settings = SimpleNamespace(
            kafka_poll_delay_guard_seconds=10.0,
            stale_guard_recovery_seconds=30.0,
            heartbeat_stale_after_seconds=180.0,
            kafka_health_check_interval_seconds=10.0,
            kafka_health_check_timeout_seconds=3.0,
            kafka_health_max_age_seconds=30.0,
            kafka_recovery_stabilization_seconds=30.0,
            kafka_topic="healthcheck",
        )
        notifier._last_poll_completed_at = time.monotonic()
        notifier._stale_suppressed_until = 0.0
        notifier._last_stale_guard_log_at = 0.0
        notifier._last_kafka_health_check_at = 0.0
        notifier._last_kafka_health_success_at = time.monotonic()
        notifier._kafka_healthy = True
        notifier._kafka_ready_at = 0.0
        notifier._last_known_lag = 0
        notifier._kafka_health_reason = "정상"
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

    def test_broker_failure_suppresses_all_device_stale_notifications(self) -> None:
        """Kafka broker 장애 시 장비별 미수신 알림을 만들지 않는지 확인한다."""
        notifier = self._notifier()
        notifier._consumer = FakeBrokenConsumer()
        notifier._refresh_kafka_health(force=True)
        notifier._notify_stale_devices()

        self.assertFalse(notifier._kafka_healthy)
        self.assertEqual([], notifier._notification_repository.calls)
        self.assertEqual([], notifier._state_repository.marked)

    def test_backlog_and_recovery_stabilization_block_stale_checks(self) -> None:
        """lag 해소와 안정화가 끝날 때까지 미수신 판정을 막는지 확인한다."""
        notifier = self._notifier()
        notifier._consumer = FakeHealthyConsumer(high=120)
        notifier._refresh_kafka_health(force=True)
        self.assertEqual(20, notifier._last_known_lag)
        self.assertIn("backlog", notifier._stale_block_reason(time.monotonic()))

        notifier._consumer.high = 100
        notifier._refresh_kafka_health(force=True)
        self.assertEqual(0, notifier._last_known_lag)
        self.assertIn("안정화", notifier._stale_block_reason(time.monotonic()))

        notifier._kafka_ready_at = 0.0
        self.assertIsNone(notifier._stale_block_reason(time.monotonic()))


if __name__ == "__main__":
    unittest.main()
