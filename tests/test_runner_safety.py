from __future__ import annotations

import importlib.util
import sys
from types import SimpleNamespace
from types import ModuleType
import time
import unittest
from dataclasses import replace

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
        self.saved = []

    def save(self, **kwargs) -> None:
        """저장 요청 인자를 테스트 검증용으로 기록한다."""
        self.saved.append(kwargs)

    def mark_stale(self, device_id: str) -> None:
        """미수신으로 표시된 장비 ID를 기록한다."""
        self.marked.append(device_id)


class FakeEquipmentStatusProvider:
    """고정 MES 상태 판정 결과를 반환하는 대역."""

    def __init__(self, allowed: bool = True, status: str = "STAB") -> None:
        """알림 허용 여부와 MAIN_STAT_CD를 저장한다."""
        self.allowed = allowed
        self.status = status

    def alert_decision(self, device_id: str):
        """설정된 장비 상태 판정 결과를 반환한다."""
        return SimpleNamespace(
            allowed=self.allowed,
            status_code=self.status,
            reason="test",
        )


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

    def __init__(
        self,
        high: int = 100,
        position: int = 100,
        committed: int | None = None,
    ) -> None:
        """watermark, 현재 position과 저장된 group offset을 준비한다."""
        self.high = high
        self.position_value = FakePosition(position)
        self.committed_offset = committed

    def list_topics(self, topic, timeout):
        """정상 topic metadata를 반환한다."""
        return FakeMetadata()

    def assignment(self):
        """할당된 partition 한 개를 반환한다."""
        return [self.position_value]

    def position(self, assignment):
        """할당 partition의 현재 consumer position을 반환한다."""
        return [self.position_value]

    def committed(self, partitions, timeout):
        """consumer group에 저장된 offset 또는 미확인 값을 반환한다."""
        offset = -1001 if self.committed_offset is None else self.committed_offset
        return [FakePosition(offset)]

    def get_watermark_offsets(self, position, timeout, cached):
        """low=0과 설정된 high watermark를 반환한다."""
        return 0, self.high


class FakeBrokenConsumer(FakeHealthyConsumer):
    """broker metadata 요청이 실패하는 consumer 대역."""

    def list_topics(self, topic, timeout):
        """Kafka broker 연결 실패를 흉내 낸다."""
        raise RuntimeError("broker unavailable")


class FakeCommitConsumer:
    """처리 완료 offset commit 호출을 기록하는 consumer 대역."""

    def __init__(self) -> None:
        self.commits = []

    def commit(self, message, asynchronous):
        self.commits.append((message, asynchronous))


class RunnerSafetyTest(unittest.TestCase):
    """poll 지연 및 backlog 상황의 미수신 오판 방지 동작을 검증한다."""

    def _notifier(self) -> HeartbeatNotifier:
        """외부 Kafka 연결 없이 안전장치를 시험할 notifier를 만든다."""
        notifier = HeartbeatNotifier.__new__(HeartbeatNotifier)
        notifier._settings = SimpleNamespace(
            kafka_poll_delay_guard_seconds=10.0,
            heartbeat_stale_after_seconds=180.0,
            kafka_health_check_interval_seconds=10.0,
            kafka_health_check_timeout_seconds=3.0,
            kafka_health_max_age_seconds=30.0,
            kafka_topic="healthcheck",
            kafka_auto_offset_reset="latest",
        )
        notifier._last_poll_completed_at = time.monotonic()
        notifier._catchup_targets = {}
        notifier._assigned_keys = {("healthcheck", 0)}
        notifier._last_stale_guard_log_at = 0.0
        notifier._last_kafka_health_check_at = 0.0
        notifier._last_kafka_health_success_at = time.monotonic()
        notifier._kafka_healthy = True
        notifier._last_known_lag = 0
        notifier._kafka_health_reason = "정상"
        notifier._notification_repository = RecordingNotificationRepository()
        notifier._state_repository = RecordingStateRepository()
        notifier._equipment_status = FakeEquipmentStatusProvider()
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

    def test_poll_delay_does_not_block_stale_check(self) -> None:
        """poll 지연은 로그만 남기고 정상 연결에서 미수신 경고를 허용한다."""
        notifier = self._notifier()
        notifier._last_poll_completed_at = time.monotonic() - 20
        notifier._observe_poll_delay()
        self.assertIsNone(notifier._stale_block_reason(time.monotonic()))

    def test_stale_notification_waits_until_snapshot_consumed(self) -> None:
        """시작 목표 처리 전에는 보류하고 처리 뒤에는 바로 경고한다."""
        notifier = self._notifier()
        notifier._catchup_targets = {("healthcheck", 0): 120}
        notifier._notify_stale_devices()
        self.assertEqual([], notifier._notification_repository.calls)

        notifier._catchup_targets = {}
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

    def test_inactive_mes_status_suppresses_device_notification(self) -> None:
        """MES 상태가 STAB/NECK가 아니면 미수신 알림을 등록하지 않는다."""
        notifier = self._notifier()
        notifier._equipment_status = FakeEquipmentStatusProvider(
            allowed=False,
            status="IDLE",
        )

        notifier._notify_stale_devices()

        self.assertEqual([], notifier._notification_repository.calls)
        self.assertEqual([], notifier._state_repository.marked)

    def test_alert_is_sent_when_equipment_later_enters_stab(self) -> None:
        """억제됐던 비정상 상태는 MES가 STAB이 되면 다음 heartbeat에서 알린다."""
        notifier = self._notifier()
        notifier._consumer = FakeCommitConsumer()
        notifier._equipment_status = FakeEquipmentStatusProvider(
            allowed=True,
            status="STAB",
        )

        notifier._process(FakeRecord(cloud_event()))

        self.assertEqual(
            [("DEVICE-001", "ALERT")],
            notifier._notification_repository.calls,
        )
        self.assertTrue(notifier._devices["DEVICE-001"].status_alert_notified)

    def test_recovery_is_not_sent_after_suppressed_alert(self) -> None:
        """최초 경고를 보내지 않았다면 UP 전환에도 복구 메일을 보내지 않는다."""
        notifier = self._notifier()
        notifier._consumer = FakeCommitConsumer()
        notifier._equipment_status = FakeEquipmentStatusProvider(
            allowed=True,
            status="STAB",
        )
        payload = cloud_event()
        payload["data"]["status"] = {
            "level": "UP",
            "code": "OK",
            "message": "running",
        }

        notifier._process(FakeRecord(payload))

        self.assertEqual([], notifier._notification_repository.calls)
        self.assertFalse(notifier._devices["DEVICE-001"].status_alert_notified)

    def test_recovery_snapshot_does_not_follow_new_messages(self) -> None:
        """복구 목표 도달 시 새 메시지의 lag가 남아도 판정을 재개한다."""
        notifier = self._notifier()
        notifier._mark_kafka_unhealthy("connection lost")
        notifier._consumer = FakeHealthyConsumer(high=120)
        notifier._refresh_kafka_health(force=True)
        self.assertEqual(20, notifier._last_known_lag)
        self.assertIn("backlog", notifier._stale_block_reason(time.monotonic()))

        notifier._consumer.high = 140
        notifier._consumer.position_value.offset = 120
        notifier._refresh_kafka_health(force=True)
        self.assertEqual(20, notifier._last_known_lag)
        self.assertIsNone(notifier._stale_block_reason(time.monotonic()))

    def test_normal_lag_does_not_block_notifications(self) -> None:
        """평상시 지속 lag도 미수신 경고를 막지 않는다."""
        notifier = self._notifier()
        notifier._consumer = FakeHealthyConsumer(high=10000)
        notifier._refresh_kafka_health(force=True)
        notifier._notify_stale_devices()
        self.assertEqual([("DEVICE-001", "MISSING")], notifier._notification_repository.calls)

    def test_startup_and_second_disconnect_create_new_snapshots(self) -> None:
        """시작 목표는 고정하고 재단절 뒤 새 목표를 확보한다."""
        notifier = self._notifier()
        notifier._kafka_healthy = False
        notifier._catchup_targets = None
        notifier._consumer = FakeHealthyConsumer(high=120)
        notifier._refresh_kafka_health(force=True)
        notifier._consumer.high = 200
        notifier._refresh_kafka_health(force=True)
        self.assertEqual({("healthcheck", 0): 120}, notifier._catchup_targets)
        notifier._mark_kafka_unhealthy("second disconnect")
        notifier._refresh_kafka_health(force=True)
        self.assertEqual({("healthcheck", 0): 200}, notifier._catchup_targets)

    def test_monitor_threshold_ignores_producer_interval(self) -> None:
        """producer interval이 커도 모니터링의 180초 기준만 사용한다."""
        notifier = self._notifier()
        state = notifier._devices["DEVICE-001"]
        state.heartbeat = replace(state.heartbeat, interval_seconds=600)
        state.last_seen_at = time.time() - 179
        notifier._notify_stale_devices()
        self.assertEqual([], notifier._notification_repository.calls)
        state.last_seen_at = time.time() - 181
        notifier._notify_stale_devices()
        notifier._notify_stale_devices()
        self.assertEqual([("DEVICE-001", "MISSING")], notifier._notification_repository.calls)

    def test_all_partition_targets_must_complete(self) -> None:
        """파티션 하나가 밀려 있으면 다른 파티션이 완료해도 보류한다."""
        notifier = self._notifier()
        notifier._mark_kafka_unhealthy("startup")
        consumer = FakeHealthyConsumer()
        first, second = FakePosition(90), FakePosition(40)
        second.partition = 1
        consumer.assignment = lambda: [first, second]
        consumer.position = lambda assignment: [first, second]
        consumer.get_watermark_offsets = lambda position, **kwargs: (
            0, 100 if position.partition == 0 else 50
        )
        notifier._consumer = consumer
        notifier._refresh_kafka_health(force=True)
        first.offset = 100
        notifier._refresh_kafka_health(force=True)
        self.assertEqual({("healthcheck", 1): 50}, notifier._catchup_targets)
        second.offset = 50
        notifier._refresh_kafka_health(force=True)
        self.assertIsNone(notifier._stale_block_reason(time.monotonic()))

    def test_assignment_change_resets_completed_gate(self) -> None:
        """동일 파티션 재할당도 새 목표를 확보할 때까지 미수신을 보류한다."""
        notifier = self._notifier()
        notifier._on_assignment_change(None, [])
        self.assertIsNone(notifier._catchup_targets)
        self.assertFalse(notifier._kafka_healthy)

    def test_late_receipt_logs_gap_without_retroactive_missing(self) -> None:
        """미경고 공백을 뒤늦게 발견하면 로그만 기록한다."""
        notifier = self._notifier()
        notifier._consumer = FakeCommitConsumer()
        payload = cloud_event()
        payload["data"]["status"]["level"] = "UP"
        with self.assertLogs("heartbeat_mailer.runner", level="WARNING") as logs:
            notifier._process(FakeRecord(payload))
        self.assertIn("수신 공백", " ".join(logs.output))
        self.assertEqual([], notifier._notification_repository.calls)

    def test_warn_receipt_after_missing_is_not_up_recovery(self) -> None:
        """WARN 메시지도 수신 재개이며 UP 복구로 표현하지 않는다."""
        notifier = self._notifier()
        notifier._consumer = FakeCommitConsumer()
        state = notifier._devices["DEVICE-001"]
        state.stale_notified = True
        state.status_alert_notified = True
        notifier._process(FakeRecord(cloud_event()))
        self.assertEqual([("DEVICE-001", "RECEIVING")], notifier._notification_repository.calls)
        self.assertFalse(notifier._devices["DEVICE-001"].stale_notified)

    def test_invalid_position_uses_committed_group_offset(self) -> None:
        """현재 position 미확인 시 저장된 group offset으로 lag를 계산한다."""
        notifier = self._notifier()
        notifier._consumer = FakeHealthyConsumer(
            high=120,
            position=-1001,
            committed=100,
        )

        notifier._refresh_kafka_health(force=True)

        self.assertTrue(notifier._kafka_healthy)
        self.assertEqual(20, notifier._last_known_lag)

    def test_missing_position_and_commit_uses_latest_watermark(self) -> None:
        """저장 offset도 없으면 latest 정책의 high watermark를 사용한다."""
        notifier = self._notifier()
        notifier._consumer = FakeHealthyConsumer(
            high=120,
            position=-1001,
            committed=None,
        )

        notifier._refresh_kafka_health(force=True)

        self.assertTrue(notifier._kafka_healthy)
        self.assertEqual(0, notifier._last_known_lag)


if __name__ == "__main__":
    unittest.main()
