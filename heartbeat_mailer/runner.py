from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass
from types import FrameType

from confluent_kafka import Consumer, KafkaError, Message

from .config import Settings
from .message import HeartbeatMessage, InvalidHeartbeat
from .notification_worker import MailNotificationWorker
from .equipment_status import (
    EquipmentStatusProvider,
    OracleEquipmentStatusCache,
)
from .storage import (
    DeviceStateRepository,
    NotificationQueueRepository,
    SQLiteDeviceStateRepository,
    SQLiteNotificationQueueRepository,
)


logger = logging.getLogger(__name__)


@dataclass
class DeviceState:
    """실행 중인 장비의 마지막 heartbeat와 알림 상태를 보관한다."""

    heartbeat: HeartbeatMessage
    last_seen_at: float
    status_signature: tuple[str, str]
    stale_notified: bool = False
    status_alert_notified: bool = False


class HeartbeatNotifier:
    """Kafka heartbeat 소비, 상태 저장, 장애·복구 알림을 조정한다."""

    def __init__(
        self,
        settings: Settings,
        state_repository: DeviceStateRepository | None = None,
        notification_repository: NotificationQueueRepository | None = None,
        notification_worker: MailNotificationWorker | None = None,
        equipment_status_provider: EquipmentStatusProvider | None = None,
    ) -> None:
        """Kafka consumer와 상태 저장소를 초기화하고 기존 상태를 복원한다.

        입력:
            settings: Kafka, SMTP, SQLite 및 알림 기준 설정.
            state_repository: 테스트나 PostgreSQL 전환 시 주입할 저장소.
                지정하지 않으면 설정된 경로의 SQLite 저장소를 생성한다.
            notification_repository: consumer가 알림을 등록할 영속 큐 저장소.
            notification_worker: 테스트에서 주입할 별도 SMTP worker.
            equipment_status_provider: MES 상태 조회 구현. 없으면 Oracle 캐시를 생성한다.
        반환:
            없음.
        예외:
            KafkaException: Kafka consumer 설정이 유효하지 않은 경우.
            sqlite3.Error: SQLite 연결 또는 초기 스키마 생성에 실패한 경우.
        """
        self._settings = settings
        consumer_config = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_group_id,
            "auto.offset.reset": settings.kafka_auto_offset_reset,
            "enable.auto.commit": False,
            "security.protocol": settings.kafka_security_protocol,
            "sasl.mechanism": settings.kafka_sasl_mechanism,
            "sasl.username": settings.kafka_sasl_username,
            "sasl.password": settings.kafka_sasl_password,
        }
        if settings.kafka_ssl_ca_location:
            consumer_config["ssl.ca.location"] = settings.kafka_ssl_ca_location
        self._consumer = Consumer(consumer_config)
        self._state_repository = state_repository or SQLiteDeviceStateRepository(
            settings.sqlite_path,
            settings.sqlite_journal_mode,
        )
        self._notification_repository = (
            notification_repository
            or SQLiteNotificationQueueRepository(
                settings.sqlite_path,
                settings.sqlite_journal_mode,
            )
        )
        self._notification_worker = notification_worker or MailNotificationWorker(
            settings,
            SQLiteNotificationQueueRepository(
                settings.sqlite_path,
                settings.sqlite_journal_mode,
            ),
        )
        self._equipment_status = (
            equipment_status_provider or OracleEquipmentStatusCache(settings)
        )
        self._running = True
        self._last_poll_completed_at = time.monotonic()
        self._stale_suppressed_until = (
            self._last_poll_completed_at
            + settings.stale_guard_recovery_seconds
        )
        self._last_stale_guard_log_at = 0.0
        self._last_lag_log_at = 0.0
        self._last_kafka_health_check_at = 0.0
        self._last_kafka_health_success_at = 0.0
        self._kafka_healthy = False
        self._kafka_ready_at = float("inf")
        self._last_known_lag: int | None = None
        self._kafka_health_reason = "Kafka 상태 확인 전"
        self._devices: dict[str, DeviceState] = {
            state.heartbeat.device_id: DeviceState(
                heartbeat=state.heartbeat,
                last_seen_at=state.last_seen_at,
                status_signature=state.status_signature,
                stale_notified=state.stale_notified,
                status_alert_notified=state.status_alert_notified,
            )
            for state in self._state_repository.load_all()
        }
        logger.info(
            "저장된 장비 상태를 불러왔습니다: count=%s",
            len(self._devices),
        )

    def stop(self, _signum: int, _frame: FrameType | None) -> None:
        """운영체제 종료 신호를 받아 소비 루프의 정상 종료를 요청한다.

        입력:
            _signum: 수신한 signal 번호. 현재 로직에서는 사용하지 않는다.
            _frame: signal 발생 시점의 stack frame. 사용하지 않는다.
        반환:
            없음. 실행 플래그를 ``False``로 변경한다.
        """
        logger.info("종료 신호를 받았습니다.")
        self._running = False

    def run(self) -> None:
        """Kafka topic을 구독하고 종료 신호까지 heartbeat를 처리한다.

        입력:
            없음.
        반환:
            없음. 루프가 끝나면 consumer와 상태 저장소를 닫는다.
        예외:
            KafkaException: 복구할 수 없는 Kafka 오류가 발생한 경우.
        """
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        self._consumer.subscribe([self._settings.kafka_topic])
        self._equipment_status.start()
        self._notification_worker.start()
        logger.info("Kafka topic 구독 시작: %s", self._settings.kafka_topic)

        try:
            while self._running:
                record = self._consumer.poll(timeout=1.0)
                self._observe_poll_delay()
                self._refresh_kafka_health()
                self._maybe_log_consumer_lag()
                if record is None:
                    self._notify_stale_devices()
                    continue
                if record.error():
                    self._handle_error(record)
                    continue
                self._process(record)
                self._notify_stale_devices()
        finally:
            self._notification_worker.stop()
            self._equipment_status.stop()
            self._consumer.close()
            self._notification_repository.close()
            self._state_repository.close()
            logger.info("Kafka consumer가 종료되었습니다.")

    def _process(self, record: Message) -> None:
        """Kafka 메시지 한 건을 검증하고 알림·저장·commit 순서로 처리한다.

        입력:
            record: 처리할 confluent-kafka ``Message`` 객체.
        반환:
            없음. 성공하면 SQLite 저장 후 해당 offset을 동기 commit한다.
        처리 규칙:
            잘못된 CloudEvent는 로그 후 건너뛰며, 메일 발송이나 상태 저장이
            실패하면 정상 처리된 것으로 commit하지 않는다.
        """
        try:
            heartbeat = HeartbeatMessage.from_kafka_record(record)
        except InvalidHeartbeat:
            logger.exception(
                "잘못된 heartbeat 메시지를 건너뜁니다: topic=%s partition=%s offset=%s",
                record.topic(),
                record.partition(),
                record.offset(),
            )
            self._consumer.commit(message=record, asynchronous=False)
            return

        if heartbeat.key and heartbeat.key != heartbeat.device_id:
            logger.warning(
                "Kafka key와 instanceId가 다릅니다: key=%s instanceId=%s",
                heartbeat.key,
                heartbeat.device_id,
            )

        previous = self._devices.get(heartbeat.device_id)
        notification: str | None = None
        detail = ""
        status_alert_notified = (
            previous.status_alert_notified if previous else False
        )
        if previous and previous.stale_notified:
            if heartbeat.status_level == "UP":
                notification = "RECOVERY"
                detail = "중단되었던 heartbeat 수신이 재개되었습니다."
            else:
                notification = "ALERT"
                detail = (
                    "heartbeat 수신은 재개되었지만 장비가 비정상 상태를 "
                    "보고했습니다."
                )
        elif heartbeat.status_level != "UP" and (
            previous is None
            or previous.status_signature != heartbeat.status_signature()
            or not previous.status_alert_notified
        ):
            notification = "ALERT"
            detail = "장비가 비정상 상태를 보고했습니다."
        elif (
            previous is not None
            and previous.status_signature[0] != "UP"
            and heartbeat.status_level == "UP"
            and previous.status_alert_notified
        ):
            notification = "RECOVERY"
            detail = "장비가 UP 상태로 복구되었습니다."

        if notification:
            allowed = self._enqueue_if_equipment_active(
                heartbeat, notification, detail
            )
            if notification == "ALERT":
                status_alert_notified = allowed
        if heartbeat.status_level == "UP":
            status_alert_notified = False

        last_seen_at = time.time()
        state = DeviceState(
            heartbeat=heartbeat,
            last_seen_at=last_seen_at,
            status_signature=heartbeat.status_signature(),
            status_alert_notified=status_alert_notified,
        )
        self._state_repository.save(
            heartbeat=heartbeat,
            last_seen_at=last_seen_at,
            stale_notified=False,
            status_alert_notified=status_alert_notified,
        )
        self._devices[heartbeat.device_id] = state
        self._consumer.commit(message=record, asynchronous=False)
        logger.info(
            "heartbeat 처리 및 commit 완료: device=%s status=%s topic=%s "
            "partition=%s offset=%s",
            heartbeat.device_id,
            heartbeat.status_level,
            heartbeat.topic,
            heartbeat.partition,
            heartbeat.offset,
        )

    def _notify_stale_devices(self) -> None:
        """마지막 수신 시각이 기준을 넘긴 장비에 미수신 알림을 보낸다.

        입력:
            없음. 메모리에 복원된 모든 장비 상태와 현재 시각을 사용한다.
        반환:
            없음. 큐 등록 성공 후 메모리와 SQLite의 미수신 표시를 갱신한다.
        """
        monotonic_now = time.monotonic()
        blocked_reason = self._stale_block_reason(monotonic_now)
        if blocked_reason is not None:
            if monotonic_now - self._last_stale_guard_log_at >= 10:
                logger.warning(
                    "장비 미수신 판정을 보류합니다: reason=%s",
                    blocked_reason,
                )
                self._last_stale_guard_log_at = monotonic_now
            return

        if monotonic_now < self._stale_suppressed_until:
            if monotonic_now - self._last_stale_guard_log_at >= 10:
                logger.warning(
                    "최근 Kafka poll 지연으로 미수신 판정을 보류합니다: "
                    "remaining=%.1fs",
                    self._stale_suppressed_until - monotonic_now,
                )
                self._last_stale_guard_log_at = monotonic_now
            return

        now = time.time()
        for state in self._devices.values():
            if state.stale_notified:
                continue
            elapsed = now - state.last_seen_at
            threshold = max(
                self._settings.heartbeat_stale_after_seconds,
                state.heartbeat.interval_seconds * 3,
            )
            if elapsed < threshold:
                continue
            detail = f"마지막 heartbeat 수신 후 {int(elapsed)}초가 지났습니다."
            if not self._enqueue_if_equipment_active(
                state.heartbeat, "MISSING", detail
            ):
                continue
            self._state_repository.mark_stale(state.heartbeat.device_id)
            state.stale_notified = True
            logger.warning(
                "heartbeat 미수신 알림 상태 기록: device=%s elapsed=%ss",
                state.heartbeat.device_id,
                int(elapsed),
            )

    def _enqueue_if_equipment_active(
        self,
        heartbeat: HeartbeatMessage,
        notification_type: str,
        detail: str,
    ) -> bool:
        """MES 상태가 STAB 또는 NECK인 경우에만 메일 큐에 등록한다.

        입력:
            heartbeat: 알림 대상 장비의 최신 heartbeat.
            notification_type: ALERT, RECOVERY 또는 MISSING.
            detail: 메일 본문에 포함할 판정 설명.
        반환:
            MES 조건을 충족해 알림 대상으로 인정했으면 ``True``.
        """
        decision = self._equipment_status.alert_decision(heartbeat.device_id)
        if not decision.allowed:
            logger.debug(
                "MES 상태 조건으로 메일 알림을 보류합니다: "
                "device=%s type=%s status=%s reason=%s",
                heartbeat.device_id,
                notification_type,
                decision.status_code,
                decision.reason,
            )
            return False
        added = self._notification_repository.enqueue(
            heartbeat, notification_type, detail
        )
        logger.info(
            "메일 알림 큐 등록: device=%s type=%s mesStatus=%s added=%s",
            heartbeat.device_id,
            notification_type,
            decision.status_code,
            added,
        )
        return True

    def _stale_block_reason(self, now: float) -> str | None:
        """현재 장비 미수신 판정을 수행해도 안전한지 판단한다.

        입력:
            now: monotonic clock 기준 현재 시각.
        반환:
            판정을 보류해야 하면 사유 문자열, 안전하면 ``None``.
        """
        if not self._kafka_healthy:
            return f"Kafka 비정상 ({self._kafka_health_reason})"
        health_age = now - self._last_kafka_health_success_at
        if health_age > self._settings.kafka_health_max_age_seconds:
            return f"Kafka 상태 확인 만료 ({health_age:.1f}초 전)"
        if self._last_known_lag is None:
            return "Kafka consumer lag 미확인"
        if self._last_known_lag > 0:
            return f"Kafka backlog 처리 중 (lag={self._last_known_lag})"
        if now < self._kafka_ready_at:
            return (
                "Kafka 복구 안정화 중 "
                f"({self._kafka_ready_at - now:.1f}초 남음)"
            )
        return None

    def _refresh_kafka_health(self, force: bool = False) -> None:
        """실제 broker 왕복 통신과 fresh watermark로 Kafka 상태를 갱신한다.

        입력:
            force: 설정된 점검 주기를 무시하고 즉시 실행할지 여부.
        반환:
            없음. 건강 상태, lag, 안정화 시각을 내부 필드에 기록한다.
        """
        now = time.monotonic()
        if (
            not force
            and now - self._last_kafka_health_check_at
            < self._settings.kafka_health_check_interval_seconds
        ):
            return
        self._last_kafka_health_check_at = now

        try:
            metadata = self._consumer.list_topics(
                topic=self._settings.kafka_topic,
                timeout=self._settings.kafka_health_check_timeout_seconds,
            )
            topic_metadata = metadata.topics.get(self._settings.kafka_topic)
            if topic_metadata is None:
                raise RuntimeError("구독 topic metadata 없음")
            topic_error = getattr(topic_metadata, "error", None)
            if topic_error is not None:
                code = topic_error.code() if hasattr(topic_error, "code") else 1
                if code:
                    raise RuntimeError(f"topic metadata 오류: {topic_error}")

            assignment = self._consumer.assignment()
            if not assignment:
                raise RuntimeError("consumer partition 미할당")
            positions = self._consumer.position(assignment)
            if len(positions) != len(assignment):
                raise RuntimeError("consumer position 조회 결과 불일치")

            unresolved = [
                position
                for position in positions
                if position.offset is None or position.offset < 0
            ]
            committed_offsets: dict[tuple[str, int], int | None] = {}
            if unresolved:
                committed = self._consumer.committed(
                    unresolved,
                    timeout=self._settings.kafka_health_check_timeout_seconds,
                )
                committed_offsets = {
                    (item.topic, item.partition): item.offset
                    for item in committed
                }

            total_lag = 0
            for position in positions:
                low, high = self._consumer.get_watermark_offsets(
                    position,
                    timeout=self._settings.kafka_health_check_timeout_seconds,
                    cached=False,
                )
                effective_offset = position.offset
                if effective_offset is None or effective_offset < 0:
                    effective_offset = committed_offsets.get(
                        (position.topic, position.partition)
                    )
                if effective_offset is None or effective_offset < 0:
                    reset_policy = (
                        self._settings.kafka_auto_offset_reset.lower()
                    )
                    if reset_policy in {"latest", "largest"}:
                        effective_offset = high
                    elif reset_policy in {"earliest", "smallest"}:
                        effective_offset = low
                    else:
                        raise RuntimeError(
                            "consumer position과 committed offset 미확인: "
                            f"{position.topic}[{position.partition}]"
                        )
                    logger.debug(
                        "저장된 consumer offset이 없어 reset 정책을 적용합니다: "
                        "topic=%s partition=%s policy=%s offset=%s",
                        position.topic,
                        position.partition,
                        reset_policy,
                        effective_offset,
                    )
                total_lag += max(0, high - effective_offset)
        except Exception as exc:
            self._mark_kafka_unhealthy(str(exc))
            return

        was_healthy = self._kafka_healthy
        previous_lag = self._last_known_lag
        self._kafka_healthy = True
        self._last_kafka_health_success_at = now
        self._last_known_lag = total_lag
        self._kafka_health_reason = "정상"

        if total_lag > 0:
            self._kafka_ready_at = float("inf")
            return
        if not was_healthy or previous_lag is None or previous_lag > 0:
            self._kafka_ready_at = (
                now + self._settings.kafka_recovery_stabilization_seconds
            )
            logger.info(
                "Kafka 연결 및 backlog 해소를 확인했습니다. 안정화 후 장비 "
                "미수신 판정을 재개합니다: stabilization=%.1fs",
                self._settings.kafka_recovery_stabilization_seconds,
            )

    def _mark_kafka_unhealthy(self, reason: str) -> None:
        """Kafka를 비정상 상태로 표시하여 장비별 미수신 판정을 차단한다.

        입력:
            reason: 로그와 보류 사유에 사용할 오류 설명.
        반환:
            없음.
        """
        changed = self._kafka_healthy or self._kafka_health_reason != reason
        self._kafka_healthy = False
        self._kafka_ready_at = float("inf")
        self._last_known_lag = None
        self._kafka_health_reason = reason
        if changed:
            logger.error(
                "Kafka broker 상태가 비정상입니다. 장비별 미수신 알림을 "
                "중지합니다: reason=%s",
                reason,
            )

    def _observe_poll_delay(self) -> None:
        """연속 poll 완료 간격을 측정하고 지연 시 미수신 판정을 보류한다.

        입력:
            없음. monotonic clock의 이전 poll 완료 시각을 사용한다.
        반환:
            없음. 기준 초과 시 보류 종료 시각을 갱신한다.
        """
        now = time.monotonic()
        elapsed = now - self._last_poll_completed_at
        self._last_poll_completed_at = now
        if elapsed <= self._settings.kafka_poll_delay_guard_seconds:
            return
        self._stale_suppressed_until = max(
            self._stale_suppressed_until,
            now + self._settings.stale_guard_recovery_seconds,
        )
        logger.warning(
            "Kafka poll 지연을 감지했습니다. 미수신 판정을 일시 보류합니다: "
            "delay=%.1fs guard=%.1fs",
            elapsed,
            self._settings.stale_guard_recovery_seconds,
        )

    def _maybe_log_consumer_lag(self) -> None:
        """설정된 주기마다 Kafka 건강 상태와 최근 consumer lag를 기록한다.

        입력:
            없음. 최근 broker health check 결과를 사용한다.
        반환:
            없음. 조회 오류는 warning 로그만 남긴다.
        """
        now = time.monotonic()
        if (
            now - self._last_lag_log_at
            < self._settings.kafka_lag_log_interval_seconds
        ):
            return
        self._last_lag_log_at = now
        logger.info(
            "Kafka 상태: healthy=%s lag=%s reason=%s",
            self._kafka_healthy,
            self._last_known_lag,
            self._kafka_health_reason,
        )

    def _handle_error(self, record: Message) -> None:
        """Kafka poll 결과에 포함된 오류를 분류한다.

        입력:
            record: ``error()`` 정보를 가진 Kafka 메시지.
        반환:
            partition EOF는 디버그 로그만 남기고 반환한다.
        처리:
            partition EOF는 무시하고, 그 외 오류는 Kafka 비정상 상태로 기록하여
            장비별 미수신 알림을 보류한다.
        """
        error = record.error()
        if error.code() == KafkaError._PARTITION_EOF:
            logger.debug("Partition 끝에 도달했습니다: %s", error)
            return
        self._mark_kafka_unhealthy(str(error))
