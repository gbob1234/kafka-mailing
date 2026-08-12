from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from .config import Settings
from .message import HeartbeatMessage, InvalidHeartbeat
from .notification_worker import MailNotificationWorker
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


class HeartbeatNotifier:
    """Kafka heartbeat 소비, 상태 저장, 장애·복구 알림을 조정한다."""

    def __init__(
        self,
        settings: Settings,
        state_repository: DeviceStateRepository | None = None,
        notification_repository: NotificationQueueRepository | None = None,
        notification_worker: MailNotificationWorker | None = None,
    ) -> None:
        """Kafka consumer와 상태 저장소를 초기화하고 기존 상태를 복원한다.

        입력:
            settings: Kafka, SMTP, SQLite 및 알림 기준 설정.
            state_repository: 테스트나 PostgreSQL 전환 시 주입할 저장소.
                지정하지 않으면 설정된 경로의 SQLite 저장소를 생성한다.
            notification_repository: consumer가 알림을 등록할 영속 큐 저장소.
            notification_worker: 테스트에서 주입할 별도 SMTP worker.
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
            settings.sqlite_path
        )
        self._notification_repository = (
            notification_repository
            or SQLiteNotificationQueueRepository(settings.sqlite_path)
        )
        self._notification_worker = notification_worker or MailNotificationWorker(
            settings,
            SQLiteNotificationQueueRepository(settings.sqlite_path),
        )
        self._running = True
        self._last_poll_completed_at = time.monotonic()
        self._stale_suppressed_until = (
            self._last_poll_completed_at
            + settings.stale_guard_recovery_seconds
        )
        self._last_stale_guard_log_at = 0.0
        self._last_lag_log_at = 0.0
        self._devices: dict[str, DeviceState] = {
            state.heartbeat.device_id: DeviceState(
                heartbeat=state.heartbeat,
                last_seen_at=state.last_seen_at,
                status_signature=state.status_signature,
                stale_notified=state.stale_notified,
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
        self._notification_worker.start()
        logger.info("Kafka topic 구독 시작: %s", self._settings.kafka_topic)

        try:
            while self._running:
                record = self._consumer.poll(timeout=1.0)
                self._observe_poll_delay()
                if record is None:
                    self._notify_stale_devices()
                    self._maybe_log_consumer_lag()
                    continue
                if record.error():
                    self._handle_error(record)
                    continue
                self._process(record)
                self._notify_stale_devices()
                self._maybe_log_consumer_lag()
        finally:
            self._notification_worker.stop()
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
        ):
            notification = "ALERT"
            detail = "장비가 비정상 상태를 보고했습니다."
        elif (
            previous is not None
            and previous.status_signature[0] != "UP"
            and heartbeat.status_level == "UP"
        ):
            notification = "RECOVERY"
            detail = "장비가 UP 상태로 복구되었습니다."

        if notification:
            added = self._notification_repository.enqueue(
                heartbeat, notification, detail
            )
            logger.info(
                "메일 알림 큐 등록: device=%s type=%s added=%s",
                heartbeat.device_id,
                notification,
                added,
            )

        last_seen_at = time.time()
        state = DeviceState(
            heartbeat=heartbeat,
            last_seen_at=last_seen_at,
            status_signature=heartbeat.status_signature(),
        )
        self._state_repository.save(
            heartbeat=heartbeat,
            last_seen_at=last_seen_at,
            stale_notified=False,
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
            added = self._notification_repository.enqueue(
                state.heartbeat, "MISSING", detail
            )
            self._state_repository.mark_stale(state.heartbeat.device_id)
            state.stale_notified = True
            logger.warning(
                "heartbeat 미수신 알림 큐 등록: device=%s added=%s elapsed=%ss",
                state.heartbeat.device_id,
                added,
                int(elapsed),
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
        """설정된 주기마다 현재 할당 partition의 consumer lag를 기록한다.

        입력:
            없음. consumer assignment, position과 cached watermark를 사용한다.
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
        try:
            assignment = self._consumer.assignment()
            if not assignment:
                logger.info("Kafka consumer lag: partition이 아직 할당되지 않았습니다.")
                return
            positions = self._consumer.position(assignment)
            lags: list[str] = []
            total_lag = 0
            for position in positions:
                _, high = self._consumer.get_watermark_offsets(
                    position, cached=True
                )
                lag = max(0, high - max(0, position.offset))
                total_lag += lag
                lags.append(f"{position.topic}[{position.partition}]={lag}")
            logger.info(
                "Kafka consumer lag: total=%s partitions=%s",
                total_lag,
                ", ".join(lags),
            )
            if total_lag > 0:
                self._stale_suppressed_until = max(
                    self._stale_suppressed_until,
                    now + self._settings.stale_guard_recovery_seconds,
                )
                logger.warning(
                    "Kafka backlog이 남아 있어 미수신 판정을 보류합니다: "
                    "lag=%s guard=%.1fs",
                    total_lag,
                    self._settings.stale_guard_recovery_seconds,
                )
        except Exception:
            logger.warning("Kafka consumer lag 조회에 실패했습니다.", exc_info=True)

    @staticmethod
    def _handle_error(record: Message) -> None:
        """Kafka poll 결과에 포함된 오류를 분류한다.

        입력:
            record: ``error()`` 정보를 가진 Kafka 메시지.
        반환:
            partition EOF는 디버그 로그만 남기고 반환한다.
        예외:
            KafkaException: partition EOF 이외의 Kafka 오류인 경우.
        """
        error = record.error()
        if error.code() == KafkaError._PARTITION_EOF:
            logger.debug("Partition 끝에 도달했습니다: %s", error)
            return
        raise KafkaException(error)
