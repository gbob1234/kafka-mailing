from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from .config import Settings
from .mailer import SmtpMailer
from .message import HeartbeatMessage, InvalidHeartbeat
from .storage import (
    DeviceStateRepository,
    SQLiteDeviceStateRepository,
)


logger = logging.getLogger(__name__)


@dataclass
class DeviceState:
    heartbeat: HeartbeatMessage
    last_seen_at: float
    status_signature: tuple[str, str]
    stale_notified: bool = False


class HeartbeatNotifier:
    def __init__(
        self,
        settings: Settings,
        state_repository: DeviceStateRepository | None = None,
    ) -> None:
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
        self._mailer = SmtpMailer(settings)
        self._state_repository = state_repository or SQLiteDeviceStateRepository(
            settings.sqlite_path
        )
        self._running = True
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
        logger.info("종료 신호를 받았습니다.")
        self._running = False

    def run(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        self._consumer.subscribe([self._settings.kafka_topic])
        logger.info("Kafka topic 구독 시작: %s", self._settings.kafka_topic)

        try:
            while self._running:
                record = self._consumer.poll(timeout=1.0)
                if record is None:
                    self._notify_stale_devices()
                    continue
                if record.error():
                    self._handle_error(record)
                    continue
                self._process(record)
                self._notify_stale_devices()
        finally:
            self._consumer.close()
            self._state_repository.close()
            logger.info("Kafka consumer가 종료되었습니다.")

    def _process(self, record: Message) -> None:
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

        if notification and not self._send_with_retry(
            heartbeat, notification, detail
        ):
            return

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
            if self._send_with_retry(state.heartbeat, "MISSING", detail):
                self._state_repository.mark_stale(state.heartbeat.device_id)
                state.stale_notified = True

    def _send_with_retry(
        self, heartbeat: HeartbeatMessage, notification: str, detail: str
    ) -> bool:
        while self._running:
            try:
                self._mailer.send_heartbeat(heartbeat, notification, detail)
                return True
            except Exception:
                logger.exception(
                    "메일 발송 실패; 5초 후 재시도합니다: device=%s type=%s",
                    heartbeat.device_id,
                    notification,
                )
                time.sleep(5)
        return False

    @staticmethod
    def _handle_error(record: Message) -> None:
        error = record.error()
        if error.code() == KafkaError._PARTITION_EOF:
            logger.debug("Partition 끝에 도달했습니다: %s", error)
            return
        raise KafkaException(error)
