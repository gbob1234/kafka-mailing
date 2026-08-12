from __future__ import annotations

import logging
import signal
import time
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from .config import Settings
from .mailer import SmtpMailer
from .message import HeartbeatMessage


logger = logging.getLogger(__name__)


class HeartbeatNotifier:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_group_id,
                "auto.offset.reset": settings.kafka_auto_offset_reset,
                "enable.auto.commit": False,
            }
        )
        self._mailer = SmtpMailer(settings)
        self._running = True
        self._last_sent_at = 0.0

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
                    continue
                if record.error():
                    self._handle_error(record)
                    continue
                self._process(record)
        finally:
            self._consumer.close()
            logger.info("Kafka consumer가 종료되었습니다.")

    def _process(self, record: Message) -> None:
        heartbeat = HeartbeatMessage.from_kafka_record(record)
        elapsed = time.monotonic() - self._last_sent_at
        remaining = self._settings.mail_min_interval_seconds - elapsed

        if self._last_sent_at and remaining > 0:
            logger.info(
                "메일 최소 간격 적용으로 메시지를 건너뜁니다: topic=%s offset=%s",
                heartbeat.topic,
                heartbeat.offset,
            )
            self._consumer.commit(message=record, asynchronous=False)
            return

        while self._running:
            try:
                self._mailer.send_heartbeat(heartbeat)
                break
            except Exception:
                logger.exception(
                    "메일 발송 실패; 5초 후 같은 메시지를 재시도합니다: "
                    "topic=%s offset=%s",
                    heartbeat.topic,
                    heartbeat.offset,
                )
                time.sleep(5)

        if not self._running:
            logger.info(
                "종료 중이므로 미발송 메시지를 commit하지 않습니다: topic=%s offset=%s",
                heartbeat.topic,
                heartbeat.offset,
            )
            return

        self._last_sent_at = time.monotonic()
        self._consumer.commit(message=record, asynchronous=False)
        logger.info(
            "메일 발송 및 commit 완료: topic=%s partition=%s offset=%s",
            heartbeat.topic,
            heartbeat.partition,
            heartbeat.offset,
        )

    @staticmethod
    def _handle_error(record: Message) -> None:
        error = record.error()
        if error.code() == KafkaError._PARTITION_EOF:
            logger.debug("Partition 끝에 도달했습니다: %s", error)
            return
        raise KafkaException(error)
