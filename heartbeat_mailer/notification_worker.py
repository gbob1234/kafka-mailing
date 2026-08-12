from __future__ import annotations

import logging
import threading
import time

from .config import Settings
from .mailer import SmtpMailer
from .storage import NotificationQueueRepository


logger = logging.getLogger(__name__)


class MailNotificationWorker:
    """SQLite 알림 큐를 읽어 Kafka 소비와 독립적으로 SMTP를 발송한다."""

    def __init__(
        self,
        settings: Settings,
        repository: NotificationQueueRepository,
        mailer: SmtpMailer | None = None,
    ) -> None:
        """메일 worker의 재시도 정책과 의존성을 초기화한다.

        입력:
            settings: 큐 polling 및 SMTP 재시도 설정.
            repository: worker 전용 알림 큐 저장소 연결.
            mailer: 테스트에서 주입할 SMTP 발송기. 없으면 실제 발송기를 생성한다.
        반환:
            없음.
        """
        self._settings = settings
        self._repository = repository
        self._mailer = mailer or SmtpMailer(settings)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="smtp-notification-worker",
            daemon=True,
        )

    def start(self) -> None:
        """백그라운드 SMTP worker thread를 시작한다.

        입력:
            없음.
        반환:
            없음.
        """
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        """worker 종료를 요청하고 제한 시간 동안 thread를 기다린다.

        입력:
            timeout: thread 종료를 기다릴 최대 초.
        반환:
            없음. SMTP 호출이 진행 중이면 daemon thread가 남을 수 있다.
        """
        self._stop_event.set()
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            logger.warning("SMTP worker가 종료 제한 시간 안에 멈추지 않았습니다.")

    def process_once(self, now: float | None = None) -> bool:
        """발송 가능한 알림 한 건을 처리한다.

        입력:
            now: 테스트에서 고정할 Unix epoch 초. 없으면 현재 시각을 사용한다.
        반환:
            처리할 작업이 있었으면 ``True``, 큐가 비어 있으면 ``False``.
        """
        current_time = time.time() if now is None else now
        job = self._repository.claim_next(current_time)
        if job is None:
            return False

        try:
            self._mailer.send_heartbeat(
                job.heartbeat, job.notification_type, job.detail
            )
        except Exception as exc:
            attempts = job.attempt_count + 1
            dead = attempts >= self._settings.mail_max_retry_attempts
            delay = min(
                self._settings.mail_retry_initial_seconds
                * (2 ** max(0, attempts - 1)),
                self._settings.mail_retry_max_seconds,
            )
            self._repository.mark_failed(
                job.id,
                f"{type(exc).__name__}: {exc}",
                current_time + delay,
                dead,
            )
            if dead:
                logger.error(
                    "메일 최대 재시도 횟수를 초과했습니다: id=%s device=%s "
                    "attempts=%s",
                    job.id,
                    job.heartbeat.device_id,
                    attempts,
                )
            else:
                logger.warning(
                    "메일 발송 실패; 재시도 예약: id=%s device=%s attempts=%s "
                    "delay=%ss",
                    job.id,
                    job.heartbeat.device_id,
                    attempts,
                    delay,
                    exc_info=True,
                )
            return True

        self._repository.mark_sent(job.id)
        logger.info(
            "메일 발송 완료: id=%s device=%s type=%s",
            job.id,
            job.heartbeat.device_id,
            job.notification_type,
        )
        return True

    def _run(self) -> None:
        """종료 요청까지 알림 큐를 polling하고 발송한다.

        입력:
            없음.
        반환:
            없음. 종료할 때 worker 전용 저장소 연결을 닫는다.
        """
        logger.info("SMTP 알림 worker가 시작되었습니다.")
        try:
            while not self._stop_event.is_set():
                try:
                    processed = self.process_once()
                except Exception:
                    logger.exception("SMTP worker 내부 오류가 발생했습니다.")
                    processed = False
                if not processed:
                    self._stop_event.wait(
                        self._settings.mail_queue_poll_seconds
                    )
        finally:
            self._repository.close()
            logger.info("SMTP 알림 worker가 종료되었습니다.")
