from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import tempfile
import time
import unittest

from heartbeat_mailer.message import HeartbeatMessage
from heartbeat_mailer.notification_worker import MailNotificationWorker
from heartbeat_mailer.storage import SQLiteNotificationQueueRepository
from tests.test_message import FakeRecord, cloud_event


class FailingMailer:
    """지정된 횟수만큼 예외를 발생시키는 SMTP 발송기 대역."""

    def __init__(self, failures: int) -> None:
        """발송 성공 전 발생시킬 실패 횟수를 저장한다."""
        self.failures = failures
        self.calls = 0

    def send_heartbeat(self, heartbeat, notification, detail) -> None:
        """호출 횟수를 기록하고 설정된 범위에서는 발송 실패를 흉내 낸다."""
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError("smtp unavailable")


def worker_settings(max_attempts: int = 2) -> SimpleNamespace:
    """메일 worker 단위 테스트에 필요한 최소 설정 객체를 반환한다."""
    return SimpleNamespace(
        mail_queue_poll_seconds=0.01,
        mail_max_retry_attempts=max_attempts,
        mail_retry_initial_seconds=5.0,
        mail_retry_max_seconds=8.0,
    )


class NotificationQueueTest(unittest.TestCase):
    """영속 알림 큐의 중복 방지, 복구 및 재시도를 검증한다."""

    def test_active_duplicate_is_ignored_but_sent_incident_can_repeat(self) -> None:
        """활성 중복은 막고 발송 완료 뒤 같은 장애의 재등록은 허용한다."""
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteNotificationQueueRepository(
                str(Path(directory) / "state.db")
            )
            heartbeat = HeartbeatMessage.from_kafka_record(
                FakeRecord(cloud_event())
            )
            self.assertTrue(repository.enqueue(heartbeat, "ALERT", "failure"))
            self.assertFalse(repository.enqueue(heartbeat, "ALERT", "failure"))
            self.assertEqual(1, repository.count_by_status("PENDING"))

            job = repository.claim_next(time.time() + 1)
            self.assertIsNotNone(job)
            repository.mark_sent(job.id)
            self.assertTrue(repository.enqueue(heartbeat, "ALERT", "new incident"))
            repository.close()

    def test_interrupted_sending_job_is_recovered_on_reopen(self) -> None:
        """발송 중 프로세스가 종료된 작업이 재시도 대상으로 복원되는지 확인한다."""
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "state.db")
            heartbeat = HeartbeatMessage.from_kafka_record(
                FakeRecord(cloud_event())
            )
            repository = SQLiteNotificationQueueRepository(database)
            repository.enqueue(heartbeat, "MISSING", "stale")
            self.assertIsNotNone(repository.claim_next(time.time() + 1))
            repository.close()

            reopened = SQLiteNotificationQueueRepository(database)
            self.assertEqual(1, reopened.count_by_status("RETRY"))
            self.assertIsNotNone(reopened.claim_next(time.time() + 1))
            reopened.close()

    def test_worker_retries_and_marks_dead_at_max_attempts(self) -> None:
        """SMTP 연속 실패가 최대 횟수에서 DEAD 상태가 되는지 확인한다."""
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteNotificationQueueRepository(
                str(Path(directory) / "state.db")
            )
            heartbeat = HeartbeatMessage.from_kafka_record(
                FakeRecord(cloud_event())
            )
            repository.enqueue(heartbeat, "ALERT", "failure")
            mailer = FailingMailer(failures=10)
            worker = MailNotificationWorker(
                worker_settings(max_attempts=2), repository, mailer
            )
            base = time.time() + 1

            self.assertTrue(worker.process_once(base))
            self.assertEqual(1, repository.count_by_status("RETRY"))
            self.assertFalse(worker.process_once(base + 4))
            self.assertTrue(worker.process_once(base + 5))
            self.assertEqual(1, repository.count_by_status("DEAD"))
            self.assertEqual(2, mailer.calls)
            repository.close()


if __name__ == "__main__":
    unittest.main()
