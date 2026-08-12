from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from heartbeat_mailer.message import HeartbeatMessage
from heartbeat_mailer.storage import SQLiteDeviceStateRepository
from tests.test_message import FakeRecord, cloud_event


class SQLiteDeviceStateRepositoryTest(unittest.TestCase):
    """SQLite 장비 상태의 영속성과 갱신 동작을 확인한다."""

    def test_state_survives_reopen_and_stale_update(self) -> None:
        """DB 재연결 후 상태와 미수신 표시가 유지되는지 확인한다.

        입력:
            없음. 테스트별 임시 디렉터리와 CloudEvent fixture를 사용한다.
        반환:
            없음. 검증 실패 시 unittest assertion 예외가 발생한다.
        """
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "state.db")
            heartbeat = HeartbeatMessage.from_kafka_record(
                FakeRecord(cloud_event())
            )

            repository = SQLiteDeviceStateRepository(database)
            repository.save(heartbeat, last_seen_at=1_700_000_000.0, stale_notified=False)
            repository.close()

            reopened = SQLiteDeviceStateRepository(database)
            states = reopened.load_all()
            self.assertEqual(1, len(states))
            self.assertEqual("DEVICE-001", states[0].heartbeat.device_id)
            self.assertEqual(("WARN", "IMAGE_KAFKA_SEND_FAILED"), states[0].status_signature)
            self.assertFalse(states[0].stale_notified)
            self.assertEqual(1_700_000_000.0, states[0].last_seen_at)

            reopened.mark_stale("DEVICE-001")
            self.assertTrue(reopened.load_all()[0].stale_notified)
            reopened.close()


if __name__ == "__main__":
    unittest.main()
