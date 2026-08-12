from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from heartbeat_mailer.message import HeartbeatMessage
from heartbeat_mailer.storage import SQLiteDeviceStateRepository
from tests.test_message import FakeRecord, cloud_event


class SQLiteDeviceStateRepositoryTest(unittest.TestCase):
    def test_state_survives_reopen_and_stale_update(self) -> None:
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
