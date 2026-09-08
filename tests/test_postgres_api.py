"""TEST_DATABASE_URL을 지정하면 실제 PostgreSQL에서 수행하는 통합 테스트."""
import os
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from uuid import uuid4
from unittest.mock import patch

from tests.test_message import FakeRecord, cloud_event
from tests import test_runner_safety
from heartbeat_mailer.message import HeartbeatMessage


@unittest.skipUnless(os.getenv('TEST_DATABASE_URL'), 'TEST_DATABASE_URL not set')
class PostgresApiTest(unittest.TestCase):
    """입력: 테스트 DB. 새 ID만 사용하여 상태/큐/관리 API를 검증한다."""

    def setUp(self):
        """테스트별 독립 장비를 등록하고 실제 DB와 연결한 API 클라이언트를 연다."""
        from fastapi.testclient import TestClient
        from heartbeat_mailer.api import create_app
        from heartbeat_mailer.postgres import PostgresRepository
        self.dsn = os.environ['TEST_DATABASE_URL']
        self.repo = PostgresRepository(self.dsn)
        self.repo.initialize()
        self.device_id = 'TEST-' + uuid4().hex
        self.payload = cloud_event()
        self.payload['data']['sourceInfo']['instanceId'] = self.device_id
        self.heartbeat = HeartbeatMessage.from_kafka_record(FakeRecord(self.payload))
        self.repo.save(self.heartbeat, time.time() - 200, False, False)
        self.token = 'integration-only-token-' + uuid4().hex
        self.client = TestClient(create_app(self.dsn, self.token, 180))
        self.client.__enter__()
        self.headers = {'Authorization': 'Bearer ' + self.token}
        self.url = '/api/v1/devices/' + self.device_id

    def tearDown(self):
        """테스트 클라이언트를 닫고 해당 테스트 장비만 감시 제외한다. 이력은 보존한다."""
        self.client.__exit__(None, None, None)
        self.repo.manage(self.device_id, False, True, 'Integration test finished')

    def test_auth_detail_and_input_validation(self):
        """인증·미수신 표시·페이지·존재하지 않는 ID·엄격한 boolean을 검증한다."""
        self.assertEqual(401, self.client.get(self.url).status_code)
        self.assertEqual(401, self.client.get(self.url, headers={'Authorization': 'Bearer bad'}).status_code)
        detail = self.client.get(self.url, headers=self.headers)
        self.assertEqual(200, detail.status_code)
        self.assertEqual('MISSING', detail.json()['receptionStatus'])
        self.assertEqual(self.device_id, detail.json()['deviceId'])
        self.assertEqual(422, self.client.get('/api/v1/devices?limit=501', headers=self.headers).status_code)
        self.assertEqual(404, self.client.get(self.url + '-unknown', headers=self.headers).status_code)
        self.assertEqual(422, self.client.patch(self.url + '/monitoring', headers=self.headers,
                                               json={'enabled': 'false'}).status_code)
        self.assertEqual(200, self.client.get('/health/ready').status_code)

    def test_disable_cancels_claimed_job_and_resume(self):
        """감시 제외는 선점한 메일까지 취소하고 heartbeat로 자동 활성화되지 않는다."""
        self.repo.enqueue(self.heartbeat, 'MISSING', 'test')
        job = self.repo.claim_next(time.time() + 1)
        response = self.client.patch(self.url + '/monitoring', headers=self.headers,
                                     json={'enabled': False, 'reason': '계획 작업'})
        self.assertEqual(200, response.status_code)
        with self.repo.delivery_guard(job) as allowed:
            self.assertFalse(allowed)
        self.repo.mark_failed(job.id, 'late error', time.time(), False)
        self.repo.save(self.heartbeat, time.time(), True, True)
        self.assertFalse(self.repo.monitoring_allowed(self.device_id))
        self.assertFalse(self.repo.get_device(self.device_id)['stale_notified'])
        with self.repo.connect() as conn:
            row = conn.execute('SELECT status FROM notification_queue WHERE id=%s', (job.id,)).fetchone()
        self.assertEqual('CANCELLED', row['status'])
        self.assertEqual(200, self.client.patch(self.url + '/monitoring', headers=self.headers,
                                              json={'enabled': True}).status_code)
        self.assertTrue(self.repo.monitoring_allowed(self.device_id))

    def test_delete_tombstone_and_explicit_restore(self):
        """논리 삭제 후 heartbeat는 상태를 갱신하지 않으며 복원과 감시 재개를 분리한다."""
        old_seen = self.repo.get_device(self.device_id)['last_seen_at']
        self.assertEqual(200, self.client.delete(self.url, headers=self.headers).status_code)
        self.repo.save(self.heartbeat, time.time(), False, False)
        self.assertEqual(old_seen, self.repo.get_device(self.device_id)['last_seen_at'])
        self.assertFalse(self.repo.enqueue(self.heartbeat, 'MISSING', 'ignored'))
        self.assertEqual(409, self.client.patch(self.url + '/monitoring', headers=self.headers,
                                              json={'enabled': True}).status_code)
        self.assertEqual(200, self.client.post(self.url + '/restore', headers=self.headers).status_code)
        row = self.repo.get_device(self.device_id)
        self.assertFalse(row['deleted'])
        self.assertFalse(row['enabled'])

    def test_queue_and_state_rollback_together(self):
        """저장 실패 시 상태와 알림 큐가 함께 rollback되는지 검증한다."""
        with self.assertRaisesRegex(RuntimeError, 'rollback'):
            with self.repo.transaction():
                self.repo.enqueue(self.heartbeat, 'MISSING', 'test')
                self.repo.mark_stale(self.device_id)
                raise RuntimeError('rollback')
        self.assertFalse(self.repo.get_device(self.device_id)['stale_notified'])
        with self.repo.connect() as conn:
            count = conn.execute('SELECT count(*) AS n FROM notification_queue WHERE device_id=%s',
                                 (self.device_id,)).fetchone()['n']
        self.assertEqual(0, count)

    def test_reason_only_update_preserves_pending_alert(self):
        """감시 상태가 그대로인 사유 수정은 경고를 취소하거나 중복 생성하지 않는다."""
        self.repo.enqueue(self.heartbeat, 'MISSING', 'test')
        self.repo.mark_stale(self.device_id)
        self.repo.manage(self.device_id, True, False, '설명 수정')
        self.assertTrue(self.repo.get_device(self.device_id)['stale_notified'])
        with self.repo.connect() as conn:
            row = conn.execute('SELECT status FROM notification_queue WHERE device_id=%s',
                               (self.device_id,)).fetchone()
        self.assertEqual('PENDING', row['status'])

    def test_missing_and_receiving_persist_across_reload(self):
        """실제 DB 복원 뒤 미수신은 한 번만 등록하고 다음 수신은 재개로 알린다."""
        notifier = test_runner_safety.RunnerSafetyTest()._notifier()
        notifier._state_repository = self.repo
        notifier._notification_repository = self.repo
        notifier._consumer = test_runner_safety.FakeCommitConsumer()
        notifier._notify_stale_devices()
        notifier._notify_stale_devices()
        self.assertTrue(self.repo.get_device(self.device_id)['stale_notified'])
        notifier._process(FakeRecord(self.payload))
        self.assertFalse(self.repo.get_device(self.device_id)['stale_notified'])
        with self.repo.connect() as conn:
            types = conn.execute('SELECT notification_type FROM notification_queue WHERE device_id=%s ORDER BY id',
                                 (self.device_id,)).fetchall()
        # 샘플은 WARN이므로 수신 재개와 별도로 보고 상태 경고도 한 건 등록한다.
        self.assertEqual(['MISSING', 'RECEIVING', 'ALERT'], [row['notification_type'] for row in types])

    def test_worker_claim_retry_dedupe_and_recovery(self):
        """중복 차단·재시도·발송 중 중단 복구·발송 완료를 검증한다."""
        self.assertTrue(self.repo.enqueue(self.heartbeat, 'MISSING', 'test'))
        self.assertFalse(self.repo.enqueue(self.heartbeat, 'MISSING', 'duplicate'))
        job = self.repo.claim_next(time.time() + 1)
        self.repo.mark_failed(job.id, 'SMTP failure', time.time() - 1, False)
        retry = self.repo.claim_next(time.time() + 1)
        self.assertEqual(1, retry.attempt_count)
        self.repo.recover_jobs()
        recovered = self.repo.claim_next(time.time() + 1)
        self.assertEqual(job.id, recovered.id)
        with self.repo.delivery_guard(recovered) as allowed:
            self.assertTrue(allowed)
            self.repo.mark_sent(recovered.id)
        self.assertTrue(self.repo.enqueue(self.heartbeat, 'MISSING', 'next incident'))

    def test_runner_reads_management_and_commits_after_db(self):
        """consumer가 API 설정을 반영하고 DB 실패 시 Kafka offset을 commit하지 않는다."""
        notifier = test_runner_safety.RunnerSafetyTest()._notifier()
        notifier._state_repository = self.repo
        notifier._notification_repository = self.repo
        notifier._consumer = test_runner_safety.FakeCommitConsumer()
        self.repo.manage(self.device_id, False, False, 'maintenance')
        notifier._notify_stale_devices()
        self.assertFalse(self.repo.get_device(self.device_id)['stale_notified'])
        notifier._process(FakeRecord(self.payload))
        self.assertEqual(1, len(notifier._consumer.commits))
        with patch.object(self.repo, 'save', side_effect=RuntimeError('DB failed')):
            with self.assertRaisesRegex(RuntimeError, 'DB failed'):
                notifier._process(FakeRecord(self.payload))
        self.assertEqual(1, len(notifier._consumer.commits))

    def test_smtp_guard_does_not_block_heartbeat_but_serializes_disable(self):
        """느린 SMTP 중에도 상태 저장은 진행되고 제외 API는 진행 중 발송 완료를 기다린다."""
        from heartbeat_mailer.postgres import PostgresRepository
        self.repo.enqueue(self.heartbeat, 'MISSING', 'test')
        job = self.repo.claim_next(time.time() + 1)
        started = Event()

        def disable():
            """별도 연결에서 감시 제외를 요청하여 발송 잠금과의 경합을 검증한다."""
            started.set()
            return PostgresRepository(self.dsn).manage(self.device_id, False, False, 'maintenance')

        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.repo.delivery_guard(job) as allowed:
                self.assertTrue(allowed)
                PostgresRepository(self.dsn).save(self.heartbeat, time.time(), False, False)
                future = pool.submit(disable)
                self.assertTrue(started.wait(2))
                self.assertFalse(future.done())
                self.repo.mark_sent(job.id)
            self.assertFalse(future.result(timeout=5)['enabled'])
