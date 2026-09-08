from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from heartbeat_mailer.equipment_status import OracleEquipmentStatusCache, _oracle_connect


class FakeCursor:
    """Oracle 상태 조회 결과를 제공하는 cursor 대역."""

    def __init__(self, rows) -> None:
        self.rows = rows
        self.query = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def execute(self, query):
        self.query = query
        return self

    def fetchall(self):
        return self.rows


class FakeConnection:
    """context manager와 cursor를 제공하는 Oracle connection 대역."""

    def __init__(self, rows) -> None:
        self.rows = rows
        self.call_timeout = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def cursor(self):
        return FakeCursor(self.rows)


def settings():
    """Oracle 캐시 테스트에 필요한 최소 설정을 반환한다."""
    return SimpleNamespace(
        oracle_user="user",
        oracle_password="password",
        oracle_dsn="db.example.com:1521/service",
        oracle_status_query="select eqp_id, main_stat_cd from STATUS_TABLE",
        oracle_call_timeout_ms=5000,
        oracle_refresh_seconds=30.0,
        oracle_cache_max_age_seconds=90.0,
        oracle_alert_status_codes=frozenset({"STAB", "NECK"}),
    )


class OracleEquipmentStatusCacheTest(unittest.TestCase):
    """Oracle 조회 결과에 따른 장비 알림 허용 정책을 검증한다."""

    def test_thick_initialization_precedes_connection_and_runs_once(self) -> None:
        """입력: Oracle 모듈 대역. 반환: 없음. 초기화 순서와 재접속 시 중복 방지를 검증한다."""
        driver = Mock()
        driver.is_thin_mode.side_effect = [True, False]
        calls = []
        driver.init_oracle_client.side_effect = lambda: calls.append('init')
        driver.connect.side_effect = lambda **kwargs: calls.append('connect') or 'connection'
        with patch.dict('sys.modules', {'oracledb': driver}):
            self.assertEqual('connection', _oracle_connect(user='u', password='p', dsn='d'))
            _oracle_connect(user='u', password='p', dsn='d')
        self.assertEqual(['init', 'connect', 'connect'], calls)
        driver.init_oracle_client.assert_called_once_with()
        driver.connect.assert_called_with(user='u', password='p', dsn='d')

    def test_thick_initialization_failure_suppresses_alerts_without_connect(self) -> None:
        """입력: Client 로딩 실패 대역. 반환: 없음. Thin 우회 없이 로그와 알림 보류를 검증한다."""
        driver = Mock()
        driver.is_thin_mode.return_value = True
        driver.init_oracle_client.side_effect = RuntimeError('DPI-1047: Oracle Client unavailable')
        cache = OracleEquipmentStatusCache(settings())
        with patch.dict('sys.modules', {'oracledb': driver}):
            with self.assertLogs('heartbeat_mailer.equipment_status', level='ERROR') as logs:
                self.assertFalse(cache.refresh_once())
        self.assertIn('DPI-1047', '\n'.join(logs.output))
        driver.connect.assert_not_called()
        self.assertFalse(cache.alert_decision('EQP-001').allowed)

    def test_only_stab_and_neck_allow_notifications(self) -> None:
        """STAB/NECK만 허용하고 IDLE과 미등록 장비는 보류한다."""
        connection = FakeConnection(
            [("EQP-001", "STAB"), ("EQP-002", "NECK"), ("EQP-003", "IDLE")]
        )
        cache = OracleEquipmentStatusCache(
            settings(), connect=lambda **kwargs: connection
        )

        self.assertTrue(cache.refresh_once())
        self.assertTrue(cache.alert_decision("EQP-001").allowed)
        self.assertTrue(cache.alert_decision("EQP-002").allowed)
        self.assertFalse(cache.alert_decision("EQP-003").allowed)
        missing = cache.alert_decision("EQP-999")
        self.assertFalse(missing.allowed)
        self.assertEqual("EQP_ID 미조회", missing.reason)

    def test_query_failure_invalidates_previous_cache(self) -> None:
        """Oracle 장애가 나면 과거 STAB 캐시로 알림을 보내지 않는다."""
        calls = 0

        def connect(**kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                return FakeConnection([("EQP-001", "STAB")])
            raise RuntimeError("oracle unavailable")

        cache = OracleEquipmentStatusCache(settings(), connect=connect)
        self.assertTrue(cache.refresh_once())
        self.assertTrue(cache.alert_decision("EQP-001").allowed)

        self.assertFalse(cache.refresh_once())
        decision = cache.alert_decision("EQP-001")
        self.assertFalse(decision.allowed)
        self.assertEqual("Oracle 상태 조회 불가", decision.reason)


if __name__ == "__main__":
    unittest.main()
