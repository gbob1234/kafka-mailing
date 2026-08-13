from __future__ import annotations

from types import SimpleNamespace
import unittest

from heartbeat_mailer.equipment_status import OracleEquipmentStatusCache


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
