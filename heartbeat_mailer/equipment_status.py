from __future__ import annotations

from dataclasses import dataclass
import logging
import threading
import time
from typing import Any, Callable, Protocol

from .config import Settings


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EquipmentAlertDecision:
    """MES 장비 상태를 근거로 한 메일 알림 허용 결과."""

    allowed: bool
    status_code: str | None
    reason: str


class EquipmentStatusProvider(Protocol):
    """장비 ID별 MES 상태와 알림 허용 여부를 제공하는 규약."""

    def start(self) -> None:
        """상태 동기화 작업을 시작한다."""
        ...

    def stop(self, timeout: float = 10.0) -> None:
        """상태 동기화 작업을 종료한다."""
        ...

    def alert_decision(self, device_id: str) -> EquipmentAlertDecision:
        """장비의 현재 MES 상태를 기준으로 알림 허용 결과를 반환한다."""
        ...


class OracleEquipmentStatusCache:
    """Oracle의 EQP_ID와 MAIN_STAT_CD를 주기적으로 메모리에 캐시한다."""

    def __init__(
        self,
        settings: Settings,
        connect: Callable[..., Any] | None = None,
    ) -> None:
        """Oracle 접속 설정과 상태 갱신 주기를 준비한다.

        입력:
            settings: Oracle DSN, 계정, 조회 SQL 및 허용 상태 설정.
            connect: 테스트에서 주입할 DB 연결 함수. 없으면 python-oracledb를 사용한다.
        반환:
            없음.
        """
        self._settings = settings
        self._connect = connect or _oracle_connect
        self._statuses: dict[str, str] = {}
        self._available = False
        self._last_success_at = 0.0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="oracle-equipment-status-cache",
            daemon=True,
        )
        self._last_warning_at: dict[str, float] = {}

    def start(self) -> None:
        """Oracle 상태를 즉시 조회하는 백그라운드 thread를 시작한다.

        입력:
            없음.
        반환:
            없음.
        """
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        """상태 갱신 thread의 종료를 요청하고 제한 시간 동안 기다린다.

        입력:
            timeout: thread 종료를 기다릴 최대 초.
        반환:
            없음.
        """
        self._stop_event.set()
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            logger.warning("Oracle 장비 상태 thread가 종료 제한 시간 안에 멈추지 않았습니다.")

    def refresh_once(self) -> bool:
        """설정된 SQL을 실행하여 전체 장비 상태 캐시를 한 번 교체한다.

        입력:
            없음. 설정의 Oracle 접속정보와 조회 SQL을 사용한다.
        반환:
            조회에 성공하면 ``True``, 실패하면 ``False``.
        처리:
            SQL 결과의 첫 컬럼은 EQP_ID, 두 번째 컬럼은 MAIN_STAT_CD로 해석한다.
            조회 실패 시 이전 캐시를 사용하지 않고 모든 장비 알림을 보류한다.
        """
        try:
            with self._connect(
                user=self._settings.oracle_user,
                password=self._settings.oracle_password,
                dsn=self._settings.oracle_dsn,
            ) as connection:
                connection.call_timeout = self._settings.oracle_call_timeout_ms
                with connection.cursor() as cursor:
                    query = self._settings.oracle_status_query.rstrip("; ")
                    rows = cursor.execute(query).fetchall()
            statuses: dict[str, str] = {}
            for row in rows:
                if len(row) < 2 or row[0] is None or row[1] is None:
                    logger.warning("Oracle 장비 상태 행을 건너뜁니다: row=%r", row)
                    continue
                equipment_id = str(row[0]).strip()
                status_code = str(row[1]).strip().upper()
                if not equipment_id or not status_code:
                    logger.warning("Oracle 장비 상태 행을 건너뜁니다: row=%r", row)
                    continue
                statuses[equipment_id] = status_code
        except Exception:
            with self._lock:
                self._available = False
            logger.exception(
                "Oracle 장비 상태 조회에 실패했습니다. 장비별 메일 알림을 보류합니다."
            )
            return False

        with self._lock:
            self._statuses = statuses
            self._available = True
            self._last_success_at = time.monotonic()
        logger.info("Oracle 장비 상태 캐시를 갱신했습니다: count=%s", len(statuses))
        return True

    def alert_decision(self, device_id: str) -> EquipmentAlertDecision:
        """EQP_ID와 정확히 일치하는 MES 상태로 메일 허용 여부를 판단한다.

        입력:
            device_id: heartbeat ``instanceId``에서 얻은 장비 ID.
        반환:
            STAB 또는 NECK이면 허용하고, 조회 불가·미등록·그 외 상태면 보류하는 결과.
        """
        with self._lock:
            available = self._available
            last_success_at = self._last_success_at
            status_code = self._statuses.get(device_id)
        cache_age = time.monotonic() - last_success_at
        if (
            not available
            or cache_age > self._settings.oracle_cache_max_age_seconds
        ):
            self._warn_once(
                f"unavailable:{device_id}",
                "Oracle 상태를 확인할 수 없어 알림을 보류합니다: device=%s",
                device_id,
            )
            reason = (
                "Oracle 상태 조회 불가"
                if not available
                else f"Oracle 상태 캐시 만료({cache_age:.1f}초)"
            )
            return EquipmentAlertDecision(False, None, reason)
        if status_code is None:
            self._warn_once(
                f"missing:{device_id}",
                "Oracle 조회 결과에 EQP_ID가 없어 알림을 보류합니다: device=%s",
                device_id,
            )
            return EquipmentAlertDecision(False, None, "EQP_ID 미조회")
        allowed = status_code in self._settings.oracle_alert_status_codes
        return EquipmentAlertDecision(
            allowed,
            status_code,
            "허용 상태" if allowed else f"MAIN_STAT_CD={status_code}",
        )

    def _run(self) -> None:
        """종료 요청까지 Oracle 장비 상태를 설정 주기로 반복 조회한다."""
        logger.info("Oracle 장비 상태 동기화를 시작합니다.")
        while not self._stop_event.is_set():
            self.refresh_once()
            self._stop_event.wait(self._settings.oracle_refresh_seconds)
        logger.info("Oracle 장비 상태 동기화가 종료되었습니다.")

    def _warn_once(self, key: str, message: str, device_id: str) -> None:
        """동일 장비의 상태 확인 경고를 주기당 한 번만 기록한다."""
        now = time.monotonic()
        previous = self._last_warning_at.get(key, 0.0)
        if now - previous < self._settings.oracle_refresh_seconds:
            return
        self._last_warning_at[key] = now
        logger.warning(message, device_id)


def _oracle_connect(**kwargs: Any) -> Any:
    """python-oracledb Thin 모드로 Oracle 연결을 생성한다.

    입력:
        kwargs: ``user``, ``password``, ``dsn`` 접속 인자.
    반환:
        context manager를 지원하는 Oracle connection.
    """
    import oracledb

    return oracledb.connect(**kwargs)
