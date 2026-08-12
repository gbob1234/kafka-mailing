from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import time
from typing import Protocol

from .message import HeartbeatMessage


@dataclass(frozen=True)
class StoredDeviceState:
    """영구 저장소에서 복원한 장비 상태와 알림 메타데이터."""

    heartbeat: HeartbeatMessage
    last_seen_at: float
    status_signature: tuple[str, str]
    stale_notified: bool


@dataclass(frozen=True)
class NotificationJob:
    """SQLite 알림 큐에서 worker가 가져온 발송 작업."""

    id: int
    dedupe_key: str
    heartbeat: HeartbeatMessage
    notification_type: str
    detail: str
    attempt_count: int


class NotificationQueueRepository(Protocol):
    """SMTP worker와 consumer가 공유하는 영속 알림 큐 저장소 규약."""

    def enqueue(
        self,
        heartbeat: HeartbeatMessage,
        notification_type: str,
        detail: str,
    ) -> bool:
        """알림을 중복 없이 큐에 추가하고 실제 추가 여부를 반환한다."""
        ...

    def claim_next(self, now: float) -> NotificationJob | None:
        """발송 가능한 가장 오래된 알림 하나를 선점하여 반환한다."""
        ...

    def mark_sent(self, job_id: int) -> None:
        """알림을 발송 완료 상태로 변경한다."""
        ...

    def mark_failed(
        self,
        job_id: int,
        error: str,
        next_attempt_at: float,
        dead: bool,
    ) -> None:
        """실패 횟수와 다음 시도 시각 또는 최종 실패 상태를 기록한다."""
        ...

    def close(self) -> None:
        """저장소 연결을 닫는다."""
        ...


class DeviceStateRepository(Protocol):
    """SQLite와 향후 PostgreSQL 구현이 따라야 할 장비 상태 저장소 규약."""

    def load_all(self) -> list[StoredDeviceState]:
        """모든 장비 상태를 불러온다.

        입력:
            없음.
        반환:
            저장된 장비 상태 목록. 데이터가 없으면 빈 목록.
        """
        ...

    def save(
        self,
        heartbeat: HeartbeatMessage,
        last_seen_at: float,
        stale_notified: bool,
    ) -> None:
        """장비의 최신 heartbeat 상태를 저장하거나 덮어쓴다.

        입력:
            heartbeat: 저장할 검증 완료 heartbeat.
            last_seen_at: Unix epoch 초 단위의 마지막 수신 시각.
            stale_notified: 미수신 알림 발송 여부.
        반환:
            없음.
        """
        ...

    def mark_stale(self, device_id: str) -> None:
        """지정 장비를 미수신 알림 완료 상태로 표시한다.

        입력:
            device_id: 갱신할 고유 장비 ID.
        반환:
            없음.
        """
        ...

    def close(self) -> None:
        """저장소가 점유한 연결과 자원을 해제한다.

        입력:
            없음.
        반환:
            없음.
        """
        ...


class SQLiteDeviceStateRepository:
    """장비별 최신 상태를 SQLite 한 행으로 관리하는 저장소 구현."""

    def __init__(self, database_path: str, journal_mode: str = "WAL") -> None:
        """SQLite 연결을 열고 필요한 테이블과 인덱스를 준비한다.

        입력:
            database_path: 생성하거나 열 SQLite 데이터베이스 파일 경로.
        반환:
            없음.
        예외:
            OSError: 상위 디렉터리를 생성하지 못한 경우.
            sqlite3.Error: DB 연결 또는 PRAGMA 적용이 실패한 경우.
        """
        path = Path(database_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._connection = sqlite3.connect(path, timeout=30)
        self._connection.row_factory = sqlite3.Row
        _configure_connection(self._connection, journal_mode)
        self._migrate()

    def _migrate(self) -> None:
        """현재 버전에 필요한 테이블과 조회 인덱스를 멱등하게 생성한다.

        입력:
            없음.
        반환:
            없음. 스키마 변경사항을 즉시 commit한다.
        예외:
            sqlite3.Error: DDL 실행 또는 commit이 실패한 경우.
        """
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS device_states (
                device_id TEXT PRIMARY KEY,
                system_id TEXT NOT NULL,
                hostname TEXT NOT NULL,
                ip_address TEXT NOT NULL,
                program_name TEXT NOT NULL,
                program_version TEXT NOT NULL,
                status_level TEXT NOT NULL,
                status_code TEXT NOT NULL,
                status_message TEXT NOT NULL,
                heartbeat_interval_seconds INTEGER NOT NULL,
                heartbeat_sequence INTEGER NOT NULL,
                generated_at TEXT NOT NULL,
                last_seen_at REAL NOT NULL,
                stale_notified INTEGER NOT NULL DEFAULT 0,
                event_id TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                kafka_key TEXT,
                kafka_topic TEXT NOT NULL,
                kafka_partition INTEGER NOT NULL,
                kafka_offset INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_device_states_last_seen
            ON device_states(last_seen_at)
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_device_states_status
            ON device_states(status_level, status_code)
            """
        )
        self._connection.commit()

    def load_all(self) -> list[StoredDeviceState]:
        """SQLite에 저장된 모든 장비 상태를 애플리케이션 객체로 복원한다.

        입력:
            없음.
        반환:
            장비 ID 순으로 정렬된 ``StoredDeviceState`` 목록.
        예외:
            sqlite3.Error: 조회에 실패한 경우.
            InvalidHeartbeat: 저장된 CloudEvent 원문이 유효하지 않은 경우.
        """
        rows = self._connection.execute(
            "SELECT * FROM device_states ORDER BY device_id"
        ).fetchall()
        states: list[StoredDeviceState] = []
        for row in rows:
            record = _StoredKafkaRecord(row)
            heartbeat = HeartbeatMessage.from_kafka_record(record)
            heartbeat = replace(
                heartbeat,
                consumed_at=datetime.fromtimestamp(
                    row["last_seen_at"], timezone.utc
                ),
            )
            states.append(
                StoredDeviceState(
                    heartbeat=heartbeat,
                    last_seen_at=row["last_seen_at"],
                    status_signature=(row["status_level"], row["status_code"]),
                    stale_notified=bool(row["stale_notified"]),
                )
            )
        return states

    def save(
        self,
        heartbeat: HeartbeatMessage,
        last_seen_at: float,
        stale_notified: bool,
    ) -> None:
        """장비 ID를 기준으로 최신 상태를 원자적으로 upsert한다.

        입력:
            heartbeat: 원본 CloudEvent와 Kafka 메타데이터를 포함한 상태.
            last_seen_at: Unix epoch 초 단위의 마지막 수신 시각.
            stale_notified: 미수신 알림을 이미 발송했는지 여부.
        반환:
            없음. SQL 실행 후 즉시 commit한다.
        예외:
            sqlite3.Error: upsert 또는 commit이 실패한 경우.
        """
        self._connection.execute(
            """
            INSERT INTO device_states (
                device_id, system_id, hostname, ip_address, program_name,
                program_version, status_level, status_code, status_message,
                heartbeat_interval_seconds, heartbeat_sequence, generated_at,
                last_seen_at, stale_notified, event_id, raw_payload, kafka_key,
                kafka_topic, kafka_partition, kafka_offset, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                system_id=excluded.system_id,
                hostname=excluded.hostname,
                ip_address=excluded.ip_address,
                program_name=excluded.program_name,
                program_version=excluded.program_version,
                status_level=excluded.status_level,
                status_code=excluded.status_code,
                status_message=excluded.status_message,
                heartbeat_interval_seconds=excluded.heartbeat_interval_seconds,
                heartbeat_sequence=excluded.heartbeat_sequence,
                generated_at=excluded.generated_at,
                last_seen_at=excluded.last_seen_at,
                stale_notified=excluded.stale_notified,
                event_id=excluded.event_id,
                raw_payload=excluded.raw_payload,
                kafka_key=excluded.kafka_key,
                kafka_topic=excluded.kafka_topic,
                kafka_partition=excluded.kafka_partition,
                kafka_offset=excluded.kafka_offset,
                updated_at=excluded.updated_at
            """,
            (
                heartbeat.device_id,
                heartbeat.system_id,
                heartbeat.hostname,
                heartbeat.ip_address,
                heartbeat.program_name,
                heartbeat.program_version,
                heartbeat.status_level,
                heartbeat.status_code,
                heartbeat.status_message,
                heartbeat.interval_seconds,
                heartbeat.sequence,
                heartbeat.generated_at,
                last_seen_at,
                int(stale_notified),
                heartbeat.event_id,
                heartbeat.raw_text,
                heartbeat.key,
                heartbeat.topic,
                heartbeat.partition,
                heartbeat.offset,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self._connection.commit()

    def mark_stale(self, device_id: str) -> None:
        """장비의 미수신 알림 발송 여부를 SQLite에 기록한다.

        입력:
            device_id: 갱신할 고유 장비 ID.
        반환:
            없음.
        예외:
            KeyError: 해당 장비가 저장되어 있지 않은 경우.
            sqlite3.Error: 갱신 또는 commit이 실패한 경우.
        """
        cursor = self._connection.execute(
            """
            UPDATE device_states
            SET stale_notified = 1, updated_at = ?
            WHERE device_id = ?
            """,
            (datetime.now(timezone.utc).isoformat(), device_id),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"Unknown device: {device_id}")
        self._connection.commit()

    def close(self) -> None:
        """SQLite 연결을 닫는다.

        입력:
            없음.
        반환:
            없음.
        """
        self._connection.close()


class SQLiteNotificationQueueRepository:
    """SQLite에 알림을 영속화하고 단일 worker가 선점하도록 관리한다."""

    _ACTIVE_STATUSES = ("PENDING", "RETRY", "SENDING")

    def __init__(self, database_path: str, journal_mode: str = "WAL") -> None:
        """SQLite 알림 큐 연결을 열고 중단된 발송 작업을 복구한다.

        입력:
            database_path: 장비 상태 DB와 공유할 SQLite 파일 경로.
        반환:
            없음.
        """
        path = Path(database_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._connection = sqlite3.connect(
            path, timeout=30, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        _configure_connection(self._connection, journal_mode)
        self._migrate()
        self._recover_interrupted_jobs()

    def _migrate(self) -> None:
        """알림 큐 테이블과 활성 중복 방지 인덱스를 생성한다.

        입력:
            없음.
        반환:
            없음. DDL을 즉시 commit한다.
        """
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS notification_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedupe_key TEXT NOT NULL,
                device_id TEXT NOT NULL,
                notification_type TEXT NOT NULL,
                detail TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL,
                last_error TEXT,
                raw_payload TEXT NOT NULL,
                kafka_key TEXT,
                kafka_topic TEXT NOT NULL,
                kafka_partition INTEGER NOT NULL,
                kafka_offset INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                sent_at TEXT
            )
            """
        )
        self._connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_active_dedupe
            ON notification_queue(dedupe_key)
            WHERE status IN ('PENDING', 'RETRY', 'SENDING')
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notification_due
            ON notification_queue(status, next_attempt_at, id)
            """
        )
        self._connection.commit()

    def _recover_interrupted_jobs(self) -> None:
        """프로세스 종료 당시 SENDING이던 작업을 재시도 상태로 되돌린다.

        입력:
            없음.
        반환:
            없음.
        """
        now = time.time()
        self._connection.execute(
            """
            UPDATE notification_queue
            SET status = 'RETRY', next_attempt_at = ?, updated_at = ?
            WHERE status = 'SENDING'
            """,
            (now, datetime.now(timezone.utc).isoformat()),
        )
        self._connection.commit()

    def enqueue(
        self,
        heartbeat: HeartbeatMessage,
        notification_type: str,
        detail: str,
    ) -> bool:
        """동일 장비·알림·상태의 활성 작업이 없을 때 큐에 추가한다.

        입력:
            heartbeat: 메일에 포함할 장비 상태.
            notification_type: ALERT, RECOVERY 또는 MISSING.
            detail: 메일에 표시할 알림 원인.
        반환:
            새 작업이 추가되면 ``True``, 활성 중복이면 ``False``.
        """
        dedupe_key = self._dedupe_key(heartbeat, notification_type)
        now = time.time()
        timestamp = datetime.now(timezone.utc).isoformat()
        cursor = self._connection.execute(
            """
            INSERT OR IGNORE INTO notification_queue (
                dedupe_key, device_id, notification_type, detail, status,
                attempt_count, next_attempt_at, raw_payload, kafka_key,
                kafka_topic, kafka_partition, kafka_offset, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'PENDING', 0, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dedupe_key,
                heartbeat.device_id,
                notification_type,
                detail,
                now,
                heartbeat.raw_text,
                heartbeat.key,
                heartbeat.topic,
                heartbeat.partition,
                heartbeat.offset,
                timestamp,
                timestamp,
            ),
        )
        self._connection.commit()
        return cursor.rowcount == 1

    def claim_next(self, now: float) -> NotificationJob | None:
        """발송 시각이 된 작업 하나를 트랜잭션으로 선점한다.

        입력:
            now: 비교에 사용할 Unix epoch 초.
        반환:
            선점한 ``NotificationJob`` 또는 대상이 없으면 ``None``.
        """
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            row = self._connection.execute(
                """
                SELECT * FROM notification_queue
                WHERE status IN ('PENDING', 'RETRY')
                  AND next_attempt_at <= ?
                ORDER BY id
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                self._connection.commit()
                return None
            self._connection.execute(
                """
                UPDATE notification_queue
                SET status = 'SENDING', updated_at = ?
                WHERE id = ?
                """,
                (datetime.now(timezone.utc).isoformat(), row["id"]),
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

        heartbeat = HeartbeatMessage.from_kafka_record(
            _QueuedKafkaRecord(row)
        )
        return NotificationJob(
            id=row["id"],
            dedupe_key=row["dedupe_key"],
            heartbeat=heartbeat,
            notification_type=row["notification_type"],
            detail=row["detail"],
            attempt_count=row["attempt_count"],
        )

    def mark_sent(self, job_id: int) -> None:
        """선점한 작업을 성공 상태로 완료한다.

        입력:
            job_id: 완료할 알림 큐 ID.
        반환:
            없음.
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        self._connection.execute(
            """
            UPDATE notification_queue
            SET status = 'SENT', sent_at = ?, updated_at = ?, last_error = NULL
            WHERE id = ? AND status = 'SENDING'
            """,
            (timestamp, timestamp, job_id),
        )
        self._connection.commit()

    def mark_failed(
        self,
        job_id: int,
        error: str,
        next_attempt_at: float,
        dead: bool,
    ) -> None:
        """발송 실패를 재시도 또는 최종 실패 상태로 기록한다.

        입력:
            job_id: 실패한 알림 큐 ID.
            error: 저장할 오류 설명.
            next_attempt_at: 다음 시도 Unix epoch 초.
            dead: 최대 횟수 초과로 재시도하지 않을지 여부.
        반환:
            없음.
        """
        self._connection.execute(
            """
            UPDATE notification_queue
            SET status = ?, attempt_count = attempt_count + 1,
                next_attempt_at = ?, last_error = ?, updated_at = ?
            WHERE id = ? AND status = 'SENDING'
            """,
            (
                "DEAD" if dead else "RETRY",
                next_attempt_at,
                error[:2000],
                datetime.now(timezone.utc).isoformat(),
                job_id,
            ),
        )
        self._connection.commit()

    def count_by_status(self, status: str) -> int:
        """테스트와 운영 점검을 위해 특정 상태 작업 수를 반환한다."""
        row = self._connection.execute(
            "SELECT COUNT(*) AS count FROM notification_queue WHERE status = ?",
            (status,),
        ).fetchone()
        return int(row["count"])

    def close(self) -> None:
        """SQLite 알림 큐 연결을 닫는다."""
        self._connection.close()

    @staticmethod
    def _dedupe_key(
        heartbeat: HeartbeatMessage, notification_type: str
    ) -> str:
        """활성 알림 중복 방지에 사용할 안정적인 식별 문자열을 만든다."""
        if notification_type == "MISSING":
            return f"{heartbeat.device_id}:MISSING"
        return (
            f"{heartbeat.device_id}:{notification_type}:"
            f"{heartbeat.status_level}:{heartbeat.status_code}"
        )


class _StoredKafkaRecord:
    """DB 행을 HeartbeatMessage 파서가 읽을 수 있는 record 형태로 변환한다."""

    def __init__(self, row: sqlite3.Row) -> None:
        """SQLite 조회 행을 보관한다.

        입력:
            row: ``device_states`` 테이블에서 조회한 한 행.
        반환:
            없음.
        """
        self._row = row

    def value(self) -> bytes:
        """저장된 CloudEvent 원문을 UTF-8 bytes로 반환한다.

        입력:
            없음.
        반환:
            Kafka value와 같은 형태의 UTF-8 bytes.
        """
        return self._row["raw_payload"].encode("utf-8")

    def key(self) -> bytes | None:
        """저장된 Kafka key를 bytes 또는 ``None``으로 반환한다.

        입력:
            없음.
        반환:
            UTF-8 Kafka key. 원래 key가 없었다면 ``None``.
        """
        value = self._row["kafka_key"]
        return value.encode("utf-8") if value is not None else None

    def topic(self) -> str:
        """저장된 Kafka topic 이름을 반환한다.

        입력:
            없음.
        반환:
            topic 문자열.
        """
        return self._row["kafka_topic"]

    def partition(self) -> int:
        """저장된 Kafka partition 번호를 반환한다.

        입력:
            없음.
        반환:
            partition 정수.
        """
        return self._row["kafka_partition"]

    def offset(self) -> int:
        """저장된 Kafka offset을 반환한다.

        입력:
            없음.
        반환:
            offset 정수.
        """
        return self._row["kafka_offset"]


class _QueuedKafkaRecord:
    """알림 큐 행을 HeartbeatMessage 파서용 Kafka record로 제공한다."""

    def __init__(self, row: sqlite3.Row) -> None:
        """조회된 알림 큐 행을 보관한다."""
        self._row = row

    def value(self) -> bytes:
        """저장된 CloudEvent JSON을 UTF-8 bytes로 반환한다."""
        return self._row["raw_payload"].encode("utf-8")

    def key(self) -> bytes | None:
        """저장된 Kafka key를 bytes 또는 None으로 반환한다."""
        value = self._row["kafka_key"]
        return value.encode("utf-8") if value is not None else None

    def topic(self) -> str:
        """저장된 Kafka topic을 반환한다."""
        return self._row["kafka_topic"]

    def partition(self) -> int:
        """저장된 Kafka partition을 반환한다."""
        return self._row["kafka_partition"]

    def offset(self) -> int:
        """저장된 Kafka offset을 반환한다."""
        return self._row["kafka_offset"]


def _configure_connection(
    connection: sqlite3.Connection, journal_mode: str
) -> None:
    """SQLite 연결에 journal, 동기화, 잠금 대기 정책을 적용한다.

    입력:
        connection: 설정할 SQLite 연결.
        journal_mode: 로컬/block PVC용 ``WAL`` 또는 NFS 호환용 ``DELETE``.
    반환:
        없음.
    예외:
        ValueError: 지원하지 않는 journal mode인 경우.
        sqlite3.Error: PRAGMA 적용에 실패한 경우.
    """
    normalized = journal_mode.strip().upper()
    if normalized not in {"WAL", "DELETE"}:
        raise ValueError("journal_mode must be WAL or DELETE")
    connection.execute(f"PRAGMA journal_mode={normalized}")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=30000")
