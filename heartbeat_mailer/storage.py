from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Protocol

from .message import HeartbeatMessage


@dataclass(frozen=True)
class StoredDeviceState:
    """영구 저장소에서 복원한 장비 상태와 알림 메타데이터."""

    heartbeat: HeartbeatMessage
    last_seen_at: float
    status_signature: tuple[str, str]
    stale_notified: bool


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

    def __init__(self, database_path: str) -> None:
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
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._connection.execute("PRAGMA busy_timeout=30000")
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
