from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Protocol

from .message import HeartbeatMessage


@dataclass(frozen=True)
class StoredDeviceState:
    heartbeat: HeartbeatMessage
    last_seen_at: float
    status_signature: tuple[str, str]
    stale_notified: bool


class DeviceStateRepository(Protocol):
    """Persistence boundary that can later be implemented with PostgreSQL."""

    def load_all(self) -> list[StoredDeviceState]: ...

    def save(
        self,
        heartbeat: HeartbeatMessage,
        last_seen_at: float,
        stale_notified: bool,
    ) -> None: ...

    def mark_stale(self, device_id: str) -> None: ...

    def close(self) -> None: ...


class SQLiteDeviceStateRepository:
    def __init__(self, database_path: str) -> None:
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
        self._connection.close()


class _StoredKafkaRecord:
    def __init__(self, row: sqlite3.Row) -> None:
        self._row = row

    def value(self) -> bytes:
        return self._row["raw_payload"].encode("utf-8")

    def key(self) -> bytes | None:
        value = self._row["kafka_key"]
        return value.encode("utf-8") if value is not None else None

    def topic(self) -> str:
        return self._row["kafka_topic"]

    def partition(self) -> int:
        return self._row["kafka_partition"]

    def offset(self) -> int:
        return self._row["kafka_offset"]
