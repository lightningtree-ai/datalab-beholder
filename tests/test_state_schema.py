"""The additive schema: migration, backoff, and the daemon liveness row."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from datalab_beholder.scanner import FileEntry
from datalab_beholder.state import StateStore

# Exactly the schema a database written before this fork has.
LEGACY_SCHEMA = """
CREATE TABLE watched_paths (
    name TEXT PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE,
    last_hot_scan REAL,
    last_warm_scan REAL,
    last_cold_scan REAL,
    last_max_dir_mtime REAL
);
CREATE TABLE files__sycamore (
    path TEXT PRIMARY KEY,
    size INTEGER,
    modified REAL,
    last_seen REAL,
    last_synced REAL,
    status TEXT DEFAULT 'new',
    ids_json TEXT DEFAULT '{}'
);
"""


def _legacy_db(path: Path) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(LEGACY_SCHEMA)
    conn.execute(
        "INSERT INTO watched_paths (name, table_name) VALUES ('sycamore', 'sycamore')"
    )
    conn.execute(
        "INSERT INTO files__sycamore (path, size, modified, last_seen, status, "
        "ids_json) VALUES ('CEL-9998.ndax', 10, 1.0, 2.0, 'new', "
        '\'{"item_id": "CEL-9998"}\')'
    )
    conn.commit()
    conn.close()
    return path


class TestMigration:
    def test_an_old_database_gains_the_columns_and_keeps_its_rows(
        self, tmp_path: Path
    ) -> None:
        db = _legacy_db(tmp_path / "state.db")

        store = StateStore(db)
        cols = {r[1] for r in store._conn.execute("PRAGMA table_info(files__sycamore)")}
        assert {"attempts", "last_error", "last_error_at", "next_retry_at"} <= cols

        pending = store.get_pending_changes("sycamore")
        assert [e.path for e in pending] == ["CEL-9998.ndax"]
        assert pending[0].ids["item_id"] == "CEL-9998"
        store.close()

    def test_migrating_twice_is_a_no_op(self, tmp_path: Path) -> None:
        db = _legacy_db(tmp_path / "state.db")
        StateStore(db).close()
        StateStore(db).close()  # must not raise "duplicate column name"

    def test_a_table_created_by_an_older_binary_is_repaired(
        self, tmp_path: Path
    ) -> None:
        """`register_watched_path` uses CREATE TABLE IF NOT EXISTS, so an
        older binary can add a watched path to an already-migrated database
        and leave one table at the old shape. A schema version counter would
        say the file was done and skip it; checking each table does not."""
        db = _legacy_db(tmp_path / "state.db")
        StateStore(db).close()

        conn = sqlite3.connect(db)
        conn.executescript(
            "CREATE TABLE files__later (path TEXT PRIMARY KEY, size INTEGER, "
            "modified REAL, last_seen REAL, last_synced REAL, "
            "status TEXT DEFAULT 'new', ids_json TEXT DEFAULT '{}');"
            "INSERT INTO watched_paths (name, table_name) VALUES ('later', 'later');"
        )
        conn.commit()
        conn.close()

        store = StateStore(db)
        cols = {r[1] for r in store._conn.execute("PRAGMA table_info(files__later)")}
        assert "attempts" in cols
        store.close()

    def test_read_only_open_of_an_unmigrated_database_does_not_write(
        self, tmp_path: Path
    ) -> None:
        """A dry run or `status` must never migrate: `mode=ro` cannot write,
        and the daemon owns the schema."""
        db = _legacy_db(tmp_path / "state.db")

        store = StateStore(db, read_only=True)
        assert [e.path for e in store.get_pending_changes("sycamore")] == [
            "CEL-9998.ndax"
        ]
        cols = {r[1] for r in store._conn.execute("PRAGMA table_info(files__sycamore)")}
        assert "attempts" not in cols
        store.close()


class TestBackoff:
    def _store(self, tmp_path: Path) -> StateStore:
        store = StateStore(tmp_path / "state.db")
        store.register_watched_path("wp")
        store.upsert_entries(
            "wp",
            [FileEntry(path="a.ndax", size=1, modified=100.0, is_directory=False)],
        )
        return store

    def test_a_failure_defers_the_next_attempt(self, tmp_path: Path) -> None:
        store = self._store(tmp_path)
        assert [e.path for e in store.get_attachable("wp")] == ["a.ndax"]

        store.record_attach_failure("wp", "a.ndax", "HTTP 500")
        assert store.get_attachable("wp") == []
        # ...but it is still pending, not forgotten.
        assert [e.path for e in store.get_pending_changes("wp")] == ["a.ndax"]

        # Once the backoff has expired it is due again.
        later = time.time() + 7200
        assert [e.path for e in store.get_attachable("wp", now=later)] == ["a.ndax"]
        store.close()

    def test_backoff_is_capped(self, tmp_path: Path) -> None:
        store = self._store(tmp_path)
        for _ in range(20):
            store.record_attach_failure("wp", "a.ndax", "HTTP 500")
        row = store._conn.execute(
            "SELECT attempts, last_error_at, next_retry_at FROM files__wp"
        ).fetchone()
        assert row["attempts"] == 20
        assert row["next_retry_at"] - row["last_error_at"] == pytest.approx(3600)
        store.close()

    def test_a_file_that_changed_since_failing_ignores_its_backoff(
        self, tmp_path: Path
    ) -> None:
        """A growing file must never sit out a backoff window."""
        store = self._store(tmp_path)
        store.record_attach_failure("wp", "a.ndax", "locked by another process")
        assert store.get_attachable("wp") == []

        # The next scan sees it has grown.
        store.upsert_entries(
            "wp",
            [
                FileEntry(
                    path="a.ndax",
                    size=2,
                    modified=time.time() + 10,
                    is_directory=False,
                )
            ],
        )
        assert [e.path for e in store.get_attachable("wp")] == ["a.ndax"]
        store.close()

    def test_a_successful_sync_clears_the_failure_state(self, tmp_path: Path) -> None:
        store = self._store(tmp_path)
        store.record_attach_failure("wp", "a.ndax", "HTTP 500")
        store.mark_synced("wp", ["a.ndax"])
        row = store._conn.execute(
            "SELECT attempts, last_error, next_retry_at FROM files__wp"
        ).fetchone()
        assert (row["attempts"], row["last_error"], row["next_retry_at"]) == (
            0,
            None,
            None,
        )
        store.close()

    def test_the_error_is_truncated(self, tmp_path: Path) -> None:
        store = self._store(tmp_path)
        store.record_attach_failure("wp", "a.ndax", "x" * 5000)
        row = store._conn.execute("SELECT last_error FROM files__wp").fetchone()
        assert len(row["last_error"]) == 500
        store.close()


class TestDaemonState:
    def test_write_is_an_upsert_of_one_row(self, tmp_path: Path) -> None:
        store = StateStore(tmp_path / "state.db")
        store.write_daemon_state(pid=1, version="0.1.0", sync_status="idle")
        store.write_daemon_state(sync_status="error")

        rows = store._conn.execute(
            "SELECT id, pid, version, sync_status FROM daemon_state"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["pid"] == 1
        assert rows[0]["version"] == "0.1.0"
        assert rows[0]["sync_status"] == "error"
        store.close()
