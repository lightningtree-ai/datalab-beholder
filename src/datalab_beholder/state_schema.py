"""Schema this fork adds on top of the tables `state.py` creates.

Everything here is additive — `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE
ADD COLUMN` with constant defaults — so a `state.db` upgrades in place, and
an older binary opening the same file keeps working. That second property
holds only because every statement in `state.py` names its columns
explicitly; there is no `SELECT *` anywhere, and there must not be.

Per-table shape is always established with `PRAGMA table_info` rather than a
single schema-version number. `register_watched_path` uses `CREATE TABLE IF
NOT EXISTS`, so an older binary can add a new watched path to an
already-upgraded database and leave one table behind at the old shape; a
version counter would say the file was migrated and skip it.
"""

from __future__ import annotations

import sqlite3

# Recorded on the file row itself, so the state of a file and the reason it
# is stuck are one read. Written from exactly one place: the attach loop.
FILE_TABLE_COLUMNS: dict[str, str] = {
    "attempts": "INTEGER NOT NULL DEFAULT 0",
    "last_error": "TEXT",
    "last_error_at": "REAL",
    "next_retry_at": "REAL",
}

# One row, id 1. The daemon's own liveness and timing, so a reader in
# another process can answer "is it running" and "when is the next attach"
# without the daemon being there to ask. `_last_attach_mono` is monotonic
# and in-memory, so it dies with the process.
DAEMON_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS daemon_state (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    pid             INTEGER,
    host            TEXT,
    version         TEXT,
    started_at      REAL,
    last_tick_at    REAL,
    last_attach_at  REAL,
    next_attach_at  REAL,
    attach_interval REAL,
    sync_status     TEXT
);
"""

# Truncated before storing: an error string is for a human reading a
# dashboard, not a transcript.
MAX_ERROR_CHARS = 500


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Column names of `table`, or an empty set if it does not exist."""
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def ensure_file_table_columns(conn: sqlite3.Connection, table_name: str) -> None:
    """Add any missing failure-tracking columns to one `files__*` table.

    `ALTER TABLE ADD COLUMN` is a metadata-only operation for columns that
    are neither PRIMARY KEY nor UNIQUE and whose default is a constant, so
    this is instant even on a large table on a slow instrument disk.
    """
    have = table_columns(conn, f"files__{table_name}")
    if not have:  # table does not exist yet
        return
    for column, decl in FILE_TABLE_COLUMNS.items():
        if column not in have:
            conn.execute(f"ALTER TABLE files__{table_name} ADD COLUMN {column} {decl}")


def migrate(conn: sqlite3.Connection) -> None:
    """Bring an open, writable connection up to this fork's schema."""
    conn.executescript(DAEMON_STATE_SCHEMA)
    for row in conn.execute("SELECT table_name FROM watched_paths").fetchall():
        ensure_file_table_columns(conn, row[0])
    conn.commit()
