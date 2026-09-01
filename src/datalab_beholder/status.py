"""One read-only snapshot of what the daemon is doing.

Everything here comes out of `state.db` over a read-only connection, not
from a live `BeholderDaemon`. That is the whole point: the GUI reads
attributes off the daemon object in-process, so nothing else can see them,
and `datalab-beholder status` can only say how many files are pending — not
whether those files are moving or wedged. Reading the database instead
means the dashboard works from another process, against a database copied
off the instrument PC, and with the daemon stopped.

The snapshot is plain JSON-able data with a `schema_version`, so anything
else that wants to watch a beholder (dataleaf, a monitoring script) can
poll `/api/status` rather than parse `beholder.log`.

Nothing here ever emits an API key.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

# Bump when a key changes meaning or disappears. Additions do not need it.
SCHEMA_VERSION = 1

# How many of each list to include. The page is a status display, not a
# file browser, and the payload is fetched every few seconds.
RECENT_LIMIT = 10
PROBLEM_LIMIT = 50

# A daemon that has not ticked in this long is not running. The tick is 1 Hz
# and the heartbeat is written every 5 s, so this is generous.
STALE_AFTER_SECONDS = 30.0


def _connect(db_path: Path) -> sqlite3.Connection | None:
    """Open `db_path` read-only, or None if it is not there."""
    if not Path(db_path).exists():
        return None
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def _daemon_section(conn: sqlite3.Connection | None, now: float) -> dict[str, Any]:
    """Liveness and timing, from the `daemon_state` row the daemon writes."""
    blank = {
        "running": False,
        "pid": None,
        "host": None,
        "version": None,
        "started_at": None,
        "uptime_seconds": None,
        "last_tick_at": None,
        "last_attach_at": None,
        "next_attach_at": None,
        "seconds_to_next_attach": None,
        "attach_interval": None,
        "sync_status": None,
    }
    if conn is None or not _table_exists(conn, "daemon_state"):
        return blank

    row = conn.execute("SELECT * FROM daemon_state WHERE id = 1").fetchone()
    if row is None:
        return blank

    last_tick = row["last_tick_at"]
    # Liveness is judged on the heartbeat, not on whether the pid exists:
    # pids are reused, and checking one is unreliable across platforms.
    running = last_tick is not None and (now - last_tick) < STALE_AFTER_SECONDS
    next_attach = row["next_attach_at"]
    return {
        "running": running,
        "pid": row["pid"],
        "host": row["host"],
        "version": row["version"],
        "started_at": row["started_at"],
        "uptime_seconds": (now - row["started_at"]) if row["started_at"] else None,
        "last_tick_at": last_tick,
        "last_attach_at": row["last_attach_at"],
        "next_attach_at": next_attach,
        # Only meaningful while the daemon is actually ticking; a stale
        # deadline from a dead process would otherwise read as "due now".
        "seconds_to_next_attach": (
            max(0.0, next_attach - now) if (running and next_attach) else None
        ),
        "attach_interval": row["attach_interval"],
        "sync_status": row["sync_status"],
    }


def _path_section(
    conn: sqlite3.Connection, registry: sqlite3.Row, wp: Any, now: float
) -> dict[str, Any]:
    table = f"files__{registry['table_name']}"
    if not _table_exists(conn, table):
        return {"name": registry["name"], "error": "no file table"}

    cols = _columns(conn, table)
    # A database written before the failure-tracking columns existed, opened
    # read-only, cannot be migrated. Report nulls rather than refusing.
    has_failures = "attempts" in cols

    counts = {
        row["status"]: row["n"]
        for row in conn.execute(
            f"SELECT status, COUNT(*) AS n FROM {table} GROUP BY status"
        )
    }
    pending = sum(n for s, n in counts.items() if s in ("new", "modified"))

    recent = [
        dict(row)
        for row in conn.execute(
            f"SELECT path, size, last_synced FROM {table} "
            "WHERE last_synced IS NOT NULL "
            "ORDER BY last_synced DESC LIMIT ?",
            (RECENT_LIMIT,),
        )
    ]

    problems: list[dict[str, Any]] = []
    if has_failures:
        problems = [
            dict(row)
            for row in conn.execute(
                f"SELECT path, attempts, last_error, last_error_at, next_retry_at "
                f"FROM {table} WHERE attempts > 0 AND status != 'synced' "
                "ORDER BY attempts DESC, last_error_at DESC LIMIT ?",
                (PROBLEM_LIMIT,),
            )
        ]

    # Pending but never attempted, and with no id to attach it to: it will
    # sit there for ever and nothing else on the page would explain why.
    unattachable = conn.execute(
        f"SELECT COUNT(*) AS n FROM {table} "
        "WHERE status IN ('new','modified') "
        "AND (ids_json IS NULL OR ids_json NOT LIKE '%\"item_id\"%')"
    ).fetchone()["n"]

    scan = {}
    for tier in ("hot", "warm", "cold"):
        last = registry[f"last_{tier}_scan"]
        interval = getattr(wp.scan, f"{tier}_interval", None) if wp else None
        scan[tier] = {
            "last": last,
            "interval": interval,
            "next": (last + interval) if (last and interval) else None,
        }

    path = getattr(wp, "path", None) if wp else None
    return {
        "name": registry["name"],
        "path": str(path) if path else None,
        "path_exists": Path(path).exists() if path else None,
        "datalab": getattr(wp, "datalab", None) if wp else None,
        "counts": counts,
        "pending": pending,
        "stuck": len(problems),
        "unattachable": unattachable,
        "failure_tracking": has_failures,
        "last_sync": conn.execute(
            f"SELECT MAX(last_synced) AS m FROM {table}"
        ).fetchone()["m"],
        "scans": scan,
        "recent_syncs": recent,
        "problems": problems,
    }


def collect_status(config: Any, db_path: Path | None = None) -> dict[str, Any]:
    """Build the snapshot. Never raises: an unreachable or missing database
    is itself part of what the dashboard has to report."""
    now = time.time()
    db_path = Path(db_path if db_path is not None else config.state_db)
    snapshot: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now,
        "state_db": str(db_path),
        "state_db_exists": db_path.exists(),
        "daemon": _daemon_section(None, now),
        "watched_paths": [],
        "error": None,
    }

    conn = None
    try:
        conn = _connect(db_path)
        if conn is None:
            snapshot["error"] = "state database does not exist yet"
            return snapshot

        snapshot["daemon"] = _daemon_section(conn, now)

        by_name = {wp.name: wp for wp in (config.watched_paths if config else [])}
        registry = conn.execute(
            "SELECT name, table_name, last_hot_scan, last_warm_scan, "
            "last_cold_scan, last_max_dir_mtime FROM watched_paths ORDER BY name"
        ).fetchall()
        snapshot["watched_paths"] = [
            _path_section(conn, row, by_name.get(row["name"]), now) for row in registry
        ]
    except Exception as e:  # pragma: no cover - defensive
        snapshot["error"] = f"{type(e).__name__}: {e}"
    finally:
        if conn is not None:
            conn.close()

    return snapshot
