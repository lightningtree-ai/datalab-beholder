"""The read-only status snapshot and the server that hands it out."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from pathlib import Path

from datalab_beholder.config import BeholderConfig
from datalab_beholder.scanner import FileEntry
from datalab_beholder.server import StatusServer
from datalab_beholder.state import StateStore
from datalab_beholder.status import STALE_AFTER_SECONDS, collect_status


def _config(tmp_path: Path, root: Path) -> BeholderConfig:
    return BeholderConfig(
        datalabs=[
            {
                "name": "lt",
                "url": "https://dl.example.org",
                "api_key": "SUPER-SECRET-KEY",
            }
        ],
        watched_paths=[
            {
                "path": str(root),
                "name": "sycamore",
                "include_patterns": ["*.ndax"],
                "scan": {
                    "hot_interval": 60,
                    "warm_interval": 3600,
                    "cold_interval": 86400,
                },
            }
        ],
        state_db=tmp_path / "state.db",
        status_port=0,
    )


def _populated(tmp_path: Path) -> BeholderConfig:
    root = tmp_path / "data"
    root.mkdir()
    config = _config(tmp_path, root)
    store = StateStore(config.state_db)
    store.register_watched_path("sycamore")
    store.upsert_entries(
        "sycamore",
        [
            FileEntry(path="a.ndax", size=10, modified=1.0, is_directory=False),
            FileEntry(path="b.ndax", size=20, modified=2.0, is_directory=False),
            FileEntry(path="c.ndax", size=30, modified=3.0, is_directory=False),
        ],
    )
    store.mark_synced("sycamore", ["a.ndax"])
    store.record_attach_failure("sycamore", "b.ndax", "HTTP 500 error at /upload-file/")
    store.write_daemon_state(
        pid=4242,
        host="cycler-pc",
        version="0.1.0rc6",
        started_at=time.time() - 3600,
        last_tick_at=time.time(),
        last_attach_at=time.time() - 60,
        next_attach_at=time.time() + 1140,
        attach_interval=1200,
        sync_status="idle",
    )
    store.close()
    return config


class TestCollectStatus:
    def test_a_missing_database_is_reported_not_raised(self, tmp_path: Path) -> None:
        config = _config(tmp_path, tmp_path)
        snap = collect_status(config)
        assert snap["state_db_exists"] is False
        assert snap["error"]
        assert snap["daemon"]["running"] is False
        assert snap["watched_paths"] == []

    def test_counts_and_problems(self, tmp_path: Path) -> None:
        snap = collect_status(_populated(tmp_path))
        (path,) = snap["watched_paths"]

        assert path["name"] == "sycamore"
        assert path["counts"]["synced"] == 1
        assert path["pending"] == 2
        assert path["stuck"] == 1
        assert path["failure_tracking"] is True

        (problem,) = path["problems"]
        assert problem["path"] == "b.ndax"
        assert problem["attempts"] == 1
        assert "HTTP 500" in problem["last_error"]
        assert problem["next_retry_at"] > problem["last_error_at"]

        assert [f["path"] for f in path["recent_syncs"]] == ["a.ndax"]

    def test_scan_tiers_carry_their_next_due_time(self, tmp_path: Path) -> None:
        config = _populated(tmp_path)
        store = StateStore(config.state_db)
        store.update_scan_timestamp("sycamore", "hot", 1000.0)
        store.close()

        (path,) = collect_status(config)["watched_paths"]
        assert path["scans"]["hot"]["last"] == 1000.0
        assert path["scans"]["hot"]["interval"] == 60
        assert path["scans"]["hot"]["next"] == 1060.0

    def test_liveness_comes_from_the_heartbeat_not_the_pid(
        self, tmp_path: Path
    ) -> None:
        config = _populated(tmp_path)
        assert collect_status(config)["daemon"]["running"] is True

        store = StateStore(config.state_db)
        store.write_daemon_state(last_tick_at=time.time() - STALE_AFTER_SECONDS - 10)
        store.close()

        daemon = collect_status(config)["daemon"]
        assert daemon["running"] is False
        # A deadline left behind by a dead process must not read as "due now".
        assert daemon["seconds_to_next_attach"] is None
        assert daemon["pid"] == 4242  # still reported, just not trusted for liveness

    def test_no_credentials_are_exposed(self, tmp_path: Path) -> None:
        """The snapshot is served over HTTP; it must carry no secrets.

        Note the test name deliberately avoids the literal key name: pytest
        puts it in tmp_path, which appears in the payload as the database
        location.
        """
        blob = json.dumps(collect_status(_populated(tmp_path)), default=str)
        assert "SUPER-SECRET-KEY" not in blob
        assert "api" + "_key" not in blob

    def test_files_with_no_item_id_are_counted(self, tmp_path: Path) -> None:
        """Otherwise a pending count that never moves has no explanation."""
        config = _populated(tmp_path)
        store = StateStore(config.state_db)
        store.upsert_entries(
            "sycamore",
            [
                FileEntry(
                    path="d.ndax",
                    size=1,
                    modified=1.0,
                    is_directory=False,
                    ids={"item_id": "CEL-1"},
                )
            ],
        )
        store.close()

        (path,) = collect_status(config)["watched_paths"]
        # a, b, c have no ids; d has one. a is synced, so three pending rows
        # remain of which the three original ones lack an item_id.
        assert path["unattachable"] == 2


class TestStatusServer:
    def _serve(self, snapshot):
        server = StatusServer(snapshot, port=0)
        assert server.start()
        # port=0 asks the OS for a free one; read back what it gave us.
        port = server._httpd.server_address[1]
        return server, f"http://127.0.0.1:{port}"

    def test_serves_the_page_and_the_json(self, tmp_path: Path) -> None:
        config = _populated(tmp_path)
        server, base = self._serve(lambda: collect_status(config))
        try:
            page = urllib.request.urlopen(base + "/", timeout=5).read().decode()
            assert "<title>beholder</title>" in page
            assert "api/status" in page

            body = urllib.request.urlopen(base + "/api/status", timeout=5).read()
            data = json.loads(body)
            assert data["schema_version"] == 1
            assert data["watched_paths"][0]["stuck"] == 1
        finally:
            server.stop()

    def test_unknown_routes_404(self, tmp_path: Path) -> None:
        server, base = self._serve(lambda: {"ok": True})
        try:
            try:
                urllib.request.urlopen(base + "/etc/passwd", timeout=5)
                raise AssertionError("expected 404")
            except urllib.error.HTTPError as e:
                assert e.code == 404
        finally:
            server.stop()

    def test_a_failing_snapshot_does_not_take_the_server_down(
        self, tmp_path: Path
    ) -> None:
        def boom():
            raise RuntimeError("nope")

        server, base = self._serve(boom)
        try:
            data = json.loads(
                urllib.request.urlopen(base + "/api/status", timeout=5).read()
            )
            assert "nope" in data["error"]
            # ...and it is still serving.
            assert urllib.request.urlopen(base + "/", timeout=5).status == 200
        finally:
            server.stop()

    def test_a_broken_log_handler_does_not_cost_the_client_its_reply(
        self, tmp_path: Path
    ) -> None:
        """Reporting the failure must not become a second failure.

        The GUI attaches a handler that writes into a Tk widget, and Tk
        raises when touched from a thread other than its main loop, which is
        exactly where the server answers from. Logging the snapshot error
        before sending the response meant the client got a closed connection
        instead of the error it asked for.
        """
        import logging

        class Exploding(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                raise RuntimeError("main thread is not in main loop")

        def boom():
            raise RuntimeError("nope")

        handler = Exploding()
        logging.getLogger("datalab_beholder").addHandler(handler)
        server, base = self._serve(boom)
        try:
            data = json.loads(
                urllib.request.urlopen(base + "/api/status", timeout=5).read()
            )
            assert "nope" in data["error"]
        finally:
            server.stop()
            logging.getLogger("datalab_beholder").removeHandler(handler)
