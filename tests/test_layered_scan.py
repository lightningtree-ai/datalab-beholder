"""End-to-end smoke tests for LocalWatchedPath.{hot,warm,cold}_scan."""

from __future__ import annotations

import os
import time
from pathlib import Path

from datalab_beholder.config import LocalWatchedPath
from datalab_beholder.state import StateStore


def _make(name: str, root: Path, **scan_overrides) -> LocalWatchedPath:
    return LocalWatchedPath(
        path=root,
        name=name,
        datalab="d",
        scan=scan_overrides or {},  # type: ignore[arg-type]
    )


def _bump_mtime(path: Path, seconds_ahead: float) -> None:
    """Force a file's mtime forward — bypasses sub-second clock issues."""
    target = time.time() + seconds_ahead
    os.utime(path, (target, target))


class TestColdScan:
    def test_cold_scan_seeds_state(self, tmp_path: Path, tmp_tree: Path) -> None:
        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        wp = _make("wp", tmp_tree)

        diff = wp.cold_scan(store)

        paths = {e.path for e in diff.new}
        assert "file1.csv" in paths
        ts = store.get_scan_timestamps("wp")
        assert ts.cold is not None
        # cold subsumes warm + hot.
        assert ts.warm == ts.cold
        assert ts.hot == ts.cold

    def test_cold_scan_detects_in_place_modification(
        self, tmp_path: Path, tmp_tree: Path
    ) -> None:
        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        wp = _make("wp", tmp_tree)
        wp.cold_scan(store)

        target = tmp_tree / "file1.csv"
        target.write_text("rewritten")
        _bump_mtime(target, 10)

        diff = wp.cold_scan(store)
        assert "file1.csv" in {e.path for e in diff.modified}


class TestWarmScan:
    def test_warm_scan_discovers_new_file(self, tmp_path: Path, tmp_tree: Path) -> None:
        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        wp = _make("wp", tmp_tree)

        # Seed via cold scan, anchor warm short-circuit.
        wp.cold_scan(store)

        # Drop a new file into a subdir; bump the dir's mtime so the
        # warm scan recognises it as changed.
        new_file = tmp_tree / "subdir" / "freshly_added.csv"
        new_file.write_text("hi")
        _bump_mtime(tmp_tree / "subdir", 10)

        diff = wp.warm_scan(store)
        assert "subdir/freshly_added.csv" in {e.path for e in diff.new}

    def test_warm_scan_skips_unchanged_dirs(
        self, tmp_path: Path, tmp_tree: Path
    ) -> None:
        """In-place file rewrites without a parent-dir mtime bump are
        invisible to the warm scan — that's by design; cold catches them."""
        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        wp = _make("wp", tmp_tree)
        wp.cold_scan(store)

        target = tmp_tree / "file1.csv"
        target.write_text("rewritten")
        _bump_mtime(target, 10)
        # Explicitly do NOT bump the parent dir's mtime.
        _bump_mtime(tmp_tree, -100)

        diff = wp.warm_scan(store)
        # File1 is in the (unchanged) root dir, so warm doesn't see the rewrite.
        assert "file1.csv" not in {e.path for e in diff.modified}


class TestHotScan:
    def test_hot_scan_picks_up_recent_modification(
        self, tmp_path: Path, tmp_tree: Path
    ) -> None:
        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        wp = _make("wp", tmp_tree, hot_window=86400)
        wp.cold_scan(store)
        store.mark_synced("wp", [e.path for e in store.get_pending_changes("wp")])

        target = tmp_tree / "file1.csv"
        target.write_text("rewritten")
        _bump_mtime(target, 10)
        # The hot scan reads recently_modified_paths(since=now-hot_window).
        # The file's stored modified timestamp is from the cold scan, so it
        # already qualifies — we just need it to differ on size/mtime.

        diff = wp.hot_scan(store)
        assert "file1.csv" in {e.path for e in diff.modified}

    def test_hot_scan_marks_missing_file_deleted(
        self, tmp_path: Path, tmp_tree: Path
    ) -> None:
        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        wp = _make("wp", tmp_tree)
        wp.cold_scan(store)

        (tmp_tree / "file1.csv").unlink()

        diff = wp.hot_scan(store)
        assert "file1.csv" in {e.path for e in diff.deleted}

    def test_hot_scan_ignores_files_outside_window(
        self, tmp_path: Path, tmp_tree: Path
    ) -> None:
        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        # 1-second window means almost every file is "stale" for the hot tier.
        wp = _make("wp", tmp_tree, hot_window=1)
        wp.cold_scan(store)
        # Force every file's stored mtime way into the past so none are
        # eligible.
        store._conn.execute("UPDATE files__wp SET modified = 0")
        store._conn.commit()

        diff = wp.hot_scan(store)
        assert not diff.has_changes


class TestWarmScanAnchor:
    """The warm-scan short-circuit anchor (`last_max_dir_mtime`)."""

    def test_future_dated_dir_does_not_blind_the_whole_tree(
        self, tmp_path: Path
    ) -> None:
        """A directory with an mtime in the future must not become the
        anchor.

        `max_dir_mtime` is a single high-water mark for the entire watched
        path. Data copied in from a USB stick or a NAS with timestamps
        preserved, or a machine whose clock is ahead, lands a directory
        mtime in the future; that used to park the anchor there and make
        every later warm scan short-circuit every directory until
        wall-clock caught up. With `cold_interval: null` it never does.
        """
        root = tmp_path / "neware"
        root.mkdir()
        live = root / "channel-1"
        live.mkdir()
        (live / "CEL-079.mpr").write_bytes(b"x")

        imported = root / "imported-from-usb"
        imported.mkdir()
        _bump_mtime(imported, 7 * 86400)  # a week ahead

        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        wp = _make("wp", root, warm_interval=0, cold_interval=None)

        wp.warm_scan(store)
        anchor = store.get_scan_timestamps("wp").max_dir_mtime
        assert anchor is not None and anchor <= time.time()

        # A brand new file lands in the live directory.
        (live / "CEL-081.mpr").write_bytes(b"y")

        diff = wp.warm_scan(store)
        assert [e.path for e in diff.new] == ["channel-1/CEL-081.mpr"]
        store.close()

    def test_file_landing_on_the_anchor_tick_is_still_found(
        self, tmp_path: Path
    ) -> None:
        """The short-circuit compares `>=`, not `>`.

        On a filesystem with coarse mtime granularity (exFAT rounds to 2 s,
        as do some SMB shares) a file can land in its parent directory
        within the same tick as the previous scan. With a strict `>` that
        file was invisible to every later warm scan.
        """
        root = tmp_path / "neware"
        root.mkdir()
        (root / "CEL-079.mpr").write_bytes(b"x")

        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        wp = _make("wp", root, warm_interval=0, cold_interval=None)
        wp.warm_scan(store)
        anchor = store.get_scan_timestamps("wp").max_dir_mtime
        assert anchor is not None

        # New file, but the directory mtime lands exactly on the anchor.
        (root / "CEL-081.mpr").write_bytes(b"y")
        os.utime(root, (anchor, anchor))

        diff = wp.warm_scan(store)
        assert [e.path for e in diff.new] == ["CEL-081.mpr"]
        store.close()


class TestWarmScanDeletionScope:
    def test_untouched_subdir_is_not_mass_marked_deleted(
        self, tmp_path: Path
    ) -> None:
        """A warm scan may only call a file deleted if it actually looked in
        the directory that would hold it.

        One new file at the top level moves the root's mtime, so the root is
        re-stat'd while a quiet subdirectory is short-circuited. The scope
        filter used to widen to the whole tree in that case, so every file
        in the untouched subdirectory was marked deleted, pruned from state
        by `remove_deleted`, and then re-uploaded from scratch by the next
        cold scan.
        """
        root = tmp_path / "neware"
        root.mkdir()
        sub = root / "old-cells"
        sub.mkdir()
        (sub / "CEL-001.mpr").write_bytes(b"x")

        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        wp = _make("wp", root, warm_interval=0, cold_interval=None)
        wp.cold_scan(store)
        store.mark_synced("wp", ["old-cells/CEL-001.mpr"])

        # The subdirectory goes quiet; a new file lands at the top level,
        # which moves the root's mtime but not the subdirectory's.
        _bump_mtime(sub, -30 * 86400)
        (root / "CEL-081.mpr").write_bytes(b"y")

        diff = wp.warm_scan(store)

        assert [e.path for e in diff.deleted] == []
        assert [e.path for e in diff.new] == ["CEL-081.mpr"]

        store.remove_deleted("wp")
        rows = {
            r[0]
            for r in store._conn.execute("SELECT path FROM files__wp").fetchall()
        }
        assert "old-cells/CEL-001.mpr" in rows
        store.close()

    def test_a_file_really_removed_from_a_scanned_dir_is_still_detected(
        self, tmp_path: Path
    ) -> None:
        """The narrower scope must not cost real deletion detection."""
        root = tmp_path / "neware"
        root.mkdir()
        (root / "CEL-079.mpr").write_bytes(b"x")
        (root / "CEL-080.mpr").write_bytes(b"y")

        store = StateStore(tmp_path / "s.db")
        store.register_watched_path("wp")
        wp = _make("wp", root, warm_interval=0, cold_interval=None)
        wp.cold_scan(store)

        (root / "CEL-080.mpr").unlink()

        diff = wp.warm_scan(store)
        assert [e.path for e in diff.deleted] == ["CEL-080.mpr"]
        store.close()
