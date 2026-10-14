"""Main daemon loop: scan watched paths, attach matched files to items."""

from __future__ import annotations

import fnmatch
import logging
import os
import platform
import signal
import threading
import time
from dataclasses import dataclass
from typing import Any

from datalab_beholder import __version__
from datalab_beholder.client import BeholderClient
from datalab_beholder.config import BeholderConfig, LocalWatchedPath
from datalab_beholder.state import StateStore

log = logging.getLogger(__name__)

TICK_SECONDS = 1.0
# How often the daemon persists its liveness row; the tick is 1 Hz.
HEARTBEAT_SECONDS = 5.0


def _match_block_type(filename: str, block_patterns: dict[str, str]) -> str | None:
    """Return the block type for the first pattern in ``block_patterns``
    that matches ``filename``, or ``None`` if nothing matches."""
    for pattern, block_type in block_patterns.items():
        if fnmatch.fnmatch(filename, pattern):
            return block_type
    return None


class BeholderDaemon:
    """Daemon that scans watched paths on layered cadences and attaches
    files whose path matched an ``id_pattern`` to their datalab item.

    Single-threaded by design — `tick()` runs all scan + attach work on
    the calling thread, so SQLite never crosses thread boundaries and
    the loop is safe to drive from a CLI sleep loop or a Tkinter
    `root.after` callback.

    Each watched path runs three independent scan cadences:

    * **hot** — re-stat recently-modified files only (cheap, frequent).
    * **warm** — directory-mtime-aware walk; discovers new files in
      active subtrees, skips per-file stats in cold subtrees.
    * **cold** — full walk; ground-truth reconciliation. Optional
      (`scan.cold_interval = null`) for write-once archives.

    Attaching runs on its own cadence (``sync.metadata_interval``)
    independent of the scan loop: scans accumulate diffs in state, the
    attach pass drains whatever is pending and has an ``item_id``.
    """

    def __init__(self, config: BeholderConfig):
        self._config = config
        self._running = False
        self._state = StateStore(config.state_db)
        for wp in config.watched_paths:
            self._state.register_watched_path(wp.name)

        self._clients: dict[str, BeholderClient] = self._build_clients(config)
        # Routing table: each watched path resolves to exactly one client.
        # Cross-field validation in BeholderConfig guarantees wp.datalab is set
        # and references a real datalab name.
        self._clients_by_wp: dict[str, BeholderClient] = {
            wp.name: self._clients[wp.datalab]
            for wp in config.watched_paths
            if wp.datalab
        }

        self._daemon_id = self._build_daemon_id()

        # Observable status for the GUI
        self.last_scan_time: float | None = None
        self.last_attach_time: float | None = None
        self.pending_count: int = 0
        self.sync_status: str = "idle"  # "idle" | "attaching" | "error"

        # Watched paths still owed their startup scan; see `setup`.
        self._pending_full_scan: set[str] = set()
        self._last_heartbeat_mono: float = float("-inf")
        # --- lightningtree fork: status dashboard ---
        self._status_server: Any = None
        # Set from the dashboard's threads, read and cleared on ours.
        self._sync_requested = threading.Event()
        # --- end fork ---

    @property
    def config(self) -> BeholderConfig:
        return self._config

    @property
    def clients(self) -> dict[str, BeholderClient]:
        """Mapping of datalab name → client. Read-only view for callers."""
        return self._clients

    def _client_for(self, wp: Any) -> BeholderClient:
        return self._clients_by_wp[wp.name]

    @staticmethod
    def _build_client(datalab: Any, log_level: str) -> BeholderClient:
        """Construct a client for one configured datalab.

        ``BaseDatalabClient`` reads its API key from the
        ``DATALAB_API_KEY`` env var (or a deployment-prefixed variant)
        during ``__init__``, so we set it from ``DatalabConfig.api_key``
        for the duration of construction — fine because the client
        snapshots the key into its own ``_headers`` dict.
        """
        import os

        prev_env = os.environ.get("DATALAB_API_KEY")
        try:
            if datalab.api_key and datalab.api_key != "your-api-key-here":
                os.environ["DATALAB_API_KEY"] = datalab.api_key
            # Otherwise leave whatever was already in the env so users
            # can keep their key out of the YAML entirely.
            return BeholderClient(
                datalab_api_url=datalab.url,
                log_level=log_level,
            )
        finally:
            if prev_env is None:
                os.environ.pop("DATALAB_API_KEY", None)
            else:
                os.environ["DATALAB_API_KEY"] = prev_env

    @staticmethod
    def _build_clients(config: BeholderConfig) -> dict[str, BeholderClient]:
        """Construct one client per configured datalab."""
        return {
            d.name: BeholderDaemon._build_client(d, config.log_level)
            for d in config.datalabs
        }

    def _build_daemon_id(self) -> str:
        names = sorted(wp.name for wp in self._config.watched_paths)
        return "-".join(names).lower().replace(" ", "-")

    def setup(self) -> None:
        """Register signal handlers and initialise loop timers.

        No scanning happens here — the first `tick()` will trigger the
        appropriate scan tier per watched path based on its registry
        timestamps (which start out NULL → "infinitely stale" → run now).
        """
        log.info(
            "Starting beholder daemon (id=%s) version %s on %s, pid %d",
            self._daemon_id,
            __version__,
            platform.platform(terse=True),
            os.getpid(),
        )
        log.info("State database: %s", self._config.state_db)
        log.info(
            "Watching %d path(s): %s",
            len(self._config.watched_paths),
            ", ".join(
                f"{wp.name} ({getattr(wp, 'path', wp.kind)})"
                for wp in self._config.watched_paths
            ),
        )
        log.info("Attach interval: %ds", self._config.sync.metadata_interval)

        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, self._handle_signal)

        self._last_attach_mono = (
            time.monotonic() - self._config.sync.metadata_interval
        )  # so attach runs on the first tick

        # The stored per-tier timestamps answer "when did a scan last
        # finish", but after a restart the question is "does state still
        # match the disk", and the daemon was down for an unmeasured
        # interval during which files could be created, appended or
        # removed with nobody watching. The tiered cadence assumes
        # continuous observation, and a restart is the one moment that
        # assumption does not hold. Without this, a file added while the
        # daemon was down waits for the next warm or cold scan — up to 24
        # hours on the default cadence — and restarting, which is the
        # first thing anyone tries when sync looks stuck, does not help.
        #
        # A set rather than a flag: `tick` runs at most one tier per
        # watched path per iteration, so a single flag cleared by the
        # first path would silently skip every other one.
        if self._config.sync.rescan_on_start:
            self._pending_full_scan = {wp.name for wp in self._config.watched_paths}
        else:
            self._pending_full_scan = set()

        self._last_heartbeat_mono = float("-inf")
        try:
            # The timing columns are cleared as well as the identity ones:
            # a row left by an earlier run carries a `last_tick_at` that the
            # dashboard would read as "running" and timings it would
            # attribute to this pid, until the first heartbeat lands.
            self._state.write_daemon_state(
                pid=os.getpid(),
                host=platform.node(),
                version=__version__,
                started_at=time.time(),
                sync_status="idle",
                last_tick_at=None,
                last_attach_at=None,
                next_attach_at=None,
            )
        except Exception:  # pragma: no cover
            log.debug("could not write daemon_state", exc_info=True)

        # --- lightningtree fork: status dashboard ---
        self._start_status_server()
        # --- end fork ---

        self._running = True

    def tick(self) -> None:
        """One iteration of the main loop.

        Per watched path: pick the highest-priority overdue scan tier
        (cold > warm > hot) and run it. Then, on its own cadence, attach
        any pending files whose path matched an ``item_id`` regex.
        """
        now_mono = time.monotonic()
        now_wall = time.time()
        failed = False

        # --- lightningtree fork: status dashboard ---
        # A "sync now" from the dashboard is read here rather than acted
        # on by the server's thread, so that all the scanning, SQLite and
        # upload work stays on this one. However many times the button was
        # pressed, what arrives is a single forced pass.
        forced = self._sync_requested.is_set()
        if forced:
            self._sync_requested.clear()
            # Scan before attaching: a file written since the last scan is
            # not in state yet, so an attach pass on its own would find
            # nothing new to send. This is the marker the restart rescan
            # uses, and `_select_scan_tier` already reads it.
            self._pending_full_scan.update(wp.name for wp in self._config.watched_paths)
            # Clear the retry backoff too. The files a human presses this
            # for are the ones the dashboard lists as not uploading, and
            # those are sitting out a delay of up to an hour -- without
            # this the forced pass walks straight past every one of them
            # and the button appears to do nothing.
            for wp in self._config.watched_paths:
                try:
                    self._state.clear_retry_backoff(wp.name)
                except Exception:
                    log.exception("Could not clear the retry backoff for %s", wp.name)
            log.info("Sync requested from the dashboard")
        # --- end fork ---

        for wp in self._config.watched_paths:
            try:
                kind = self._select_scan_tier(wp, now_wall)
            except Exception:
                log.exception("Error selecting scan tier for %s", wp.name)
                continue
            if kind is None:
                continue
            try:
                self._run_scan(wp, kind)
                self.last_scan_time = now_wall
                self._pending_full_scan.discard(wp.name)
            except NotImplementedError:
                log.debug("Scan kind %s not implemented for %s", kind, wp.name)
                self._pending_full_scan.discard(wp.name)
            except Exception:
                log.exception("Error running %s scan for %s", kind, wp.name)
                failed = True

        # `forced` is the fork's dashboard sync request; see above.
        due = now_mono - self._last_attach_mono >= self._config.sync.metadata_interval
        if forced or due:
            self.sync_status = "attaching"
            try:
                self._attach_matched_files()
                self.last_attach_time = now_wall
            except Exception:
                log.exception("Error in attach loop")
                failed = True
            self._last_attach_mono = now_mono

        # Derived from this tick rather than latched. A scan failure used to
        # set "error" with nothing but a later *successful attach pass* able
        # to clear it, so a single transient hiccup showed as an error for up
        # to metadata_interval -- twenty minutes on the default cadence.
        self.sync_status = "error" if failed else "idle"

        self._update_pending_count()
        self._write_heartbeat(now_mono, now_wall)

    def _write_heartbeat(self, now_mono: float, now_wall: float) -> None:
        """Persist liveness and timing for readers in other processes.

        `_last_attach_mono` is monotonic and in memory, so it dies with the
        process and nothing outside can work out when the next attach is
        due. Persisting the derived wall-clock deadline gives a reader that,
        while leaving the scheduling itself on the monotonic clock, where it
        is immune to NTP steps and DST.

        Throttled: the tick runs at 1 Hz and this is not worth a commit a
        second.
        """
        if now_mono - self._last_heartbeat_mono < HEARTBEAT_SECONDS:
            return
        self._last_heartbeat_mono = now_mono
        interval = self._config.sync.metadata_interval
        try:
            self._state.write_daemon_state(
                last_tick_at=now_wall,
                last_attach_at=self.last_attach_time,
                next_attach_at=now_wall
                + max(0.0, interval - (now_mono - self._last_attach_mono)),
                attach_interval=interval,
                sync_status=self.sync_status,
            )
        except Exception:  # pragma: no cover - never break the loop over this
            log.debug("could not write daemon_state", exc_info=True)

    def start(self) -> None:
        """CLI entry: setup + tick loop. Blocks until ``stop()`` is called."""
        self.setup()

        log.info("Daemon running. Press Ctrl+C to stop.")
        try:
            while self._running:
                time.sleep(TICK_SECONDS)
                self.tick()
        finally:
            self.shutdown()

    # --- lightningtree fork: status dashboard ---
    def _start_status_server(self) -> None:
        """Serve the read-only dashboard on loopback, if configured.

        The snapshot callable opens its own read-only connection each time:
        sqlite3 connections belong to the thread that made them, and the
        server answers on its own threads. Reading the database rather than
        this object's attributes is also what lets the page work when the
        daemon is not running.
        """
        # The GUI reloads its config from disk when it starts the daemon,
        # so a --status-port given to `beholder gui` reaches us here
        # rather than through the config object.
        override = os.environ.get("BEHOLDER_STATUS_PORT")
        port = (
            int(override)
            if override and override.isdigit()
            else getattr(self._config, "status_port", 0)
        )
        if not port or self._status_server is not None:
            return
        from datalab_beholder.server import StatusServer
        from datalab_beholder.status import collect_status

        server = StatusServer(
            lambda: collect_status(self._config),
            port=int(port),
            on_sync_request=self.request_sync_now,
        )
        if server.start():
            self._status_server = server

    def request_sync_now(self) -> None:
        """Ask the loop to scan every watched path and run an upload pass
        on its next tick, rather than waiting out the configured intervals.

        Called from one of the dashboard's threads, so all it does is set
        an event: `tick` picks it up within a second and does the work on
        the daemon's own thread. Requests made before that tick collapse
        into one pass. A request made after it starts another full scan.
        """
        self._sync_requested.set()

    # --- end fork ---

    def shutdown(self) -> None:
        """Release resources."""
        # --- lightningtree fork: status dashboard ---
        if self._status_server is not None:
            self._status_server.stop()
            self._status_server = None
        # --- end fork ---
        # `start()` calls this from a `finally`, so it must not raise: an
        # exception here would replace whatever ended the loop.
        try:
            self._state.close()
        except Exception:  # pragma: no cover
            log.debug("could not close the state database", exc_info=True)
        log.info("Daemon stopped.")

    def stop(self) -> None:
        log.info("Stopping daemon...")
        self._running = False

    def _handle_signal(self, signum: int, frame: Any) -> None:
        log.info("Received signal %d, shutting down...", signum)
        self.stop()

    # ------------------------------------------------------------------
    # Scan dispatch
    # ------------------------------------------------------------------

    def _select_scan_tier(self, wp: Any, now: float) -> str | None:
        """Return the highest-priority overdue scan tier for `wp`, or
        None if nothing is due.

        NULL timestamps are treated as infinitely stale, so each tier
        fires on its first tick after the daemon starts. Cold supersedes
        warm supersedes hot (the higher-tier scan does the work of the
        lower ones, which is reflected in `update_scan_timestamp`).
        """
        if wp.name in self._pending_full_scan:
            # Deliberately *not* discarded here. The marker is cleared by
            # `tick` only once the scan has actually run: a startup scan
            # that raises -- the watched share not being mounted yet is the
            # ordinary case a second after boot -- must be retried on the
            # next tick, not silently dropped until the next restart.
            #
            # Degrade to warm where cold is disabled: `cold_interval: null`
            # is chosen for write-once archives where a full walk is the
            # thing being avoided.
            return "cold" if wp.scan.cold_interval is not None else "warm"

        ts = self._state.get_scan_timestamps(wp.name)

        cold_interval = wp.scan.cold_interval
        if cold_interval is not None:
            if ts.cold is None or now - ts.cold >= cold_interval:
                return "cold"

        if ts.warm is None or now - ts.warm >= wp.scan.warm_interval:
            return "warm"

        if ts.hot is None or now - ts.hot >= wp.scan.hot_interval:
            return "hot"

        return None

    def _run_scan(self, wp: Any, kind: str) -> None:
        if kind == "cold":
            diff = wp.cold_scan(self._state)
        elif kind == "warm":
            diff = wp.warm_scan(self._state)
        elif kind == "hot":
            diff = wp.hot_scan(self._state)
        else:  # pragma: no cover - defensive
            raise ValueError(f"unknown scan kind: {kind}")

        if diff.has_changes:
            log.info(
                "%s scan of %s: %d new, %d modified, %d deleted",
                kind,
                wp.name,
                len(diff.new),
                len(diff.modified),
                len(diff.deleted),
            )
        else:
            log.debug("%s scan of %s: no changes", kind, wp.name)

    # ------------------------------------------------------------------
    # Attach
    # ------------------------------------------------------------------

    def _update_pending_count(self) -> None:
        total = 0
        for wp in self._config.watched_paths:
            total += len(self._state.get_pending_changes(wp.name))
        self.pending_count = total

    def _attach_matched_files(self) -> None:
        """Attach pending files whose path matched an ``item_id`` regex.

        Per watched path:

        1. Pull pending entries from state and keep the ones with
           ``status in {new, modified}`` and ``ids["item_id"]`` set.
        2. Per item id, fetch the item once (creating it if missing
           and ``item_type`` is configured) and re-use that snapshot
           for every file on that item — saves a round-trip per file
           in the common case of many files sharing one item id.
        3. For each attachable file, look up an existing attachment
           with the same basename. If found, upload with
           ``replace_file_id`` to overwrite in place; otherwise upload
           as a new attachment.
        4. If the file matched a ``block_patterns`` entry and the item
           doesn't already have a block of that type wired to this
           exact file (matched via the block's own ``file_id``, since
           the file's own record isn't reliably kept in sync), create
           one wired to the newly-uploaded file.
        5. Mark successful uploads synced. A ``304`` reply (the server
           already holds identical content, matched by hash) counts as
           success, so an unchanged-but-retouched file isn't re-uploaded
           forever. Failures are left un-synced and retried next tick.

        File uploads only support local paths today; SSH/Cloud entries
        are skipped with a debug log until those backends grow upload
        paths of their own.
        """
        # One reachability probe per client per pass, not per watched path.
        # Several watched paths routinely share one datalab, and during an
        # outage each probe blocks for the connect timeout on the thread
        # running the tick -- which, under the GUI, is the Tk main loop.
        reachable_by_client: dict[str, bool] = {}

        for wp in self._config.watched_paths:
            if not isinstance(wp, LocalWatchedPath):
                log.debug("Skipping attach for %s: only local paths supported", wp.name)
                continue

            # Rows for files that have gone from disk are dropped whatever
            # else happens below: the early exits (nothing attachable, the
            # server unreachable) would otherwise let them accumulate for
            # as long as the condition lasts.
            self._state.remove_deleted(wp.name)

            pending = self._state.get_pending_changes(wp.name)
            # Skips files whose backoff has not expired, so a file that keeps
            # failing stops costing a full-size upload on every single pass.
            attachable = [
                e for e in self._state.get_attachable(wp.name) if e.ids.get("item_id")
            ]

            # Pending rows with no captured `item_id` can never be attached.
            # They are not an error, but silently dropping them is how a
            # watched path ends up reporting a pending count that never goes
            # down with nothing in the log to say why. The dry-run path
            # already reports this; the daemon did not.
            unattachable = [
                e
                for e in pending
                if e.status in ("new", "modified") and not e.ids.get("item_id")
            ]
            if unattachable:
                log.info(
                    "%s: %d pending file(s) have no item_id and cannot be "
                    "attached; check id_patterns",
                    wp.name,
                    len(unattachable),
                )
                for e in unattachable:
                    log.debug("  no item_id: %s", e.path)

            if not attachable:
                continue

            client = self._client_for(wp)

            # A server-wide outage is not each file's fault. Without this,
            # twenty minutes of downtime would drive every pending file's
            # attempt count up and push them all to the one-hour backoff
            # cap, so recovery would be slower than the outage.
            if wp.datalab not in reachable_by_client:
                reachable_by_client[wp.datalab] = client.check_connection()[0]
            if not reachable_by_client[wp.datalab]:
                log.warning(
                    "%s: datalab is unreachable; leaving %d file(s) pending "
                    "without counting an attempt",
                    wp.name,
                    len(attachable),
                )
                continue

            log.info("Attaching %d file(s) for %s", len(attachable), wp.name)

            # Per-pass cache: item_id → item dict (or None if the item
            # couldn't be ensured). Avoids re-querying for each file on
            # the same item.
            item_cache: dict[str, dict[str, Any] | None] = {}

            attached = 0
            for entry in attachable:
                file_path = wp.path / entry.path
                item_id = entry.ids["item_id"]

                try:
                    if item_id not in item_cache:
                        if wp.item_type:
                            item_cache[item_id] = client.ensure_item(
                                item_id=item_id,
                                item_type=wp.item_type,
                                collection_id=entry.ids.get("collection_id"),
                                group_id=entry.ids.get("group_id"),
                            )
                        else:
                            # No item_type configured → don't create, only
                            # attach if the item already exists.
                            item_cache[item_id] = client.fetch_item(item_id)

                    item = item_cache[item_id]
                    if item is None:
                        log.warning(
                            "Skipping %s: item %s not found and not creatable",
                            entry.path,
                            item_id,
                        )
                        self._state.record_attach_failure(
                            wp.name,
                            entry.path,
                            f"item {item_id} not found and not creatable",
                        )
                        continue

                    replace_id = client.find_existing_file_id(item, file_path.name)
                    result = client.attach_file(
                        item_id=item_id,
                        file_path=file_path,
                        replace_file_id=replace_id,
                    )
                    if result is None:
                        # attach_file logs and swallows; count it so the
                        # backoff applies and the dashboard can see it.
                        self._state.record_attach_failure(
                            wp.name,
                            entry.path,
                            client.last_error or "upload did not succeed",
                        )
                        continue

                    # Mark it synced as soon as the bytes are on the
                    # server, before the block attempt below. Anything
                    # that goes wrong afterwards must not cause this file
                    # to be uploaded all over again on the next pass.
                    self._state.mark_synced(wp.name, [entry.path])
                    attached += 1

                    if result.get("not_modified"):
                        # Server already holds identical content (it
                        # compares hashes). Marking it synced anyway is
                        # the point: otherwise the entry stays pending
                        # and we re-upload it on every tick.
                        log.info(
                            "%s already up to date on item %s",
                            entry.path,
                            item_id,
                        )
                    else:
                        log.info(
                            "Attached %s -> item %s%s",
                            entry.path,
                            item_id,
                            f" (replaced file {replace_id})" if replace_id else "",
                        )

                    block_type = _match_block_type(file_path.name, wp.block_patterns)
                    file_id = result.get("file_id")
                    if (
                        block_type
                        and file_id
                        and client.find_block_for_file(item, block_type, file_id)
                        is None
                    ):
                        block = client.create_block(
                            item_id=item_id,
                            block_type=block_type,
                            file_id=file_id,
                        )
                        if block is not None:
                            log.info(
                                "Created %s block on item %s for %s",
                                block_type,
                                item_id,
                                entry.path,
                            )
                except Exception as exc:
                    # One unhappy file must not take down the pass. Files
                    # already uploaded keep their synced mark, and every
                    # entry after this one still gets its turn.
                    log.exception("Failed to attach %s to item %s", entry.path, item_id)
                    self._state.record_attach_failure(wp.name, entry.path, str(exc))
                    # The cache is deliberately left alone. `fetch_item` now
                    # raises rather than returning None when the server could
                    # not be asked, so caching None here would turn one
                    # transient error into "item not found and not creatable"
                    # for every other file on the same item -- the exact
                    # whole-item skid that raising was meant to end. Not
                    # caching costs at most one extra lookup per file, and
                    # only on a pass that is already failing.

            if attached != len(attachable):
                log.warning(
                    "Attached %d of %d file(s) for %s",
                    attached,
                    len(attachable),
                    wp.name,
                )


# ----------------------------------------------------------------------
# Dry run
# ----------------------------------------------------------------------


@dataclass
class PlannedAction:
    """One thing the daemon would have done, as reported by `dry_run`."""

    watched_path: str
    action: str  # "create_item" | "upload" | "replace" | "create_block" | "skip"
    item_id: str
    path: str = ""
    detail: str = ""


def dry_run(
    config: BeholderConfig,
    clients: dict[str, BeholderClient] | None = None,
) -> list[PlannedAction]:
    """Simulate one full scan + attach pass without changing anything.

    Walks each local watched path with a full (cold-style) scan,
    classifies the results against the state DB opened read-only (a
    missing DB classifies everything as new), then probes the datalab
    server with GET-only requests to report exactly what the daemon
    would do on its next attach pass. Neither the state DB nor the
    server is written to.

    A datalab that can't be reached is not fatal: the scan and local
    classification still run, pending files are reported with an
    ``attach_unknown`` action, and the connection failure is logged as
    an error.

    ``clients`` may be injected (used by tests); otherwise one client
    per configured datalab is constructed as in the real daemon. A
    datalab name missing from an injected mapping is treated as
    unreachable.
    """
    from datalab_beholder.scanner import scan_directory
    from datalab_beholder.state import DiffEntry, DiffResult

    if clients is None:
        clients = {}
        for d in config.datalabs:
            try:
                clients[d.name] = BeholderDaemon._build_client(d, config.log_level)
            except Exception as e:
                log.error(
                    "Cannot connect to datalab %r at %s: %s — scans will "
                    "still run, but server-side state for its watched "
                    "paths is unknown",
                    d.name,
                    d.url,
                    e,
                )

    state: StateStore | None = None
    if config.state_db.exists():
        state = StateStore(config.state_db, read_only=True)
    else:
        log.info(
            "No state DB at %s — treating every matched file as new",
            config.state_db,
        )

    actions: list[PlannedAction] = []
    try:
        for wp in config.watched_paths:
            if not isinstance(wp, LocalWatchedPath):
                log.info(
                    "Skipping %s: only local paths supported (kind=%s)",
                    wp.name,
                    wp.kind,
                )
                continue

            # A scan that cannot run at all is fatal: a dry run against a
            # missing/mistyped path would otherwise report "0 files
            # matched", which reads as "nothing to do" instead of
            # "misconfigured".
            if not wp.path.is_dir():
                raise FileNotFoundError(
                    f"watched path {wp.name!r} cannot be scanned: {wp.path} "
                    "does not exist or is not a directory"
                )

            log.info("Dry-run scan of %s (%s)", wp.name, wp.path)
            scan = scan_directory(
                wp.path,
                name=wp.name,
                include_patterns=wp.include_patterns,
                exclude_patterns=wp.exclude_patterns,
                id_patterns=wp.id_patterns,
                item_id_template=wp.item_id_template,
                collection_id_template=wp.collection_id_template,
                max_depth=wp.max_depth,
            )
            log.info(
                "%s: %d file(s) matched patterns in %d ms "
                "(run with --log-level debug to see every skipped file)",
                wp.name,
                scan.total_files,
                scan.scan_duration_ms,
            )

            if state is not None:
                diff = state.classify_scan(scan)
            else:
                diff = DiffResult(watched_path_name=wp.name)
                diff.new = [
                    DiffEntry(
                        path=e.path,
                        size=e.size,
                        modified=e.modified,
                        is_directory=False,
                        status="new",
                        ids=dict(e.ids),
                    )
                    for e in scan.entries
                ]

            log.info(
                "%s vs local state: %d new, %d modified, %d unchanged, %d deleted",
                wp.name,
                len(diff.new),
                len(diff.modified),
                diff.unchanged,
                len(diff.deleted),
            )
            if diff.deleted:
                log.info(
                    "%s: %d deleted file(s) would be pruned from local state "
                    "(no server change)",
                    wp.name,
                    len(diff.deleted),
                )

            pending = diff.new + diff.modified
            attachable = [e for e in pending if e.ids.get("item_id")]
            if len(pending) != len(attachable):
                log.info(
                    "%s: %d pending file(s) have no item_id and would not be attached",
                    wp.name,
                    len(pending) - len(attachable),
                )
            if not attachable:
                continue

            # Config validation guarantees wp.datalab resolves to a real
            # datalab name (see BeholderConfig.validate_datalab_refs);
            # it can still be absent from `clients` if unreachable.
            assert wp.datalab is not None
            client = clients.get(wp.datalab)
            if client is None:
                log.error(
                    "%s: datalab %r is unreachable — %d pending file(s) "
                    "would be attached, but whether items/files/blocks "
                    "would be created or replaced is unknown",
                    wp.name,
                    wp.datalab,
                    len(attachable),
                )
                for entry in attachable:
                    log.info(
                        "would attach %s to item %s (server state unknown)",
                        entry.path,
                        entry.ids["item_id"],
                    )
                    actions.append(
                        PlannedAction(
                            wp.name,
                            "attach_unknown",
                            entry.ids["item_id"],
                            entry.path,
                            "server unreachable",
                        )
                    )
                continue

            item_cache: dict[str, dict[str, Any] | None] = {}
            would_create: set[str] = set()

            for entry in attachable:
                item_id = entry.ids["item_id"]
                filename = entry.path.rsplit("/", 1)[-1]

                if item_id not in item_cache:
                    try:
                        item_cache[item_id] = client.fetch_item(item_id)
                    except Exception as e:
                        # fetch_item now distinguishes "does not exist" from
                        # "could not ask". A dry run must not report "would
                        # create" on the strength of a failed lookup.
                        log.warning(
                            "cannot determine whether item %s exists: %s",
                            item_id,
                            e,
                        )
                        actions.append(
                            PlannedAction(
                                wp.name,
                                "attach_unknown",
                                item_id,
                                entry.path,
                                "server state unknown",
                            )
                        )
                        continue
                item = item_cache[item_id]

                replace_id: str | None = None
                if item is None:
                    if not wp.item_type:
                        log.warning(
                            "would skip %s: item %s does not exist and no "
                            "item_type is configured to create it",
                            entry.path,
                            item_id,
                        )
                        actions.append(
                            PlannedAction(
                                wp.name,
                                "skip",
                                item_id,
                                entry.path,
                                "item missing, not creatable",
                            )
                        )
                        continue
                    if item_id not in would_create:
                        would_create.add(item_id)
                        log.info(
                            "would create item %s (type=%s, collection=%s, group=%s)",
                            item_id,
                            wp.item_type,
                            entry.ids.get("collection_id"),
                            entry.ids.get("group_id"),
                        )
                        actions.append(
                            PlannedAction(
                                wp.name,
                                "create_item",
                                item_id,
                                detail=f"type={wp.item_type}",
                            )
                        )
                else:
                    replace_id = client.find_existing_file_id(item, filename)

                if replace_id:
                    log.info(
                        "would replace file %s on item %s with %s",
                        replace_id,
                        item_id,
                        entry.path,
                    )
                    actions.append(
                        PlannedAction(
                            wp.name,
                            "replace",
                            item_id,
                            entry.path,
                            f"file_id={replace_id}",
                        )
                    )
                else:
                    log.info(
                        "would upload %s as a new file on item %s",
                        entry.path,
                        item_id,
                    )
                    actions.append(
                        PlannedAction(wp.name, "upload", item_id, entry.path)
                    )

                block_type = _match_block_type(filename, wp.block_patterns)
                if block_type:
                    # A block can only already exist for a file that is
                    # already attached (i.e. the replace case); a new
                    # upload gets a fresh file_id, so the daemon would
                    # always create a block for it.
                    has_block = (
                        item is not None
                        and replace_id is not None
                        and client.find_block_for_file(item, block_type, replace_id)
                        is not None
                    )
                    if has_block:
                        log.debug(
                            "item %s already has a %s block for %s",
                            item_id,
                            block_type,
                            entry.path,
                        )
                    else:
                        log.info(
                            "would create %s block on item %s for %s",
                            block_type,
                            item_id,
                            entry.path,
                        )
                        actions.append(
                            PlannedAction(
                                wp.name,
                                "create_block",
                                item_id,
                                entry.path,
                                detail=block_type,
                            )
                        )
    finally:
        if state is not None:
            state.close()

    return actions
