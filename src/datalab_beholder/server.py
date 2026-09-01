"""A small read-only HTTP view of the daemon's state.

Two routes: `/` serves the page, `/api/status` serves the JSON the page
polls. That JSON is the whole integration surface — anything that wants to
watch a beholder reads it rather than parsing `beholder.log`.

Bound to loopback only. `DESIGN.md` describes the instrument-PC network
posture as outbound HTTPS only, and the point of that is that these
machines are not reachable. Opening a listening socket cuts against it, so
the socket is not reachable from off the machine, there is no option to
make it so, and consequently there is nothing to authenticate. It also
avoids a Windows firewall prompt on startup.

Stdlib only, deliberately: beholder ships as a single-file PyInstaller
binary, and a web framework would add tens of megabytes to it. The page is
a module-level string rather than a data file for the same reason — no
`datas` entry in beholder.spec and no `_MEIPASS` path handling.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

log = logging.getLogger(__name__)

DEFAULT_PORT = 8477
HOST = "127.0.0.1"

PAGE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>beholder</title>
<style>
:root{
  --bg:#f6f7f9; --card:#fff; --ink:#1a1d21; --dim:#6b7280; --line:#e3e6ea;
  --ok:#177245; --okbg:#e7f4ec; --warn:#8a5a00; --warnbg:#fdf3e0;
  --bad:#a3231d; --badbg:#fbeceb; --idle:#4b5563; --idlebg:#eef0f3;
}
@media (prefers-color-scheme:dark){:root{
  --bg:#15171a; --card:#1d2024; --ink:#e6e8ea; --dim:#9aa3ad; --line:#2c3036;
  --ok:#6ee7a8; --okbg:#12301f; --warn:#f0c274; --warnbg:#33270e;
  --bad:#f29089; --badbg:#3a1a18; --idle:#9aa3ad; --idlebg:#24282d;
}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
 font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
.wrap{max-width:1100px;margin:0 auto;padding:24px 20px 64px}
h1{font-size:20px;margin:0;letter-spacing:-.01em}
h2{font-size:15px;margin:0}
header{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;margin-bottom:4px}
.sub{color:var(--dim);font-size:12.5px;margin-bottom:20px}
.pill{display:inline-block;padding:2px 9px;border-radius:999px;font-size:12px;
 font-weight:600;background:var(--idlebg);color:var(--idle)}
.pill.ok{background:var(--okbg);color:var(--ok)}
.pill.warn{background:var(--warnbg);color:var(--warn)}
.pill.bad{background:var(--badbg);color:var(--bad)}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;
 padding:16px 18px;margin-bottom:16px}
.cardhead{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:2px}
.path{color:var(--dim);font-size:12.5px;font-family:ui-monospace,SFMono-Regular,
 Menlo,Consolas,monospace;word-break:break-all;margin-bottom:14px}
.tiles{display:flex;gap:26px;flex-wrap:wrap;margin-bottom:14px}
.tile .n{font-size:22px;font-weight:600;font-variant-numeric:tabular-nums;
 letter-spacing:-.02em}
.tile .l{font-size:11.5px;color:var(--dim);text-transform:uppercase;
 letter-spacing:.05em}
.tile.bad .n{color:var(--bad)} .tile.warn .n{color:var(--warn)}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{text-align:left;padding:5px 10px 5px 0;border-bottom:1px solid var(--line);
 vertical-align:top}
th{font-size:11.5px;color:var(--dim);text-transform:uppercase;
 letter-spacing:.05em;font-weight:600}
tr:last-child td{border-bottom:0}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px}
.err{color:var(--bad)}
.scroll{overflow-x:auto}
.sec{font-size:11.5px;color:var(--dim);text-transform:uppercase;
 letter-spacing:.05em;font-weight:600;margin:16px 0 6px}
.empty{color:var(--dim);font-style:italic}
.banner{background:var(--badbg);color:var(--bad);border-radius:8px;
 padding:10px 14px;margin-bottom:16px;font-size:13px}
</style></head><body><div class="wrap">
<header><h1>beholder</h1><span id="state" class="pill">connecting</span></header>
<div class="sub" id="meta"></div>
<div id="body"></div>
<div class="sub" id="foot"></div>
</div>
<script>
const $ = (id) => document.getElementById(id);

// Times arrive as unix seconds. Show "how long ago", which is the thing you
// actually want here, with the exact stamp on hover.
function ago(t){
  if(!t) return "never";
  const d = Date.now()/1000 - t;
  const a = Math.abs(d), f = d < 0 ? "in " : "", s = d < 0 ? "" : " ago";
  let v;
  if(a < 45) v = Math.round(a)+"s";
  else if(a < 3600) v = Math.round(a/60)+" min";
  else if(a < 86400) v = (a/3600).toFixed(1)+" h";
  else v = (a/86400).toFixed(1)+" d";
  return f+v+s;
}
const stamp = (t) => t ? new Date(t*1000).toLocaleString() : "";
const T = (t) => t ? `<span title="${stamp(t)}">${ago(t)}</span>` : '<span class="empty">never</span>';
const esc = (s) => String(s == null ? "" : s).replace(/[&<>"]/g,
  c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const bytes = (n) => n == null ? "" :
  n < 1024 ? n+" B" :
  n < 1048576 ? (n/1024).toFixed(0)+" KB" :
  n < 1073741824 ? (n/1048576).toFixed(1)+" MB" : (n/1073741824).toFixed(2)+" GB";

function tile(n, label, cls){
  return `<div class="tile ${cls||''}"><div class="n">${n}</div>
          <div class="l">${label}</div></div>`;
}

function renderPath(p){
  if(p.error) return `<div class="card"><h2>${esc(p.name)}</h2>
    <div class="err">${esc(p.error)}</div></div>`;
  const c = p.counts || {};
  let h = `<div class="card"><div class="cardhead"><h2>${esc(p.name)}</h2>`;
  if(p.path_exists === false)
    h += `<span class="pill bad">directory missing</span>`;
  if(p.datalab) h += `<span class="pill">${esc(p.datalab)}</span>`;
  h += `</div><div class="path">${esc(p.path || "")}</div>`;

  h += `<div class="tiles">
    ${tile(c.synced || 0, "synced")}
    ${tile(p.pending || 0, "pending", p.pending ? "warn" : "")}
    ${tile(p.stuck || 0, "stuck", p.stuck ? "bad" : "")}
    ${tile(p.unattachable || 0, "no item_id", p.unattachable ? "warn" : "")}
    <div class="tile"><div class="n" style="font-size:15px;padding-top:6px">
      ${T(p.last_sync)}</div><div class="l">last upload</div></div>
  </div>`;

  h += `<div class="sec">Scans</div><div class="scroll"><table>
    <tr><th>Tier</th><th>Interval</th><th>Last</th><th>Next</th></tr>`;
  for(const k of ["hot","warm","cold"]){
    const s = (p.scans||{})[k] || {};
    h += `<tr><td>${k}</td>
      <td class="mono">${s.interval == null ? "off" : s.interval+"s"}</td>
      <td>${T(s.last)}</td><td>${T(s.next)}</td></tr>`;
  }
  h += `</table></div>`;

  if(p.failure_tracking === false){
    h += `<div class="sec">Problems</div><div class="empty">This state
      database predates failure tracking. Start the daemon on it to record
      why a file is not uploading.</div>`;
  } else if(p.problems && p.problems.length){
    h += `<div class="sec">Not uploading</div><div class="scroll"><table>
      <tr><th>File</th><th>Attempts</th><th>Last failure</th>
      <th>Next try</th><th>Error</th></tr>`;
    for(const f of p.problems){
      h += `<tr><td class="mono">${esc(f.path)}</td>
        <td class="mono">${f.attempts}</td><td>${T(f.last_error_at)}</td>
        <td>${T(f.next_retry_at)}</td>
        <td class="err mono">${esc(f.last_error)}</td></tr>`;
    }
    h += `</table></div>`;
  }

  if(p.recent_syncs && p.recent_syncs.length){
    h += `<div class="sec">Recently uploaded</div><div class="scroll"><table>
      <tr><th>File</th><th>Size</th><th>When</th></tr>`;
    for(const f of p.recent_syncs){
      h += `<tr><td class="mono">${esc(f.path)}</td>
        <td class="mono">${bytes(f.size)}</td>
        <td>${T(f.last_synced)}</td></tr>`;
    }
    h += `</table></div>`;
  }
  return h + `</div>`;
}

function render(s){
  const d = s.daemon || {};
  const pill = $("state");
  const stuck = (s.watched_paths||[]).reduce((n,p) => n + (p.stuck||0), 0);
  if(!d.running){ pill.className = "pill"; pill.textContent = "not running"; }
  else if(d.sync_status === "error"){ pill.className = "pill bad"; pill.textContent = "error"; }
  else if(stuck){ pill.className = "pill warn"; pill.textContent = stuck+" stuck"; }
  else { pill.className = "pill ok"; pill.textContent = d.sync_status || "running"; }

  const bits = [];
  if(d.version) bits.push("v"+esc(d.version));
  if(d.host) bits.push(esc(d.host));
  if(d.pid) bits.push("pid "+d.pid);
  if(d.uptime_seconds) bits.push("up "+ago(Date.now()/1000 - d.uptime_seconds)
    .replace(" ago",""));
  if(d.running && d.next_attach_at != null)
    bits.push("next upload pass "+ago(d.next_attach_at));
  else if(d.last_attach_at) bits.push("last upload pass "+ago(d.last_attach_at));
  bits.push(esc(s.state_db));
  $("meta").innerHTML = bits.join(" &middot; ");

  let h = "";
  if(s.error) h += `<div class="banner">${esc(s.error)}</div>`;
  if(!d.running && !s.error)
    h += `<div class="banner">No recent heartbeat: the daemon is not running,
      or this database was copied from another machine. The figures below are
      the last state it recorded.</div>`;
  const paths = s.watched_paths || [];
  h += paths.length ? paths.map(renderPath).join("")
     : `<div class="card empty">No watched paths in this state database.</div>`;
  $("body").innerHTML = h;
  $("foot").textContent = "updated " + new Date().toLocaleTimeString();
}

async function poll(){
  try{
    const r = await fetch("api/status", {cache:"no-store"});
    render(await r.json());
  }catch(e){
    $("state").className = "pill bad";
    $("state").textContent = "dashboard unreachable";
  }
}
poll();
setInterval(poll, 5000);
</script></body></html>
"""


class _Handler(BaseHTTPRequestHandler):
    server_version = "beholder-status"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's name
        route = self.path.split("?", 1)[0].rstrip("/") or "/"
        if route == "/":
            self._send(200, "text/html; charset=utf-8", PAGE.encode("utf-8"))
        elif route == "/api/status":
            try:
                body = json.dumps(self.server.snapshot(), default=str)  # type: ignore[attr-defined]
            except Exception as e:  # pragma: no cover - defensive
                log.exception("status snapshot failed")
                body = json.dumps({"error": f"{type(e).__name__}: {e}"})
            self._send(200, "application/json", body.encode("utf-8"))
        else:
            self._send(404, "text/plain; charset=utf-8", b"not found\n")

    def _send(self, code: int, content_type: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        # The default writes straight to stderr, which would interleave with
        # the daemon's own console output once a second.
        log.debug("status: " + fmt, *args)


class StatusServer:
    """The status server on a background thread.

    The snapshot callable must open its own database connection: sqlite3
    connections belong to the thread that created them, and this runs off
    the daemon's thread.
    """

    def __init__(
        self, snapshot: Callable[[], dict[str, Any]], port: int = DEFAULT_PORT
    ) -> None:
        self._snapshot = snapshot
        self._port = port
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        return f"http://{HOST}:{self._port}/"

    def start(self, background: bool = True) -> bool:
        """Bind the port and begin serving.

        Returns False if the port could not be bound, which is never a
        reason to stop the daemon from syncing files. With
        ``background=False`` the caller is expected to call
        ``serve_forever`` itself.
        """
        try:
            httpd = ThreadingHTTPServer((HOST, self._port), _Handler)
        except OSError as e:
            log.warning(
                "Status dashboard disabled: cannot bind %s:%d (%s)",
                HOST,
                self._port,
                e,
            )
            return False
        httpd.snapshot = self._snapshot  # type: ignore[attr-defined]
        httpd.daemon_threads = True
        self._httpd = httpd
        if background:
            self._thread = threading.Thread(
                target=httpd.serve_forever, name="beholder-status", daemon=True
            )
            self._thread.start()
        log.info("Status dashboard on %s", self.url)
        return True

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def serve_forever(self) -> None:
        """Block in this thread. Used by the standalone `dashboard` command,
        which has no daemon loop of its own to get on with."""
        if self._httpd is None:
            raise RuntimeError("call start(background=False) first")
        self._httpd.serve_forever()
