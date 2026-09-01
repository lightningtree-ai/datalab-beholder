<div align="center">

<img width="200" height="200" alt="Untitled" src="https://github.com/user-attachments/assets/555b8331-af04-47d1-85c8-cf464ec6b69d" title="Pixel art beholder generated with Nano Banana"/>


# *datalab* beholder


</div>

> ⚠️ **Under construction.** APIs, config shape, and server endpoints are still in flux. Not yet recommended for production deployments.

A daemon that watches instrument directories and attaches matching files to items in one or more [*datalab*](https://github.com/datalab-org/datalab) instances. The daemon extracts a datalab `item_id` from each file path with a regex, creates the item on the server if it doesn't already exist, and uploads the file to it — replacing in place if a same-named attachment is already there. Network traffic is outbound only; no inbound firewall rules are needed on the instrument PC.

## How it works

The daemon runs a single-threaded `tick()` loop that drives three independent scan tiers per watched path plus an attach pass:

1. **Cold scan** *(default: every 24 h)* — full directory walk; ground-truth reconciliation against the state DB.
2. **Warm scan** *(every 1 h)* — directory-mtime-aware walk; discovers new files in active subtrees, skips per-file stats in cold subtrees.
3. **Hot scan** *(every 60 s)* — re-stats only files modified in the last `hot_window` seconds; cheap and frequent.
4. **Attach pass** *(every 20 min)* — for every file pending in state with an `item_id` extracted, ensure the datalab item exists then upload the file (with `replace_file_id` set if a same-named attachment is already there).

Each tier writes its findings into a local SQLite state DB; the attach pass drains whatever is pending. Cold can be disabled (`cold_interval: null`) for write-once archives.

## Installation

Either clone this repository and `uv pip install .` (or add to your
`pyproject.toml`), or use the standalone PyInstaller executable published for
each release on GitHub for your platform.

## Quick start

Generate a config template:

```
datalab-beholder init
```

This writes `~/.datalab-beholder/config.yaml`. Edit it to point at your datalab instance and the directories you want to watch:

```yaml
datalabs:
  - name: "main"
    url: "https://datalab.example.org"
    api_key: "your-api-key-here"   # or omit and use a <PREFIX>_DATALAB_API_KEY env var

watched_paths:
  - kind: "local"
    path: "/mnt/instrument/echem"
    name: "digibat"
    datalab: "main"
    item_type: "cells"
    include_patterns: ["*.mpr"]
    id_patterns:
      # `\D*\.mpr$` anchors the capture on the *last* digit run before
      # the extension, so subdirectories of arbitrary depth are fine.
      - "^(?P<group_id>P[0-9]{3,})/.*?(?P<item_id>[0-9]+)\\D*\\.mpr$"
    # Optional templates that turn capture groups into the values the
    # daemon actually sends to datalab. Resolved at scan time.
    item_id_template: "{group_id}-{item_id}"
    # collection_id_template: "{group_id}"

sync:
  metadata_interval: 1200   # attach pending files every 20 minutes

log_level: info
```

Multiple `datalabs` entries are supported; each `watched_paths[].datalab` field references one by name. API keys are resolved by the underlying datalab client, which transparently checks `<PREFIX>_DATALAB_API_KEY` env vars (where `<PREFIX>` matches the deployment's identifier prefix) when no literal key is provided.

### Direct attach via `id_patterns`

`id_patterns` is a list of Python regexes with named capture groups. Each file path (relative to the watched root) is tested against the patterns; the first match wins, and its captured groups become the file's identity for the rest of the pipeline. Allowed group names are:

- `item_id` — **required** in every pattern; the datalab item the file attaches to.
- `group_id` — optional; passed as `group_ids` when the daemon creates the item (access control).
- `collection_id` — optional; passed as `collection_ids` when the daemon creates the item.

Files that don't match any pattern are skipped silently. Set `item_type` to let the daemon create items that don't yet exist; without it the daemon only attaches to items that are already there.

#### Templating ids from capture groups

`item_id_template` and `collection_id_template` are optional Python `str.format` strings that compose the values *actually sent* to the server out of the regex's capture groups. They're resolved at scan time, so the resolved id lands in the state DB and is what the `scan` CLI prints — what you see is what the daemon will create.

Example: `id_patterns: ["^(?P<group_id>P[0-9]+)/(?P<item_id>[0-9]+)-.*\\.mpr$"]` with `item_id_template: "{group_id}-{item_id}"` turns `P042/7-cycle.mpr` into a request for an item called `P042-7`.

If a template references a capture group the regex didn't produce, the file is skipped with a warning rather than crashing the attach pass.

### Running

`start` is the default command, so the bare invocation runs the daemon:

```
datalab-beholder
```

To point at a non-default config:

```
datalab-beholder --config /path/to/config.yaml
```

(Equivalent to `datalab-beholder start --config ...`.)

## CLI reference

### `datalab-beholder init`

Creates a config template at `~/.datalab-beholder/config.yaml` (or at `--path`).

```
datalab-beholder init --path ./my-config.yaml
```

### `datalab-beholder start`

Starts the daemon. Blocks until interrupted (Ctrl+C / SIGTERM). This is the default command — running `datalab-beholder` with no subcommand is equivalent.

```
datalab-beholder start [--config PATH] [--log-level debug|info|warning|error]
```

### `datalab-beholder dry-run`

Simulates one full scan + attach pass without changing anything — no uploads, no item/block creation, no writes to the local state DB (it is opened read-only; a missing one is treated as "everything is new"). The server is only queried with GETs to work out what a real run would do: which items would be created, which files uploaded or replaced, and which blocks created. A summary is printed at the end.

```
datalab-beholder dry-run [--config PATH] [--log-level debug|info|warning|error]
```

At `--log-level debug`, every file that *doesn't* match is logged with the reason (excluded by which pattern, failed `include_patterns`, no `id_pattern` matched) — the fastest way to debug a config against a real directory tree.

An unreachable datalab is not fatal: the scan and pattern report still run, pending files are listed as "would attach (server state unknown)", and the connection failure is logged as an error. A watched path that cannot be scanned (missing or not a directory) *is* fatal — a mistyped path would otherwise read as "nothing to do".

### `datalab-beholder scan`

One-off directory scan — useful for testing your patterns and id templates before running the daemon. With `--config`, scans every `watched_paths[]` entry and prints one JSON object per line per path; without `--config`, takes ad-hoc args.

```
# Use the same config the daemon would use — handy for verifying that
# id_patterns + templates resolve to what you expect.
datalab-beholder scan --config ./config.yaml . --pretty

# Or ad-hoc:
datalab-beholder scan /mnt/instrument/data \
  --name "XRD-Lab-A" \
  --include "*.raw" --include "*.csv" \
  --exclude "*.tmp" \
  --max-depth 3 \
  --pretty
```

The JSON includes each entry's resolved `ids` dict (capture groups + templated `item_id`/`collection_id`) — that's exactly what the daemon will use.

### `datalab-beholder status`

Shows the current state database and recent sync history.

```
datalab-beholder status [--config PATH]
```

### `datalab-beholder gui`

Launches a small Tkinter status GUI showing connection state per configured datalab, recent activity, and a settings editor for the YAML config. Driven by the same `tick()` loop as the CLI daemon.

```
datalab-beholder gui [--config PATH]
```

### `datalab-beholder dashboard`

Serve the status page against an existing state database, with no daemon
running. Use it on a `state.db` copied off an instrument PC, or on one left
behind by a daemon that has stopped.

```
datalab-beholder dashboard [--config PATH] [--state-db PATH] [--port N]
```

With no `--config` it reads the database on its own; the watched-path
locations and scan intervals come from the config, so without one those
fields are reported as null.

## Status dashboard

While the daemon runs it serves a read-only status page on
`http://127.0.0.1:8477/`. It answers the question `datalab-beholder status`
cannot: whether the pending files are moving or wedged, and if wedged, why.

The page shows, per watched path, the counts by status, when each scan tier
last ran and when it is next due, when the last upload landed, and every file
that is failing with its attempt count, its error, and when it will be tried
again. The header carries the daemon's version, host, pid, uptime and the
time until the next upload pass.

Set `status_port: 0` in the config, or pass `--status-port 0`, to turn it off.
The socket is bound to `127.0.0.1` and there is no option to bind anything
else, so it is not reachable from another machine and carries no
authentication. It never includes an API key.

Everything on the page comes from `/api/status` as JSON, which is the
supported way to read a running beholder from anything else:

```
$ curl -s http://127.0.0.1:8477/api/status
{
  "daemon": {
    "running": true,
    "version": "0.1.0rc6",
    "sync_status": "idle",
    "seconds_to_next_attach": 659.7631480693817
  },
  "watched_paths": [
    {
      "name": "sycamore",
      "pending": 3,
      "stuck": 3,
      "problems": [
        {
          "path": "CEL-9997_127.0.0.1-BTS85-253-1-3-12.ndax",
          "attempts": 9,
          "last_error": "HTTP 304 error at https://datalab.lightningtree.ai/api/upload-file/: b''"
        }
      ]
    }
  ]
}
```

The response carries a `schema_version`, currently `1`. Keys get added without
bumping it; a key that changes meaning or disappears bumps it.

Everything is read out of `state.db` over a read-only connection rather than
from the running daemon, which is why `datalab-beholder dashboard` can serve a
database with no daemon attached. A database written before this version has
no failure-tracking columns, and a read-only reader cannot add them; the page
says so instead of showing an empty problems list. Start the daemon on it once
to upgrade it in place.

## Standalone executable (PyInstaller)

For deployment onto instrument PCs without a Python install, a single-file executable can be built with [PyInstaller](https://pyinstaller.org/) using the bundled `beholder.spec`:

```
uv run pyinstaller beholder.spec
```

Run with no subcommand, the resulting binary in `dist/` starts the daemon (`start`), reading `config.yaml` from the directory next to the executable; pass `gui` for the windowed control panel. Pre-built Windows binaries are produced by the project's CI workflow.

PyInstaller does not cross-compile — a Windows binary must be built on Windows. To build binaries for an arbitrary commit without tagging a release, trigger the CI workflow manually (Actions → CI → *Run workflow*, or `gh workflow run ci.yml --ref main -f ref=<sha>`); manual runs upload the Linux/Windows/macOS binaries as downloadable workflow artifacts.

## Configuration

| Section | Field | Default | Description |
|---------|-------|---------|-------------|
| `datalabs[]` | `name` | *(required)* | Unique label referenced by `watched_paths[].datalab` |
| `datalabs[]` | `url` | *(required)* | URL of the datalab instance |
| `datalabs[]` | `api_key` | *(optional)* | API key; if omitted, resolved from env by the datalab client |
| `watched_paths[]` | `path` | *(required)* | Directory to watch |
| `watched_paths[]` | `name` | *(required)* | Label shown in the datalab UI |
| `watched_paths[]` | `datalab` | *(required if >1 datalab)* | Name of the datalab instance to push to |
| `watched_paths[]` | `include_patterns` | `["*"]` | Glob patterns for files to include |
| `watched_paths[]` | `exclude_patterns` | `[]` | Glob patterns for files to exclude |
| `watched_paths[]` | `id_patterns` | `[]` | Regexes with named `item_id`/`group_id`/`collection_id` groups |
| `watched_paths[]` | `item_id_template` | `null` | `str.format` template, e.g. `"{group_id}-{item_id}"` |
| `watched_paths[]` | `collection_id_template` | `null` | `str.format` template for the collection id |
| `watched_paths[]` | `item_type` | `null` | Datalab item type; required to auto-create missing items |
| `watched_paths[]` | `max_depth` | `10` | Max directory traversal depth |
| `watched_paths[]` | `scan.hot_interval` | `60` | Seconds between hot scans (recently-modified file re-stat) |
| `watched_paths[]` | `scan.warm_interval` | `3600` | Seconds between warm scans (directory-mtime walk) |
| `watched_paths[]` | `scan.cold_interval` | `86400` | Seconds between cold scans (full walk); `null` disables |
| `watched_paths[]` | `scan.hot_window` | `86400` | "Recent" cutoff (s) for hot-scan eligibility |
| `sync` | `metadata_interval` | `1200` | Seconds between attach passes |
| | `log_level` | `info` | Logging verbosity |
| | `state_db` | next to package/exe | Path to the local SQLite database |
| | `status_port` | `8477` | Port for the read-only status dashboard on `127.0.0.1`; `0` disables it |

## Roadmap

The 0.1.x version ships only the **direct-attach** mode described above. A second mode is planned for a future release:

- **Metadata-exposure mode** — surface every matching file in the *datalab* UI as a browsable listing without uploading anything. Users browse the listing and request specific files on demand; the daemon then uploads only the requested ones. Useful when watched directories are large or contain files that shouldn't be uploaded by default. This requires server-side endpoints that don't yet exist on the *datalab* side, and will land alongside them.

Other things on the list:

- **SSH and cloud-storage watched paths.** The config schema already discriminates on `kind` (`local` / `ssh` / `cloud`); only `local` is implemented today.

Issues and design discussion welcome — file them at [github.com/datalab-industries/datalab-beholder/issues](https://github.com/datalab-industries/datalab-beholder/issues).

## Development

```
git clone https://github.com/datalab-industries/datalab-beholder.git
cd datalab-beholder
uv sync
uv run pytest -v
```

See [DESIGN.md](DESIGN.md) for the original design notes and motivation.
