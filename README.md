<div align="center">

# sub2api-scheduler

**Lane-based failover for your Sub2API upstream accounts.**

A self-hostable daemon that organizes your AI gateway's upstream accounts into
priority lanes and automatically fails over between them — so a dead upstream
never takes your gateway down.

[![Go Version](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/yiiilin/sub2api-scheduler/pulls)

</div>

---

## What is this?

You run [Sub2API](https://github.com/Wei-Shaw/sub2api) as your AI gateway. Each
model is backed by several upstream accounts — an OpenClaw API key, a proxy
reseller, the official provider. When one account starts failing (5xx, 429,
network errors), requests still get routed to it until you notice and disable it
by hand.

**sub2api-scheduler fixes that.** It groups the accounts for a model into
*lanes* in priority order, watches every lane with a rolling error window, and
automatically:

- **Disables** an account once it fails enough times in the window.
- **Switches** to the next healthy lane, so traffic keeps flowing.
- **Probes** disabled accounts periodically and **recovers** them the moment
  they're healthy again — while refusing to re-enable accounts whose real
  traffic is still failing (no flapping).
- **Rebuilds** the same ladder for multiple models side by side.

Model-limit mutations use the shared PostgreSQL transaction and
`account_changed` outbox, while probes and account-level scheduling toggles use
Sub2API's admin API. This keeps the account row and scheduler snapshot
consistent without a GET/PUT lost-update window.

---

## Features

- **Lane-based routing** — a model's accounts are ordered by priority; only the
  highest healthy lane receives traffic, all lower lanes are suppressed.
- **Rolling failure detection** — counts upstream 5xx / 429 / network errors in
  a sliding window (default 60 s), disables an account past the threshold.
- **Probe & recovery gating** — disabled accounts are tested with a real
  upstream call; recovery only happens when *both* the probe succeeds *and* the
  real-traffic error window is clean, so an account can't flap back into service
  while still broken.
- **External-state awareness** — respects Sub2API's own schedulable flag,
  status, cooldowns, and model rate limits; re-opens a scheduler-closed account
  with debounce, never fighting the gateway.
- **Scheduler-cache correct** — model-limit updates are atomic with the shared
  PostgreSQL row and `account_changed` outbox; probes and account toggles use the
  Sub2API admin API.
- **Web dashboard** — one page per model showing lane state, failure counts,
  probe status, and a manual probe button.
- **Self-hosted** — single static Go binary, listens on localhost, config via
  YAML.

---

## Quick start

### Prerequisites

- A current Sub2API installation with the `scheduler_outbox` migration applied.
- An admin API key for Sub2API (Server → API Keys → Admin API Key).
- The PostgreSQL user in `database.dsn` must be allowed to read/update `accounts`,
  create/alter the scheduler's `lane_*` tables, and insert `scheduler_outbox` rows.

### Build

```bash
go build -o sub2api-scheduler .
```

### Configure

```bash
cp config.example.yaml config.yaml
# edit: database DSN and sub2api base_url + admin_api_key
```

### Run

```bash
./sub2api-scheduler
# or: CONFIG_PATH=/etc/sub2api-scheduler/config.yaml ./sub2api-scheduler
```

Open the dashboard at `http://127.0.0.1:8090`.

### Docker / docker compose

There is no published image; build it yourself (the result is a small static
binary on Alpine).

```bash
# build and run with compose
cp config.example.yaml config.yaml   # edit DSN + admin_api_key first
docker compose up -d --build

# or build the image only
docker build -t sub2api-scheduler:latest .
```

The container reads `/app/config.yaml` (override with the `CONFIG_PATH`
environment variable) and listens on port 8090. Mount your real `config.yaml`
as shown in `docker-compose.yml`.

The scheduler shares Sub2API's PostgreSQL database, so the container must be
able to reach both Sub2API's Postgres (the DSN in `config.yaml`) and its admin
API (`sub2api.base_url`). When Sub2API runs in Docker, put this container on
the same Docker network (see the commented `networks` section in
`docker-compose.yml`) or use a host-accessible Postgres address in the DSN.

Logs: `docker compose logs -f scheduler`.

### Container health

`/api/health` returns `{"ok": true}` and is used by the compose healthcheck;
the container also exits immediately if the config or database is invalid.

---

## Database setup

No manual SQL is required. On startup the daemon creates and migrates its
`lane_boards`, `lane_boards_lanes`, and `lane_account_states` tables in the
Sub2API PostgreSQL database. It also upgrades tables created from older README
versions by adding lane IDs, defaults, state rows, and uniqueness constraints.

Create and edit boards from the web dashboard. Each model may belong to only
one board, and an account may appear only once in that board. An account must
be able to serve the board's model under Sub2API's explicit mapping, platform
default mapping, or pass-through rules.

Startup fails fast if existing data contains duplicate board names, duplicate board
models, or duplicate account memberships within a board; resolve those
duplicates before restarting. The migration also ignores NULL and non-positive
elements in legacy account ID arrays.

Only one scheduler instance may use a database at a time; startup takes a PostgreSQL
advisory lock and refuses a second instance.

---

## How it works

```
        ┌──────────────────────────────┐
        │      sub2api-scheduler       │
        │                              │
        │  CheckErrors (5s)            │
        │   count upstream 5xx/429/err │
        │   in window → disable        │
        │                              │
        │  ProbeLoop (30s)             │
        │   test disabled accounts     │
        │   recover only if gate ok    │
        │                              │
        │  reconcile (per cycle)       │
        │   find active lane           │
        │   suppress lower lanes       │
        │   release & verify higher    │
        └──────────┬───────────────────┘
                   │ PostgreSQL row lock + account_changed outbox
                   │ admin API (probe / schedulable)
                   ▼
        ┌──────────────────────────────┐
        │  Sub2API gateway             │
        │  account scheduler snapshot  │
        │  (outbox → Redis, consistent)│
        └──────────────────────────────┘
```

### State machine

| State | Meaning | Scheduled by Sub2API? |
|---|---|---|
| `healthy` | receiving traffic | ✅ |
| `disabled` | failed too much / probe failed | ❌ (rate-limit entry) |
| `suppressed` | healthy but a higher lane is active | ❌ (rate-limit entry) |

### Recovery rule

A disabled account is only brought back when **both** hold:

1. **ProbeLoop passes** — a real `/admin/accounts/:id/test` call succeeds.
2. **CheckErrors passes** — the account's upstream failures in the last
   `window_seconds` are below `fail_threshold`.

This prevents flapping: an account that is still failing real traffic never gets
re-enabled to immediately trip the circuit again.

---

## Configuration

| Key | Default | Description |
| --- | --- | --- |
| `server.host` | `127.0.0.1` | Bind address (set to `0.0.0.0` to expose on all interfaces) |
| `server.port` | `8090` | Dashboard port |
| `database.dsn` | — | Sub2API PostgreSQL DSN (**required**) |
| `sub2api.base_url` | `http://sub2api:8080` | Sub2API internal API |
| `sub2api.admin_api_key` | — | Sub2API admin API key (**required**) |

`database.dsn` and `sub2api.admin_api_key` are required: the daemon refuses to
start without them (fail-fast instead of silently running with a broken
control plane).

Per-board tuning lives in the database (`lane_boards` table):

| Column | Meaning |
| --- | --- |
| `fail_threshold` | disables an account after this many upstream failures in the window |
| `window_seconds` | sliding error window length |
| `probe_interval` | seconds between probe cycles |

---

## License

[MIT](LICENSE) © 2026 yiiilin