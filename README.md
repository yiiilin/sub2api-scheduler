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

Everything runs through Sub2API's own admin API — no direct database writes, no
stale scheduler cache, no 503s after a lane switch.

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
- **Scheduler-cache correct** — all control goes through Sub2API's admin API,
  which invalidates its own Redis snapshot via the outbox — DB and scheduler
  stay in sync.
- **Web dashboard** — one page per model showing lane state, failure counts,
  probe status, and a manual probe button.
- **Self-hosted** — single static Go binary, listens on localhost, config via
  YAML.

---

## Quick start

### Prerequisites

- A running [Sub2API](https://github.com/Wei-Shaw/sub2api) instance with its
  PostgreSQL and Redis.
- An admin API key for Sub2API (Server → API Keys → Admin API Key).
- The `lane_*` tables in the Sub2API database (see
  [Database setup](#database-setup)).

### Build

```bash
go build -o sub2api-scheduler .
```

### Configure

```bash
cp config.example.yaml config.yaml
# edit: database DSN, sub2api base_url + admin_api_key, redis addr
```

### Run

```bash
./sub2api-scheduler
# or: CONFIG_PATH=/etc/sub2api-scheduler/config.yaml ./sub2api-scheduler
```

Open the dashboard at `http://127.0.0.1:8090`.

---

## Database setup

The scheduler reads/writes Sub2API's shared schema. Create the lane tables in
the same PostgreSQL database as Sub2API (run once):

```sql
CREATE TABLE IF NOT EXISTS lane_boards (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    model           TEXT NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT true,
    fail_threshold  INT NOT NULL DEFAULT 3,
    window_seconds  INT NOT NULL DEFAULT 60,
    probe_interval  INT NOT NULL DEFAULT 30,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lane_boards_lanes (
    board_id    BIGINT NOT NULL REFERENCES lane_boards(id) ON DELETE CASCADE,
    position    INT NOT NULL,
    name        TEXT NOT NULL,
    account_ids BIGINT[] NOT NULL,
    PRIMARY KEY (board_id, position)
);

CREATE TABLE IF NOT EXISTS lane_account_states (
    board_id       BIGINT NOT NULL REFERENCES lane_boards(id) ON DELETE CASCADE,
    account_id     BIGINT NOT NULL,
    state          TEXT NOT NULL DEFAULT 'healthy',  -- healthy | disabled | suppressed
    fail_count     INT NOT NULL DEFAULT 0,
    disabled_at    TIMESTAMPTZ,
    checked_at     TIMESTAMPTZ,
    last_probe_at  TIMESTAMPTZ,
    last_probe_ok  BOOLEAN,
    last_probe_msg TEXT,
    PRIMARY KEY (board_id, account_id)
);
```

Then insert your boards: each board owns one model and its lanes, e.g.

```sql
INSERT INTO lane_boards (name, model) VALUES ('my-flash', 'flash-v1');

INSERT INTO lane_boards_lanes (board_id, position, name, account_ids) VALUES
(1, 0, 'primary-proxy',   '{11}'),
(1, 1, 'reseller',        '{22}'),
(1, 2, 'official',        '{33}');
```

The scheduler picks lanes left to right; a lane is *active* when it has the
first healthy account.

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
                   │ admin API (PUT/POST/DELETE)
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
| `server.port` | `8090` | Dashboard port |
| `database.dsn` | — | Sub2API PostgreSQL DSN |
| `sub2api.base_url` | `http://sub2api:8080` | Sub2API internal API |
| `sub2api.admin_api_key` | — | Sub2API admin API key |
| `redis.addr` | `redis:6379` | Sub2API Redis |
| `redis.password` | — | Redis password |

Per-board tuning lives in the database (`lane_boards` table):

| Column | Meaning |
| --- | --- |
| `fail_threshold` | disables an account after this many upstream failures in the window |
| `window_seconds` | sliding error window length |
| `probe_interval` | seconds between probe cycles |

---

## License

[MIT](LICENSE) © 2026 yiiilin