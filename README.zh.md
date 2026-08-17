<div align="center">

# sub2api-scheduler

**为你的 Sub2API 上游账号做泳道故障切换。**

一个可自托管的守护进程，把 AI 网关的上游账号按优先级排成泳道并自动故障切换——上游挂了，网关不停。

[![Go Version](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/yiiilin/sub2api-scheduler/pulls)

</div>

---

## 这是什么？

你在用 [Sub2API](https://github.com/Wei-Shaw/sub2api) 当 AI 网关。每个模型背后通常有好几个上游账号——自建代理、中转商、官方渠道。一旦某个账号开始报错（5xx / 429 / 网络错误），请求还是会持续路由到它，直到你手动发现并禁用它。

**sub2api-scheduler 解决这个问题。** 它把同一模型的多张账号卡按优先级排成泳道，用滚动错误窗口监控，自动：

- **禁用** 窗口内失败次数超阈值的账号；
- **切换** 到下一个健康泳道，流量不断流；
- **探测** 被禁用账号，一旦恢复健康就自动启用——但**真实流量仍在失败的账号不会被重新启用**（防乒乓）；
- **多模型并存**，各自独立泳道互不干扰。

所有控制都走 Sub2API 官方管理 API——不直接写库、不产生调度缓存不一致、切换后不会出现莫名 503。

---

## 特性

- **泳道路由** — 同模型账号按优先级排序，只有最高健康泳道接流量，低泳道全部压制。
- **滚动失败检测** — 统计滑动窗口（默认 60s）内的上游 5xx / 429 / 网络错误，超阈值禁用账号。
- **探测 + 恢复门槛** — 被禁用账号用真实 upstream 调用探测；只有**探测通过 且 真实流量窗口内无失败**才恢复，防止坏账号反复横跳。
- **感知外部状态** — 尊重 Sub2API 自身的 schedulable 开关、status、冷却、模型限流；对网关自己关闭的账号防抖重开，不与网关打架。
- **调度缓存一致** — 全部控制走 Sub2API admin API，经 outbox 失效其 Redis 快照——DB 与调度器永远同步。
- **Web 看板** — 每模型一页，显示泳道状态、失败数、探测状态、手动探测按钮。
- **自托管** — 单个静态 Go 二进制，监听 localhost，YAML 配置。

---

## 快速开始

### 前置

- 一个运行中的 [Sub2API](https://github.com/Wei-Shaw/sub2api)（含 PostgreSQL 和 Redis）。
- Sub2API 的 Admin API Key（Server → API Keys → Admin API Key）。
- Sub2API 数据库里的 `lane_*` 表（见[数据库初始化](#数据库初始化)）。

### 编译

```bash
go build -o sub2api-scheduler .
```

### 配置

```bash
cp config.example.yaml config.yaml
# 编辑：database DSN、sub2api base_url + admin_api_key、redis addr
```

### 运行

```bash
./sub2api-scheduler
# 或：CONFIG_PATH=/etc/sub2api-scheduler/config.yaml ./sub2api-scheduler
```

打开看板：`http://127.0.0.1:8090`。

---

## 数据库初始化

调度器复用 Sub2API 的共享库。在 Sub2API 同一个 PostgreSQL 里建泳道相关表（一次性执行）：

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

然后插入泳道图：每个图对应一个模型和它的泳道，例如

```sql
INSERT INTO lane_boards (name, model) VALUES ('my-flash', 'flash-v1');

INSERT INTO lane_boards_lanes (board_id, position, name, account_ids) VALUES
(1, 0, 'primary-proxy',   '{11}'),
(1, 1, 'reseller',        '{22}'),
(1, 2, 'official',        '{33}');
```

调度器从左到右取泳道；第一个存在健康账号的泳道为 active。

---

## 工作原理

```
        ┌──────────────────────────────┐
        │      sub2api-scheduler       │
        │                              │
        │  CheckErrors (5s)            │
        │   统计窗口内上游 5xx/429/错误 │
        │   → 超阈值禁用               │
        │                              │
        │  ProbeLoop (30s)             │
        │   探测被禁用账号             │
        │   门槛通过才恢复             │
        │                              │
        │  reconcile (每周期)          │
        │   找 active 泳道             │
        │   压制低泳道                 │
        │   释放并验证高泳道           │
        └──────────┬───────────────────┘
                   │ admin API (PUT/POST/DELETE)
                   ▼
        ┌──────────────────────────────┐
        │  Sub2API 网关                │
        │  账号调度快照                │
        │  (outbox → Redis, 一致)      │
        └──────────────────────────────┘
```

### 状态机

| 状态 | 含义 | Sub2API 是否调度 |
|---|---|---|
| `healthy` | 正常接流量 | ✅ |
| `disabled` | 失败过多 / 探测失败 | ❌ (限流条目) |
| `suppressed` | 健康但更高泳道在激活 | ❌ (限流条目) |

### 恢复规则

被禁用账号只有在**两个条件同时满足**时才恢复：

1. **ProbeLoop 通过** — 真实 `/admin/accounts/:id/test` 调用成功；
2. **CheckErrors 通过** — 该账号最近 `window_seconds` 内上游失败数低于 `fail_threshold`。

这防乒乓：真实流量仍在失败的账号永远不会被重新启用后立刻再次熔断。

---

## 配置

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `server.port` | `8090` | 看板端口 |
| `database.dsn` | — | Sub2API PostgreSQL DSN |
| `sub2api.base_url` | `http://sub2api:8080` | Sub2API 内部 API |
| `sub2api.admin_api_key` | — | Sub2API Admin API Key |
| `redis.addr` | `redis:6379` | Sub2API Redis |
| `redis.password` | — | Redis 密码 |

每个泳道图的调参在数据库里（`lane_boards` 表）：

| 列 | 含义 |
| --- | --- |
| `fail_threshold` | 窗口内失败多少次即禁用账号 |
| `window_seconds` | 滑动错误窗口长度 |
| `probe_interval` | 探测周期（秒） |

---

## License

[MIT](LICENSE) © 2026 yiiilin