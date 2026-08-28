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

模型限流变更通过共享 PostgreSQL 行锁事务和 `account_changed` outbox 原子完成；探测和账号级
调度开关仍调用 Sub2API Admin API。这样不会产生 GET/PUT 丢失更新，也能持续刷新网关调度快照。

---

## 特性

- **泳道路由** — 同模型账号按优先级排序，只有最高健康泳道接流量，低泳道全部压制。
- **滚动失败检测** — 统计滑动窗口（默认 60s）内的上游 5xx / 429 / 网络错误，超阈值禁用账号。
- **探测 + 恢复门槛** — 被禁用账号用真实 upstream 调用探测；只有**探测通过 且 真实流量窗口内无失败**才恢复，防止坏账号反复横跳。
- **感知外部状态** — 尊重 Sub2API 自身的 schedulable 开关、status、冷却、模型限流；对网关自己关闭的账号防抖重开，不与网关打架。
- **调度缓存一致** — 模型限流更新与共享 PostgreSQL 行锁、`account_changed` outbox 原子完成；探测和账号开关走 Sub2API Admin API。
- **Web 看板** — 每模型一页，显示泳道状态、失败数、探测状态、手动探测按钮。
- **自托管** — 单个静态 Go 二进制，监听 localhost，YAML 配置。

---

## 快速开始

### 前置

- 一个已应用 `scheduler_outbox` 迁移的最新 [Sub2API](https://github.com/Wei-Shaw/sub2api) 实例（含 PostgreSQL 和 Redis）。
- Sub2API 的 Admin API Key（Server → API Keys → Admin API Key）。
- `database.dsn` 使用的 PostgreSQL 用户需要具备读写 `accounts`、创建/迁移调度器 `lane_*` 表以及写入
  `scheduler_outbox` 的权限。

### 编译

```bash
go build -o sub2api-scheduler .
```

### 配置

```bash
cp config.example.yaml config.yaml
# 编辑：database DSN、sub2api base_url + admin_api_key
```

### 运行

```bash
./sub2api-scheduler
# 或：CONFIG_PATH=/etc/sub2api-scheduler/config.yaml ./sub2api-scheduler
```

打开看板：`http://127.0.0.1:8090`。

---

## 数据库初始化

无需手工执行 SQL。守护进程启动时会在 Sub2API PostgreSQL 数据库中自动创建并迁移
`lane_boards`、`lane_boards_lanes` 和 `lane_account_states`。旧版 README
创建的表也会自动补齐泳道 ID、默认值、状态行和唯一约束。

请通过 Web 看板创建和编辑泳道图。每个模型只能属于一个泳道图，同一账号在图中只能出现一次；账号需要按照 Sub2API 的显式 mapping、平台默认 mapping
或透传规则支持该模型。

如果存量数据存在重复的泳道图名称、模型或同一图内的账号归属，启动迁移会主动失败，需
先清理重复数据再重启。迁移也会忽略旧账号 ID 数组中的 NULL 和非正数元素。

同一个数据库同一时间只能运行一个调度器实例；启动时会获取 PostgreSQL advisory lock，
第二个实例会直接拒绝启动。

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
                    │ PostgreSQL 行锁 + account_changed outbox
                    │ admin API（探测 / schedulable）
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
| `server.host` | `127.0.0.1` | 监听地址（设为 `0.0.0.0` 可对外暴露） |
| `server.port` | `8090` | 看板端口 |
| `database.dsn` | — | Sub2API PostgreSQL DSN（**必填**） |
| `sub2api.base_url` | `http://sub2api:8080` | Sub2API 内部 API |
| `sub2api.admin_api_key` | — | Sub2API Admin API Key（**必填**） |

`database.dsn` 与 `sub2api.admin_api_key` 为必填项：缺失时守护进程直接拒绝启动
（fail-fast），避免带着失控的控制面静默运行。

每个泳道图的调参在数据库里（`lane_boards` 表）：

| 列 | 含义 |
| --- | --- |
| `fail_threshold` | 窗口内失败多少次即禁用账号 |
| `window_seconds` | 滑动错误窗口长度 |
| `probe_interval` | 探测周期（秒） |

---

## License

[MIT](LICENSE) © 2026 yiiilin