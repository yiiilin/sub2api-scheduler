package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// ============================================================================
// 泳道图（Lane Board）调度方案
//
// 概念：
//   - 一个泳道图 = 一个模型（图列表 = 关心的模型）
//   - 泳道从左到右 = 优先级从高到低（position 升序）
//   - 只有映射了该模型的账号才能加入泳道图
//
// 账号级状态机（每 图×账号 独立）：
//   - healthy:   60s 窗口内该模型请求失败 >= fail_threshold 次 → disabled（真实流量信号判定）
//   - disabled:  仅当处于 active 泳道及以上（接管候选）时，每 probe_interval 秒真实探测一次，
//                成功 → healthy；healthy 不探测（避免无差别定时探测，恢复靠探测、挂靠流量失败）
//
// 泳道级规则（严格分层）：
//   - 高优先级泳道存在 healthy 账号时，更低泳道所有账号一律压制（suppressed），不调度
//   - active 泳道（第一个有 healthy 账号的泳道）内所有账号 disabled → 释放+立即探测下一泳道
//   - 被压制账号不探测；释放时立即探测验证（失败则转为 disabled）
//   - 探测范围 = active 泳道及以上泳道的 disabled 账号（active 内挂掉的 + 更高泳道候选）；
//     active 之下的 disabled 不探测（没资格接管，等上游全挂升为候选再恢复）
//
// 控制手段（模型级）：
//   - 禁用:  写 accounts.extra.model_rate_limits.<model>（rate_limit_reset_at=null 手动禁用）
//   - 恢复:  删除自己写入的条目（reason 前缀 lane_board:），sub2api 自动限流条目不动
//   - 每次变更后 DEL Redis sched:acc:<id>（调度缓存 TTL=-1 持久，必须清）
// ============================================================================

// LaneBoard 泳道图（= 一个模型）
type LaneBoard struct {
	ID            int64     `json:"id"`
	Name          string    `json:"name"`
	Model         string    `json:"model"`
	Enabled       bool      `json:"enabled"`
	FailThreshold int       `json:"fail_threshold"` // 窗口内失败次数阈值，默认 3
	WindowSeconds int       `json:"window_seconds"` // 失败统计窗口，默认 60
	ProbeInterval int       `json:"probe_interval"` // 探测间隔秒，默认 30
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	Lanes         []Lane    `json:"lanes"`
}

// Lane 泳道（同优先级账号组）
type Lane struct {
	ID         int64   `json:"id"`
	BoardID    int64   `json:"board_id"`
	Position   int     `json:"position"`
	Name       string  `json:"name"`
	AccountIDs []int64 `json:"account_ids"`
}

// AccountState 账号在某个泳道图中的状态（本地表持久化）
type AccountState struct {
	BoardID      int64      `json:"board_id"`
	AccountID    int64      `json:"account_id"`
	State        string     `json:"state"` // healthy / disabled
	DisabledAt   *time.Time `json:"disabled_at"`
	LastProbeAt  *time.Time `json:"last_probe_at"`
	LastProbeOK  *bool      `json:"last_probe_ok"`
	LastProbeMsg string     `json:"last_probe_msg"`
	FailCount    int        `json:"fail_count"` // 最近一次错误窗口计数
	CheckedAt    *time.Time `json:"checked_at"`
}

const (
	LaneStateHealthy    = "healthy"
	LaneStateDisabled   = "disabled"
	LaneStateSuppressed = "suppressed" // 被更高优先级泳道压制（即使健康也不调度）
	laneReasonPrefix    = "lane_board:"
	laneSuppressPrefix  = "lane_board:suppressed:"
	// 压制/禁用条目用遥远未来时间戳而非 null：
	// 官方 UI 只渲染 rate_limit_reset_at 在未来 的条目（null 视为"手动禁用"不显示）；
	// 2099 永不过期 → 调度器持续不调度，sub2api 也不会自动清理
	laneFarFuture = "2099-12-31T23:59:59Z"
)

// ============================ Schema ============================

func (d *DB) ensureLaneBoardSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS lane_boards (
			id              BIGSERIAL PRIMARY KEY,
			name            TEXT NOT NULL UNIQUE,
			model           TEXT NOT NULL,
			enabled         BOOLEAN NOT NULL DEFAULT true,
			fail_threshold  INT NOT NULL DEFAULT 3,
			window_seconds  INT NOT NULL DEFAULT 60,
			probe_interval  INT NOT NULL DEFAULT 30,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS lane_boards_lanes (
			id          BIGSERIAL PRIMARY KEY,
			board_id    BIGINT NOT NULL REFERENCES lane_boards(id) ON DELETE CASCADE,
			position    INT NOT NULL DEFAULT 0,
			name        TEXT NOT NULL DEFAULT '',
			account_ids BIGINT[] NOT NULL DEFAULT '{}'
		)`,
		`CREATE TABLE IF NOT EXISTS lane_account_states (
			board_id       BIGINT NOT NULL REFERENCES lane_boards(id) ON DELETE CASCADE,
			account_id     BIGINT NOT NULL,
			state          TEXT NOT NULL DEFAULT 'healthy',
			disabled_at    TIMESTAMPTZ,
			last_probe_at  TIMESTAMPTZ,
			last_probe_ok  BOOLEAN,
			last_probe_msg TEXT NOT NULL DEFAULT '',
			fail_count     INT NOT NULL DEFAULT 0,
			checked_at     TIMESTAMPTZ,
			PRIMARY KEY (board_id, account_id)
		)`,
	}
	for _, s := range stmts {
		if _, err := d.pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// ============================ CRUD ============================

func (d *DB) ListBoards(ctx context.Context) ([]LaneBoard, error) {
	rows, err := d.pool.Query(ctx, `
SELECT id, name, model, enabled, fail_threshold, window_seconds, probe_interval, created_at, updated_at
FROM lane_boards ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LaneBoard
	for rows.Next() {
		var b LaneBoard
		if err := rows.Scan(&b.ID, &b.Name, &b.Model, &b.Enabled, &b.FailThreshold,
			&b.WindowSeconds, &b.ProbeInterval, &b.CreatedAt, &b.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		lanes, err := d.ListLanes(ctx, out[i].ID)
		if err != nil {
			return nil, err
		}
		out[i].Lanes = lanes
	}
	return out, nil
}

func (d *DB) ListLanes(ctx context.Context, boardID int64) ([]Lane, error) {
	rows, err := d.pool.Query(ctx, `
SELECT id, board_id, position, name, account_ids
FROM lane_boards_lanes WHERE board_id=$1 ORDER BY position ASC, id ASC`, boardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Lane
	for rows.Next() {
		var l Lane
		if err := rows.Scan(&l.ID, &l.BoardID, &l.Position, &l.Name, &l.AccountIDs); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

func (d *DB) GetBoard(ctx context.Context, id int64) (*LaneBoard, error) {
	var b LaneBoard
	err := d.pool.QueryRow(ctx, `
SELECT id, name, model, enabled, fail_threshold, window_seconds, probe_interval, created_at, updated_at
FROM lane_boards WHERE id=$1`, id).
		Scan(&b.ID, &b.Name, &b.Model, &b.Enabled, &b.FailThreshold,
			&b.WindowSeconds, &b.ProbeInterval, &b.CreatedAt, &b.UpdatedAt)
	if err != nil {
		return nil, err
	}
	lanes, err := d.ListLanes(ctx, id)
	if err != nil {
		return nil, err
	}
	b.Lanes = lanes
	return &b, nil
}

// SaveBoard 创建或更新泳道图（含泳道，事务内全量替换）
func (d *DB) SaveBoard(ctx context.Context, b *LaneBoard) error {
	if b.FailThreshold <= 0 {
		b.FailThreshold = 3
	}
	if b.WindowSeconds <= 0 {
		b.WindowSeconds = 60
	}
	if b.ProbeInterval <= 0 {
		b.ProbeInterval = 30
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if b.ID == 0 {
		err = tx.QueryRow(ctx, `
INSERT INTO lane_boards (name, model, enabled, fail_threshold, window_seconds, probe_interval)
VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at, updated_at`,
			b.Name, b.Model, b.Enabled, b.FailThreshold, b.WindowSeconds, b.ProbeInterval).
			Scan(&b.ID, &b.CreatedAt, &b.UpdatedAt)
	} else {
		_, err = tx.Exec(ctx, `
UPDATE lane_boards SET name=$2, model=$3, enabled=$4, fail_threshold=$5, window_seconds=$6, probe_interval=$7, updated_at=now()
WHERE id=$1`,
			b.ID, b.Name, b.Model, b.Enabled, b.FailThreshold, b.WindowSeconds, b.ProbeInterval)
		if err == nil {
			// 全量替换泳道
			if _, err = tx.Exec(ctx, `DELETE FROM lane_boards_lanes WHERE board_id=$1`, b.ID); err != nil {
				return err
			}
			// 清理不再存在的账号状态
			if _, err = tx.Exec(ctx, `
DELETE FROM lane_account_states WHERE board_id=$1 AND NOT (account_id = ANY(
  (SELECT array_agg(aid) FROM (
    SELECT unnest(account_ids) AS aid FROM lane_boards_lanes WHERE board_id=$1
  ) t WHERE aid IS NOT NULL)::bigint[]
))`, b.ID); err != nil {
				return err
			}
		}
	}
	if err != nil {
		return err
	}
	for i := range b.Lanes {
		l := &b.Lanes[i]
		var lid int64
		if err := tx.QueryRow(ctx, `
INSERT INTO lane_boards_lanes (board_id, position, name, account_ids)
VALUES ($1,$2,$3,$4) RETURNING id`,
			b.ID, i, l.Name, l.AccountIDs).Scan(&lid); err != nil {
			return err
		}
		l.ID = lid
		l.BoardID = b.ID
		// 为新账号补 healthy 状态行
		for _, aid := range l.AccountIDs {
			if _, err := tx.Exec(ctx, `
INSERT INTO lane_account_states (board_id, account_id) VALUES ($1,$2)
ON CONFLICT (board_id, account_id) DO NOTHING`, b.ID, aid); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func (d *DB) DeleteBoard(ctx context.Context, id int64) error {
	_, err := d.pool.Exec(ctx, `DELETE FROM lane_boards WHERE id=$1`, id)
	return err
}

func (d *DB) GetAccountStates(ctx context.Context, boardID int64) (map[int64]AccountState, error) {
	rows, err := d.pool.Query(ctx, `
SELECT board_id, account_id, state, disabled_at, last_probe_at, last_probe_ok, last_probe_msg, fail_count, checked_at
FROM lane_account_states WHERE board_id=$1`, boardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int64]AccountState)
	for rows.Next() {
		var s AccountState
		if err := rows.Scan(&s.BoardID, &s.AccountID, &s.State, &s.DisabledAt,
			&s.LastProbeAt, &s.LastProbeOK, &s.LastProbeMsg, &s.FailCount, &s.CheckedAt); err != nil {
			return nil, err
		}
		out[s.AccountID] = s
	}
	return out, rows.Err()
}

// ============================ Monitor ============================

// LaneBoardMonitor 泳道图健康监控器
type LaneBoardMonitor struct {
	db     *DB
	client *Sub2APIClient
	rdb    *RedisClient
	mu     sync.Mutex
	// 上次打开账号调度开关的时间（防抖：sub2api 自动关闭=上游真实失败信号，短时间不重开）
	schedOpenAt map[int64]time.Time
	// schedOpenAt 专用锁：ensureSchedulable 可能在 m.mu 已持有（ProbeLoop/releaseVerify 链）时被调用，
	// 用独立锁避免 sync.Mutex 不可重入造成的死锁（2026-08-13 实测：ProbeLoop 卡死 6 小时）
	schedMu sync.Mutex
}

func NewLaneBoardMonitor(db *DB, client *Sub2APIClient, rdb *RedisClient) *LaneBoardMonitor {
	return &LaneBoardMonitor{db: db, client: client, rdb: rdb, schedOpenAt: make(map[int64]time.Time)}
}

// Start 启动两个循环：
//   - 错误统计：5s 周期，统计最近 1 分钟（WindowSeconds）窗口内失败数，超阈值 → 限流
//   - 探测：30s 周期（默认 probe_interval）
func (m *LaneBoardMonitor) Start(ctx context.Context) {
	log.Printf("[laneboard] monitor started (error_check=5s, probe=30s)")
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.CheckErrors(ctx)
			}
		}
	}()
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.ProbeLoop(ctx)
			}
		}
	}()
}

// CheckErrors 每 5s 统计每个图×账号最近 1 分钟窗口内失败数，超过阈值 → 限流
// 只统计当前 active 泳道及更高优先级（position <= activeIdx）的账号；
// 更低泳道（备用/压制态）不接流量，不统计。
func (m *LaneBoardMonitor) CheckErrors(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	boards, err := m.db.ListBoards(ctx)
	if err != nil {
		log.Printf("[laneboard] list boards: %v", err)
		return
	}
	for _, b := range boards {
		if !b.Enabled {
			continue
		}
		m.checkBoardErrors(ctx, &b)
	}
}

func (m *LaneBoardMonitor) checkBoardErrors(ctx context.Context, b *LaneBoard) {
	// 先做分层压制 reconcile（active 泳道变化也在此驱动），拿到 activeIdx
	activeIdx := m.reconcileBoard(ctx, b)
	// 收集该图所有账号 ID
	var ids []int64
	for _, l := range b.Lanes {
		ids = append(ids, l.AccountIDs...)
	}
	if len(ids) == 0 {
		return
	}
	window := time.Duration(b.WindowSeconds) * time.Second
	failCounts, err := m.db.CountModelFailures(ctx, b.Model, ids, window)
	if err != nil {
		log.Printf("[laneboard] board=%s count failures: %v", b.Name, err)
		return
	}
	states, err := m.db.GetAccountStates(ctx, b.ID)
	if err != nil {
		log.Printf("[laneboard] board=%s get states: %v", b.Name, err)
		return
	}
	now := time.Now()
	for i, l := range b.Lanes {
		// 只统计当前 active 泳道及更高优先级（position <= activeIdx）的账号；
		// 全挂（activeIdx=-1）时无 active 泳道，所有账号都是恢复候选，全部统计
		if activeIdx >= 0 && i > activeIdx {
			continue
		}
		for _, aid := range l.AccountIDs {
			st, ok := states[aid]
			if !ok {
				continue
			}
			st.FailCount = failCounts[aid]
			st.CheckedAt = &now
			// 只有 active 泳道的 healthy 账号才做失败判定；suppressed/disabled 跳过
			// （外部抑制已在 reconcileBoard 统一转为 disabled）
			if st.State == LaneStateHealthy && failCounts[aid] >= b.FailThreshold {
				m.disableAccount(ctx, b, aid, st)
			} else {
				_ = m.db.UpdateAccountStateCheck(ctx, b.ID, aid, st.FailCount, now)
			}
		}
	}
}

// externallyDisable 因 sub2api 外部抑制而禁用（不写泳道图条目——原生条目已在挡，避免覆盖）
func (m *LaneBoardMonitor) externallyDisable(ctx context.Context, b *LaneBoard, aid int64, st AccountState, reason string) {
	now := time.Now()
	_ = m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now)
	m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "disable", "sub2api 外部抑制: "+reason+"，标记禁用等待恢复")
	log.Printf("[laneboard] board=%s account=%d externally blocked (%s) -> disabled", b.Name, aid, reason)
}

// reconcileBoard 严格分层：找到第一个存在 healthy 账号的泳道（active），
// 压制所有更低泳道的 healthy 账号；active 泳道内的 suppressed 账号释放
func (m *LaneBoardMonitor) reconcileBoard(ctx context.Context, b *LaneBoard) int {
	states, err := m.db.GetAccountStates(ctx, b.ID)
	if err != nil {
		log.Printf("[laneboard] board=%s reconcile states: %v", b.Name, err)
		return -1
	}
	now := time.Now()
	// 统一处理外部抑制：healthy 但被 sub2api 原生机制（503冷却/临时禁调度）挡住 → 立即 disabled
	// 任何入口（check/probe/manual）都走这里，保证状态一致，不会误判 active
	extBlocks, _ := m.db.GetExternalBlocks(ctx, boardAccountIDs(b), b.Model)
	for i := range b.Lanes {
		for _, aid := range b.Lanes[i].AccountIDs {
			st, ok := states[aid]
			if !ok || st.State != LaneStateHealthy {
				continue
			}
			if extBlocks[aid].blocked(now) {
				m.externallyDisable(ctx, b, aid, st, extBlocks[aid].blockedReason(now))
				st.State = LaneStateDisabled
				states[aid] = st
			}
		}
	}
	// 找 active 泳道（第一个存在 healthy 账号的；被外部抑制的已转 disabled 不算）
	activeIdx := -1
	for i := range b.Lanes {
		l := &b.Lanes[i]
		for _, aid := range l.AccountIDs {
			if st, ok := states[aid]; ok && st.State == LaneStateHealthy {
				activeIdx = i
				break
			}
		}
		if activeIdx >= 0 {
			break
		}
	}
	// 没有 active 泳道（全部 disabled/suppressed）：释放所有 suppressed，等待探测恢复决定
	if activeIdx == -1 {
		for i := range b.Lanes {
			for _, aid := range b.Lanes[i].AccountIDs {
				st, ok := states[aid]
				if !ok || st.State != LaneStateSuppressed {
					continue
				}
				deleted, err := m.db.ClearSuppressIfOwned(ctx, aid, b.Model)
				if err != nil {
					log.Printf("[laneboard] board=%s account=%d release sql: %v", b.Name, aid, err)
					continue
				}
				if deleted {
					m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "release", "全部泳道不可用，恢复 "+b.Model+" 调度等待探测")
					log.Printf("[laneboard] board=%s account=%d released (no active lane)", b.Name, aid)
				} else {
					// 本地没有抑制条目记录（可能是 API 方式写入的），直接用 API 清一次确保干净
					if err := m.client.ClearModelRateLimit(ctx, aid, b.Model); err != nil {
						log.Printf("[laneboard] board=%s account=%d release api: %v", b.Name, aid, err)
					}
				}
				// 切换验证：真实调用一次；失败 → 写限流条目禁用，等定时探测恢复
				m.releaseVerify(ctx, b, aid, st, now)
			}
		}
		return -1
	}
	// 压制 active 之后所有泳道的 healthy/suppressed 账号（disabled 保持，等待探测恢复）
	// 注意：状态为 suppressed 但条目缺失（历史 bug 或手动清过）也要补写 —— 幂等 ensure
	for i := activeIdx + 1; i < len(b.Lanes); i++ {
		for _, aid := range b.Lanes[i].AccountIDs {
			st, ok := states[aid]
			if !ok || st.State == LaneStateDisabled {
				continue
			}
			if st.State == LaneStateHealthy {
				m.suppressAccount(ctx, b, aid)
				_ = m.db.SetAccountState(ctx, b.ID, aid, LaneStateSuppressed, &now)
				continue
			}
			// state 已是 suppressed：确认抑制条目存在，缺失则补写
			has, err := m.db.HasSuppressEntry(ctx, aid, b.Model)
			if err != nil {
				log.Printf("[laneboard] board=%s account=%d has-suppress: %v", b.Name, aid, err)
				continue
			}
			if !has {
				m.suppressAccount(ctx, b, aid)
				log.Printf("[laneboard] board=%s account=%d re-suppressed (entry was missing)", b.Name, aid)
			}
		}
	}
	// 释放 active 泳道及更高泳道（position <= activeIdx）内的 suppressed 账号：
	//   - active 泳道内 suppressed：上层全挂后本泳道成为 active 的情形，必须释放并验证
	//   - 更高泳道 suppressed：历史残留（该泳道已无 healthy 但 suppressed 未清），
	//     释放+验证成功 → healthy → active 升回更高泳道（期望的恢复）
	// 必须走 releaseVerify 验证调用：直接置 healthy 会产生未验证的假 healthy
	for i := 0; i <= activeIdx; i++ {
		for _, aid := range b.Lanes[i].AccountIDs {
			st, ok := states[aid]
			if !ok || st.State != LaneStateSuppressed {
				continue
			}
			deleted, err := m.db.ClearSuppressIfOwned(ctx, aid, b.Model)
			if err != nil {
				log.Printf("[laneboard] board=%s account=%d release sql: %v", b.Name, aid, err)
				continue
			}
			if deleted {
				m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "release", "高泳道不可用，恢复 "+b.Model+" 调度")
				log.Printf("[laneboard] board=%s account=%d suppression released", b.Name, aid)
			} else {
				// 本地无抑制条目记录，直接用 API 清一次确保干净
				if err := m.client.ClearModelRateLimit(ctx, aid, b.Model); err != nil {
					log.Printf("[laneboard] board=%s account=%d release api: %v", b.Name, aid, err)
				}
			}
			m.releaseVerify(ctx, b, aid, st, now)
		}
	}
	return activeIdx
}

// suppressAccount 压制：通过 sub2api 管理 API 写抑制条目（reason lane_board:suppressed:<board>）
func (m *LaneBoardMonitor) suppressAccount(ctx context.Context, b *LaneBoard, aid int64) {
	entry := map[string]any{
		"reason":              laneSuppressPrefix + b.Name,
		"rate_limited_at":     time.Now().UTC().Format(time.RFC3339),
		"rate_limit_reset_at": laneFarFuture,
	}
	if err := m.client.SetModelRateLimit(ctx, aid, b.Model, entry); err != nil {
		log.Printf("[laneboard] board=%s account=%d suppress api: %v", b.Name, aid, err)
		return
	}
	m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "suppress", "高泳道可用，暂停 "+b.Model+" 调度")
	log.Printf("[laneboard] board=%s account=%d suppressed", b.Name, aid)
}

// ProbeLoop 探测所有 disabled 账号；泳道全挂 → 下一泳道立即探测
func (m *LaneBoardMonitor) ProbeLoop(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	boards, err := m.db.ListBoards(ctx)
	if err != nil {
		log.Printf("[laneboard] list boards: %v", err)
		return
	}
	for _, b := range boards {
		if !b.Enabled {
			continue
		}
		m.probeBoard(ctx, &b)
	}
}

func (m *LaneBoardMonitor) probeBoard(ctx context.Context, b *LaneBoard) {
	// 先 reconcile：active 泳道变化、外部抑制、释放（全挂释放 / 候选恢复）都在这里处理
	activeIdx := m.reconcileBoard(ctx, b)
	states, err := m.db.GetAccountStates(ctx, b.ID)
	if err != nil {
		log.Printf("[laneboard] board=%s states: %v", b.Name, err)
		return
	}
	interval := time.Duration(b.ProbeInterval) * time.Second
	now := time.Now()
	// 外部抑制（sub2api 原生冷却/临时禁调度）：探测恢复前要跳过
	extBlocks, _ := m.db.GetExternalBlocks(ctx, boardAccountIDs(b), b.Model)
	// 探测集合 = active 泳道及以上（position <= activeIdx）的 disabled 账号：
	//   - active 泳道内挂掉的账号（真实流量失败判定后禁用）必须探测恢复
	//   - 更高泳道的 disabled 是潜在接管候选，也要探测（恢复 = 流量回到更高优先级）
	//   - active 之下的 disabled 不探测（没资格接管，探测成功也不能调度，等上游全挂升为候选）
	//   - healthy 不探测（在正常工作）；suppressed 不探测（不该接流量）
	// 全挂（activeIdx == -1）→ 全部 disabled 都探测（都是恢复候选）
	for i := range b.Lanes {
		if activeIdx >= 0 && i > activeIdx {
			break
		}
		for _, aid := range b.Lanes[i].AccountIDs {
			st, ok := states[aid]
			if !ok || st.State != LaneStateDisabled {
				continue
			}
			eb := extBlocks[aid]
			// 原生冷却/临时禁调度中的账号跳过（冷却中探测无意义，过期后自然恢复探测）；
			// 但仅"账号调度开关关闭"的不跳过——泳道图验证成功就要重新打开开关（否则永远无法恢复）
			if eb.blocked(now) && !(eb.Status == "active" && eb.NativeCoolUntil == nil && eb.TempUnschedUntil == nil) {
				continue
			}
			if st.LastProbeAt != nil && now.Sub(*st.LastProbeAt) < interval {
				continue
			}
			m.probeAccount(ctx, b, aid, st, now)
		}
	}
}

// boardAccountIDs 收集泳道图所有账号 ID
func boardAccountIDs(b *LaneBoard) []int64 {
	var out []int64
	for _, l := range b.Lanes {
		out = append(out, l.AccountIDs...)
	}
	return out
}

// checkErrorsGate 恢复前置门槛（用户规则 2026-08-17）：
// 探测恢复必须 CheckErrors 与 ProbeLoop 同时通过——
//   CheckErrors 通过 = 账号在 WindowSeconds 窗口内失败数 < fail_threshold
//   ProbeLoop 通过  = /test 探测调用成功
// CheckErrors 未通过 = 真实流量仍在窗口内失败超阈值，恢复后立刻又会被禁用（乒乓根因），禁止恢复。
// 返回 (是否通过, 窗口失败数, 错误)
func (m *LaneBoardMonitor) checkErrorsGate(ctx context.Context, b *LaneBoard, aid int64) (bool, int, error) {
	window := time.Duration(b.WindowSeconds) * time.Second
	counts, err := m.db.CountModelFailures(ctx, b.Model, []int64{aid}, window)
	if err != nil {
		return false, 0, err
	}
	n := counts[aid]
	return n < b.FailThreshold, n, nil
}

// probeAccount 用图模型真实探测账号；成功且无外部抑制 → 恢复调度
func (m *LaneBoardMonitor) probeAccount(ctx context.Context, b *LaneBoard, aid int64, st AccountState, now time.Time) {
	ok, msg, err := m.client.TestAccountModel(ctx, aid, b.Model)
	if err != nil {
		msg = err.Error()
		ok = false
	}
	// 恢复前置门槛（用户规则）：CheckErrors 与 ProbeLoop 同时通过
	// 探测成功但窗口内失败数仍 ≥ 阈值 → 不恢复（真实流量还在失败）
	if ok {
		gatePass, n, gerr := m.checkErrorsGate(ctx, b, aid)
		if gerr != nil {
			log.Printf("[laneboard] board=%s account=%d check-errors gate: %v", b.Name, aid, gerr)
			_ = m.db.UpdateAccountStateProbe(ctx, b.ID, aid, ok, "CheckErrors 检查失败，暂不恢复", now)
			return
		}
		if !gatePass {
			gmsg := fmt.Sprintf("探测成功但%d秒窗口内%d次失败≥阈值%d，CheckErrors未通过不恢复", b.WindowSeconds, n, b.FailThreshold)
			_ = m.db.UpdateAccountStateProbe(ctx, b.ID, aid, ok, gmsg, now)
			// 无条目则补写禁用（防止 sub2api 照常路由一个正在失败中的账号）
			m.disableAccount(ctx, b, aid, st)
			log.Printf("[laneboard] board=%s account=%d probe ok but %s", b.Name, aid, gmsg)
			return
		}
	}
	// 探测成功但被 sub2api 外部抑制挡住 → 不能恢复（避免 探测成功→恢复→又禁用 循环）
	// 但账号开关关闭的：泳道图验证通过 → 重新打开（否则永远无法恢复）
	if ok {
		canSched, reason := m.ensureSchedulable(ctx, b, aid)
		if !canSched {
			_ = m.db.UpdateAccountStateProbe(ctx, b.ID, aid, ok, "探测成功但"+reason, now)
			log.Printf("[laneboard] board=%s account=%d probe ok but %s, stay disabled", b.Name, aid, reason)
			return
		}
	}
	// 更新探测状态
	_ = m.db.UpdateAccountStateProbe(ctx, b.ID, aid, ok, msg, now)
	if ok {
		_ = m.db.ResetProbeFail(ctx, b.ID, aid, now)
		log.Printf("[laneboard] board=%s account=%d recovered", b.Name, aid)
		m.enableAccount(ctx, b, aid, st)
	} else {
		newFail, ferr := m.db.IncProbeFail(ctx, b.ID, aid, now)
		if ferr != nil {
			log.Printf("[laneboard] board=%s account=%d inc fail: %v", b.Name, aid, ferr)
			return
		}
		if newFail >= b.FailThreshold {
			// 连续探测失败达阈值 → 写条目禁用（sub2api 停止路由），等后续探测恢复
			// disabled 且无条目的账号（reconcile 转的）探测失败同样要写条目，否则 sub2api 照常路由
			st.FailCount = newFail
			m.disableAccount(ctx, b, aid, st)
			log.Printf("[laneboard] board=%s account=%d probe fail x%d -> disabled w/ entry", b.Name, aid, newFail)
		} else {
			log.Printf("[laneboard] board=%s account=%d probe fail: %s", b.Name, aid, msg)
		}
	}
}

// disableAccount 禁用：写 model_rate_limits（无活跃自动条目时）+ 清缓存
// ensureSchedulable 验证调用成功后，确保账号级调度开关打开
// 泳道图判定可用（探测通过）→ 账号必须真正可调度；sub2api 自动关闭的开关（连续失败等）
// 在泳道图验证通过后重新打开。仅当阻塞原因纯粹是账号开关关闭时打开；
// 若有原生冷却/临时禁调度/状态异常则不动（那些机制在管，不该强制覆盖）。
// 返回 (最终是否可调度, 阻塞原因)
func (m *LaneBoardMonitor) ensureSchedulable(ctx context.Context, b *LaneBoard, aid int64) (bool, string) {
	blocks, err := m.db.GetExternalBlocks(ctx, []int64{aid}, b.Model)
	if err != nil {
		return false, "查询外部抑制失败: " + err.Error()
	}
	eb := blocks[aid]
	if !eb.blocked(time.Now()) {
		return true, ""
	}
	// 仅因账号开关关闭（其余维度都正常）→ 重新打开（带防抖）
	if !eb.Schedulable && eb.Status == "active" && eb.NativeCoolUntil == nil && eb.TempUnschedUntil == nil {
		m.schedMu.Lock()
		lastOpen, opened := m.schedOpenAt[aid]
		m.schedMu.Unlock()
		if opened && time.Since(lastOpen) < 2*time.Minute {
			// sub2api 反复关闭 = 上游真实流量持续失败，尊重它，不重开（防乒乓）
			return false, "sub2api 已关闭调度（上游真实流量失败），防抖期内不重开"
		}
		if _, err := m.client.SetSchedulable(ctx, aid, true); err != nil {
			return false, "打开账号调度开关失败: " + err.Error()
		}
		m.schedMu.Lock()
		m.schedOpenAt[aid] = time.Now()
		m.schedMu.Unlock()
		m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "enable", "验证调用成功，重新打开账号调度开关")
		log.Printf("[laneboard] board=%s account=%d re-enabled schedulable", b.Name, aid)
		// 重查确认
		blocks2, err2 := m.db.GetExternalBlocks(ctx, []int64{aid}, b.Model)
		if err2 == nil && !blocks2[aid].blocked(time.Now()) {
			return true, ""
		}
		return false, "打开后仍被抑制"
	}
	return false, eb.blockedReason(time.Now())
}

// releaseVerify 泳道切换后的验证调用：对释放的账号真实调用一次
//   - 成功且无外部抑制 → healthy（sub2api 正常调度）
//   - 成功但外部抑制（账号开关关闭/原生冷却等）→ disabled（不写条目，原生机制已在挡）
//   - 失败 → 写 lane_board failed 条目真正禁用（sub2api 停止路由），等定时探测恢复
func (m *LaneBoardMonitor) releaseVerify(ctx context.Context, b *LaneBoard, aid int64, st AccountState, now time.Time) {
	ok, msg, err := m.client.TestAccountModel(ctx, aid, b.Model)
	if err != nil {
		msg = err.Error()
		ok = false
	}
	_ = m.db.UpdateAccountStateProbe(ctx, b.ID, aid, ok, msg, now)
	if ok {
		// 恢复前置门槛（用户规则）：CheckErrors 与 ProbeLoop 同时通过
		// 验证调用成功但窗口内失败数仍 ≥ 阈值 → 不恢复（真实流量还在失败）
		gatePass, n, gerr := m.checkErrorsGate(ctx, b, aid)
		if gerr != nil {
			_ = m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now)
			m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "disable", "释放后验证成功但 CheckErrors 检查失败，保持禁用")
			log.Printf("[laneboard] board=%s account=%d released, check-errors gate err: %v", b.Name, aid, gerr)
			return
		}
		if !gatePass {
			// 补写禁用条目（无则写）：suppress 条目已删，防止 sub2api 照常路由一个正在失败中的账号
			m.disableAccount(ctx, b, aid, st)
			gmsg := fmt.Sprintf("释放后验证成功但%d秒窗口内%d次失败≥阈值%d，CheckErrors未通过不恢复", b.WindowSeconds, n, b.FailThreshold)
			m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "disable", gmsg)
			log.Printf("[laneboard] board=%s account=%d released, probe ok but %s", b.Name, aid, gmsg)
			return
		}
		// 验证成功：确保账号级调度开关打开（sub2api 自动关闭的要重新打开）
		canSched, reason := m.ensureSchedulable(ctx, b, aid)
		if !canSched {
			_ = m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now)
			m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "disable", "释放后验证调用成功但"+reason)
			log.Printf("[laneboard] board=%s account=%d released, probe ok but %s, disabled", b.Name, aid, reason)
			return
		}
		_ = m.db.SetAccountState(ctx, b.ID, aid, LaneStateHealthy, nil)
		log.Printf("[laneboard] board=%s account=%d released+probe ok, healthy", b.Name, aid)
		return
	}
	// 验证调用失败：通过管理 API 写限流条目真正禁用（sub2api 才会停止路由），等定时探测恢复
	entry := map[string]any{
		"reason":              laneReasonPrefix + b.Name,
		"rate_limited_at":     time.Now().UTC().Format(time.RFC3339),
		"rate_limit_reset_at": laneFarFuture,
	}
	if err := m.client.SetModelRateLimit(ctx, aid, b.Model, entry); err != nil {
		log.Printf("[laneboard] board=%s account=%d release-verify disable api: %v", b.Name, aid, err)
	}
	_ = m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now)
	m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "disable", "释放后验证调用失败: "+msg)
	log.Printf("[laneboard] board=%s account=%d released but probe fail (%s), disabled w/ entry", b.Name, aid, msg)
}

func (m *LaneBoardMonitor) disableAccount(ctx context.Context, b *LaneBoard, aid int64, st AccountState) {
	entry := map[string]any{
		"reason":              laneReasonPrefix + b.Name,
		"rate_limited_at":     time.Now().UTC().Format(time.RFC3339),
		"rate_limit_reset_at": laneFarFuture, // 2099：官方 UI 可见（UI 只渲染未来 reset_at）+ 永不过期不被自动清
	}
	// 通过管理 API 写条目（内部自动刷新调度快照）
	if err := m.client.SetModelRateLimit(ctx, aid, b.Model, entry); err != nil {
		log.Printf("[laneboard] board=%s account=%d disable api: %v", b.Name, aid, err)
	}
	now := time.Now()
	_ = m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now)
	m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "disable", fmt.Sprintf("%ds内%d次失败≥阈值%d，关闭%s调度", b.WindowSeconds, st.FailCount, b.FailThreshold, b.Model))
	log.Printf("[laneboard] board=%s account=%d disabled (%d fails/%ds)", b.Name, aid, st.FailCount, b.WindowSeconds)
}

// enableAccount 恢复：通过管理 API 删除自己的 model_rate_limits 条目（自动刷新快照）
func (m *LaneBoardMonitor) enableAccount(ctx context.Context, b *LaneBoard, aid int64, st AccountState) {
	if err := m.client.ClearModelRateLimit(ctx, aid, b.Model); err != nil {
		log.Printf("[laneboard] board=%s account=%d enable api: %v", b.Name, aid, err)
	}
	_ = m.db.SetAccountState(ctx, b.ID, aid, LaneStateHealthy, nil)
	m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "enable", "探测成功，恢复 "+b.Model+" 调度")
	log.Printf("[laneboard] board=%s account=%d enabled", b.Name, aid)
}

// ManualProbe 手动探测（UI 用）
func (m *LaneBoardMonitor) ManualProbe(ctx context.Context, boardID, accountID int64) (bool, string, error) {
	b, err := m.db.GetBoard(ctx, boardID)
	if err != nil {
		return false, "", err
	}
	ok, msg, err := m.client.TestAccountModel(ctx, accountID, b.Model)
	now := time.Now()
	_ = m.db.UpdateAccountStateProbe(ctx, boardID, accountID, ok, msg, now)
	if ok {
		// 恢复前置门槛（用户规则）：CheckErrors 与 ProbeLoop 同时通过
		gatePass, n, gerr := m.checkErrorsGate(ctx, b, accountID)
		if gerr != nil {
			return false, "CheckErrors 检查失败: " + gerr.Error(), gerr
		}
		if !gatePass {
			return false, fmt.Sprintf("探测成功但%d秒窗口内%d次失败≥阈值%d，CheckErrors未通过不恢复", b.WindowSeconds, n, b.FailThreshold), nil
		}
		m.enableAccount(ctx, b, accountID, AccountState{})
		m.reconcileBoard(ctx, b)
	}
	return ok, msg, err
}

var _ = strings.TrimSpace
