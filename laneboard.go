package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
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
// 控制手段（模型级）
//   - 禁用/恢复: 通过共享 PostgreSQL 账号行锁事务更新 model_rate_limits，并写入 scheduler_outbox
//   - 恢复仅删除自己写入的条目（reason 为当前 board 的精确 owner）
//   - model_rate_limits 使用账号映射后的上游模型键；outbox 负责调度快照同步
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
	State        string     `json:"state"` // healthy / disabled / suppressed
	DisabledAt   *time.Time `json:"disabled_at"`
	LastProbeAt  *time.Time `json:"last_probe_at"`
	LastProbeOK  *bool      `json:"last_probe_ok"`
	LastProbeMsg string     `json:"last_probe_msg"`
	FailCount    int        `json:"fail_count"` // 连续探测失败次数
	CheckedAt    *time.Time `json:"checked_at"`
}

const (
	LaneStateHealthy    = "healthy"
	LaneStateDisabled   = "disabled"
	LaneStateSuppressed = "suppressed" // 被更高优先级泳道压制（即使健康也不调度）
	laneReasonPrefix    = "lane_board:"
	laneSuppressPrefix  = "lane_board:suppressed:"
	// Sub2API 只有在 rate_limit_reset_at 是未来时间时才把模型条目视为有效；
	// 使用 2099 保持阻断，直到本调度器通过管理 API 精确清理 owner 条目。
	laneFarFuture = "2099-12-31T23:59:59Z"
)

// ============================ Schema ============================

func (d *DB) ensureLaneBoardSchema(ctx context.Context) error {
	createStatements := []string{
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
	for _, statement := range createStatements {
		if _, err := d.pool.Exec(ctx, statement); err != nil {
			return err
		}
	}

	// Upgrade tables created by older README versions. Those definitions lacked
	// lane IDs, defaults, and state rows; CREATE TABLE IF NOT EXISTS cannot repair
	// an existing table.
	migrationStatements := []string{
		`CREATE SEQUENCE IF NOT EXISTS lane_boards_lanes_id_seq`,
		`ALTER TABLE lane_boards_lanes ADD COLUMN IF NOT EXISTS id BIGINT`,
		`ALTER TABLE lane_boards_lanes ALTER COLUMN id SET DEFAULT nextval('lane_boards_lanes_id_seq')`,
		`UPDATE lane_boards_lanes SET id=nextval('lane_boards_lanes_id_seq') WHERE id IS NULL`,
		`SELECT setval('lane_boards_lanes_id_seq', COALESCE((SELECT MAX(id) FROM lane_boards_lanes), 0) + 1, false)`,
		`ALTER TABLE lane_boards_lanes ALTER COLUMN id SET NOT NULL`,
		`ALTER TABLE lane_boards_lanes ALTER COLUMN position SET DEFAULT 0`,
		`ALTER TABLE lane_boards_lanes ALTER COLUMN name SET DEFAULT ''`,
		`ALTER TABLE lane_boards_lanes ALTER COLUMN account_ids SET DEFAULT '{}'::bigint[]`,
		`UPDATE lane_boards_lanes SET account_ids=ARRAY(
		   SELECT account_id FROM unnest(COALESCE(account_ids, '{}'::bigint[])) AS account_id
		   WHERE account_id > 0
		)`,
		`UPDATE lane_boards SET fail_threshold=3 WHERE fail_threshold IS NULL OR fail_threshold <= 0`,
		`UPDATE lane_boards SET window_seconds=60 WHERE window_seconds IS NULL OR window_seconds < 10`,
		`UPDATE lane_boards SET probe_interval=30 WHERE probe_interval IS NULL OR probe_interval < 10`,
		`CREATE UNIQUE INDEX IF NOT EXISTS lane_boards_lanes_id_key ON lane_boards_lanes(id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS lane_boards_lanes_board_position_key ON lane_boards_lanes(board_id, position)`,
		`ALTER TABLE lane_account_states ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMPTZ`,
		`ALTER TABLE lane_account_states ADD COLUMN IF NOT EXISTS last_probe_at TIMESTAMPTZ`,
		`ALTER TABLE lane_account_states ADD COLUMN IF NOT EXISTS last_probe_ok BOOLEAN`,
		`ALTER TABLE lane_account_states ADD COLUMN IF NOT EXISTS last_probe_msg TEXT`,
		`ALTER TABLE lane_account_states ADD COLUMN IF NOT EXISTS fail_count INT`,
		`ALTER TABLE lane_account_states ADD COLUMN IF NOT EXISTS checked_at TIMESTAMPTZ`,
		`UPDATE lane_account_states SET last_probe_msg='' WHERE last_probe_msg IS NULL`,
		`UPDATE lane_account_states SET fail_count=0 WHERE fail_count IS NULL`,
		`ALTER TABLE lane_account_states ALTER COLUMN last_probe_msg SET DEFAULT ''`,
		`ALTER TABLE lane_account_states ALTER COLUMN last_probe_msg SET NOT NULL`,
		`ALTER TABLE lane_account_states ALTER COLUMN fail_count SET DEFAULT 0`,
		`ALTER TABLE lane_account_states ALTER COLUMN fail_count SET NOT NULL`,
		`INSERT INTO lane_account_states (board_id, account_id)
		 SELECT lanes.board_id, account_ids.account_id
		 FROM lane_boards_lanes AS lanes
		 CROSS JOIN LATERAL unnest(lanes.account_ids) AS account_ids(account_id)
		 WHERE account_ids.account_id IS NOT NULL
		 ON CONFLICT (board_id, account_id) DO NOTHING`,
		`DO $$
		 BEGIN
		   IF EXISTS (SELECT 1 FROM lane_boards GROUP BY name HAVING count(*) > 1) THEN
		     RAISE EXCEPTION 'duplicate lane board names must be resolved before migration';
		   END IF;
		   IF EXISTS (SELECT 1 FROM lane_boards GROUP BY model HAVING count(*) > 1) THEN
		     RAISE EXCEPTION 'duplicate lane board models must be resolved before migration';
		   END IF;
		   IF EXISTS (
		     SELECT lanes.board_id, account_ids.account_id
		     FROM lane_boards_lanes AS lanes
		     CROSS JOIN LATERAL unnest(lanes.account_ids) AS account_ids(account_id)
		     WHERE account_ids.account_id IS NOT NULL AND account_ids.account_id > 0
		     GROUP BY lanes.board_id, account_ids.account_id
		     HAVING count(*) > 1
		   ) THEN
		     RAISE EXCEPTION 'duplicate lane account memberships must be resolved before migration';
		   END IF;
		 END $$`,
		`CREATE UNIQUE INDEX IF NOT EXISTS lane_boards_name_key ON lane_boards(name)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS lane_boards_model_key ON lane_boards(model)`,
	}
	for _, statement := range migrationStatements {
		if _, err := d.pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("migrate lane board schema: %w", err)
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
SELECT id, board_id, position, name,
       COALESCE(ARRAY(
         SELECT account_id FROM unnest(COALESCE(account_ids, '{}'::bigint[])) AS account_id
         WHERE account_id > 0
       ), '{}'::bigint[])
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
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: board %d", ErrBoardNotFound, id)
		}
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

	var previousModel string
	if b.ID != 0 {
		if err := tx.QueryRow(ctx, `SELECT model FROM lane_boards WHERE id=$1 FOR UPDATE`, b.ID).Scan(&previousModel); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: board %d", ErrBoardNotFound, b.ID)
			}
			return err
		}
	}

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
			if _, err = tx.Exec(ctx, `DELETE FROM lane_boards_lanes WHERE board_id=$1`, b.ID); err != nil {
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
	accountIDs := uniqueBoardAccountIDs(b)
	if accountIDs == nil {
		accountIDs = []int64{}
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM lane_account_states
WHERE board_id=$1 AND NOT (account_id = ANY($2))`, b.ID, accountIDs); err != nil {
		return err
	}
	if b.ID != 0 && previousModel != b.Model {
		if _, err := tx.Exec(ctx, `
UPDATE lane_account_states
SET state='healthy', disabled_at=NULL, last_probe_at=NULL, last_probe_ok=NULL,
    last_probe_msg='', fail_count=0, checked_at=NULL
WHERE board_id=$1`, b.ID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (d *DB) DeleteBoard(ctx context.Context, id int64) error {
	tag, err := d.pool.Exec(ctx, `DELETE FROM lane_boards WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: board %d", ErrBoardNotFound, id)
	}
	return nil
}

func (d *DB) ValidateBoardAccounts(ctx context.Context, model string, accountIDs []int64) error {
	if len(accountIDs) == 0 {
		return fmt.Errorf("%w: at least one account is required", ErrInvalidBoard)
	}
	rows, err := d.pool.Query(ctx, `
SELECT id, platform, type,
       credentials->'model_mapping',
       COALESCE(credentials->>'oauth_type', ''),
       COALESCE(credentials->>'project_id', ''),
       COALESCE((extra->>'openai_passthrough') = 'true', false),
       COALESCE((extra->>'openai_oauth_passthrough') = 'true', false)
FROM accounts
WHERE deleted_at IS NULL
  AND id = ANY($1)`, accountIDs)
	if err != nil {
		return err
	}
	defer rows.Close()
	found := make(map[int64]struct{}, len(accountIDs))
	for rows.Next() {
		var id int64
		var platform, accountType, oauthType, projectID string
		var passThrough, oauthPassThrough bool
		var mappingJSON []byte
		if err := rows.Scan(&id, &platform, &accountType, &mappingJSON, &oauthType, &projectID, &passThrough, &oauthPassThrough); err != nil {
			return err
		}
		credentials := map[string]any{
			"oauth_type":               oauthType,
			"project_id":               projectID,
			"openai_passthrough":       passThrough,
			"openai_oauth_passthrough": oauthPassThrough,
		}
		if len(mappingJSON) > 0 && string(mappingJSON) != "null" {
			var mapping map[string]any
			if err := json.Unmarshal(mappingJSON, &mapping); err != nil {
				return fmt.Errorf("decode account %d model mapping: %w", id, err)
			}
			credentials["model_mapping"] = mapping
		}
		if modelMappingSupportsRequestedModelForAccount(platform, accountType, credentials, model) {
			found[id] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range accountIDs {
		if _, ok := found[id]; !ok {
			return fmt.Errorf("%w: account %d does not exist or is not mapped to model %q", ErrInvalidBoard, id, model)
		}
	}
	return nil
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

	configMu   sync.Mutex
	scheduleMu sync.RWMutex
	locksMu    sync.Mutex
	boardLocks map[int64]*sync.Mutex

	// 上次打开账号调度开关的时间（防抖：sub2api 自动关闭=上游真实失败信号，短时间不重开）
	schedOpenAt map[int64]time.Time
	schedMu     sync.Mutex
	loopsWG     sync.WaitGroup
}

func NewLaneBoardMonitor(db *DB, client *Sub2APIClient) *LaneBoardMonitor {
	return &LaneBoardMonitor{
		db:          db,
		client:      client,
		boardLocks:  make(map[int64]*sync.Mutex),
		schedOpenAt: make(map[int64]time.Time),
	}
}

func (m *LaneBoardMonitor) lockForBoard(boardID int64) *sync.Mutex {
	m.locksMu.Lock()
	defer m.locksMu.Unlock()
	lock := m.boardLocks[boardID]
	if lock == nil {
		lock = &sync.Mutex{}
		m.boardLocks[boardID] = lock
	}
	return lock
}

func (m *LaneBoardMonitor) executeBoardCleanup(ctx context.Context, ops []BoardCleanupOp) error {
	seen := make(map[int64]struct{}, len(ops))
	for _, op := range ops {
		if _, exists := seen[op.AccountID]; exists {
			continue
		}
		seen[op.AccountID] = struct{}{}
		cleared, err := m.client.ClearAllOwnedModelRateLimits(ctx, op.AccountID, op.BoardName)
		if err != nil {
			if errors.Is(err, ErrSub2APIAccountNotFound) {
				log.Printf("[laneboard] board=%s account=%d no longer exists in Sub2API; skip cleanup", op.BoardName, op.AccountID)
				continue
			}
			return fmt.Errorf("clear board %q account %d model limits: %w", op.BoardName, op.AccountID, err)
		}
		if cleared > 0 {
			m.db.LogLaneEvent(ctx, op.BoardName, op.Model, op.AccountID, "release", "配置变更，清理泳道图拥有的模型限制")
		}
	}
	return nil
}

// SaveBoard serializes configuration changes with monitor cycles. Remote
// cleanup is confirmed before the previous configuration is replaced.
func (m *LaneBoardMonitor) SaveBoard(ctx context.Context, board *LaneBoard) error {
	m.configMu.Lock()
	defer m.configMu.Unlock()
	m.scheduleMu.Lock()
	defer m.scheduleMu.Unlock()
	if board == nil {
		return fmt.Errorf("board is required")
	}
	isNew := board.ID == 0
	if !isNew {
		lock := m.lockForBoard(board.ID)
		lock.Lock()
		defer lock.Unlock()
	}

	existing, err := m.db.ListBoards(ctx)
	if err != nil {
		return err
	}
	if err := validateBoardDefinition(board, existing); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBoard, err)
	}
	if err := m.db.ValidateBoardAccounts(ctx, board.Model, uniqueBoardAccountIDs(board)); err != nil {
		return err
	}

	var previous *LaneBoard
	if board.ID != 0 {
		previous, err = m.db.GetBoard(ctx, board.ID)
		if err != nil {
			return err
		}
	}
	if err := m.executeBoardCleanup(ctx, planBoardCleanup(previous, board)); err != nil {
		if previous != nil && previous.Enabled {
			if _, reconcileErr := m.reconcileBoard(ctx, previous); reconcileErr != nil {
				log.Printf("[laneboard] board=%s restore after cleanup failure: %v", previous.Name, reconcileErr)
			}
		}
		return err
	}
	if err := m.db.SaveBoard(ctx, board); err != nil {
		if previous != nil && previous.Enabled {
			if _, reconcileErr := m.reconcileBoard(ctx, previous); reconcileErr != nil {
				log.Printf("[laneboard] board=%s restore after cleanup failure: %v", previous.Name, reconcileErr)
			}
		}
		return err
	}
	if isNew {
		lock := m.lockForBoard(board.ID)
		lock.Lock()
		defer lock.Unlock()
	}
	if board.Enabled {
		if _, err := m.reconcileBoard(ctx, board); err != nil {
			return fmt.Errorf("board saved but initial reconcile failed: %w", err)
		}
	}
	return nil
}

// DeleteBoard removes every board-owned model limit before deleting the only
// durable description of that ownership.
func (m *LaneBoardMonitor) DeleteBoard(ctx context.Context, boardID int64) error {
	m.configMu.Lock()
	defer m.configMu.Unlock()
	m.scheduleMu.Lock()
	defer m.scheduleMu.Unlock()
	lock := m.lockForBoard(boardID)
	lock.Lock()
	defer lock.Unlock()

	board, err := m.db.GetBoard(ctx, boardID)
	if err != nil {
		return err
	}
	if err := m.executeBoardCleanup(ctx, planBoardCleanup(board, nil)); err != nil {
		if board.Enabled {
			if _, reconcileErr := m.reconcileBoard(ctx, board); reconcileErr != nil {
				log.Printf("[laneboard] board=%s restore after cleanup failure: %v", board.Name, reconcileErr)
			}
		}
		return err
	}
	if err := m.db.DeleteBoard(ctx, boardID); err != nil {
		if board.Enabled {
			if _, reconcileErr := m.reconcileBoard(ctx, board); reconcileErr != nil {
				log.Printf("[laneboard] board=%s restore after cleanup failure: %v", board.Name, reconcileErr)
			}
		}
		return err
	}
	return nil
}

// Start 启动两个循环：
//   - 错误统计：5s 周期，统计最近 1 分钟（WindowSeconds）窗口内失败数，超阈值 → 限流
//   - 探测：30s 周期（默认 probe_interval）
func (m *LaneBoardMonitor) Start(ctx context.Context) {
	log.Printf("[laneboard] monitor started (error_check=5s, probe=30s)")
	m.loopsWG.Add(2)
	go func() {
		defer m.loopsWG.Done()
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
		defer m.loopsWG.Done()
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

func (m *LaneBoardMonitor) Wait() {
	m.loopsWG.Wait()
}

func (m *LaneBoardMonitor) runEnabledBoards(ctx context.Context, boards []LaneBoard, run func(context.Context, *LaneBoard)) {
	m.scheduleMu.RLock()
	defer m.scheduleMu.RUnlock()
	const maxParallelBoards = 4
	semaphore := make(chan struct{}, maxParallelBoards)
	var wait sync.WaitGroup
	for i := range boards {
		if !boards[i].Enabled {
			continue
		}
		board := boards[i]
		wait.Add(1)
		go func() {
			defer wait.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				return
			}
			lock := m.lockForBoard(board.ID)
			lock.Lock()
			defer lock.Unlock()
			current, err := m.db.GetBoard(ctx, board.ID)
			if err != nil {
				if errors.Is(err, ErrBoardNotFound) {
					return
				}
				log.Printf("[laneboard] reload board=%d: %v", board.ID, err)
				return
			}
			if !current.Enabled {
				return
			}
			run(ctx, current)
		}()
	}
	wait.Wait()
}

// CheckErrors 每 5s 统计每个图×账号最近 1 分钟窗口内失败数，超过阈值 → 限流
// 只统计当前 active 泳道及更高优先级（position <= activeIdx）的账号；
// 更低泳道（备用/压制态）不接流量，不统计。
func (m *LaneBoardMonitor) CheckErrors(ctx context.Context) {
	boards, err := m.db.ListBoards(ctx)
	if err != nil {
		log.Printf("[laneboard] list boards: %v", err)
		return
	}
	m.runEnabledBoards(ctx, boards, func(ctx context.Context, board *LaneBoard) {
		m.checkBoardErrors(ctx, board)
	})
}

func (m *LaneBoardMonitor) checkBoardErrors(ctx context.Context, b *LaneBoard) {
	activeIdx, err := m.reconcileBoard(ctx, b)
	if err != nil {
		log.Printf("[laneboard] board=%s reconcile before failure check: %v", b.Name, err)
		return
	}
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
				if err := m.disableAccount(ctx, b, aid, st); err != nil {
					log.Printf("[laneboard] board=%s account=%d disable: %v", b.Name, aid, err)
				}
			} else if err := m.db.UpdateAccountStateCheck(ctx, b.ID, aid, st.FailCount, now); err != nil {
				log.Printf("[laneboard] board=%s account=%d update check: %v", b.Name, aid, err)
			}
		}
	}
}

// externallyDisable 因 sub2api 外部抑制而禁用（不写泳道图条目——原生条目已在挡，避免覆盖）
func (m *LaneBoardMonitor) externallyDisable(ctx context.Context, b *LaneBoard, aid int64, reason string) error {
	now := time.Now()
	if err := m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now); err != nil {
		return err
	}
	m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "disable", "sub2api 外部抑制: "+reason+"，标记禁用等待恢复")
	log.Printf("[laneboard] board=%s account=%d externally blocked (%s) -> disabled", b.Name, aid, reason)
	return nil
}

// reconcileBoard 严格分层：找到第一个存在 healthy 账号的泳道（active），
// 压制更低泳道，并在验证通过之前保留候选账号原有的阻断条目。
func (m *LaneBoardMonitor) reconcileBoard(ctx context.Context, b *LaneBoard) (int, error) {
	states, err := m.db.GetAccountStates(ctx, b.ID)
	if err != nil {
		return -1, err
	}
	now := time.Now()
	extBlocks, err := m.db.GetExternalBlocks(ctx, boardAccountIDs(b), b.Model, b.Name)
	if err != nil {
		return -1, fmt.Errorf("query external blocks: %w", err)
	}

	// Missing or unknown local state is fail-closed. It must never be treated as
	// healthy, otherwise Sub2API may continue routing around the state table.
	for _, aid := range uniqueBoardAccountIDs(b) {
		st, ok := states[aid]
		if ok && validLaneAccountState(st.State) {
			continue
		}
		if err := m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now); err != nil {
			return -1, fmt.Errorf("initialize account %d state: %w", aid, err)
		}
		states[aid] = AccountState{BoardID: b.ID, AccountID: aid, State: LaneStateDisabled, DisabledAt: &now}
	}

	// Sub2API 原生阻塞优先；只有状态落库成功后才改变本地视图。
	for i := range b.Lanes {
		for _, aid := range b.Lanes[i].AccountIDs {
			st, ok := states[aid]
			if !ok || st.State != LaneStateHealthy {
				continue
			}
			block := extBlocks[aid]
			if block.OwnedModelLimit {
				if err := m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now); err != nil {
					return -1, fmt.Errorf("mark account %d locally blocked: %w", aid, err)
				}
				st.State = LaneStateDisabled
				st.DisabledAt = &now
				states[aid] = st
				log.Printf("[laneboard] board=%s account=%d local healthy but owned remote limit remains; fail closed", b.Name, aid)
				continue
			}
			if !block.blocked(now) {
				continue
			}
			if err := m.externallyDisable(ctx, b, aid, block.blockedReason(now)); err != nil {
				return -1, fmt.Errorf("mark account %d externally disabled: %w", aid, err)
			}
			st.State = LaneStateDisabled
			states[aid] = st
		}
	}

	// A disabled local state must also have a real remote blocker once native
	// cooldowns expire. This also retries earlier failed control writes.
	for i := range b.Lanes {
		for _, aid := range b.Lanes[i].AccountIDs {
			st, ok := states[aid]
			if !ok || st.State != LaneStateDisabled || extBlocks[aid].blocked(now) {
				continue
			}
			if err := m.ensureDisabledAccount(ctx, b, aid); err != nil {
				if errors.Is(err, ErrForeignModelRateLimit) {
					continue
				}
				return -1, fmt.Errorf("ensure account %d disabled: %w", aid, err)
			}
		}
	}

	activeIdx := -1
	for i := range b.Lanes {
		for _, aid := range b.Lanes[i].AccountIDs {
			if st, ok := states[aid]; ok && st.State == LaneStateHealthy {
				activeIdx = i
				break
			}
		}
		if activeIdx >= 0 {
			break
		}
	}

	// No healthy lane: recover only the first candidate lane that succeeds.
	// Lower lanes must stay suppressed once a higher lane comes back.
	if activeIdx == -1 {
		for i := range b.Lanes {
			for _, aid := range b.Lanes[i].AccountIDs {
				if st, ok := states[aid]; ok && st.State == LaneStateSuppressed && m.releaseVerify(ctx, b, aid, st, now) {
					return i, nil
				}
			}
		}
		return -1, nil
	}

	for i := activeIdx + 1; i < len(b.Lanes); i++ {
		for _, aid := range b.Lanes[i].AccountIDs {
			st, ok := states[aid]
			if !ok || st.State == LaneStateDisabled {
				continue
			}
			if st.State == LaneStateHealthy {
				if err := m.suppressAccount(ctx, b, aid); err != nil {
					log.Printf("[laneboard] board=%s account=%d suppress: %v", b.Name, aid, err)
					continue
				}
				if err := m.db.SetAccountState(ctx, b.ID, aid, LaneStateSuppressed, &now); err != nil {
					return -1, fmt.Errorf("persist account %d suppression: %w", aid, err)
				}
				continue
			}
			if err := m.ensureSuppressedAccount(ctx, b, aid); err != nil {
				log.Printf("[laneboard] board=%s account=%d ensure suppression: %v", b.Name, aid, err)
			}
		}
	}

	// Recover stale suppressed accounts in the active lane or higher. If a
	// higher lane recovers, do not release any lower lane from this snapshot.
	for i := 0; i <= activeIdx; i++ {
		for _, aid := range b.Lanes[i].AccountIDs {
			if st, ok := states[aid]; ok && st.State == LaneStateSuppressed {
				if m.releaseVerify(ctx, b, aid, st, now) && i < activeIdx {
					return i, nil
				}
			}
		}
	}
	return activeIdx, nil
}

func boardLimitEntry(reason string) map[string]any {
	return map[string]any{
		"reason":              reason,
		"rate_limited_at":     time.Now().UTC().Format(time.RFC3339),
		"rate_limit_reset_at": laneFarFuture,
	}
}

func (m *LaneBoardMonitor) ensureDisabledAccount(ctx context.Context, b *LaneBoard, aid int64) error {
	return m.client.SetOwnedModelRateLimit(ctx, aid, b.Model, b.Name, boardLimitEntry(laneReasonPrefix+b.Name))
}

func (m *LaneBoardMonitor) ensureSuppressedAccount(ctx context.Context, b *LaneBoard, aid int64) error {
	return m.client.SetOwnedModelRateLimit(ctx, aid, b.Model, b.Name, boardLimitEntry(laneSuppressPrefix+b.Name))
}

// suppressAccount writes the remote blocker first; callers persist local state
// only after this method succeeds.
func (m *LaneBoardMonitor) suppressAccount(ctx context.Context, b *LaneBoard, aid int64) error {
	if err := m.ensureSuppressedAccount(ctx, b, aid); err != nil {
		return err
	}
	m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "suppress", "高泳道可用，暂停 "+b.Model+" 调度")
	log.Printf("[laneboard] board=%s account=%d suppressed", b.Name, aid)
	return nil
}

// ProbeLoop 探测所有 disabled 账号；泳道全挂 → 下一泳道立即探测
func (m *LaneBoardMonitor) ProbeLoop(ctx context.Context) {
	boards, err := m.db.ListBoards(ctx)
	if err != nil {
		log.Printf("[laneboard] list boards: %v", err)
		return
	}
	m.runEnabledBoards(ctx, boards, func(ctx context.Context, board *LaneBoard) {
		m.probeBoard(ctx, board)
	})
}

func (m *LaneBoardMonitor) probeBoard(ctx context.Context, b *LaneBoard) {
	activeIdx, err := m.reconcileBoard(ctx, b)
	if err != nil {
		log.Printf("[laneboard] board=%s reconcile before probe: %v", b.Name, err)
		return
	}
	states, err := m.db.GetAccountStates(ctx, b.ID)
	if err != nil {
		log.Printf("[laneboard] board=%s states: %v", b.Name, err)
		return
	}
	interval := time.Duration(b.ProbeInterval) * time.Second
	now := time.Now()
	// 外部阻塞读取失败时不探测、不改变任何状态。
	extBlocks, err := m.db.GetExternalBlocks(ctx, boardAccountIDs(b), b.Model, b.Name)
	if err != nil {
		log.Printf("[laneboard] board=%s external blocks before probe: %v", b.Name, err)
		return
	}
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
			if !externalBlockAllowsProbe(eb, now) {
				continue
			}
			if st.LastProbeAt != nil && now.Sub(*st.LastProbeAt) < interval {
				continue
			}
			if m.probeAccount(ctx, b, aid, st, now) {
				if _, err := m.reconcileBoard(ctx, b); err != nil {
					log.Printf("[laneboard] board=%s reconcile after recovery: %v", b.Name, err)
				}
				return
			}
		}
	}
}

// boardAccountIDs 收集并去重泳道图所有账号 ID
func boardAccountIDs(b *LaneBoard) []int64 {
	return uniqueBoardAccountIDs(b)
}

// checkErrorsGate 恢复前置门槛（用户规则 2026-08-17）：
// 探测恢复必须 CheckErrors 与 ProbeLoop 同时通过——
//
//	CheckErrors 通过 = 账号在 WindowSeconds 窗口内失败数 < fail_threshold
//	ProbeLoop 通过  = /test 探测调用成功
//
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

// probeAccount keeps the remote blocker in place until both the traffic gate and
// the real probe pass. TestAccountModel restores Sub2API's automatic cleanup.
func (m *LaneBoardMonitor) updateProbeState(ctx context.Context, boardID, accountID int64, ok bool, msg string, at time.Time) {
	if err := m.db.UpdateAccountStateProbe(ctx, boardID, accountID, ok, msg, at); err != nil {
		log.Printf("[laneboard] board=%d account=%d update probe state: %v", boardID, accountID, err)
	}
}

func (m *LaneBoardMonitor) probeAccount(ctx context.Context, b *LaneBoard, aid int64, st AccountState, now time.Time) bool {
	gatePass, n, gateErr := m.checkErrorsGate(ctx, b, aid)
	if gateErr != nil {
		msg := "CheckErrors 检查失败，暂不探测: " + gateErr.Error()
		m.updateProbeState(ctx, b.ID, aid, false, msg, now)
		log.Printf("[laneboard] board=%s account=%d %s", b.Name, aid, msg)
		return false
	}
	if !gatePass {
		msg := fmt.Sprintf("%d秒窗口内%d次失败≥阈值%d，CheckErrors未通过不探测", b.WindowSeconds, n, b.FailThreshold)
		m.updateProbeState(ctx, b.ID, aid, false, msg, now)
		return false
	}

	ok, msg, err := m.client.TestAccountModel(ctx, aid, b.Model)
	if err != nil {
		msg = err.Error()
		ok = false
	}
	if err := m.db.UpdateAccountStateProbe(ctx, b.ID, aid, ok, msg, now); err != nil {
		log.Printf("[laneboard] board=%s account=%d update probe: %v", b.Name, aid, err)
	}
	if !ok {
		newFail, failErr := m.db.IncProbeFail(ctx, b.ID, aid, now)
		if failErr != nil {
			log.Printf("[laneboard] board=%s account=%d increment probe failure: %v", b.Name, aid, failErr)
			return false
		}
		st.FailCount = newFail
		if err := m.disableAccount(ctx, b, aid, st); err != nil {
			log.Printf("[laneboard] board=%s account=%d keep disabled after probe failure: %v", b.Name, aid, err)
			return false
		}
		log.Printf("[laneboard] board=%s account=%d probe failed: %s", b.Name, aid, msg)
		return false
	}

	canSched, reason := m.ensureSchedulable(ctx, b, aid)
	if !canSched {
		m.updateProbeState(ctx, b.ID, aid, true, "探测成功但"+reason, now)
		log.Printf("[laneboard] board=%s account=%d probe ok but %s, stay disabled", b.Name, aid, reason)
		return false
	}
	if err := m.enableAccount(ctx, b, aid); err != nil {
		log.Printf("[laneboard] board=%s account=%d enable after probe: %v", b.Name, aid, err)
		return false
	}
	if err := m.db.ResetProbeFail(ctx, b.ID, aid, now); err != nil {
		log.Printf("[laneboard] board=%s account=%d reset probe failures: %v", b.Name, aid, err)
	}
	log.Printf("[laneboard] board=%s account=%d recovered", b.Name, aid)
	return true
}

// disableAccount 禁用：写 model_rate_limits（无活跃自动条目时）+ 清缓存
// ensureSchedulable 验证调用成功后，确保账号级调度开关打开
// 泳道图判定可用（探测通过）→ 账号必须真正可调度；sub2api 自动关闭的开关（连续失败等）
// 在泳道图验证通过后重新打开。仅当阻塞原因纯粹是账号开关关闭时打开；
// 若有原生冷却/临时禁调度/状态异常则不动（那些机制在管，不该强制覆盖）。
// 返回 (最终是否可调度, 阻塞原因)
func (m *LaneBoardMonitor) ensureSchedulable(ctx context.Context, b *LaneBoard, aid int64) (bool, string) {
	blocks, err := m.db.GetExternalBlocks(ctx, []int64{aid}, b.Model, b.Name)
	if err != nil {
		return false, "查询外部抑制失败: " + err.Error()
	}
	eb := blocks[aid]
	if !eb.blocked(time.Now()) {
		return true, ""
	}
	// 仅因账号开关关闭（其余维度都正常）→ 重新打开（带防抖）
	if !eb.Schedulable && eb.Status == "active" && !eb.Expired && !eb.QuotaExceeded && eb.NativeCoolUntil == nil && eb.RateLimitUntil == nil && eb.OverloadUntil == nil && eb.TempUnschedUntil == nil {
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
		blocks2, err2 := m.db.GetExternalBlocks(ctx, []int64{aid}, b.Model, b.Name)
		if err2 == nil && !blocks2[aid].blocked(time.Now()) {
			return true, ""
		}
		return false, "打开后仍被抑制"
	}
	return false, eb.blockedReason(time.Now())
}

// releaseVerify validates a suppressed candidate without removing its blocker.
// The owned entry is cleared only after the traffic gate, probe, and external
// schedulability checks all pass.
func (m *LaneBoardMonitor) releaseVerify(ctx context.Context, b *LaneBoard, aid int64, st AccountState, now time.Time) bool {
	interval := time.Duration(b.ProbeInterval) * time.Second
	if st.LastProbeAt != nil && now.Sub(*st.LastProbeAt) < interval {
		return false
	}
	blocks, err := m.db.GetExternalBlocks(ctx, []int64{aid}, b.Model, b.Name)
	if err != nil {
		msg := "释放前外部状态检查失败，保持压制: " + err.Error()
		m.updateProbeState(ctx, b.ID, aid, false, msg, now)
		return false
	}
	if block := blocks[aid]; !externalBlockAllowsProbe(block, now) {
		msg := "外部阻塞仍生效，保持压制: " + block.blockedReason(now)
		m.updateProbeState(ctx, b.ID, aid, false, msg, now)
		return false
	}

	gatePass, n, gateErr := m.checkErrorsGate(ctx, b, aid)
	if gateErr != nil {
		msg := "释放前 CheckErrors 检查失败，保持压制: " + gateErr.Error()
		m.updateProbeState(ctx, b.ID, aid, false, msg, now)
		log.Printf("[laneboard] board=%s account=%d %s", b.Name, aid, msg)
		return false
	}
	if !gatePass {
		msg := fmt.Sprintf("释放前%d秒窗口内%d次失败≥阈值%d，保持压制", b.WindowSeconds, n, b.FailThreshold)
		m.updateProbeState(ctx, b.ID, aid, false, msg, now)
		return false
	}

	ok, msg, err := m.client.TestAccountModel(ctx, aid, b.Model)
	if err != nil {
		msg = err.Error()
		ok = false
	}
	if err := m.db.UpdateAccountStateProbe(ctx, b.ID, aid, ok, msg, now); err != nil {
		log.Printf("[laneboard] board=%s account=%d update release probe: %v", b.Name, aid, err)
	}
	if !ok {
		if newFail, failErr := m.db.IncProbeFail(ctx, b.ID, aid, now); failErr == nil {
			st.FailCount = newFail
		} else {
			log.Printf("[laneboard] board=%s account=%d increment release probe failure: %v", b.Name, aid, failErr)
		}
		if err := m.disableAccount(ctx, b, aid, st); err != nil {
			log.Printf("[laneboard] board=%s account=%d disable failed candidate: %v", b.Name, aid, err)
			return false
		}
		m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "disable", "候选泳道验证失败: "+msg)
		return false
	}

	canSched, reason := m.ensureSchedulable(ctx, b, aid)
	if !canSched {
		if err := m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now); err != nil {
			log.Printf("[laneboard] board=%s account=%d persist external disable: %v", b.Name, aid, err)
			return false
		}
		m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "disable", "候选探测成功但"+reason)
		return false
	}

	cleared, err := m.client.ClearAllOwnedModelRateLimits(ctx, aid, b.Name)
	if err != nil {
		log.Printf("[laneboard] board=%s account=%d clear verified suppression: %v", b.Name, aid, err)
		return false
	}
	if cleared == 0 {
		log.Printf("[laneboard] board=%s account=%d verified release had no owned limit", b.Name, aid)
	}
	if err := m.confirmAccountSchedulable(ctx, b, aid); err != nil {
		if reblockErr := m.ensureSuppressedAccount(ctx, b, aid); reblockErr != nil && !errors.Is(reblockErr, ErrForeignModelRateLimit) {
			log.Printf("[laneboard] board=%s account=%d reblock after confirmation failure: %v", b.Name, aid, reblockErr)
		}
		if stateErr := m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now); stateErr != nil {
			log.Printf("[laneboard] board=%s account=%d persist blocked recovery: %v", b.Name, aid, stateErr)
		}
		log.Printf("[laneboard] board=%s account=%d verified release remains blocked: %v", b.Name, aid, err)
		return false
	}
	if err := m.db.SetAccountState(ctx, b.ID, aid, LaneStateHealthy, nil); err != nil {
		if reblockErr := m.ensureSuppressedAccount(ctx, b, aid); reblockErr != nil && !errors.Is(reblockErr, ErrForeignModelRateLimit) {
			log.Printf("[laneboard] board=%s account=%d reblock after state persistence failure: %v", b.Name, aid, reblockErr)
		}
		log.Printf("[laneboard] board=%s account=%d persist verified recovery: %v", b.Name, aid, err)
		return false
	}
	if err := m.db.ResetProbeFail(ctx, b.ID, aid, now); err != nil {
		log.Printf("[laneboard] board=%s account=%d reset probe failures: %v", b.Name, aid, err)
	}
	m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "release", "验证通过，恢复 "+b.Model+" 调度")
	log.Printf("[laneboard] board=%s account=%d verified and released", b.Name, aid)
	return true
}

func (m *LaneBoardMonitor) disableAccount(ctx context.Context, b *LaneBoard, aid int64, st AccountState) error {
	if err := m.ensureDisabledAccount(ctx, b, aid); err != nil {
		return err
	}
	now := time.Now()
	if err := m.db.SetAccountState(ctx, b.ID, aid, LaneStateDisabled, &now); err != nil {
		return err
	}
	m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "disable", fmt.Sprintf("%ds内%d次失败≥阈值%d，关闭%s调度", b.WindowSeconds, st.FailCount, b.FailThreshold, b.Model))
	log.Printf("[laneboard] board=%s account=%d disabled (%d fails/%ds)", b.Name, aid, st.FailCount, b.WindowSeconds)
	return nil
}

func (m *LaneBoardMonitor) confirmAccountSchedulable(ctx context.Context, b *LaneBoard, aid int64) error {
	blocks, err := m.db.GetExternalBlocks(ctx, []int64{aid}, b.Model, b.Name)
	if err != nil {
		return err
	}
	block, ok := blocks[aid]
	if !ok {
		return fmt.Errorf("account %d was not found", aid)
	}
	if block.OwnedModelLimit {
		return fmt.Errorf("account still has a board-owned model limit")
	}
	if block.blocked(time.Now()) {
		return fmt.Errorf("account remains externally blocked: %s", block.blockedReason(time.Now()))
	}
	return nil
}

func (m *LaneBoardMonitor) enableAccount(ctx context.Context, b *LaneBoard, aid int64) error {
	if _, err := m.client.ClearAllOwnedModelRateLimits(ctx, aid, b.Name); err != nil {
		return err
	}
	if err := m.confirmAccountSchedulable(ctx, b, aid); err != nil {
		if reblockErr := m.ensureDisabledAccount(ctx, b, aid); reblockErr != nil && !errors.Is(reblockErr, ErrForeignModelRateLimit) {
			log.Printf("[laneboard] board=%s account=%d reblock after enable confirmation failure: %v", b.Name, aid, reblockErr)
		}
		return err
	}
	if err := m.db.SetAccountState(ctx, b.ID, aid, LaneStateHealthy, nil); err != nil {
		if reblockErr := m.ensureDisabledAccount(ctx, b, aid); reblockErr != nil && !errors.Is(reblockErr, ErrForeignModelRateLimit) {
			log.Printf("[laneboard] board=%s account=%d reblock after enable state failure: %v", b.Name, aid, reblockErr)
		}
		return err
	}
	m.db.LogLaneEvent(ctx, b.Name, b.Model, aid, "enable", "探测成功，恢复 "+b.Model+" 调度")
	log.Printf("[laneboard] board=%s account=%d enabled", b.Name, aid)
	return nil
}

// ManualProbe serializes with monitor cycles and validates board ownership.
func (m *LaneBoardMonitor) ManualProbe(ctx context.Context, boardID, accountID int64) (bool, string, error) {
	lock := m.lockForBoard(boardID)
	lock.Lock()
	defer lock.Unlock()

	b, err := m.db.GetBoard(ctx, boardID)
	if err != nil {
		return false, "", err
	}
	if !b.Enabled {
		return false, "", fmt.Errorf("%w: board %q is disabled", ErrInvalidBoard, b.Name)
	}
	belongs := false
	for _, id := range boardAccountIDs(b) {
		if id == accountID {
			belongs = true
			break
		}
	}
	if !belongs {
		return false, "", fmt.Errorf("%w: account %d does not belong to board %q", ErrInvalidBoard, accountID, b.Name)
	}

	now := time.Now()
	blocks, err := m.db.GetExternalBlocks(ctx, []int64{accountID}, b.Model, b.Name)
	if err != nil {
		return false, "外部状态检查失败: " + err.Error(), err
	}
	if block := blocks[accountID]; !externalBlockAllowsProbe(block, now) {
		return false, "外部阻塞仍生效: " + block.blockedReason(now), nil
	}

	gatePass, n, err := m.checkErrorsGate(ctx, b, accountID)
	if err != nil {
		return false, "CheckErrors 检查失败: " + err.Error(), err
	}
	if !gatePass {
		msg := fmt.Sprintf("%d秒窗口内%d次失败≥阈值%d，CheckErrors未通过不探测", b.WindowSeconds, n, b.FailThreshold)
		m.updateProbeState(ctx, boardID, accountID, false, msg, now)
		return false, msg, nil
	}

	ok, msg, err := m.client.TestAccountModel(ctx, accountID, b.Model)
	if err != nil {
		msg = err.Error()
		ok = false
	}
	if updateErr := m.db.UpdateAccountStateProbe(ctx, boardID, accountID, ok, msg, now); updateErr != nil {
		return false, msg, updateErr
	}
	if !ok {
		return false, msg, err
	}
	canSched, reason := m.ensureSchedulable(ctx, b, accountID)
	if !canSched {
		return false, "探测成功但" + reason, nil
	}
	if err := m.enableAccount(ctx, b, accountID); err != nil {
		return false, "探测成功但恢复调度失败: " + err.Error(), err
	}
	if _, err := m.reconcileBoard(ctx, b); err != nil {
		return false, "探测成功但泳道重排失败: " + err.Error(), err
	}
	return true, msg, nil
}
