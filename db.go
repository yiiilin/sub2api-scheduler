package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Account 账号（只读安全字段，绝不包含 credentials）
type Account struct {
	ID                int64    `json:"id"`
	Name              string   `json:"name"`
	Platform          string   `json:"platform"`
	Priority          int      `json:"priority"`
	Schedulable       bool     `json:"schedulable"`
	Status            string   `json:"status"`
	TempUnschedulable *string  `json:"temp_unschedulable_until,omitempty"`
	RateLimited       *string  `json:"rate_limit_reset_at,omitempty"`
	OverloadUntil     *string  `json:"overload_until,omitempty"`
	ErrorMsg          *string  `json:"error_message,omitempty"`
	Notes             *string  `json:"notes,omitempty"`
	RateMultiplier    *float64 `json:"rate_multiplier"`
	LoadFactor        *float64 `json:"load_factor"`
}

// SwitchHistory 切换历史记录（本地表）
type SwitchHistory struct {
	ID          int64     `json:"id"`
	RuleName    string    `json:"rule_name"`
	Action      string    `json:"action"`
	AccountID   int64     `json:"account_id"`
	AccountName string    `json:"account_name"`
	OldPriority int       `json:"old_priority"`
	NewPriority int       `json:"new_priority"`
	SchedFrom   *bool     `json:"sched_from"` // 调度状态变化（新语义）
	SchedTo     *bool     `json:"sched_to"`
	TriggeredBy string    `json:"triggered_by"` // cron / manual / failover
	Status      string    `json:"status"`       // success / error
	Message     string    `json:"message"`
	CreatedAt   time.Time `json:"created_at"`
}

const schedulerAdvisoryLockName = "sub2api-scheduler:lane-monitor"

type DB struct {
	pool     *pgxpool.Pool
	lockConn *pgxpool.Conn
}

func NewDB(ctx context.Context, dsn string) (*DB, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if poolConfig.MaxConns < 2 {
		poolConfig.MaxConns = 2
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("acquire scheduler lock connection: %w", err)
	}
	var acquired bool
	if err := lockConn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, schedulerAdvisoryLockName).Scan(&acquired); err != nil {
		lockConn.Release()
		pool.Close()
		return nil, fmt.Errorf("acquire scheduler advisory lock: %w", err)
	}
	if !acquired {
		lockConn.Release()
		pool.Close()
		return nil, fmt.Errorf("another scheduler instance already uses database")
	}

	db := &DB{pool: pool, lockConn: lockConn}
	if err := db.initSchema(ctx); err != nil {
		db.Close()
		return nil, err
	}
	log.Printf("[db] connected, scheduler lock acquired, schema ready")
	return db, nil
}

func (d *DB) Close() {
	if d == nil {
		return
	}
	if d.lockConn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = d.lockConn.Exec(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, schedulerAdvisoryLockName)
		cancel()
		d.lockConn.Release()
		d.lockConn = nil
	}
	if d.pool != nil {
		d.pool.Close()
	}
}

// initSchema 建本地历史表+规则表（不影响 sub2api 自身表）
func (d *DB) initSchema(ctx context.Context) error {
	if _, err := d.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS switch_history (
    id           BIGSERIAL PRIMARY KEY,
    rule_name    TEXT NOT NULL,
    action       TEXT NOT NULL,
    account_id   BIGINT NOT NULL,
    account_name TEXT NOT NULL DEFAULT '',
    old_priority INT NOT NULL DEFAULT 0,
    new_priority INT NOT NULL DEFAULT 0,
    sched_from   BOOLEAN,
    sched_to     BOOLEAN,
    triggered_by TEXT NOT NULL,
    status       TEXT NOT NULL,
    message      TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
)`); err != nil {
		return err
	}
	// 兼容旧表：补列
	if _, err := d.pool.Exec(ctx, `
ALTER TABLE switch_history ADD COLUMN IF NOT EXISTS sched_from BOOLEAN;
ALTER TABLE switch_history ADD COLUMN IF NOT EXISTS sched_to BOOLEAN;`); err != nil {
		return err
	}
	if err := d.ensureLaneBoardSchema(ctx); err != nil {
		return err
	}
	return d.ensureSchedulerOutbox(ctx)
}

func (d *DB) ensureSchedulerOutbox(ctx context.Context) error {
	var exists bool
	if err := d.pool.QueryRow(ctx, `SELECT to_regclass('scheduler_outbox') IS NOT NULL`).Scan(&exists); err != nil {
		return fmt.Errorf("check Sub2API scheduler_outbox: %w", err)
	}
	if !exists {
		return fmt.Errorf("Sub2API scheduler_outbox table is missing; apply the Sub2API scheduler migration first")
	}
	return nil
}

// logSwitch 记录切换历史（sched_from=旧状态, sched_to=新状态）
// old_priority/new_priority 是旧表 NOT NULL 列，显式填 0 兼容
func (d *DB) logSwitch(ctx context.Context, ruleName, action string, accountID int64, accountName string, from, to bool, message string) {
	_, err := d.pool.Exec(ctx, `
INSERT INTO switch_history (rule_name, action, account_id, account_name, old_priority, new_priority, sched_from, sched_to, triggered_by, status, message)
VALUES ($1,$2,$3,$4,0,0,$5,$6,$7,'success',$8)`,
		ruleName, action, accountID, accountName, from, to, action, message)
	if err != nil {
		log.Printf("[db] logSwitch failed: %v", err)
	}
}

// ListAccounts 列出账号（排除已删除，隐藏凭据）
func (d *DB) ListAccounts(ctx context.Context) ([]Account, error) {
	rows, err := d.pool.Query(ctx, `
SELECT id, name, platform, priority, schedulable, status,
       temp_unschedulable_until::text, rate_limit_reset_at::text, overload_until::text,
       error_message, notes, rate_multiplier, load_factor
FROM accounts
WHERE deleted_at IS NULL
ORDER BY priority ASC, id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Account
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Name, &a.Platform, &a.Priority, &a.Schedulable,
			&a.Status, &a.TempUnschedulable, &a.RateLimited, &a.OverloadUntil,
			&a.ErrorMsg, &a.Notes, &a.RateMultiplier, &a.LoadFactor); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ListHistory 最近切换历史（分页，按时间倒序）
func (d *DB) ListHistory(ctx context.Context, page, pageSize int) ([]SwitchHistory, int, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 15
	}
	var total int
	if err := d.pool.QueryRow(ctx, `SELECT count(*) FROM switch_history`).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := d.pool.Query(ctx, `
SELECT id, rule_name, action, account_id, account_name,
       old_priority, new_priority, sched_from, sched_to, triggered_by, status, message, created_at
FROM switch_history ORDER BY id DESC LIMIT $1 OFFSET $2`, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []SwitchHistory
	for rows.Next() {
		var h SwitchHistory
		if err := rows.Scan(&h.ID, &h.RuleName, &h.Action, &h.AccountID, &h.AccountName,
			&h.OldPriority, &h.NewPriority, &h.SchedFrom, &h.SchedTo,
			&h.TriggeredBy, &h.Status, &h.Message, &h.CreatedAt); err != nil {
			return nil, 0, err
		}
		out = append(out, h)
	}
	return out, total, rows.Err()
}

// GetAccountsByIDs 取指定 ID 的账号（校验存在性）
func (d *DB) GetAccountsByIDs(ctx context.Context, ids []int64) (map[int64]Account, error) {
	out := make(map[int64]Account)
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := d.pool.Query(ctx, `
SELECT id, name, platform, priority, schedulable, status,
       temp_unschedulable_until::text, rate_limit_reset_at::text, overload_until::text,
       error_message, notes, rate_multiplier, load_factor
FROM accounts WHERE deleted_at IS NULL AND id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var a Account
		if err := rows.Scan(&a.ID, &a.Name, &a.Platform, &a.Priority, &a.Schedulable,
			&a.Status, &a.TempUnschedulable, &a.RateLimited, &a.OverloadUntil,
			&a.ErrorMsg, &a.Notes, &a.RateMultiplier, &a.LoadFactor); err != nil {
			return nil, err
		}
		out[a.ID] = a
	}
	return out, rows.Err()
}
