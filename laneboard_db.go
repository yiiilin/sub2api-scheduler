package main

import (
	"context"
	"log"
	"strings"
	"time"
)

// ============================ 错误统计 ============================

// CountModelFailures 统计指定账号在窗口内、指定模型的失败次数
// 失败判定：error_phase='upstream' 且
//   - upstream_status_code IS NULL（网络错误）或
//   - upstream_status_code >= 500（上游 5xx）或
//   - upstream_status_code = 429（限流）
// 排除 4xx 业务错误（model_not_found / 参数错误等属于配置问题，不是健康问题）
func (d *DB) CountModelFailures(ctx context.Context, model string, accountIDs []int64, window time.Duration) (map[int64]int, error) {
	if len(accountIDs) == 0 {
		return map[int64]int{}, nil
	}
	rows, err := d.pool.Query(ctx, `
SELECT account_id, count(*)::int
FROM ops_error_logs
WHERE created_at > now() - $1::interval
  AND requested_model = $2
  AND account_id = ANY($3)
  AND error_phase = 'upstream'
  AND (upstream_status_code IS NULL
       OR upstream_status_code >= 500
       OR upstream_status_code = 429)
GROUP BY account_id`,
		window.String(), model, accountIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int64]int)
	for rows.Next() {
		var id int64
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

// ============================ 账号状态 ============================

func (d *DB) SetAccountState(ctx context.Context, boardID, accountID int64, state string, disabledAt *time.Time) error {
	_, err := d.pool.Exec(ctx, `
UPDATE lane_account_states SET state=$3, disabled_at=$4
WHERE board_id=$1 AND account_id=$2`, boardID, accountID, state, disabledAt)
	return err
}

func (d *DB) UpdateAccountStateCheck(ctx context.Context, boardID, accountID int64, failCount int, checkedAt time.Time) error {
	// 仅更新 checked_at；fail_count 是"连续探测失败次数"，由 IncProbeFail/ResetProbeFail 原子维护
	// （否则 checkErrors 循环会把探测失败计数覆盖成 0，永远到不了阈值）
	_, err := d.pool.Exec(ctx, `
UPDATE lane_account_states SET checked_at=$3
WHERE board_id=$1 AND account_id=$2`, boardID, accountID, checkedAt)
	return err
}

// IncProbeFail 原子递增连续探测失败次数，返回新值
func (d *DB) IncProbeFail(ctx context.Context, boardID, accountID int64, at time.Time) (int, error) {
	var n int
	err := d.pool.QueryRow(ctx, `
UPDATE lane_account_states SET fail_count = COALESCE(fail_count, 0) + 1, checked_at=$3
WHERE board_id=$1 AND account_id=$2
RETURNING fail_count`, boardID, accountID, at).Scan(&n)
	return n, err
}

// ResetProbeFail 清零连续探测失败次数（探测成功时）
func (d *DB) ResetProbeFail(ctx context.Context, boardID, accountID int64, at time.Time) error {
	_, err := d.pool.Exec(ctx, `
UPDATE lane_account_states SET fail_count=0, checked_at=$3
WHERE board_id=$1 AND account_id=$2`, boardID, accountID, at)
	return err
}

func (d *DB) UpdateAccountStateProbe(ctx context.Context, boardID, accountID int64, ok bool, msg string, at time.Time) error {
	_, err := d.pool.Exec(ctx, `
UPDATE lane_account_states SET last_probe_at=$3, last_probe_ok=$4, last_probe_msg=$5
WHERE board_id=$1 AND account_id=$2`, boardID, accountID, at, ok, msg)
	return err
}

// ============================ model_rate_limits 控制 ============================

// SetModelRateLimitIfIdle 写入手动禁用条目，仅当该模型当前没有活跃的自动限流条目
// 返回是否真的写入（false = 上游自动限流已在管，不覆盖）
//
// 坑：jsonb_set 的 path 不能创建不存在的两级路径！当 extra 里没有 model_rate_limits
// 键时，jsonb_set(extra, ARRAY['model_rate_limits',$2], ...) 是 no-op（返回原值），
// 但 updated_at 变化会让 RowsAffected=1 造成"写入成功"假象。
// 正确写法：COALESCE(extra->'model_rate_limits','{}') || jsonb_build_object(...) 合并后，
// 用 jsonb_set 写单层 '{model_rate_limits}' 路径（单层缺失可创建）。
func (d *DB) SetModelRateLimitIfIdle(ctx context.Context, accountID int64, model, entryJSON string) (bool, error) {
	tag, err := d.pool.Exec(ctx, `
UPDATE accounts
SET extra = jsonb_set(
      COALESCE(extra, '{}'::jsonb),
      '{model_rate_limits}',
      COALESCE(COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits', '{}'::jsonb)
        || jsonb_build_object($2::text, $3::jsonb),
      true),
    updated_at = now()
WHERE id = $1 AND deleted_at IS NULL
  AND (
    NOT (COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits' ? $2::text)
    OR (COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits' -> $2::text ->> 'rate_limit_reset_at') IS NULL
    OR (COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits' -> $2::text ->> 'rate_limit_reset_at')::timestamptz < now()
  )`,
		accountID, model, entryJSON)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// ClearModelRateLimitIfOwned 删除自己写入的 failed 禁用条目（reason = lane_board:<board>），返回是否删除
// suppressed 条目（lane_board:suppressed:*）和 sub2api 自动限流条目不动
// 注意：不能用 `-> 'model_rate_limits' - $2` —— PG 会把 key 字面量误解析为 jsonb
// （jsonb - jsonb 重载优先于 jsonb - text），报 "invalid input syntax for type json"。
// 必须用 jsonb_delete() 函数显式指定 text 语义。
func (d *DB) ClearModelRateLimitIfOwned(ctx context.Context, accountID int64, model, boardName string) (bool, error) {
	tag, err := d.pool.Exec(ctx, `
UPDATE accounts
SET extra = jsonb_set(
      COALESCE(extra, '{}'::jsonb),
      '{model_rate_limits}',
      jsonb_delete(COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits', $2::text),
      true),
    updated_at = now()
WHERE id = $1 AND deleted_at IS NULL
  AND COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits' -> $2::text ->> 'reason' = 'lane_board:' || $3::text`,
		accountID, model, boardName)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// ClearSuppressIfOwned 删除抑制条目（reason LIKE lane_board:suppressed:%），返回是否删除
func (d *DB) ClearSuppressIfOwned(ctx context.Context, accountID int64, model string) (bool, error) {
	tag, err := d.pool.Exec(ctx, `
UPDATE accounts
SET extra = jsonb_set(
      COALESCE(extra, '{}'::jsonb),
      '{model_rate_limits}',
      jsonb_delete(COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits', $2::text),
      true),
    updated_at = now()
WHERE id = $1 AND deleted_at IS NULL
  AND COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits' -> $2::text ->> 'reason' LIKE 'lane\_board:suppressed:%'`,
		accountID, model)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// ============================ 事件记录 ============================

// LogLaneEvent 泳道图事件写入 switch_history（复用现有历史表）
func (d *DB) LogLaneEvent(ctx context.Context, boardName, model string, accountID int64, action, msg string) {
	accName := ""
	if acc, err := d.GetAccountsByIDs(ctx, []int64{accountID}); err == nil {
		if a, ok := acc[accountID]; ok {
			accName = a.Name
		}
	}
	ruleName := "[泳道图] " + boardName + " (" + model + ")"
	_, err := d.pool.Exec(ctx, `
INSERT INTO switch_history (rule_name, action, account_id, account_name, old_priority, new_priority, triggered_by, status, message)
VALUES ($1,$2,$3,$4,0,0,'laneboard','success',$5)`,
		ruleName, action, accountID, accName, msg)
	if err != nil {
		log.Printf("[laneboard] log event: %v", err)
	}
}


// ExternalBlock 外部抑制信息：sub2api 原生机制挡住调度（非泳道图写入）
type ExternalBlock struct {
	// 账号级调度开关（sub2api 可自动关闭：连续失败/暂停）
	Schedulable bool
	// 账号状态（active 之外都不可调度）
	Status string
	// 原生 model_rate_limits 冷却：reason 非 lane_board: 前缀且 rate_limit_reset_at > now
	NativeCoolUntil *time.Time
	// 账号级临时禁调度
	TempUnschedUntil *time.Time
}

// blocked 是否当前被外部抑制
func (e ExternalBlock) blocked(now time.Time) bool {
	if !e.Schedulable {
		return true
	}
	if e.Status != "active" {
		return true
	}
	if e.NativeCoolUntil != nil && e.NativeCoolUntil.After(now) {
		return true
	}
	if e.TempUnschedUntil != nil && e.TempUnschedUntil.After(now) {
		return true
	}
	return false
}

// blockedReason 外部抑制描述（UI/日志用）
func (e ExternalBlock) blockedReason(now time.Time) string {
	if !e.Schedulable {
		return "账号调度开关关闭"
	}
	if e.Status != "active" {
		return "账号状态 " + e.Status
	}
	if e.NativeCoolUntil != nil && e.NativeCoolUntil.After(now) {
		return "上游冷却至 " + e.NativeCoolUntil.In(time.FixedZone("CST", 8*3600)).Format("15:04")
	}
	if e.TempUnschedUntil != nil && e.TempUnschedUntil.After(now) {
		return "临时禁调度至 " + e.TempUnschedUntil.In(time.FixedZone("CST", 8*3600)).Format("15:04")
	}
	return ""
}

// GetExternalBlocks 查询一批账号的外部抑制状态（泳道图不写、只感知）
// 原生冷却 = model_rate_limits.<model> 中 reason 非 lane_board: 前缀且 rate_limit_reset_at > now
// 临时禁调度 = temp_unschedulable_until > now
func (d *DB) GetExternalBlocks(ctx context.Context, accountIDs []int64, model string) (map[int64]ExternalBlock, error) {
	out := make(map[int64]ExternalBlock, len(accountIDs))
	if len(accountIDs) == 0 {
		return out, nil
	}
	rows, err := d.pool.Query(ctx, `
SELECT id, schedulable, status,
  (COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits' -> $2::text ->> 'rate_limit_reset_at')::timestamptz AS cool_until,
  (COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits' -> $2::text ->> 'reason') AS cool_reason,
  temp_unschedulable_until
FROM accounts
WHERE id = ANY($1) AND deleted_at IS NULL`,
		accountIDs, model)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var schedulable bool
		var status string
		var coolUntil, tempUntil *time.Time
		var coolReason *string
		if err := rows.Scan(&id, &schedulable, &status, &coolUntil, &coolReason, &tempUntil); err != nil {
			return nil, err
		}
		var eb ExternalBlock
		eb.Schedulable = schedulable
		eb.Status = status
		// 原生冷却：仅当 reason 非 lane_board: 前缀（泳道图自己的条目不算外部抑制）
		if coolUntil != nil && (coolReason == nil || !strings.HasPrefix(*coolReason, "lane_board:")) {
			eb.NativeCoolUntil = coolUntil
		}
		eb.TempUnschedUntil = tempUntil
		out[id] = eb
	}
	return out, rows.Err()
}


// LastSuccessfulCalls 查一批账号在窗口内、指定模型最近一次实际调用成功的时间（usage_logs 只记成功请求）
func (d *DB) LastSuccessfulCalls(ctx context.Context, accountIDs []int64, model string, window time.Duration) (map[int64]time.Time, error) {
	out := make(map[int64]time.Time, len(accountIDs))
	if len(accountIDs) == 0 {
		return out, nil
	}
	rows, err := d.pool.Query(ctx, `
SELECT account_id, max(created_at)::timestamptz
FROM usage_logs
WHERE created_at > now() - $1::interval
  AND requested_model = $2
  AND account_id = ANY($3)
GROUP BY account_id`,
		window.String(), model, accountIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var at time.Time
		if err := rows.Scan(&id, &at); err != nil {
			return nil, err
		}
		out[id] = at
	}
	return out, rows.Err()
}

// HasSuppressEntry 检查账号是否存在泳道图抑制条目（reason LIKE lane_board:suppressed:%）
func (d *DB) HasSuppressEntry(ctx context.Context, accountID int64, model string) (bool, error) {
	var has bool
	err := d.pool.QueryRow(ctx, `
SELECT EXISTS(
  SELECT 1 FROM accounts
  WHERE id = $1 AND deleted_at IS NULL
    AND COALESCE(extra, '{}'::jsonb) -> 'model_rate_limits' -> $2::text ->> 'reason' LIKE 'lane\_board:suppressed:%'
)`, accountID, model).Scan(&has)
	return has, err
}
