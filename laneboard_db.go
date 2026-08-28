package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// ============================ 错误统计 ============================

// CountModelFailures 统计指定账号在窗口内、指定模型的失败次数
// 失败判定：error_phase='upstream' 且
//   - upstream_status_code IS NULL（网络错误）或
//   - upstream_status_code >= 500（上游 5xx）或
//   - upstream_status_code = 429（限流）
//
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
INSERT INTO lane_account_states (board_id, account_id, state, disabled_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (board_id, account_id) DO UPDATE SET
  state=EXCLUDED.state,
  disabled_at=EXCLUDED.disabled_at`, boardID, accountID, state, disabledAt)
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

// ExternalBlock 是 Sub2API 原生机制施加的调度阻塞（泳道图不覆盖）。
type ExternalBlock struct {
	Schedulable bool
	Status      string

	// 账号过期自动暂停、API key/Bedrock 配额阻塞。
	Expired       bool
	QuotaExceeded bool
	// 当前 board 自己的模型限制，用于发现“本地 healthy、远端仍阻断”的失配。
	OwnedModelLimit bool
	// 原生 model_rate_limits 中的未来冷却（其它 owner 也视为外部阻塞）。
	NativeCoolUntil *time.Time
	// 账号级运行时限流、过载和临时禁调度都会被 Sub2API 调度器跳过。
	RateLimitUntil   *time.Time
	OverloadUntil    *time.Time
	TempUnschedUntil *time.Time
}

func (e ExternalBlock) blocked(now time.Time) bool {
	if !e.Schedulable || e.Status != "active" || e.Expired || e.QuotaExceeded {
		return true
	}
	for _, until := range []*time.Time{e.NativeCoolUntil, e.RateLimitUntil, e.OverloadUntil, e.TempUnschedUntil} {
		if until != nil && until.After(now) {
			return true
		}
	}
	return false
}

func (e ExternalBlock) blockedReason(now time.Time) string {
	if !e.Schedulable {
		return "账号调度开关关闭"
	}
	if e.Status != "active" {
		return "账号状态 " + e.Status
	}
	if e.Expired {
		return "账号已过期"
	}
	if e.QuotaExceeded {
		return "账号配额已用尽"
	}
	if e.NativeCoolUntil != nil && e.NativeCoolUntil.After(now) {
		return "模型级上游冷却至 " + e.NativeCoolUntil.In(time.FixedZone("CST", 8*3600)).Format("15:04")
	}
	if e.RateLimitUntil != nil && e.RateLimitUntil.After(now) {
		return "账号级限流至 " + e.RateLimitUntil.In(time.FixedZone("CST", 8*3600)).Format("15:04")
	}
	if e.OverloadUntil != nil && e.OverloadUntil.After(now) {
		return "账号过载至 " + e.OverloadUntil.In(time.FixedZone("CST", 8*3600)).Format("15:04")
	}
	if e.TempUnschedUntil != nil && e.TempUnschedUntil.After(now) {
		return "临时禁调度至 " + e.TempUnschedUntil.In(time.FixedZone("CST", 8*3600)).Format("15:04")
	}
	return ""
}

// GetExternalBlocks 查询账号级和目标模型级的 Sub2API 原生调度阻塞。
// 模型级限制按账号的 model_mapping 解析；只有当前 board 自己的 owner 条目会被排除。
func (d *DB) GetExternalBlocks(ctx context.Context, accountIDs []int64, model, boardName string) (map[int64]ExternalBlock, error) {
	out := make(map[int64]ExternalBlock, len(accountIDs))
	if len(accountIDs) == 0 {
		return out, nil
	}
	rows, err := d.pool.Query(ctx, `
SELECT id, platform, type, schedulable, status,
       rate_limit_reset_at, overload_until, temp_unschedulable_until,
       auto_pause_on_expired, expires_at,
       credentials->'model_mapping',
       COALESCE(credentials->>'oauth_type', ''),
       COALESCE(credentials->>'project_id', ''),
       COALESCE((extra->>'openai_passthrough') = 'true', false),
       COALESCE((extra->>'openai_oauth_passthrough') = 'true', false),
       extra->'model_rate_limits',
       jsonb_build_object(
         'quota_limit', extra->'quota_limit',
         'quota_used', extra->'quota_used',
         'quota_reset_timezone', extra->'quota_reset_timezone',
          'quota_daily_limit', extra->'quota_daily_limit',
         'quota_daily_used', extra->'quota_daily_used',
         'quota_daily_start', extra->'quota_daily_start',
         'quota_daily_reset_mode', extra->'quota_daily_reset_mode',
          'quota_daily_reset_hour', extra->'quota_daily_reset_hour',
         'quota_daily_reset_at', extra->'quota_daily_reset_at',
         'quota_weekly_limit', extra->'quota_weekly_limit',
         'quota_weekly_used', extra->'quota_weekly_used',
         'quota_weekly_start', extra->'quota_weekly_start',
         'quota_weekly_reset_mode', extra->'quota_weekly_reset_mode',
          'quota_weekly_reset_day', extra->'quota_weekly_reset_day',
          'quota_weekly_reset_hour', extra->'quota_weekly_reset_hour',
         'quota_weekly_reset_at', extra->'quota_weekly_reset_at'
       ) AS quota_state
FROM accounts
WHERE id = ANY($1) AND deleted_at IS NULL`, accountIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := time.Now()
	for rows.Next() {
		var id int64
		var platform, accountType string
		var schedulable bool
		var status string
		var autoPauseOnExpired bool
		var expiresAt *time.Time
		var rateLimitUntil, overloadUntil, tempUntil *time.Time
		var mappingJSON, limitsJSON, quotaJSON []byte
		var oauthType, projectID string
		var passThrough, oauthPassThrough bool
		if err := rows.Scan(&id, &platform, &accountType, &schedulable, &status,
			&rateLimitUntil, &overloadUntil, &tempUntil, &autoPauseOnExpired, &expiresAt,
			&mappingJSON, &oauthType, &projectID, &passThrough, &oauthPassThrough,
			&limitsJSON, &quotaJSON); err != nil {
			return nil, err
		}

		mapping := make(map[string]any)
		if len(mappingJSON) > 0 && string(mappingJSON) != "null" {
			if err := json.Unmarshal(mappingJSON, &mapping); err != nil {
				return nil, fmt.Errorf("decode account %d model mapping: %w", id, err)
			}
		}
		credentials := map[string]any{
			"model_mapping":            mapping,
			"oauth_type":               oauthType,
			"project_id":               projectID,
			"openai_passthrough":       passThrough,
			"openai_oauth_passthrough": oauthPassThrough,
		}
		limits := make(map[string]any)
		if len(limitsJSON) > 0 && string(limitsJSON) != "null" {
			if err := json.Unmarshal(limitsJSON, &limits); err != nil {
				return nil, fmt.Errorf("decode account %d model rate limits: %w", id, err)
			}
		}
		quotaState := make(map[string]any)
		if len(quotaJSON) > 0 && string(quotaJSON) != "null" {
			if err := json.Unmarshal(quotaJSON, &quotaState); err != nil {
				return nil, fmt.Errorf("decode account %d quota state: %w", id, err)
			}
		}

		eb := ExternalBlock{
			Schedulable:      schedulable,
			Status:           status,
			RateLimitUntil:   rateLimitUntil,
			OverloadUntil:    overloadUntil,
			TempUnschedUntil: tempUntil,
			Expired:          autoPauseOnExpired && expiresAt != nil && !expiresAt.After(now),
			QuotaExceeded:    (accountType == "apikey" || accountType == "bedrock") && accountQuotaExceeded(quotaState, now),
		}
		account := &SubAccount{Platform: platform, Type: accountType, Credentials: credentials, Extra: credentials}
		for _, modelKey := range accountModelRateLimitKeys(account, model) {
			entry, exists := limits[modelKey]
			if !exists {
				continue
			}
			modelReset := modelRateLimitResetAt(entry)
			if modelReset == nil {
				continue
			}
			if !modelReset.After(now) {
				continue
			}
			if modelRateLimitEntryOwnedBy(entry, boardName) {
				eb.OwnedModelLimit = true
				continue
			}
			if eb.NativeCoolUntil == nil || modelReset.After(*eb.NativeCoolUntil) {
				eb.NativeCoolUntil = modelReset
			}
		}
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
