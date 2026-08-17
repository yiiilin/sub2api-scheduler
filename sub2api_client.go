package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Sub2APIClient 通过 sub2api 正式管理 API 操作账号调度（参考 sub2api_giftcode 项目）
// 认证：x-api-key: <admin_api_key>（服务端持有，不依赖 iframe token）
type Sub2APIClient struct {
	BaseURL    string
	AdminAPIKey string
	HTTP       *http.Client
}

// Account sub2api 账号（管理 API 返回结构）
type SubAccount struct {
	ID                      int64          `json:"id"`
	Name                    string         `json:"name"`
	Platform                string         `json:"platform"`
	Type                    string         `json:"type"`
	Status                  string         `json:"status"`
	Schedulable             *bool          `json:"schedulable"`
	RateLimitedAt           *time.Time     `json:"rate_limited_at"`
	RateLimitResetAt        *time.Time     `json:"rate_limit_reset_at"`
	OverloadUntil           *time.Time     `json:"overload_until"`
	TempUnschedulableUntil  *time.Time     `json:"temp_unschedulable_until"`
	TempUnschedulableReason string         `json:"temp_unschedulable_reason"`
	LastUsedAt              *time.Time     `json:"last_used_at"`
	Extra                   map[string]any `json:"extra"`
	CreatedAt               time.Time      `json:"created_at"`
	UpdatedAt               time.Time      `json:"updated_at"`
}

// IsSchedulable 便捷访问（*bool → bool）
func (a *SubAccount) IsSchedulable() bool {
	return a != nil && a.Schedulable != nil && *a.Schedulable
}

type subEnvelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type subPaginated struct {
	Items json.RawMessage `json:"items"`
	Total int             `json:"total"`
}

func NewSub2APIClient(baseURL, adminAPIKey string) *Sub2APIClient {
	return &Sub2APIClient{
		BaseURL:     strings.TrimRight(baseURL, "/"),
		AdminAPIKey: adminAPIKey,
		HTTP:        &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Sub2APIClient) do(ctx context.Context, method, path string, body any, out any) error {
	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("x-api-key", c.AdminAPIKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("sub2api request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("sub2api %s %s → HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if out == nil {
		return nil
	}
	var env subEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		// 非 envelope 格式直接解析 out
		return json.Unmarshal(raw, out)
	}
	if len(env.Data) > 0 {
		return json.Unmarshal(env.Data, out)
	}
	return nil
}

// ListAccounts 列出 openai 平台账号（分页拉全）
func (c *Sub2APIClient) ListAccounts(ctx context.Context) ([]SubAccount, error) {
	var all []SubAccount
	page := 1
	for {
		path := fmt.Sprintf("/api/v1/admin/accounts?platform=openai&page=%d&page_size=200", page)
		var pageData struct {
			Items []SubAccount `json:"items"`
			Total int          `json:"total"`
		}
		if err := c.do(ctx, http.MethodGet, path, nil, &pageData); err != nil {
			return nil, err
		}
		all = append(all, pageData.Items...)
		if len(all) >= pageData.Total || len(pageData.Items) == 0 {
			break
		}
		page++
	}
	return all, nil
}

// GetAccount 获取单个账号
func (c *Sub2APIClient) GetAccount(ctx context.Context, id int64) (*SubAccount, error) {
	var out SubAccount
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/v1/admin/accounts/%d", id), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SetSchedulable 设置账号调度开关（正式管理 API，自动处理 outbox/缓存/审计）
func (c *Sub2APIClient) SetSchedulable(ctx context.Context, id int64, schedulable bool) (*SubAccount, error) {
	var out SubAccount
	body := map[string]bool{"schedulable": schedulable}
	if err := c.do(ctx, http.MethodPost, fmt.Sprintf("/api/v1/admin/accounts/%d/schedulable", id), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SetModelRateLimit 通过正式管理 API 设置某账号某模型的限流条目（泳道图禁用/压制）
// 策略：GET 当前账号 → 合并 model_rate_limits（保留其他模型条目，避免误删多图共存条目）
//   → PUT /admin/accounts/:id 提交完整 extra（sub2api 内部写 scheduler_outbox → 快照刷新）
// 注意：PUT 的 extra 是整体替换，必须从当前账号读回再合并，否则会丢掉 quota 等运行态键。
func (c *Sub2APIClient) SetModelRateLimit(ctx context.Context, id int64, model string, entry map[string]any) error {
	acc, err := c.GetAccount(ctx, id)
	if err != nil {
		return err
	}
	extra := make(map[string]any, len(acc.Extra)+1)
	for k, v := range acc.Extra {
		extra[k] = v
	}
	mrl, _ := extra["model_rate_limits"].(map[string]any)
	if mrl == nil {
		mrl = make(map[string]any)
	}
	mrl[model] = entry
	extra["model_rate_limits"] = mrl
	body := map[string]any{"extra": extra}
	var out SubAccount
	return c.do(ctx, http.MethodPut, fmt.Sprintf("/api/v1/admin/accounts/%d", id), body, &out)
}

// ClearModelRateLimit 通过正式管理 API 删除某账号某模型的限流条目（泳道图恢复）
// 只删目标模型条目，保留其他模型条目（多图共存账号不误伤）。
func (c *Sub2APIClient) ClearModelRateLimit(ctx context.Context, id int64, model string) error {
	acc, err := c.GetAccount(ctx, id)
	if err != nil {
		return err
	}
	extra := make(map[string]any, len(acc.Extra)+1)
	for k, v := range acc.Extra {
		extra[k] = v
	}
	mrl, _ := extra["model_rate_limits"].(map[string]any)
	if mrl == nil {
		return nil // 本来就没有条目，无需操作
	}
	delete(mrl, model)
	extra["model_rate_limits"] = mrl
	body := map[string]any{"extra": extra}
	var out SubAccount
	return c.do(ctx, http.MethodPut, fmt.Sprintf("/api/v1/admin/accounts/%d", id), body, &out)
}

// TestAccount 真实调用测试账号（POST /admin/accounts/:id/test）
// 返回 (成功, 错误信息)
// 注意：sub2api 测试成功后会自动 RecoverAccountAfterSuccessfulTest（清除限流状态）
func (c *Sub2APIClient) TestAccount(ctx context.Context, id int64) (bool, string, error) {
	return c.TestAccountModel(ctx, id, "")
}

// TestAccountModel 用指定模型真实调用测试账号
// modelID 为空时用账号默认模型
func (c *Sub2APIClient) TestAccountModel(ctx context.Context, id int64, modelID string) (bool, string, error) {
	body := map[string]string{
		"model_id": modelID,
		"prompt":   "ping",
		"mode":     "",
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/api/v1/admin/accounts/%d/test", c.BaseURL, id), mustJSON(body))
	if err != nil {
		return false, "", err
	}
	req.Header.Set("x-api-key", c.AdminAPIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, "", fmt.Errorf("test request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return false, "", fmt.Errorf("test HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	// 解析 SSE 流直到 test_complete
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var lastErr string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var ev struct {
			Type    string `json:"type"`
			Success bool   `json:"success"`
			Error   string `json:"error"`
			Status  string `json:"status"`
			Text    string `json:"text"`
		}
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			continue
		}
		switch ev.Type {
		case "test_complete":
			if ev.Success {
				return true, "", nil
			}
			if ev.Error != "" {
				lastErr = ev.Error
			}
			return false, lastErr, nil
		case "test_error":
			if ev.Error != "" {
				lastErr = ev.Error
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return false, lastErr, fmt.Errorf("sse read: %w", err)
	}
	if lastErr == "" {
		lastErr = "SSE 流未收到 test_complete"
	}
	return false, lastErr, nil
}

func mustJSON(v any) io.Reader {
	b, _ := json.Marshal(v)
	return bytes.NewReader(b)
}

// HasUsageAfter 账号是否自 since 以来有调用记录（恢复判断）
func (c *Sub2APIClient) HasUsageAfter(ctx context.Context, accountID int64, since time.Time) (bool, error) {
	startDate := since.UTC().Format("2006-01-02")
	endDate := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	path := fmt.Sprintf("/api/v1/admin/usage?account_id=%d&start_date=%s&end_date=%s&timezone=UTC&page=1&page_size=1&sort_by=created_at&sort_order=desc&exact_total=false",
		accountID, url.QueryEscape(startDate), url.QueryEscape(endDate))
	var pageData struct {
		Items []struct {
			CreatedAt time.Time `json:"created_at"`
		} `json:"items"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &pageData); err != nil {
		return false, err
	}
	if len(pageData.Items) == 0 {
		return false, nil
	}
	return pageData.Items[0].CreatedAt.After(since), nil
}

// IsPrimaryUnavailable 主账号是否当前不可用（参考 giftcode accountHasAuxTemporaryUnavailability）
func IsPrimaryUnavailable(a *SubAccount, now time.Time) bool {
	if a == nil {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(a.Status), "active") {
		return true
	}
	for _, until := range []*time.Time{a.TempUnschedulableUntil, a.RateLimitResetAt, a.OverloadUntil} {
		if until != nil && now.Before(*until) {
			return true
		}
	}
	// 模型级限流
	if rawLimits, ok := a.Extra["model_rate_limits"].(map[string]any); ok {
		for _, rawEntry := range rawLimits {
			resetAt := parseModelRateLimitResetAt(rawEntry)
			if resetAt != nil && now.Before(*resetAt) {
				return true
			}
		}
	}
	return false
}

func parseModelRateLimitResetAt(raw any) *time.Time {
	switch v := raw.(type) {
	case string:
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil
		}
		return &t
	case map[string]any:
		if s, ok := v["rate_limit_reset_at"].(string); ok {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return nil
			}
			return &t
		}
	}
	return nil
}
