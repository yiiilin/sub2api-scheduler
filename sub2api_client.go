package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Sub2APIClient 通过 sub2api 正式管理 API 操作账号调度（参考 sub2api_giftcode 项目）
// 认证：x-api-key: <admin_api_key>（服务端持有，不依赖 iframe token）
type Sub2APIClient struct {
	BaseURL     string
	AdminAPIKey string
	HTTP        *http.Client
	store       *DB

	accountLocksMu sync.Mutex
	accountLocks   map[int64]*sync.Mutex
}

// Account sub2api 账号（管理 API 返回结构）
type SubAccount struct {
	ID                      int64          `json:"id"`
	Name                    string         `json:"name"`
	Platform                string         `json:"platform"`
	Type                    string         `json:"type"`
	Status                  string         `json:"status"`
	Credentials             map[string]any `json:"credentials"`
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

func NewSub2APIClient(baseURL, adminAPIKey string, stores ...*DB) *Sub2APIClient {
	var store *DB
	if len(stores) > 0 {
		store = stores[0]
	}
	return &Sub2APIClient{
		BaseURL:      strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		AdminAPIKey:  strings.TrimSpace(adminAPIKey),
		HTTP:         &http.Client{Timeout: 30 * time.Second},
		store:        store,
		accountLocks: make(map[int64]*sync.Mutex),
	}
}

func (c *Sub2APIClient) lockAccount(accountID int64) func() {
	c.accountLocksMu.Lock()
	if c.accountLocks == nil {
		c.accountLocks = make(map[int64]*sync.Mutex)
	}
	lock := c.accountLocks[accountID]
	if lock == nil {
		lock = &sync.Mutex{}
		c.accountLocks[accountID] = lock
	}
	c.accountLocksMu.Unlock()

	lock.Lock()
	return lock.Unlock
}

func (c *Sub2APIClient) do(ctx context.Context, method, path string, body any, out any) error {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal Sub2API request body: %w", err)
		}
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
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("read Sub2API response: %w", err)
	}
	if resp.StatusCode >= 400 {
		message := strings.TrimSpace(string(raw))
		if resp.StatusCode == http.StatusNotFound && strings.Contains(path, "/accounts/") {
			return fmt.Errorf("%w: %s %s", ErrSub2APIAccountNotFound, method, path)
		}
		return fmt.Errorf("sub2api %s %s → HTTP %d: %s", method, path, resp.StatusCode, message)
	}

	var envelopeFields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelopeFields); err == nil {
		if _, isEnvelope := envelopeFields["code"]; isEnvelope {
			var env subEnvelope
			if err := json.Unmarshal(raw, &env); err != nil {
				return fmt.Errorf("decode Sub2API envelope: %w", err)
			}
			if env.Code != 0 {
				return fmt.Errorf("sub2api %s %s → code %d: %s", method, path, env.Code, strings.TrimSpace(env.Message))
			}
			if out == nil {
				return nil
			}
			if len(bytes.TrimSpace(env.Data)) == 0 || bytes.Equal(bytes.TrimSpace(env.Data), []byte("null")) {
				return fmt.Errorf("sub2api %s %s: success envelope is missing data", method, path)
			}
			return json.Unmarshal(env.Data, out)
		}
	}

	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

// ListAccounts 列出全部未删除账号（分页拉全）
func (c *Sub2APIClient) ListAccounts(ctx context.Context) ([]SubAccount, error) {
	var all []SubAccount
	page := 1
	for {
		path := fmt.Sprintf("/api/v1/admin/accounts?page=%d&page_size=200", page)
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
	unlock := c.lockAccount(id)
	defer unlock()

	var out SubAccount
	body := map[string]bool{"schedulable": schedulable}
	if err := c.do(ctx, http.MethodPost, fmt.Sprintf("/api/v1/admin/accounts/%d/schedulable", id), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

var (
	ErrForeignModelRateLimit  = errors.New("model rate-limit entry is owned by another controller")
	ErrSub2APIAccountNotFound = errors.New("Sub2API account not found")
)

// mappedModelRateLimitKey mirrors Sub2API's model mapping lookup for the
// persisted model_rate_limits key. Board.Model is the incoming request model;
// Sub2API stores/checks limits under the mapped upstream model.
func mappedModelRateLimitKey(credentials map[string]any, requestedModel string) string {
	return resolveMappedModel(rawModelMapping(credentials), requestedModel)
}

func rawModelMapping(credentials map[string]any) map[string]string {
	if credentials == nil {
		return nil
	}
	mapping := make(map[string]string)
	switch raw := credentials["model_mapping"].(type) {
	case map[string]any:
		for key, value := range raw {
			if target, ok := value.(string); ok && strings.TrimSpace(target) != "" {
				mapping[key] = strings.TrimSpace(target)
			}
		}
	case map[string]string:
		for key, value := range raw {
			if strings.TrimSpace(value) != "" {
				mapping[key] = strings.TrimSpace(value)
			}
		}
	}
	return mapping
}

var defaultAntigravityModelMapping = map[string]string{
	"claude-fable-5": "claude-fable-5", "claude-opus-4-8": "claude-opus-4-8",
	"claude-opus-4-7": "claude-opus-4-7", "claude-opus-4-6-thinking": "claude-opus-4-6-thinking",
	"claude-opus-4-6": "claude-opus-4-6-thinking", "claude-opus-4-5-thinking": "claude-opus-4-6-thinking",
	"claude-opus-4-5-20251101": "claude-opus-4-6-thinking", "claude-sonnet-4-6": "claude-sonnet-4-6",
	"claude-sonnet-4-5": "claude-sonnet-4-5", "claude-sonnet-4-5-thinking": "claude-sonnet-4-6",
	"claude-sonnet-4-5-20250929": "claude-sonnet-4-6", "claude-haiku-4-5": "claude-sonnet-4-6",
	"claude-haiku-4-5-20251001": "claude-sonnet-4-6", "gemini-2.5-flash": "gemini-2.5-flash",
	"gemini-2.5-flash-image": "gemini-2.5-flash-image", "gemini-2.5-flash-image-preview": "gemini-2.5-flash-image",
	"gemini-2.5-flash-lite": "gemini-2.5-flash-lite", "gemini-2.5-flash-thinking": "gemini-2.5-flash-thinking",
	"gemini-2.5-pro": "gemini-2.5-pro", "gemini-3-flash": "gemini-3-flash",
	"gemini-3-pro-high": "gemini-3-pro-high", "gemini-3-pro-low": "gemini-3-pro-low",
	"gemini-3-flash-preview": "gemini-3-flash", "gemini-3-pro-preview": "gemini-3-pro-high",
	"gemini-3.1-pro": "gemini-pro-agent", "gemini-3.1-pro-high": "gemini-pro-agent",
	"gemini-3.1-pro-preview": "gemini-pro-agent", "gemini-pro-agent": "gemini-pro-agent",
	"gemini-3.1-pro-low": "gemini-3.1-pro-low", "gemini-3.1-flash-image": "gemini-3.1-flash-image",
	"gemini-3.1-flash-image-preview": "gemini-3.1-flash-image", "gemini-3.6-flash": "gemini-3.6-flash",
	"gemini-3.6-flash-high": "gemini-3.6-flash-high", "gemini-3.6-flash-low": "gemini-3.6-flash-low",
	"gemini-3.6-flash-medium": "gemini-3.6-flash-medium", "gemini-3.6-flash-tiered": "gemini-3.6-flash-tiered",
	"gemini-3-pro-image": "gemini-3.1-flash-image", "gemini-3-pro-image-preview": "gemini-3.1-flash-image",
	"gpt-oss-120b-medium": "gpt-oss-120b-medium", "tab_flash_lite_preview": "tab_flash_lite_preview",
}

var defaultGrokModelMapping = map[string]string{
	"grok-4.6": "grok-4.6", "grok-4.5": "grok-4.5", "grok-4.3": "grok-4.3", "grok-3-mini": "grok-3-mini", "grok-3-mini-fast": "grok-3-mini-fast",
	"grok-build-0.1": "grok-build-0.1", "grok-composer-2.5-fast": "grok-composer-2.5-fast",
	"grok-4.20-0309-reasoning": "grok-4.20-0309-reasoning", "grok-4.20-0309-non-reasoning": "grok-4.20-0309-non-reasoning", "grok-4.20-multi-agent-0309": "grok-4.20-multi-agent-0309",
	"grok-imagine-image-quality": "grok-imagine-image-quality", "grok-imagine-image": "grok-imagine-image", "grok-imagine-image-2.0": "grok-imagine-image-2.0",
	"grok-imagine-video": "grok-imagine-video", "grok-imagine-video-1.5": "grok-imagine-video-1.5",
	"grok": "grok-4.6", "grok-latest": "grok-4.6", "grok-4.6-latest": "grok-4.6",
	"grok-4.5-latest": "grok-4.5", "grok-4.3-latest": "grok-4.3", "grok-build": "grok-build-0.1",
	"grok-build-latest": "grok-build-0.1", "grok-composer": "grok-composer-2.5-fast", "composer-2.5": "grok-composer-2.5-fast",
	"grok-4.20-reasoning": "grok-4.20-0309-reasoning", "grok-4.20-non-reasoning": "grok-4.20-0309-non-reasoning", "grok-4.20-multi-agent": "grok-4.20-multi-agent-0309",
	"grok-imagine": "grok-imagine-image-quality", "grok-imagine-1": "grok-imagine-image-quality", "grok-imagine-edit": "grok-imagine-image-quality",
	"grok-imagine-video-1.5-preview": "grok-imagine-video-1.5", "grok-video-1.5": "grok-imagine-video-1.5",
}

var defaultGoogleOneModelMapping = map[string]string{
	"gemini-2.0-flash": "gemini-2.0-flash", "gemini-2.5-flash": "gemini-2.5-flash", "gemini-2.5-pro": "gemini-2.5-pro",
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func boolValue(values map[string]any, key string) bool {
	value, _ := values[key].(bool)
	return value
}

func effectiveModelMapping(account *SubAccount) map[string]string {
	if account == nil {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(account.Platform), "openai") &&
		(boolValue(account.Extra, "openai_passthrough") || boolValue(account.Extra, "openai_oauth_passthrough")) {
		return nil
	}
	mapping := rawModelMapping(account.Credentials)
	if len(mapping) > 0 {
		if strings.EqualFold(strings.TrimSpace(account.Platform), "antigravity") {
			mapping = cloneStringMap(mapping)
			for _, model := range []string{
				"gemini-3-flash", "gemini-3.1-pro-high", "gemini-3.1-pro-low", "gemini-3.6-flash",
				"gemini-3.6-flash-high", "gemini-3.6-flash-low", "gemini-3.6-flash-medium", "gemini-3.6-flash-tiered",
			} {
				if _, exists := mapping[model]; !exists && !mappingHasWildcardForModel(mapping, model) {
					mapping[model] = model
				}
			}
			if target := strings.TrimSpace(mapping["gemini-pro-agent"]); target != "" {
				for model, legacyTarget := range map[string]string{
					"gemini-3.1-pro":         "gemini-3.1-pro",
					"gemini-3.1-pro-high":    "gemini-3.1-pro-high",
					"gemini-3.1-pro-preview": "gemini-3.1-pro-preview",
				} {
					current, exists := mapping[model]
					if !exists || current == legacyTarget || (model == "gemini-3.1-pro-preview" && current == "gemini-3.1-pro-high") {
						if !mappingHasWildcardForModel(mapping, model) || exists {
							mapping[model] = target
						}
					}
				}
			}
		}
		return mapping
	}
	switch strings.ToLower(strings.TrimSpace(account.Platform)) {
	case "antigravity":
		return defaultAntigravityModelMapping
	case "grok":
		mapping := cloneStringMap(defaultGrokModelMapping)
		for key, value := range cloneStringMap(mapping) {
			lower := strings.ToLower(key)
			if strings.Contains(key, "/") || (!strings.HasPrefix(lower, "grok") && !strings.HasPrefix(lower, "imagine") && !strings.HasPrefix(lower, "composer")) {
				continue
			}
			for _, prefix := range []string{"xai/", "x-ai/", "grok/"} {
				mapping[prefix+key] = value
			}
		}
		return mapping
	case "gemini":
		if strings.EqualFold(strings.TrimSpace(stringValue(account.Credentials, "oauth_type")), "google_one") {
			return defaultGoogleOneModelMapping
		}
	}
	return nil
}

func mappingHasWildcardForModel(mapping map[string]string, requestedModel string) bool {
	for pattern := range mapping {
		if strings.HasSuffix(pattern, "*") && strings.HasPrefix(requestedModel, strings.TrimSuffix(pattern, "*")) {
			return true
		}
	}
	return false
}

func normalizeRequestedModelForAccount(platform, requestedModel string) string {
	requestedModel = strings.TrimSpace(requestedModel)
	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "antigravity":
		requestedModel = strings.TrimPrefix(requestedModel, "models/")
		if requestedModel == "gemini-3.1-pro-preview-customtools" {
			return "gemini-3.1-pro-preview"
		}
	case "gemini":
		if requestedModel == "gemini-3.1-pro-preview-customtools" {
			return "gemini-3.1-pro-preview"
		}
	case "grok":
		return requestedModel
	}
	return requestedModel
}

func stringValue(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func mappedModelRateLimitKeyForAccount(account *SubAccount, requestedModel string) string {
	if account == nil {
		return strings.TrimSpace(requestedModel)
	}
	requestedModel = normalizeRequestedModelForAccount(account.Platform, requestedModel)
	return resolveMappedModel(effectiveModelMapping(account), requestedModel)
}

func resolveMappedModel(mapping map[string]string, requestedModel string) string {
	requestedModel = strings.TrimSpace(requestedModel)
	if requestedModel == "" || len(mapping) == 0 {
		return requestedModel
	}
	if target := strings.TrimSpace(mapping[requestedModel]); target != "" {
		return target
	}
	bestPattern, bestTarget := "", ""
	for pattern, target := range mapping {
		pattern = strings.TrimSpace(pattern)
		target = strings.TrimSpace(target)
		matches := pattern == requestedModel || (strings.HasSuffix(pattern, "*") && strings.HasPrefix(requestedModel, strings.TrimSuffix(pattern, "*")))
		if matches && (len(pattern) > len(bestPattern) || (len(pattern) == len(bestPattern) && pattern < bestPattern)) {
			bestPattern, bestTarget = pattern, target
		}
	}
	if bestTarget != "" {
		return bestTarget
	}
	return requestedModel
}
func modelMappingSupportsRequestedModel(credentials map[string]any, requestedModel string) bool {
	return modelMappingSupportsRequestedModelForAccount("", "", credentials, requestedModel)
}

var defaultBedrockModels = map[string]struct{}{
	"claude-fable-5": {}, "claude-opus-5": {}, "claude-opus-4-8": {}, "claude-opus-4-7": {},
	"claude-opus-4-6-thinking": {}, "claude-opus-4-6": {}, "claude-opus-4-5-thinking": {}, "claude-opus-4-5-20251101": {},
	"claude-opus-4-1": {}, "claude-opus-4-20250514": {}, "claude-sonnet-5": {}, "claude-sonnet-4-6-thinking": {},
	"claude-sonnet-4-6": {}, "claude-sonnet-4-5": {}, "claude-sonnet-4-5-thinking": {}, "claude-sonnet-4-5-20250929": {},
	"claude-sonnet-4-20250514": {}, "claude-haiku-4-5": {}, "claude-haiku-4-5-20251001": {},
}

func defaultBedrockModelSupported(requestedModel string) bool {
	_, ok := defaultBedrockModels[strings.ToLower(strings.TrimSpace(requestedModel))]
	return ok
}

func bedrockModelSupported(requestedModel string) bool {
	model := strings.ToLower(strings.TrimSpace(requestedModel))
	if model == "" {
		return false
	}
	if strings.HasPrefix(model, "arn:") {
		return true
	}
	for _, prefix := range []string{"anthropic.", "amazon.", "meta.", "mistral.", "cohere.", "ai21.", "deepseek.", "stability.", "writer.", "nova.", "us.", "eu.", "apac.", "jp.", "au.", "us-gov.", "global."} {
		if strings.HasPrefix(model, prefix) {
			return true
		}
	}
	return false
}

func modelMappingSupportsRequestedModelForAccount(platform, accountType string, credentials map[string]any, requestedModel string) bool {
	account := &SubAccount{Platform: platform, Type: accountType, Credentials: credentials, Extra: credentials}
	if strings.EqualFold(strings.TrimSpace(platform), "openai") &&
		(boolValue(credentials, "openai_passthrough") || boolValue(credentials, "openai_oauth_passthrough")) {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(platform), "anthropic") && strings.EqualFold(strings.TrimSpace(accountType), "bedrock") && len(rawModelMapping(credentials)) == 0 {
		return bedrockModelSupported(requestedModel) || defaultBedrockModelSupported(requestedModel)
	}
	mapping := effectiveModelMapping(account)
	requestedModel = normalizeRequestedModelForAccount(platform, requestedModel)
	if len(mapping) > 0 {
		requestedModel = strings.TrimSpace(requestedModel)
		if requestedModel == "" {
			return false
		}
		if _, exists := mapping[requestedModel]; exists {
			return true
		}
		for pattern, target := range mapping {
			if strings.TrimSpace(target) == "" {
				continue
			}
			if pattern == requestedModel || (strings.HasSuffix(pattern, "*") && strings.HasPrefix(requestedModel, strings.TrimSuffix(pattern, "*"))) {
				return true
			}
		}
		return false
	}

	// Sub2API treats an empty mapping as pass-through for ordinary accounts.
	// OpenAI OAuth is the exception: its Codex upstream cannot serve known
	// models belonging to other providers.
	if strings.EqualFold(strings.TrimSpace(platform), "openai") && strings.EqualFold(strings.TrimSpace(accountType), "oauth") {
		model := strings.ToLower(strings.TrimSpace(requestedModel))
		if slash := strings.LastIndex(model, "/"); slash >= 0 {
			model = model[slash+1:]
		}
		if model == "k3" || model == "k3-256k" {
			return false
		}
		for _, prefix := range []string{
			"deepseek-", "glm-", "kimi-", "moonshot-", "qwen-", "qwen2-", "qwen3-", "qwen4-", "qwq-",
			"minimax-", "gemini-", "gemma-", "grok-", "doubao-", "hunyuan-", "llama-", "llama2-", "llama3-",
			"meta-llama", "mistral-", "mixtral-", "baichuan-", "ernie-", "step-", "seed-", "yi-",
		} {
			if strings.HasPrefix(model, prefix) {
				return false
			}
		}
	}
	return true
}

func accountModelRateLimitKey(account *SubAccount, requestedModel string) string {
	keys := accountModelRateLimitKeys(account, requestedModel)
	if len(keys) == 0 {
		return strings.TrimSpace(requestedModel)
	}
	return keys[0]
}

func normalizeAntigravityModelKey(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	for _, marker := range []string{"/publishers/google/models/", "/publishers/anthropic/models/", "/models/"} {
		if index := strings.LastIndex(model, marker); index >= 0 {
			return model[index+len(marker):]
		}
	}
	for _, prefix := range []string{"publishers/google/models/", "publishers/anthropic/models/", "models/"} {
		model = strings.TrimPrefix(model, prefix)
	}
	return model
}

func accountModelRateLimitKeys(account *SubAccount, requestedModel string) []string {
	if account == nil {
		return []string{strings.TrimSpace(requestedModel)}
	}
	primary := mappedModelRateLimitKeyForAccount(account, requestedModel)
	if primary == "" {
		return nil
	}
	keys := []string{primary}
	switch strings.ToLower(strings.TrimSpace(account.Platform)) {
	case "openai":
		lowerRequest := strings.ToLower(strings.TrimSpace(requestedModel))
		isImageModel := strings.HasPrefix(lowerRequest, "gpt-image-") || strings.HasPrefix(lowerRequest, "grok-imagine") || strings.HasPrefix(strings.ToLower(primary), "gpt-image-")
		if isImageModel && primary != "openai:image_generation" {
			keys = append(keys, "openai:image_generation")
		}
	case "antigravity":
		normalizedPrimary := normalizeAntigravityModelKey(primary)
		if strings.HasPrefix(normalizedPrimary, "gemini-") && primary != "antigravity:gemini" {
			keys = append(keys, "antigravity:gemini")
		}
		if primary == "claude-sonnet-4-5" {
			keys = append(keys, "claude-sonnet-4-5-thinking")
		}
	case "anthropic":
		if strings.Contains(strings.ToLower(primary), "fable") && primary != "claude-fable-5" {
			keys = append(keys, "claude-fable-5")
		}
	}
	return keys
}

func modelRateLimitEntryReason(entry any) string {
	switch limit := entry.(type) {
	case string:
		return ""
	case map[string]any:
		reason, _ := limit["reason"].(string)
		return reason
	case map[string]string:
		return limit["reason"]
	default:
		return ""
	}
}

func modelRateLimitEntryOwnedBy(entry any, boardName string) bool {
	if boardName == "" {
		return false
	}
	reason := modelRateLimitEntryReason(entry)
	return reason == laneReasonPrefix+boardName || reason == laneSuppressPrefix+boardName
}

func activeModelRateLimit(entry any, now time.Time) bool {
	resetAt := modelRateLimitResetAt(entry)
	return resetAt != nil && resetAt.After(now)
}

func modelRateLimitResetAt(entry any) *time.Time {
	var raw string
	switch limit := entry.(type) {
	case string:
		raw = limit
	case map[string]any:
		raw, _ = limit["rate_limit_reset_at"].(string)
	case map[string]string:
		raw = limit["rate_limit_reset_at"]
	default:
		return nil
	}
	at, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
	if err != nil {
		return nil
	}
	return &at
}

func copyExtraWithModelRateLimits(extra map[string]any) (map[string]any, map[string]any) {
	copyExtra := make(map[string]any, len(extra)+1)
	for key, value := range extra {
		copyExtra[key] = value
	}

	limits := make(map[string]any)
	if current, ok := extra["model_rate_limits"].(map[string]any); ok {
		for key, value := range current {
			limits[key] = value
		}
	}
	return copyExtra, limits
}

func (c *Sub2APIClient) updateAccountExtra(ctx context.Context, id int64, extra map[string]any) error {
	var out SubAccount
	return c.do(ctx, http.MethodPut, fmt.Sprintf("/api/v1/admin/accounts/%d", id), map[string]any{"extra": extra}, &out)
}

// SetOwnedModelRateLimit writes a board-owned entry without overwriting an
// active entry managed by Sub2API or another board.
func (c *Sub2APIClient) SetOwnedModelRateLimit(ctx context.Context, id int64, model, boardName string, entry map[string]any) error {
	if !modelRateLimitEntryOwnedBy(entry, boardName) {
		return fmt.Errorf("model rate-limit entry does not belong to board %q", boardName)
	}
	unlock := c.lockAccount(id)
	defer unlock()
	if c.store != nil {
		return c.store.SetOwnedModelRateLimitAtomically(ctx, id, model, boardName, entry)
	}

	acc, err := c.GetAccount(ctx, id)
	if err != nil {
		return err
	}
	extra, limits := copyExtraWithModelRateLimits(acc.Extra)
	modelKeys := accountModelRateLimitKeys(acc, model)
	if len(modelKeys) == 0 {
		return fmt.Errorf("account %d model %q has no resolved model key", id, model)
	}
	reason := modelRateLimitEntryReason(entry)
	for _, modelKey := range modelKeys {
		if current, exists := limits[modelKey]; !exists {
			continue
		} else if !modelRateLimitEntryOwnedBy(current, boardName) {
			if resetAt := modelRateLimitResetAt(current); resetAt == nil || resetAt.After(time.Now()) {
				return fmt.Errorf("account %d model %q: %w", id, model, ErrForeignModelRateLimit)
			}
		} else if modelRateLimitEntryReason(current) == reason && activeModelRateLimit(current, time.Now()) {
			// The other keys may still need to be repaired below.
			continue
		}
	}
	changed := false
	for _, modelKey := range modelKeys {
		current, exists := limits[modelKey]
		if !exists || modelRateLimitEntryReason(current) != reason || !activeModelRateLimit(current, time.Now()) {
			limits[modelKey] = entry
			changed = true
		}
	}
	if !changed {
		return nil
	}
	extra["model_rate_limits"] = limits
	return c.updateAccountExtra(ctx, id, extra)
}

// ClearOwnedModelRateLimit removes only an entry written by this board. It
// deliberately leaves native and other-board entries untouched.
func (c *Sub2APIClient) ClearOwnedModelRateLimit(ctx context.Context, id int64, model, boardName string) (bool, error) {
	unlock := c.lockAccount(id)
	defer unlock()
	if c.store != nil {
		return c.store.ClearOwnedModelRateLimitAtomically(ctx, id, model, boardName)
	}

	acc, err := c.GetAccount(ctx, id)
	if err != nil {
		return false, err
	}
	extra, limits := copyExtraWithModelRateLimits(acc.Extra)
	cleared := false
	for _, modelKey := range accountModelRateLimitKeys(acc, model) {
		current, exists := limits[modelKey]
		if exists && modelRateLimitEntryOwnedBy(current, boardName) {
			delete(limits, modelKey)
			cleared = true
		}
	}
	if !cleared {
		return false, nil
	}
	extra["model_rate_limits"] = limits
	if err := c.updateAccountExtra(ctx, id, extra); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Sub2APIClient) ClearAllOwnedModelRateLimits(ctx context.Context, id int64, boardName string) (int, error) {
	unlock := c.lockAccount(id)
	defer unlock()
	if c.store != nil {
		return c.store.ClearAllOwnedModelRateLimitsAtomically(ctx, id, boardName)
	}

	acc, err := c.GetAccount(ctx, id)
	if err != nil {
		return 0, err
	}
	extra, limits := copyExtraWithModelRateLimits(acc.Extra)
	cleared := 0
	for model, entry := range limits {
		if modelRateLimitEntryOwnedBy(entry, boardName) {
			delete(limits, model)
			cleared++
		}
	}
	if cleared == 0 {
		return 0, nil
	}
	extra["model_rate_limits"] = limits
	if err := c.updateAccountExtra(ctx, id, extra); err != nil {
		return 0, err
	}
	return cleared, nil
}

// TestAccount 真实调用测试账号（POST /admin/accounts/:id/test）。
// Sub2API 会在成功测试后清空该账号全部 model_rate_limits，因此本客户端
// 会在测试完成后原子恢复测试前仍存在的泳道图 owner 条目；原生限流由测试成功语义处理。
func (c *Sub2APIClient) TestAccount(ctx context.Context, id int64) (bool, string, error) {
	return c.TestAccountModel(ctx, id, "")
}

// TestAccountModel 用指定模型真实调用测试账号，并原子保留测试前的泳道 owner 条目。
func (c *Sub2APIClient) TestAccountModel(ctx context.Context, id int64, modelID string) (bool, string, error) {
	unlock := c.lockAccount(id)
	defer unlock()

	before, err := c.GetAccount(ctx, id)
	if err != nil {
		return false, "", err
	}

	ok, msg, err := c.testAccountModel(ctx, id, modelID)
	if err != nil || !ok {
		return ok, msg, err
	}
	if c.store != nil {
		beforeLimits, _ := before.Extra["model_rate_limits"].(map[string]any)
		ownedLimits := ownedModelRateLimitEntries(beforeLimits)
		if err := c.store.MergeModelRateLimitsAtomically(ctx, id, ownedLimits, time.Now()); err != nil {
			return false, "restore model rate limits after test: " + err.Error(), err
		}
	} else if err := c.restoreModelRateLimits(ctx, id, before.Extra, time.Now()); err != nil {
		return false, "restore model rate limits after test: " + err.Error(), err
	}
	return true, msg, nil
}

func ownedModelRateLimitEntries(entries map[string]any) map[string]any {
	owned := make(map[string]any)
	for model, entry := range entries {
		if strings.HasPrefix(modelRateLimitEntryReason(entry), laneReasonPrefix) {
			owned[model] = entry
		}
	}
	return owned
}

func (c *Sub2APIClient) restoreModelRateLimits(ctx context.Context, id int64, beforeExtra map[string]any, now time.Time) error {
	beforeLimits, hadBeforeLimits := beforeExtra["model_rate_limits"].(map[string]any)
	if !hadBeforeLimits || len(beforeLimits) == 0 {
		return nil
	}

	after, err := c.GetAccount(ctx, id)
	if err != nil {
		return err
	}
	extra, afterLimits := copyExtraWithModelRateLimits(after.Extra)
	changed := false
	for model, entry := range beforeLimits {
		if !strings.HasPrefix(modelRateLimitEntryReason(entry), laneReasonPrefix) {
			continue
		}
		// Expired scheduler limits must not be resurrected by a successful probe.
		if _, exists := afterLimits[model]; !exists && activeModelRateLimit(entry, now) {
			afterLimits[model] = entry
			changed = true
		}
	}
	if !changed {
		return nil
	}
	extra["model_rate_limits"] = afterLimits
	return c.updateAccountExtra(ctx, id, extra)
}

func (c *Sub2APIClient) testAccountModel(ctx context.Context, id int64, modelID string) (bool, string, error) {
	body := map[string]string{
		"model_id": modelID,
		"prompt":   "ping",
		"mode":     "",
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return false, "", fmt.Errorf("marshal test request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/api/v1/admin/accounts/%d/test", c.BaseURL, id), bytes.NewReader(payload))
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

	// Continue draining after test_complete. Sub2API performs recovery after
	// emitting that event and before closing the stream.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var lastErr string
	complete := false
	completeOK := false
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
		}
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			continue
		}
		switch ev.Type {
		case "test_complete":
			complete = true
			completeOK = ev.Success
			if ev.Error != "" {
				lastErr = ev.Error
			}
		case "test_error":
			if ev.Error != "" {
				lastErr = ev.Error
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return false, lastErr, fmt.Errorf("sse read: %w", err)
	}
	if !complete {
		if lastErr == "" {
			lastErr = "SSE 流未收到 test_complete"
		}
		return false, lastErr, nil
	}
	return completeOK, lastErr, nil
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
