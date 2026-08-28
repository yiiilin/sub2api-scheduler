package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestMappedModelRateLimitKeyUsesExactAndLongestWildcard(t *testing.T) {
	credentials := map[string]any{
		"model_mapping": map[string]any{
			"claude-*":        "claude-default",
			"claude-sonnet-*": "claude-sonnet-upstream",
			"exact-model":     "exact-upstream",
		},
	}
	if got := mappedModelRateLimitKey(credentials, "exact-model"); got != "exact-upstream" {
		t.Fatalf("exact mapping = %q, want exact-upstream", got)
	}
	if got := mappedModelRateLimitKey(credentials, "claude-sonnet-4"); got != "claude-sonnet-upstream" {
		t.Fatalf("longest wildcard mapping = %q, want claude-sonnet-upstream", got)
	}
	if got := mappedModelRateLimitKey(credentials, "other-model"); got != "other-model" {
		t.Fatalf("unmapped model = %q, want other-model", got)
	}
}

func TestDefaultModelMappingsMatchSub2API(t *testing.T) {
	antigravity := &SubAccount{Platform: "antigravity"}
	if got := mappedModelRateLimitKeyForAccount(antigravity, "claude-opus-4-6"); got != "claude-opus-4-6-thinking" {
		t.Fatalf("Antigravity default mapping = %q", got)
	}
	if !modelMappingSupportsRequestedModelForAccount("antigravity", "oauth", nil, "gemini-3-pro-preview") {
		t.Fatal("Antigravity default model was rejected")
	}
	if modelMappingSupportsRequestedModelForAccount("antigravity", "oauth", nil, "custom-unknown-model") {
		t.Fatal("unsupported Antigravity model was accepted")
	}

	grok := &SubAccount{Platform: "grok"}
	if got := mappedModelRateLimitKeyForAccount(grok, "grok-build"); got != "grok-build-0.1" {
		t.Fatalf("Grok default mapping = %q", got)
	}
	if got := mappedModelRateLimitKeyForAccount(grok, "xai/grok-4.6"); got != "grok-4.6" {
		t.Fatalf("Grok provider-prefixed mapping = %q", got)
	}
	googleOneCredentials := map[string]any{"oauth_type": "google_one"}
	if !modelMappingSupportsRequestedModelForAccount("gemini", "oauth", googleOneCredentials, "gemini-2.5-pro") {
		t.Fatal("Google One supported model was rejected")
	}
	if modelMappingSupportsRequestedModelForAccount("gemini", "oauth", googleOneCredentials, "gemini-3.1-pro") {
		t.Fatal("Google One unsupported model was accepted")
	}
	if !modelMappingSupportsRequestedModelForAccount("openai", "apikey", nil, "custom-model") {
		t.Fatal("ordinary pass-through account was rejected")
	}
	if !modelMappingSupportsRequestedModelForAccount("openai", "oauth", map[string]any{
		"openai_passthrough": true,
		"model_mapping":      map[string]any{"known-model": "mapped-model"},
	}, "gemini-2.5-pro") {
		t.Fatal("OpenAI passthrough account was rejected because of stale mapping")
	}
	if !modelMappingSupportsRequestedModelForAccount("anthropic", "bedrock", nil, "claude-sonnet-4-5") {
		t.Fatal("Bedrock default Claude model was rejected")
	}
	if modelMappingSupportsRequestedModelForAccount("anthropic", "bedrock", nil, "not-a-bedrock-model") {
		t.Fatal("invalid Bedrock model was accepted")
	}
}

func TestAccountModelRateLimitKeysIncludeProviderFamilyKeys(t *testing.T) {
	account := &SubAccount{
		Platform: "antigravity",
		Credentials: map[string]any{
			"model_mapping": map[string]any{"gemini-alias": "gemini-3-pro"},
		},
	}
	keys := accountModelRateLimitKeys(account, "gemini-alias")
	if !reflect.DeepEqual(keys, []string{"gemini-3-pro", "antigravity:gemini"}) {
		t.Fatalf("rate-limit keys = %#v", keys)
	}
	resourcePathAccount := &SubAccount{Platform: "antigravity", Credentials: map[string]any{
		"model_mapping": map[string]any{"gemini-alias": "publishers/google/models/gemini-3-pro"},
	}}
	resourceKeys := accountModelRateLimitKeys(resourcePathAccount, "gemini-alias")
	if !reflect.DeepEqual(resourceKeys, []string{"publishers/google/models/gemini-3-pro", "antigravity:gemini"}) {
		t.Fatalf("resource-path rate-limit keys = %#v", resourceKeys)
	}
	openAIKeys := accountModelRateLimitKeys(&SubAccount{Platform: "openai"}, "gpt-image-2")
	if !reflect.DeepEqual(openAIKeys, []string{"gpt-image-2", "openai:image_generation"}) {
		t.Fatalf("OpenAI image rate-limit keys = %#v", openAIKeys)
	}
}

func TestModelRateLimitResetAtSupportsLegacyStringEntry(t *testing.T) {
	if got := modelRateLimitResetAt("2099-01-01T00:00:00Z"); got == nil {
		t.Fatal("legacy string model limit was not parsed")
	}
	if modelRateLimitEntryOwnedBy("2099-01-01T00:00:00Z", "board-a") {
		t.Fatal("legacy string entry was incorrectly treated as board-owned")
	}
}

func TestSub2APIClientDoRejectsEnvelopeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":409,"message":"account update rejected"}`))
	}))
	defer server.Close()

	client := NewSub2APIClient(server.URL, "test-key")
	err := client.do(context.Background(), http.MethodPut, "/api/v1/admin/accounts/1", map[string]any{"extra": map[string]any{}}, &SubAccount{})
	if err == nil {
		t.Fatal("expected a non-zero Sub2API envelope code to return an error")
	}
	if !strings.Contains(err.Error(), "409") || !strings.Contains(err.Error(), "account update rejected") {
		t.Fatalf("unexpected envelope error: %v", err)
	}
}

func TestSub2APIClientDoRejectsEnvelopeWithoutData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":null}`))
	}))
	defer server.Close()

	err := NewSub2APIClient(server.URL, "test-key").do(
		context.Background(), http.MethodGet, "/api/v1/admin/accounts/1", nil, &SubAccount{},
	)
	if err == nil || !strings.Contains(err.Error(), "missing data") {
		t.Fatalf("missing envelope data error = %v", err)
	}
}

func TestSub2APIClientDoClassifiesMissingAccount(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	err := NewSub2APIClient(server.URL, "test-key").do(
		context.Background(), http.MethodGet, "/api/v1/admin/accounts/42", nil, &SubAccount{},
	)
	if !errors.Is(err, ErrSub2APIAccountNotFound) {
		t.Fatalf("missing account error = %v", err)
	}
}

type accountAPIFake struct {
	mu                sync.Mutex
	extra             map[string]any
	credentials       map[string]any
	puts              int
	clearLimitsOnTest bool
}

func (f *accountAPIFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{"id": 1, "credentials": f.credentials, "extra": f.extra},
		})
	case http.MethodPost:
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"test_complete\",\"success\":true}\n\n"))
		if f.clearLimitsOnTest {
			delete(f.extra, "model_rate_limits")
		}
	case http.MethodPut:
		var body struct {
			Extra map[string]any `json:"extra"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.extra = body.Extra
		f.puts++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{"id": 1, "credentials": f.credentials, "extra": f.extra},
		})
	default:
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
	}
}

func TestSub2APIClientListAccountsIncludesAllPlatforms(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/accounts" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		if r.URL.Query().Get("platform") != "" {
			t.Errorf("unexpected platform filter: %q", r.URL.Query().Get("platform"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{
				"items": []map[string]any{{"id": 1, "platform": "openai"}, {"id": 2, "platform": "anthropic"}},
				"total": 2,
			},
		})
	}))
	defer server.Close()

	accounts, err := NewSub2APIClient(server.URL, "test-key").ListAccounts(context.Background())
	if err != nil {
		t.Fatalf("list all accounts: %v", err)
	}
	if len(accounts) != 2 || accounts[1].Platform != "anthropic" {
		t.Fatalf("accounts = %#v, want all platforms", accounts)
	}
}

func TestClearOwnedModelRateLimitPreservesForeignEntry(t *testing.T) {
	fake := &accountAPIFake{extra: map[string]any{
		"model_rate_limits": map[string]any{
			"flash-v1": map[string]any{
				"reason":              "upstream:429",
				"rate_limit_reset_at": "2099-01-01T00:00:00Z",
			},
		},
	}}
	server := httptest.NewServer(fake)
	defer server.Close()

	client := NewSub2APIClient(server.URL, "test-key")
	cleared, err := client.ClearOwnedModelRateLimit(context.Background(), 1, "flash-v1", "board-a")
	if err != nil {
		t.Fatalf("clear owned model rate limit: %v", err)
	}
	if cleared {
		t.Fatal("foreign model rate-limit entry must not be cleared")
	}
	if fake.puts != 0 {
		t.Fatalf("foreign model rate-limit entry triggered %d PUT requests", fake.puts)
	}
}

func TestClearOwnedModelRateLimitClearsOnlyMatchingEntry(t *testing.T) {
	fake := &accountAPIFake{extra: map[string]any{
		"quota_used": 12,
		"model_rate_limits": map[string]any{
			"flash-v1": map[string]any{
				"reason":              "lane_board:board-a",
				"rate_limit_reset_at": "2099-01-01T00:00:00Z",
			},
			"other-model": map[string]any{
				"reason":              "upstream:429",
				"rate_limit_reset_at": "2099-01-01T00:00:00Z",
			},
		},
	}}
	server := httptest.NewServer(fake)
	defer server.Close()

	client := NewSub2APIClient(server.URL, "test-key")
	cleared, err := client.ClearOwnedModelRateLimit(context.Background(), 1, "flash-v1", "board-a")
	if err != nil {
		t.Fatalf("clear owned model rate limit: %v", err)
	}
	if !cleared {
		t.Fatal("expected the board-owned entry to be cleared")
	}
	if fake.puts != 1 {
		t.Fatalf("got %d PUT requests, want 1", fake.puts)
	}
	limits := fake.extra["model_rate_limits"].(map[string]any)
	if _, exists := limits["flash-v1"]; exists {
		t.Fatal("target model entry was not removed")
	}
	if _, exists := limits["other-model"]; !exists {
		t.Fatal("unrelated model entry was removed")
	}
	if quota, ok := fake.extra["quota_used"].(float64); !ok || quota != 12 {
		t.Fatalf("unrelated extra field was lost or changed: %#v", fake.extra["quota_used"])
	}
}

func TestSetOwnedModelRateLimitUsesMappedModelKey(t *testing.T) {
	fake := &accountAPIFake{
		credentials: map[string]any{
			"model_mapping": map[string]any{"requested-model": "mapped-model"},
		},
		extra: map[string]any{},
	}
	server := httptest.NewServer(fake)
	defer server.Close()

	client := NewSub2APIClient(server.URL, "test-key")
	err := client.SetOwnedModelRateLimit(context.Background(), 1, "requested-model", "board-a", map[string]any{
		"reason":              "lane_board:board-a",
		"rate_limit_reset_at": "2099-12-31T23:59:59Z",
	})
	if err != nil {
		t.Fatalf("set mapped model limit: %v", err)
	}
	limits, ok := fake.extra["model_rate_limits"].(map[string]any)
	if !ok {
		t.Fatalf("model limits missing: %#v", fake.extra["model_rate_limits"])
	}
	if _, exists := limits["mapped-model"]; !exists {
		t.Fatalf("mapped key missing: %#v", limits)
	}
	if _, exists := limits["requested-model"]; exists {
		t.Fatalf("request key was used instead of mapped key: %#v", limits)
	}
}

func TestClearAllOwnedModelRateLimitsHandlesMappedAndLegacyKeys(t *testing.T) {
	fake := &accountAPIFake{extra: map[string]any{
		"model_rate_limits": map[string]any{
			"requested-model": map[string]any{"reason": "lane_board:board-a", "rate_limit_reset_at": "2099-01-01T00:00:00Z"},
			"mapped-model":    map[string]any{"reason": "lane_board:suppressed:board-a", "rate_limit_reset_at": "2099-01-01T00:00:00Z"},
			"foreign-model":   map[string]any{"reason": "upstream:429", "rate_limit_reset_at": "2099-01-01T00:00:00Z"},
		},
	}}
	server := httptest.NewServer(fake)
	defer server.Close()

	client := NewSub2APIClient(server.URL, "test-key")
	cleared, err := client.ClearAllOwnedModelRateLimits(context.Background(), 1, "board-a")
	if err != nil {
		t.Fatalf("clear all owned limits: %v", err)
	}
	if cleared != 2 {
		t.Fatalf("cleared %d entries, want 2", cleared)
	}
	limits := fake.extra["model_rate_limits"].(map[string]any)
	if len(limits) != 1 {
		t.Fatalf("remaining limits = %#v", limits)
	}
	if _, exists := limits["foreign-model"]; !exists {
		t.Fatal("foreign limit was removed")
	}
}

func TestSetOwnedModelRateLimitDoesNotReplaceForeignActiveEntry(t *testing.T) {
	fake := &accountAPIFake{extra: map[string]any{
		"model_rate_limits": map[string]any{
			"flash-v1": map[string]any{
				"reason":              "upstream:429",
				"rate_limit_reset_at": "2099-01-01T00:00:00Z",
			},
		},
	}}
	server := httptest.NewServer(fake)
	defer server.Close()

	client := NewSub2APIClient(server.URL, "test-key")
	err := client.SetOwnedModelRateLimit(context.Background(), 1, "flash-v1", "board-a", map[string]any{
		"reason":              "lane_board:board-a",
		"rate_limit_reset_at": "2099-12-31T23:59:59Z",
	})
	if !errors.Is(err, ErrForeignModelRateLimit) {
		t.Fatalf("got %v, want ErrForeignModelRateLimit", err)
	}
	if fake.puts != 0 {
		t.Fatalf("foreign model rate-limit entry triggered %d PUT requests", fake.puts)
	}
}

func TestSetOwnedModelRateLimitOverwritesExpiredForeignEntry(t *testing.T) {
	fake := &accountAPIFake{extra: map[string]any{
		"model_rate_limits": map[string]any{
			"flash-v1": map[string]any{
				"reason":              "upstream:429",
				"rate_limit_reset_at": "2020-01-01T00:00:00Z",
			},
		},
	}}
	server := httptest.NewServer(fake)
	defer server.Close()

	client := NewSub2APIClient(server.URL, "test-key")
	if err := client.SetOwnedModelRateLimit(context.Background(), 1, "flash-v1", "board-a", map[string]any{
		"reason":              "lane_board:board-a",
		"rate_limit_reset_at": "2099-12-31T23:59:59Z",
	}); err != nil {
		t.Fatalf("set board limit over expired foreign entry: %v", err)
	}
	if fake.puts != 1 {
		t.Fatalf("got %d PUT requests, want 1", fake.puts)
	}
	limits := fake.extra["model_rate_limits"].(map[string]any)
	if modelRateLimitEntryReason(limits["flash-v1"]) != "lane_board:board-a" {
		t.Fatalf("entry reason = %q", modelRateLimitEntryReason(limits["flash-v1"]))
	}
}

func TestSetOwnedModelRateLimitIsIdempotentForSameReason(t *testing.T) {
	fake := &accountAPIFake{extra: map[string]any{
		"model_rate_limits": map[string]any{
			"flash-v1": map[string]any{
				"reason":              "lane_board:board-a",
				"rate_limited_at":     "2026-01-01T00:00:00Z",
				"rate_limit_reset_at": "2099-12-31T23:59:59Z",
			},
		},
	}}
	server := httptest.NewServer(fake)
	defer server.Close()

	client := NewSub2APIClient(server.URL, "test-key")
	err := client.SetOwnedModelRateLimit(context.Background(), 1, "flash-v1", "board-a", map[string]any{
		"reason":              "lane_board:board-a",
		"rate_limited_at":     "2026-02-01T00:00:00Z",
		"rate_limit_reset_at": "2099-12-31T23:59:59Z",
	})
	if err != nil {
		t.Fatalf("idempotent owned limit: %v", err)
	}
	if fake.puts != 0 {
		t.Fatalf("same owned reason triggered %d PUT requests", fake.puts)
	}
}

func TestAccountModelRestoresModelLimitsAfterSuccessfulTest(t *testing.T) {
	fake := &accountAPIFake{
		clearLimitsOnTest: true,
		extra: map[string]any{
			"model_rate_limits": map[string]any{
				"flash-v1": map[string]any{
					"reason":              "lane_board:board-a",
					"rate_limit_reset_at": "2099-12-31T23:59:59Z",
				},
				"other-model": map[string]any{
					"reason":              "upstream:429",
					"rate_limit_reset_at": "2099-01-01T00:00:00Z",
				},
			},
		},
	}
	server := httptest.NewServer(fake)
	defer server.Close()

	client := NewSub2APIClient(server.URL, "test-key")
	ok, msg, err := client.TestAccountModel(context.Background(), 1, "flash-v1")
	if err != nil || !ok {
		t.Fatalf("test account: ok=%v msg=%q err=%v", ok, msg, err)
	}
	if fake.puts != 1 {
		t.Fatalf("got %d PUT requests, want 1 restoration PUT", fake.puts)
	}
	limits, ok := fake.extra["model_rate_limits"].(map[string]any)
	if !ok || len(limits) != 1 {
		t.Fatalf("model limits were not restored: %#v", fake.extra["model_rate_limits"])
	}
	if _, exists := limits["flash-v1"]; !exists {
		t.Fatal("board-owned model limit was not restored")
	}
	if _, exists := limits["other-model"]; exists {
		t.Fatal("foreign model limit was resurrected")
	}
}

func TestAccountModelDoesNotRestoreExpiredModelLimit(t *testing.T) {
	fake := &accountAPIFake{
		clearLimitsOnTest: true,
		extra: map[string]any{
			"model_rate_limits": map[string]any{
				"flash-v1": map[string]any{
					"reason":              "upstream:429",
					"rate_limit_reset_at": "2020-01-01T00:00:00Z",
				},
			},
		},
	}
	server := httptest.NewServer(fake)
	defer server.Close()

	ok, _, err := NewSub2APIClient(server.URL, "test-key").TestAccountModel(context.Background(), 1, "flash-v1")
	if err != nil || !ok {
		t.Fatalf("test account: ok=%v err=%v", ok, err)
	}
	if fake.puts != 0 {
		t.Fatalf("expired model limit was restored with %d PUT requests", fake.puts)
	}
}
