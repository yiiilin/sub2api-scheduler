package main

import (
	"reflect"
	"testing"
	"time"
)

func TestPlanBoardCleanup(t *testing.T) {
	oldBoard := &LaneBoard{
		ID:    7,
		Name:  "primary",
		Model: "flash-v1",
		Lanes: []Lane{
			{AccountIDs: []int64{11, 12}},
			{AccountIDs: []int64{13}},
		},
	}

	tests := []struct {
		name string
		next *LaneBoard
		want []BoardCleanupOp
	}{
		{
			name: "delete clears every account",
			next: nil,
			want: []BoardCleanupOp{
				{AccountID: 11, Model: "flash-v1", BoardName: "primary"},
				{AccountID: 12, Model: "flash-v1", BoardName: "primary"},
				{AccountID: 13, Model: "flash-v1", BoardName: "primary"},
			},
		},
		{
			name: "removed accounts are cleared",
			next: &LaneBoard{
				ID:      7,
				Name:    "primary",
				Model:   "flash-v1",
				Enabled: true,
				Lanes:   []Lane{{AccountIDs: []int64{11}}},
			},
			want: []BoardCleanupOp{
				{AccountID: 12, Model: "flash-v1", BoardName: "primary"},
				{AccountID: 13, Model: "flash-v1", BoardName: "primary"},
			},
		},
		{
			name: "rename clears previous ownership",
			next: &LaneBoard{
				ID:    7,
				Name:  "renamed",
				Model: "flash-v1",
				Lanes: oldBoard.Lanes,
			},
			want: []BoardCleanupOp{
				{AccountID: 11, Model: "flash-v1", BoardName: "primary"},
				{AccountID: 12, Model: "flash-v1", BoardName: "primary"},
				{AccountID: 13, Model: "flash-v1", BoardName: "primary"},
			},
		},
		{
			name: "model change clears previous model",
			next: &LaneBoard{
				ID:    7,
				Name:  "primary",
				Model: "reasoning-v2",
				Lanes: oldBoard.Lanes,
			},
			want: []BoardCleanupOp{
				{AccountID: 11, Model: "flash-v1", BoardName: "primary"},
				{AccountID: 12, Model: "flash-v1", BoardName: "primary"},
				{AccountID: 13, Model: "flash-v1", BoardName: "primary"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := planBoardCleanup(oldBoard, tt.next); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("cleanup plan = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExternalBlockAllowsProbe(t *testing.T) {
	now := time.Now()
	future := now.Add(time.Hour)
	tests := []struct {
		name  string
		block ExternalBlock
		want  bool
	}{
		{name: "healthy", block: ExternalBlock{Schedulable: true, Status: "active"}, want: true},
		{name: "only scheduler switch closed", block: ExternalBlock{Schedulable: false, Status: "active"}, want: true},
		{name: "recoverable error status", block: ExternalBlock{Schedulable: false, Status: "error"}, want: true},
		{name: "account rate limited", block: ExternalBlock{Schedulable: true, Status: "active", RateLimitUntil: &future}},
		{name: "overloaded", block: ExternalBlock{Schedulable: true, Status: "active", OverloadUntil: &future}},
		{name: "temporary unschedulable", block: ExternalBlock{Schedulable: true, Status: "active", TempUnschedUntil: &future}},
		{name: "native model cooldown", block: ExternalBlock{Schedulable: true, Status: "active", NativeCoolUntil: &future}},
		{name: "inactive", block: ExternalBlock{Schedulable: true, Status: "inactive"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := externalBlockAllowsProbe(tt.block, now); got != tt.want {
				t.Fatalf("externalBlockAllowsProbe() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExternalBlockIncludesExpiryAndQuota(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	if !(ExternalBlock{Schedulable: true, Status: "active", Expired: true}).blocked(now) {
		t.Fatal("expired account was treated as schedulable")
	}
	if !(ExternalBlock{Schedulable: true, Status: "error", Expired: true}).blocked(now) {
		t.Fatal("expired error account was treated as schedulable")
	}
	if externalBlockAllowsProbe(ExternalBlock{Schedulable: true, Status: "error", QuotaExceeded: true}, now) {
		t.Fatal("quota-exhausted error account was allowed to probe")
	}

	currentStart := now.Add(-time.Hour).Format(time.RFC3339)
	if !accountQuotaExceeded(map[string]any{
		"quota_limit":       10.0,
		"quota_used":        9.0,
		"quota_daily_limit": 5.0,
		"quota_daily_used":  5.0,
		"quota_daily_start": currentStart,
	}, now) {
		t.Fatal("current daily quota exhaustion was not detected")
	}
	if accountQuotaExceeded(map[string]any{
		"quota_daily_limit": 5.0,
		"quota_daily_used":  5.0,
		"quota_daily_start": now.Add(-25 * time.Hour).Format(time.RFC3339),
	}, now) {
		t.Fatal("expired daily quota was incorrectly treated as active")
	}
	if accountQuotaExceeded(map[string]any{
		"quota_weekly_limit":      10.0,
		"quota_weekly_used":       10.0,
		"quota_weekly_reset_mode": "fixed",
		"quota_weekly_reset_day":  1,
		"quota_weekly_reset_hour": 10,
		"quota_weekly_start":      "2026-08-24T09:00:00Z",
		"quota_reset_timezone":    "UTC",
	}, now) {
		t.Fatal("fixed weekly quota before reset was incorrectly detected")
	}
	if !accountQuotaExceeded(map[string]any{
		"quota_weekly_limit":      10.0,
		"quota_weekly_used":       10.0,
		"quota_weekly_reset_mode": "fixed",
		"quota_weekly_reset_day":  1,
		"quota_weekly_reset_hour": 10,
		"quota_weekly_start":      "2026-08-24T11:00:00Z",
		"quota_reset_timezone":    "UTC",
	}, now) {
		t.Fatal("fixed weekly quota after reset was not detected")
	}
}

func TestValidLaneAccountState(t *testing.T) {
	for _, state := range []string{LaneStateHealthy, LaneStateDisabled, LaneStateSuppressed} {
		if !validLaneAccountState(state) {
			t.Fatalf("state %q was rejected", state)
		}
	}
	for _, state := range []string{"", "unknown", "HEALTHY"} {
		if validLaneAccountState(state) {
			t.Fatalf("state %q was accepted", state)
		}
	}
}

func TestValidateBoardDefinition(t *testing.T) {
	existing := []LaneBoard{{ID: 9, Name: "existing", Model: "flash-v1"}}
	valid := &LaneBoard{
		ID:    10,
		Name:  "new-board",
		Model: "reasoning-v2",
		Lanes: []Lane{{AccountIDs: []int64{11, 12}}},
	}
	if err := validateBoardDefinition(valid, existing); err != nil {
		t.Fatalf("valid board rejected: %v", err)
	}

	tests := []struct {
		name  string
		board LaneBoard
	}{
		{name: "duplicate model", board: LaneBoard{ID: 10, Name: "duplicate-model", Model: "flash-v1", Lanes: []Lane{{AccountIDs: []int64{11}}}}},
		{name: "empty lanes", board: LaneBoard{ID: 10, Name: "empty", Model: "reasoning-v2"}},
		{name: "duplicate account", board: LaneBoard{ID: 10, Name: "duplicate-account", Model: "reasoning-v2", Lanes: []Lane{{AccountIDs: []int64{11}}, {AccountIDs: []int64{11}}}}},
		{name: "short window", board: LaneBoard{ID: 10, Name: "short-window", Model: "reasoning-v2", WindowSeconds: 9, Lanes: []Lane{{AccountIDs: []int64{11}}}}},
		{name: "short probe interval", board: LaneBoard{ID: 10, Name: "short-probe", Model: "reasoning-v2", ProbeInterval: 9, Lanes: []Lane{{AccountIDs: []int64{11}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateBoardDefinition(&tt.board, existing); err == nil {
				t.Fatal("expected board validation error")
			}
		})
	}
}
