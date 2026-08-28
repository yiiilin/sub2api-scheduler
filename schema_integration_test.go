//go:build integration

package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLegacyLaneSchemaMigration(t *testing.T) {
	// This test drops and recreates tables; use only a disposable database.
	dsn := os.Getenv("TEST_DATABASE_DSN")
	if dsn == "" {
		t.Skip("TEST_DATABASE_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open migration database: %v", err)
	}
	defer pool.Close()

	legacySchema := `
DROP TABLE IF EXISTS lane_account_states CASCADE;
DROP TABLE IF EXISTS lane_boards_lanes CASCADE;
DROP TABLE IF EXISTS lane_boards CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS switch_history CASCADE;
DROP TABLE IF EXISTS scheduler_outbox CASCADE;
CREATE TABLE scheduler_outbox (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    account_id BIGINT,
    group_id BIGINT,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE accounts (
    id BIGINT PRIMARY KEY,
    platform TEXT NOT NULL DEFAULT 'openai',
    type TEXT NOT NULL DEFAULT 'apikey',
    schedulable BOOLEAN NOT NULL DEFAULT true,
    status TEXT NOT NULL DEFAULT 'active',
    rate_limit_reset_at TIMESTAMPTZ,
    overload_until TIMESTAMPTZ,
    temp_unschedulable_until TIMESTAMPTZ,
    auto_pause_on_expired BOOLEAN NOT NULL DEFAULT true,
    expires_at TIMESTAMPTZ,
    credentials JSONB,
    extra JSONB,
    deleted_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO accounts (id, platform, credentials, extra)
VALUES (11, 'openai', '{"model_mapping":{"requested-model":"mapped-model"}}',
        '{"model_rate_limits":{"mapped-model":{"reason":"upstream:429","rate_limit_reset_at":"2099-01-01T00:00:00Z"}}}'),
       (12, 'antigravity', '{"model_mapping":{"requested-model":"gemini-3-pro"}}',
        '{"model_rate_limits":{"antigravity:gemini":{"reason":"upstream:429","rate_limit_reset_at":"2099-01-02T00:00:00Z"}}}'),
       (13, 'openai', '{"model_mapping":{"requested-model":"mapped-model"}}', '{}'),
        (14, 'openai', '{"model_mapping":{"requested-model":"mapped-model"}}', '{}'),
       (15, 'openai', '{"model_mapping":{"requested-model":"mapped-model"}}', '{"quota_limit":1,"quota_used":1}');
UPDATE accounts SET expires_at='2020-01-01T00:00:00Z', auto_pause_on_expired=true WHERE id=14;
CREATE TABLE lane_boards (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    model TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    fail_threshold INT NOT NULL DEFAULT 3,
    window_seconds INT NOT NULL DEFAULT 60,
    probe_interval INT NOT NULL DEFAULT 30,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE lane_boards_lanes (
    board_id BIGINT NOT NULL REFERENCES lane_boards(id) ON DELETE CASCADE,
    position INT NOT NULL,
    name TEXT NOT NULL,
    account_ids BIGINT[] NOT NULL,
    PRIMARY KEY (board_id, position)
);
CREATE TABLE lane_account_states (
    board_id BIGINT NOT NULL REFERENCES lane_boards(id) ON DELETE CASCADE,
    account_id BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'healthy',
    fail_count INT NOT NULL DEFAULT 0,
    disabled_at TIMESTAMPTZ,
    checked_at TIMESTAMPTZ,
    last_probe_at TIMESTAMPTZ,
    last_probe_ok BOOLEAN,
    last_probe_msg TEXT,
    PRIMARY KEY (board_id, account_id)
);
INSERT INTO lane_boards (name, model) VALUES ('legacy-board', 'flash-v1');
INSERT INTO lane_boards_lanes (board_id, position, name, account_ids)
VALUES (1, 0, 'legacy-lane', '{11,12,NULL,0,-1}');`
	if _, err := pool.Exec(ctx, legacySchema); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}

	db, err := NewDB(ctx, dsn)
	if err != nil {
		t.Fatalf("migrate legacy schema: %v", err)
	}
	defer db.Close()

	board, err := db.GetBoard(ctx, 1)
	if err != nil {
		t.Fatalf("read migrated board: %v", err)
	}
	if len(board.Lanes) != 1 || board.Lanes[0].ID <= 0 {
		t.Fatalf("legacy lane id was not backfilled: %#v", board.Lanes)
	}
	states, err := db.GetAccountStates(ctx, 1)
	if err != nil {
		t.Fatalf("read migrated states: %v", err)
	}
	for _, accountID := range []int64{11, 12} {
		state, ok := states[accountID]
		if !ok || state.State != LaneStateHealthy || state.LastProbeMsg != "" {
			t.Fatalf("account %d state not backfilled: %#v", accountID, state)
		}
	}
	if len(states) != 2 {
		t.Fatalf("unexpected state rows after NULL array element: %#v", states)
	}
	blocks, err := db.GetExternalBlocks(ctx, []int64{11, 12}, "requested-model", "legacy-board")
	if err != nil {
		t.Fatalf("read mapped external blocks: %v", err)
	}
	if blocks[11].NativeCoolUntil == nil {
		t.Fatalf("mapped model cooldown was not detected: %#v", blocks[11])
	}
	if blocks[12].NativeCoolUntil == nil {
		t.Fatalf("provider family cooldown was not detected: %#v", blocks[12])
	}
	extraBlocks, err := db.GetExternalBlocks(ctx, []int64{14, 15}, "requested-model", "legacy-board")
	if err != nil {
		t.Fatalf("read expiry/quota external blocks: %v", err)
	}
	if !extraBlocks[14].Expired {
		t.Fatalf("expired account was not blocked: %#v", extraBlocks[14])
	}
	if !extraBlocks[15].QuotaExceeded {
		t.Fatalf("quota account was not blocked: %#v", extraBlocks[15])
	}
	entry := map[string]any{
		"reason":              "lane_board:legacy-board",
		"rate_limit_reset_at": "2099-12-31T23:59:59Z",
	}
	if err := db.SetOwnedModelRateLimitAtomically(ctx, 13, "requested-model", "legacy-board", entry); err != nil {
		t.Fatalf("atomic set model limit: %v", err)
	}
	var stored []byte
	if err := db.pool.QueryRow(ctx, `SELECT extra->'model_rate_limits'->'mapped-model' FROM accounts WHERE id=13`).Scan(&stored); err != nil {
		t.Fatalf("read atomic model limit: %v", err)
	}
	if len(stored) == 0 {
		t.Fatal("atomic model limit was not stored under mapped key")
	}
	var outboxCount int
	if err := db.pool.QueryRow(ctx, `SELECT count(*) FROM scheduler_outbox WHERE account_id=13`).Scan(&outboxCount); err != nil {
		t.Fatalf("read scheduler outbox: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("outbox rows = %d, want 1", outboxCount)
	}
	cleared, err := db.ClearAllOwnedModelRateLimitsAtomically(ctx, 13, "legacy-board")
	if err != nil || cleared != 1 {
		t.Fatalf("atomic clear = %d, err=%v; want 1", cleared, err)
	}
	if _, err := db.pool.Exec(ctx, `INSERT INTO lane_boards (name, model) VALUES ('duplicate-model', 'flash-v1')`); err == nil {
		t.Fatal("model uniqueness constraint was not installed")
	}
	var nextLaneID int64
	if err := db.pool.QueryRow(ctx, `
INSERT INTO lane_boards_lanes (board_id, position, name, account_ids)
VALUES (1, 1, 'second-lane', '{13}') RETURNING id`).Scan(&nextLaneID); err != nil {
		t.Fatalf("insert migrated lane: %v", err)
	}
	if nextLaneID <= board.Lanes[0].ID {
		t.Fatalf("lane sequence was not advanced: first=%d next=%d", board.Lanes[0].ID, nextLaneID)
	}
	second, err := NewDB(ctx, dsn)
	if err == nil {
		second.Close()
		t.Fatal("second scheduler instance acquired the database lock")
	}
}
