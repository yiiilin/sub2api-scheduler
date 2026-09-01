//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type monitorAPIFake struct {
	mu         sync.Mutex
	probeOK    map[int64]bool
	probeCall  map[int64]int
	probeHook  map[int64]func()
	probeDelay map[int64]time.Duration
}

func newMonitorAPIFake() *monitorAPIFake {
	return &monitorAPIFake{
		probeOK:    make(map[int64]bool),
		probeCall:  make(map[int64]int),
		probeHook:  make(map[int64]func()),
		probeDelay: make(map[int64]time.Duration),
	}
}

func (f *monitorAPIFake) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probeOK = make(map[int64]bool)
	f.probeCall = make(map[int64]int)
	f.probeHook = make(map[int64]func())
	f.probeDelay = make(map[int64]time.Duration)
}

func (f *monitorAPIFake) setProbeResult(accountID int64, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probeOK[accountID] = ok
}

func (f *monitorAPIFake) setProbeHook(accountID int64, hook func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probeHook[accountID] = hook
}

func (f *monitorAPIFake) setProbeDelay(accountID int64, delay time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probeDelay[accountID] = delay
}

func (f *monitorAPIFake) calls(accountID int64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.probeCall[accountID]
}

func (f *monitorAPIFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 5 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "admin" || parts[3] != "accounts" {
		http.NotFound(w, r)
		return
	}
	accountID, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		http.Error(w, "invalid account id", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{
				"id": accountID, "name": fmt.Sprintf("account-%d", accountID),
				"platform": "openai", "type": "apikey", "status": "active",
				"schedulable": true, "credentials": map[string]any{}, "extra": map[string]any{},
			},
		})
	case http.MethodPost:
		f.mu.Lock()
		ok, configured := f.probeOK[accountID]
		if !configured {
			ok = true
		}
		hook := f.probeHook[accountID]
		delay := f.probeDelay[accountID]
		f.probeCall[accountID]++
		f.mu.Unlock()
		if hook != nil {
			hook()
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: {\"type\":\"test_complete\",\"success\":%t}\n\n", ok)
	case http.MethodPut:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{"id": accountID, "schedulable": true},
		})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func openMonitorIntegrationDB(t *testing.T) *DB {
	t.Helper()
	// These QA scenarios drop and recreate scheduler/Sub2API tables. The DSN
	// must point to a disposable database.
	dsn := os.Getenv("TEST_DATABASE_DSN")
	if dsn == "" {
		t.Skip("TEST_DATABASE_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open fixture database: %v", err)
	}
	fixtureSchema := `
DROP TABLE IF EXISTS lane_account_states CASCADE;
DROP TABLE IF EXISTS lane_boards_lanes CASCADE;
DROP TABLE IF EXISTS lane_boards CASCADE;
DROP TABLE IF EXISTS switch_history CASCADE;
DROP TABLE IF EXISTS scheduler_outbox CASCADE;
DROP TABLE IF EXISTS ops_error_logs CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
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
    name TEXT NOT NULL,
    platform TEXT NOT NULL DEFAULT 'openai',
    type TEXT NOT NULL DEFAULT 'apikey',
    priority INT NOT NULL DEFAULT 0,
    schedulable BOOLEAN NOT NULL DEFAULT true,
    status TEXT NOT NULL DEFAULT 'active',
    temp_unschedulable_until TIMESTAMPTZ,
    rate_limit_reset_at TIMESTAMPTZ,
    overload_until TIMESTAMPTZ,
    auto_pause_on_expired BOOLEAN NOT NULL DEFAULT false,
    expires_at TIMESTAMPTZ,
    error_message TEXT,
    notes TEXT,
    rate_multiplier DOUBLE PRECISION,
    load_factor DOUBLE PRECISION,
    credentials JSONB NOT NULL DEFAULT '{}'::jsonb,
    extra JSONB NOT NULL DEFAULT '{}'::jsonb,
    deleted_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE ops_error_logs (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL,
    requested_model TEXT NOT NULL,
    error_phase TEXT NOT NULL,
    upstream_status_code INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`
	if _, err := pool.Exec(ctx, fixtureSchema); err != nil {
		pool.Close()
		t.Fatalf("create monitor fixture schema: %v", err)
	}
	pool.Close()

	db, err := NewDB(ctx, dsn)
	if err != nil {
		t.Fatalf("initialize monitor database: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

func resetMonitorScenario(t *testing.T, db *DB, fake *monitorAPIFake) {
	t.Helper()
	fake.reset()
	ctx := context.Background()
	_, err := db.pool.Exec(ctx, `
TRUNCATE scheduler_outbox, switch_history, ops_error_logs;
DELETE FROM lane_boards;
DELETE FROM accounts;
INSERT INTO accounts (id, name, priority) VALUES
    (1, 'primary-a', 1), (2, 'primary-b', 2),
    (3, 'backup-a', 3), (4, 'backup-b', 4);`)
	if err != nil {
		t.Fatalf("reset monitor scenario: %v", err)
	}
}

func createMonitorBoard(t *testing.T, db *DB, monitor *LaneBoardMonitor, lanes ...[]int64) *LaneBoard {
	t.Helper()
	board := &LaneBoard{
		Name: "qa-board", Model: "qa-model", Enabled: true,
		FailThreshold: 2, WindowSeconds: 60, ProbeInterval: 10,
	}
	for i, accountIDs := range lanes {
		board.Lanes = append(board.Lanes, Lane{Name: fmt.Sprintf("lane-%d", i+1), AccountIDs: accountIDs})
	}
	if err := db.SaveBoard(context.Background(), board); err != nil {
		t.Fatalf("save QA board: %v", err)
	}
	if _, err := monitor.reconcileBoard(context.Background(), board); err != nil {
		t.Fatalf("initial board reconcile: %v", err)
	}
	return board
}

func addModelFailures(t *testing.T, db *DB, accountID int64, count int) {
	t.Helper()
	for range count {
		if _, err := db.pool.Exec(context.Background(), `
INSERT INTO ops_error_logs (account_id, requested_model, error_phase, upstream_status_code)
VALUES ($1, 'qa-model', 'upstream', 503)`, accountID); err != nil {
			t.Fatalf("insert model failure: %v", err)
		}
	}
}

func requireAccountStates(t *testing.T, db *DB, boardID int64, want map[int64]string) {
	t.Helper()
	states, err := db.GetAccountStates(context.Background(), boardID)
	if err != nil {
		t.Fatalf("read account states: %v", err)
	}
	for accountID, wantState := range want {
		state, exists := states[accountID]
		if !exists {
			t.Errorf("account %d has no state row", accountID)
			continue
		}
		if state.State != wantState {
			t.Errorf("account %d state = %q, want %q", accountID, state.State, wantState)
		}
	}
}

func requireOwnedModelLimit(t *testing.T, db *DB, board *LaneBoard, accountID int64, want bool) {
	t.Helper()
	blocks, err := db.GetExternalBlocks(context.Background(), []int64{accountID}, board.Model, board.Name)
	if err != nil {
		t.Fatalf("read account %d model limit: %v", accountID, err)
	}
	if blocks[accountID].OwnedModelLimit != want {
		t.Fatalf("account %d owned model limit = %v, want %v", accountID, blocks[accountID].OwnedModelLimit, want)
	}
}

func installFailingOutboxTrigger(t *testing.T, db *DB, accountID int64) {
	t.Helper()
	ctx := context.Background()
	statement := fmt.Sprintf(`
DROP TRIGGER IF EXISTS qa_fail_outbox_trigger ON scheduler_outbox;
CREATE OR REPLACE FUNCTION qa_fail_outbox_insert() RETURNS trigger AS $$
BEGIN
    IF NEW.account_id = %d THEN
        RAISE EXCEPTION 'injected outbox failure for account %%', NEW.account_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER qa_fail_outbox_trigger
BEFORE INSERT ON scheduler_outbox
FOR EACH ROW EXECUTE FUNCTION qa_fail_outbox_insert();`, accountID)
	if _, err := db.pool.Exec(ctx, statement); err != nil {
		t.Fatalf("install outbox failure trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.pool.Exec(context.Background(), `
DROP TRIGGER IF EXISTS qa_fail_outbox_trigger ON scheduler_outbox;
DROP FUNCTION IF EXISTS qa_fail_outbox_insert();`)
	})
}

func TestCheckBoardErrorsStateTransitions(t *testing.T) {
	db := openMonitorIntegrationDB(t)
	fake := newMonitorAPIFake()
	server := httptest.NewServer(fake)
	defer server.Close()
	monitor := NewLaneBoardMonitor(db, NewSub2APIClient(server.URL, "test-key", db))

	t.Run("below threshold keeps current lane", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 1)

		monitor.checkBoardErrors(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateHealthy, 3: LaneStateSuppressed})
		if fake.calls(3) != 0 {
			t.Fatalf("backup was probed below threshold: calls=%d", fake.calls(3))
		}
	})

	t.Run("partial active-lane failure does not fail over", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1, 2}, []int64{3})
		addModelFailures(t, db, 1, 2)

		monitor.checkBoardErrors(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{
			1: LaneStateDisabled, 2: LaneStateHealthy, 3: LaneStateSuppressed,
		})
		if fake.calls(3) != 0 {
			t.Fatalf("backup was probed while an active peer remained healthy: calls=%d", fake.calls(3))
		}
	})

	t.Run("complete active-lane failure releases backup in same cycle", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1, 2}, []int64{3})
		addModelFailures(t, db, 1, 2)
		addModelFailures(t, db, 2, 2)

		monitor.checkBoardErrors(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{
			1: LaneStateDisabled, 2: LaneStateDisabled, 3: LaneStateHealthy,
		})
		if fake.calls(3) != 1 {
			t.Fatalf("backup probe calls = %d, want 1", fake.calls(3))
		}
	})

	t.Run("failed backup verification stays disabled", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		fake.setProbeResult(3, false)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 2)

		monitor.checkBoardErrors(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateDisabled, 3: LaneStateDisabled})
		if fake.calls(3) != 1 {
			t.Fatalf("backup probe calls = %d, want 1", fake.calls(3))
		}
	})

	t.Run("selected backup lane releases all healthy peers", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{2, 3}, []int64{4})
		addModelFailures(t, db, 1, 2)

		monitor.checkBoardErrors(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{
			1: LaneStateDisabled, 2: LaneStateHealthy, 3: LaneStateHealthy, 4: LaneStateSuppressed,
		})
		if fake.calls(2) != 1 || fake.calls(3) != 1 || fake.calls(4) != 0 {
			t.Fatalf("candidate probe calls = account2:%d account3:%d account4:%d", fake.calls(2), fake.calls(3), fake.calls(4))
		}
	})

	t.Run("failed candidate lane falls through to next lane", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		fake.setProbeResult(2, false)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{2}, []int64{3})
		addModelFailures(t, db, 1, 2)

		monitor.checkBoardErrors(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{
			1: LaneStateDisabled, 2: LaneStateDisabled, 3: LaneStateHealthy,
		})
		if fake.calls(2) != 1 || fake.calls(3) != 1 {
			t.Fatalf("candidate probe calls = account2:%d account3:%d, want one each", fake.calls(2), fake.calls(3))
		}
	})

	t.Run("failed recovery respects per-board probe interval", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 2)
		monitor.checkBoardErrors(context.Background(), board)
		if _, err := db.pool.Exec(context.Background(), `TRUNCATE ops_error_logs`); err != nil {
			t.Fatalf("clear failure window: %v", err)
		}
		fake.setProbeResult(1, false)

		monitor.probeBoard(context.Background(), board)
		monitor.probeBoard(context.Background(), board)
		if fake.calls(1) != 1 {
			t.Fatalf("recovery probe repeated before interval: calls=%d", fake.calls(1))
		}
		if _, err := db.pool.Exec(context.Background(), `
UPDATE lane_account_states SET last_probe_at=now() - interval '11 seconds'
WHERE board_id=$1 AND account_id=1`, board.ID); err != nil {
			t.Fatalf("age last probe: %v", err)
		}
		monitor.probeBoard(context.Background(), board)
		if fake.calls(1) != 2 {
			t.Fatalf("recovery probe did not run after interval: calls=%d", fake.calls(1))
		}
	})

	t.Run("native cooldown probes immediately after expiry", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 2)
		monitor.checkBoardErrors(context.Background(), board)
		if _, err := db.pool.Exec(context.Background(), `
TRUNCATE ops_error_logs;
UPDATE accounts SET rate_limit_reset_at=now() + interval '1 hour' WHERE id=1`); err != nil {
			t.Fatalf("set native cooldown: %v", err)
		}

		monitor.probeBoard(context.Background(), board)
		if fake.calls(1) != 0 {
			t.Fatalf("account was probed during native cooldown: calls=%d", fake.calls(1))
		}
		if _, err := db.pool.Exec(context.Background(), `
UPDATE accounts SET rate_limit_reset_at=now() - interval '1 second' WHERE id=1`); err != nil {
			t.Fatalf("expire native cooldown: %v", err)
		}
		monitor.probeBoard(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateHealthy, 3: LaneStateSuppressed})
		if fake.calls(1) != 1 {
			t.Fatalf("account was not probed after native cooldown expiry: calls=%d", fake.calls(1))
		}
	})

	t.Run("recovered higher lane suppresses lower lane immediately", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 2)
		monitor.checkBoardErrors(context.Background(), board)
		if _, err := db.pool.Exec(context.Background(), `TRUNCATE ops_error_logs`); err != nil {
			t.Fatalf("clear failure window: %v", err)
		}

		monitor.probeBoard(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateHealthy, 3: LaneStateSuppressed})
		if fake.calls(1) != 1 {
			t.Fatalf("higher-lane probe calls = %d, want 1", fake.calls(1))
		}
	})

	t.Run("stale suppressed recovery re-suppresses old active lane", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{2}, []int64{3})
		if err := monitor.disableAccount(context.Background(), board, 1, AccountState{FailCount: 2}); err != nil {
			t.Fatalf("disable highest lane: %v", err)
		}
		if _, err := monitor.client.ClearAllOwnedModelRateLimits(context.Background(), 3, board.Name); err != nil {
			t.Fatalf("release old active account: %v", err)
		}
		if err := db.SetAccountState(context.Background(), board.ID, 3, LaneStateHealthy, nil); err != nil {
			t.Fatalf("mark old active account healthy: %v", err)
		}

		if _, err := monitor.reconcileBoard(context.Background(), board); err != nil {
			t.Fatalf("reconcile stale suppression: %v", err)
		}

		requireAccountStates(t, db, board.ID, map[int64]string{
			1: LaneStateDisabled, 2: LaneStateHealthy, 3: LaneStateSuppressed,
		})
		requireOwnedModelLimit(t, db, board, 2, false)
		requireOwnedModelLimit(t, db, board, 3, true)
	})

	t.Run("successful probe rechecks traffic gate before recovery", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 2)
		monitor.checkBoardErrors(context.Background(), board)
		if _, err := db.pool.Exec(context.Background(), `TRUNCATE ops_error_logs`); err != nil {
			t.Fatalf("clear failure window: %v", err)
		}
		hookErr := make(chan error, 1)
		fake.setProbeHook(1, func() {
			_, err := db.pool.Exec(context.Background(), `
INSERT INTO ops_error_logs (account_id, requested_model, error_phase, upstream_status_code)
VALUES (1, 'qa-model', 'upstream', 503), (1, 'qa-model', 'upstream', 503)`)
			hookErr <- err
		})

		monitor.probeBoard(context.Background(), board)
		if err := <-hookErr; err != nil {
			t.Fatalf("inject failures during probe: %v", err)
		}

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateDisabled, 3: LaneStateHealthy})
		requireOwnedModelLimit(t, db, board, 1, true)
		if fake.calls(1) != 1 {
			t.Fatalf("higher-lane probe calls = %d, want 1", fake.calls(1))
		}
	})

	t.Run("disabled backup is probed immediately during failover", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		if err := monitor.disableAccount(context.Background(), board, 3, AccountState{FailCount: 1}); err != nil {
			t.Fatalf("pre-disable backup: %v", err)
		}
		if err := db.UpdateAccountStateProbe(context.Background(), board.ID, 3, false, "recent failure", time.Now()); err != nil {
			t.Fatalf("record recent backup probe: %v", err)
		}
		addModelFailures(t, db, 1, 2)

		monitor.checkBoardErrors(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateDisabled, 3: LaneStateHealthy})
		if fake.calls(3) != 1 {
			t.Fatalf("disabled backup probe calls = %d, want 1", fake.calls(3))
		}
	})

	t.Run("recently probed suppressed backup still takes over immediately", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		if err := db.UpdateAccountStateProbe(context.Background(), board.ID, 3, true, "recent verification", time.Now()); err != nil {
			t.Fatalf("record recent suppressed probe: %v", err)
		}
		addModelFailures(t, db, 1, 2)

		monitor.checkBoardErrors(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateDisabled, 3: LaneStateHealthy})
		if fake.calls(3) != 1 {
			t.Fatalf("suppressed backup probe calls = %d, want 1", fake.calls(3))
		}
	})

	t.Run("traffic gate rejection does not consume probe interval", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 2)
		monitor.checkBoardErrors(context.Background(), board)

		monitor.probeBoard(context.Background(), board)
		states, err := db.GetAccountStates(context.Background(), board.ID)
		if err != nil {
			t.Fatalf("read gate-rejected state: %v", err)
		}
		if states[1].LastProbeAt != nil {
			t.Fatalf("gate rejection advanced last_probe_at to %s", states[1].LastProbeAt)
		}
		if _, err := db.pool.Exec(context.Background(), `TRUNCATE ops_error_logs`); err != nil {
			t.Fatalf("clear failure window: %v", err)
		}
		monitor.probeBoard(context.Background(), board)
		if fake.calls(1) != 1 {
			t.Fatalf("probe did not run immediately after gate cleared: calls=%d", fake.calls(1))
		}
	})

	t.Run("manual probe cannot activate a lower lane", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})

		ok, msg, err := monitor.ManualProbe(context.Background(), board.ID, 3)
		if err != nil {
			t.Fatalf("manual lower-lane probe: %v", err)
		}
		if ok {
			t.Fatalf("manual lower-lane probe reported recovery: %q", msg)
		}
		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateHealthy, 3: LaneStateSuppressed})
		requireOwnedModelLimit(t, db, board, 3, true)
	})

	t.Run("candidate release rechecks traffic gate after probe", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 2)
		hookErr := make(chan error, 1)
		fake.setProbeHook(3, func() {
			_, err := db.pool.Exec(context.Background(), `
INSERT INTO ops_error_logs (account_id, requested_model, error_phase, upstream_status_code)
VALUES (3, 'qa-model', 'upstream', 503), (3, 'qa-model', 'upstream', 503)`)
			hookErr <- err
		})

		monitor.checkBoardErrors(context.Background(), board)
		if err := <-hookErr; err != nil {
			t.Fatalf("inject candidate failures during probe: %v", err)
		}

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateDisabled, 3: LaneStateSuppressed})
		requireOwnedModelLimit(t, db, board, 3, true)
	})

	t.Run("manual recovery rechecks traffic gate after probe", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 2)
		monitor.checkBoardErrors(context.Background(), board)
		if _, err := db.pool.Exec(context.Background(), `TRUNCATE ops_error_logs`); err != nil {
			t.Fatalf("clear failure window: %v", err)
		}
		hookErr := make(chan error, 1)
		fake.setProbeHook(1, func() {
			_, err := db.pool.Exec(context.Background(), `
INSERT INTO ops_error_logs (account_id, requested_model, error_phase, upstream_status_code)
VALUES (1, 'qa-model', 'upstream', 503), (1, 'qa-model', 'upstream', 503)`)
			hookErr <- err
		})

		ok, _, err := monitor.ManualProbe(context.Background(), board.ID, 1)
		if err != nil {
			t.Fatalf("manual higher-lane probe: %v", err)
		}
		if err := <-hookErr; err != nil {
			t.Fatalf("inject manual-probe failures: %v", err)
		}
		if ok {
			t.Fatal("manual probe recovered account after traffic gate became unhealthy")
		}
		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateDisabled, 3: LaneStateHealthy})
		requireOwnedModelLimit(t, db, board, 1, true)
	})

	t.Run("missing suppression write returns reconcile error", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		if _, err := monitor.client.ClearAllOwnedModelRateLimits(context.Background(), 3, board.Name); err != nil {
			t.Fatalf("remove lower-lane suppression: %v", err)
		}
		installFailingOutboxTrigger(t, db, 3)

		if _, err := monitor.reconcileBoard(context.Background(), board); err == nil {
			t.Fatal("reconcile hid a failed suppression write")
		}
		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateHealthy, 3: LaneStateSuppressed})
		requireOwnedModelLimit(t, db, board, 3, false)
	})

	t.Run("failed lower-lane suppression rolls back higher recovery", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 2)
		monitor.checkBoardErrors(context.Background(), board)
		if _, err := db.pool.Exec(context.Background(), `TRUNCATE ops_error_logs`); err != nil {
			t.Fatalf("clear failure window: %v", err)
		}
		installFailingOutboxTrigger(t, db, 3)

		monitor.probeBoard(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateDisabled, 3: LaneStateHealthy})
		requireOwnedModelLimit(t, db, board, 1, true)
		requireOwnedModelLimit(t, db, board, 3, false)
	})

	t.Run("manual recovery rollback preserves previous disabled state", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		addModelFailures(t, db, 1, 2)
		monitor.checkBoardErrors(context.Background(), board)
		if _, err := db.pool.Exec(context.Background(), `TRUNCATE ops_error_logs`); err != nil {
			t.Fatalf("clear failure window: %v", err)
		}
		installFailingOutboxTrigger(t, db, 3)

		ok, _, err := monitor.ManualProbe(context.Background(), board.ID, 1)
		if err == nil || ok {
			t.Fatalf("manual recovery result: ok=%v err=%v, want rollback error", ok, err)
		}
		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateDisabled, 3: LaneStateHealthy})
		requireOwnedModelLimit(t, db, board, 1, true)
		requireOwnedModelLimit(t, db, board, 3, false)
	})

	t.Run("stale suppressed recovery rollback preserves suppression", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{2}, []int64{3})
		if err := monitor.disableAccount(context.Background(), board, 1, AccountState{FailCount: 2}); err != nil {
			t.Fatalf("disable highest lane: %v", err)
		}
		if _, err := monitor.client.ClearAllOwnedModelRateLimits(context.Background(), 3, board.Name); err != nil {
			t.Fatalf("release old active account: %v", err)
		}
		if err := db.SetAccountState(context.Background(), board.ID, 3, LaneStateHealthy, nil); err != nil {
			t.Fatalf("mark old active account healthy: %v", err)
		}
		installFailingOutboxTrigger(t, db, 3)

		if _, err := monitor.reconcileBoard(context.Background(), board); err == nil {
			t.Fatal("stale recovery did not report failed old-active suppression")
		}
		requireAccountStates(t, db, board.ID, map[int64]string{
			1: LaneStateDisabled, 2: LaneStateSuppressed, 3: LaneStateHealthy,
		})
		requireOwnedModelLimit(t, db, board, 2, true)
		requireOwnedModelLimit(t, db, board, 3, false)
	})

	t.Run("all-down retries respect probe interval after immediate failover attempt", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		fake.setProbeResult(3, false)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		if err := monitor.disableAccount(context.Background(), board, 3, AccountState{FailCount: 1}); err != nil {
			t.Fatalf("pre-disable backup: %v", err)
		}
		addModelFailures(t, db, 1, 2)

		monitor.checkBoardErrors(context.Background(), board)
		if fake.calls(3) != 1 {
			t.Fatalf("immediate failover probe calls = %d, want 1", fake.calls(3))
		}
		if _, err := monitor.reconcileBoard(context.Background(), board); err != nil {
			t.Fatalf("repeat all-down reconcile: %v", err)
		}
		monitor.probeBoard(context.Background(), board)
		if fake.calls(3) != 1 {
			t.Fatalf("all-down probe interval was bypassed: calls=%d, want 1", fake.calls(3))
		}
	})

	t.Run("probe interval starts after a slow probe completes", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		fake.setProbeResult(3, false)
		fake.setProbeResult(1, false)
		fake.setProbeDelay(3, 1200*time.Millisecond)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		board.ProbeInterval = 1
		if err := monitor.disableAccount(context.Background(), board, 1, AccountState{FailCount: 2}); err != nil {
			t.Fatalf("disable primary: %v", err)
		}
		if err := monitor.disableAccount(context.Background(), board, 3, AccountState{FailCount: 1}); err != nil {
			t.Fatalf("disable backup: %v", err)
		}

		started := time.Now()
		if _, err := monitor.reconcileBoard(context.Background(), board); err != nil {
			t.Fatalf("slow all-down probe: %v", err)
		}
		states, err := db.GetAccountStates(context.Background(), board.ID)
		if err != nil {
			t.Fatalf("read slow probe state: %v", err)
		}
		if states[3].LastProbeAt == nil || states[3].LastProbeAt.Before(started.Add(600*time.Millisecond)) {
			t.Fatalf("last_probe_at was recorded before probe completion: %v", states[3].LastProbeAt)
		}
		if fake.calls(3) != 1 {
			t.Fatalf("initial slow probe calls = %d, want 1", fake.calls(3))
		}

		if _, err := monitor.reconcileBoard(context.Background(), board); err != nil {
			t.Fatalf("repeat slow all-down reconcile: %v", err)
		}
		if fake.calls(3) != 1 {
			t.Fatalf("probe repeated immediately after slow completion: calls=%d", fake.calls(3))
		}
	})

	t.Run("candidate recovery repairs lower suppression drift before return", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{2}, []int64{3})
		if _, err := monitor.client.ClearAllOwnedModelRateLimits(context.Background(), 3, board.Name); err != nil {
			t.Fatalf("remove lower suppression: %v", err)
		}
		addModelFailures(t, db, 1, 2)

		monitor.checkBoardErrors(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{
			1: LaneStateDisabled, 2: LaneStateHealthy, 3: LaneStateSuppressed,
		})
		requireOwnedModelLimit(t, db, board, 2, false)
		requireOwnedModelLimit(t, db, board, 3, true)
	})

	t.Run("lower all-down lane is never probed while higher lane is active", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		if err := monitor.disableAccount(context.Background(), board, 3, AccountState{FailCount: 3}); err != nil {
			t.Fatalf("fail the lower lane: %v", err)
		}
		// 即使上次探测时间早已超过间隔，也不允许探测被压制的低泳道
		if _, err := db.pool.Exec(context.Background(), `
UPDATE lane_account_states SET last_probe_at=now() - interval '2 minutes', last_probe_ok=false
WHERE board_id=$1 AND account_id=3`, board.ID); err != nil {
			t.Fatalf("age lower-lane probe timestamp: %v", err)
		}

		monitor.probeBoard(context.Background(), board)
		monitor.probeBoard(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateHealthy, 3: LaneStateDisabled})
		requireOwnedModelLimit(t, db, board, 3, true)
		if fake.calls(3) != 0 {
			t.Fatalf("lower all-down lane was probed while higher lane was active: calls=%d", fake.calls(3))
		}
	})

	t.Run("lower suppressed lane is never probed while higher lane is active", func(t *testing.T) {
		resetMonitorScenario(t, db, fake)
		board := createMonitorBoard(t, db, monitor, []int64{1}, []int64{3})
		// createMonitorBoard 的初始 reconcile 会把低泳道 B 压制为 suppressed
		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateHealthy, 3: LaneStateSuppressed})

		monitor.probeBoard(context.Background(), board)
		monitor.probeBoard(context.Background(), board)

		requireAccountStates(t, db, board.ID, map[int64]string{1: LaneStateHealthy, 3: LaneStateSuppressed})
		requireOwnedModelLimit(t, db, board, 3, true)
		if fake.calls(3) != 0 {
			t.Fatalf("suppressed lower lane was probed while higher lane was active: calls=%d", fake.calls(3))
		}
	})
}
