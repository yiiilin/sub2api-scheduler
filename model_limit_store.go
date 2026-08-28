package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func decodeJSONMap(raw []byte) (map[string]any, error) {
	result := make(map[string]any)
	if len(raw) == 0 || string(raw) == "null" {
		return result, nil
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (d *DB) loadAccountModelLimitsTx(ctx context.Context, tx pgx.Tx, accountID int64) (*SubAccount, map[string]any, error) {
	var platform, accountType, oauthType, projectID string
	var passThrough, oauthPassThrough bool
	var mappingJSON, limitsJSON []byte
	err := tx.QueryRow(ctx, `
SELECT platform, type, COALESCE(credentials->>'oauth_type', ''), COALESCE(credentials->>'project_id', ''),
       COALESCE((extra->>'openai_passthrough') = 'true', false),
       COALESCE((extra->>'openai_oauth_passthrough') = 'true', false),
       credentials->'model_mapping', extra->'model_rate_limits'
FROM accounts
WHERE id=$1 AND deleted_at IS NULL
FOR UPDATE`, accountID).Scan(&platform, &accountType, &oauthType, &projectID, &passThrough, &oauthPassThrough, &mappingJSON, &limitsJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrSub2APIAccountNotFound
		}
		return nil, nil, err
	}
	mapping, err := decodeJSONMap(mappingJSON)
	if err != nil {
		return nil, nil, fmt.Errorf("decode account %d model mapping: %w", accountID, err)
	}
	limits, err := decodeJSONMap(limitsJSON)
	if err != nil {
		return nil, nil, fmt.Errorf("decode account %d model rate limits: %w", accountID, err)
	}
	credentials := map[string]any{
		"model_mapping":            mapping,
		"oauth_type":               oauthType,
		"project_id":               projectID,
		"openai_passthrough":       passThrough,
		"openai_oauth_passthrough": oauthPassThrough,
	}
	return &SubAccount{ID: accountID, Platform: platform, Type: accountType, Credentials: credentials, Extra: credentials}, limits, nil
}

func updateModelLimitsTx(ctx context.Context, tx pgx.Tx, accountID int64, limits map[string]any) error {
	encoded, err := json.Marshal(limits)
	if err != nil {
		return fmt.Errorf("encode account %d model rate limits: %w", accountID, err)
	}
	var affected int64
	if len(limits) == 0 {
		commandTag, execErr := tx.Exec(ctx, `
UPDATE accounts
SET extra=COALESCE(extra, '{}'::jsonb) - 'model_rate_limits', updated_at=now()
WHERE id=$1 AND deleted_at IS NULL`, accountID)
		if execErr != nil {
			return execErr
		}
		affected = commandTag.RowsAffected()
	} else {
		commandTag, execErr := tx.Exec(ctx, `
UPDATE accounts
SET extra=jsonb_set(COALESCE(extra, '{}'::jsonb), '{model_rate_limits}', $2::jsonb, true), updated_at=now()
WHERE id=$1 AND deleted_at IS NULL`, accountID, encoded)
		if execErr != nil {
			return execErr
		}
		affected = commandTag.RowsAffected()
	}
	if affected == 0 {
		return ErrSub2APIAccountNotFound
	}
	_, err = tx.Exec(ctx, `
INSERT INTO scheduler_outbox (event_type, account_id)
VALUES ('account_changed', $1)`, accountID)
	return err
}

// SetOwnedModelRateLimitAtomically updates only model limits while holding the
// account row lock. It preserves concurrent changes to unrelated extra fields.
func (d *DB) SetOwnedModelRateLimitAtomically(ctx context.Context, accountID int64, requestedModel, boardName string, entry map[string]any) error {
	if !modelRateLimitEntryOwnedBy(entry, boardName) {
		return fmt.Errorf("model rate-limit entry does not belong to board %q", boardName)
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	account, limits, err := d.loadAccountModelLimitsTx(ctx, tx, accountID)
	if err != nil {
		return err
	}
	modelKeys := accountModelRateLimitKeys(account, requestedModel)
	if len(modelKeys) == 0 {
		return fmt.Errorf("account %d model %q has no resolved model key", accountID, requestedModel)
	}
	reason := modelRateLimitEntryReason(entry)
	for _, modelKey := range modelKeys {
		if current, exists := limits[modelKey]; !exists {
			continue
		} else if !modelRateLimitEntryOwnedBy(current, boardName) {
			if resetAt := modelRateLimitResetAt(current); resetAt == nil || resetAt.After(time.Now()) {
				return fmt.Errorf("account %d model %q: %w", accountID, requestedModel, ErrForeignModelRateLimit)
			}
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
	if err := updateModelLimitsTx(ctx, tx, accountID, limits); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ClearOwnedModelRateLimitAtomically removes only the current board's entry.
func (d *DB) ClearOwnedModelRateLimitAtomically(ctx context.Context, accountID int64, requestedModel, boardName string) (bool, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	account, limits, err := d.loadAccountModelLimitsTx(ctx, tx, accountID)
	if err != nil {
		return false, err
	}
	cleared := false
	for _, modelKey := range accountModelRateLimitKeys(account, requestedModel) {
		current, exists := limits[modelKey]
		if exists && modelRateLimitEntryOwnedBy(current, boardName) {
			delete(limits, modelKey)
			cleared = true
		}
	}
	if !cleared {
		return false, nil
	}
	if err := updateModelLimitsTx(ctx, tx, accountID, limits); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// ClearAllOwnedModelRateLimitsAtomically removes all entries owned by a board,
// including keys left behind after an account mapping changed.
func (d *DB) ClearAllOwnedModelRateLimitsAtomically(ctx context.Context, accountID int64, boardName string) (int, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	_, limits, err := d.loadAccountModelLimitsTx(ctx, tx, accountID)
	if err != nil {
		return 0, err
	}
	cleared := 0
	for modelKey, entry := range limits {
		if modelRateLimitEntryOwnedBy(entry, boardName) {
			delete(limits, modelKey)
			cleared++
		}
	}
	if cleared == 0 {
		return 0, nil
	}
	if err := updateModelLimitsTx(ctx, tx, accountID, limits); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return cleared, nil
}

// MergeModelRateLimitsAtomically restores only missing, still-active board-owned
// entries after Sub2API's successful test recovery cleared the whole map.
func (d *DB) MergeModelRateLimitsAtomically(ctx context.Context, accountID int64, entries map[string]any, now time.Time) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var raw []byte
	err = tx.QueryRow(ctx, `
SELECT extra->'model_rate_limits'
FROM accounts
WHERE id=$1 AND deleted_at IS NULL
FOR UPDATE`, accountID).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSub2APIAccountNotFound
		}
		return err
	}
	limits, err := decodeJSONMap(raw)
	if err != nil {
		return fmt.Errorf("decode account %d model rate limits: %w", accountID, err)
	}
	changed := false
	for modelKey, entry := range entries {
		if !strings.HasPrefix(modelRateLimitEntryReason(entry), laneReasonPrefix) {
			continue
		}
		if _, exists := limits[modelKey]; !exists && activeModelRateLimit(entry, now) {
			limits[modelKey] = entry
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := updateModelLimitsTx(ctx, tx, accountID, limits); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
