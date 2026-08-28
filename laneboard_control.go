package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	ErrInvalidBoard  = errors.New("invalid lane board")
	ErrBoardNotFound = errors.New("lane board not found")
)

// BoardCleanupOp removes a model limit owned by a previous board identity.
type BoardCleanupOp struct {
	AccountID int64
	Model     string
	BoardName string
}

func uniqueBoardAccountIDs(board *LaneBoard) []int64 {
	if board == nil {
		return nil
	}
	seen := make(map[int64]struct{})
	var ids []int64
	for _, lane := range board.Lanes {
		for _, accountID := range lane.AccountIDs {
			if accountID <= 0 {
				continue
			}
			if _, exists := seen[accountID]; exists {
				continue
			}
			seen[accountID] = struct{}{}
			ids = append(ids, accountID)
		}
	}
	return ids
}

// planBoardCleanup identifies limits that belong to the previous board
// identity and therefore must be removed before the old configuration stops
// reconciling them.
func planBoardCleanup(previous, next *LaneBoard) []BoardCleanupOp {
	if previous == nil {
		return nil
	}

	oldIDs := uniqueBoardAccountIDs(previous)
	cleanupAll := next == nil || !next.Enabled || previous.Name != next.Name || previous.Model != next.Model
	keep := make(map[int64]struct{})
	if !cleanupAll {
		for _, accountID := range uniqueBoardAccountIDs(next) {
			keep[accountID] = struct{}{}
		}
	}

	ops := make([]BoardCleanupOp, 0, len(oldIDs))
	for _, accountID := range oldIDs {
		if !cleanupAll {
			if _, exists := keep[accountID]; exists {
				continue
			}
		}
		ops = append(ops, BoardCleanupOp{
			AccountID: accountID,
			Model:     previous.Model,
			BoardName: previous.Name,
		})
	}
	return ops
}

func externalBlockAllowsProbe(block ExternalBlock, now time.Time) bool {
	if block.Expired || block.QuotaExceeded {
		return false
	}
	if !block.blocked(now) {
		return true
	}
	for _, until := range []*time.Time{block.NativeCoolUntil, block.RateLimitUntil, block.OverloadUntil, block.TempUnschedUntil} {
		if until != nil && until.After(now) {
			return false
		}
	}
	if block.Status == "error" {
		return true
	}
	return !block.Schedulable && block.Status == "active"
}

func quotaNumber(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case json.Number:
		parsed, _ := typed.Float64()
		return parsed
	case string:
		parsed, _ := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed
	default:
		return 0
	}
}

func parseOptionalTime(value any) time.Time {
	text, ok := value.(string)
	if !ok {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(text))
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func quotaPeriodExpired(extra map[string]any, prefix string, duration time.Duration, now time.Time) bool {
	start := parseOptionalTime(extra[prefix+"_start"])
	if start.IsZero() {
		return true
	}
	if mode, _ := extra[prefix+"_reset_mode"].(string); mode == "fixed" {
		tzName, _ := extra["quota_reset_timezone"].(string)
		if strings.TrimSpace(tzName) == "" {
			tzName = "UTC"
		}
		tz, err := time.LoadLocation(tzName)
		if err != nil {
			tz = time.UTC
		}
		hour := int(quotaNumber(extra[prefix+"_reset_hour"]))
		if hour < 0 || hour > 23 {
			hour = 0
		}
		var lastReset time.Time
		if prefix == "quota_weekly" {
			day := 1
			if rawDay, exists := extra["quota_weekly_reset_day"]; exists {
				day = int(quotaNumber(rawDay))
			}
			if day < 0 || day > 6 {
				day = 1
			}
			localNow := now.In(tz)
			todayReset := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), hour, 0, 0, 0, tz)
			daysBack := (int(todayReset.Weekday()) - day + 7) % 7
			if daysBack == 0 && now.Before(todayReset) {
				daysBack = 7
			}
			lastReset = todayReset.AddDate(0, 0, -daysBack)
		} else {
			localNow := now.In(tz)
			lastReset = time.Date(localNow.Year(), localNow.Month(), localNow.Day(), hour, 0, 0, 0, tz)
			if now.Before(lastReset) {
				lastReset = lastReset.AddDate(0, 0, -1)
			}
		}
		return start.Before(lastReset)
	}
	return now.Sub(start) >= duration
}

func accountQuotaExceeded(extra map[string]any, now time.Time) bool {
	if limit := quotaNumber(extra["quota_limit"]); limit > 0 && quotaNumber(extra["quota_used"]) >= limit {
		return true
	}
	if limit := quotaNumber(extra["quota_daily_limit"]); limit > 0 &&
		!quotaPeriodExpired(extra, "quota_daily", 24*time.Hour, now) && quotaNumber(extra["quota_daily_used"]) >= limit {
		return true
	}
	if limit := quotaNumber(extra["quota_weekly_limit"]); limit > 0 &&
		!quotaPeriodExpired(extra, "quota_weekly", 7*24*time.Hour, now) && quotaNumber(extra["quota_weekly_used"]) >= limit {
		return true
	}
	return false
}

func validLaneAccountState(state string) bool {
	switch state {
	case LaneStateHealthy, LaneStateDisabled, LaneStateSuppressed:
		return true
	default:
		return false
	}
}

func validateBoardDefinition(board *LaneBoard, existing []LaneBoard) error {
	if board == nil {
		return fmt.Errorf("board is required")
	}
	board.Name = strings.TrimSpace(board.Name)
	board.Model = strings.TrimSpace(board.Model)
	if board.Name == "" {
		return fmt.Errorf("board name is required")
	}
	if board.Model == "" {
		return fmt.Errorf("board model is required")
	}
	if board.FailThreshold <= 0 {
		board.FailThreshold = 3
	}
	if board.WindowSeconds <= 0 {
		board.WindowSeconds = 60
	}
	if board.ProbeInterval <= 0 {
		board.ProbeInterval = 30
	}
	if board.WindowSeconds < 10 {
		return fmt.Errorf("window_seconds must be at least 10")
	}
	if board.ProbeInterval < 10 {
		return fmt.Errorf("probe_interval must be at least 10")
	}
	if len(board.Lanes) == 0 {
		return fmt.Errorf("at least one lane is required")
	}

	seenAccounts := make(map[int64]struct{})
	accountCount := 0
	for laneIndex := range board.Lanes {
		lane := &board.Lanes[laneIndex]
		lane.Name = strings.TrimSpace(lane.Name)
		if len(lane.AccountIDs) == 0 {
			return fmt.Errorf("lane %d must contain at least one account", laneIndex+1)
		}
		for _, accountID := range lane.AccountIDs {
			if accountID <= 0 {
				return fmt.Errorf("lane %d has an invalid account id", laneIndex+1)
			}
			if _, exists := seenAccounts[accountID]; exists {
				return fmt.Errorf("account %d appears in more than one lane", accountID)
			}
			seenAccounts[accountID] = struct{}{}
			accountCount++
		}
	}
	if accountCount == 0 {
		return fmt.Errorf("at least one account is required")
	}

	for _, candidate := range existing {
		if candidate.ID != board.ID && candidate.Model == board.Model {
			return fmt.Errorf("model %q already belongs to board %q", board.Model, candidate.Name)
		}
		if candidate.ID != board.ID && candidate.Name == board.Name {
			return fmt.Errorf("board name %q already exists", board.Name)
		}
	}
	return nil
}
