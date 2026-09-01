package main

import (
	"regexp"
	"strconv"
	"testing"
)

func TestDashboardRefreshesStatePromptly(t *testing.T) {
	page, err := webFS.ReadFile("web/index.html")
	if err != nil {
		t.Fatalf("read embedded dashboard: %v", err)
	}
	match := regexp.MustCompile(`setInterval\([^;]+,\s*([0-9]+)\s*\)`).FindSubmatch(page)
	if len(match) != 2 {
		t.Fatal("dashboard auto-refresh interval was not found")
	}
	milliseconds, err := strconv.Atoi(string(match[1]))
	if err != nil {
		t.Fatalf("parse dashboard auto-refresh interval: %v", err)
	}
	if milliseconds > 5000 {
		t.Fatalf("dashboard refresh interval = %dms, state changes can remain stale for more than 5s", milliseconds)
	}
}
