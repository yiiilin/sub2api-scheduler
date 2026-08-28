package main

import "testing"

func TestBoardLocksAreScopedByBoard(t *testing.T) {
	monitor := NewLaneBoardMonitor(nil, nil)
	boardOne := monitor.lockForBoard(1)
	boardOneAgain := monitor.lockForBoard(1)
	boardTwo := monitor.lockForBoard(2)

	if boardOne != boardOneAgain {
		t.Fatal("same board did not reuse the same lock")
	}
	if boardOne == boardTwo {
		t.Fatal("different boards unexpectedly share one lock")
	}

	boardOne.Lock()
	defer boardOne.Unlock()
	if boardOneAgain.TryLock() {
		boardOneAgain.Unlock()
		t.Fatal("same-board operation was not serialized")
	}
	if !boardTwo.TryLock() {
		t.Fatal("different-board operation was unnecessarily blocked")
	}
	boardTwo.Unlock()
}
