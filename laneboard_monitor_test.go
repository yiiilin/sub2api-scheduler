package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMonitorRunsInitialCyclesImmediately(t *testing.T) {
	monitor := NewLaneBoardMonitor(nil, nil)
	monitor.errorCheckInterval = time.Hour
	monitor.probeScanInterval = time.Hour
	checked := make(chan struct{}, 1)
	probed := make(chan struct{}, 1)
	monitor.checkErrorsCycle = func(context.Context) { checked <- struct{}{} }
	monitor.probeCycle = func(context.Context) { probed <- struct{}{} }

	ctx, cancel := context.WithCancel(context.Background())
	monitor.Start(ctx)
	defer func() {
		cancel()
		monitor.Wait()
	}()

	deadline := time.After(100 * time.Millisecond)
	for checked != nil || probed != nil {
		select {
		case <-checked:
			checked = nil
		case <-probed:
			probed = nil
		case <-deadline:
			t.Fatal("monitor did not run both state cycles immediately after startup")
		}
	}
}

func TestMonitorDoesNotRunCyclesForCanceledContext(t *testing.T) {
	monitor := NewLaneBoardMonitor(nil, nil)
	var checks atomic.Int32
	var probes atomic.Int32
	monitor.checkErrorsCycle = func(context.Context) { checks.Add(1) }
	monitor.probeCycle = func(context.Context) { probes.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	monitor.Start(ctx)
	monitor.Wait()

	if checks.Load() != 0 || probes.Load() != 0 {
		t.Fatalf("cycles ran after cancellation: checks=%d probes=%d", checks.Load(), probes.Load())
	}
}

func TestMonitorProbeScanSupportsMinimumBoardInterval(t *testing.T) {
	const maximumSchedulingDelay = 5 * time.Second
	if laneProbeScanInterval > maximumSchedulingDelay {
		t.Fatalf("probe scan interval = %s, state transitions can be delayed by more than %s", laneProbeScanInterval, maximumSchedulingDelay)
	}
}

func TestMonitorCyclesRepeatAndStopWithContext(t *testing.T) {
	monitor := NewLaneBoardMonitor(nil, nil)
	monitor.errorCheckInterval = 10 * time.Millisecond
	monitor.probeScanInterval = 10 * time.Millisecond
	var checks atomic.Int32
	var probes atomic.Int32
	monitor.checkErrorsCycle = func(context.Context) { checks.Add(1) }
	monitor.probeCycle = func(context.Context) { probes.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	monitor.Start(ctx)
	deadline := time.Now().Add(250 * time.Millisecond)
	for (checks.Load() < 3 || probes.Load() < 3) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if checks.Load() < 3 || probes.Load() < 3 {
		cancel()
		monitor.Wait()
		t.Fatalf("cycles did not repeat: checks=%d probes=%d", checks.Load(), probes.Load())
	}

	cancel()
	monitor.Wait()
	checksAfterStop := checks.Load()
	probesAfterStop := probes.Load()
	time.Sleep(25 * time.Millisecond)
	if checks.Load() != checksAfterStop || probes.Load() != probesAfterStop {
		t.Fatalf("cycles continued after cancellation: checks=%d->%d probes=%d->%d", checksAfterStop, checks.Load(), probesAfterStop, probes.Load())
	}
}

func TestSlowErrorCheckDoesNotDelayProbeCycle(t *testing.T) {
	monitor := NewLaneBoardMonitor(nil, nil)
	monitor.errorCheckInterval = time.Hour
	monitor.probeScanInterval = time.Hour
	checkStarted := make(chan struct{})
	releaseCheck := make(chan struct{})
	probeRan := make(chan struct{}, 1)
	monitor.checkErrorsCycle = func(context.Context) {
		close(checkStarted)
		<-releaseCheck
	}
	monitor.probeCycle = func(context.Context) { probeRan <- struct{}{} }

	ctx, cancel := context.WithCancel(context.Background())
	monitor.Start(ctx)
	defer func() {
		close(releaseCheck)
		cancel()
		monitor.Wait()
	}()

	select {
	case <-checkStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("initial error check did not start")
	}
	select {
	case <-probeRan:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("probe cycle was blocked by a slow error check")
	}
}

type blockingTestLocker struct {
	entered chan struct{}
	release chan struct{}
}

func (l *blockingTestLocker) Lock() {
	close(l.entered)
	<-l.release
}

func (l *blockingTestLocker) Unlock() {}

func TestBoardLockWaitDoesNotConsumeParallelSlot(t *testing.T) {
	semaphore := make(chan struct{}, 1)
	blockedLock := &blockingTestLocker{entered: make(chan struct{}), release: make(chan struct{})}
	blockedDone := make(chan struct{})
	go func() {
		runBoardTask(context.Background(), semaphore, blockedLock, func() {})
		close(blockedDone)
	}()
	<-blockedLock.entered

	independentDone := make(chan struct{})
	go func() {
		runBoardTask(context.Background(), semaphore, &sync.Mutex{}, func() {
			close(independentDone)
		})
	}()
	select {
	case <-independentDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("a board waiting on its lock consumed the parallel execution slot")
	}

	close(blockedLock.release)
	select {
	case <-blockedDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("blocked board task did not finish after its lock was released")
	}
}

func TestBoardLockWaitHonorsContextCancellation(t *testing.T) {
	monitor := NewLaneBoardMonitor(nil, nil)
	lock := monitor.lockForBoard(1)
	lock.Lock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	acquired := make(chan bool, 1)
	go func() {
		acquired <- lockWithContext(ctx, lock)
	}()

	select {
	case got := <-acquired:
		if got {
			lock.Unlock()
			t.Fatal("canceled board lock waiter acquired the lock")
		}
	case <-time.After(100 * time.Millisecond):
		lock.Unlock()
		t.Fatal("board lock waiter did not honor context cancellation")
	}
	lock.Unlock()
}

func TestMonitorOverrunWaitsBeforeStartingNextCycle(t *testing.T) {
	const interval = 20 * time.Millisecond
	starts := make(chan time.Time, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int32
	go runMonitorLoop(ctx, interval, func(context.Context) {
		call := calls.Add(1)
		starts <- time.Now()
		if call == 1 {
			time.Sleep(50 * time.Millisecond)
		} else {
			cancel()
		}
	})

	first := <-starts
	second := <-starts
	if elapsed := second.Sub(first); elapsed < 50*time.Millisecond+interval {
		t.Fatalf("overrun loop restarted too soon: elapsed=%s, want at least %s", elapsed, 50*time.Millisecond+interval)
	}
}

func TestEachMonitorLoopRunsAtMostOneCycleAtATime(t *testing.T) {
	monitor := NewLaneBoardMonitor(nil, nil)
	monitor.errorCheckInterval = 2 * time.Millisecond
	monitor.probeScanInterval = time.Hour
	var running atomic.Int32
	var maxRunning atomic.Int32
	monitor.checkErrorsCycle = func(context.Context) {
		current := running.Add(1)
		for {
			maximum := maxRunning.Load()
			if current <= maximum || maxRunning.CompareAndSwap(maximum, current) {
				break
			}
		}
		time.Sleep(8 * time.Millisecond)
		running.Add(-1)
	}
	monitor.probeCycle = func(context.Context) {}

	ctx, cancel := context.WithCancel(context.Background())
	monitor.Start(ctx)
	time.Sleep(35 * time.Millisecond)
	cancel()
	monitor.Wait()

	if maxRunning.Load() != 1 {
		t.Fatalf("error-check cycles overlapped: max concurrency=%d", maxRunning.Load())
	}
}
