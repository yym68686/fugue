package imagebilling

import (
	"context"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"
)

func TestBillingImageStorageRefreshSchedulerDebouncesRapidRequests(t *testing.T) {
	t.Parallel()

	scheduler := NewScheduler(20*time.Millisecond, time.Second)
	t.Cleanup(func() {
		scheduler.Wait()
	})
	logger := log.New(io.Discard, "", 0)
	started := make(chan string, 4)
	var calls atomic.Int32

	refresh := func(context.Context, string) error {
		calls.Add(1)
		started <- "started"
		return nil
	}

	scheduler.Schedule("tenant-a", logger, refresh)
	scheduler.Schedule("tenant-a", logger, refresh)
	scheduler.Schedule("tenant-a", logger, refresh)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for debounced refresh")
	}

	time.Sleep(80 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one debounced refresh, got %d", got)
	}
}

func TestBillingImageStorageRefreshSchedulerRunsTrailingRefreshAfterInFlightRequest(t *testing.T) {
	t.Parallel()

	scheduler := NewScheduler(10*time.Millisecond, time.Second)
	t.Cleanup(func() {
		scheduler.Wait()
	})
	logger := log.New(io.Discard, "", 0)
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var calls atomic.Int32

	refresh := func(context.Context, string) error {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return nil
	}

	scheduler.Schedule("tenant-a", logger, refresh)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first refresh")
	}

	scheduler.Schedule("tenant-a", logger, refresh)
	release <- struct{}{}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for trailing refresh")
	}

	release <- struct{}{}
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected two refreshes after trailing schedule, got %d", got)
	}
}
