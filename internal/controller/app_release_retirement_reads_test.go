package controller

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

type blockingRetirementEdgeLister struct {
	entered chan struct{}
	resume  chan struct{}
}

func (l blockingRetirementEdgeLister) ListActiveEdgeNodes(string) ([]model.EdgeNode, []model.EdgeGroup, error) {
	l.entered <- struct{}{}
	<-l.resume
	return nil, nil, nil
}

func TestRetirementPublicationReadsBoundedAndCancellationJoins(t *testing.T) {
	ctx, cancel := context.WithCancel(withReleaseRetirementReadLimit(context.Background()))
	defer cancel()
	lister := blockingRetirementEdgeLister{make(chan struct{}, 100), make(chan struct{})}
	observer := storeSafeRolloutEdgeBundleObserver{Store: lister}
	var wg sync.WaitGroup
	var cancelled atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := observer.requiredNodesForObservation(ctx)
			if errors.Is(err, context.Canceled) {
				cancelled.Add(1)
			} else if err != nil {
				t.Errorf("read: %v", err)
			}
		}()
	}
	for i := 0; i < appReleaseRetirementReadConcurrency; i++ {
		select {
		case <-lister.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("read slots were not available")
		}
	}
	select {
	case <-lister.entered:
		t.Fatal("publication reads exceeded the heavy read limit")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	close(lister.resume)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled readers did not join")
	}
	if cancelled.Load() != 100-appReleaseRetirementReadConcurrency {
		t.Fatalf("cancelled reads=%d", cancelled.Load())
	}
	if len(lister.entered) != 0 {
		t.Fatal("a cancelled waiter started a publication read")
	}
}

func TestRetirementReadSlotsReleasedWithoutCachingEvidence(t *testing.T) {
	ctx := withReleaseRetirementReadLimit(context.Background())
	for round := 0; round < 2; round++ {
		var release []func()
		for i := 0; i < appReleaseRetirementReadConcurrency; i++ {
			unlock, err := acquireReleaseRetirementRead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			release = append(release, unlock)
		}
		blocked, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		if _, err := acquireReleaseRetirementRead(blocked); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("saturated read slots: %v", err)
		}
		cancel()
		for _, unlock := range release {
			unlock()
		}
	}
}
