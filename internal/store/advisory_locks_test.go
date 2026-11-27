package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPostgresAdvisoryLocksPreserveIndependentWriters(t *testing.T) {
	const fixture = "advisory-locks"
	s := newStoreTestDatabase(t, fixture)
	peer := newStoreTestDatabase(t, fixture)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := s.WithAdvisoryLock(ctx, "producer", func() error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("initial PostgreSQL lock was not acquired: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if acquired, err := peer.WithAdvisoryLock(ctx, "producer", nil); err != nil || acquired {
		t.Fatal("same name was not excluded", err)
	}
	sentinel := errors.New("independent writer failure")
	if acquired, err := peer.WithAdvisoryLock(ctx, "dns", func() error { return sentinel }); !acquired || !errors.Is(err, sentinel) {
		t.Fatal("independent writer starved", err)
	}
	if acquired, err := s.WithAdvisoryLock(ctx, "dns", nil); !acquired || err != nil {
		t.Fatal("error retained lock", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if acquired, err := peer.WithAdvisoryLock(ctx, "producer", nil); !acquired || err != nil {
		t.Fatal("producer did not release lock", err)
	}
	canceledCtx, cancelImmediately := context.WithCancel(ctx)
	cancelImmediately()
	if acquired, err := peer.WithAdvisoryLock(canceledCtx, "producer", func() error { t.Error("canceled writer entered"); return nil }); acquired || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled lock acquired", err)
	}
}
