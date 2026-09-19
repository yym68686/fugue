package store

import (
	"context"
	"errors"
	"testing"
)

func TestFileAdvisoryLocksPreserveIndependentWriters(t *testing.T) {
	s := New(t.TempDir() + "/state.json")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := s.WithAdvisoryLock(context.Background(), "producer", func() error { close(entered); <-release; return nil })
		done <- err
	}()
	<-entered
	if acquired, err := s.WithAdvisoryLock(context.Background(), "producer", func() error { t.Error("duplicate producer entered"); return nil }); err != nil || acquired {
		t.Fatal("same name was not excluded", err)
	}
	sentinel := errors.New("independent writer failure")
	if acquired, err := s.WithAdvisoryLock(context.Background(), "dns", func() error { return sentinel }); !acquired || !errors.Is(err, sentinel) {
		t.Fatal("independent writer starved", err)
	}
	if acquired, err := s.WithAdvisoryLock(context.Background(), "dns", nil); !acquired || err != nil {
		t.Fatal("error retained lock", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if acquired, err := s.WithAdvisoryLock(context.Background(), "producer", nil); !acquired || err != nil {
		t.Fatal("producer did not release lock", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if acquired, err := s.WithAdvisoryLock(ctx, "producer", func() error { t.Error("canceled writer entered"); return nil }); acquired || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled lock acquired", err)
	}
}
