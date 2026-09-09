package tui

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

type watchTestProvider struct {
	testProvider
	opened chan Target
	closed chan Target
}

func (p *watchTestProvider) Watch(ctx context.Context, target Target, cursor string, notify func(Notice)) error {
	p.opened <- target
	<-ctx.Done()
	p.closed <- target
	return ctx.Err()
}
func receiveTarget(t *testing.T, ch <-chan Target) Target {
	t.Helper()
	select {
	case target := <-ch:
		return target
	case <-time.After(time.Second):
		t.Fatal("subscription did not start/close")
		return Target{}
	}
}
func TestWatchStopsOnNavigationPauseAndIgnoresOldEvents(t *testing.T) {
	p := &watchTestProvider{opened: make(chan Target, 8), closed: make(chan Target, 8)}
	m := New(p, Request{Target: Target{Kind: "project", ID: "project-a"}}, Options{})
	m.syncWatch()
	receiveTarget(t, p.opened)
	epoch, generation := m.epoch, m.watchGeneration
	m.setTarget(Request{Target: Target{Kind: "project", ID: "project-b"}})
	receiveTarget(t, p.closed)
	m.Update(watchMsg{epoch: epoch, generation: generation, notice: Notice{Logs: []string{"old tenant data"}}})
	if len(m.snapshot.Logs) != 0 {
		t.Fatal("old subscription leaked into new target")
	}
	receiveTarget(t, p.opened)
	m.paused = true
	m.syncWatch()
	receiveTarget(t, p.closed)
	if m.watchCancel != nil {
		t.Fatal("pause retained subscription")
	}
}
func TestStreamingLogsPreserveRepeatedTextAndBoundMemory(t *testing.T) {
	m := New(&testProvider{}, Request{Target: Target{Kind: "app", ID: "app-a"}}, Options{})
	for i := 0; i < maxLogLines+10; i++ {
		m.Update(watchMsg{epoch: m.epoch, generation: m.watchGeneration, notice: Notice{Cursor: fmt.Sprint(i), Logs: []string{"same text\x1b]52;c;secret\a"}}})
	}
	if len(m.snapshot.Logs) != maxLogLines {
		t.Fatalf("buffer = %d", len(m.snapshot.Logs))
	}
	if strings.Contains(m.snapshot.Logs[0], "\x1b") || strings.Contains(m.snapshot.Logs[0], "secret") {
		t.Fatal("unsafe streamed control sequence")
	}
	before := len(m.snapshot.Logs)
	m.Update(watchMsg{epoch: m.epoch, generation: m.watchGeneration, notice: Notice{Cursor: m.snapshot.LogCursor, Logs: []string{"duplicate cursor"}}})
	if len(m.snapshot.Logs) != before || m.snapshot.Logs[before-1] == "duplicate cursor" {
		t.Fatal("cursor replay appended")
	}
}
