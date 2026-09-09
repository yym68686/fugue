package tui

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
)

type soakProvider struct {
	testProvider
	loads  atomic.Int64
	active atomic.Int64
}

func (p *soakProvider) Load(ctx context.Context, r Request) (Snapshot, error) {
	p.loads.Add(1)
	if time.Now().Unix()%60 >= 30 && time.Now().Unix()%60 < 35 {
		return Snapshot{}, fmt.Errorf("synthetic outage")
	}
	s := fixture(1000)
	s.Target = r.Target
	ops := Table{ID: "operations", Title: "Operations", Columns: []string{"Operation", "Status"}}
	for i := 0; i < 10; i++ {
		ops.Rows = append(ops.Rows, Row{ID: fmt.Sprint(i), Cells: []string{fmt.Sprint(i), "running"}})
	}
	s.Tables = append(s.Tables, ops)
	return s, nil
}
func (p *soakProvider) Watch(ctx context.Context, target Target, cursor string, notify func(Notice)) error {
	p.active.Add(1)
	defer p.active.Add(-1)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case at := <-ticker.C:
			notify(Notice{Cursor: at.String(), Logs: []string{"repeated log record"}})
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func TestSoak(t *testing.T) {
	durationText := os.Getenv("FUGUE_TUI_SOAK")
	if durationText == "" {
		t.Skip("set FUGUE_TUI_SOAK=10m for sustained runtime validation")
	}
	duration, err := time.ParseDuration(durationText)
	if err != nil || duration < time.Minute {
		t.Fatal("soak requires at least 1m")
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration+time.Minute)
	defer cancel()
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	provider := &soakProvider{}
	m := New(provider, Request{Target: Target{Kind: "app", ID: "app-a"}}, Options{Preferences: DefaultPreferences(), Interval: time.Second})
	m.ctx = ctx
	program := tea.NewProgram(m, tea.WithContext(ctx), tea.WithInput(input), tea.WithOutput(io.Discard), tea.WithWindowSize(200, 50), tea.WithFPS(30))
	finished := make(chan error, 1)
	go func() { _, err := program.Run(); finished <- err }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	started := time.Now()
	baselineG := 0
	var baseline runtime.MemStats
	iteration := 0
	for time.Since(started) < duration {
		select {
		case err := <-finished:
			t.Fatalf("early exit: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
		iteration++
		key := "1"
		switch iteration % 12 {
		case 1, 2, 3:
			key = "4"
		case 4:
			key = "p"
		case 5:
			key = "p"
		case 6:
			key = "2"
		case 7:
			key = "j"
		case 8:
			key = "6"
		case 9:
			key = "?"
		case 10:
			key = "1"
		}
		program.Send(tea.KeyPressMsg{Code: rune(key[0]), Text: key})
		if iteration%15 == 0 {
			program.Send(tea.WindowSizeMsg{Width: 60, Height: 18})
		}
		if iteration%15 == 1 {
			program.Send(tea.WindowSizeMsg{Width: 200, Height: 50})
		}
		if iteration == 30 {
			runtime.GC()
			runtime.ReadMemStats(&baseline)
			baselineG = runtime.NumGoroutine()
		}
	}
	program.Quit()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("quit stalled")
	}
	m.stopWatch()
	cancel()
	if m.viewCancel != nil {
		m.viewCancel()
	}
	deadline := time.Now().Add(time.Second)
	for provider.active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)
	finalG := runtime.NumGoroutine()
	t.Logf("duration=%s loads=%d baseline_goroutines=%d final_goroutines=%d heap_baseline=%d heap_final=%d retained_logs=%d active_streams=%d", time.Since(started).Round(time.Second), provider.loads.Load(), baselineG, finalG, baseline.HeapAlloc, final.HeapAlloc, len(m.snapshot.Logs), provider.active.Load())
	if finalG > baselineG+8 || final.HeapAlloc > baseline.HeapAlloc+16*1024*1024 || len(m.snapshot.Logs) > maxLogLines || provider.active.Load() != 0 {
		t.Fatal("unbounded resource growth")
	}
}
