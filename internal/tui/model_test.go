package tui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
)

type testProvider struct {
	load       func(context.Context, Request) (Snapshot, error)
	executions atomic.Int32
}

func (p *testProvider) Load(ctx context.Context, r Request) (Snapshot, error) {
	if p.load != nil {
		return p.load(ctx, r)
	}
	return fixture(100), nil
}
func (p *testProvider) Plan(ctx context.Context, r ActionRequest) (Plan, error) {
	return Plan{ID: "plan", Request: r, Confirmation: "restart sample", ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (p *testProvider) Execute(ctx context.Context, plan Plan) (Receipt, error) {
	p.executions.Add(1)
	return Receipt{Operation: Target{Kind: "operation", ID: "op_1"}}, nil
}
func fixture(count int) Snapshot {
	now := time.Now().Truncate(time.Second)
	s := Snapshot{Target: Target{Kind: "app", ID: "app_1", Name: "Sample API"}, Title: "Sample API", Subtitle: "production · region A", Status: "ready", ObservedAt: now, Admin: true, Fields: []Field{{Label: "Ready", Value: "3 / 3"}, {Label: "Runtime", Value: "shared compute"}, {Label: "Image", Value: "sha256:12abcdef"}}, Sources: []Source{{ID: "pods", State: "available", ObservedAt: now}}, Actions: []Action{{ID: "restart", Label: "Restart application", Enabled: true}}, Logs: []string{"first line", "second line", "third line"}}
	table := Table{ID: "pods", Title: "Pods", Columns: []string{"Pod", "Status", "CPU", "Memory", "Region"}}
	for i := 0; i < count; i++ {
		target := Target{Kind: "pod", ID: fmt.Sprint(i), Name: fmt.Sprintf("Sample %d", i)}
		table.Rows = append(table.Rows, Row{ID: fmt.Sprint(i), Cells: []string{fmt.Sprintf("sample-%03d", i), "ready", fmt.Sprint(i) + "m", "128MiB", "region A"}, Target: &target, Numbers: map[int]float64{2: float64(i)}})
	}
	s.Tables = []Table{table}
	for i, name := range []string{"CPU", "Memory", "Requests", "Latency", "Errors"} {
		series := Series{ID: name, Label: name, Unit: "%", Source: "synthetic fixture", State: "available"}
		for j := 0; j < 120; j++ {
			v := float64((j*(i+1)*7)%81 + 8)
			series.Points = append(series.Points, Point{At: now.Add(time.Duration(j-119) * 5 * time.Second), Value: &v})
		}
		s.Series = append(s.Series, series)
	}
	return s
}
func testModel() *Model {
	m := New(&testProvider{}, Request{Target: Target{Kind: "app", ID: "app_1"}}, Options{Preferences: DefaultPreferences()})
	m.accept("overview", fixture(100))
	return m
}
func press(m *Model, key string) tea.Cmd {
	return m.key(tea.KeyPressMsg{Code: []rune(key)[0], Text: key})
}

func TestRenderBoundsAndNoColor(t *testing.T) {
	for _, width := range []int{24, 60, 80, 100, 140, 200} {
		for _, height := range []int{8, 18, 30, 50} {
			for _, screen := range []string{"dashboard", "resources", "events", "logs", "tasks", "details", "help"} {
				m := testModel()
				m.width, m.height, m.screen = width, height, screen
				m.snapshot.Title = "测试应用 e\u0301 long-name"
				view := m.View()
				lines := strings.Split(view.Content, "\n")
				if len(lines) > height {
					t.Fatalf("%dx%d %s: height %d", width, height, screen, len(lines))
				}
				for _, line := range lines {
					if n := ansi.StringWidth(line); n > width {
						t.Fatalf("%dx%d %s: line width %d %q", width, height, screen, n, line)
					}
				}
				if strings.Contains(view.Content, "\x1b") {
					t.Fatal("color-disabled view contains escape sequence")
				}
			}
		}
	}
}
func TestLayoutHitRegionsMatchVisibleLabels(t *testing.T) {
	m := testModel()
	m.width, m.height = 140, 40
	for _, h := range m.layout().hits {
		if h.label == "" {
			continue
		}
		content := strings.Split(m.View().Content, "\n")
		line := content[h.bounds.Min.Y]
		label := strings.TrimSpace(ansi.Cut(line, h.bounds.Min.X, h.bounds.Max.X))
		if label != h.label {
			t.Fatalf("hit %s shows %q, want %q", h.id, label, h.label)
		}
	}
	for _, h := range m.layout().hits {
		if h.id == "screen:logs" {
			m.click(tea.MouseClickMsg{X: h.bounds.Min.X, Y: h.bounds.Min.Y, Button: tea.MouseLeft})
			if m.screen != "logs" {
				t.Fatal("tab click did not navigate")
			}
			break
		}
	}
}
func TestSchedulerHasOneOutstandingRequestAndBackoff(t *testing.T) {
	m := testModel()
	cmd := m.schedule("overview", true)
	if cmd == nil {
		t.Fatal("missing request")
	}
	if m.schedule("overview", true) != nil {
		t.Fatal("duplicate outstanding request")
	}
	m.Update(fetchedMsg{epoch: m.epoch, section: "overview", err: errors.New("offline")})
	if m.schedule("overview", false) != nil {
		t.Fatal("retry ignored backoff")
	}
	if m.snapshot.Title == "" {
		t.Fatal("failure cleared snapshot")
	}
	m.now = m.now.Add(time.Minute)
	if m.schedule("overview", false) == nil {
		t.Fatal("retry never scheduled")
	}
}
func TestCacheCoalescesAndCopies(t *testing.T) {
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	p := &testProvider{load: func(ctx context.Context, r Request) (Snapshot, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return fixture(2), nil
		case <-ctx.Done():
			return Snapshot{}, ctx.Err()
		}
	}}
	store := NewStore(p)
	var wg sync.WaitGroup
	values := make(chan Snapshot, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := store.Fetch(context.Background(), Request{}, time.Minute, false)
			if err == nil {
				values <- s
			}
		}()
	}
	<-started
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	close(values)
	if calls.Load() != 1 {
		t.Fatalf("duplicate loads %d", calls.Load())
	}
	for s := range values {
		s.Tables[0].Rows[0].Cells[0] = "changed"
	}
	s, err := store.Fetch(context.Background(), Request{}, time.Minute, false)
	if err != nil || s.Tables[0].Rows[0].Cells[0] == "changed" {
		t.Fatal("cache exposed mutable slice")
	}
}
func TestSelectionAndNavigateIgnoreOldResponses(t *testing.T) {
	m := testModel()
	epoch := m.epoch
	m.navigate(Target{Kind: "app", ID: "app_2", Name: "Other"})
	m.Update(fetchedMsg{epoch: epoch, section: "overview", value: fixture(2)})
	if m.snapshot.Title != "Other" {
		t.Fatal("old response overwrote new target")
	}
	m.back()
	if m.snapshot.Title != "Sample API" {
		t.Fatal("back did not restore view")
	}
}
func TestPlanRequiresExactConfirmationAndSubmitsOnce(t *testing.T) {
	p := &testProvider{}
	m := New(p, Request{Target: Target{Kind: "app", ID: "app_1"}}, Options{})
	m.plan = &Plan{ID: "p", Confirmation: "restart sample", ExpiresAt: time.Now().Add(time.Minute)}
	m.modal = "confirm"
	m.input = "restart"
	if cmd := m.key(tea.KeyPressMsg{Code: tea.KeyEnter}); cmd != nil {
		t.Fatal("partial confirmation submitted")
	}
	m.input = "restart sample"
	cmd := m.key(tea.KeyPressMsg{Code: tea.KeyEnter})
	if cmd == nil {
		t.Fatal("missing submit")
	}
	if duplicate := m.execute(); duplicate != nil {
		t.Fatal("duplicate execution")
	}
	cmd()
	if p.executions.Load() != 1 {
		t.Fatal("action not executed once")
	}
}
func TestControlSequenceSanitization(t *testing.T) {
	value := Plain("hello\x1b]52;c;ZXhmaWw=\a\x1b[31mred\x1b[0m\rworld")
	if strings.ContainsAny(value, "\x1b\a\r") {
		t.Fatalf("unsafe controls %q", value)
	}
	if strings.Contains(value, "ZXhmaWw") {
		t.Fatal("OSC payload survived")
	}
}
func TestSeriesDoesNotFabricateOrHideGaps(t *testing.T) {
	now := time.Now()
	v := 42.0
	series := Series{ID: "cpu", Source: "test", Unit: "%", Points: []Point{{At: now.Add(-time.Minute), Value: &v}, {At: now, Value: &v}}}
	merged := MergeSeries(series, series, now)
	if len(merged.Points) != 2 {
		t.Fatal("cached samples duplicated")
	}
	b := Buckets(merged, now.Add(-time.Minute), now, 10)
	if b[5].Last != nil {
		t.Fatal("gap interpolated")
	}
	if b[0].Last == nil || b[9].Last == nil {
		t.Fatal("real samples lost")
	}
}
func TestLogSelectionCellCoordinates(t *testing.T) {
	m := testModel()
	m.accept("logs", fixture(1))
	m.screen = "logs"
	box := m.layout().body
	m.selection = textSelection{start: box.Min, end: box.Min, dragging: true}
	m.release(tea.MouseReleaseMsg{X: box.Min.X + 4, Y: box.Min.Y, Button: tea.MouseLeft})
	if m.selection.text != "first" {
		t.Fatalf("selection %q", m.selection.text)
	}
}

func BenchmarkDashboard(b *testing.B) {
	m := testModel()
	m.accept("overview", fixture(1000))
	m.width, m.height = 200, 50
	b.ReportAllocs()
	for b.Loop() {
		_ = m.View()
	}
}
