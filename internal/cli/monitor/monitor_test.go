package monitor

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"fugue/internal/cli/terminal"
)

func TestSessionSkipsUnchangedSnapshotsAndPause(t *testing.T) {
	now := time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC)
	snapshot := Snapshot{
		Title:      "Operation op_123",
		ObservedAt: now,
		Summary:    []string{"operation_id=op_123", "status=running"},
	}
	var session Session
	if !session.Accept(snapshot) {
		t.Fatal("expected first snapshot to render")
	}
	if session.Accept(snapshot) {
		t.Fatal("expected unchanged snapshot to be skipped")
	}
	session.ApplyKey("space")
	changed := snapshot
	changed.Summary = []string{"operation_id=op_123", "status=completed"}
	if session.Accept(changed) {
		t.Fatal("expected paused session to skip changed snapshot")
	}
}

func TestRendererFilterSortResizeAndHelp(t *testing.T) {
	now := time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC)
	snapshot := Snapshot{
		Title:      "Cluster top",
		ObservedAt: now,
		Controls:   Controls{Filter: "ready", Sort: "NODE"},
		Sections: []Section{
			{
				Title:   "nodes",
				Headers: []string{"NODE", "STATUS", "CPU"},
				Rows: [][]string{
					{"z-node", "degraded", "90%"},
					{"a-node", "ready", "42%"},
				},
			},
		},
	}
	out := NewRenderer(64, terminal.Palette{}).Render(snapshot)
	for _, want := range []string{"Cluster top", "filter=ready", "sort=NODE", "a-node", "q quit", "space pause"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got %q", want, out)
		}
	}
	if strings.Contains(out, "z-node") {
		t.Fatalf("expected filter to hide z-node, got %q", out)
	}
	narrow := NewRenderer(34, terminal.Palette{}).Render(snapshot)
	for _, line := range strings.Split(strings.TrimRight(narrow, "\n"), "\n") {
		if utf8.RuneCountInString(line) > 34 {
			t.Fatalf("expected narrow render line <= 34 cells, got %d %q in\n%s", utf8.RuneCountInString(line), line, narrow)
		}
	}
}

func TestSnapshotWithErrorPreservesLastScreen(t *testing.T) {
	now := time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC)
	var session Session
	session.Accept(Snapshot{
		Title:      "Project demo",
		ObservedAt: now,
		Summary:    []string{"project=demo", "apps=1"},
		Sections: []Section{{
			Title:   "apps",
			Headers: []string{"APP", "STATUS"},
			Rows:    [][]string{{"web", "ready"}},
		}},
	})
	snapshot := session.SnapshotWithError(errors.New("temporary 502"), now.Add(time.Second))
	out := NewRenderer(80, terminal.Palette{}).Render(snapshot)
	for _, want := range []string{"transient_error=temporary 502", "web", "ready"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected error overlay to preserve %q, got %q", want, out)
		}
	}
}

func TestCtrlCSummaryIncludesResumeHint(t *testing.T) {
	now := time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC)
	lines := CtrlCSummary(Snapshot{
		Title:      "Operation op_123",
		ObservedAt: now,
		Summary:    []string{"operation_id=op_123", "status=running"},
		ResumeHint: "fugue operation watch op_123",
	})
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"last_view=Operation op_123", "operation_id=op_123", "resume=fugue operation watch op_123"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected summary to contain %q, got %q", want, joined)
		}
	}
}
