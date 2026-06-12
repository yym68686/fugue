package ui

import (
	"strings"
	"testing"

	"fugue/internal/cli/terminal"
	"fugue/internal/cli/viewmodel"
)

func TestRendererWideSnapshot(t *testing.T) {
	t.Parallel()

	renderer := NewRenderer(72, terminal.Palette{Level: terminal.ColorNone})
	got := renderer.Panel("App web", renderOverviewBody(renderer))
	want := `+ App web -------------------------------------------------------------+
| status [ready] replicas [####----] 1/2                               |
| route  [github] -> [build] -> [runtime] -> [edge] ->                 |
| [https://web.example.com]                                            |
|                                                                      |
| o op_import [completed] import                                       |
| * op_deploy [running] deploy - waiting for route                     |
|                                                                      |
| next                                                                 |
|   fugue app logs runtime web --follow                                |
+----------------------------------------------------------------------+
`
	assertSnapshot(t, got, want)
}

func TestRendererNarrowSnapshot(t *testing.T) {
	t.Parallel()

	renderer := NewRenderer(42, terminal.Palette{Level: terminal.ColorNone})
	got := renderer.Panel("App with a very long name", renderOverviewBody(renderer))
	want := `+ App with a very long name -------------+
| status [ready] replicas [####----] 1/2 |
| route  [github] -> [build] ->          |
| [runtime] -> [edge] ->                 |
| [https://web.example.com]              |
|                                        |
| o op_import [completed] import         |
| * op_deploy [running] deploy -         |
| waiting for route                      |
|                                        |
| next                                   |
|   fugue app logs runtime web --follow  |
+----------------------------------------+
`
	assertSnapshot(t, got, want)
}

func TestRendererNoColorAndStateSnapshots(t *testing.T) {
	t.Parallel()

	renderer := NewRenderer(72, terminal.Palette{Level: terminal.ColorNone})
	got := strings.Join([]string{
		renderer.ErrorBlock(viewmodel.EmptyDiagnosisEvidence("no diagnosis available")),
		renderer.ErrorBlock(viewmodel.PermissionDiagnosisEvidence("admin permission required")),
		renderer.ErrorBlock(viewmodel.ErrorDiagnosisEvidence(assertErr("api unavailable"))),
	}, "")
	want := `[empty] no diagnosis available
[permission] admin permission required
[error] api unavailable
`
	assertSnapshot(t, got, want)
}

func TestRendererTableFitsPanelWidth(t *testing.T) {
	t.Parallel()

	renderer := NewRenderer(100, terminal.Palette{Level: terminal.ColorNone})
	table := renderer.Table(
		[]string{"NODE", "STATUS", "REGION", "RUNTIME", "CPU", "MEM", "POLICY"},
		[][]string{{
			"v2202605354515455529",
			"ready",
			"United States",
			"runtime_1778408670_9adbdffeed1f",
			"12%",
			"55%",
			"app,build,shared",
		}},
	)
	got := renderer.Panel("Admin cluster top", strings.Join([]string{"cluster nodes", table}, "\n"))
	for _, line := range strings.Split(strings.TrimRight(got, "\n"), "\n") {
		if len(line) > 100 {
			t.Fatalf("expected table panel line <= 100 chars, got %d %q in\n%s", len(line), line, got)
		}
	}
	if strings.Contains(got, "\n| app,build,shared") {
		t.Fatalf("expected policy label to stay in the table row, got\n%s", got)
	}
}

func renderOverviewBody(renderer Renderer) string {
	timeline := viewmodel.OperationTimelineView{
		State:       viewmodel.ReadyState(),
		ActiveCount: 1,
		LatestID:    "op_deploy",
		Steps: []viewmodel.OperationTimelineStep{
			{ID: "op_import", Type: "import", Status: "completed", Tone: viewmodel.TonePositive},
			{ID: "op_deploy", Type: "deploy", Status: "running", Tone: viewmodel.ToneWarning, Active: true, Message: "waiting for route"},
		},
	}
	body := strings.Join([]string{
		"status " + renderer.StatusChip("ready", viewmodel.TonePositive) + " " + renderer.MetricBar("replicas", 1, 2, 8),
		"route  " + renderer.RouteChain([]RouteSegment{
			{Label: "github", Tone: viewmodel.ToneNeutral},
			{Label: "build", Tone: viewmodel.TonePositive},
			{Label: "runtime", Tone: viewmodel.ToneWarning},
			{Label: "edge", Tone: viewmodel.TonePositive},
			{Label: "https://web.example.com", Tone: viewmodel.TonePositive},
		}),
		"",
		renderer.OperationTimeline(timeline),
		renderer.CopyBlock("next", "fugue app logs runtime web --follow"),
	}, "\n")
	return body
}

type assertErr string

func (e assertErr) Error() string {
	return string(e)
}

func assertSnapshot(t *testing.T, got string, want string) {
	t.Helper()

	if got != want {
		t.Fatalf("snapshot mismatch\n--- got ---\n%s--- want ---\n%s", got, want)
	}
}
