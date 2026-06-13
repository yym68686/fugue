package console

import (
	"strings"
	"testing"
	"unicode/utf8"

	"fugue/internal/cli/terminal"
)

func TestKeyboardNavigationAndCommandPalette(t *testing.T) {
	model := NewModel(sampleView())
	model = model.UpdateKey("tab")
	if model.View.ActivePage != PageApps {
		t.Fatalf("expected tab to move to apps, got %s", model.View.ActivePage)
	}
	model = model.UpdateKey("down").UpdateKey("/")
	if model.View.FocusIndex != 1 || !model.View.PaletteOpen {
		t.Fatalf("expected focus and palette update, got focus=%d palette=%t", model.View.FocusIndex, model.View.PaletteOpen)
	}
	model = model.UpdateKey("esc").UpdateKey("shift+tab")
	if model.View.PaletteOpen || model.View.ActivePage != PageProjects {
		t.Fatalf("expected escape and reverse tab, got page=%s palette=%t", model.View.ActivePage, model.View.PaletteOpen)
	}
}

func TestRendererWideNarrowStatesAndMouseOptional(t *testing.T) {
	model := NewModel(sampleView()).UpdateKey("mouse:on").UpdateKey("/")
	wide := NewRenderer(100, terminal.Palette{}).Render(model)
	for _, want := range []string{"Fugue console", "preview=true", "[Projects]", "mouse=optional", "command palette", "tab next"} {
		if !strings.Contains(wide, want) {
			t.Fatalf("expected wide render to contain %q, got %q", want, wide)
		}
	}
	narrow := NewRenderer(42, terminal.Palette{}).Render(model)
	for _, want := range []string{"nav=Projects", "7 pages", "command palette"} {
		if !strings.Contains(narrow, want) {
			t.Fatalf("expected narrow render to contain %q, got %q", want, narrow)
		}
	}
	for _, line := range strings.Split(strings.TrimRight(narrow, "\n"), "\n") {
		if utf8.RuneCountInString(line) > 42 {
			t.Fatalf("expected narrow render line <= 42 cells, got %d %q\n%s", utf8.RuneCountInString(line), line, narrow)
		}
	}
}

func TestEmptyErrorPermissionOfflineStatesRender(t *testing.T) {
	for _, state := range []State{
		{Kind: StateLoading, Message: "loading projects"},
		{Kind: StateEmpty, Message: "no projects"},
		{Kind: StateError, Message: "api failed"},
		{Kind: StatePermission, Message: "admin required"},
		{Kind: StateOffline, Message: "network unavailable"},
	} {
		view := sampleView()
		view.State = state
		out := NewRenderer(80, terminal.Palette{}).Render(NewModel(view))
		if !strings.Contains(out, string(state.Kind)) || !strings.Contains(out, state.Message) {
			t.Fatalf("expected state %s to render, got %q", state.Kind, out)
		}
	}
}

func sampleView() View {
	return View{
		State:      State{Kind: StateReady},
		Project:    "demo",
		ActivePage: PageProjects,
		Summary:    []string{"projects=1", "apps=1", "operations=1"},
		Tables: []Table{
			{Title: string(PageProjects), Headers: []string{"PROJECT", "APPS"}, Rows: []Row{{Cells: []string{"demo", "1"}}}},
			{Title: string(PageApps), Headers: []string{"APP", "STATUS"}, Rows: []Row{{Cells: []string{"web", "ready"}}, {Cells: []string{"worker", "degraded"}}}},
		},
		Logs:    []string{"line 1", "line 2"},
		Actions: []string{"restart requires confirmation", "redeploy requires confirmation"},
	}
}
