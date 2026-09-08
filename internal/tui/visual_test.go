package tui

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Opt-in export for terminal-emulator screenshot review. Fixtures are synthetic
// and never appear as production data or require a network connection.
func TestExportVisualFixtures(t *testing.T) {
	dir := os.Getenv("FUGUE_TUI_SNAPSHOT_DIR")
	if dir == "" {
		t.Skip("set FUGUE_TUI_SNAPSHOT_DIR for visual review")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, size := range [][2]int{{60, 18}, {80, 30}, {100, 30}, {140, 40}, {200, 50}} {
		for _, theme := range []string{"carbon", "light", "terminal"} {
			m := testModel()
			m.width, m.height = size[0], size[1]
			m.opts.Color = true
			m.prefs.Theme = theme
			name := fmt.Sprintf("dashboard-%dx%d-%s.ansi", size[0], size[1], theme)
			if err := os.WriteFile(filepath.Join(dir, name), []byte(m.View().Content), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	m := testModel()
	m.width, m.height = 100, 30
	m.opts.Color = true
	m.screen = "logs"
	m.accept("logs", fixture(1))
	if err := os.WriteFile(filepath.Join(dir, "logs-100x30.ansi"), []byte(m.View().Content), 0600); err != nil {
		t.Fatal(err)
	}
}
