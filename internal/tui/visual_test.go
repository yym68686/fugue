package tui

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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
	for _, scenario := range []string{"startup", "gap"} {
		for _, size := range [][2]int{{140, 40}, {200, 50}} {
			m := testModel()
			m.width, m.height, m.opts.Color = size[0], size[1], true
			m.prefs.Theme, m.prefs.Graph = "carbon", "braille"
			for i := range m.snapshot.Series {
				series := m.snapshot.Series[i]
				series.Interval = 30 * time.Second
				series.Points = series.Points[len(series.Points)-7:]
				for j := range series.Points {
					series.Points[j].At = m.now.Add(time.Duration(j-6) * 30 * time.Second)
					if scenario == "gap" && j == 3 {
						series.Points[j].Value = nil
					}
				}
				m.snapshot.Series[i], m.series[series.ID] = series, series
			}
			name := fmt.Sprintf("%s-%dx%d-carbon.ansi", scenario, size[0], size[1])
			if err := os.WriteFile(filepath.Join(dir, name), []byte(m.View().Content), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, size := range [][2]int{{60, 18}, {80, 30}, {100, 30}, {140, 40}, {200, 50}, {250, 72}} {
		m := New(&testProvider{}, Request{Target: Target{Kind: "cluster"}}, Options{Preferences: DefaultPreferences()})
		m.accept("overview", clusterFixture(m.now))
		m.width, m.height, m.opts.Color = size[0], size[1], true
		for _, palette := range []string{"carbon", "light", "terminal"} {
			m.prefs.Theme = palette
			name := fmt.Sprintf("cluster-%dx%d-%s.ansi", size[0], size[1], palette)
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
