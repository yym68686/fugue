package console

import (
	"fmt"
	"strings"

	"fugue/internal/cli/monitor"
	"fugue/internal/cli/terminal"
	"fugue/internal/cli/ui"
)

type Page string

const (
	PageProjects Page = "Projects"
	PageApps     Page = "Apps"
	PageDetail   Page = "Project detail"
	PageOps      Page = "Operations"
	PageLogs     Page = "Logs"
	PageRuntime  Page = "Runtime"
	PageAdmin    Page = "Admin"
)

var DefaultPages = []Page{PageProjects, PageApps, PageDetail, PageOps, PageLogs, PageRuntime, PageAdmin}

type StateKind string

const (
	StateLoading    StateKind = "loading"
	StateReady      StateKind = "ready"
	StateEmpty      StateKind = "empty"
	StateError      StateKind = "error"
	StatePermission StateKind = "permission"
	StateOffline    StateKind = "offline"
)

type State struct {
	Kind    StateKind `json:"kind"`
	Message string    `json:"message,omitempty"`
}

type Row struct {
	Cells []string `json:"cells"`
}

type Table struct {
	Title   string   `json:"title"`
	Headers []string `json:"headers"`
	Rows    []Row    `json:"rows"`
}

type View struct {
	State       State    `json:"state"`
	Project     string   `json:"project,omitempty"`
	Preview     bool     `json:"preview"`
	Pages       []Page   `json:"pages"`
	ActivePage  Page     `json:"active_page"`
	FocusIndex  int      `json:"focus_index"`
	PaletteOpen bool     `json:"palette_open"`
	Mouse       bool     `json:"mouse"`
	LowWidth    bool     `json:"low_width"`
	Summary     []string `json:"summary,omitempty"`
	Tables      []Table  `json:"tables,omitempty"`
	Logs        []string `json:"logs,omitempty"`
	Actions     []string `json:"actions,omitempty"`
}

type Model struct {
	View View
}

func NewModel(view View) Model {
	if len(view.Pages) == 0 {
		view.Pages = append([]Page(nil), DefaultPages...)
	}
	if view.ActivePage == "" {
		view.ActivePage = view.Pages[0]
	}
	view.Preview = true
	return Model{View: view}
}

func (m Model) UpdateKey(key string) Model {
	key = strings.ToLower(strings.TrimSpace(key))
	switch key {
	case "tab", "right", "l":
		m.View.ActivePage = nextPage(m.View.Pages, m.View.ActivePage, 1)
		m.View.FocusIndex = 0
	case "shift+tab", "left", "h":
		m.View.ActivePage = nextPage(m.View.Pages, m.View.ActivePage, -1)
		m.View.FocusIndex = 0
	case "down", "j":
		m.View.FocusIndex++
	case "up", "k":
		if m.View.FocusIndex > 0 {
			m.View.FocusIndex--
		}
	case "/", "cmd+k", "command+k":
		m.View.PaletteOpen = true
	case "esc":
		m.View.PaletteOpen = false
	case "mouse:on":
		m.View.Mouse = true
	case "mouse:off":
		m.View.Mouse = false
	}
	maxRows := maxVisibleRows(m.View)
	if maxRows > 0 && m.View.FocusIndex >= maxRows {
		m.View.FocusIndex = maxRows - 1
	}
	return m
}

type Renderer struct {
	Width   int
	Palette terminal.Palette
}

func NewRenderer(width int, palette terminal.Palette) Renderer {
	if width <= 0 {
		width = ui.DefaultWidth
	}
	return Renderer{Width: width, Palette: palette}
}

func (r Renderer) Render(model Model) string {
	view := model.View
	view.LowWidth = r.Width < 72
	renderer := ui.NewRenderer(r.Width, r.Palette)
	body := []string{
		"preview=true",
		"state=" + stateLabel(view.State),
		"page=" + string(view.ActivePage),
	}
	if view.Project != "" {
		body = append(body, "project="+view.Project)
	}
	if view.Mouse {
		body = append(body, "mouse=optional")
	}
	if len(view.Summary) > 0 {
		body = append(body, "", "summary")
		body = append(body, view.Summary...)
	}
	body = append(body, "", renderTabs(view.Pages, view.ActivePage, view.LowWidth))
	for _, table := range visibleTables(view) {
		body = append(body, "", table.Title)
		rows := make([][]string, 0, len(table.Rows))
		for index, row := range table.Rows {
			cells := append([]string(nil), row.Cells...)
			if index == view.FocusIndex {
				if len(cells) == 0 {
					cells = []string{">"}
				} else {
					cells[0] = "> " + cells[0]
				}
			}
			rows = append(rows, cells)
		}
		body = append(body, renderer.Table(table.Headers, rows))
	}
	if len(view.Logs) > 0 {
		body = append(body, "", "logs")
		body = append(body, trimLogLines(view.Logs, 8)...)
	}
	if len(view.Actions) > 0 {
		body = append(body, "", "actions")
		body = append(body, view.Actions...)
	}
	body = append(body, "", "tab next  shift+tab previous  arrows focus  / palette  ? help  q quit")
	if view.PaletteOpen {
		body = append(body, "", commandPalette(view))
	}
	return renderer.Panel("Fugue console", strings.Join(body, "\n"))
}

func SnapshotFromView(view View) monitor.Snapshot {
	sections := make([]monitor.Section, 0, len(view.Tables))
	for _, table := range view.Tables {
		rows := make([][]string, 0, len(table.Rows))
		for _, row := range table.Rows {
			rows = append(rows, row.Cells)
		}
		sections = append(sections, monitor.Section{Title: table.Title, Headers: table.Headers, Rows: rows})
	}
	return monitor.Snapshot{Title: "Fugue console", Summary: view.Summary, Sections: sections}
}

func renderTabs(pages []Page, active Page, lowWidth bool) string {
	if lowWidth {
		return "nav=" + string(active) + " (" + fmt.Sprintf("%d pages", len(pages)) + ")"
	}
	parts := make([]string, 0, len(pages))
	for _, page := range pages {
		label := string(page)
		if page == active {
			label = "[" + label + "]"
		}
		parts = append(parts, label)
	}
	return strings.Join(parts, "  ")
}

func commandPalette(view View) string {
	commands := []string{"project:open", "app:open", "operation:explain", "logs:tail", "runtime:show", "admin:cockpit"}
	return "command palette\n" + strings.Join(commands, "\n")
}

func visibleTables(view View) []Table {
	return view.Tables
}

func trimLogLines(lines []string, limit int) []string {
	if len(lines) <= limit {
		return lines
	}
	return lines[len(lines)-limit:]
}

func stateLabel(state State) string {
	if state.Kind == "" {
		return string(StateReady)
	}
	if state.Message == "" {
		return string(state.Kind)
	}
	return string(state.Kind) + " " + state.Message
}

func nextPage(pages []Page, active Page, delta int) Page {
	if len(pages) == 0 {
		return active
	}
	index := 0
	for i, page := range pages {
		if page == active {
			index = i
			break
		}
	}
	index = (index + delta + len(pages)) % len(pages)
	return pages[index]
}

func maxVisibleRows(view View) int {
	maxRows := 0
	for _, table := range visibleTables(view) {
		if len(table.Rows) > maxRows {
			maxRows = len(table.Rows)
		}
	}
	return maxRows
}
