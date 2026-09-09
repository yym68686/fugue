package tui

import (
	"fmt"
	"image"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

func (m *Model) View() tea.View {
	v := tea.NewView(m.render())
	v.AltScreen = m.prefs.Mode == "fullscreen"
	if m.prefs.Mouse {
		v.MouseMode = tea.MouseModeCellMotion
	}
	v.WindowTitle = "Fugue | " + Plain(m.snapshot.Title)
	if m.opts.Color && m.prefs.Theme != "terminal" {
		p := theme(m.prefs.Theme)
		v.BackgroundColor = lipgloss.Color(p.background)
		v.ForegroundColor = lipgloss.Color(p.foreground)
	}
	return v
}
func (m *Model) render() string {
	w, h := max(1, m.width), max(1, m.height)
	p := theme(m.prefs.Theme)
	base := strings.Repeat(strings.Repeat(" ", w)+"\n", max(0, h-1)) + strings.Repeat(" ", w)
	if m.opts.Color && m.prefs.Theme != "terminal" {
		base = lipgloss.NewStyle().Background(lipgloss.Color(p.background)).Foreground(lipgloss.Color(p.foreground)).Render(base)
	}
	layers := []*lipgloss.Layer{lipgloss.NewLayer(base)}
	put := func(rect image.Rectangle, text string) {
		rect = rect.Intersect(image.Rect(0, 0, w, h))
		if rect.Empty() {
			return
		}
		lines := strings.Split(text, "\n")
		if len(lines) > rect.Dy() {
			lines = lines[:rect.Dy()]
		}
		for i := range lines {
			lines[i] = pad(lines[i], rect.Dx())
		}
		for len(lines) < rect.Dy() {
			lines = append(lines, strings.Repeat(" ", rect.Dx()))
		}
		layers = append(layers, lipgloss.NewLayer(strings.Join(lines, "\n")).X(rect.Min.X).Y(rect.Min.Y))
	}
	if w < 30 || h < 10 {
		put(image.Rect(0, 0, w, h), "FUGUE\nTerminal too small\nResize or use --plain")
		return lipgloss.NewCompositor(layers...).Render()
	}
	l := m.layout()
	status := first(m.snapshot.Status, "loading")
	title := first(m.snapshot.Title, m.request.Target.Name, "Workspace")
	head := m.style(p.accent).Bold(true).Render("FUGUE") + "  " + m.style(p.foreground).Bold(true).Render(title) + "  " + m.style(m.tone(status)).Render("● "+status)
	put(image.Rect(1, 0, w-1, 1), head)
	for _, control := range l.hits {
		if control.label == "" {
			continue
		}
		style := m.style(p.muted)
		if control.id == "screen:"+m.screen {
			style = m.style(p.accent).Bold(true)
			if m.opts.Color {
				style = style.Background(lipgloss.Color(p.selection))
			}
		}
		put(control.bounds, style.Render(" "+control.label+" "))
	}
	meta := m.snapshot.Subtitle
	if !m.snapshot.ObservedAt.IsZero() {
		meta += "  updated " + m.snapshot.ObservedAt.Local().Format("15:04:05")
	}
	if m.paused {
		meta += "  PAUSED"
	}
	if state := m.fetches["overview"]; state.err != "" {
		age := "no successful snapshot"
		if !m.snapshot.ObservedAt.IsZero() {
			age = m.now.Sub(m.snapshot.ObservedAt).Round(time.Second).String()
		}
		meta += "  STALE " + age + " | " + state.err
	} else if state.pending {
		meta += "  refreshing"
	}
	if m.filter != "" {
		meta += "  filter: " + m.filter
	}
	if m.editing != "" {
		meta = "Search: " + m.input + "▏"
	}
	put(image.Rect(1, 1, w-1, 2), m.style(p.muted).Render(clip(meta, w-2)))
	if m.screen == "dashboard" {
		for i, rect := range l.charts {
			index := m.chartOffset + i
			if index < len(m.snapshot.Series) {
				put(rect, m.renderChart(m.snapshot.Series[index], rect.Dx(), rect.Dy(), i))
			} else {
				put(rect, m.style(p.muted).Render("PERFORMANCE\nWaiting for metric observations"))
			}
		}
		put(l.table, m.renderTable(l.table))
		if !l.summary.Empty() {
			put(l.summary, m.renderSummary(l.summary.Dx(), l.summary.Dy()))
		}
	} else {
		switch m.screen {
		case "resources":
			put(l.body, m.renderTable(l.body))
		case "logs":
			put(l.body, m.renderLogs(l.body))
		case "events":
			put(l.body, m.renderEvents(l.body))
		case "tasks":
			put(l.body, m.renderTextViewport(l.body))
		case "details":
			put(l.body, m.renderTextViewport(l.body))
		case "help":
			put(l.body, m.renderTextViewport(l.body))
		}
	}
	if m.toast != "" && m.now.Before(m.toastUntil) {
		put(image.Rect(1, h-2, w-1, h-1), m.style(p.amber).Render(m.toast))
	} else {
		put(image.Rect(1, h-2, w-1, h-1), m.style(p.muted).Render(m.statusLine()))
	}
	if m.modal != "" {
		rect := m.modalBounds()
		put(rect, m.renderModal(rect))
	}
	output := lipgloss.NewCompositor(layers...).Render()
	if !m.opts.Color {
		return ansi.Strip(output)
	}
	return output
}
func (m *Model) statusLine() string {
	status := fmt.Sprintf("%d resources · %d sources · %s", len(m.rows()), len(m.snapshot.Sources), m.request.Target.Kind)
	partial := 0
	for _, s := range m.snapshot.Sources {
		if s.State != "available" && s.State != "empty" {
			partial++
		}
	}
	if partial > 0 {
		status += fmt.Sprintf(" · %d sources unavailable", partial)
	}
	if len(m.snapshot.Series) > len(m.layout().charts) && m.screen == "dashboard" {
		status += fmt.Sprintf(" · %d more charts (n/b)", len(m.snapshot.Series)-len(m.layout().charts))
	}
	return status + " · ? help"
}
func (m *Model) renderTable(rect image.Rectangle) string {
	p := theme(m.prefs.Theme)
	width, height := rect.Dx(), rect.Dy()
	if width <= 0 || height <= 0 {
		return ""
	}
	if len(m.snapshot.Tables) == 0 {
		return m.style(p.accent).Render("RESOURCES") + "\n" + m.style(p.muted).Render("No resources available")
	}
	table := m.snapshot.Tables[min(m.table, len(m.snapshot.Tables)-1)]
	rows := m.rows()
	columns := min(len(table.Columns), max(1, width/13))
	widths := make([]int, columns)
	for i := range widths {
		widths[i] = max(3, (width-3-columns)/max(1, columns))
	}
	if columns > 0 {
		widths[0] += width - 3 - columns - sum(widths)
	}
	header := []string{}
	for i := 0; i < columns; i++ {
		header = append(header, pad(table.Columns[i], widths[i]))
	}
	lines := []string{m.style(p.accent).Bold(true).Render(pad(strings.ToUpper(table.Title), max(1, width-8)) + "s sort"), m.style(p.muted).Render("   " + strings.Join(header, " "))}
	if len(rows) == 0 {
		lines = append(lines, m.style(p.muted).Render("   No matching resources"))
	}
	for i := m.offset; i < min(len(rows), m.offset+max(1, height-3)); i++ {
		row := rows[i]
		values := []string{}
		for j := 0; j < columns; j++ {
			values = append(values, pad(cell(row, j), widths[j]))
		}
		marker := "   "
		style := m.style(p.foreground)
		selectedText := m.selection.surface == "table" && (m.selection.dragging || m.selection.text != "") && i-m.offset+rect.Min.Y+2 >= min(m.selection.start.Y, m.selection.end.Y) && i-m.offset+rect.Min.Y+2 <= max(m.selection.start.Y, m.selection.end.Y)
		if i == m.selected || selectedText {
			marker = " › "
			style = m.style(p.accent)
			if m.opts.Color {
				style = style.Background(lipgloss.Color(p.selection))
			}
		}
		lines = append(lines, style.Render(pad(marker+strings.Join(values, " "), width)))
	}
	lines = append(lines, m.style(p.muted).Render(fmt.Sprintf("%d/%d  %d columns hidden", min(len(rows), m.selected+1), len(rows), len(table.Columns)-columns)+"  "+scrollPosition(m.offset, len(rows), max(1, height-3), 8)))
	return strings.Join(lines, "\n")
}
func (m *Model) renderSummary(width, height int) string {
	p := theme(m.prefs.Theme)
	lines := []string{m.style(p.accent).Bold(true).Render("STATE & EVIDENCE")}
	for _, f := range m.snapshot.Fields {
		value := f.Value
		if m.screen != "details" {
			value = clip(value, max(1, width-min(19, width/3)-1))
		}
		lines = append(lines, m.style(p.muted).Render(pad(f.Label, min(19, width/3)))+" "+value)
	}
	lines = append(lines, "")
	for _, s := range m.snapshot.Sources {
		meta := s.ID + "  " + s.State
		if !s.ObservedAt.IsZero() {
			meta += " · " + s.ObservedAt.Local().Format("15:04:05")
		}
		lines = append(lines, m.style(m.tone(s.State)).Render(meta))
		if s.Message != "" {
			lines = append(lines, s.Message)
		}
	}
	return strings.Join(lines, "\n")
}
func (m *Model) renderLogs(rect image.Rectangle) string {
	p := theme(m.prefs.Theme)
	lines := []string{}
	for i := m.logOffset; i < min(len(m.snapshot.Logs), m.logOffset+rect.Dy()); i++ {
		line := clip(m.snapshot.Logs[i], rect.Dx())
		y := i - m.logOffset + rect.Min.Y
		start, end := m.selection.start.Y, m.selection.end.Y
		if start > end {
			start, end = end, start
		}
		if (m.selection.dragging || m.selection.text != "") && y >= start && y <= end && m.opts.Color {
			line = m.style(p.foreground).Background(lipgloss.Color(p.selection)).Render(pad(line, rect.Dx()))
		}
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		return m.style(p.muted).Render("No log lines available")
	}
	return strings.Join(lines, "\n")
}
func (m *Model) renderEvents(rect image.Rectangle) string {
	p := theme(m.prefs.Theme)
	lines := []string{m.style(p.accent).Bold(true).Render("EVENTS")}
	for i := m.eventOffset; i < min(len(m.snapshot.Events), m.eventOffset+max(1, rect.Dy()-1)); i++ {
		event := m.snapshot.Events[i]
		lines = append(lines, m.style(m.tone(event.Severity)).Render(event.At.Local().Format("15:04:05")+" "+pad(event.Severity, 9))+" "+event.Message)
	}
	if len(lines) == 1 {
		lines = append(lines, "No events available")
	}
	return strings.Join(lines, "\n")
}
func (m *Model) renderTasks(rect image.Rectangle) string {
	p := theme(m.prefs.Theme)
	lines := []string{m.style(p.accent).Bold(true).Render("COLLECTION & OPERATIONS")}
	keys := []string{}
	for key := range m.fetches {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		s := m.fetches[key]
		status := "idle"
		if s.pending {
			status = "fetching " + m.now.Sub(s.started).Round(time.Second).String()
		} else if s.err != "" {
			status = "retry in " + s.due.Sub(m.now).Round(time.Second).String()
		}
		lines = append(lines, fmt.Sprintf("%-12s %-20s %6dms  failures %d", key, status, s.elapsed.Milliseconds(), s.failures))
		if s.err != "" {
			lines = append(lines, s.err)
		}
	}
	for _, table := range m.snapshot.Tables {
		if table.ID == "operations" {
			lines = append(lines, "", strings.ToUpper(table.Title))
			for _, row := range table.Rows {
				lines = append(lines, strings.Join(row.Cells, "  "))
			}
		}
	}
	return strings.Join(lines, "\n")
}
func (m *Model) help() string {
	return "KEYBOARD\n\n1 Dashboard   2 Resources   3 Events   4 Logs   5 Tasks   6 Details\nTab / Shift+Tab  resource table     Enter  open selection\nJ/K or arrows   move               Backspace / Esc  return\n/  search       S  sort            Ctrl+P  command palette\nP  pause        R  refresh         A  actions\nW  time window  G  graph style     T  theme\nM  mouse        C  copy selection  ,  preferences\n[ / ]  inspect chart time   { / }  select chart         Ctrl+A  select log text\nQ / Ctrl+C  exit\n\nMOUSE\n\nClick tabs or controls; select a row, double-click to open.\nWheel scrolls the focused view. Drag log or table text to select, C to copy.\nDisable mouse with M to use the terminal's own selection.\n\nSOURCES\n\nGaps indicate missing observations. UI refreshes do not create samples.\nActions require a server-backed plan and exact confirmation."
}
func (m *Model) renderModal(rect image.Rectangle) string {
	p := theme(m.prefs.Theme)
	width := max(1, rect.Dx()-4)
	lines := []string{strings.ToUpper(m.modal), ""}
	if m.modal == "confirm" && m.plan != nil {
		lines = append(lines, m.plan.Title)
		lines = append(lines, m.plan.Effects...)
		lines = append(lines, "", "Type: "+m.plan.Confirmation, m.input+"▏")
		if m.executing {
			lines = append(lines, "Submitting…")
		}
	} else if m.modal == "planning" {
		lines = append(lines, "Loading current state and checking preconditions…")
	} else if m.modal == "argument" {
		lines = append(lines, m.action.Label, m.action.Argument, m.input+"▏")
	} else {
		items := m.modalItems()
		for i := m.modalScrollOffset(); i < min(len(items), m.modalScrollOffset()+max(1, rect.Dy()-5)); i++ {
			item := items[i]
			prefix := "  "
			if i == m.modalIndex%max(1, len(items)) {
				prefix = "› "
			}
			lines = append(lines, prefix+item)
		}
	}
	footer := pad("[Cancel]", max(1, width/2)) + "[Select]"
	if m.modal == "confirm" {
		footer = pad("[Cancel]", max(1, width/2)) + "[Confirm]"
	}
	available := max(1, rect.Dy()-3)
	if len(lines) > available {
		if m.modal == "confirm" {
			tail := []string{"Type: " + m.plan.Confirmation, m.input + "▏"}
			lines = append(lines[:max(0, available-len(tail))], tail...)
		} else {
			lines = lines[:available]
		}
	}
	for len(lines) < available {
		lines = append(lines, "")
	}
	lines = append(lines, footer)
	for i := range lines {
		lines[i] = pad(lines[i], width)
	}
	content := strings.Join(lines[:min(len(lines), max(1, rect.Dy()-2))], "\n")
	style := lipgloss.NewStyle().Border(lipgloss.NormalBorder()).Padding(0, 1).Width(max(1, rect.Dx()-2)).Height(max(1, rect.Dy()-2))
	if m.opts.Color {
		style = style.Foreground(lipgloss.Color(p.foreground)).Background(lipgloss.Color(p.selection)).BorderForeground(lipgloss.Color(p.accent))
	}
	return style.Render(content)
}
func first(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
func sum(values []int) int {
	n := 0
	for _, v := range values {
		n += v
	}
	return n
}

func scrollPosition(offset, total, visible, width int) string {
	if total <= visible {
		return ""
	}
	position := min(width-1, offset*width/max(1, total-visible))
	cells := []rune(strings.Repeat("─", width))
	cells[position] = '●'
	return string(cells)
}
