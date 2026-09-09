package tui

import (
	"context"
	"fmt"
	"image"
	"sort"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
)

type textSelection struct {
	start, end image.Point
	dragging   bool
	text       string
	surface    string
}

func (m *Model) paste(text string) {
	if m.editing != "" || m.modal == "argument" || m.modal == "confirm" {
		m.input = clip(m.input+Plain(text), 512)
		if m.editing == "search" {
			m.filter = m.input
			m.selected = 0
			m.offset = 0
		}
	}
}
func (m *Model) key(msg tea.KeyPressMsg) tea.Cmd {
	key := msg.String()
	if key == "ctrl+c" {
		return tea.Quit
	}
	if m.executing {
		if key == "q" {
			return tea.Quit
		}
		return nil
	}
	if m.editing != "" || m.modal == "argument" || m.modal == "confirm" {
		switch key {
		case "esc":
			m.editing = ""
			m.modal = ""
			m.input = ""
			return nil
		case "enter":
			if m.editing != "" {
				m.editing = ""
				return nil
			}
			if m.modal == "argument" {
				arg := strings.TrimSpace(m.input)
				if arg == "" {
					return nil
				}
				if m.action.ID == "scale" {
					n, err := strconv.Atoi(arg)
					if err != nil || n < 0 {
						m.notify("Replicas must be a nonnegative integer")
						return nil
					}
				}
				return m.makePlan(m.action, arg)
			}
			if m.plan != nil && m.input == m.plan.Confirmation {
				return m.execute()
			}
			m.notify("Confirmation must match exactly")
			return nil
		case "backspace":
			r := []rune(m.input)
			if len(r) > 0 {
				m.input = string(r[:len(r)-1])
			}
		default:
			if msg.Key().Text != "" {
				m.input = clip(m.input+Plain(msg.Key().Text), 512)
			}
		}
		if m.editing == "search" {
			m.filter = m.input
			m.selected = 0
			m.offset = 0
		}
		return nil
	}
	if m.modal != "" {
		switch key {
		case "esc", "q":
			m.modal = ""
		case "j", "down":
			m.modalIndex = (m.modalIndex + 1) % max(1, len(m.modalItems()))
		case "k", "up":
			m.modalIndex = max(0, m.modalIndex-1)
		case "enter":
			return m.activateModal()
		}
		return nil
	}
	for action, binding := range m.prefs.Keys {
		if key == binding {
			key = defaultKeys()[action]
			break
		}
	}
	switch key {
	case "q":
		return tea.Quit
	case "esc", "backspace":
		return m.back()
	case "r":
		return tea.Batch(m.schedule("overview", true), m.schedule("metrics", true))
	case "p", "space":
		m.paused = !m.paused
	case "/":
		m.editing = "search"
		m.input = m.filter
	case "ctrl+p", ":":
		m.modal = "palette"
		m.modalIndex = 0
	case "?":
		m.screen = "help"
	case "1":
		m.screen = "dashboard"
	case "2":
		m.screen = "resources"
	case "3":
		m.screen = "events"
	case "4":
		if reason := m.snapshot.UnavailableScreens["logs"]; reason != "" {
			m.notify(reason)
			return nil
		}
		m.screen = "logs"
		return m.schedule("logs", false)
	case "5":
		m.screen = "tasks"
	case "6":
		m.screen = "details"
	case "tab", "right":
		m.table = (m.table + 1) % max(1, len(m.snapshot.Tables))
		m.selected = 0
		m.offset = 0
	case "shift+tab", "left":
		m.table = (m.table + max(1, len(m.snapshot.Tables)) - 1) % max(1, len(m.snapshot.Tables))
		m.selected = 0
		m.offset = 0
	case "j", "down":
		m.move(1)
	case "k", "up":
		m.move(-1)
	case "pgdown":
		m.move(max(1, m.height/2))
	case "pgup":
		m.move(-max(1, m.height/2))
	case "home":
		m.offset = 0
		m.selected = 0
		m.logOffset = 0
		m.eventOffset = 0
		m.textOffset = 0
	case "end":
		m.selected = max(0, len(m.rows())-1)
		m.logOffset = max(0, len(m.snapshot.Logs)-m.height+8)
		m.eventOffset = max(0, len(m.snapshot.Events)-m.height+8)
		m.textOffset = max(0, len(m.textLines())-m.layout().body.Dy())
	case "enter":
		return m.openSelected()
	case "s":
		m.modal = "sort"
		m.modalIndex = 0
	case "a":
		m.modal = "actions"
		m.modalIndex = 0
	case "t":
		m.prefs.Theme = cycle([]string{"carbon", "light", "terminal"}, m.prefs.Theme)
		return m.save()
	case "w":
		m.prefs.Window = cycle([]string{"5m", "15m", "1h"}, m.prefs.Window)
		m.request.Window, _ = time.ParseDuration(m.prefs.Window)
		m.graphCursor = -1
		return tea.Batch(m.save(), m.schedule("metrics", true))
	case "g":
		m.prefs.Graph = cycle([]string{"braille", "block", "line", "ascii"}, m.prefs.Graph)
		return m.save()
	case "m":
		m.prefs.Mouse = !m.prefs.Mouse
		return m.save()
	case "c", "y":
		text := m.copyText()
		if text != "" {
			m.notify("Copied selection")
			return tea.SetClipboard(text)
		}
	case "ctrl+a":
		m.selection.text = strings.Join(m.snapshot.Logs, "\n")
		m.notify("Log text selected")
	case ",":
		m.modal = "settings"
		m.modalIndex = 0
	case "{":
		m.graphIndex = max(0, m.graphIndex-1)
	case "}":
		m.graphIndex = min(max(0, len(m.layout().charts)-1), m.graphIndex+1)
	case "[":
		m.graphCursor = max(0, m.graphCursor-1)
	case "]":
		m.graphCursor++
	case "n":
		m.chartOffset = (m.chartOffset + max(1, len(m.layout().charts))) % max(1, len(m.snapshot.Series))
	case "b":
		m.chartOffset = max(0, m.chartOffset-max(1, len(m.layout().charts)))
	}
	m.ensureVisible()
	return nil
}
func defaultKeys() map[string]string {
	return map[string]string{"quit": "q", "refresh": "r", "pause": "p", "search": "/", "palette": "ctrl+p", "back": "backspace", "copy": "c", "theme": "t", "window": "w", "graph": "g", "settings": ","}
}
func cycle(values []string, current string) string {
	for i, v := range values {
		if v == current {
			return values[(i+1)%len(values)]
		}
	}
	return values[0]
}
func (m *Model) move(delta int) {
	switch m.screen {
	case "logs":
		m.logOffset = max(0, min(max(0, len(m.snapshot.Logs)-1), m.logOffset+delta))
	case "details", "tasks", "help":
		m.textOffset = max(0, min(max(0, len(m.textLines())-m.layout().body.Dy()), m.textOffset+delta))
	case "events":
		m.eventOffset = max(0, min(max(0, len(m.snapshot.Events)-1), m.eventOffset+delta))
	default:
		m.selected = max(0, min(max(0, len(m.rows())-1), m.selected+delta))
	}
	m.ensureVisible()
}
func (m *Model) ensureVisible() {
	rows := m.rows()
	m.selected = max(0, min(max(0, len(rows)-1), m.selected))
	visible := max(1, m.layout().table.Dy()-3)
	if m.selected < m.offset {
		m.offset = m.selected
	}
	if m.selected >= m.offset+visible {
		m.offset = m.selected - visible + 1
	}
	m.offset = max(0, min(m.offset, max(0, len(rows)-visible)))
}
func (m *Model) rows() []Row {
	if len(m.snapshot.Tables) == 0 {
		return nil
	}
	table := m.snapshot.Tables[min(m.table, len(m.snapshot.Tables)-1)]
	filter := strings.ToLower(m.filter)
	out := make([]Row, 0, len(table.Rows))
	for _, row := range table.Rows {
		haystack := strings.ToLower(strings.Join(row.Cells, " "))
		if strings.Contains(haystack, filter) && strings.Contains(haystack, strings.ToLower(m.fixedFilter)) {
			out = append(out, row)
		}
	}
	if column, ok := m.sortColumns[table.ID]; ok {
		sort.SliceStable(out, func(i, j int) bool {
			a, b := out[i], out[j]
			av, aok := a.Numbers[column]
			bv, bok := b.Numbers[column]
			if aok && bok {
				return av > bv
			}
			return cell(a, column) < cell(b, column)
		})
	}
	return out
}
func cell(row Row, index int) string {
	if index >= 0 && index < len(row.Cells) {
		return row.Cells[index]
	}
	return ""
}
func (m *Model) openSelected() tea.Cmd {
	rows := m.rows()
	if m.selected < len(rows) && rows[m.selected].Target != nil {
		return m.navigate(*rows[m.selected].Target)
	}
	if m.selected < len(rows) {
		row := rows[m.selected]
		m.snapshot.Fields = nil
		table := m.snapshot.Tables[min(m.table, len(m.snapshot.Tables)-1)]
		for i, value := range row.Cells {
			label := fmt.Sprintf("Column %d", i+1)
			if i < len(table.Columns) {
				label = table.Columns[i]
			}
			m.snapshot.Fields = append(m.snapshot.Fields, Field{Label: label, Value: value})
		}
	}
	m.screen = "details"
	m.textOffset = 0
	return nil
}
func (m *Model) copyText() string {
	if m.selection.text != "" {
		return m.selection.text
	}
	if m.screen == "logs" {
		return strings.Join(m.snapshot.Logs, "\n")
	}
	rows := m.rows()
	if m.selected < len(rows) {
		return strings.Join(rows[m.selected].Cells, "\t")
	}
	return m.snapshot.Title
}

func (m *Model) makePlan(action Action, argument string) tea.Cmd {
	if !action.Enabled {
		m.notify(action.Reason)
		return nil
	}
	m.action = action
	m.modal = "planning"
	m.planning = true
	request := ActionRequest{Target: m.request.Target, Action: action.ID, Argument: argument}
	epoch, provider, ctx := m.epoch, m.provider, m.ctx
	return func() tea.Msg {
		bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		plan, err := provider.Plan(bounded, request)
		return plannedMsg{epoch, plan, err}
	}
}
func (m *Model) execute() tea.Cmd {
	if m.plan == nil || m.executing {
		return nil
	}
	if m.now.After(m.plan.ExpiresAt) {
		m.notify("Plan expired; open the action again")
		m.modal = ""
		return nil
	}
	m.executing = true
	plan, provider, ctx := *m.plan, m.provider, m.ctx
	return func() tea.Msg {
		bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		receipt, err := provider.Execute(bounded, plan)
		return executedMsg{receipt, err}
	}
}
func (m *Model) modalItems() []string {
	switch m.modal {
	case "palette":
		items := []string{"Dashboard", "Resources", "Events", "Tasks", "Details", "Refresh", "Settings", "Help"}
		if m.snapshot.UnavailableScreens["logs"] == "" {
			items = append(items, "Logs")
		}
		if len(m.snapshot.Actions) > 0 {
			items = append(items, "Actions")
		}
		if m.snapshot.Admin {
			items = append(items, "Cluster")
		}
		return items
	case "actions":
		items := make([]string, 0, len(m.snapshot.Actions))
		for _, action := range m.snapshot.Actions {
			label := action.Label
			if !action.Enabled {
				label += " [unavailable: " + action.Reason + "]"
			}
			items = append(items, label)
		}
		return items
	case "sort":
		if len(m.snapshot.Tables) > 0 {
			return m.snapshot.Tables[min(m.table, len(m.snapshot.Tables)-1)].Columns
		}
	case "settings":
		return []string{"Theme: " + m.prefs.Theme, "Graph: " + m.prefs.Graph, "Window: " + m.prefs.Window, "Mouse: " + strconv.FormatBool(m.prefs.Mouse), "Mode: " + m.prefs.Mode, "Metrics panel: " + strconv.FormatBool(!m.prefs.panelHidden("metrics")), "Summary panel: " + strconv.FormatBool(!m.prefs.panelHidden("summary"))}
	}
	return nil
}
func (m *Model) activateModal() tea.Cmd {
	items := m.modalItems()
	if len(items) == 0 {
		m.modal = ""
		return nil
	}
	index := m.modalIndex % len(items)
	switch m.modal {
	case "palette":
		m.modal = ""
		switch items[index] {
		case "Actions":
			m.modal = "actions"
			m.modalIndex = 0
		case "Refresh":
			return m.schedule("overview", true)
		case "Settings":
			m.modal = "settings"
			m.modalIndex = 0
		case "Cluster":
			return m.navigate(Target{Kind: "cluster", Name: "Cluster"})
		default:
			m.textOffset = 0
			m.screen = strings.ToLower(items[index])
			if m.screen == "logs" {
				return m.schedule("logs", false)
			}
		}
	case "actions":
		action := m.snapshot.Actions[index]
		if !action.Enabled {
			m.notify(action.Reason)
			return nil
		}
		m.action = action
		if action.Argument != "" {
			m.modal = "argument"
			m.input = ""
			return nil
		}
		return m.makePlan(action, "")
	case "sort":
		table := &m.snapshot.Tables[m.table]
		m.sortColumns[table.ID] = index
		sort.SliceStable(table.Rows, func(i, j int) bool {
			a, b := table.Rows[i], table.Rows[j]
			av, aok := a.Numbers[index]
			bv, bok := b.Numbers[index]
			if aok && bok {
				return av > bv
			}
			return cell(a, index) < cell(b, index)
		})
		m.modal = ""
		m.selected = 0
		m.offset = 0
	case "settings":
		switch index {
		case 0:
			m.prefs.Theme = cycle([]string{"carbon", "light", "terminal"}, m.prefs.Theme)
		case 1:
			m.prefs.Graph = cycle([]string{"braille", "block", "line", "ascii"}, m.prefs.Graph)
		case 2:
			m.prefs.Window = cycle([]string{"5m", "15m", "1h"}, m.prefs.Window)
			m.request.Window, _ = time.ParseDuration(m.prefs.Window)
		case 3:
			m.prefs.Mouse = !m.prefs.Mouse
		case 5:
			m.prefs.togglePanel("metrics")
		case 6:
			m.prefs.togglePanel("summary")
		case 4:
			m.prefs.Mode = cycle([]string{"fullscreen", "compact"}, m.prefs.Mode)
		}
		return m.save()
	}
	return nil
}

func (m *Model) click(msg tea.MouseClickMsg) tea.Cmd {
	if !m.prefs.Mouse || msg.Button != tea.MouseLeft {
		return nil
	}
	p := image.Pt(msg.X, msg.Y)
	layout := m.layout()
	if m.modal != "" {
		box := m.modalBounds()
		if !p.In(box) {
			return nil
		}
		if p.Y == box.Max.Y-2 {
			if p.X < box.Min.X+box.Dx()/2 {
				if !m.executing {
					m.modal = ""
				}
				return nil
			}
			if m.modal == "confirm" && m.plan != nil && m.input == m.plan.Confirmation {
				return m.execute()
			}
			if m.modal == "argument" {
				return m.key(tea.KeyPressMsg{Code: tea.KeyEnter})
			}
			return m.activateModal()
		}
		if m.modal == "confirm" || m.modal == "argument" || m.modal == "planning" {
			return nil
		}
		index := p.Y - box.Min.Y - 3 + m.modalScrollOffset()
		if index >= 0 && index < len(m.modalItems()) {
			m.modalIndex = index
			return m.activateModal()
		}
		return nil
	}
	for _, hit := range layout.hits {
		if p.In(hit.bounds) {
			return m.activateHit(hit.id)
		}
	}
	if p.In(layout.table) {
		index := p.Y - layout.table.Min.Y - 2 + m.offset
		rows := m.rows()
		if index >= 0 && index < len(rows) {
			id := rows[index].ID
			m.selected = index
			m.selection = textSelection{start: p, end: p, dragging: true, surface: "table"}
			if m.lastClick == id && time.Since(m.lastClickAt) < 500*time.Millisecond {
				return m.openSelected()
			}
			m.lastClick = id
			m.lastClickAt = time.Now()
		}
		return nil
	}
	for i, box := range layout.charts {
		if p.In(box) {
			m.graphIndex = i
			m.graphCursor = p.X - box.Min.X - 1
			return nil
		}
	}
	if m.screen == "logs" && p.In(layout.body) {
		m.selection = textSelection{start: p, end: p, dragging: true}
	}
	return nil
}
func (m *Model) activateHit(id string) tea.Cmd {
	if strings.HasPrefix(id, "screen:") {
		m.textOffset = 0
		m.screen = strings.TrimPrefix(id, "screen:")
		if m.screen == "logs" {
			return m.schedule("logs", false)
		}
		return nil
	}
	switch id {
	case "back":
		return m.back()
	case "refresh":
		return tea.Batch(m.schedule("overview", true), m.schedule("metrics", true))
	case "pause":
		m.paused = !m.paused
	case "window":
		m.prefs.Window = cycle([]string{"5m", "15m", "1h"}, m.prefs.Window)
		m.request.Window, _ = time.ParseDuration(m.prefs.Window)
		return tea.Batch(m.save(), m.schedule("metrics", true))
	case "actions":
		m.modal = "actions"
		m.modalIndex = 0
	case "search":
		m.editing = "search"
		m.input = m.filter
	case "palette":
		m.modal = "palette"
		m.modalIndex = 0
	case "table":
		m.table = (m.table + 1) % max(1, len(m.snapshot.Tables))
		m.selected = 0
		m.offset = 0
	case "sort":
		m.modal = "sort"
		m.modalIndex = 0
	}
	return nil
}
func (m *Model) wheel(msg tea.MouseWheelMsg) {
	if !m.prefs.Mouse {
		return
	}
	delta := 3
	if msg.Button == tea.MouseWheelUp {
		delta = -3
	}
	if m.modal != "" {
		m.modalIndex = max(0, min(max(0, len(m.modalItems())-1), m.modalIndex+delta))
		return
	}
	m.move(delta)
}
func (m *Model) drag(msg tea.MouseMotionMsg) {
	if m.selection.dragging {
		m.selection.end = image.Pt(msg.X, msg.Y)
	}
}
func (m *Model) release(msg tea.MouseReleaseMsg) {
	if !m.selection.dragging {
		return
	}
	m.selection.dragging = false
	m.selection.end = image.Pt(msg.X, msg.Y)
	box := m.layout().body
	lines := m.snapshot.Logs
	offset := m.logOffset
	if m.selection.surface == "table" {
		box = m.layout().table
		lines = strings.Split(ansi.Strip(m.renderTable(box)), "\n")
		offset = 0
	}
	start, end := m.selection.start, m.selection.end
	if start.Y > end.Y || (start.Y == end.Y && start.X > end.X) {
		start, end = end, start
	}
	out := []string{}
	for y := max(box.Min.Y, start.Y); y <= min(box.Max.Y-1, end.Y); y++ {
		index := offset + y - box.Min.Y
		if index < 0 || index >= len(lines) {
			continue
		}
		line := lines[index]
		left, right := 0, box.Dx()
		if y == start.Y {
			left = max(0, start.X-box.Min.X)
		}
		if y == end.Y {
			right = max(left, end.X-box.Min.X+1)
		}
		out = append(out, ansi.Cut(line, left, right))
	}
	if start == end {
		m.selection.text = ""
	} else {
		m.selection.text = strings.Join(out, "\n")
	}
}

func (m *Model) modalScrollOffset() int {
	return max(0, m.modalIndex%max(1, len(m.modalItems()))-max(0, m.modalBounds().Dy()-6))
}
