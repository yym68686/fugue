package tui

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
)

type Options struct {
	InitialFilter, InitialSearch, InitialSort string
	Interval, Window                          time.Duration
	Mouse, Color, AltScreen                   bool
	Input                                     io.Reader
	Output                                    io.Writer
	Preferences                               Preferences
	SavePreferences                           func(Preferences) error
	Extensions                                []Extension
}
type tickMsg time.Time
type fetchedMsg struct {
	epoch   int
	section string
	value   Snapshot
	err     error
	elapsed time.Duration
}
type plannedMsg struct {
	epoch int
	plan  Plan
	err   error
}
type executedMsg struct {
	receipt Receipt
	err     error
}
type savedMsg struct{ err error }
type watchMsg struct {
	epoch, generation int
	notice            Notice
	done              bool
}
type fetchState struct {
	pending  bool
	due      time.Time
	failures int
	elapsed  time.Duration
	started  time.Time
	err      string
}
type navState struct {
	request                 Request
	snapshot                Snapshot
	screen                  string
	table, selected, offset int
	filter                  string
}

type Model struct {
	ctx                                             context.Context
	provider                                        Provider
	store                                           *Store
	request                                         Request
	opts                                            Options
	prefs                                           Preferences
	snapshot                                        Snapshot
	series                                          map[string]Series
	fetches                                         map[string]fetchState
	epoch                                           int
	width, height                                   int
	screen                                          string
	navigation                                      []navState
	table, selected, offset, logOffset, eventOffset int
	textOffset                                      int
	filter, editing, input                          string
	fixedFilter                                     string
	paused                                          bool
	now                                             time.Time
	modal                                           string
	modalIndex                                      int
	plan                                            *Plan
	planning, executing                             bool
	action                                          Action
	toast                                           string
	toastUntil                                      time.Time
	graphIndex, graphCursor                         int
	selection                                       textSelection
	lastClick                                       string
	lastClickAt                                     time.Time
	watchEvents                                     chan watchMsg
	watchCancel                                     context.CancelFunc
	watchCtx                                        context.Context
	watchKey                                        string
	watchGeneration                                 int
	watchRetry                                      time.Time
	watchFailures                                   int
	viewCtx                                         context.Context
	viewCancel                                      context.CancelFunc
	sortColumns                                     map[string]int
	chartOffset                                     int
}

func New(provider Provider, request Request, opts Options) *Model {
	p := opts.Preferences
	if p.Version == 0 {
		p = DefaultPreferences()
		p.Mouse = opts.Mouse
		if !opts.AltScreen {
			p.Mode = "compact"
		}
	}
	if opts.Interval <= 0 {
		opts.Interval, _ = time.ParseDuration(p.Interval)
	}
	if opts.Window <= 0 {
		opts.Window, _ = time.ParseDuration(p.Window)
	}
	request.Window = opts.Window
	return &Model{filter: opts.InitialSearch, fixedFilter: opts.InitialFilter, ctx: context.Background(), provider: provider, store: NewStore(provider), request: request, opts: opts, prefs: p, series: map[string]Series{}, fetches: map[string]fetchState{}, width: 100, height: 30, screen: p.DefaultScreen, now: time.Now(), graphCursor: -1, sortColumns: map[string]int{}}
}

func Run(ctx context.Context, provider Provider, request Request, opts Options) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	m := New(provider, request, opts)
	m.ctx = ctx
	defer m.stopWatch()
	defer func() {
		if m.viewCancel != nil {
			m.viewCancel()
		}
	}()
	programOptions := []tea.ProgramOption{tea.WithContext(ctx), tea.WithFPS(30)}
	if opts.Input != nil {
		programOptions = append(programOptions, tea.WithInput(opts.Input))
	}
	if opts.Output != nil {
		programOptions = append(programOptions, tea.WithOutput(opts.Output))
	}
	if !opts.Color {
		programOptions = append(programOptions, tea.WithColorProfile(colorprofile.Ascii))
	}
	_, err := tea.NewProgram(m, programOptions...).Run()
	if errors.Is(err, tea.ErrInterrupted) || (errors.Is(err, tea.ErrProgramKilled) && ctx.Err() != nil) {
		return nil
	}
	return err
}

func (m *Model) Init() tea.Cmd {
	return tea.Batch(m.schedule("overview", true), m.tick(), m.syncWatch())
}
func (m *Model) tick() tea.Cmd {
	ctx := m.ctx
	return func() tea.Msg {
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		select {
		case t := <-timer.C:
			return tickMsg(t)
		case <-ctx.Done():
			return nil
		}
	}
}
func (m *Model) ttl(section string) time.Duration {
	switch section {
	case "metrics":
		return max(10*time.Second, m.opts.Interval)
	case "logs":
		return max(5*time.Second, m.opts.Interval)
	default:
		return m.opts.Interval
	}
}
func (m *Model) schedule(section string, force bool) tea.Cmd {
	state := m.fetches[section]
	if state.pending || (!force && m.now.Before(state.due)) {
		return nil
	}
	state.pending = true
	state.started = m.now
	m.fetches[section] = state
	request := m.request
	request.Section = section
	if m.viewCtx == nil {
		m.viewCtx, m.viewCancel = context.WithCancel(m.ctx)
	}
	epoch, ctx, store, ttl := m.epoch, m.viewCtx, m.store, m.ttl(section)
	return func() tea.Msg {
		start := time.Now()
		value, err := store.Fetch(ctx, request, ttl, force)
		return fetchedMsg{epoch, section, value, err, time.Since(start)}
	}
}
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	model, cmd := m.update(msg)
	return model, tea.Batch(cmd, m.syncWatch())
}
func (m *Model) update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch v := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = max(1, v.Width), max(1, v.Height)
		m.selection = textSelection{}
		m.ensureVisible()
	case tea.KeyPressMsg:
		return m, m.key(v)
	case tea.PasteMsg:
		m.paste(v.Content)
	case tea.MouseClickMsg:
		return m, m.click(v)
	case tea.MouseWheelMsg:
		m.wheel(v)
	case tea.MouseMotionMsg:
		m.drag(v)
	case tea.MouseReleaseMsg:
		m.release(v)
	case tickMsg:
		m.now = time.Time(v)
		cmds := []tea.Cmd{m.tick()}
		if !m.paused {
			cmds = append(cmds, m.schedule("overview", false))
			if m.request.Target.Kind == "app" || m.request.Target.Kind == "node" || m.request.Target.Kind == "cluster" {
				cmds = append(cmds, m.schedule("metrics", false))
			}
			if m.screen == "logs" && m.watchCancel == nil {
				cmds = append(cmds, m.schedule("logs", false))
			}
		}
		return m, tea.Batch(cmds...)
	case fetchedMsg:
		if v.epoch != m.epoch {
			return m, nil
		}
		state := m.fetches[v.section]
		state.pending = false
		state.elapsed = v.elapsed
		state.err = ""
		if v.err != nil {
			state.failures++
			state.err = Plain(v.err.Error())
			delay := m.ttl(v.section) * time.Duration(1<<min(4, state.failures-1))
			state.due = m.now.Add(min(30*time.Second, delay))
		} else {
			state.failures = 0
			state.due = m.now.Add(m.ttl(v.section))
			m.accept(v.section, v.value)
		}
		m.fetches[v.section] = state
		m.ensureVisible()
	case watchMsg:
		if v.epoch != m.epoch || v.generation != m.watchGeneration {
			return m, nil
		}
		notice := v.notice
		if v.done {
			m.stopWatch()
			m.watchFailures++
			m.watchRetry = m.now.Add(time.Second * time.Duration(1<<min(5, m.watchFailures)))
		}
		if notice.Err != nil {
			m.notify("Live stream unavailable; polling remains active: " + notice.Err.Error())
			m.mergeSources([]Source{{ID: "live stream", State: "stale", Message: Plain(notice.Err.Error())}})
		} else if !v.done {
			m.watchFailures = 0
			m.mergeSources([]Source{{ID: "live stream", State: "available", ObservedAt: m.now}})
		}
		if !m.paused && len(notice.Logs) > 0 && (notice.Cursor == "" || notice.Cursor != m.snapshot.LogCursor) {
			m.appendLogs(notice.Logs)
			m.snapshot.LogCursor = notice.Cursor
		}
		if !m.paused && notice.Section == "overview" {
			// An invalidation respects the normal TTL; streams never amplify polling.
			return m, tea.Batch(m.schedule("overview", false), m.waitWatch())
		}
		return m, m.waitWatch()
	case plannedMsg:
		if v.epoch != m.epoch {
			return m, nil
		}
		m.planning = false
		if v.err != nil {
			m.modal = ""
			m.notify(v.err.Error())
		} else {
			m.plan = &v.plan
			m.modal = "confirm"
			m.input = ""
		}
	case executedMsg:
		m.executing = false
		m.modal = ""
		m.plan = nil
		if v.err != nil {
			m.notify("Submission failed or unknown: " + v.err.Error() + ". Inspect before retrying.")
			return m, m.schedule("overview", true)
		}
		m.notify(v.receipt.Message)
		if v.receipt.Operation.ID != "" {
			return m, m.navigate(v.receipt.Operation)
		}
		return m, m.schedule("overview", true)
	case savedMsg:
		if v.err != nil {
			m.notify("Preferences: " + v.err.Error())
		}
	case tea.InterruptMsg, tea.QuitMsg:
		return m, tea.Quit
	}
	return m, nil
}
func (m *Model) stopWatch() {
	if m.watchCancel != nil {
		m.watchCancel()
	}
	m.watchCancel, m.watchCtx, m.watchEvents = nil, nil, nil
	m.watchKey = ""
	m.watchGeneration++
}
func (m *Model) syncWatch() tea.Cmd {
	provider, ok := m.provider.(WatchProvider)
	target := m.request.Target
	wanted := ok && !m.paused && (target.Kind == "workspace" || target.Kind == "project" || ((target.Kind == "app" || target.Kind == "pod") && target.ID != "" && m.screen == "logs"))
	key := target.Key()
	if !wanted || (m.watchKey != "" && m.watchKey != key) {
		if m.watchCancel != nil {
			m.stopWatch()
		}
	}
	if !wanted || m.watchCancel != nil || m.now.Before(m.watchRetry) {
		return nil
	}
	ctx, cancel := context.WithCancel(m.ctx)
	m.watchCtx, m.watchCancel, m.watchKey = ctx, cancel, key
	m.watchEvents = make(chan watchMsg, 64)
	ch, epoch, generation, cursor := m.watchEvents, m.epoch, m.watchGeneration, m.snapshot.LogCursor
	go func() {
		defer close(ch)
		err := provider.Watch(ctx, target, cursor, func(notice Notice) {
			select {
			case ch <- watchMsg{epoch: epoch, generation: generation, notice: notice}:
			case <-ctx.Done():
			}
		})
		select {
		case ch <- watchMsg{epoch: epoch, generation: generation, notice: Notice{Err: err}, done: true}:
		case <-ctx.Done():
		}
	}()
	return m.waitWatch()
}
func (m *Model) waitWatch() tea.Cmd {
	if m.watchEvents == nil {
		return nil
	}
	ch, ctx := m.watchEvents, m.watchCtx
	return func() tea.Msg {
		select {
		case value, ok := <-ch:
			if ok {
				return value
			}
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

const maxLogLines = 2000

func (m *Model) appendLogs(lines []string) {
	atTail := m.logOffset >= max(0, len(m.snapshot.Logs)-m.layout().body.Dy())
	for _, line := range lines {
		m.snapshot.Logs = append(m.snapshot.Logs, clip(Plain(line), 4096))
	}
	if extra := len(m.snapshot.Logs) - maxLogLines; extra > 0 {
		m.snapshot.Logs = append([]string(nil), m.snapshot.Logs[extra:]...)
		m.logOffset = max(0, m.logOffset-extra)
	}
	if atTail {
		m.logOffset = max(0, len(m.snapshot.Logs)-m.layout().body.Dy())
	}
}
func (m *Model) mergeSources(sources []Source) {
	for _, source := range sources {
		found := false
		for i := range m.snapshot.Sources {
			if m.snapshot.Sources[i].ID == source.ID {
				if source.State != "available" && source.ObservedAt.IsZero() {
					source.ObservedAt = m.snapshot.Sources[i].ObservedAt
				}
				m.snapshot.Sources[i] = source
				found = true
				break
			}
		}
		if !found {
			m.snapshot.Sources = append(m.snapshot.Sources, source)
		}
	}
}
func (m *Model) accept(section string, s Snapshot) {
	s = sanitizeSnapshot(s)
	if section == "overview" {
		selectedID := ""
		rows := m.rows()
		if m.selected < len(rows) {
			selectedID = rows[m.selected].ID
		}
		m.snapshot.Target = s.Target
		m.snapshot.Title = s.Title
		m.snapshot.Subtitle = s.Subtitle
		m.snapshot.Status = s.Status
		m.snapshot.ObservedAt = s.ObservedAt
		m.snapshot.Fields = s.Fields
		oldTables := m.snapshot.Tables
		m.snapshot.Tables = s.Tables
		// Keep confirmed rows for failed sources instead of blanking their panel.
		for _, source := range s.Sources {
			if source.State == "available" {
				continue
			}
			for _, old := range oldTables {
				if old.ID == source.ID {
					found := false
					for _, next := range s.Tables {
						if next.ID == old.ID {
							found = true
						}
					}
					if !found {
						m.snapshot.Tables = append(m.snapshot.Tables, old)
					}
				}
			}
		}
		if len(s.Tables) > 0 && m.opts.InitialSort != "" {
			table := s.Tables[0]
			if _, ok := m.sortColumns[table.ID]; !ok {
				for i, column := range table.Columns {
					if strings.EqualFold(column, m.opts.InitialSort) {
						m.sortColumns[table.ID] = i
						break
					}
				}
			}
		}
		m.snapshot.Events = s.Events
		m.snapshot.Actions = s.Actions
		m.snapshot.Admin = s.Admin
		if s.Target.Kind != "" {
			m.request.Target = s.Target
		}
		for i, row := range m.rows() {
			if row.ID == selectedID {
				m.selected = i
				break
			}
		}
	} else if section == "logs" {
		if m.watchCancel == nil {
			m.snapshot.Logs = nil
			m.appendLogs(s.Logs)
		}
	}
	m.mergeSources(s.Sources)
	for _, series := range s.Series {
		m.series[series.ID] = MergeSeries(m.series[series.ID], series, m.now)
	}
	m.snapshot.Series = nil
	keys := make([]string, 0, len(m.series))
	for key := range m.series {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		m.snapshot.Series = append(m.snapshot.Series, m.series[key])
	}
	if section == "overview" {
		for _, extension := range m.opts.Extensions {
			copy, err := cloneSnapshot(m.snapshot)
			if err == nil {
				m.snapshot.Fields = append(m.snapshot.Fields, extension.Fields(copy)...)
			}
		}
	}
}
func (m *Model) navigate(target Target) tea.Cmd {
	if m.executing {
		return nil
	}
	m.navigation = append(m.navigation, navState{m.request, m.snapshot, m.screen, m.table, m.selected, m.offset, m.filter})
	if len(m.navigation) > 20 {
		m.navigation = m.navigation[1:]
	}
	return m.setTarget(Request{Target: target, Window: m.request.Window})
}
func (m *Model) setTarget(request Request) tea.Cmd {
	m.stopWatch()
	if m.viewCancel != nil {
		m.viewCancel()
		m.viewCancel = nil
		m.viewCtx = nil
	}
	m.watchRetry = time.Time{}
	m.epoch++
	m.request = request
	m.snapshot = Snapshot{Target: request.Target, Title: request.Target.Name, Status: "loading"}
	m.series = map[string]Series{}
	m.fetches = map[string]fetchState{}
	m.table = 0
	m.selected = 0
	m.offset = 0
	m.filter = ""
	m.textOffset, m.logOffset, m.eventOffset, m.chartOffset = 0, 0, 0, 0
	m.screen = "dashboard"
	m.modal = ""
	m.plan = nil
	m.selection = textSelection{}
	return m.schedule("overview", false)
}
func (m *Model) back() tea.Cmd {
	if len(m.navigation) == 0 {
		m.screen = "dashboard"
		return nil
	}
	last := m.navigation[len(m.navigation)-1]
	m.navigation = m.navigation[:len(m.navigation)-1]
	cmd := m.setTarget(last.request)
	m.snapshot = last.snapshot
	m.screen = last.screen
	m.table = last.table
	m.selected = last.selected
	m.offset = last.offset
	m.filter = last.filter
	for _, s := range last.snapshot.Series {
		m.series[s.ID] = s
	}
	return cmd
}
func (m *Model) notify(text string) { m.toast = Plain(text); m.toastUntil = m.now.Add(8 * time.Second) }
func (m *Model) save() tea.Cmd {
	if m.opts.SavePreferences == nil {
		return nil
	}
	prefs, save := m.prefs, m.opts.SavePreferences
	return func() tea.Msg { return savedMsg{save(prefs)} }
}
