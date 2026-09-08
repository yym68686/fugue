package tui

import (
	"context"
	"errors"
	"io"
	"sort"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
)

type Options struct {
	Interval, Window        time.Duration
	Mouse, Color, AltScreen bool
	Input                   io.Reader
	Output                  io.Writer
	Preferences             Preferences
	SavePreferences         func(Preferences) error
	Extensions              []Extension
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
type watchMsg struct{ err error }
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
	filter, editing, input                          string
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
	return &Model{ctx: context.Background(), provider: provider, store: NewStore(provider), request: request, opts: opts, prefs: p, series: map[string]Series{}, fetches: map[string]fetchState{}, width: 100, height: 30, screen: p.DefaultScreen, now: time.Now(), graphCursor: -1, sortColumns: map[string]int{}}
}

func Run(ctx context.Context, provider Provider, request Request, opts Options) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	m := New(provider, request, opts)
	m.ctx = ctx
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
	return tea.Batch(m.schedule("overview", true), m.tick(), m.startWatch())
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
	epoch, ctx, store, ttl := m.epoch, m.ctx, m.store, m.ttl(section)
	return func() tea.Msg {
		start := time.Now()
		value, err := store.Fetch(ctx, request, ttl, force)
		return fetchedMsg{epoch, section, value, err, time.Since(start)}
	}
}
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
			if m.screen == "logs" {
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
		if v.err != nil {
			m.notify("Live stream unavailable; polling remains active")
		}
		if !m.paused {
			return m, tea.Batch(m.schedule("overview", true), m.waitWatch())
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
func (m *Model) startWatch() tea.Cmd {
	provider, ok := m.provider.(WatchProvider)
	if !ok {
		return nil
	}
	if m.request.Target.Kind != "workspace" && m.request.Target.Kind != "project" {
		return nil
	}
	ctx, cancel := context.WithCancel(m.ctx)
	m.watchCancel = cancel
	m.watchEvents = make(chan watchMsg, 1)
	ch, epoch, target := m.watchEvents, m.epoch, m.request.Target
	go func() {
		err := provider.Watch(ctx, target, func(_ Notice) {
			select {
			case ch <- watchMsg{}:
			default:
			}
		})
		if err != nil && ctx.Err() == nil {
			select {
			case ch <- watchMsg{err: err}:
			default:
			}
		}
		_ = epoch
	}()
	return m.waitWatch()
}
func (m *Model) waitWatch() tea.Cmd {
	if m.watchEvents == nil {
		return nil
	}
	ch, ctx := m.watchEvents, m.ctx
	return func() tea.Msg {
		select {
		case value := <-ch:
			return value
		case <-ctx.Done():
			return nil
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
		m.snapshot.Tables = s.Tables
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
		m.snapshot.Logs = s.Logs
	}
	for _, source := range s.Sources {
		found := false
		for i := range m.snapshot.Sources {
			if m.snapshot.Sources[i].ID == source.ID {
				m.snapshot.Sources[i] = source
				found = true
				break
			}
		}
		if !found {
			m.snapshot.Sources = append(m.snapshot.Sources, source)
		}
	}
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
	m.epoch++
	m.request = request
	m.snapshot = Snapshot{Target: request.Target, Title: request.Target.Name, Status: "loading"}
	m.series = map[string]Series{}
	m.fetches = map[string]fetchState{}
	m.table = 0
	m.selected = 0
	m.offset = 0
	m.filter = ""
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
