package monitor

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/cli/terminal"
	"fugue/internal/cli/ui"
)

type Controls struct {
	Paused bool
	Filter string
	Search string
	Sort   string
}

type Section struct {
	Title   string     `json:"title"`
	Lines   []string   `json:"lines,omitempty"`
	Headers []string   `json:"headers,omitempty"`
	Rows    [][]string `json:"rows,omitempty"`
}

type Snapshot struct {
	Title       string    `json:"title"`
	ObservedAt  time.Time `json:"observed_at"`
	Controls    Controls  `json:"controls"`
	Summary     []string  `json:"summary,omitempty"`
	Sections    []Section `json:"sections,omitempty"`
	Error       string    `json:"error,omitempty"`
	ResumeHint  string    `json:"resume_hint,omitempty"`
	Interactive bool      `json:"interactive,omitempty"`
}

type Renderer struct {
	Width   int
	Palette terminal.Palette
}

type Session struct {
	Last     Snapshot
	LastHash [32]byte
	HaveLast bool
	Controls Controls
}

func NewRenderer(width int, palette terminal.Palette) Renderer {
	if width <= 0 {
		width = ui.DefaultWidth
	}
	return Renderer{Width: width, Palette: palette}
}

func (r Renderer) Render(snapshot Snapshot) string {
	renderer := ui.NewRenderer(r.Width, r.Palette)
	title := strings.TrimSpace(snapshot.Title)
	if title == "" {
		title = "Fugue monitor"
	}
	lines := make([]string, 0)
	lines = append(lines, fmt.Sprintf("observed_at=%s", snapshot.ObservedAt.UTC().Format(time.RFC3339)))
	if snapshot.Controls.Paused {
		lines = append(lines, "state=paused")
	}
	if snapshot.Controls.Filter != "" {
		lines = append(lines, "filter="+snapshot.Controls.Filter)
	}
	if snapshot.Controls.Search != "" {
		lines = append(lines, "search="+snapshot.Controls.Search)
	}
	if snapshot.Controls.Sort != "" {
		lines = append(lines, "sort="+snapshot.Controls.Sort)
	}
	if snapshot.Error != "" {
		lines = append(lines, "transient_error="+snapshot.Error)
	}
	if len(snapshot.Summary) > 0 {
		lines = append(lines, "", "summary")
		lines = append(lines, snapshot.Summary...)
	}
	for _, section := range snapshot.Sections {
		rendered := renderSection(renderer, snapshot.Controls, section)
		if strings.TrimSpace(rendered) == "" {
			continue
		}
		lines = append(lines, "", rendered)
	}
	lines = append(lines, "", strings.Join(Help(), "  "))
	if snapshot.ResumeHint != "" {
		lines = append(lines, "resume="+snapshot.ResumeHint)
	}
	return renderer.Panel(title, strings.Join(lines, "\n"))
}

func (s *Session) Accept(snapshot Snapshot) bool {
	snapshot.Controls = mergeControls(snapshot.Controls, s.Controls)
	hash := Hash(snapshot)
	if s.Controls.Paused {
		return false
	}
	if s.HaveLast && hash == s.LastHash {
		return false
	}
	s.Last = snapshot
	s.LastHash = hash
	s.HaveLast = true
	return true
}

func (s *Session) SnapshotWithError(err error, observedAt time.Time) Snapshot {
	snapshot := s.Last
	if snapshot.ObservedAt.IsZero() {
		snapshot.ObservedAt = observedAt
	}
	if err != nil {
		snapshot.Error = err.Error()
	}
	snapshot.Controls = mergeControls(snapshot.Controls, s.Controls)
	return snapshot
}

func (s *Session) ApplyKey(key string) {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case " ", "space", "p", "pause":
		s.Controls.Paused = !s.Controls.Paused
	case "esc", "clear":
		s.Controls.Filter = ""
		s.Controls.Search = ""
	}
}

func Hash(snapshot Snapshot) [32]byte {
	return HashAny(snapshot)
}

func HashAny(value any) [32]byte {
	payload, err := json.Marshal(value)
	if err != nil {
		return sha256.Sum256([]byte(fmt.Sprintf("%#v", value)))
	}
	return sha256.Sum256(payload)
}

func Help() []string {
	return []string{"q quit", "space pause", "/ search", "f filter", "s sort", "r refresh"}
}

func CtrlCSummary(snapshot Snapshot) []string {
	out := []string{}
	if title := strings.TrimSpace(snapshot.Title); title != "" {
		out = append(out, "last_view="+title)
	}
	if !snapshot.ObservedAt.IsZero() {
		out = append(out, "last_observed_at="+snapshot.ObservedAt.UTC().Format(time.RFC3339))
	}
	for _, value := range snapshot.Summary {
		if strings.TrimSpace(value) != "" {
			out = append(out, value)
		}
	}
	if snapshot.ResumeHint != "" {
		out = append(out, "resume="+snapshot.ResumeHint)
	}
	return out
}

func renderSection(renderer ui.Renderer, controls Controls, section Section) string {
	title := strings.TrimSpace(section.Title)
	var body string
	switch {
	case len(section.Headers) > 0:
		body = renderer.Table(section.Headers, applyControls(section.Headers, section.Rows, controls))
	default:
		body = strings.Join(section.Lines, "\n")
	}
	if title == "" {
		return body
	}
	return renderer.Section(title, body)
}

func applyControls(headers []string, rows [][]string, controls Controls) [][]string {
	filter := strings.ToLower(strings.TrimSpace(firstNonEmpty(controls.Search, controls.Filter)))
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		if filter == "" || rowContains(row, filter) {
			out = append(out, append([]string(nil), row...))
		}
	}
	sortKey := strings.ToLower(strings.TrimSpace(controls.Sort))
	if sortKey == "" {
		return out
	}
	index := -1
	for i, header := range headers {
		if strings.EqualFold(strings.TrimSpace(header), sortKey) {
			index = i
			break
		}
	}
	if index < 0 {
		return out
	}
	sort.SliceStable(out, func(i, j int) bool {
		return strings.Compare(column(out[i], index), column(out[j], index)) < 0
	})
	return out
}

func rowContains(row []string, needle string) bool {
	for _, value := range row {
		if strings.Contains(strings.ToLower(value), needle) {
			return true
		}
	}
	return false
}

func column(row []string, index int) string {
	if index < 0 || index >= len(row) {
		return ""
	}
	return row[index]
}

func mergeControls(base, override Controls) Controls {
	base.Paused = base.Paused || override.Paused
	if override.Filter != "" {
		base.Filter = override.Filter
	}
	if override.Search != "" {
		base.Search = override.Search
	}
	if override.Sort != "" {
		base.Sort = override.Sort
	}
	return base
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
