package ui

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"fugue/internal/cli/terminal"
	"fugue/internal/cli/viewmodel"
)

const DefaultWidth = 100

type Renderer struct {
	Width   int
	Palette terminal.Palette
}

func NewRenderer(width int, palette terminal.Palette) Renderer {
	if width <= 0 {
		width = DefaultWidth
	}
	return Renderer{Width: width, Palette: palette}
}

func (r Renderer) Section(title string, body string) string {
	title = strings.TrimSpace(title)
	body = strings.TrimRight(body, "\n")
	if title == "" {
		return body + "\n"
	}
	if body == "" {
		return r.Palette.Style(terminal.RoleAccent, title) + "\n"
	}
	return r.Palette.Style(terminal.RoleAccent, title) + "\n" + body + "\n"
}

func (r Renderer) Panel(title string, body string) string {
	width := maxInt(24, r.Width)
	innerWidth := maxInt(1, width-4)
	title = truncate(strings.TrimSpace(title), innerWidth)
	topLabel := " " + title + " "
	top := "+" + topLabel + strings.Repeat("-", maxInt(0, width-2-displayWidth(topLabel))) + "+"
	bottom := "+" + strings.Repeat("-", width-2) + "+"
	lines := wrapLines(body, innerWidth)
	if len(lines) == 0 {
		lines = []string{""}
	}
	var b strings.Builder
	b.WriteString(r.Palette.Style(terminal.RoleBorder, top))
	b.WriteByte('\n')
	for _, line := range lines {
		padded := padRight(line, innerWidth)
		b.WriteString(r.Palette.Style(terminal.RoleBorder, "| "))
		b.WriteString(padded)
		b.WriteString(r.Palette.Style(terminal.RoleBorder, " |"))
		b.WriteByte('\n')
	}
	b.WriteString(r.Palette.Style(terminal.RoleBorder, bottom))
	b.WriteByte('\n')
	return b.String()
}

func (r Renderer) Table(headers []string, rows [][]string) string {
	if len(headers) == 0 {
		return ""
	}
	widths := make([]int, len(headers))
	for i, header := range headers {
		widths[i] = displayWidth(header)
	}
	for _, row := range rows {
		for i := range headers {
			value := ""
			if i < len(row) {
				value = row[i]
			}
			widths[i] = maxInt(widths[i], minInt(32, displayWidth(value)))
		}
	}
	widths = fitTableWidths(headers, widths, maxInt(1, r.Width-4))
	var b strings.Builder
	b.WriteString(renderRow(truncateValues(headers, widths), widths))
	b.WriteByte('\n')
	separators := make([]string, len(headers))
	for i := range headers {
		separators[i] = strings.Repeat("-", widths[i])
	}
	b.WriteString(r.Palette.Style(terminal.RoleBorder, renderRow(separators, widths)))
	b.WriteByte('\n')
	for _, row := range rows {
		values := make([]string, len(headers))
		for i := range headers {
			if i < len(row) {
				values[i] = truncate(row[i], widths[i])
			}
		}
		b.WriteString(renderRow(values, widths))
		b.WriteByte('\n')
	}
	return b.String()
}

func fitTableWidths(headers []string, widths []int, maxWidth int) []int {
	fitted := append([]int(nil), widths...)
	if len(fitted) == 0 || maxWidth <= 0 {
		return fitted
	}
	minWidths := make([]int, len(fitted))
	for i := range fitted {
		minWidths[i] = minInt(fitted[i], maxInt(3, displayWidth(headers[i])))
	}
	for totalTableWidth(fitted) > maxWidth {
		index := widestShrinkableColumn(fitted, minWidths)
		if index < 0 {
			break
		}
		fitted[index]--
	}
	for totalTableWidth(fitted) > maxWidth {
		index := widestShrinkableColumn(fitted, nil)
		if index < 0 {
			break
		}
		fitted[index]--
	}
	return fitted
}

func totalTableWidth(widths []int) int {
	total := 0
	for i, width := range widths {
		if i > 0 {
			total += 2
		}
		total += width
	}
	return total
}

func widestShrinkableColumn(widths []int, minWidths []int) int {
	index := -1
	for i, width := range widths {
		minWidth := 1
		if i < len(minWidths) {
			minWidth = minWidths[i]
		}
		if width <= minWidth {
			continue
		}
		if index < 0 || width > widths[index] {
			index = i
		}
	}
	return index
}

func truncateValues(values []string, widths []int) []string {
	out := make([]string, len(widths))
	for i := range widths {
		if i < len(values) {
			out[i] = truncate(values[i], widths[i])
		}
	}
	return out
}

func (r Renderer) StatusChip(label string, tone viewmodel.Tone) string {
	label = strings.TrimSpace(label)
	if label == "" {
		label = "unknown"
	}
	role := terminal.RoleMuted
	switch tone {
	case viewmodel.TonePositive:
		role = terminal.RoleSuccess
	case viewmodel.ToneWarning:
		role = terminal.RoleWarning
	case viewmodel.ToneDanger:
		role = terminal.RoleDanger
	case viewmodel.ToneMuted:
		role = terminal.RoleMuted
	default:
		role = terminal.RoleAccent
	}
	return r.Palette.Style(role, "["+label+"]")
}

type RouteSegment struct {
	Label string
	Tone  viewmodel.Tone
}

func (r Renderer) RouteChain(segments []RouteSegment) string {
	parts := make([]string, 0, len(segments))
	for _, segment := range segments {
		label := strings.TrimSpace(segment.Label)
		if label == "" {
			continue
		}
		parts = append(parts, r.StatusChip(label, segment.Tone))
	}
	return strings.Join(parts, " -> ")
}

func (r Renderer) OperationTimeline(timeline viewmodel.OperationTimelineView) string {
	if timeline.State.Kind != viewmodel.StateReady {
		return r.stateLine(timeline.State)
	}
	var b strings.Builder
	for _, step := range timeline.Steps {
		marker := "o"
		if step.Active {
			marker = "*"
		}
		line := fmt.Sprintf("%s %s %s %s", marker, step.ID, r.StatusChip(step.Status, step.Tone), step.Type)
		if strings.TrimSpace(step.Message) != "" {
			line += " - " + strings.TrimSpace(step.Message)
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

func (r Renderer) MetricBar(label string, current int, total int, width int) string {
	if width <= 0 {
		width = 16
	}
	if total <= 0 {
		total = current
	}
	filled := 0
	if total > 0 {
		filled = int(float64(current) / float64(total) * float64(width))
	}
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}
	return fmt.Sprintf("%s [%s%s] %d/%d", strings.TrimSpace(label), strings.Repeat("#", filled), strings.Repeat("-", width-filled), current, total)
}

func (r Renderer) ErrorBlock(view viewmodel.DiagnosisEvidenceView) string {
	if view.State.Kind != viewmodel.StateReady {
		return r.stateLine(view.State)
	}
	var b strings.Builder
	if view.Category != "" {
		b.WriteString("category=")
		b.WriteString(r.Palette.Style(terminal.RoleDanger, view.Category))
		b.WriteByte('\n')
	}
	if view.Summary != "" {
		b.WriteString("summary=")
		b.WriteString(view.Summary)
		b.WriteByte('\n')
	}
	if view.Hint != "" {
		b.WriteString("hint=")
		b.WriteString(view.Hint)
		b.WriteByte('\n')
	}
	for _, evidence := range view.Evidence {
		b.WriteString("evidence=")
		b.WriteString(evidence)
		b.WriteByte('\n')
	}
	for _, command := range view.NextCommands {
		b.WriteString("next=")
		b.WriteString(command)
		b.WriteByte('\n')
	}
	return b.String()
}

func (r Renderer) CopyBlock(label string, value string) string {
	label = strings.TrimSpace(label)
	value = strings.TrimSpace(value)
	if label == "" {
		return value + "\n"
	}
	return label + "\n  " + value + "\n"
}

func (r Renderer) DangerConfirmDialog(plan viewmodel.ActionPlanView) string {
	body := []string{
		"action=" + firstNonEmpty(plan.Action, "unknown"),
		"target=" + firstNonEmpty(plan.Target, "-"),
		"scope=" + firstNonEmpty(plan.Scope, "-"),
		"api_call=" + firstNonEmpty(plan.APICall, "-"),
		"operation=" + firstNonEmpty(plan.OperationType, "-"),
		"risk=" + firstNonEmpty(plan.Risk, "-"),
		"rollback=" + firstNonEmpty(plan.RollbackHint, "-"),
		"confirm=" + firstNonEmpty(plan.ConfirmText, "-"),
	}
	for _, command := range plan.NextCommands {
		body = append(body, "next="+command)
	}
	return r.Panel("Confirm "+firstNonEmpty(plan.Action, "action"), strings.Join(body, "\n"))
}

func (r Renderer) stateLine(state viewmodel.State) string {
	label := string(state.Kind)
	if label == "" {
		label = string(viewmodel.StateEmpty)
	}
	if strings.TrimSpace(state.Message) == "" {
		return r.StatusChip(label, toneForState(state.Kind)) + "\n"
	}
	return r.StatusChip(label, toneForState(state.Kind)) + " " + strings.TrimSpace(state.Message) + "\n"
}

func toneForState(kind viewmodel.StateKind) viewmodel.Tone {
	switch kind {
	case viewmodel.StateReady:
		return viewmodel.TonePositive
	case viewmodel.StateEmpty:
		return viewmodel.ToneMuted
	case viewmodel.StatePermission:
		return viewmodel.ToneWarning
	case viewmodel.StateError:
		return viewmodel.ToneDanger
	default:
		return viewmodel.ToneNeutral
	}
}

func renderRow(values []string, widths []int) string {
	parts := make([]string, len(widths))
	for i := range widths {
		value := ""
		if i < len(values) {
			value = values[i]
		}
		parts[i] = padRight(value, widths[i])
	}
	return strings.Join(parts, "  ")
}

func wrapLines(value string, width int) []string {
	value = strings.TrimRight(value, "\n")
	if value == "" {
		return nil
	}
	rawLines := strings.Split(value, "\n")
	out := make([]string, 0, len(rawLines))
	for _, raw := range rawLines {
		line := strings.TrimRight(raw, " ")
		for displayWidth(line) > width {
			cut := cutWidthAtWord(line, width)
			out = append(out, strings.TrimRight(line[:cut], " "))
			line = strings.TrimLeft(line[cut:], " ")
		}
		out = append(out, line)
	}
	return out
}

func truncate(value string, width int) string {
	if width <= 0 || displayWidth(value) <= width {
		return value
	}
	if width <= 1 {
		return value[:0]
	}
	cut := cutWidth(value, width-1)
	return strings.TrimRight(value[:cut], " ") + "."
}

func cutWidthAtWord(value string, width int) int {
	hardCut := cutWidth(value, width)
	if hardCut <= 0 || hardCut >= len(value) {
		return hardCut
	}
	for i := hardCut; i > 0; i-- {
		if value[i-1] == ' ' || value[i-1] == '\t' {
			return i
		}
	}
	return hardCut
}

func cutWidth(value string, width int) int {
	if width <= 0 {
		return 0
	}
	count := 0
	for index, r := range value {
		next := count + runeWidth(r)
		if next > width {
			return index
		}
		count = next
	}
	return len(value)
}

func displayWidth(value string) int {
	width := 0
	for _, r := range value {
		width += runeWidth(r)
	}
	return width
}

func runeWidth(r rune) int {
	if r == utf8.RuneError {
		return 1
	}
	return 1
}

func padRight(value string, width int) string {
	padding := width - displayWidth(value)
	if padding <= 0 {
		return value
	}
	return value + strings.Repeat(" ", padding)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
