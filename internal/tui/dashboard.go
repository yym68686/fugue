package tui

import (
	"fmt"
	"math"
	"strings"
	"time"
)

func (m *Model) panelFrame(width, height int, title, color string) string {
	if width < 2 || height < 2 {
		return ""
	}
	p := theme(m.prefs.Theme)
	h, v, tl, tr, bl, br := "─", "│", "╭", "╮", "╰", "╯"
	if m.prefs.Graph == "ascii" {
		h, v, tl, tr, bl, br = "-", "|", "+", "+", "+", "+"
	}
	border := m.style(blendColor(color, p.background, 0.5))
	label := clip(" "+title+" ", max(0, width-4))
	line := border.Render(tl+h) + m.style(color).Bold(true).Render(label) + border.Render(strings.Repeat(h, max(0, width-3-displayWidth(label)))+tr)
	lines := []string{line}
	for i := 1; i < height-1; i++ {
		lines = append(lines, border.Render(v)+strings.Repeat(" ", width-2)+border.Render(v))
	}
	lines = append(lines, border.Render(bl+strings.Repeat(h, width-2)+br))
	return strings.Join(lines, "\n")
}

func (m *Model) loadColor(value float64) string {
	p := theme(m.prefs.Theme)
	if value >= 90 {
		return p.red
	}
	if value >= 75 {
		return p.amber
	}
	return p.green
}

func (m *Model) meter(value, maximum float64, width int, color string) string {
	if width < 1 {
		return ""
	}
	if maximum <= 0 {
		maximum = 1
	}
	fill := min(width, max(0, int(math.Round(value/maximum*float64(width)))))
	a, b := "━", "─"
	if m.prefs.Graph == "ascii" {
		a, b = "#", "-"
	}
	return m.style(color).Render(strings.Repeat(a, fill)) + m.style(theme(m.prefs.Theme).border).Render(strings.Repeat(b, width-fill))
}

func latestPoint(series Series, now time.Time) (Point, bool) {
	for i := len(series.Points) - 1; i >= 0; i-- {
		p := series.Points[i]
		if p.Value != nil && !p.At.After(now) {
			return p, true
		}
	}
	return Point{}, false
}

func (m *Model) renderCapacity(width, height int) string {
	p := theme(m.prefs.Theme)
	lines := []string{}
	var detail *ResourceDetail
	if row := m.selectedRow(); !m.overviewCharts && row != nil {
		detail = row.Detail
	}
	for _, s := range m.chartSeries() {
		if len(lines)+3 > height {
			break
		}
		point, ok := latestPoint(s, m.chartNow())
		if !ok {
			continue
		}
		value := *point.Value
		maximum := 0.0
		if s.Unit == "%" {
			maximum = 100
		}
		if s.Unit == "ratio" {
			maximum = 1
		}
		if s.Limit != nil {
			maximum = *s.Limit
		}
		if maximum == 0 {
			if limit, ok := m.snapshot.MetricLimits[s.ID]; ok {
				maximum = limit
			}
		}
		label := strings.ToUpper(s.Label)
		current := formatValue(s.Unit, value)
		color := p.accent
		if maximum > 0 {
			color = m.loadColor(value / maximum * 100)
		}
		lines = append(lines, m.style(p.muted).Render(pad(label, max(1, width-displayWidth(current))))+m.style(color).Bold(true).Render(current))
		if maximum > 0 {
			lines = append(lines, m.meter(value, maximum, width, color))
		}
		capacity := "observed " + point.At.Local().Format("15:04:05")
		if maximum > 0 && s.Unit != "%" && s.Unit != "ratio" {
			capacity = current + " / " + formatValue(s.Unit, maximum) + " · used / limit"
		}
		if detail != nil {
			for _, f := range detail.Capacity {
				if strings.EqualFold(f.Label, s.Label) {
					capacity = f.Value + " · used / total"
				}
			}
		}
		if m.chartNow().Sub(point.At) > staleAfter(s) {
			capacity = "STALE · " + point.At.Local().Format("15:04:05")
		}
		lines = append(lines, m.style(p.muted).Render(clip(capacity, width)))
	}
	if len(lines) == 0 {
		lines = append(lines, "Waiting for observations")
	}
	return strings.Join(lines, "\n")
}

func (m *Model) fieldRows(fields []Field, width int) []string {
	p := theme(m.prefs.Theme)
	lines := []string{}
	labelWidth := min(22, max(10, width/3))
	for _, f := range fields {
		if strings.TrimSpace(f.Value) == "" {
			continue
		}
		color := p.foreground
		if f.Tone != "" {
			color = m.tone(f.Tone)
		}
		lines = append(lines, m.style(p.muted).Render(pad(f.Label, labelWidth))+" "+m.style(color).Render(clip(f.Value, max(1, width-labelWidth-1))))
	}
	return lines
}

func (m *Model) renderOverview(width, height int) string {
	p := theme(m.prefs.Theme)
	lines := m.fieldRows(m.snapshot.Fields, width)
	// Fit the state summary and always reserve room for source health.
	if len(lines) > max(1, height-len(m.snapshot.Sources)-3) {
		lines = lines[:max(1, height-len(m.snapshot.Sources)-3)]
	}
	if len(lines) > 0 {
		lines = append(lines, "")
	}
	for _, source := range m.snapshot.Sources {
		stamp := "--"
		if !source.ObservedAt.IsZero() {
			stamp = source.ObservedAt.Local().Format("15:04:05")
		}
		lines = append(lines, m.style(m.tone(source.State)).Render("● ")+pad(source.ID, max(8, width/3))+" "+pad(source.State, 12)+" "+m.style(p.muted).Render(stamp))
	}
	for _, s := range m.snapshot.Series {
		if len(s.Points) == 0 && s.State != "available" {
			lines = append(lines, m.style(p.muted).Render(s.Label+" · "+s.State))
		}
	}
	if len(m.snapshot.Events) > 0 && len(lines)+2 < height {
		lines = append(lines, "", m.style(p.amber).Bold(true).Render("RECENT EVENTS"))
		for _, e := range m.snapshot.Events {
			if len(lines) >= height {
				break
			}
			lines = append(lines, m.style(m.tone(e.Severity)).Render(e.At.Local().Format("15:04:05")+" "+e.Message))
		}
	}
	return strings.Join(lines, "\n")
}

func (m *Model) renderInspector(width, height int) string {
	p := theme(m.prefs.Theme)
	row := m.selectedRow()
	if row == nil {
		return "Select a resource to inspect"
	}
	fields := []Field{}
	if m.table < len(m.snapshot.Tables) {
		for i, label := range m.snapshot.Tables[m.table].Columns {
			fields = append(fields, Field{Label: label, Value: cell(*row, i)})
		}
	}
	if row.Detail != nil {
		fields = row.Detail.Fields
	}
	lines := m.fieldRows(fields, width)
	if row.Detail != nil {
		d := row.Detail
		if !m.layout().context.Empty() {
			return strings.Join(lines, "\n")
		}
		if len(lines) > max(1, height/2) {
			lines = lines[:max(1, height/2)]
		}
		lines = append(lines, "", m.style(p.accent).Bold(true).Render(fmt.Sprintf("%s · %d", strings.ToUpper(first(d.ItemsTitle, "Related resources")), len(d.Items))))
		if len(d.Items) == 0 {
			lines = append(lines, m.style(p.muted).Render("No workloads on this node"))
		}
		visible := max(0, height-len(lines)-1)
		for _, f := range d.Items[:min(len(d.Items), visible)] {
			lines = append(lines, pad(f.Label, max(1, width-displayWidth(f.Value)-2))+"  "+m.style(p.muted).Render(f.Value))
		}
		if len(d.Items) > visible {
			lines = append(lines, m.style(p.muted).Render(fmt.Sprintf("+%d more · Enter opens the full workload list", len(d.Items)-visible)))
		}
	} else if len(lines)+2 < height {
		if len(m.snapshot.Fields) > 0 {
			lines = append(lines, "", m.style(p.accent).Bold(true).Render(strings.ToUpper(m.request.Target.Kind)+" · "+m.snapshot.Title))
			lines = append(lines, m.fieldRows(m.snapshot.Fields, width)...)
		} else {
			lines = append(lines, "", m.style(p.muted).Render("Enter open resource · Backspace return"))
		}
		if len(m.snapshot.Events) > 0 {
			lines = append(lines, m.style(p.amber).Bold(true).Render("LATEST ACTIVITY"))
			for _, e := range m.snapshot.Events {
				if len(lines) >= height {
					break
				}
				lines = append(lines, e.At.Local().Format("15:04:05")+" "+e.Message)
			}
		}
	}
	return strings.Join(lines, "\n")
}

func (m *Model) renderRelated(width, height int) string {
	p := theme(m.prefs.Theme)
	lines := []string{}
	if row := m.selectedRow(); row != nil && row.Detail != nil {
		d := row.Detail
		lines = append(lines, m.style(p.accent).Bold(true).Render(fmt.Sprintf("%s · %d", strings.ToUpper(d.ItemsTitle), len(d.Items))))
		if len(d.Items) == 0 {
			lines = append(lines, "No workloads on this node")
		}
		for _, f := range d.Items {
			if len(lines) >= height-3 {
				break
			}
			lines = append(lines, pad(f.Label, max(1, width-displayWidth(f.Value)-1))+" "+m.style(p.muted).Render(f.Value))
		}
		if len(d.Items) > max(0, height-4) {
			lines = append(lines, "Enter opens the full workload list")
		}
	}
	if len(m.snapshot.Events) > 0 && len(lines) < height-2 {
		lines = append(lines, "", m.style(p.amber).Bold(true).Render("RECENT EVENTS"))
		for _, e := range m.snapshot.Events {
			if len(lines) >= height {
				break
			}
			lines = append(lines, m.style(m.tone(e.Severity)).Render(e.At.Local().Format("15:04:05")+" "+e.Message))
		}
	}
	return strings.Join(lines, "\n")
}

func tableWidths(table Table, width int) []int {
	out := []int{}
	remaining := width - 3
	for i, label := range table.Columns {
		size := 12
		switch strings.ToLower(label) {
		case "cpu", "memory", "disk":
			size = 7
		case "status", "phase":
			size = 10
		case "ready", "pods":
			size = 6
		case "workloads", "restarts":
			size = 9
		case "region":
			size = 14
		}
		if i == 0 {
			size = min(28, max(14, width/4))
		}
		if size+1 > remaining {
			if i == 0 {
				out = append(out, max(1, remaining))
			}
			break
		}
		out = append(out, size)
		remaining -= size + 1
	}
	if len(out) > 0 {
		out[0] += max(0, remaining)
	}
	return out
}
