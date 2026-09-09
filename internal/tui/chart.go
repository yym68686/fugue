package tui

import (
	"fmt"
	"math"
	"strings"
	"time"
)

func (m *Model) renderChart(series Series, width, height, index int) string {
	if width < 4 || height < 3 {
		return ""
	}
	if value, ok := m.snapshot.MetricLimits[series.ID]; ok && series.Limit == nil {
		series.Limit = &value
	}
	p := theme(m.prefs.Theme)
	color := p.graphs[index%len(p.graphs)]
	plotWidth, plotHeight := max(1, width-5), max(1, height-4)
	now := m.chartNow()
	start, end := chartRange(series, now, m.request.Window)
	buckets := plotBuckets(series, start, end, plotWidth)
	latest := "--"
	var latestAt time.Time
	maximum := 0.0
	valid := 0
	for _, point := range series.Points {
		if point.At.Before(start) || point.At.After(end) || point.Value == nil {
			continue
		}
		valid++
		latest = formatValue(series.Unit, *point.Value)
		latestAt = point.At
		if *point.Value > maximum {
			maximum = *point.Value
		}
	}
	peak := maximum
	if series.Limit != nil {
		maximum = math.Max(maximum, *series.Limit)
	}
	if series.Unit == "%" {
		maximum = 100
	}
	if series.Unit == "ratio" {
		maximum = 1
	}
	if maximum <= 0 {
		maximum = 1
	}
	caption := series.State
	if caption == "" {
		caption = "collecting"
	}
	if valid > 0 {
		caption = "observed " + latestAt.Local().Format("15:04:05")
	}
	if valid > 0 && series.State != "" && series.State != "available" {
		caption = series.State + " · " + latestAt.Local().Format("15:04:05")
	}
	if valid == 1 {
		caption = "collecting · 1 sample"
	}
	if valid > 0 && now.Sub(latestAt) > staleAfter(series) {
		caption = "stale · " + latestAt.Local().Format("15:04:05")
	}
	title := pad(strings.ToUpper(series.Label), max(1, width-len(latest)-1)) + latest
	lines := []string{m.style(color).Bold(true).Render(clip(title, width))}
	if valid < 2 {
		lines = append(lines, m.style(p.muted).Render(pad(caption, plotWidth)))
		for len(lines) < height-2 {
			lines = append(lines, "")
		}
	} else {
		grid := drawPlot(buckets, plotWidth, plotHeight, maximum, m.prefs.Graph, series.Limit)
		for y, line := range grid {
			tick := ""
			scale, suffix := 1.0, ""
			if series.Unit == "%" {
				suffix = "%"
			}
			if series.Unit == "ratio" {
				scale, suffix = 100, "%"
			}
			if y == 0 {
				tick = axisNumber(maximum*scale) + suffix
			} else if y == plotHeight-1 {
				tick = "0" + suffix
			} else if y == plotHeight/2 {
				tick = axisNumber(maximum*scale/2) + suffix
			}
			shade := blendColor(color, p.background, 0.95-0.5*float64(y)/float64(max(1, plotHeight-1)))
			lines = append(lines, m.style(p.muted).Render(pad(tick, 4)+" ")+m.style(shade).Render(line))
		}
	}
	if m.graphIndex == index && m.graphCursor >= 0 && len(buckets) > 0 {
		b := buckets[min(m.graphCursor, len(buckets)-1)]
		caption = b.At.Local().Format("15:04:05") + " · no sample"
		if b.Last != nil {
			caption = b.LastAt.Local().Format("15:04:05") + " · " + formatValue(series.Unit, *b.Last)
			if b.Interpolated {
				caption += " (interpolated)"
			}
		}
	}
	windowLabel := m.prefs.Window
	if valid > 0 && end.Sub(start) < m.request.Window {
		windowLabel = chartDuration(end.Sub(start)) + " / " + windowLabel
	}
	axis := windowLabel + " · peak " + formatValue(series.Unit, peak)
	if valid == 0 {
		axis = "No measurements · " + m.prefs.Window
	}
	lines = append(lines, m.style(p.muted).Render(clip(axis, width)), m.style(p.muted).Render(clip(caption, width)))
	source := series.Source
	if series.Subject != "" {
		source = "latest peak: " + series.Subject
	}
	lines[len(lines)-1] = m.style(p.muted).Render(clip(caption+" · "+source, width))
	return strings.Join(lines, "\n")
}

func axisNumber(value float64) string {
	for _, unit := range []struct {
		size  float64
		label string
	}{{1 << 40, "T"}, {1 << 30, "G"}, {1 << 20, "M"}, {1 << 10, "K"}} {
		if value >= unit.size {
			return fmt.Sprintf("%.0f%s", value/unit.size, unit.label)
		}
	}
	if value < 1 && value > 0 {
		return fmt.Sprintf("%.1f", value)
	}
	return fmt.Sprintf("%.0f", value)
}

func chartDuration(span time.Duration) string {
	span = span.Round(time.Second)
	if span >= time.Hour && span%time.Hour == 0 {
		return fmt.Sprintf("%dh", span/time.Hour)
	}
	if span >= time.Minute && span%time.Minute == 0 {
		return fmt.Sprintf("%dm", span/time.Minute)
	}
	return span.String()
}

// Each raster column corresponds to a time bucket. Braille has four vertical
// subcells; missing observations remain gaps after cadence-aware interpolation.
func drawPlot(buckets []Bucket, width, height int, maximum float64, mode string, limit *float64) []string {
	if width < 1 || height < 1 {
		return nil
	}
	rows := make([][]rune, height)
	for y := range rows {
		rows[y] = []rune(strings.Repeat(" ", width))
	}
	if maximum <= 0 {
		maximum = 1
	}
	for x, b := range buckets {
		if x >= width || b.Last == nil {
			continue
		}
		top := int(math.Round((1 - math.Min(1, math.Max(0, *b.High/maximum))) * float64(height*4-1)))
		bottom := height*4 - 1
		if mode == "line" || mode == "ascii" {
			bottom = int(math.Round((1 - math.Min(1, math.Max(0, *b.Low/maximum))) * float64(height*4-1)))
		}
		for y := 0; y < height; y++ {
			bits := rune(0)
			filled := 0
			for sub := 0; sub < 4; sub++ {
				pixel := y*4 + sub
				if pixel >= top && pixel <= bottom {
					filled++
					switch sub {
					case 0:
						bits |= 0x09
					case 1:
						bits |= 0x12
					case 2:
						bits |= 0x24
					case 3:
						bits |= 0xc0
					}
				}
			}
			if filled == 0 {
				continue
			}
			switch mode {
			case "ascii":
				rows[y][x] = '*'
			case "block":
				rows[y][x] = []rune(" ▂▄▆█")[filled]
			default:
				rows[y][x] = 0x2800 + bits
			}
		}
	}
	if limit != nil && *limit >= 0 && *limit <= maximum {
		y := min(height-1, max(0, int((1-*limit/maximum)*float64(height-1))))
		for x := range rows[y] {
			if rows[y][x] == ' ' {
				rows[y][x] = '┄'
				if mode == "ascii" {
					rows[y][x] = '-'
				}
			}
		}
	}
	result := make([]string, height)
	for i := range rows {
		result[i] = string(rows[i])
	}
	return result
}
func formatValue(unit string, value float64) string {
	switch unit {
	case "bytes", "B":
		for _, scale := range []struct {
			value float64
			name  string
		}{{1 << 40, "TiB"}, {1 << 30, "GiB"}, {1 << 20, "MiB"}, {1 << 10, "KiB"}} {
			if value >= scale.value {
				return fmt.Sprintf("%.1f%s", value/scale.value, scale.name)
			}
		}
		return fmt.Sprintf("%.0fB", value)
	case "%":
		return fmt.Sprintf("%.1f%%", value)
	case "ratio":
		return fmt.Sprintf("%.1f%%", 100*value)
	case "ms":
		return fmt.Sprintf("%.0fms", value)
	default:
		return fmt.Sprintf("%.1f %s", value, unit)
	}
}
