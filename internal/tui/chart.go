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
	p := theme(m.prefs.Theme)
	color := p.graphs[index%len(p.graphs)]
	plotWidth, plotHeight := max(1, width-2), max(1, height-4)
	start, end := m.now.Add(-m.request.Window), m.now
	buckets := Buckets(series, start, end, plotWidth)
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
	if series.Limit != nil {
		maximum = math.Max(maximum, *series.Limit)
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
	if valid == 1 {
		caption += " · 1 sample"
	}
	title := pad(strings.ToUpper(series.Label), max(1, width-len(latest)-1)) + latest
	lines := []string{m.style(color).Bold(true).Render(clip(title, width))}
	if valid == 0 {
		lines = append(lines, m.style(p.muted).Render(pad(caption, plotWidth)))
		for len(lines) < height-2 {
			lines = append(lines, "")
		}
	} else {
		grid := drawPlot(buckets, plotWidth, plotHeight, maximum, m.prefs.Graph, series.Limit)
		for _, line := range grid {
			lines = append(lines, m.style(color).Render(line))
		}
	}
	if m.graphIndex == index && m.graphCursor >= 0 && len(buckets) > 0 {
		b := buckets[min(m.graphCursor, len(buckets)-1)]
		caption = b.At.Local().Format("15:04:05") + " · no sample"
		if b.Last != nil {
			caption = b.At.Local().Format("15:04:05") + " · " + formatValue(series.Unit, *b.Last)
		}
	}
	lines = append(lines, m.style(p.muted).Render(clip("0 "+series.Unit+"   "+m.prefs.Window+"   max "+formatValue(series.Unit, maximum), width)), m.style(p.muted).Render(clip(caption, width)))
	return strings.Join(lines, "\n")
}

// Each raster column corresponds to a time bucket. Braille has four vertical
// subcells; empty buckets never connect to adjacent samples across a gap.
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
