package tui

import (
	"charm.land/lipgloss/v2"
	"fmt"
	"strconv"
)

type palette struct {
	background, foreground, muted, border, accent, green, amber, red, selection string
	graphs                                                                      []string
}

func theme(name string) palette {
	if name == "light" {
		return palette{"#fafafa", "#202329", "#65707b", "#b7c1c7", "#007c91", "#13804a", "#976200", "#c3274b", "#dcecef", []string{"#007c91", "#13804a", "#ad5a00", "#b23368"}}
	}
	return palette{"#0d1117", "#dce6f0", "#8393a3", "#283b49", "#5ee1ed", "#80db9a", "#efbf71", "#f0788c", "#203846", []string{"#5ee1ed", "#80db9a", "#efbf71", "#be9bf5", "#f0788c"}}
}

func blendColor(foreground, background string, strength float64) string {
	f, _ := strconv.ParseUint(foreground[1:], 16, 24)
	b, _ := strconv.ParseUint(background[1:], 16, 24)
	var value uint64
	for _, shift := range []uint{16, 8, 0} {
		channel := uint64(float64((f>>shift)&255)*strength + float64((b>>shift)&255)*(1-strength))
		value |= channel << shift
	}
	return fmt.Sprintf("#%06x", value)
}
func (m *Model) style(color string) lipgloss.Style {
	if !m.opts.Color {
		return lipgloss.NewStyle()
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color(color))
}
func (m *Model) tone(value string) string {
	p := theme(m.prefs.Theme)
	switch value {
	case "ready", "healthy", "completed", "running", "available", "success":
		return p.green
	case "failed", "error", "unhealthy", "crashloopbackoff":
		return p.red
	case "pending", "degraded", "stale", "warning", "permission_denied":
		return p.amber
	}
	return p.muted
}
