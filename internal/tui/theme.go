package tui

import (
	"charm.land/lipgloss/v2"
)

type palette struct {
	background, foreground, muted, border, accent, green, amber, red, selection string
	graphs                                                                      []string
}

func theme(name string) palette {
	if name == "light" {
		return palette{"#fafafa", "#202329", "#65707b", "#b7c1c7", "#007c91", "#13804a", "#976200", "#c3274b", "#dcecef", []string{"#007c91", "#13804a", "#ad5a00", "#b23368"}}
	}
	return palette{"#15191c", "#e0e7e9", "#8d9b9e", "#46565a", "#65d6db", "#83d6a2", "#e8ba69", "#ed8291", "#293d42", []string{"#65d6db", "#83d6a2", "#e8ba69", "#e297bd"}}
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
