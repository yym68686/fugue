package tui

import (
	"github.com/charmbracelet/x/ansi"
	"image"
	"strings"
)

func (m *Model) textLines() []string {
	rect := m.layout().body
	text := ""
	switch m.screen {
	case "details":
		text = m.renderSummary(rect.Dx(), rect.Dy())
	case "tasks":
		text = m.renderTasks(rect)
	case "help":
		text = m.help()
	}
	return strings.Split(ansi.Hardwrap(text, max(1, rect.Dx()-1), true), "\n")
}
func (m *Model) renderTextViewport(rect image.Rectangle) string {
	lines := m.textLines()
	offset := max(0, min(m.textOffset, max(0, len(lines)-rect.Dy())))
	return strings.Join(lines[offset:min(len(lines), offset+max(1, rect.Dy()))], "\n")
}
