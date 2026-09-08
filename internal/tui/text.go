package tui

import (
	"strings"
	"unicode"

	"github.com/charmbracelet/x/ansi"
)

// Plain removes terminal control sequences from untrusted server data before it
// reaches the terminal renderer, including OSC clipboard/title commands.
func Plain(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == 0x7f {
			return ' '
		}
		return r
	}, ansi.Strip(value))
}
func clip(value string, width int) string {
	if width <= 0 {
		return ""
	}
	return ansi.Truncate(value, width, "…")
}
func pad(value string, width int) string {
	value = clip(value, width)
	return value + strings.Repeat(" ", max(0, width-ansi.StringWidth(value)))
}
func sanitizeSnapshot(s Snapshot) Snapshot {
	s.Title = Plain(s.Title)
	s.Subtitle = Plain(s.Subtitle)
	s.Status = Plain(s.Status)
	for i := range s.Fields {
		s.Fields[i].Label = Plain(s.Fields[i].Label)
		s.Fields[i].Value = Plain(s.Fields[i].Value)
	}
	for i := range s.Tables {
		s.Tables[i].Title = Plain(s.Tables[i].Title)
		for j := range s.Tables[i].Columns {
			s.Tables[i].Columns[j] = Plain(s.Tables[i].Columns[j])
		}
		for j := range s.Tables[i].Rows {
			for k := range s.Tables[i].Rows[j].Cells {
				s.Tables[i].Rows[j].Cells[k] = Plain(s.Tables[i].Rows[j].Cells[k])
			}
		}
	}
	for i := range s.Events {
		s.Events[i].Message = Plain(s.Events[i].Message)
	}
	for i := range s.Logs {
		s.Logs[i] = Plain(s.Logs[i])
	}
	for i := range s.Sources {
		s.Sources[i].Message = Plain(s.Sources[i].Message)
	}
	for i := range s.Series {
		s.Series[i].Label = Plain(s.Series[i].Label)
		s.Series[i].Source = Plain(s.Series[i].Source)
		s.Series[i].Unit = Plain(s.Series[i].Unit)
	}
	for i := range s.Actions {
		s.Actions[i].Label = Plain(s.Actions[i].Label)
		s.Actions[i].Reason = Plain(s.Actions[i].Reason)
	}
	return s
}
