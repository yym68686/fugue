package terminal

import "strings"

type ColorLevel int

const (
	ColorNone ColorLevel = iota
	ColorBasic16
	ColorANSI256
	ColorTrueColor
)

func DetectColorLevel(mode Mode, stdoutTTY bool, lookup func(string) (string, bool)) ColorLevel {
	if mode == ModeNever {
		return ColorNone
	}
	if hasNoColor(lookup) && mode != ModeAlways {
		return ColorNone
	}
	term := lookupValue(lookup, "TERM")
	colorTerm := strings.ToLower(lookupValue(lookup, "COLORTERM"))
	if mode == ModeAuto && (!stdoutTTY || strings.EqualFold(term, "dumb")) {
		return ColorNone
	}
	if strings.Contains(colorTerm, "truecolor") || strings.Contains(colorTerm, "24bit") {
		return ColorTrueColor
	}
	if strings.Contains(strings.ToLower(term), "256color") {
		return ColorANSI256
	}
	return ColorBasic16
}

type Role string

const (
	RoleSuccess   Role = "success"
	RoleWarning   Role = "warning"
	RoleDanger    Role = "danger"
	RoleMuted     Role = "muted"
	RoleAccent    Role = "accent"
	RoleSelection Role = "selection"
	RoleBorder    Role = "border"
)

type Palette struct {
	Level ColorLevel
}

func (p Palette) Enabled() bool {
	return p.Level != ColorNone
}

func (p Palette) Style(role Role, value string) string {
	if !p.Enabled() || value == "" {
		return value
	}
	code := p.ansiCode(role)
	if code == "" {
		return value
	}
	return "\x1b[" + code + "m" + value + "\x1b[0m"
}

func (p Palette) ansiCode(role Role) string {
	switch role {
	case RoleSuccess:
		return "32"
	case RoleWarning:
		return "33"
	case RoleDanger:
		return "31"
	case RoleMuted, RoleBorder:
		return "2"
	case RoleAccent:
		return "36"
	case RoleSelection:
		return "7"
	default:
		return ""
	}
}

func lookupValue(lookup func(string) (string, bool), key string) string {
	if lookup == nil {
		return ""
	}
	value, _ := lookup(key)
	return strings.TrimSpace(value)
}
