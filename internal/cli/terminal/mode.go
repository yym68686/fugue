package terminal

import (
	"fmt"
	"strings"
)

type Mode string

const (
	ModeAuto   Mode = "auto"
	ModeAlways Mode = "always"
	ModeNever  Mode = "never"
)

func ParseMode(value string) (Mode, error) {
	switch Mode(strings.ToLower(strings.TrimSpace(value))) {
	case "", ModeAuto:
		return ModeAuto, nil
	case ModeAlways:
		return ModeAlways, nil
	case ModeNever:
		return ModeNever, nil
	default:
		return "", fmt.Errorf("unsupported terminal mode %q; expected auto, always, or never", value)
	}
}

func (m Mode) String() string {
	if m == "" {
		return string(ModeAuto)
	}
	return string(m)
}
