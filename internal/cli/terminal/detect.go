package terminal

import (
	"os"
	"strconv"
	"strings"

	xterm "golang.org/x/term"
)

type StreamInfo struct {
	Name   string
	IsTTY  bool
	Width  int
	Height int
}

type Environment struct {
	Stdout      StreamInfo
	Stderr      StreamInfo
	Stdin       StreamInfo
	ColorMode   Mode
	Interactive Mode
	ColorLevel  ColorLevel
	NoColor     bool
}

type Detector struct {
	Stdout       *os.File
	Stderr       *os.File
	Stdin        *os.File
	LookupEnv    func(string) (string, bool)
	IsTerminal   func(fd int) bool
	TerminalSize func(fd int) (int, int, error)
}

func NewDetector() Detector {
	return Detector{
		Stdout:       os.Stdout,
		Stderr:       os.Stderr,
		Stdin:        os.Stdin,
		LookupEnv:    os.LookupEnv,
		IsTerminal:   xterm.IsTerminal,
		TerminalSize: xterm.GetSize,
	}
}

func (d Detector) Detect(colorMode, interactiveMode Mode) Environment {
	if d.LookupEnv == nil {
		d.LookupEnv = os.LookupEnv
	}
	return Environment{
		Stdout:      d.detectStream("stdout", d.Stdout),
		Stderr:      d.detectStream("stderr", d.Stderr),
		Stdin:       d.detectStream("stdin", d.Stdin),
		ColorMode:   colorMode,
		Interactive: interactiveMode,
		NoColor:     hasNoColor(d.LookupEnv),
		ColorLevel:  DetectColorLevel(colorMode, d.detectStream("stdout", d.Stdout).IsTTY, d.LookupEnv),
	}
}

func (d Detector) detectStream(name string, file *os.File) StreamInfo {
	info := StreamInfo{Name: name}
	if file == nil {
		info.Width, info.Height = envSize(d.LookupEnv)
		return info
	}
	fd := int(file.Fd())
	if d.IsTerminal != nil {
		info.IsTTY = d.IsTerminal(fd)
	}
	if d.TerminalSize != nil && info.IsTTY {
		if width, height, err := d.TerminalSize(fd); err == nil {
			info.Width = width
			info.Height = height
		}
	}
	if info.Width <= 0 || info.Height <= 0 {
		info.Width, info.Height = envSize(d.LookupEnv)
	}
	return info
}

func hasNoColor(lookup func(string) (string, bool)) bool {
	if lookup == nil {
		return false
	}
	value, ok := lookup("NO_COLOR")
	return ok && strings.TrimSpace(value) != ""
}

func envSize(lookup func(string) (string, bool)) (int, int) {
	width := envInt(lookup, "COLUMNS")
	height := envInt(lookup, "LINES")
	return width, height
}

func envInt(lookup func(string) (string, bool), key string) int {
	if lookup == nil {
		return 0
	}
	raw, ok := lookup(key)
	if !ok {
		return 0
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0
	}
	return value
}
