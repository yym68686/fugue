package terminal

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseMode(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", "auto", "AUTO"} {
		mode, err := ParseMode(value)
		if err != nil {
			t.Fatalf("parse %q: %v", value, err)
		}
		if mode != ModeAuto {
			t.Fatalf("expected auto for %q, got %q", value, mode)
		}
	}
	for _, value := range []string{"always", "never"} {
		if _, err := ParseMode(value); err != nil {
			t.Fatalf("parse %q: %v", value, err)
		}
	}
	if _, err := ParseMode("sometimes"); err == nil {
		t.Fatal("expected invalid mode to fail")
	}
}

func TestDetectorUsesTTYAndEnvFallbacks(t *testing.T) {
	t.Parallel()

	lookup := mapLookup(map[string]string{
		"COLUMNS":   "100",
		"LINES":     "32",
		"COLORTERM": "truecolor",
		"TERM":      "xterm-256color",
	})
	detector := Detector{
		Stdout:    os.Stdout,
		Stderr:    os.Stderr,
		Stdin:     os.Stdin,
		LookupEnv: lookup,
		IsTerminal: func(fd int) bool {
			return true
		},
		TerminalSize: func(fd int) (int, int, error) {
			return 120, 40, nil
		},
	}

	env := detector.Detect(ModeAuto, ModeAuto)
	if !env.Stdout.IsTTY || env.Stdout.Width != 120 || env.Stdout.Height != 40 {
		t.Fatalf("unexpected stdout detection %+v", env.Stdout)
	}
	if env.ColorLevel != ColorTrueColor {
		t.Fatalf("expected truecolor, got %v", env.ColorLevel)
	}
}

func TestDetectorHonorsNoColorAndNonTTY(t *testing.T) {
	t.Parallel()

	lookup := mapLookup(map[string]string{
		"NO_COLOR": "1",
		"COLUMNS":  "88",
		"LINES":    "24",
		"TERM":     "xterm-256color",
	})
	detector := Detector{
		Stdout:    os.Stdout,
		Stderr:    os.Stderr,
		Stdin:     os.Stdin,
		LookupEnv: lookup,
		IsTerminal: func(fd int) bool {
			return false
		},
	}

	env := detector.Detect(ModeAuto, ModeAuto)
	if env.Stdout.IsTTY || env.Stdout.Width != 88 || env.Stdout.Height != 24 {
		t.Fatalf("unexpected non-tty detection %+v", env.Stdout)
	}
	if !env.NoColor || env.ColorLevel != ColorNone {
		t.Fatalf("expected NO_COLOR to disable color, got no_color=%v level=%v", env.NoColor, env.ColorLevel)
	}
	if level := DetectColorLevel(ModeAlways, false, lookup); level != ColorANSI256 {
		t.Fatalf("expected --color always to force terminal color capability, got %v", level)
	}
}

func TestPaletteAndGuardedWriter(t *testing.T) {
	t.Parallel()

	palette := Palette{Level: ColorBasic16}
	colored := palette.Style(RoleSuccess, "ready")
	if !strings.Contains(colored, "\x1b[32mready\x1b[0m") {
		t.Fatalf("expected green success styling, got %q", colored)
	}
	if got := (Palette{Level: ColorNone}).Style(RoleSuccess, "ready"); got != "ready" {
		t.Fatalf("expected no-color palette to return plain text, got %q", got)
	}

	var out bytes.Buffer
	writer := GuardedWriter{Writer: &out, AllowANSI: false}
	if _, err := writer.Write([]byte("a\x1b[31mred\x1b[0m z")); err != nil {
		t.Fatalf("write guarded ansi: %v", err)
	}
	if got := out.String(); got != "ared z" {
		t.Fatalf("expected ANSI to be stripped, got %q", got)
	}
}

func TestRunBoundedProbeTimesOut(t *testing.T) {
	t.Parallel()

	start := time.Now()
	result := RunBoundedProbe(context.Background(), 10*time.Millisecond, func(ctx context.Context) ProbeResult {
		time.Sleep(50 * time.Millisecond)
		return ProbeResult{CursorRow: 10}
	})
	if !result.TimedOut || result.Err == nil {
		t.Fatalf("expected timed-out probe, got %+v", result)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("expected bounded probe to return quickly, took %s", elapsed)
	}
}

func TestRunBoundedProbeReturnsResult(t *testing.T) {
	t.Parallel()

	result := RunBoundedProbe(context.Background(), DefaultProbeTimeout, func(ctx context.Context) ProbeResult {
		return ProbeResult{CursorRow: 3, CursorColumn: 9, DefaultForeground: "default", KeyboardEnhancement: true}
	})
	if result.TimedOut || result.CursorRow != 3 || result.CursorColumn != 9 || !result.KeyboardEnhancement {
		t.Fatalf("unexpected probe result %+v", result)
	}
}

func TestSessionRestoresOnSuccessAndPanic(t *testing.T) {
	t.Parallel()

	controller := &fakeRawModeController{}
	var out bytes.Buffer
	err := RunWithSession(SessionOptions{
		Writer:            &out,
		AltScreen:         true,
		RawMode:           true,
		BracketedPaste:    true,
		HideCursor:        true,
		RawModeController: controller,
	}, func(*Session) error {
		return nil
	})
	if err != nil {
		t.Fatalf("run session: %v", err)
	}
	for _, want := range []string{escapeAltScreenEnter, escapeCursorHide, escapeBracketedPasteOn, escapeBracketedPasteOff, escapeCursorShow, escapeAltScreenExit} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("expected session output to contain %q, got %q", want, out.String())
		}
	}
	if !controller.enabled || !controller.restored {
		t.Fatalf("expected raw mode controller to enable and restore, got %+v", controller)
	}

	controller = &fakeRawModeController{}
	out.Reset()
	err = RunWithSession(SessionOptions{
		Writer:            &out,
		AltScreen:         true,
		RawMode:           true,
		RawModeController: controller,
	}, func(*Session) error {
		panic("boom")
	})
	var panicErr PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("expected panic error, got %v", err)
	}
	if !strings.Contains(out.String(), escapeAltScreenExit) || !controller.restored {
		t.Fatalf("expected panic path to restore terminal, output=%q controller=%+v", out.String(), controller)
	}
}

func TestSessionRestoresOnCanceledAndFatalErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "ctrl-c context cancellation", err: context.Canceled},
		{name: "fatal command error", err: errors.New("fatal render error")},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			controller := &fakeRawModeController{}
			var out bytes.Buffer
			err := RunWithSession(SessionOptions{
				Writer:            &out,
				AltScreen:         true,
				RawMode:           true,
				BracketedPaste:    true,
				HideCursor:        true,
				RawModeController: controller,
			}, func(*Session) error {
				return tc.err
			})
			if !errors.Is(err, tc.err) {
				t.Fatalf("expected %v, got %v", tc.err, err)
			}
			for _, want := range []string{escapeBracketedPasteOff, escapeCursorShow, escapeAltScreenExit} {
				if !strings.Contains(out.String(), want) {
					t.Fatalf("expected restore output to contain %q, got %q", want, out.String())
				}
			}
			if !controller.restored {
				t.Fatalf("expected raw mode restore for %s", tc.name)
			}
		})
	}
}

type fakeRawModeController struct {
	enabled  bool
	restored bool
}

func (c *fakeRawModeController) EnableRawMode() (func() error, error) {
	c.enabled = true
	return func() error {
		c.restored = true
		return nil
	}, nil
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
