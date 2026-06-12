package terminal

import (
	"fmt"
	"io"
)

const (
	escapeAltScreenEnter    = "\x1b[?1049h"
	escapeAltScreenExit     = "\x1b[?1049l"
	escapeCursorHide        = "\x1b[?25l"
	escapeCursorShow        = "\x1b[?25h"
	escapeBracketedPasteOn  = "\x1b[?2004h"
	escapeBracketedPasteOff = "\x1b[?2004l"
)

type RawModeController interface {
	EnableRawMode() (func() error, error)
}

type SessionOptions struct {
	Writer            io.Writer
	AltScreen         bool
	RawMode           bool
	BracketedPaste    bool
	HideCursor        bool
	RawModeController RawModeController
}

type Session struct {
	opts       SessionOptions
	restoreFns []func() error
	active     bool
}

type PanicError struct {
	Value any
}

func (e PanicError) Error() string {
	return fmt.Sprintf("terminal session panic: %v", e.Value)
}

func StartSession(opts SessionOptions) (*Session, error) {
	session := &Session{opts: opts, active: true}
	if opts.RawMode && opts.RawModeController != nil {
		restore, err := opts.RawModeController.EnableRawMode()
		if err != nil {
			return nil, err
		}
		if restore != nil {
			session.restoreFns = append(session.restoreFns, restore)
		}
	}
	if opts.Writer != nil {
		if opts.AltScreen {
			if _, err := io.WriteString(opts.Writer, escapeAltScreenEnter); err != nil {
				return nil, err
			}
		}
		if opts.HideCursor {
			if _, err := io.WriteString(opts.Writer, escapeCursorHide); err != nil {
				return nil, err
			}
		}
		if opts.BracketedPaste {
			if _, err := io.WriteString(opts.Writer, escapeBracketedPasteOn); err != nil {
				return nil, err
			}
		}
	}
	return session, nil
}

func (s *Session) Restore() error {
	if s == nil || !s.active {
		return nil
	}
	s.active = false
	var firstErr error
	if s.opts.Writer != nil {
		if s.opts.BracketedPaste {
			if _, err := io.WriteString(s.opts.Writer, escapeBracketedPasteOff); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if s.opts.HideCursor {
			if _, err := io.WriteString(s.opts.Writer, escapeCursorShow); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if s.opts.AltScreen {
			if _, err := io.WriteString(s.opts.Writer, escapeAltScreenExit); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	for i := len(s.restoreFns) - 1; i >= 0; i-- {
		if err := s.restoreFns[i](); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func RunWithSession(opts SessionOptions, run func(*Session) error) (err error) {
	session, err := StartSession(opts)
	if err != nil {
		return err
	}
	defer func() {
		restoreErr := session.Restore()
		if recovered := recover(); recovered != nil {
			err = PanicError{Value: recovered}
			return
		}
		if err == nil {
			err = restoreErr
		}
	}()
	if run == nil {
		return nil
	}
	return run(session)
}
