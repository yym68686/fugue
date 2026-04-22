package cli

import (
	"errors"
	"strings"
)

const (
	ExitCodeSuccess          = 0
	ExitCodeUserInput        = 2
	ExitCodePermissionDenied = 3
	ExitCodeNotFound         = 4
	ExitCodeSystemFault      = 5
	ExitCodeIndeterminate    = 6
)

type exitCodeCarrier interface {
	ExitCode() int
}

type exitCodeError struct {
	code int
	err  error
}

func (e *exitCodeError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *exitCodeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *exitCodeError) ExitCode() int {
	if e == nil {
		return ExitCodeIndeterminate
	}
	return e.code
}

func withExitCode(err error, code int) error {
	if err == nil {
		return nil
	}
	var existing exitCodeCarrier
	if errors.As(err, &existing) {
		return err
	}
	return &exitCodeError{code: code, err: err}
}

func ExitCodeForError(err error) int {
	if err == nil {
		return ExitCodeSuccess
	}
	var coded exitCodeCarrier
	if errors.As(err, &coded) {
		if code := coded.ExitCode(); code > 0 {
			return code
		}
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case looksPermissionError(message):
		return ExitCodePermissionDenied
	case looksNotFoundError(message):
		return ExitCodeNotFound
	case looksUserInputError(message):
		return ExitCodeUserInput
	case looksSystemFaultError(message):
		return ExitCodeSystemFault
	default:
		return ExitCodeIndeterminate
	}
}

func looksPermissionError(message string) bool {
	if message == "" {
		return false
	}
	for _, needle := range []string{
		"status=401",
		"status=403",
		"unauthorized",
		"forbidden",
		"permission denied",
	} {
		if strings.Contains(message, needle) {
			return true
		}
	}
	return false
}

func looksNotFoundError(message string) bool {
	if message == "" {
		return false
	}
	for _, needle := range []string{
		"status=404",
		" not found",
		"no matching pods found",
		"no running app pods found",
		"path not found",
		"resource not found",
	} {
		if strings.Contains(message, needle) {
			return true
		}
	}
	return false
}

func looksUserInputError(message string) bool {
	if message == "" {
		return false
	}
	for _, needle := range []string{
		"unknown command",
		"unsupported ",
		"unsupported output format",
		"accepts ",
		"expected ",
		" is required",
		" are required",
		"at least one",
		"either --",
		"one of --",
		"cannot be used together",
		"must use ",
		"must be ",
		"must match ",
		"refusing unredacted output",
	} {
		if strings.Contains(message, needle) {
			return true
		}
	}
	return false
}

func looksSystemFaultError(message string) bool {
	if message == "" {
		return false
	}
	for _, needle := range []string{
		"status=500",
		"status=502",
		"status=503",
		"status=504",
		"service unavailable",
		"temporarily unavailable",
		"bad gateway",
		"gateway timeout",
		"filesystem target is not ready",
		"unable to upgrade connection",
		"unexpected eof",
		"eof",
		"connection refused",
		"connection reset",
		"i/o timeout",
		"tls handshake timeout",
	} {
		if strings.Contains(message, needle) {
			return true
		}
	}
	return false
}
