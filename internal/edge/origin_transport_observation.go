package edge

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
)

// Classify typed transport errors without logging their text, which can contain
// origin URLs or credentials. This describes transport or response-body failures;
// it does not infer whether the origin processed the request or make it retryable.
func originTransportErrorKind(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset"
	case errors.Is(err, syscall.EPIPE):
		return "broken_pipe"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(err, net.ErrClosed):
		return "connection_closed"
	}
	if originTransportTimeout(err) {
		return "timeout"
	}
	return "other"
}

func originTransportTimeout(err error) bool {
	// An outer url.Error can return Timeout=false when an intermediate wrapper
	// does not implement Timeout. Check the complete error chain in that case.
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() error }:
		return originTransportTimeout(wrapped.Unwrap())
	case interface{ Unwrap() []error }:
		for _, child := range wrapped.Unwrap() {
			if originTransportTimeout(child) {
				return true
			}
		}
	}
	return false
}
