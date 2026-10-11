package managedssh

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

type runtimeCheckError struct {
	code  string
	cause error
}

func (failure *runtimeCheckError) Error() string {
	return "managed SSH runtime check failed: " + failure.code
}

func (failure *runtimeCheckError) Unwrap() error {
	return failure.cause
}

func RuntimeCheckFailure(stage, reason string, cause error) error {
	switch stage {
	case "bound_before", "capability_probe", "capability_output", "helper_probe", "bound_after":
	default:
		stage = "unknown"
	}
	switch reason {
	case "deadline", "canceled", "forbidden", "not_found", "binding", "eof", "unavailable", "empty", "malformed", "architecture", "user":
	default:
		code, err := strconv.Atoi(strings.TrimPrefix(reason, "exit_"))
		if !strings.HasPrefix(reason, "exit_") || err != nil || code < 0 || code > 255 || reason != "exit_"+strconv.Itoa(code) {
			reason = "unknown"
		}
	}
	return &runtimeCheckError{code: stage + ":" + reason, cause: cause}
}

func RuntimeFailureReason(err error) string {
	var failure *runtimeCheckError
	if errors.As(err, &failure) {
		return failure.code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "unclassified"
}
