package routeprobe

import (
	"context"
	"errors"
	"net/url"
	"testing"
)

func TestConnectionClassificationPreservesExistingLeaseSemantics(t *testing.T) {
	for _, original := range []error{context.DeadlineExceeded, errors.New("connection refused with private address")} {
		classified := classifyTransportError(original)
		wrapped := &url.Error{Op: "Get", URL: "https://private.example.test", Err: connectionError{err: classified}}
		result := classifyTransportError(wrapped)
		if !errors.Is(result, ErrConnectionUnavailable) || errors.Is(result, ErrUnavailable) != errors.Is(classified, ErrUnavailable) || result.Error() != classified.Error() {
			t.Fatal("connection classification changed lease semantics or leaked transport details", result)
		}
	}
	if errors.Is(classifyTransportError(context.DeadlineExceeded), ErrConnectionUnavailable) {
		t.Fatal("an unspecified transport timeout was promoted to a connection failure")
	}
}
