package httpx

import (
	"net/http"
	"testing"
)

func TestClassifyResourceSafetyErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		category  string
		code      string
		retryable bool
		status    int
	}{
		{status: http.StatusRequestTimeout, code: "request_timeout", category: "transport", retryable: true},
		{status: http.StatusRequestEntityTooLarge, code: "request_too_large", category: "validation"},
		{status: http.StatusUnsupportedMediaType, code: "unsupported_media_type", category: "validation"},
		{status: http.StatusTooManyRequests, code: "rate_limited", category: "rate_limit", retryable: true},
	}

	for _, test := range tests {
		code, category, retryable := classifyError(test.status, "upload rejected")
		if code != test.code || category != test.category || retryable != test.retryable {
			t.Fatalf(
				"status %d: got code=%q category=%q retryable=%t; want code=%q category=%q retryable=%t",
				test.status,
				code,
				category,
				retryable,
				test.code,
				test.category,
				test.retryable,
			)
		}
	}
}
