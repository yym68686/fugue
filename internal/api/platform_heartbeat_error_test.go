package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/httpx"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestTrustedHeartbeatLockContentionIsRetryableWithoutChangingValidation(t *testing.T) {
	busy := &pgconn.PgError{Code: "55P03", Message: "private database detail"}
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{"lock timeout", busy, http.StatusServiceUnavailable},
		{"wrapped writer barrier", fmt.Errorf("admission: %w", busy), http.StatusServiceUnavailable},
		{"changed proof", errors.Join(busy, store.ErrConflict), http.StatusConflict},
		{"invalid identity", errors.Join(busy, platformcontrol.ErrPlatformConsumerHeartbeatImpersonation), http.StatusForbidden},
		{"invalid evidence", errors.Join(busy, platformcontrol.ErrPlatformConsumerHeartbeatEvidence), http.StatusUnprocessableEntity},
		{"unknown SQL failure", &pgconn.PgError{Code: "XX000", Message: busy.Message}, http.StatusInternalServerError},
		{"text resembling busy", errors.New("55P03: private database detail"), http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			writeTrustedPlatformConsumerHeartbeatError(response, test.err)
			if response.Code != test.status || strings.Contains(response.Body.String(), "private") {
				t.Fatal("heartbeat error leaked details or changed classification", response.Code, response.Body.String())
			}
			var body httpx.ErrorResponse
			mustDecodeJSON(t, response, &body)
			if test.status == http.StatusServiceUnavailable {
				if !body.Retryable || body.Code != "temporarily_unavailable" || response.Header().Get("Retry-After") != "1" {
					t.Fatal("contention lost retry advice", body, response.Header())
				}
			} else if response.Header().Get("Retry-After") != "" {
				t.Fatal("validation or unknown error received lock retry advice")
			}
		})
	}
}
