package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/store"
)

type pendingResourceSQLFailure string

func (e pendingResourceSQLFailure) Error() string {
	return "private-neutral postgres://role:secret@host SELECT private_column /private/path"
}
func (e pendingResourceSQLFailure) SQLState() string { return string(e) }

func TestPendingResourceObservationStatusRedactsSourceFailuresAndDoesNotRetry(t *testing.T) {
	for code, category := range map[string]string{"42501": "privilege", "53100": "disk-full", "53400": "configuration-limit", "57014": "query-canceled", "55P03": "lock-unavailable", "XX999": "other"} {
		t.Run(code, func(t *testing.T) {
			var calls atomic.Int32
			_, err := observePendingResources(t.Context(), &kubeLogsClient{}, "control-neutral", "cluster-neutral", "storage-neutral", func(context.Context) (store.PendingRootDatabaseResources, error) {
				calls.Add(1)
				return store.PendingRootDatabaseResources{}, pendingResourceSQLFailure(code)
			})
			if !errors.Is(err, errPendingResourceObservation) || err.Error() != errPendingResourceObservation.Error() {
				t.Fatal("public error changed or leaked", err)
			}
			s := &Server{} // No store or Kubernetes client: status must use memory only.
			s.pendingResourceStatus.record(time.Now().UTC(), pendingResourcePhase("initial", err))
			value, statusErr := s.PendingResourceObservationStatus(t.Context())
			if statusErr != nil || calls.Load() != 1 {
				t.Fatal("diagnostic retried source", calls.Load(), statusErr)
			}
			last := value.(map[string]any)["last_completed_attempt"].(*pendingResourceLastResult)
			if last.Succeeded || last.Phase != "initial" || last.Stage != "database-before" || last.Reason != "query" || last.SQLCategory != category || last.CompletedAt.Before(last.StartedAt) {
				t.Fatal("missing refusal scope", last)
			}
			raw, marshalErr := json.Marshal(value)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			for _, secret := range []string{"private-neutral", "postgres://", "secret", "SELECT", "/private/path", code} {
				if strings.Contains(string(raw), secret) {
					t.Fatal("source detail leaked", secret)
				}
			}
			last.Reason = "caller mutation"
			again, _ := s.PendingResourceObservationStatus(t.Context())
			if again.(map[string]any)["last_completed_attempt"].(*pendingResourceLastResult).Reason != "query" {
				t.Fatal("status aliases mutable state")
			}
			s.pendingResourceStatus.record(time.Now().UTC(), nil)
			success, _ := s.PendingResourceObservationStatus(t.Context())
			recent := success.(map[string]any)["last_completed_attempt"].(*pendingResourceLastResult)
			if !recent.Succeeded || recent.Reason != "" || recent.SQLCategory != "" || success.(map[string]any)["admission_allowed"] != false || success.(map[string]any)["whole_operation_budget_proven"] != false {
				t.Fatal("stale failure or admission claim", success)
			}
		})
	}
}

func TestPendingResourceObservationStatusClassifiesHTTPWithoutBodyDisclosure(t *testing.T) {
	for _, tc := range []struct {
		status       int
		body, reason string
	}{
		{401, "private-neutral", "http-denied"}, {403, "private-neutral", "http-denied"},
		{404, "private-neutral", "http-not-found"}, {500, "private-neutral", "http-unavailable"},
		{200, "private-neutral", "response-format"}, {200, strings.Repeat("x", (2<<20)+1), "response-limit"},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		}))
		err := pendingResourceGet(t.Context(), &kubeLogsClient{client: server.Client(), baseURL: server.URL}, "volume-stats", "/private-neutral", &map[string]any{})
		server.Close()
		var refusal *pendingResourceRefusal
		if !errors.As(err, &refusal) || refusal.reason != tc.reason || refusal.stage != "volume-stats" || err.Error() != errPendingResourceObservation.Error() {
			t.Fatal("HTTP refusal classification lost", tc.status, err)
		}
	}
}

func TestPendingResourceObservationStatusTracksReturnedAttemptsConcurrently(t *testing.T) {
	s := &Server{}
	initial, _ := s.PendingResourceObservationStatus(t.Context())
	if initial.(map[string]any)["last_completed_attempt"] != (*pendingResourceLastResult)(nil) {
		t.Fatal("invented prior attempt")
	}
	if _, err := s.PendingResourceObservation(nil); err == nil {
		t.Fatal("invalid context accepted")
	}
	s.pendingResourceObservationMu.Lock()
	defer s.pendingResourceObservationMu.Unlock()
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if _, err := s.PendingResourceObservation(t.Context()); err == nil {
				t.Error("busy provider accepted")
			}
			_, _ = s.PendingResourceObservationStatus(t.Context())
		})
	}
	wg.Wait()
	value, _ := s.PendingResourceObservationStatus(t.Context())
	last := value.(map[string]any)["last_completed_attempt"].(*pendingResourceLastResult)
	if last.Succeeded || last.Stage != "request" || last.Reason != "busy" {
		t.Fatal("busy attempt not recorded", last)
	}
	canceled := pendingResourceRefuse("cluster", "transport", context.Canceled)
	if !errors.Is(canceled, context.Canceled) {
		t.Fatal("cancellation lost")
	}
}
