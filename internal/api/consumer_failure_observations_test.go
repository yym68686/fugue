package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/runtimeobservation"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestConsumerFailurePreservesTypedCauseAndIndependentRetention(t *testing.T) {
	s := &Server{}
	ctx := runtimeobservation.WithDatabaseOperation(context.Background(), "consumer-heartbeat")
	err := errors.Join(store.ErrConflict, &pgconn.PgError{Code: "55P03", Message: "private SQL and identity"})
	s.retainConsumerRequestFailure(ctx, time.Now().UTC(), "trusted_heartbeat", "store_admission", err)
	for range captureReceiptLimit * 2 {
		_, trace := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
		s.finishCaptureTrace(trace, "ready", nil, 0, 0)
		s.retainConsumerRequestFailure(ctx, time.Now(), "assignment", "resolve", nil)
	}
	v := s.captureSnapshot().(map[string]any)["consumer_failures"].(map[string]any)
	rows := v["receipts"].([]consumerRequestFailure)
	if len(rows) != 1 || !rows[0].Conflict || rows[0].SQLState != "55P03" || rows[0].DatabaseOperation != runtimeobservation.DatabaseOperationFromContext(ctx) || v["total"] != uint64(1) {
		t.Fatal("original cause/lineage lost or successful reads evicted failure", v)
	}
	for range consumerFailureLimit + 1 {
		s.retainConsumerRequestFailure(ctx, time.Now(), "private-operation", "private-stage", &pgconn.PgError{Code: "private-state", Message: "private-error"})
	}
	if rows[0].Sequence != 1 {
		t.Fatal("snapshot shared eviction backing array")
	}
	v = s.captureSnapshot().(map[string]any)["consumer_failures"].(map[string]any)
	if len(v["receipts"].([]consumerRequestFailure)) != consumerFailureLimit || v["evicted"] != uint64(2) {
		t.Fatal("unbounded or unaccounted failures", v)
	}
	raw, _ := json.Marshal(v)
	if strings.Contains(string(raw), "private") {
		t.Fatal("arbitrary failure input retained")
	}
}

func TestConsumerFailureHeartbeatHandlerRetainsOriginalFailure(t *testing.T) {
	for _, shadow := range []bool{false, true} {
		_, s, _, _, _, _ := setupAppDomainTestServer(t)
		keys := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "test", Keys: map[string]string{"test": "synthetic-consumer-credential"}}
		token, err := platformcontrol.IssuePlatformComponentIdentity(keys, platformcontrol.PlatformComponentIdentityClaims{
			CredentialID: "test", Component: model.PlatformConsumerComponentEdgeWorker, NodeID: "test-node", ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle},
		}, time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		handler := (&auth.Authenticator{PlatformComponentIdentityKeyring: keys}).RequirePlatformComponent(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.handleVerifiedPlatformConsumerHeartbeat(w, r, shadow)
		}))
		// The exact failing store read is retained; malformed input is outside
		// this provider's coverage and must preserve the existing 400 response.
		for _, body := range []string{`{`, `{"expected_consumer_set_id":"missing-set"}`} {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			want := http.StatusNotFound
			if body == "{" {
				want = http.StatusBadRequest
			}
			if w.Code != want {
				t.Fatal("original heartbeat rejection changed", w.Code, w.Body.String())
			}
		}
		v := s.captureSnapshot().(map[string]any)["consumer_failures"].(map[string]any)
		rows := v["receipts"].([]consumerRequestFailure)
		wantOperation := "trusted_heartbeat"
		if shadow {
			wantOperation = "shadow_heartbeat"
		}
		if len(rows) != 1 || rows[0].Operation != wantOperation || rows[0].Stage != "expected_set" || rows[0].DatabaseOperation.ID == 0 {
			t.Fatal("actual heartbeat failure was not observed", v)
		}
	}
}

func TestConsumerFailureConcurrentSnapshots(t *testing.T) {
	s := &Server{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 20 {
				s.retainConsumerRequestFailure(context.Background(), time.Now(), "assignment", "input_recheck", store.ErrConflict)
				_ = s.captureSnapshot()
			}
		})
	}
	wg.Wait()
	v := s.captureSnapshot().(map[string]any)["consumer_failures"].(map[string]any)
	if v["total"] != uint64(160) || v["evicted"] != uint64(160-consumerFailureLimit) {
		t.Fatal("concurrent counts lost", v)
	}
}

func TestConsumerInputReadFailureCopiesOnlyFixedFieldNames(t *testing.T) {
	s := &Server{}
	ctx := withConsumerInputReadObservation(t.Context())
	before := consumerArtifactLookup{Artifact: model.PlatformArtifact{ID: "private-artifact", Content: map[string]any{"private-map-key": "private-content"}}}
	after := before
	after.Artifact.ID = "private-other-artifact"
	after.Artifact.Content = map[string]any{"private-other-key": "private-other-content"}
	observeConsumerInputReadFailure(ctx, "assignment_changed", store.PlatformInputReadFailure{}, &before, &after)
	s.retainConsumerRequestFailure(ctx, time.Now(), "assignment", "input_recheck", store.ErrConflict)
	retained := s.captureSnapshot().(map[string]any)["consumer_failures"].(map[string]any)["receipts"].([]consumerRequestFailure)[0]
	if retained.InputRead == nil || len(retained.InputRead.ChangedFields) != 2 {
		t.Fatal("comparison fields missing", retained)
	}
	ctx.Value(consumerInputReadObservationKey{}).(*consumerInputReadFailure).ChangedFields[0] = "private-mutation"
	raw, _ := json.Marshal(retained)
	if strings.Contains(string(raw), "private") || !strings.Contains(string(raw), "artifact.content") || !strings.Contains(string(raw), "artifact.id") {
		t.Fatal("receipt retained values or caller backing memory", string(raw))
	}
}
