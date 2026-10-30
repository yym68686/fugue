package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestImageTrackingRetryWaitsForWithdrawnResourcesIncludingNewDigest(t *testing.T) {
	for _, scenario := range []string{"failed", "new_digest", "retired_cleanup_failed", "cleaned", "unbound_missing", "reader_error"} {
		t.Run(scenario, func(t *testing.T) {
			s, app, old, f := newDrainWorkloadFixtureWithOutcome(t, true)
			last := time.Now().Add(-2 * time.Hour)
			tracking, err := s.Store.UpsertAppImageTracking(model.AppImageTracking{TenantID: app.TenantID, AppID: app.ID,
				ImageRef: "ghcr.io/example/service:main", Enabled: true, LastQueuedDigest: "sha256:old", LastTriggeredAt: &last})
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "retired_cleanup_failed" {
				old.Role, old.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
				if _, err := s.Store.UpdateAppRelease(old); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "unbound_missing" {
				if _, err := s.Store.CreateAppRelease(model.AppRelease{ID: "unbound-failure", AppID: app.ID, TenantID: app.TenantID,
					Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusFailed, DeploymentName: "absent", ServiceName: "absent"}); err != nil {
					t.Fatal(err)
				}
			}
			reads := 0
			f.extra = func(w http.ResponseWriter, r *http.Request) bool {
				if strings.HasSuffix(r.URL.Path, "/deployments") || strings.HasSuffix(r.URL.Path, "/services") {
					reads++
					if scenario == "reader_error" {
						http.Error(w, "unavailable", 503)
						return true
					}
					var items []map[string]any
					if scenario != "cleaned" && scenario != "unbound_missing" {
						if strings.HasSuffix(r.URL.Path, "/deployments") {
							items = append(items, f.deployment)
						} else {
							items = append(items, f.service)
						}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
					return true
				}
				return false
			}
			digest := "sha256:old"
			if scenario == "new_digest" {
				digest = "sha256:new"
			}
			s.now = func() time.Time { return time.Now() }
			s.resolveRemoteImageDigest = func(context.Context, string) (string, error) { return digest, nil }
			if err := s.syncTrackedAppImages(context.Background()); err != nil {
				t.Fatal(err)
			}
			ops, err := s.Store.ListOperationSummariesFiltered(app.TenantID, true, store.OperationListFilter{AppID: app.ID, Types: []string{model.OperationTypeImport}})
			if err != nil {
				t.Fatal(err)
			}
			clean := scenario == "cleaned" || scenario == "unbound_missing"
			if (len(ops) == 1) != clean || reads > 2 {
				t.Fatalf("tracking bypassed cleanup or amplified history reads: ops=%d metadata_reads=%d", len(ops), reads)
			}
			updated, err := s.Store.GetAppImageTracking(app.TenantID, true, app.ID)
			if err != nil || (!clean && updated.LastOperationID != tracking.LastOperationID) {
				t.Fatal("suppressed retry changed the queued operation", err)
			}
		})
	}
}

func TestImageTrackingConsecutiveFailureBackoffIsBoundedAndNewDigestResetsIt(t *testing.T) {
	st, tenant, _, app := newImageTrackingSyncTestStore(t)
	for i := 0; i < 8; i++ {
		op, err := st.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy,
			RequestedByID: model.OperationRequestedByImageTracking, DesiredSpec: &app.Spec})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok, err := st.TryClaimPendingOperation(op.ID); err != nil || !ok {
			t.Fatal("claim", err)
		}
		if _, err := st.FailOperation(op.ID, "temporary deployment failure"); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	tracking := model.AppImageTracking{LastQueuedDigest: "sha256:old", LastTriggeredAt: &now}
	s := &Service{Store: st, now: func() time.Time { return now.Add(10 * time.Minute) }}
	if reason, err := s.imageTrackingRetrySuppression(context.Background(), app, tracking, "sha256:old"); err != nil || !strings.Contains(reason, "1h0m0s") {
		t.Fatal("consecutive failures did not back off", reason, err)
	}
	if reason, err := s.imageTrackingRetrySuppression(context.Background(), app, tracking, "sha256:new"); err != nil || reason != "" {
		t.Fatal("new digest retained old failure backoff", reason, err)
	}
	s.now = func() time.Time { return now.Add(61 * time.Minute) }
	if reason, err := s.imageTrackingRetrySuppression(context.Background(), app, tracking, "sha256:old"); err != nil || reason != "" {
		t.Fatal("backoff never expired", reason, err)
	}
}
