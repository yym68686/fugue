package appmail

import (
	"context"
	"encoding/json"
	"testing"

	"fugue/internal/store"
)

func TestAwaitReconciliationDoesNotReportSupersededProgress(t *testing.T) {
	binding := testBinding("target")
	for _, tc := range []struct {
		name, wantErr           string
		result                  store.AppMailReconcileResult
		generation, observedGen uint64
		wantObserve             bool
	}{
		{name: "matched", generation: 9, observedGen: 9, result: store.AppMailReconcileSynced, wantObserve: true},
		{name: "superseded", generation: 10, result: store.AppMailReconcileSynced, wantErr: "mail binding changed"},
		{name: "provider moved", generation: 9, observedGen: 10, result: store.AppMailReconcileSynced, wantObserve: true, wantErr: "mail provider identity does not match desired state"},
		{name: "provider failed", generation: 9, result: store.AppMailReconcileProviderUnavailable, wantErr: "mail provider unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := false
			status := func(_ context.Context, appID string) (store.AppMailReconcileStatus, error) {
				if appID != binding.AppID {
					t.Fatal("wrong binding status identity")
				}
				return store.AppMailReconcileStatus{Generation: tc.generation, LastResult: tc.result}, nil
			}
			get := func(context.Context) (json.RawMessage, error) {
				observed = true
				raw, err := json.Marshal(map[string]any{
					"appId": binding.AppID, "tenantId": binding.TenantID, "projectId": binding.ProjectID,
					"hostname": binding.Hostname, "state": binding.State, "generation": tc.observedGen,
				})
				return json.RawMessage(raw), err
			}
			out, err := AwaitReconciliation(t.Context(), binding, status, get)
			if observed != tc.wantObserve {
				t.Fatalf("provider GET=%t, want %t", observed, tc.wantObserve)
			}
			if tc.wantErr == "" {
				if err != nil || !json.Valid(out) {
					t.Fatalf("valid observation not returned: %q %v", out, err)
				}
			} else if err == nil || err.Error() != tc.wantErr || out != nil {
				t.Fatalf("unexpected observation or error: %q %v", out, err)
			}
		})
	}
}

func TestAwaitReconciliationReturnsPendingWhenCallerLeaves(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	out, err := AwaitReconciliation(ctx, testBinding("target"), func(context.Context, string) (store.AppMailReconcileStatus, error) {
		cancel()
		return store.AppMailReconcileStatus{Generation: 8, LastResult: store.AppMailReconcileSynced}, nil
	}, func(context.Context) (json.RawMessage, error) {
		t.Fatal("pending request must not call provider")
		return nil, nil
	})
	if out != nil || err == nil || err.Error() != "mail reconciliation pending" {
		t.Fatalf("cancellation: %q %v", out, err)
	}
}
