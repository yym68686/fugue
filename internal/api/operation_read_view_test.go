package api

import (
	"errors"
	"net/http"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestOperationReadReportsFirstExecutionAndCurrentSafeRolloutWait(t *testing.T) {
	for _, tc := range []struct{ wait, category string }{
		{model.OperationResultTrafficConfirmationPending, "safe-rollout-traffic-confirmation"},
		{model.OperationResultCanonicalHandoffPending, "safe-rollout-canonical-handoff"},
	} {
		t.Run(tc.category, func(t *testing.T) {
			st, server, key, _, _, app, _, _, _, _ := setupAppImagesTestServer(t)
			op, err := st.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID,
				Type: model.OperationTypeDeploy, ExecutionMode: model.ExecutionModeManaged, DesiredSpec: &app.Spec})
			if err != nil {
				t.Fatal(err)
			}
			first, claimed, err := st.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed || first.StartedAt == nil {
				t.Fatal("first claim", claimed, err)
			}
			attempt, err := st.CreateReleaseAttempt(model.ReleaseAttempt{TenantID: app.TenantID, AppID: app.ID,
				TriggerType: model.ReleaseAttemptTriggerImageTrackingAuto, RootOperationID: op.ID})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.RecordReleaseStep(model.ReleaseStep{TenantID: app.TenantID, ReleaseAttemptID: attempt.ID,
				OperationID: op.ID, Type: model.ReleaseStepTypeDeployApply,
				Status: model.ReleaseStepStatusRunning, StartedAt: *first.StartedAt}); err != nil {
				t.Fatal(err)
			}
			if _, err := st.RequeueManagedOperationGuarded(first, tc.wait); err != nil {
				t.Fatal(err)
			}
			latest, claimed, err := st.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed || latest.StartedAt == nil || !latest.StartedAt.After(*first.StartedAt) {
				t.Fatal("second claim did not advance the fence", latest, err)
			}
			if _, err := st.RequeueManagedOperationGuarded(first, tc.wait); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("stale claim could change operation: %v", err)
			}
			if _, err := st.RequeueManagedOperationGuarded(latest, tc.wait); err != nil {
				t.Fatal(err)
			}
			path := "/v1/operations/" + op.ID
			rec := performJSONRequest(t, server, http.MethodGet, path, key, nil)
			var detail struct{ Operation model.Operation }
			mustDecodeJSON(t, rec, &detail)
			if detail.Operation.StartedAt == nil || !detail.Operation.StartedAt.Equal(*first.StartedAt) {
				t.Fatal("detail lost first execution time", detail.Operation.StartedAt)
			}
			rec = performJSONRequest(t, server, http.MethodGet, "/v1/operations?status=pending&app_id="+app.ID, key, nil)
			var list struct{ Operations []model.Operation }
			mustDecodeJSON(t, rec, &list)
			if len(list.Operations) != 1 || list.Operations[0].StartedAt == nil || !list.Operations[0].StartedAt.Equal(*first.StartedAt) {
				t.Fatal("summary lost first execution time", list)
			}
			rec = performJSONRequest(t, server, http.MethodGet, path+"/diagnosis", key, nil)
			var diagnosis struct{ Diagnosis model.OperationDiagnosis }
			mustDecodeJSON(t, rec, &diagnosis)
			if diagnosis.Diagnosis.Category != tc.category || diagnosis.Diagnosis.ControllerLane != nil || diagnosis.Diagnosis.Summary != tc.wait {
				t.Fatal("in-progress rollout was misdiagnosed", diagnosis.Diagnosis)
			}
		})
	}
}
