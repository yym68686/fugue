package operationcancel

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestTransition(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		name, status, kind, message, want string
	}{
		{"pending migration", model.OperationStatusPending, model.OperationTypeMigrate, "", DefaultMessage},
		{"running deploy", model.OperationStatusRunning, model.OperationTypeDeploy, "  superseded  ", "superseded"},
		{"running migration", model.OperationStatusRunning, model.OperationTypeMigrate, "", ""},
		{"waiting agent", model.OperationStatusWaitingAgent, model.OperationTypeDeploy, "", ""},
		{"terminal", model.OperationStatusCompleted, model.OperationTypeDeploy, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := model.Operation{ID: "op_fixture", TenantID: "tenant_fixture", Type: tc.kind, Status: tc.status}
			got, ok := Transition(op, tc.message, now)
			if tc.want == "" {
				if ok || !reflect.DeepEqual(got, op) {
					t.Fatalf("rejected transition mutated operation: %v %+v", ok, got)
				}
				return
			}
			if !ok || got.Status != model.OperationStatusCanceled || got.CompletedAt == nil ||
				!got.CompletedAt.Equal(now) || !got.UpdatedAt.Equal(now) || got.ResultMessage != tc.want ||
				got.ID != op.ID || got.TenantID != op.TenantID || got.Type != op.Type {
				t.Fatalf("invalid transition: %v %+v", ok, got)
			}
		})
	}
}
