package operationview

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

type representativeReads struct {
	Repository
	operations map[string]model.Operation
	apps       map[string]model.App
	evidence   []model.OperationEvidence
	starts     map[string]time.Time
	filter     Filter
	timingIDs  []string
}

func (r *representativeReads) GetOperation(id string) (model.Operation, error) {
	op, found := r.operations[id]
	if !found {
		return model.Operation{}, errors.New("not found")
	}
	return op, nil
}
func (r *representativeReads) GetApp(id string) (model.App, error) {
	app, found := r.apps[id]
	if !found {
		return model.App{}, errors.New("not found")
	}
	return app, nil
}
func (r *representativeReads) ListOperationEvidence(model.OperationEvidenceFilter) ([]model.OperationEvidence, error) {
	return r.evidence, nil
}
func (r *representativeReads) FirstDeployExecutionStarts(_ context.Context, ids []string) (map[string]time.Time, error) {
	r.timingIDs = ids
	return r.starts, nil
}
func (r *representativeReads) ListOperationSummariesFiltered(_ string, _ bool, f Filter) ([]model.Operation, error) {
	r.filter = f
	return nil, nil
}

func TestProjectAuthorizationIsBoundToBothOperationAndApp(t *testing.T) {
	principal := model.Principal{TenantID: "tenant", ProjectID: "project"}
	reads := &representativeReads{
		operations: map[string]model.Operation{
			"visible":   {ID: "visible", TenantID: "tenant", AppID: "visible"},
			"foreign":   {ID: "foreign", TenantID: "foreign", AppID: "visible"},
			"confused":  {ID: "confused", TenantID: "tenant", AppID: "foreign"},
			"unrelated": {ID: "unrelated", TenantID: "tenant", AppID: "unrelated"},
		},
		apps: map[string]model.App{
			"visible":   {ID: "visible", TenantID: "tenant", ProjectID: "project"},
			"foreign":   {ID: "foreign", TenantID: "foreign", ProjectID: "project"},
			"unrelated": {ID: "unrelated", TenantID: "tenant", ProjectID: "other"},
		},
	}
	if op, err := LoadAuthorized(reads, principal, " visible "); err != nil || op.ID != "visible" {
		t.Fatalf("op=%v err=%v", op, err)
	}
	for _, id := range []string{"foreign", "confused", "unrelated"} {
		if _, err := LoadAuthorized(reads, principal, id); !errors.Is(err, ErrNotVisible) {
			t.Fatalf("%s Authorization error: %v", id, err)
		}
	}
	if _, err := List(reads, principal, Filter{TenantID: "foreign"}, false); !errors.Is(err, ErrNotVisible) {
		t.Fatalf("unexpected tenant authorization result: %v", err)
	}
	if _, err := List(reads, principal, Filter{ProjectID: "other"}, false); !errors.Is(err, ErrNotVisible) {
		t.Fatalf("unexpected project authorization result: %v", err)
	}
	if _, err := List(reads, principal, Filter{}, false); err != nil || reads.filter.ProjectID != "project" {
		t.Fatalf("filter=%v, err=%v", reads.filter, err)
	}
}

func TestProjectionNeverLinksADifferentTenantOrApp(t *testing.T) {
	op := model.Operation{ID: "parent", AppID: "app", TenantID: "tenant", Type: model.OperationTypeImport, Status: model.OperationStatusCompleted}
	read := &representativeReads{
		operations: map[string]model.Operation{
			"foreign":   {TenantID: "foreign", AppID: op.AppID, Type: model.OperationTypeDeploy},
			"unrelated": {TenantID: op.TenantID, AppID: "different", Type: model.OperationTypeDeploy},
			"related":   {TenantID: op.TenantID, AppID: op.AppID, Type: model.OperationTypeDeploy},
		},
		evidence: []model.OperationEvidence{
			{OperationID: op.ID, Payload: map[string]any{"queued_deploy_operation_id": "foreign"}},
			{OperationID: op.ID, Payload: map[string]any{"queued_deploy_operation_id": "unrelated"}},
			{OperationID: op.ID, Payload: map[string]any{"queued_deploy_operation_id": "related"}},
		},
	}
	AttachQueuedDeploy(read, &op)
	if op.QueuedDeployOperationID != "related" {
		t.Fatalf("linked %q", op.QueuedDeployOperationID)
	}
}

func TestTimingProjectionNeverChangesOriginalAttempt(t *testing.T) {
	created := time.Unix(100, 0)
	first, retry, completed := created.Add(time.Second), created.Add(2*time.Second), created.Add(3*time.Second)
	original := model.Operation{ID: "deploy", Type: model.OperationTypeDeploy, ExecutionMode: model.ExecutionModeManaged,
		CreatedAt: created, StartedAt: &retry, CompletedAt: &completed}
	reads := &representativeReads{starts: map[string]time.Time{"deploy": first}}
	page := []model.Operation{original, {ID: "nondeploy", Type: model.OperationTypeDelete}}
	if err := ProjectFirstDeployStarts(t.Context(), reads, page); err != nil || !page[0].StartedAt.Equal(first) {
		t.Fatalf("projection failed: %v, %v", page[0].StartedAt, err)
	}
	if !original.StartedAt.Equal(retry) || !reflect.DeepEqual(reads.timingIDs, []string{"deploy"}) {
		t.Fatal("projection changed the attempt snapshot or requested unrelated evidence")
	}
}
