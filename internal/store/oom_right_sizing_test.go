package store

import (
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestOOMRightSizingWritePortRejectsMalformedOrUnscopedRequest(t *testing.T) {
	// Rejection occurs before beginning any database operation.
	s := &Store{}
	app := model.App{ID: "app", TenantID: "tenant"}
	exact := oomRightSizingPrefix + "0123456789abcdef01234567"
	for _, id := range []string{
		model.OperationRequestedByOOMRightSizing, oomRightSizingPrefix + "0123",
		oomRightSizingPrefix + "0123456789ABCDEF01234567",
		oomRightSizingPrefix + "0123456789abcdef0123456z",
		" " + exact, exact + " ",
	} {
		if _, err := s.EnqueueOOMRightSizingDeploy(app, id, app.Spec, nil); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("malformed event ID %q returned %v", id, err)
		}
	}
	for _, id := range []string{model.OperationRequestedByOOMRightSizing, exact} {
		if _, err := s.CreateOperation(model.Operation{
			AppID: app.ID, TenantID: app.TenantID, Type: model.OperationTypeDeploy,
			RequestedByID: id, RequestedByType: model.ActorTypeSystem, DesiredSpec: &app.Spec,
		}); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("generic write with OOM actor %q returned %v", id, err)
		}
	}
}

func TestPGOOMRightSizingWritePortKeepsTenantAndEventFence(t *testing.T) {
	t.Parallel()
	const fixture = "oom-admission"
	s := newStoreTestDatabase(t, fixture)
	second := newStoreTestDatabase(t, fixture)
	tenant, err := s.CreateTenant("owning tenant")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CreateTenant("unrelated tenant")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "test-app", "", model.AppSpec{
		RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Image: "registry.example/test:one",
	})
	if err != nil {
		t.Fatal(err)
	}
	id := oomRightSizingPrefix + "0123456789abcdef01234567"
	wrongTenant := app
	wrongTenant.TenantID = other.ID
	if _, err := s.EnqueueOOMRightSizingDeploy(wrongTenant, id, app.Spec.DeepCopy(), nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant write returned %v", err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, writer := range []*Store{s, second} {
		go func() {
			<-start
			_, err := writer.EnqueueOOMRightSizingDeploy(app, id, app.Spec.DeepCopy(), nil)
			results <- err
		}()
	}
	close(start)
	created, conflicts := 0, 0
	for range 2 {
		switch err := <-results; {
		case err == nil:
			created++
		case errors.Is(err, ErrConflict):
			conflicts++
		default:
			t.Errorf("unexpected enqueue result: %v", err)
		}
	}
	if created != 1 || conflicts != 1 {
		t.Fatalf("created=%d conflicts=%d, want one of each", created, conflicts)
	}
	ops, err := s.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil || len(ops) != 1 {
		t.Fatalf("expected exactly one persisted operation, got %d: %v", len(ops), err)
	}
	if op := ops[0]; op.Type != model.OperationTypeDeploy || op.RequestedByType != model.ActorTypeSystem || op.RequestedByID != id {
		t.Fatalf("specialized write stored wrong type or actor: %s %s %s", op.Type, op.RequestedByType, op.RequestedByID)
	}
}
