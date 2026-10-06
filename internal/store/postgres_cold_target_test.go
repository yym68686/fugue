package store

import (
	"errors"
	"fugue/internal/model"
	"fugue/internal/storagerecovery"
	"testing"
)

func TestColdTargetCommitLeaseCASAndStableEndpoint(t *testing.T) {
	f := newManagedPostgresPlacementFixture(t, true)
	current := *model.CloneAppPostgresSpec(f.service.Spec.Postgres)
	next := current
	next.RuntimeID = f.targetRuntime.ID
	next.ServiceName = "restored-cluster"
	next.EndpointServiceName = current.ServiceName
	next.StorageClassName = "cloneable"
	next.StorageSize = "4Gi"
	desired := f.app.Spec
	requested := current
	requested.RuntimeID = next.RuntimeID
	requested.StorageClassName = next.StorageClassName
	requested.StorageSize = next.StorageSize
	desired.Postgres = &requested
	op, err := f.store.CreateOperation(model.Operation{TenantID: f.tenant.ID, AppID: f.app.ID, ServiceID: f.service.ID, Type: storagerecovery.OperationType, TargetRuntimeID: f.targetRuntime.ID, DesiredSpec: &desired})
	if err != nil {
		t.Fatal(err)
	}
	hash := PostgresSpecFingerprint(current)
	if _, err := f.store.CommitColdPostgresTarget(op.ID, hash, next); !errors.Is(err, ErrConflict) {
		t.Fatalf("pending operation committed: %v", err)
	}
	op, claimed, err := f.store.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatalf("claim: %v", err)
	}
	if _, err := f.store.UpdateBackingServiceSpec(f.service.ID, f.service.Spec); !errors.Is(err, ErrConflict) {
		t.Fatalf("concurrent change permitted: %v", err)
	}
	wrong := next
	wrong.Password = "different"
	if _, err := f.store.CommitColdPostgresTarget(op.ID, hash, wrong); !errors.Is(err, ErrConflict) {
		t.Fatalf("credential changed: %v", err)
	}
	wrong = next
	wrong.EndpointServiceName = "another-host"
	if _, err := f.store.CommitColdPostgresTarget(op.ID, hash, wrong); !errors.Is(err, ErrConflict) {
		t.Fatalf("stable endpoint changed: %v", err)
	}
	if _, err := f.store.CommitColdPostgresTarget(op.ID, "stale", next); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale configuration committed: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := f.store.CommitColdPostgresTarget(op.ID, hash, next); err != nil {
			t.Fatalf("commit/retry %d: %v", i, err)
		}
	}
	app, err := f.store.GetApp(f.app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if app.Spec.RuntimeID != f.app.Spec.RuntimeID || app.Spec.Replicas != f.app.Spec.Replicas {
		t.Fatal("database commit overwrote application placement")
	}
	if _, err := f.store.CompleteManagedOperationWithResult(op.ID, "", "verified", &app.Spec, nil); err != nil {
		t.Fatal(err)
	}
	service, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatal(err)
	}
	if service.Spec.Postgres.ServiceName != next.ServiceName || service.Spec.Postgres.EndpointServiceName != current.ServiceName {
		t.Fatal("completion reverted target")
	}
	if _, err := f.store.CommitColdPostgresTarget(op.ID, hash, next); !errors.Is(err, ErrConflict) {
		t.Fatalf("completed lease reused: %v", err)
	}
}
