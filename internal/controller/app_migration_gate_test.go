package controller

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
)

func migrationImageEvidenceFixture(t *testing.T, strict bool) (*Service, model.App, model.Operation, string) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Now().UTC()
	app := model.App{
		ID: "app_migration_image", TenantID: "tenant_migration_image",
		Spec: model.AppSpec{Image: "registry.fugue.internal:5000/fugue-apps/demo:v1", Replicas: 1, RuntimeID: "runtime-old"},
	}
	op := model.Operation{
		ID: "op_migration_image", AppID: app.ID, TenantID: app.TenantID,
		Type: model.OperationTypeMigrate, SourceRuntimeID: "runtime-old", TargetRuntimeID: "runtime-new",
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID: app.TenantID, AppID: app.ID, ImageRef: app.Spec.Image,
		RuntimeID: "runtime-source", Status: model.ImageLocationStatusPresent, LastSeenAt: &now,
	}); err != nil {
		t.Fatalf("record source image location: %v", err)
	}
	svc := &Service{
		Store: stateStore, Logger: log.New(io.Discard, "", 0),
		Config: config.ControllerConfig{ImageStoreMode: map[bool]string{true: "distributed", false: "distributed-with-registry-fallback"}[strict]},
		now:    func() time.Time { return now },
	}
	return svc, app, op, app.Spec.Image
}

func TestMigrationTargetImageReplicationRequiresTargetScopedEvidence(t *testing.T) {
	t.Parallel()
	svc, app, op, imageRef := migrationImageEvidenceFixture(t, true)
	verified, reason, err := svc.verifyMigrationTargetImageReplication(context.Background(), app, op, imageRef, "", true)
	if err != nil || verified || reason == "" {
		t.Fatalf("source image evidence must not verify target replication: verified=%v reason=%q err=%v", verified, reason, err)
	}

	now := time.Now().UTC()
	if _, err := svc.Store.UpsertImageLocation(model.ImageLocation{
		TenantID: app.TenantID, AppID: app.ID, ImageRef: imageRef,
		RuntimeID: op.TargetRuntimeID, Status: model.ImageLocationStatusPresent, LastSeenAt: &now,
	}); err != nil {
		t.Fatalf("record target image location: %v", err)
	}
	verified, reason, err = svc.verifyMigrationTargetImageReplication(context.Background(), app, op, imageRef, "", true)
	if err != nil || !verified || reason == "" {
		t.Fatalf("fresh target image evidence must verify replication: verified=%v reason=%q err=%v", verified, reason, err)
	}
}

func TestMigrationTargetImageReplicationRequiresPreflightOutsideStrictStore(t *testing.T) {
	t.Parallel()
	svc, app, op, imageRef := migrationImageEvidenceFixture(t, false)
	verified, reason, err := svc.verifyMigrationTargetImageReplication(context.Background(), app, op, imageRef, "", false)
	if err != nil || verified || reason == "" {
		t.Fatalf("missing target preflight must block fallback cutover: verified=%v reason=%q err=%v", verified, reason, err)
	}
	verified, reason, err = svc.verifyMigrationTargetImageReplication(context.Background(), app, op, imageRef, "", true)
	if err != nil || !verified || reason == "" {
		t.Fatalf("successful target preflight must permit fallback cutover: verified=%v reason=%q err=%v", verified, reason, err)
	}
}

func TestMigrationTargetImageReplicationUsesAppliedSchedulingNode(t *testing.T) {
	t.Parallel()
	svc, app, op, imageRef := migrationImageEvidenceFixture(t, true)
	now := time.Now().UTC()
	if _, err := svc.Store.UpsertImageLocation(model.ImageLocation{
		TenantID: app.TenantID, AppID: app.ID, ImageRef: imageRef,
		RuntimeID: "runtime-physical-node", ClusterNodeName: "node-de",
		Status: model.ImageLocationStatusPresent, LastSeenAt: &now,
	}); err != nil {
		t.Fatalf("record physical target image location: %v", err)
	}

	verified, reason, err := svc.verifyMigrationTargetImageReplication(
		context.Background(), app, op, imageRef, "node-de", true,
	)
	if err != nil || !verified || reason == "" {
		t.Fatalf("applied target scheduling node must verify replication: verified=%v reason=%q err=%v", verified, reason, err)
	}
}

func TestMigrationReplicaCountExcludesPreviousRevision(t *testing.T) {
	t.Parallel()
	if got := minMigrationReplicaCount(0, 1, 1); got != 0 {
		t.Fatalf("ready replicas from a previous revision counted as current: %d", got)
	}
	if got := minMigrationReplicaCount(2, 2, 2); got != 2 {
		t.Fatalf("current updated/ready/available replicas = %d, want 2", got)
	}
}

func TestSourceClusterIDForMigrationRequiresAuthoritativeRuntimeIdentity(t *testing.T) {
	t.Parallel()
	svc, _, _, _ := migrationImageEvidenceFixture(t, false)
	op := model.Operation{
		ID: "op-cluster-identity", SourceRuntimeID: "runtime-without-cluster-label", TargetRuntimeID: "runtime-target",
	}
	if _, err := svc.sourceClusterIDForMigration(op, "target-cluster"); err == nil {
		t.Fatal("cross-runtime migration accepted a missing source cluster identity")
	}

	op.SourceRuntimeID = model.DefaultManagedRuntimeID
	clusterID, err := svc.sourceClusterIDForMigration(op, "managed-cluster")
	if err != nil || clusterID != "managed-cluster" {
		t.Fatalf("managed source should use the controller's live Kubernetes identity: cluster=%q err=%v", clusterID, err)
	}

	op.SourceRuntimeID = op.TargetRuntimeID
	clusterID, err = svc.sourceClusterIDForMigration(op, "target-cluster")
	if err != nil || clusterID != "target-cluster" {
		t.Fatalf("same-runtime migration should use the observed target identity: cluster=%q err=%v", clusterID, err)
	}
}
