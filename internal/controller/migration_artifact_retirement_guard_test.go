package controller

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/appimages"
	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
)

func migrationArtifactGuardFixture(t *testing.T) (*store.Store, model.App, model.Operation) {
	t.Helper()
	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("migration-retirement")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "project", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	imageRef := "registry.fugue.internal:5000/fugue-apps/app:v1"
	app, err := s.CreateApp(tenant.ID, project.ID, "app", "", model.AppSpec{
		Image: imageRef, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	target, _, err := s.CreateRuntime(tenant.ID, "target", model.RuntimeTypeExternalOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	desired := app.Spec
	desired.RuntimeID = target.ID
	op, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID, Type: model.OperationTypeMigrate, AppID: app.ID,
		TargetRuntimeID: target.ID, DesiredSpec: &desired,
	})
	if err != nil {
		t.Fatalf("create migration operation: %v", err)
	}
	return s, app, op
}

func TestManagedImageRetentionStopsBeforeMigrationCutover(t *testing.T) {
	s, app, _ := migrationArtifactGuardFixture(t)
	apps, err := s.ListAppsMetadata("", true)
	if err != nil {
		t.Fatalf("list apps: %v", err)
	}
	ops, err := s.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	var deleted []string
	svc := &Service{
		Store:            s,
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: "registry.fugue.internal:5000",
		inspectManagedImage: func(context.Context, string) (bool, map[string]int64, error) {
			return true, nil, nil
		},
		deleteManagedImage: func(_ context.Context, ref string) (appimages.DeleteResult, error) {
			deleted = append(deleted, ref)
			return appimages.DeleteResult{ImageRef: ref, Deleted: true}, nil
		},
	}
	if err := svc.pruneExcessManagedAppImagesWithSnapshot(context.Background(), app, ops, apps, ops, nil); err != nil {
		t.Fatalf("retention guard: %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("migration artifacts were deleted before cutover: %v", deleted)
	}
	blocked, reason, err := s.MigrationArtifactsRetirementBlocked(app.ID)
	if err != nil || !blocked || reason == "" {
		t.Fatalf("retirement attempt did not remain blocked: blocked=%v reason=%q err=%v", blocked, reason, err)
	}
}

func TestDistributedRetentionStopsBeforeMigrationCutover(t *testing.T) {
	s, app, _ := migrationArtifactGuardFixture(t)
	svc := &Service{
		Store:  s,
		Config: config.ControllerConfig{ImageStoreMode: "distributed"},
	}
	plan, err := svc.reconcileDistributedImageRetentionForApp(context.Background(), app, nil, nil)
	if err != nil {
		t.Fatalf("distributed retention guard: %v", err)
	}
	if plan.AppID != app.ID || len(plan.DropImageIDs) != 0 {
		t.Fatalf("unexpected retention plan while migration is pending: %+v", plan)
	}
}

func TestImageCachePruneProtectsPendingMigrationImage(t *testing.T) {
	s, app, _ := migrationArtifactGuardFixture(t)
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := s.UpsertImage(model.Image{
		TenantID: app.TenantID, AppID: "historical-image-owner",
		ImageRef: app.Spec.Image, CanonicalDigest: digest,
		LifecycleState: model.ImageLifecycleDeleted,
	}); err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	created := nowUTCForMigrationGuardTest().Add(-48 * time.Hour)
	if _, err := s.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID: "machine-1", ClusterNodeName: "worker-1", RuntimeID: "runtime-1",
		ManifestCount: 1, ObservedAt: nowUTCForMigrationGuardTest(),
	}, []model.ImageCacheManifest{{
		Repo: "fugue-apps/app", Target: "v1", Digest: digest,
		CreatedAtObserved: &created, LastSeenAt: nowUTCForMigrationGuardTest(), Present: true,
	}}); err != nil {
		t.Fatalf("upsert cache inventory: %v", err)
	}
	svc := &Service{Store: s, Config: config.ControllerConfig{ImageStoreOrphanPruneGracePeriod: 0}}
	protected, err := svc.controllerImageCacheProtectedSet(context.Background())
	if err != nil {
		t.Fatalf("build protected set: %v", err)
	}
	plan, err := svc.computeControllerImageCachePrunePlan(context.Background(), model.ImageCacheNodeInventory{
		NodeID: "machine-1", ClusterNodeName: "worker-1", RuntimeID: "runtime-1",
	}, protected, model.ImageCachePruneModeObserve)
	if err != nil {
		t.Fatalf("compute cache plan: %v", err)
	}
	if len(plan.Candidates) != 0 || len(plan.ProtectedManifests) != 1 || plan.ProtectedManifests[0].SkipReason != "migration_cutover_pending" {
		t.Fatalf("pending migration image was not protected: %+v", plan)
	}
}

func nowUTCForMigrationGuardTest() time.Time { return time.Now().UTC() }
