package controller

import (
	"context"
	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHistoricalImageAttributionRequiresCompletedImmutableReference(t *testing.T) {
	for _, tc := range []struct {
		name, status, ref string
		receipt           bool
		want              int
	}{
		{"completed digest", model.OperationStatusCompleted, "registry.example/fugue-apps/sample@sha256:" + strings.Repeat("a", 64), false, 1},
		{"mutable tag", model.OperationStatusCompleted, "registry.example/fugue-apps/sample:old", false, 0},
		{"failed operation", model.OperationStatusFailed, "registry.example/fugue-apps/sample@sha256:" + strings.Repeat("a", 64), false, 0},
		{"failed deployment with receipt", model.OperationStatusFailed, "registry.example/fugue-apps/sample:old", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := s.CreateTenant("history")
			if err != nil {
				t.Fatal(err)
			}
			project, err := s.CreateProject(tenant.ID, "sample", "")
			if err != nil {
				t.Fatal(err)
			}
			app, err := s.CreateApp(tenant.ID, project.ID, "sample", "", model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Image: "registry.example/fugue-apps/sample:current", Replicas: 1})
			if err != nil {
				t.Fatal(err)
			}
			op, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ResolvedImageRef: tc.ref}, DesiredSpec: &model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Image: tc.ref, Replicas: 1}})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.ClaimNextPendingOperation(); err != nil {
				t.Fatal(err)
			}
			if tc.status == model.OperationStatusCompleted {
				_, err = s.CompleteManagedOperation(op.ID, "manifest", "complete")
			} else {
				_, err = s.FailOperation(op.ID, "failed")
			}
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			old := now.Add(-72 * time.Hour)
			_, err = s.UpsertImageCacheInventory(model.ImageCacheNodeInventory{NodeID: "node", ClusterNodeName: "worker", ObservedAt: now}, []model.ImageCacheManifest{{Repo: "fugue-apps/sample", Target: "old", Digest: "sha256:" + strings.Repeat("a", 64), Present: true, TotalBlobBytes: 100, ReferencedBlobs: []string{"sha256:" + strings.Repeat("b", 64)}, LastSeenAt: now, CreatedAtObserved: &old}})
			if err != nil {
				t.Fatal(err)
			}
			if tc.receipt {
				_, err = s.SaveBuildArtifact(model.BuildArtifact{TenantID: tenant.ID, AppID: app.ID, OperationID: op.ID, JobName: "build", ImageRef: tc.ref, Digest: "sha256:" + strings.Repeat("a", 64), CacheEndpoint: "http://worker:5000", ClusterNodeName: "worker", VerifiedAt: &now})
				if err != nil {
					t.Fatal(err)
				}
			}
			svc := &Service{Store: s, registryPushBase: "registry.example", Config: config.ControllerConfig{ImageStoreMode: "distributed"}}
			if err := svc.reconcileHistoricalImageProvenance(context.Background()); err != nil {
				t.Fatal(err)
			}
			images, err := s.ListImages(model.ImageFilter{PlatformAdmin: true})
			if err != nil || len(images) != tc.want {
				t.Fatalf("historical attribution: %+v %v", images, err)
			}
			if tc.want == 1 && (images[0].SourceOperationID != op.ID || images[0].LifecycleState != model.ImageLifecycleLost) {
				t.Fatalf("recovery invented serving/deletion authority: %+v", images)
			}
		})
	}
}
