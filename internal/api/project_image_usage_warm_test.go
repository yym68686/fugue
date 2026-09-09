package api

import (
	"context"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBackgroundSnapshotCannotReplaceNewerForegroundData(t *testing.T) {
	cache := newExpiringResponseCache[string](time.Minute)
	started := time.Now()
	cache.set("scope", "new foreground evidence")
	before, _ := cache.getEntry("scope")
	cache.setUnlessUpdatedAfter("scope", "older background evidence", started)
	after, _ := cache.getEntry("scope")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("background refresh replaced newer evidence or extended its lifetime")
	}
}

func TestSharedImageWarmPreservesTenantSnapshotsAndObservationTimes(t *testing.T) {
	s, server, _, tenant, _, app, _, _, currentRef, _ := setupAppImagesTestServer(t)
	server.imageStoreMode = "distributed"
	foreign, err := s.CreateTenant("another tenant")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(foreign.ID, "other images", "")
	if err != nil {
		t.Fatal(err)
	}
	foreignRef := "registry.push.example/fugue-apps/other:current"
	other, err := s.CreateImportedApp(foreign.ID, project.ID, "other", "", model.AppSpec{Image: foreignRef, Ports: []int{8080}, Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID}, model.AppSource{Type: model.AppSourceTypeDockerImage, ResolvedImageRef: foreignRef}, model.AppRoute{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-10 * time.Second)
	for i, entry := range []struct {
		app model.App
		ref string
	}{{app, currentRef}, {other, foreignRef}} {
		observed := now.Add(time.Duration(i) * time.Second)
		if _, err := s.UpsertImageLocation(model.ImageLocation{TenantID: entry.app.TenantID, AppID: entry.app.ID, ImageRef: entry.ref, Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", NodeID: "worker", Status: model.ImageLocationStatusPresent, LastSeenAt: &observed, SizeBytes: int64(100 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	principals := []model.Principal{
		{TenantID: tenant.ID},
		{TenantID: foreign.ID},
		{Scopes: map[string]struct{}{"platform.admin": {}}},
	}
	baseline := make(map[string]projectImageUsageResponse)
	for _, principal := range principals {
		response, err := server.loadProjectImageUsageResponse(context.Background(), principal)
		if err != nil {
			t.Fatal(err)
		}
		baseline[projectImageUsageCacheKey(principal)] = response
	}
	if err := server.refreshDistributedProjectImageUsageSnapshots(context.Background()); err != nil {
		t.Fatal(err)
	}
	for key, want := range baseline {
		got, ok := server.projectImageUsageCache.get(key)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("scope %q changed complete evidence: got=%+v want=%+v", key, got, want)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	key := projectImageUsageCacheKey(principals[0])
	before, _ := server.projectImageUsageCache.getEntry(key)
	if err := server.refreshDistributedProjectImageUsageSnapshots(ctx); err != context.Canceled {
		t.Fatalf("cancelled refresh continued: %v", err)
	}
	after, _ := server.projectImageUsageCache.getEntry(key)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("cancelled refresh replaced data or extended freshness")
	}
}
