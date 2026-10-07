package controller

import (
	"context"
	"fugue/internal/model"
	"fugue/internal/store"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestVerifiedReplicaRestoresLostImageWithoutResurrectingRetirement(t *testing.T) {
	st := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	im, err := st.UpsertImage(model.Image{TenantID: "tenant_fixture", AppID: "app_fixture", ImageRef: "registry.example/demo:v1", CanonicalDigest: "sha256:" + strings.Repeat("a", 64), LifecycleState: model.ImageLifecycleLost, RequiredReplicaCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	lease := now.Add(time.Hour)
	_, err = st.UpsertImageReplica(model.ImageReplica{ImageID: im.ID, TenantID: im.TenantID, Digest: im.CanonicalDigest, RuntimeID: "runtime_fixture", ClusterNodeName: "worker", CacheEndpoint: "http://worker:5000", Status: model.ImageReplicaStatusPresent, LastVerifiedAt: &now, LeaseExpiresAt: &lease})
	if err != nil {
		t.Fatal(err)
	}
	svc := &Service{Store: st}
	if err := svc.ensureImageReplicaPolicyWithEligibility(context.Background(), im, imageReplicationEligibility{}); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetImage(im.ID, "", true)
	if err != nil || got.LifecycleState != model.ImageLifecycleAvailable {
		t.Fatal("fresh verification did not restore available image", got.LifecycleState, err)
	}
	for _, state := range []string{model.ImageLifecycleDeleting, model.ImageLifecycleDeleted} {
		got.LifecycleState = state
		if _, err = st.UpsertImage(got); err != nil {
			t.Fatal(err)
		}
		svc.restoreLostDistributedImageFromLocations(im, []model.ImageLocation{{TenantID: im.TenantID, Digest: im.CanonicalDigest, Status: model.ImageLocationStatusPresent, LastSeenAt: &now}})
		current, err := st.GetImage(im.ID, "", true)
		if err != nil || current.LifecycleState != state {
			t.Fatal("stale lost snapshot resurrected retirement", current.LifecycleState, err)
		}
	}
}
