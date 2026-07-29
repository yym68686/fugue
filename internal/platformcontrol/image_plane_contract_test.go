package platformcontrol

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestImageReplicationPlanTargetsNodeScopedImageCacheConsumers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 1, 0, 0, 0, time.UTC)
	set, err := BuildExpectedConsumerSet(ExpectedConsumerSetBuildRequest{
		ReleaseSetID:      "release-image-plan",
		ArtifactReleaseID: "artifact-image-plan",
		ArtifactKind:      model.PlatformArtifactKindImageReplicationPlan,
		Scope:             model.PlatformArtifactScope{ScopeType: "node", Key: "node:worker-a", NodeID: "worker-a"},
		ScopeKey:          "node:worker-a",
		Generation:        "image-plan-7",
		PreparedAt:        now,
		Topology: ExpectedConsumerTopology{NodeUpdaters: []model.NodeUpdater{
			{ID: "updater-a", ClusterNodeName: "worker-a", Status: model.NodeUpdaterStatusActive},
			{ID: "updater-b", ClusterNodeName: "worker-b", Status: model.NodeUpdaterStatusActive},
		}},
	})
	if err != nil {
		t.Fatalf("build image replication expected consumers: %v", err)
	}
	if len(set.Consumers) != 1 || set.Consumers[0].Component != model.PlatformConsumerComponentImageCache || set.Consumers[0].NodeID != "worker-a" {
		t.Fatalf("node-scoped image plan targeted the wrong consumers: %+v", set.Consumers)
	}
	if set.Consumers[0].ConsumerID != "image-cache:worker-a" || set.Consumers[0].ArtifactKind != model.PlatformArtifactKindImageReplicationPlan {
		t.Fatalf("image-cache consumer identity drifted: %+v", set.Consumers[0])
	}
}

func TestImageCachePlatformIdentityIsNodeAndArtifactBound(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 1, 0, 0, 0, time.UTC)
	keyring := DerivePlatformComponentIdentityKeyring("image-plane-signing-key", "image-plane-v1", "", "", nil)
	claims := PlatformComponentIdentityClaims{
		CredentialID:  "image-cache-credential-worker-a",
		Component:     model.PlatformConsumerComponentImageCache,
		NodeID:        "worker-a",
		ScopeKey:      "node:worker-a",
		ArtifactKinds: []string{model.PlatformArtifactKindImageReplicationPlan},
	}
	token, err := IssuePlatformComponentIdentity(keyring, claims, now, 5*time.Minute)
	if err != nil {
		t.Fatalf("issue image-cache identity: %v", err)
	}
	parsed, err := ParsePlatformComponentIdentity(keyring, token, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("parse image-cache identity: %v", err)
	}
	bound, err := BindPlatformConsumerHeartbeat(parsed, PlatformConsumerHeartbeatEnvelope{
		ConsumerID:   "image-cache:worker-a",
		Component:    model.PlatformConsumerComponentImageCache,
		NodeID:       "worker-a",
		ArtifactKind: model.PlatformArtifactKindImageReplicationPlan,
		ScopeKey:     "node:worker-a",
	})
	if err != nil || bound.ConsumerID != "image-cache:worker-a" {
		t.Fatalf("valid image-cache heartbeat was not bound: heartbeat=%+v err=%v", bound, err)
	}
	for _, bad := range []PlatformConsumerHeartbeatEnvelope{
		{ConsumerID: "image-cache:worker-b", NodeID: "worker-b", ArtifactKind: model.PlatformArtifactKindImageReplicationPlan, ScopeKey: "node:worker-b"},
		{ConsumerID: "image-cache:worker-a", NodeID: "worker-a", ArtifactKind: model.PlatformArtifactKindNodeDesiredState, ScopeKey: "node:worker-a"},
	} {
		if _, err := BindPlatformConsumerHeartbeat(parsed, bad); !errors.Is(err, ErrPlatformConsumerHeartbeatImpersonation) {
			t.Fatalf("identity accepted image-cache drift %+v: %v", bad, err)
		}
	}
}

func TestImageCachePlatformIdentityRejectsBroadOrCrossNodeClaims(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 1, 0, 0, 0, time.UTC)
	keyring := DerivePlatformComponentIdentityKeyring("image-plane-signing-key", "image-plane-v1", "", "", nil)
	base := PlatformComponentIdentityClaims{
		CredentialID:  "image-cache-credential-worker-a",
		Component:     model.PlatformConsumerComponentImageCache,
		NodeID:        "worker-a",
		ScopeKey:      "node:worker-a",
		ArtifactKinds: []string{model.PlatformArtifactKindImageReplicationPlan},
	}
	for name, mutate := range map[string]func(*PlatformComponentIdentityClaims){
		"global scope": func(claims *PlatformComponentIdentityClaims) {
			claims.ScopeKey = "global"
		},
		"another node scope": func(claims *PlatformComponentIdentityClaims) {
			claims.ScopeKey = "node:worker-b"
		},
		"extra artifact capability": func(claims *PlatformComponentIdentityClaims) {
			claims.ArtifactKinds = append(claims.ArtifactKinds, model.PlatformArtifactKindNodeDesiredState)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			claims := base
			claims.ArtifactKinds = append([]string(nil), base.ArtifactKinds...)
			mutate(&claims)
			if _, err := IssuePlatformComponentIdentity(keyring, claims, now, 5*time.Minute); !errors.Is(err, ErrPlatformComponentIdentityInvalid) {
				t.Fatalf("unsafe image-cache claims were issued: %+v err=%v", claims, err)
			}
		})
	}
}
