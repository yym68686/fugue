package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func TestPlatformHostedOnlyConsumerNeedsSignedHeldIdentity(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Second)
	s := New(filepath.Join(t.TempDir(), "store.json"))
	keys := bundleauth.NewKeyring("test-key-for-hosted-only-registration", "test", "", "", nil)
	s.ConfigurePlatformArtifactSigning(keys)
	hold := platformproducer.OfflineEdge{EdgeID: "edge-test", EdgeGroupID: "edge-group-test", PublicIPv4: "8.8.8.8", Reason: "quota_exhausted", KeepHostedRouteTLS: true,
		Recovery: &platformproducer.OfflineEdgeRecoveryPolicy{Hostname: "app.example.test", Path: "/", ProbeIntervalSeconds: 300, ProbeTimeoutSeconds: 5, SuccessThreshold: 3, FailureThreshold: 1, FreshnessSeconds: 1800}}
	sign := func(t *testing.T, a model.PlatformArtifact) model.PlatformArtifact {
		t.Helper()
		a.Scope = model.PlatformArtifactScope{ScopeType: "global"}
		a.Status = model.PlatformArtifactStatusValidated
		a.CreatedAt = at
		a, err := normalizePlatformArtifactForStore(a)
		if err != nil {
			t.Fatal(err)
		}
		a.GenerationSequence = 1
		a, err = platformsafety.SignPlatformArtifact(a, keys)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	for _, scenario := range []string{"signed", "missing_grant", "foreign_group", "foreign_enrollment", "foreign_creator", "tampered_policy", "recovered"} {
		t.Run(scenario, func(t *testing.T) {
			h := hold
			if scenario == "missing_grant" {
				h.KeepHostedRouteTLS = false
			}
			policy := sign(t, model.PlatformArtifact{ID: "policy", ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Content: map[string]any{"offline_edges": []platformproducer.OfflineEdge{h}}})
			metadata := map[string]string{platformproducer.DNSPolicyIDMetadata: policy.ID, platformproducer.DNSPolicyDigestMetadata: policy.ContentHash}
			if scenario == "recovered" {
				o := platformproducer.OfflineRecoveryObservation{EdgeID: h.EdgeID, DeclarationDigest: platformproducer.OfflineRecoveryDigest(h), LastAttempt: at, LastSuccess: at, ProofValidUntil: at.Add(time.Minute), Successes: 3, Admitted: true}
				raw, err := json.Marshal(map[string]any{"captured_at": at, "observations": []platformproducer.OfflineRecoveryObservation{o}})
				if err != nil {
					t.Fatal(err)
				}
				metadata[platformproducer.OfflineRecoveryMetadata] = string(raw)
			}
			actor := platformproducer.Actor
			if scenario == "foreign_creator" {
				actor = "foreign"
			}
			parent := sign(t, model.PlatformArtifact{ID: "parent", ArtifactKind: model.PlatformArtifactKindReleaseSet, CreatedByType: model.ActorTypeBootstrap, CreatedByID: actor, Metadata: metadata, Content: map[string]any{}})
			if scenario == "tampered_policy" {
				policy.Content["offline_edges"] = nil
			}
			top := platformcontrol.ExpectedConsumerTopology{HostedRouteTLSOnly: []model.EdgeNode{{ID: h.EdgeID, EdgeGroupID: h.EdgeGroupID, PublicIPv4: h.PublicIPv4}}}
			switch scenario {
			case "foreign_group":
				top.HostedRouteTLSOnly[0].EdgeGroupID = "edge-group-other"
			case "foreign_enrollment":
				top.WorkerEnrollments = map[string]string{h.EdgeID: "0123456789abcdef0123456789abcdef"}
			}
			set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: parent.ID, ArtifactReleaseID: "release", ArtifactKind: model.PlatformArtifactKindCaddyRouteConfig, ScopeKey: "global", Generation: "gen", Revision: 1, PreparedAt: at, Topology: top})
			if err != nil {
				t.Fatal(err)
			}
			err = s.validateHostedRouteTLSExpectations(stateDNSRouteSourceReader(&model.State{PlatformArtifacts: []model.PlatformArtifact{parent, policy}}), set)
			if (err == nil) != (scenario == "signed") {
				t.Fatalf("unexpected registration result: %v", err)
			}
		})
	}
}

func TestPlatformExpectedConsumerSetPersistsAndCannotMutateRevision(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	set := expectedConsumerSetFixture(t, 1)
	created, err := s.CreatePlatformExpectedConsumerSet(set)
	if err != nil {
		t.Fatalf("create expected consumer set: %v", err)
	}
	if created.ScopeKey != "global" || created.Scope.Key != "global" || len(created.Consumers) != 1 {
		t.Fatalf("unexpected normalized expected consumer set: %+v", created)
	}

	created.Consumers[0].AcceptedProtocolVersions[0] = "mutated"
	got, err := s.GetPlatformExpectedConsumerSet(set.ID)
	if err != nil {
		t.Fatalf("get expected consumer set: %v", err)
	}
	if got.Consumers[0].AcceptedProtocolVersions[0] != model.PlatformConsumerProtocolVersionV1 {
		t.Fatalf("stored expected consumer set was mutated through returned data: %+v", got.Consumers[0])
	}

	reopened := New(path)
	if err := reopened.Init(); err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	listed, err := reopened.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{
		ReleaseSetID: set.ReleaseSetID,
		ArtifactKind: set.ArtifactKind,
		ScopeKey:     "GLOBAL",
	})
	if err != nil {
		t.Fatalf("list expected consumer sets: %v", err)
	}
	if len(listed) != 1 || !reflect.DeepEqual(listed[0], got) {
		t.Fatalf("unexpected persisted expected consumer sets: %+v", listed)
	}

	if _, err := reopened.CreatePlatformExpectedConsumerSet(set); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected duplicate expected consumer set id to conflict, got %v", err)
	}
	sameRevision := set
	sameRevision.ID = "expectedconsumerset_conflicting_revision"
	if _, err := reopened.CreatePlatformExpectedConsumerSet(sameRevision); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected release revision mutation to conflict, got %v", err)
	}
}

func TestPlatformExpectedConsumerSetRejectsInvalidCardinality(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	set := expectedConsumerSetFixture(t, 1)
	set.RequiredCardinality++
	if _, err := s.CreatePlatformExpectedConsumerSet(set); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid cardinality to be rejected, got %v", err)
	}
}

func expectedConsumerSetFixture(t *testing.T, revision int64) model.PlatformExpectedConsumerSet {
	t.Helper()
	preparedAt := time.Date(2026, 7, 10, 9, 0, 0, 0, time.UTC)
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID:      "release-set-test",
		ArtifactReleaseID: "artifact-release-test",
		ArtifactKind:      model.PlatformArtifactKindCaddyRouteConfig,
		Scope:             model.PlatformArtifactScope{ScopeType: "global"},
		ScopeKey:          "global",
		Generation:        "generation-test",
		Revision:          revision,
		PreparedAt:        preparedAt.Add(time.Duration(revision-1) * time.Minute),
		Topology: platformcontrol.ExpectedConsumerTopology{
			EdgeNodes: []model.EdgeNode{{
				ID:          "edge-test",
				EdgeGroupID: "edge-group-test",
				Country:     "US",
			}},
		},
	})
	if err != nil {
		t.Fatalf("build expected consumer set fixture: %v", err)
	}
	return set
}
