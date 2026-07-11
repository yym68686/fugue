package rollbackpreflight

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func TestCollectArtifactStoreObservationsPassesAllPersistedAssetKinds(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 11, 0, 0, 0, time.UTC)
	requirements := requirementsForTest()
	inventory := ArtifactStoreInventory{
		SourceID:   "platform-store-primary",
		ObservedAt: now,
		ValidFor:   5 * time.Minute,
	}
	for _, requirement := range requirements {
		if requirement.Kind == platformsafety.RollbackAssetKindImageDigest {
			continue
		}
		artifact, content, snapshot := artifactStoreFixture(t, requirement, now, now.Add(time.Hour))
		inventory.Artifacts = append(inventory.Artifacts, artifact)
		inventory.Contents = append(inventory.Contents, content)
		inventory.LKGSnapshots = append(inventory.LKGSnapshots, snapshot)
	}

	observations := CollectArtifactStoreObservations(requirements, inventory, testKeyring())
	if len(observations) != len(requirements)-1 {
		t.Fatalf("artifact store observation count = %d, want %d: %+v", len(observations), len(requirements)-1, observations)
	}
	evidence := Evidence(observations)
	evidence = append(evidence, passingImageEvidence(requirements[0], now))
	result := platformsafety.EvaluateRollbackAssetPreflight(requirements, evidence, now)
	if !result.Pass {
		t.Fatalf("complete registry and artifact store rollback evidence must pass: %+v", result)
	}
	for _, check := range result.Checks {
		if !check.Pass || check.State != model.InvariantEvidenceStatePass {
			t.Fatalf("rollback asset check did not pass: %+v", check)
		}
	}
}

func TestCollectArtifactStoreObservationsFailsClosed(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 11, 0, 0, 0, time.UTC)
	requirement := requirementsForTest()[3]
	tests := []struct {
		name       string
		mutate     func(*ArtifactStoreInventory, *testing.T)
		wantState  string
		wantDetail string
	}{
		{
			name: "missing source identity",
			mutate: func(inventory *ArtifactStoreInventory, _ *testing.T) {
				inventory.SourceID = ""
			},
			wantState:  model.InvariantEvidenceStateUnknown,
			wantDetail: "source identity is missing",
		},
		{
			name: "invalid validity",
			mutate: func(inventory *ArtifactStoreInventory, _ *testing.T) {
				inventory.ValidFor = 0
			},
			wantState:  model.InvariantEvidenceStateUnknown,
			wantDetail: "validity must be positive",
		},
		{
			name: "missing artifact",
			mutate: func(inventory *ArtifactStoreInventory, _ *testing.T) {
				inventory.Artifacts = nil
			},
			wantState:  model.InvariantEvidenceStateUnknown,
			wantDetail: "artifact generation is missing",
		},
		{
			name: "duplicate artifact",
			mutate: func(inventory *ArtifactStoreInventory, _ *testing.T) {
				inventory.Artifacts = append(inventory.Artifacts, inventory.Artifacts[0])
			},
			wantState:  model.InvariantEvidenceStateFail,
			wantDetail: "multiple artifacts match",
		},
		{
			name: "missing content",
			mutate: func(inventory *ArtifactStoreInventory, _ *testing.T) {
				inventory.Contents = nil
			},
			wantState:  model.InvariantEvidenceStateUnknown,
			wantDetail: "payload is missing",
		},
		{
			name: "corrupt content",
			mutate: func(inventory *ArtifactStoreInventory, _ *testing.T) {
				inventory.Contents[0].Content = map[string]any{"generation": "tampered"}
			},
			wantState:  model.InvariantEvidenceStateFail,
			wantDetail: "hash or size does not match",
		},
		{
			name: "same size corrupt content",
			mutate: func(inventory *ArtifactStoreInventory, _ *testing.T) {
				inventory.Contents[0].Content["generation"] = strings.Repeat("x", len(requirement.Identity))
			},
			wantState:  model.InvariantEvidenceStateFail,
			wantDetail: "failed integrity verification",
		},
		{
			name: "duplicate content",
			mutate: func(inventory *ArtifactStoreInventory, _ *testing.T) {
				inventory.Contents = append(inventory.Contents, inventory.Contents[0])
			},
			wantState:  model.InvariantEvidenceStateFail,
			wantDetail: "payload is duplicated",
		},
		{
			name: "missing lkg",
			mutate: func(inventory *ArtifactStoreInventory, _ *testing.T) {
				inventory.LKGSnapshots = nil
			},
			wantState:  model.InvariantEvidenceStateUnknown,
			wantDetail: "LKG snapshot",
		},
		{
			name: "expired lkg",
			mutate: func(inventory *ArtifactStoreInventory, testT *testing.T) {
				snapshot := inventory.LKGSnapshots[0]
				snapshot.ExpiresAt = now.Add(-time.Minute)
				var err error
				inventory.LKGSnapshots[0], err = platformsafety.SignPlatformLKGSnapshot(snapshot, testKeyring())
				if err != nil {
					testT.Fatalf("sign expired LKG fixture: %v", err)
				}
			},
			wantState:  model.InvariantEvidenceStateStale,
			wantDetail: platformsafety.InvariantLKGNotExpired,
		},
		{
			name: "invalid lkg signature",
			mutate: func(inventory *ArtifactStoreInventory, _ *testing.T) {
				inventory.LKGSnapshots[0].SnapshotProvenance.Signature = "invalid"
			},
			wantState:  model.InvariantEvidenceStateFail,
			wantDetail: platformsafety.InvariantLKGSignature,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			artifact, content, snapshot := artifactStoreFixture(t, requirement, now, now.Add(time.Hour))
			inventory := ArtifactStoreInventory{
				Artifacts:    []model.PlatformArtifact{artifact},
				Contents:     []model.PlatformArtifactContent{content},
				LKGSnapshots: []model.PlatformLKGSnapshot{snapshot},
				SourceID:     "platform-store-primary",
				ObservedAt:   now,
				ValidFor:     5 * time.Minute,
			}
			test.mutate(&inventory, t)
			observations := CollectArtifactStoreObservations(
				[]platformsafety.RollbackAssetRequirement{requirement},
				inventory,
				testKeyring(),
			)
			if len(observations) != 1 || observations[0].Evidence.State != test.wantState ||
				!strings.Contains(observations[0].Detail, test.wantDetail) {
				t.Fatalf("observations = %+v, want state=%q detail containing %q", observations, test.wantState, test.wantDetail)
			}
			result := platformsafety.EvaluateRollbackAssetPreflight(
				[]platformsafety.RollbackAssetRequirement{requirement},
				Evidence(observations),
				now,
			)
			if result.Pass || len(result.Checks) != 1 {
				t.Fatalf("artifact store observation must fail closed: %+v", result)
			}
		})
	}
}

func TestCollectArtifactStoreObservationsUsesNewestMatchingLKG(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 11, 0, 0, 0, time.UTC)
	requirement := requirementsForTest()[1]
	artifact, content, latest := artifactStoreFixture(t, requirement, now, now.Add(time.Hour))
	older := latest
	older.ID = "lkg-older"
	older.ExpiresAt = now.Add(-time.Minute)
	older.CreatedAt = now.Add(-3 * time.Minute)
	older.UpdatedAt = now.Add(-3 * time.Minute)
	var err error
	older, err = platformsafety.SignPlatformLKGSnapshot(older, testKeyring())
	if err != nil {
		t.Fatalf("sign older LKG fixture: %v", err)
	}
	inventory := ArtifactStoreInventory{
		Artifacts:    []model.PlatformArtifact{artifact},
		Contents:     []model.PlatformArtifactContent{content},
		LKGSnapshots: []model.PlatformLKGSnapshot{latest, older},
		SourceID:     "platform-store-primary",
		ObservedAt:   now,
		ValidFor:     5 * time.Minute,
	}
	observations := CollectArtifactStoreObservations(
		[]platformsafety.RollbackAssetRequirement{requirement},
		inventory,
		testKeyring(),
	)
	if len(observations) != 1 || observations[0].Evidence.State != model.InvariantEvidenceStatePass {
		t.Fatalf("newest fresh LKG must supersede older expired history: %+v", observations)
	}

	latest.SnapshotProvenance.Signature = "invalid"
	inventory.LKGSnapshots = []model.PlatformLKGSnapshot{older, latest}
	observations = CollectArtifactStoreObservations(
		[]platformsafety.RollbackAssetRequirement{requirement},
		inventory,
		testKeyring(),
	)
	if len(observations) != 1 || observations[0].Evidence.State != model.InvariantEvidenceStateFail ||
		!strings.Contains(observations[0].Detail, platformsafety.InvariantLKGSignature) {
		t.Fatalf("latest corrupt LKG must not be hidden by older history: %+v", observations)
	}
}

func artifactStoreFixture(
	t *testing.T,
	requirement platformsafety.RollbackAssetRequirement,
	now time.Time,
	expiresAt time.Time,
) (model.PlatformArtifact, model.PlatformArtifactContent, model.PlatformLKGSnapshot) {
	t.Helper()
	contentMap := map[string]any{
		"generation": requirement.Identity,
		"kind":       requirement.Kind,
	}
	artifact := model.PlatformArtifact{
		ID:                 "artifact-" + requirement.Identity,
		ArtifactKind:       requirement.Kind,
		Scope:              model.PlatformArtifactScope{ScopeType: "test", Key: requirement.ScopeKey},
		ScopeKey:           requirement.ScopeKey,
		SchemaVersion:      model.PlatformArtifactSchemaVersionV1,
		Generation:         requirement.Identity,
		GenerationSequence: 7,
		Status:             model.PlatformArtifactStatusValidated,
		Content:            contentMap,
		Metadata:           map[string]string{},
		CreatedAt:          now.Add(-2 * time.Minute),
		UpdatedAt:          now.Add(-2 * time.Minute),
	}
	artifact.ContentHash = contentHash(artifact.Content)
	var err error
	artifact, err = platformsafety.SignPlatformArtifact(artifact, testKeyring())
	if err != nil {
		t.Fatalf("sign rollback artifact fixture: %v", err)
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		t.Fatalf("marshal rollback artifact fixture: %v", err)
	}
	content := model.PlatformArtifactContent{
		ContentHash: artifact.ContentHash,
		Content:     artifact.Content,
		SizeBytes:   int64(len(raw)),
		CreatedAt:   artifact.CreatedAt,
		UpdatedAt:   artifact.UpdatedAt,
	}
	snapshot, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{
		ID:                       "lkg-" + requirement.Identity,
		ArtifactID:               artifact.ID,
		ArtifactKind:             artifact.ArtifactKind,
		Scope:                    artifact.Scope,
		ScopeKey:                 artifact.ScopeKey,
		SchemaVersion:            artifact.SchemaVersion,
		Generation:               artifact.Generation,
		GenerationSequence:       artifact.GenerationSequence,
		ContentHash:              artifact.ContentHash,
		ArtifactProvenance:       artifact.Provenance,
		VerifiedByReleaseID:      "release-" + requirement.Identity,
		VerificationEvidenceHash: "sha256:" + strings.Repeat("c", 64),
		ExpiresAt:                expiresAt,
		CreatedAt:                now.Add(-time.Minute),
		UpdatedAt:                now.Add(-time.Minute),
	}, testKeyring())
	if err != nil {
		t.Fatalf("sign rollback LKG fixture: %v", err)
	}
	return artifact, content, snapshot
}

func requirementsForTest() []platformsafety.RollbackAssetRequirement {
	return []platformsafety.RollbackAssetRequirement{
		{
			Kind: platformsafety.RollbackAssetKindImageDigest, ScopeKey: "control-plane", Reference: "ghcr.io/acme/fugue-api", Identity: "sha256:" + strings.Repeat("a", 64),
		},
		{
			Kind: platformsafety.RollbackAssetKindCaddyConfig, ScopeKey: "edge:global", Reference: "caddy-routes", Identity: "caddy_gen_7",
		},
		{
			Kind: platformsafety.RollbackAssetKindDNSBundle, ScopeKey: "dns:global", Reference: "authoritative-answers", Identity: "dns_gen_7",
		},
		{
			Kind: platformsafety.RollbackAssetKindEdgeRouteBundle, ScopeKey: "edge:global", Reference: "http-routes", Identity: "routes_gen_7",
		},
		{
			Kind: platformsafety.RollbackAssetKindNodeDesiredState, ScopeKey: "node:edge-a", Reference: "node-updater", Identity: "node_gen_7",
		},
	}
}

func passingImageEvidence(requirement platformsafety.RollbackAssetRequirement, now time.Time) platformsafety.RollbackAssetEvidence {
	return platformsafety.RollbackAssetEvidence{
		Kind:       requirement.Kind,
		ScopeKey:   requirement.ScopeKey,
		Reference:  requirement.Reference,
		Identity:   requirement.Identity,
		State:      model.InvariantEvidenceStatePass,
		SourceID:   "registry-verifier",
		ObservedAt: now.Add(-time.Minute),
		ExpiresAt:  now.Add(5 * time.Minute),
	}
}

func testKeyring() bundleauth.Keyring {
	return bundleauth.NewKeyring("rollback-preflight-test-key", "rollback-preflight-test", "", "", nil)
}

func contentHash(content map[string]any) string {
	raw, err := json.Marshal(content)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
