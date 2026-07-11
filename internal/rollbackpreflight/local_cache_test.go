package rollbackpreflight

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func TestCollectLocalCacheObservationsPassesExactRouteAndDNSGenerations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	requirements := localCacheRequirementsForTest()
	inventory := LocalCacheInventory{
		SourceID:         "edge-node-a",
		ObservedAt:       now,
		ValidFor:         2 * time.Minute,
		ClockUncertainty: 2 * time.Second,
		Candidates: []LocalCacheCandidate{
			localRouteCacheCandidate(t, requirements[0], "route-current", now, 10*time.Minute, time.Hour),
			localDNSCacheCandidate(t, requirements[1], "dns-current", now, 10*time.Minute, time.Hour, true),
		},
	}

	observations := CollectLocalCacheObservations(requirements, inventory, testKeyring())
	if len(observations) != 2 {
		t.Fatalf("local cache observation count = %d, want 2: %+v", len(observations), observations)
	}
	result := platformsafety.EvaluateRollbackAssetPreflight(requirements, LocalCacheEvidence(observations), now)
	if !result.Pass {
		t.Fatalf("verified exact local cache generations must pass: %+v", result)
	}
	for _, observation := range observations {
		if observation.Evidence.State != model.InvariantEvidenceStatePass ||
			!strings.Contains(observation.Detail, "verified local cache candidate") &&
				!strings.Contains(observation.Detail, "verified local cache candidate(s)") {
			t.Fatalf("unexpected passing observation: %+v", observation)
		}
	}
}

func TestCollectLocalCacheObservationsSelectsPinnedGenerationInsteadOfPreviousFilename(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	requirement := localCacheRequirementsForTest()[0]
	newer := requirement
	newer.Identity = "routegen_newer"
	inventory := LocalCacheInventory{
		SourceID:   "edge-node-a",
		ObservedAt: now,
		ValidFor:   time.Minute,
		Candidates: []LocalCacheCandidate{
			localRouteCacheCandidate(t, newer, "current", now, 10*time.Minute, time.Hour),
			localRouteCacheCandidate(t, newer, "previous", now.Add(-time.Second), 10*time.Minute, time.Hour),
			localRouteCacheCandidate(t, requirement, "versions/routegen_rollback", now.Add(-time.Minute), 10*time.Minute, time.Hour),
		},
	}

	observations := CollectLocalCacheObservations(
		[]platformsafety.RollbackAssetRequirement{requirement},
		inventory,
		testKeyring(),
	)
	if len(observations) != 1 || observations[0].Evidence.State != model.InvariantEvidenceStatePass {
		t.Fatalf("pinned archived generation must be selected by content, not filename ordering: %+v", observations)
	}
}

func TestCollectLocalCacheObservationsFailsClosed(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	requirement := localCacheRequirementsForTest()[0]
	tests := []struct {
		name       string
		mutate     func(*LocalCacheInventory)
		keyring    bundleauth.Keyring
		wantState  string
		wantDetail string
	}{
		{
			name: "missing source",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.SourceID = ""
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateUnknown, wantDetail: "source identity",
		},
		{
			name: "missing observation time",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.ObservedAt = time.Time{}
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateUnknown, wantDetail: "observation time",
		},
		{
			name: "missing keyring",
			mutate: func(_ *LocalCacheInventory) {
			},
			keyring: bundleauth.Keyring{}, wantState: model.InvariantEvidenceStateUnknown, wantDetail: "keyring",
		},
		{
			name: "missing candidate",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.Candidates = nil
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateUnknown, wantDetail: "no candidates",
		},
		{
			name: "invalid inventory validity",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.ValidFor = 0
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateUnknown, wantDetail: "validity must be positive",
		},
		{
			name: "invalid clock uncertainty",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.ClockUncertainty = -time.Second
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateUnknown, wantDetail: "clock uncertainty",
		},
		{
			name: "missing candidate identity",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.Candidates[0].CandidateID = ""
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateFail, wantDetail: "identity is missing",
		},
		{
			name: "duplicate candidate identity",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.Candidates = append(inventory.Candidates, inventory.Candidates[0])
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateFail, wantDetail: "identity is duplicated",
		},
		{
			name: "invalid max stale",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.Candidates[0].MaxStale = -time.Second
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateFail, wantDetail: "max stale duration",
		},
		{
			name: "malformed cache",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.Candidates[0].Data = []byte("{")
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateFail, wantDetail: "unattributed_corrupt=1",
		},
		{
			name: "invalid signature",
			mutate: func(inventory *LocalCacheInventory) {
				var cached localEdgeRouteCacheFile
				if err := json.Unmarshal(inventory.Candidates[0].Data, &cached); err != nil {
					panic(err)
				}
				cached.Bundle.Signature = "invalid"
				cached.Bundle.Signatures = nil
				inventory.Candidates[0].Data, _ = json.Marshal(cached)
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateFail, wantDetail: "matching_fail=1",
		},
		{
			name: "generation and version mismatch",
			mutate: func(inventory *LocalCacheInventory) {
				var cached localEdgeRouteCacheFile
				if err := json.Unmarshal(inventory.Candidates[0].Data, &cached); err != nil {
					panic(err)
				}
				cached.Bundle.Version = "routegen_other"
				inventory.Candidates[0].Data, _ = json.Marshal(cached)
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateFail, wantDetail: "unattributed_corrupt=1",
		},
		{
			name: "future generated bundle",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.Candidates[0] = localRouteCacheCandidate(t, requirement, "current", now.Add(time.Minute), 10*time.Minute, time.Hour)
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateFail, wantDetail: "matching_fail=1",
		},
		{
			name: "pinned generation absent",
			mutate: func(inventory *LocalCacheInventory) {
				other := requirement
				other.Identity = "routegen_other"
				inventory.Candidates[0] = localRouteCacheCandidate(t, other, "current", now, 10*time.Minute, time.Hour)
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateUnknown, wantDetail: "absent",
		},
		{
			name: "expired beyond max stale",
			mutate: func(inventory *LocalCacheInventory) {
				inventory.Candidates[0] = localRouteCacheCandidate(t, requirement, "archive", now.Add(-2*time.Hour), 10*time.Minute, time.Hour)
			},
			keyring: testKeyring(), wantState: model.InvariantEvidenceStateStale, wantDetail: "are stale",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			inventory := LocalCacheInventory{
				SourceID:   "edge-node-a",
				ObservedAt: now,
				ValidFor:   time.Minute,
				Candidates: []LocalCacheCandidate{
					localRouteCacheCandidate(t, requirement, "current", now, 10*time.Minute, time.Hour),
				},
			}
			test.mutate(&inventory)
			observations := CollectLocalCacheObservations(
				[]platformsafety.RollbackAssetRequirement{requirement},
				inventory,
				test.keyring,
			)
			if len(observations) != 1 || observations[0].Evidence.State != test.wantState ||
				!strings.Contains(observations[0].Detail, test.wantDetail) {
				t.Fatalf("observations = %+v, want state=%q detail containing %q", observations, test.wantState, test.wantDetail)
			}
			result := platformsafety.EvaluateRollbackAssetPreflight(
				[]platformsafety.RollbackAssetRequirement{requirement},
				LocalCacheEvidence(observations),
				now,
			)
			if result.Pass {
				t.Fatalf("invalid local cache evidence must fail closed: %+v", result)
			}
		})
	}
}

func TestCollectLocalCacheObservationsAllowsRuntimeMaxStaleWindow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	requirement := localCacheRequirementsForTest()[0]
	inventory := LocalCacheInventory{
		SourceID:   "edge-node-a",
		ObservedAt: now,
		ValidFor:   time.Minute,
		Candidates: []LocalCacheCandidate{
			localRouteCacheCandidate(t, requirement, "archive", now.Add(-20*time.Minute), 10*time.Minute, time.Hour),
		},
	}
	observations := CollectLocalCacheObservations(
		[]platformsafety.RollbackAssetRequirement{requirement}, inventory, testKeyring(),
	)
	if len(observations) != 1 || observations[0].Evidence.State != model.InvariantEvidenceStatePass {
		t.Fatalf("signed cache within configured max-stale must remain available: %+v", observations)
	}
}

func TestCollectLocalCacheObservationsAllowsConfiguredClockUncertainty(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	requirement := localCacheRequirementsForTest()[0]
	inventory := LocalCacheInventory{
		SourceID:         "edge-node-a",
		ObservedAt:       now,
		ValidFor:         time.Minute,
		ClockUncertainty: 2 * time.Second,
		Candidates: []LocalCacheCandidate{
			localRouteCacheCandidate(t, requirement, "current", now.Add(time.Second), 10*time.Minute, time.Hour),
		},
	}
	observations := CollectLocalCacheObservations(
		[]platformsafety.RollbackAssetRequirement{requirement}, inventory, testKeyring(),
	)
	if len(observations) != 1 || observations[0].Evidence.State != model.InvariantEvidenceStatePass {
		t.Fatalf("generation time inside explicit clock uncertainty must pass: %+v", observations)
	}
}

func TestCollectLocalCacheObservationsUsesVerifiedCopyWhenAnotherCopyIsCorrupt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	requirement := localCacheRequirementsForTest()[0]
	corrupt := localRouteCacheCandidate(t, requirement, "previous", now, 10*time.Minute, time.Hour)
	var cached localEdgeRouteCacheFile
	if err := json.Unmarshal(corrupt.Data, &cached); err != nil {
		t.Fatalf("decode route fixture: %v", err)
	}
	cached.Bundle.Signature = "invalid"
	cached.Bundle.Signatures = nil
	corrupt.Data, _ = json.Marshal(cached)
	inventory := LocalCacheInventory{
		SourceID:   "edge-node-a",
		ObservedAt: now,
		ValidFor:   time.Minute,
		Candidates: []LocalCacheCandidate{
			corrupt,
			localRouteCacheCandidate(t, requirement, "versions/routegen_rollback", now.Add(-time.Minute), 10*time.Minute, time.Hour),
		},
	}
	observations := CollectLocalCacheObservations(
		[]platformsafety.RollbackAssetRequirement{requirement}, inventory, testKeyring(),
	)
	if len(observations) != 1 || observations[0].Evidence.State != model.InvariantEvidenceStatePass ||
		!strings.Contains(observations[0].Detail, "matching_fail=1") {
		t.Fatalf("one independently verified copy must prove availability while retaining degraded detail: %+v", observations)
	}
}

func TestCollectLocalCacheObservationsVerifiesDNSEnvelopeAndInnerBundle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
	requirement := localCacheRequirementsForTest()[1]
	tests := []struct {
		name       string
		candidate  func(*testing.T) LocalCacheCandidate
		wantState  string
		wantDetail string
	}{
		{
			name: "legacy direct cache",
			candidate: func(t *testing.T) LocalCacheCandidate {
				return localDNSCacheCandidate(t, requirement, "legacy", now, 10*time.Minute, time.Hour, false)
			},
			wantState: model.InvariantEvidenceStatePass, wantDetail: "verified local cache candidate",
		},
		{
			name: "envelope hash mismatch",
			candidate: func(t *testing.T) LocalCacheCandidate {
				candidate := localDNSCacheCandidate(t, requirement, "current", now, 10*time.Minute, time.Hour, true)
				var envelope lkgcache.Envelope
				if err := json.Unmarshal(candidate.Data, &envelope); err != nil {
					t.Fatalf("decode envelope fixture: %v", err)
				}
				envelope.ContentHash = "sha256:" + strings.Repeat("0", 64)
				candidate.Data, _ = json.Marshal(envelope)
				return candidate
			},
			wantState: model.InvariantEvidenceStateFail, wantDetail: "matching_fail=1",
		},
		{
			name: "expired envelope",
			candidate: func(t *testing.T) LocalCacheCandidate {
				candidate := localDNSCacheCandidate(t, requirement, "current", now.Add(-time.Minute), 10*time.Minute, time.Hour, true)
				var envelope lkgcache.Envelope
				if err := json.Unmarshal(candidate.Data, &envelope); err != nil {
					t.Fatalf("decode envelope fixture: %v", err)
				}
				envelope.ExpiresAt = now.Add(-time.Second)
				candidate.Data, _ = json.Marshal(envelope)
				return candidate
			},
			wantState: model.InvariantEvidenceStateStale, wantDetail: "are stale",
		},
		{
			name: "future envelope creation",
			candidate: func(t *testing.T) LocalCacheCandidate {
				candidate := localDNSCacheCandidate(t, requirement, "current", now, 10*time.Minute, time.Hour, true)
				var envelope lkgcache.Envelope
				if err := json.Unmarshal(candidate.Data, &envelope); err != nil {
					t.Fatalf("decode envelope fixture: %v", err)
				}
				envelope.CreatedAt = now.Add(time.Minute)
				candidate.Data, _ = json.Marshal(envelope)
				return candidate
			},
			wantState: model.InvariantEvidenceStateFail, wantDetail: "matching_fail=1",
		},
		{
			name: "wrong envelope kind",
			candidate: func(t *testing.T) LocalCacheCandidate {
				candidate := localDNSCacheCandidate(t, requirement, "current", now, 10*time.Minute, time.Hour, true)
				var envelope lkgcache.Envelope
				if err := json.Unmarshal(candidate.Data, &envelope); err != nil {
					t.Fatalf("decode envelope fixture: %v", err)
				}
				envelope.Kind = "other_kind"
				candidate.Data, _ = json.Marshal(envelope)
				return candidate
			},
			wantState: model.InvariantEvidenceStateFail, wantDetail: "matching_fail=1",
		},
		{
			name: "envelope and bundle generation mismatch",
			candidate: func(t *testing.T) LocalCacheCandidate {
				candidate := localDNSCacheCandidate(t, requirement, "current", now, 10*time.Minute, time.Hour, false)
				var cached localDNSCacheFile
				if err := json.Unmarshal(candidate.Data, &cached); err != nil {
					t.Fatalf("decode dns fixture: %v", err)
				}
				cached.Bundle.Version = "dnsgen_other"
				cached.Bundle.Generation = "dnsgen_other"
				cached.Bundle = bundleauth.SignEdgeDNSBundleWithKeyring(cached.Bundle, testKeyring(), 10*time.Minute)
				payload, err := json.Marshal(cached)
				if err != nil {
					t.Fatalf("marshal mismatched dns payload: %v", err)
				}
				envelope, err := lkgcache.NewEnvelope(
					model.PlatformArtifactKindDNSAnswerBundle,
					requirement.Identity,
					payload,
					cached.Bundle.ValidUntil.Add(time.Hour),
					now,
				)
				if err != nil {
					t.Fatalf("create mismatched dns envelope: %v", err)
				}
				candidate.Data, _ = json.Marshal(envelope)
				return candidate
			},
			wantState: model.InvariantEvidenceStateFail, wantDetail: "matching_fail=1",
		},
		{
			name: "unsupported cache payload version",
			candidate: func(t *testing.T) LocalCacheCandidate {
				candidate := localDNSCacheCandidate(t, requirement, "legacy", now, 10*time.Minute, time.Hour, false)
				var cached localDNSCacheFile
				if err := json.Unmarshal(candidate.Data, &cached); err != nil {
					t.Fatalf("decode dns fixture: %v", err)
				}
				cached.Version = 2
				candidate.Data, _ = json.Marshal(cached)
				return candidate
			},
			wantState: model.InvariantEvidenceStateFail, wantDetail: "matching_fail=1",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			inventory := LocalCacheInventory{
				SourceID:   "dns-node-a",
				ObservedAt: now,
				ValidFor:   time.Minute,
				Candidates: []LocalCacheCandidate{test.candidate(t)},
			}
			observations := CollectLocalCacheObservations(
				[]platformsafety.RollbackAssetRequirement{requirement}, inventory, testKeyring(),
			)
			if len(observations) != 1 || observations[0].Evidence.State != test.wantState ||
				!strings.Contains(observations[0].Detail, test.wantDetail) {
				t.Fatalf("observations = %+v, want state=%q detail containing %q", observations, test.wantState, test.wantDetail)
			}
		})
	}
}

func localCacheRequirementsForTest() []platformsafety.RollbackAssetRequirement {
	return []platformsafety.RollbackAssetRequirement{
		{
			Kind:      platformsafety.RollbackAssetKindEdgeRouteBundle,
			ScopeKey:  "edge:node-a",
			Reference: "route-cache",
			Identity:  "routegen_rollback",
		},
		{
			Kind:      platformsafety.RollbackAssetKindDNSBundle,
			ScopeKey:  "dns:node-a:example.test",
			Reference: "dns-cache",
			Identity:  "dnsgen_rollback",
		},
	}
}

func localRouteCacheCandidate(
	t *testing.T,
	requirement platformsafety.RollbackAssetRequirement,
	candidateID string,
	generatedAt time.Time,
	validFor time.Duration,
	maxStale time.Duration,
) LocalCacheCandidate {
	t.Helper()
	bundle := bundleauth.SignEdgeRouteBundleWithKeyring(model.EdgeRouteBundle{
		Version:      requirement.Identity,
		Generation:   requirement.Identity,
		GeneratedAt:  generatedAt,
		EdgeID:       "edge-node-a",
		Routes:       []model.EdgeRouteBinding{},
		TLSAllowlist: []model.EdgeTLSAllowlistEntry{},
	}, testKeyring(), validFor)
	data, err := json.Marshal(localEdgeRouteCacheFile{
		Version:  1,
		CachedAt: generatedAt,
		Bundle:   bundle,
	})
	if err != nil {
		t.Fatalf("marshal edge route cache fixture: %v", err)
	}
	return LocalCacheCandidate{
		Kind:        requirement.Kind,
		ScopeKey:    requirement.ScopeKey,
		Reference:   requirement.Reference,
		CandidateID: candidateID,
		Data:        data,
		MaxStale:    maxStale,
	}
}

func localDNSCacheCandidate(
	t *testing.T,
	requirement platformsafety.RollbackAssetRequirement,
	candidateID string,
	generatedAt time.Time,
	validFor time.Duration,
	maxStale time.Duration,
	envelope bool,
) LocalCacheCandidate {
	t.Helper()
	bundle := bundleauth.SignEdgeDNSBundleWithKeyring(model.EdgeDNSBundle{
		Version:     requirement.Identity,
		Generation:  requirement.Identity,
		GeneratedAt: generatedAt,
		DNSNodeID:   "dns-node-a",
		Zone:        "example.test",
		Records:     []model.EdgeDNSRecord{},
	}, testKeyring(), validFor)
	payload, err := json.Marshal(localDNSCacheFile{
		Version:  1,
		CachedAt: generatedAt,
		Bundle:   bundle,
	})
	if err != nil {
		t.Fatalf("marshal dns cache fixture: %v", err)
	}
	data := payload
	if envelope {
		wrapped, err := lkgcache.NewEnvelope(
			model.PlatformArtifactKindDNSAnswerBundle,
			requirement.Identity,
			payload,
			bundle.ValidUntil.Add(maxStale),
			generatedAt,
		)
		if err != nil {
			t.Fatalf("create dns cache envelope fixture: %v", err)
		}
		data, err = json.Marshal(wrapped)
		if err != nil {
			t.Fatalf("marshal dns cache envelope fixture: %v", err)
		}
	}
	return LocalCacheCandidate{
		Kind:        requirement.Kind,
		ScopeKey:    requirement.ScopeKey,
		Reference:   requirement.Reference,
		CandidateID: candidateID,
		Data:        data,
		MaxStale:    maxStale,
	}
}
