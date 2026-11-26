package store

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func TestSafeRolloutServingMembershipRequiresVerifiedOfflinePublication(t *testing.T) {
	for _, scenario := range []string{"verified", "recovered", "invalid_recovery", "historical_hold", "unverified_full", "missing_lkg", "previous_lkg", "failed_verification", "expired_lkg", "tampered_source", "wrong_identity", "still_in_dns", "missing_dns_plan", "cell_publication", "producer_mismatch",
		"transition_excluded", "transition_bad_pin", "transition_bad_gray", "transition_retains_member"} {
		t.Run(scenario, func(t *testing.T) {
			f := newServingFixture(t, "")
			source, err := f.s.GetPlatformArtifact(f.policy.DNSPolicyArtifactID)
			if err != nil {
				t.Fatal(err)
			}
			holds := []platformproducer.OfflineEdge{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "maintenance", ExpectedRecoveryDate: "2020-01-01"}}
			if scenario == "recovered" || scenario == "invalid_recovery" || scenario == "historical_hold" {
				holds[0].Recovery = &platformproducer.OfflineEdgeRecoveryPolicy{Hostname: "api.example.test", Path: "/", ProbeIntervalSeconds: 300, ProbeTimeoutSeconds: 5, SuccessThreshold: 3, FailureThreshold: 1, FreshnessSeconds: 1800}
			}
			source.Content["offline_edges"] = holds
			source.Content["generation"] = "offline-input"
			source = savePhysicalDNSTestArtifact(t, f.s, source.ArtifactKind, source.ScopeKey, "offline-input", source.Content)
			f.policy.Generation = "offline-producer"
			f.policy.DNSPolicyArtifactID, f.policy.DNSPolicyDigest = source.ID, source.ContentHash
			authority := savePhysicalDNSTestArtifact(t, f.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, f.policy.Generation, f.policy)
			ownerRelease := model.PlatformArtifactRelease{ID: "offline-authority", ArtifactID: authority.ID, ArtifactKind: authority.ArtifactKind, ScopeKey: authority.ScopeKey, Generation: authority.Generation, ReleaseChannel: "shadow", FencingToken: 2}
			plan := platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{{EdgeID: "edge-b", Address: "9.9.9.9:443"}}, Records: []platformconfig.DNSReadinessRecord{{Hostname: "app.example.test", Targets: []platformconfig.DNSReadinessTarget{{EdgeID: "edge-b", Address: "9.9.9.9"}}}}}
			if scenario == "recovered" || scenario == "invalid_recovery" {
				plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{EdgeID: "edge-a", Address: "8.8.8.8:443"})
				plan.Records[0].Targets = append(plan.Records[0].Targets, platformconfig.DNSReadinessTarget{EdgeID: "edge-a", Address: "8.8.8.8"})
			}
			if scenario == "still_in_dns" {
				plan.Records[0].Targets[0].Address = "8.8.8.8"
			}
			if scenario == "missing_dns_plan" {
				plan.Probes = nil
			}
			dns := savePhysicalDNSTestArtifact(t, f.s, model.PlatformArtifactKindDNSAnswerBundle, "global", "offline-dns", map[string]any{"readiness_plan": plan})
			parent := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindReleaseSet, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: "offline-parent", CreatedByType: model.ActorTypeBootstrap, CreatedByID: platformproducer.Actor, Content: map[string]any{"artifact_ids": []any{dns.ID}, "artifact_kinds": []any{dns.ArtifactKind}}, Metadata: map[string]string{platformproducer.DNSPolicyIDMetadata: source.ID, platformproducer.DNSPolicyDigestMetadata: source.ContentHash, platformproducer.StaticIntentIDMetadata: f.base.ID, platformproducer.StaticIntentDigestMetadata: f.base.ContentHash, platformproducer.PolicyReleaseMetadata: ownerRelease.ID}}
			if scenario == "recovered" || scenario == "invalid_recovery" {
				at := time.Now().UTC().Add(-time.Second)
				o := platformproducer.OfflineRecoveryObservation{EdgeID: "edge-a", DeclarationDigest: platformproducer.OfflineRecoveryDigest(holds[0]), LastAttempt: at, LastSuccess: at, ProofValidUntil: at.Add(time.Hour), Successes: 3, Admitted: true}
				if scenario == "invalid_recovery" {
					o.DeclarationDigest = "foreign"
				}
				raw, err := json.Marshal(map[string]any{"captured_at": at, "observations": []platformproducer.OfflineRecoveryObservation{o}})
				if err != nil {
					t.Fatal(err)
				}
				parent.Metadata[platformproducer.OfflineRecoveryMetadata] = string(raw)
			}
			if scenario == "cell_publication" {
				parent.Content["publication_role"] = platformconfig.PublicationRoleCellRoutes
			}
			if scenario == "producer_mismatch" {
				parent.Metadata[platformproducer.StaticIntentDigestMetadata] = "sha256:" + strings.Repeat("e", 64)
			}
			parent, err = f.s.CreatePlatformArtifact(parent)
			if err != nil {
				t.Fatal(err)
			}
			parent, err = f.s.ValidatePlatformArtifact(parent.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			full := model.PlatformArtifactRelease{ID: "offline-full", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey, Generation: parent.Generation, ReleaseChannel: "full", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 1, VerificationState: model.PlatformArtifactVerificationStateVerified, ReleasedAt: now}
			transition := strings.HasPrefix(scenario, "transition_")
			var candidateFull, candidateGray model.PlatformArtifactRelease
			if transition {
				full.Status, full.VerifiedLKGGeneration, full.VerifiedAt = model.PlatformArtifactReleaseStatusSuperseded, full.Generation, &now
				full.ReleasedByType, full.ReleasedByID = model.ActorTypeBootstrap, platformproducer.Actor
				candidate := parent
				candidate.ID, candidate.Generation = "", "offline-successor"
				if scenario == "transition_retains_member" {
					plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{EdgeID: "edge-a", Address: "8.8.8.8:443"})
					child := savePhysicalDNSTestArtifact(t, f.s, dns.ArtifactKind, dns.ScopeKey, "new-dns", map[string]any{"readiness_plan": plan})
					candidate.Content = map[string]any{"artifact_ids": []any{child.ID}, "artifact_kinds": []any{child.ArtifactKind}}
				}
				candidate, err = f.s.CreatePlatformArtifact(candidate)
				if err != nil {
					t.Fatal(err)
				}
				candidate, err = f.s.ValidatePlatformArtifact(candidate.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
				if err != nil {
					t.Fatal(err)
				}
				candidateFull = full
				candidateFull.ID, candidateFull.ArtifactID, candidateFull.Generation = "candidate-full", candidate.ID, candidate.Generation
				candidateFull.Status, candidateFull.VerificationState, candidateFull.FencingToken = model.PlatformArtifactReleaseStatusActive, model.PlatformArtifactVerificationStateServingUnverified, 3
				candidateFull.PinnedRollbackGeneration, candidateFull.RollbackTargetGeneration, candidateFull.ReleasedAt = full.Generation, full.Generation, now.Add(time.Second)
				candidateGray = candidateFull
				candidateGray.ID, candidateGray.ReleaseChannel, candidateGray.FencingToken, candidateGray.ReleasedAt = "candidate-gray", "gray", 2, now
				if scenario == "transition_bad_pin" {
					candidateFull.PinnedRollbackGeneration = "another-generation"
				}
				if scenario == "transition_bad_gray" {
					candidateGray.ArtifactID, candidateGray.Generation = parent.ID, parent.Generation
				}
			}
			if scenario == "unverified_full" {
				full.VerificationState = model.PlatformArtifactVerificationStateServingUnverified
			}
			if scenario == "failed_verification" {
				full.VerificationState = model.PlatformArtifactVerificationStateFailed
			}
			expires := now.Add(time.Hour)
			if scenario == "expired_lkg" {
				expires = now.Add(-time.Hour)
			}
			lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{ID: "offline-lkg", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey, SchemaVersion: parent.SchemaVersion, Generation: parent.Generation, GenerationSequence: parent.GenerationSequence, ContentHash: parent.ContentHash, ArtifactProvenance: parent.Provenance, VerifiedByReleaseID: full.ID, VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64), ExpiresAt: expires, CreatedAt: now, UpdatedAt: now}, f.s.platformArtifactSigningKeyring())
			if err != nil {
				t.Fatal(err)
			}
			if err := f.s.withLockedState(true, func(state *model.State) error {
				state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, ownerRelease, full)
				if transition {
					state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, candidateGray, candidateFull)
				}
				if scenario == "previous_lkg" {
					lkg.VerifiedByReleaseID = "previous-full"
				}
				if scenario != "missing_lkg" {
					state.PlatformLKGSnapshots = append(state.PlatformLKGSnapshots, lkg)
				}
				if scenario == "tampered_source" {
					state.PlatformArtifacts[platformArtifactIndex(state.PlatformArtifacts, source.ID)].Provenance.Signature = "forged"
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			nodes := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8"}, {ID: "edge-b", EdgeGroupID: "edge-group-a", PublicIPv4: "9.9.9.9"}}
			if scenario == "wrong_identity" {
				nodes[0].PublicIPv4 = "1.1.1.1"
			}
			selected, err := f.s.SelectSafeRolloutServingEdges(nodes)
			if transition {
				if !errors.Is(err, ErrSafeRolloutServingPublicationPending) {
					t.Fatalf("candidate must not acquire verified status from continuity: %v", err)
				}
				selected, err = f.s.SelectContinuouslyExcludedRolloutEdges(nodes)
				if scenario == "transition_excluded" {
					if err != nil || len(selected) != 1 || selected[0].ID != "edge-b" {
						t.Fatalf("lost continuous exclusion: %v %v", selected, err)
					}
				} else if err == nil || len(selected) != 0 {
					t.Fatalf("unsafe continuous exclusion: %v %v", selected, err)
				}
				return
			}
			if scenario == "verified" || scenario == "historical_hold" || scenario == "recovered" {
				wantCount, firstID := 1, "edge-b"
				if scenario == "recovered" {
					wantCount, firstID = 2, "edge-a"
				}
				if err != nil || len(selected) != wantCount || selected[0].ID != firstID {
					t.Fatalf("verified offline member retained: %+v %v", selected, err)
				}
			} else {
				if err == nil || len(selected) != 0 {
					t.Fatalf("unsafe exclusion accepted for %s: %+v %v", scenario, selected, err)
				}
				pending := scenario == "unverified_full" || scenario == "missing_lkg" || scenario == "previous_lkg"
				if errors.Is(err, ErrSafeRolloutServingPublicationPending) != pending {
					t.Fatalf("incorrect retry classification for %s: %v", scenario, err)
				}
				if want, ok := map[string]string{
					"invalid_recovery":    "pinned_offline_recovery",
					"failed_verification": "publication_verification_state",
					"expired_lkg":         "lkg_integrity/",
					"tampered_source":     "dns_policy_integrity",
					"wrong_identity":      "offline_inventory_identity",
					"still_in_dns":        "dns_readiness_offline_exclusion",
					"missing_dns_plan":    "dns_readiness_plan_unavailable",
					"cell_publication":    "global_publication_identity",
					"producer_mismatch":   "producer_policy_binding",
				}[scenario]; ok {
					var diagnostic *SafeRolloutServingMembershipError
					if !errors.As(err, &diagnostic) || !strings.HasPrefix(diagnostic.Check, want) {
						t.Fatalf("missing failure check %q: %v", want, err)
					}
				}
			}
		})
	}
}
