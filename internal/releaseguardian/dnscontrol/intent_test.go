package dnscontrol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/productioncontrol"
)

func testArtifact(t *testing.T, id, kind, scope, generation string, seq int64, content any) model.PlatformArtifact {
	t.Helper()
	a := model.PlatformArtifact{
		ID: id, ArtifactKind: kind, Status: model.PlatformArtifactStatusValidated, SchemaVersion: model.PlatformArtifactSchemaVersionV1,
		ScopeKey: scope, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope},
		Generation: generation, GenerationSequence: seq,
		Provenance: model.PlatformArtifactProvenance{Issuer: model.PlatformArtifactIssuerFugue, KeyID: "observer-fixture", Algorithm: model.PlatformSignatureHMACSHA256, Signature: "synthetic-fixture", SignedAt: time.Now().Add(-time.Minute)},
	}
	raw, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &a.Content); err != nil {
		t.Fatal(err)
	}
	a.ContentHash, err = platformconfig.Digest(a.Content)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func testSources(t *testing.T, now time.Time) (Sources, Preview, Preview, productioncontrol.ResourceState) {
	t.Helper()
	consumers := []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}
	static := testArtifact(t, "immutable-static", model.PlatformArtifactKindPlatformIntent, "global", "static-1", 1,
		platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Generation: "static-1", Scope: "global", DNSConsumers: consumers})
	probe := &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 90, MaxConcurrency: 4, MaxProbes: 1024}
	projection := platformproducer.ProjectionPolicyInput{
		SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: "dns-1", DNSPlacementMode: platformconfig.DNSPlacementConsumerReadiness,
		DNSQueryPolicy: &platformconfig.DNSQueryPolicy{RankingMode: "active", PreferenceMode: "runtime_locality", ECSEnabled: true, ExplorationPercent: 5, MinimumTTLSeconds: 60, MaximumTTLSeconds: 120},
		Authorities:    []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}},
		Clients:        []platformconfig.DNSClientPolicy{{NodeID: "dns-a"}}, DNSReadiness: probe, TLSReadiness: probe,
		Cohorts: []platformconfig.TrafficRolloutCohort{{ID: "fixture", EdgeGroupIDs: []string{"edge-group-a"}}},
	}
	oldPolicy := testArtifact(t, "old-dns", model.PlatformArtifactKindPolicySnapshot, "global", projection.Generation, 2, projection)
	projection.Generation = "dns-2"
	projection.OfflineEdges = []platformproducer.OfflineEdge{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "maintenance"}}
	nextPolicy := testArtifact(t, "next-dns", model.PlatformArtifactKindPolicySnapshot, "global", projection.Generation, 3, projection)
	p := platformproducer.Policy{
		SchemaVersion: platformproducer.Schema, Generation: "producer-1", Mode: "serving", InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 60, RefreshSeconds: 300,
		StaticIntentArtifactID: static.ID, StaticIntentDigest: static.ContentHash, DNSPolicyArtifactID: oldPolicy.ID, DNSPolicyDigest: oldPolicy.ContentHash,
		RequireDNSQueryPolicy: true, RequireRouteDefaults: true, RequireApplicationDomains: true,
		Serving: &platformproducer.ServingPolicy{CanaryRuleRef: "cohort=fixture", GrayMinSeconds: 10, FullMinSeconds: 10, RolloutTimeoutSeconds: 180},
	}
	old := testArtifact(t, "old-producer", model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, p.Generation, 4, p)
	p.Generation, p.DNSPolicyArtifactID, p.DNSPolicyDigest = "producer-2", nextPolicy.ID, nextPolicy.ContentHash
	next := testArtifact(t, "next-producer", model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, p.Generation, 5, p)
	pub := func(a model.PlatformArtifact, channel string, fence int64) Publication {
		return Publication{a, model.PlatformArtifactRelease{ID: a.ID + "-release", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, Scope: a.Scope, ScopeKey: a.ScopeKey, Generation: a.Generation,
			ReleaseChannel: channel, Status: model.PlatformArtifactReleaseStatusActive, FencingToken: fence, LaneKey: platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, channel)}}
	}
	producer := pub(old, "shadow", 11)
	fullArtifact := testArtifact(t, "verified-full", model.PlatformArtifactKindReleaseSet, "global", "full-1", 6,
		platformconfig.ReleaseSet{Scope: "global", Generation: "full-1", ArtifactIDs: []string{"route-child", "dns-child", "caddy-child"}, ArtifactKinds: platformconfig.PublicationArtifactKinds("")})
	fullArtifact.Metadata = map[string]string{platformproducer.PolicyReleaseMetadata: producer.Release.ID,
		platformproducer.StaticIntentIDMetadata: static.ID, platformproducer.StaticIntentDigestMetadata: static.ContentHash,
		platformproducer.DNSPolicyIDMetadata: oldPolicy.ID, platformproducer.DNSPolicyDigestMetadata: oldPolicy.ContentHash}
	full := pub(fullArtifact, "full", 38)
	full.Release.ReleasedByType, full.Release.ReleasedByID = model.ActorTypeBootstrap, platformproducer.Actor
	full.Release.VerificationState, full.Release.VerifiedLKGGeneration = model.PlatformArtifactVerificationStateVerified, fullArtifact.Generation
	evidence, _ := platformconfig.Digest("fixture LKG evidence")
	lkg := model.PlatformLKGSnapshot{ID: "lkg-a", ArtifactID: fullArtifact.ID, ArtifactKind: fullArtifact.ArtifactKind, Scope: fullArtifact.Scope,
		ScopeKey: "global", SchemaVersion: fullArtifact.SchemaVersion, Generation: fullArtifact.Generation, GenerationSequence: fullArtifact.GenerationSequence,
		ContentHash: fullArtifact.ContentHash, ArtifactProvenance: fullArtifact.Provenance, SnapshotProvenance: fullArtifact.Provenance,
		VerifiedByReleaseID: full.Release.ID, VerificationEvidenceHash: evidence, ExpiresAt: now.Add(time.Hour)}
	s := Sources{Producer: producer, Full: full, LKG: lkg, Static: static, PreviousDNS: oldPolicy, NextDNS: nextPolicy, NextProducer: next}
	at := now.Add(-10 * time.Second)
	snapshot, _ := platformconfig.Digest("complete synthetic capture")
	before := Preview{PreviewMode: "runtime_summary", BusinessRevision: "business-revision-a", BusinessAt: at.Add(-time.Second), CapturedAt: at,
		Intent: platformconfig.PlatformIntent{Scope: "global", Generation: "intent-a"},
		Policy: platformconfig.PolicySnapshot{Scope: "global", Generation: "compiled-before", MinimumHealthyEdges: 1,
			DNSAnswerRules: []platformconfig.DNSAnswerRule{{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", TTLSeconds: 60, SelectionMode: "physical_order"}}},
		Runtime: platformconfig.RuntimeSnapshot{IntentGeneration: "intent-a", PolicyGeneration: "compiled-before", CapturedAt: &at,
			DNSEdgeEndpoints: []platformconfig.DNSEdgeEndpoint{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", A: []string{"8.8.8.8"}, ObservedAt: at}, {EdgeID: "edge-b", EdgeGroupID: "edge-group-a", A: []string{"9.9.9.9"}, ObservedAt: at}}},
		Omitted: []string{omittedEvidence}, Summary: &RuntimeSummary{Schema: summarySchema, FullSnapshotDigest: snapshot}}
	after := before
	after.Policy.Generation, after.Runtime.PolicyGeneration = "compiled-after", "compiled-after"
	after.Runtime.DNSEdgeEndpoints = before.Runtime.DNSEdgeEndpoints[1:]
	head := productioncontrol.ResourceState{Baseline: productioncontrol.Baseline{Scope: Scope("fixture"), Verified: productioncontrol.Version{Generation: 11, Digest: old.ContentHash}, EvidenceDigest: evidence},
		LastVerified: productioncontrol.Version{Generation: 11, Digest: old.ContentHash}}
	return s, before, after, head
}

func TestDNSDraftUsesExactProducerBoundary(t *testing.T) {
	now := time.Now().UTC()
	for _, scenario := range []string{"ok", "business", "policy", "held", "alias", "expired", "producer-fence", "full-release", "foreign-source", "journal", "pending", "bypass-policy", "missing-summary"} {
		t.Run(scenario, func(t *testing.T) {
			s, b, a, head := testSources(t, now)
			switch scenario {
			case "business":
				a.BusinessRevision = "different"
			case "policy":
				a.Policy.MinimumHealthyEdges++
			case "held":
				a.Runtime.DNSEdgeEndpoints = b.Runtime.DNSEdgeEndpoints
			case "alias":
				a.Runtime.DNSEdgeEndpoints[0].A = []string{"8.8.8.8"}
			case "expired":
				s.LKG.ExpiresAt = now
			case "producer-fence":
				s.Producer.Release.FencingToken++
			case "full-release":
				s.Full.Release.ID = "foreign"
			case "foreign-source":
				s.NextDNS.ID = "foreign"
			case "journal":
				head.LastVerified.Digest = s.NextProducer.ContentHash
			case "pending":
				head.Pending = s.NextProducer.ContentHash
			case "bypass-policy":
				s.NextDNS.Content["minimum_healthy_edges"] = float64(123)
				s.NextDNS.ContentHash, _ = platformconfig.Digest(s.NextDNS.Content)
				s.NextProducer.Content["dns_policy_digest"] = s.NextDNS.ContentHash
				s.NextProducer.ContentHash, _ = platformconfig.Digest(s.NextProducer.Content)
			case "missing-summary":
				a.Summary = nil
			}
			d, err := Draft("fixture", "one-change", "https://api.example.test", head, s, b, a, now)
			if (err == nil) != (scenario == "ok") {
				t.Fatalf("unexpected draft: %v", err)
			}
			if err != nil {
				return
			}
			p, err := Inspect(d)
			if err != nil || p.ChangedEdge != "edge-a" || p.AfterHold == nil || p.BeforeHold != nil ||
				d.Intent.ResourceID != Resource || d.Intent.Generation != 11 || d.Intent.CurrentDigest != s.Producer.Artifact.ContentHash ||
				d.Intent.TargetDigest != s.NextProducer.ContentHash || d.Intent.RollbackDigest != d.Intent.CurrentDigest {
				t.Fatalf("draft did not bind one producer CAS and its exact rollback: %+v %v", p, err)
			}
			bad := d
			bad.Intent.ResourceID = "producer/edge-a"
			if _, err := Inspect(bad); err == nil {
				t.Fatal("per-edge parallel execution boundary accepted")
			}
		})
	}
}

func TestDNSReaderUsesGETAndRechecksSelectedLKG(t *testing.T) {
	now := time.Now().UTC()
	s, b, a, _ := testSources(t, now)
	for _, scenario := range []string{"ok", "racing-producer", "racing-full", "racing-lkg", "redirect"} {
		t.Run(scenario, func(t *testing.T) {
			var producers, fulls, previews atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer fixture-reader" {
					t.Errorf("unexpected observation request %s", r.Method)
					http.Error(w, "no", http.StatusMethodNotAllowed)
					return
				}
				emit := func(x any) {
					w.Header().Set("Content-Type", "application/json")
					if err := json.NewEncoder(w).Encode(x); err != nil {
						t.Error(err)
					}
				}
				state := func(p Publication, lkg *model.PlatformLKGSnapshot) {
					emit(model.PlatformStateArtifactResponse{Artifact: &p.Artifact, Release: &p.Release, Generation: p.Artifact.Generation, LKG: lkg})
				}
				switch r.URL.Path {
				case "/v1/platform-state/artifacts/policy_snapshot":
					p := s.Producer
					if producers.Add(1) > 1 && scenario == "racing-producer" {
						p.Release.FencingToken++
					}
					state(p, nil)
				case "/v1/platform-state/artifacts/release_set":
					f, l := s.Full, s.LKG
					if fulls.Add(1) > 1 && scenario == "racing-full" {
						f.Release.FencingToken++
					}
					if fulls.Load() > 1 && scenario == "racing-lkg" {
						l.ExpiresAt = l.ExpiresAt.Add(time.Minute)
					}
					state(f, &l)
				case "/v1/admin/platform-config/routes/project":
					previews.Add(1)
					switch r.URL.Query().Get("producer_policy_artifact_id") {
					case s.Producer.Artifact.ID:
						emit(b)
					case s.NextProducer.ID:
						emit(a)
					default:
						http.NotFound(w, r)
					}
				default:
					if !strings.HasPrefix(r.URL.Path, "/v1/admin/artifacts/") {
						http.NotFound(w, r)
						return
					}
					id := strings.TrimPrefix(r.URL.Path, "/v1/admin/artifacts/")
					if scenario == "redirect" {
						http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
						return
					}
					for _, a := range []model.PlatformArtifact{s.Static, s.PreviousDNS, s.NextDNS, s.NextProducer} {
						if a.ID == id {
							emit(model.PlatformArtifactResponse{Artifact: a})
							return
						}
					}
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			reader, err := NewReader(server.URL, "fixture-reader")
			if err != nil {
				t.Fatal(err)
			}
			reader.client.Transport = server.Client().Transport // trust only the loopback fixture
			o, err := reader.Observe(context.Background(), s.NextProducer.ID, func() time.Time { return now })
			if (err == nil) != (scenario == "ok") {
				t.Fatalf("observation %s: %v", scenario, err)
			}
			if err == nil && (o.Sources.NextProducer.ID != s.NextProducer.ID || !reflect.DeepEqual(o.After.Runtime.DNSEdgeEndpoints, a.Runtime.DNSEdgeEndpoints) || producers.Load() != 2 || fulls.Load() != 2 || previews.Load() != 2) {
				t.Fatal("did not compare both exact selected publications around dry-run previews")
			}
		})
	}
}
