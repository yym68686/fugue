package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
)

func availabilityTestBaseline() (platformStateArtifactEnvelope, model.PlatformPublicationPrecondition, *model.PlatformLKGSnapshot) {
	digest := "sha256:" + strings.Repeat("a", 64)
	a := &model.PlatformArtifact{ID: "baseline", ArtifactKind: "release_set", ScopeKey: "global", ContentHash: digest, Generation: "baseline-gen", Metadata: map[string]string{platformproducer.PolicyReleaseMetadata: "producer-release"}}
	r := &model.PlatformArtifactRelease{ID: "baseline-release", ArtifactID: a.ID, Status: "active", FencingToken: 7, VerificationState: "verified", VerifiedLKGGeneration: a.Generation, ReleasedByType: model.ActorTypeBootstrap, ReleasedByID: platformproducer.Actor}
	lkg := &model.PlatformLKGSnapshot{ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: "global", ContentHash: digest, Generation: a.Generation, VerifiedByReleaseID: r.ID, VerificationEvidenceHash: digest, ExpiresAt: time.Now().Add(time.Hour)}
	return platformStateArtifactEnvelope{Artifact: a, Release: r}, model.PlatformPublicationPrecondition{ReleaseID: "producer-release"}, lkg
}

func TestAvailabilityBaselineDoesNotInventRollbackVerification(t *testing.T) {
	full, authority, lkg := availabilityTestBaseline()
	if err := availabilityBaseline(full, authority, lkg); err != nil {
		t.Fatal(err)
	}
	full.Release.VerificationState = "serving_unverified"
	full.Release.PinnedRollbackGeneration = full.Artifact.Generation
	full.Release.RollbackTargetGeneration = full.Artifact.Generation
	lkg.VerifiedByReleaseID = "original-positive-release"
	if availabilityBaseline(full, authority, lkg) == nil {
		t.Fatal("accepted rollback without exact ledger")
	}
	full.Messages = []model.PlatformReleaseMessage{{MessageType: "rollback", ReleaseID: full.Release.ID, ArtifactID: full.Artifact.ID, ArtifactKind: full.Artifact.ArtifactKind, ScopeKey: "global", ReleaseChannel: "full", Generation: full.Artifact.Generation, CreatedAt: full.Release.ReleasedAt}}
	if err := availabilityBaseline(full, authority, lkg); err != nil {
		t.Fatal(err)
	}
	lkg.ExpiresAt = time.Now().Add(-time.Second)
	if availabilityBaseline(full, authority, lkg) == nil {
		t.Fatal("accepted expired LKG")
	}
}

func TestAvailabilitySourcePreservesConstraintsAndIsIdempotent(t *testing.T) {
	policy := model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
	content := map[string]any{"generation": "source", "offline_edges": []any{map[string]any{"edge_id": "edge-c", "reason": "quota_exhausted"}}, "dns_query_policy": map[string]any{"ordered_projection": map[string]any{"immutable": true}, "availability_routes": []platformconfig.DNSAvailabilityRoute{{Hostname: "other.example.test", Policy: policy}}}}
	original, _ := cloneAvailabilityContent(content)
	next, changed, err := availabilitySource(content, "app.example.test", policy)
	if err != nil || !changed {
		t.Fatal(changed, err)
	}
	if !reflect.DeepEqual(next["offline_edges"], original["offline_edges"]) {
		t.Fatal("offline holds changed")
	}
	normalized, _ := cloneAvailabilityContent(content)
	if !reflect.DeepEqual(original, normalized) {
		t.Fatal("original source mutated")
	}
	next, _ = cloneAvailabilityContent(next)
	_, changed, err = availabilitySource(next, "app.example.test", policy)
	if err != nil || changed {
		t.Fatal("identical retry changes source", err)
	}
}

// Exercise the actual command, signed-input API transaction shape and retry
// path. Configuration acceptance must never be mislabeled serving success.
func TestAvailabilityCommandTransactionAndRetry(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("preview_changed_%t", mismatch), func(t *testing.T) {
			full, authority, lkg := availabilityTestBaseline()
			digest := "sha256:" + strings.Repeat("a", 64)
			p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "producer-one", Mode: "serving", InputSource: "business-static-intent", TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 300, StaticIntentArtifactID: "static", StaticIntentDigest: digest, DNSPolicyArtifactID: "source", DNSPolicyDigest: digest, RequireApplicationDomains: true, RequireRouteDefaults: true, RequireDNSQueryPolicy: true, Serving: &platformproducer.ServingPolicy{CanaryRuleRef: "cohort=complete", GrayMinSeconds: 1, FullMinSeconds: 1, RolloutTimeoutSeconds: 60}}
			raw, _ := json.Marshal(p)
			var content map[string]any
			_ = json.Unmarshal(raw, &content)
			producer := model.PlatformArtifact{ID: "producer", ArtifactKind: "policy_snapshot", ScopeKey: platformproducer.Scope, Generation: p.Generation, ContentHash: digest, Status: "validated", Content: content}
			current := platformStateArtifactEnvelope{Artifact: &producer, Release: &model.PlatformArtifactRelease{ID: authority.ReleaseID, ArtifactID: producer.ID, Status: "active", FencingToken: 3}}
			source := model.PlatformArtifact{ID: "source", ArtifactKind: "policy_snapshot", ScopeKey: "global", Generation: "source-one", Status: "validated", ContentHash: digest, Content: map[string]any{"generation": "source-one", "offline_edges": []any{map[string]any{"edge_id": "edge-c", "reason": "quota_exhausted"}}, "dns_query_policy": map[string]any{"ranking_mode": "active"}}}
			artifacts := map[string]model.PlatformArtifact{"source": source, "producer": producer}
			creates, releases := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				path := r.URL.Path
				switch {
				case strings.HasPrefix(path, "/v1/platform-state/artifacts/"):
					if r.URL.Query().Get("scope_key") == platformproducer.Scope {
						writeJSONResponse(t, w, current)
					} else {
						writeJSONResponse(t, w, full)
					}
				case path == "/v1/admin/artifacts/baseline/lkg":
					writeJSONResponse(t, w, platformArtifactLKGEnvelope{LKG: lkg})
				case path == "/v1/admin/artifacts" && r.Method == "GET":
					list := []model.PlatformArtifact{}
					for _, artifact := range artifacts {
						if artifact.Generation == r.URL.Query().Get("generation") && artifact.ScopeKey == r.URL.Query().Get("scope") {
							list = append(list, artifact)
						}
					}
					writeJSONResponse(t, w, platformArtifactListEnvelope{Artifacts: list})
				case path == "/v1/admin/artifacts" && r.Method == "POST":
					var req model.PlatformArtifactCreateRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					creates++
					artifact := model.PlatformArtifact{ID: fmt.Sprintf("draft-%d", creates), ArtifactKind: req.ArtifactKind, ScopeKey: req.Scope.Key, Generation: req.Generation, Content: req.Content, Status: "draft", ContentHash: digest}
					artifacts[artifact.ID] = artifact
					writeJSONResponse(t, w, platformArtifactEnvelope{Artifact: artifact})
				case strings.HasSuffix(path, "/validate"):
					id := strings.TrimSuffix(strings.TrimPrefix(path, "/v1/admin/artifacts/"), "/validate")
					artifact := artifacts[id]
					artifact.Status = "validated"
					artifacts[id] = artifact
					writeJSONResponse(t, w, platformArtifactValidationEnvelope{Artifact: artifact, Pass: true})
				case path == "/v1/admin/platform-config/routes/project":
					intent := map[string]any{"scope": "global", "routes": []any{}}
					if mismatch && r.URL.Query().Get("producer_policy_artifact_id") != producer.ID {
						intent["scope"] = "changed"
					}
					writeJSONResponse(t, w, map[string]any{"preview_mode": r.URL.Query().Get("view"), "business_snapshot_revision": "synthetic-revision", "intent": intent, "issues": []any{}, "runtime_summary": map[string]any{"schema": "fugue.runtime-preview-summary/v1", "full_snapshot_digest": digest}, "policy": map[string]any{"dns_answer_rules": []any{map[string]any{"hostname": "app.example.test"}}}, "runtime_snapshot": map[string]any{"facts": map[string]any{"dynamic_quality_coverage": map[string]any{"mode": "all_dynamic", "query_count": 1}}, "dns_selections": []any{map[string]any{"hostname": "app.example.test", "node_id": "dns-a", "candidates": []any{map[string]any{"edge_id": "edge-a"}}}}}})
				case strings.HasSuffix(path, "/release"):
					var req model.PlatformArtifactReleaseRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					if req.ReleaseChannel != "shadow" || req.ProducerReconfiguration == nil || req.ProducerReconfiguration.Operation != "dns_availability_recovery" || req.SoftOverride || req.ForcePublish || req.KernelBreakGlass != nil || !strings.HasPrefix(req.IdempotencyKey, "producer-reconfiguration/sha256:") {
						t.Error("unsafe or incomplete transaction", req)
					}
					previousRef, _ := availabilityPublication(current)
					fullRef, _ := availabilityPublication(full)
					if req.ProducerReconfiguration == nil || req.ProducerReconfiguration.PreviousPolicy != previousRef || req.ProducerReconfiguration.ServingFull != fullRef || req.ProducerReconfiguration.VerificationEvidenceHash != lkg.VerificationEvidenceHash {
						t.Error("transaction lost exact producer/full/LKG fences")
					}
					id := strings.TrimSuffix(strings.TrimPrefix(path, "/v1/admin/artifacts/"), "/release")
					artifact := artifacts[id]
					releases++
					current = platformStateArtifactEnvelope{Artifact: &artifact, Release: &model.PlatformArtifactRelease{ID: "successor-release", ArtifactID: id, Status: "active", FencingToken: 4}}
					writeJSONResponse(t, w, platformArtifactReleaseEnvelope{Artifact: artifact, Release: *current.Release})
				case strings.HasPrefix(path, "/v1/admin/artifacts/"):
					writeJSONResponse(t, w, platformArtifactEnvelope{Artifact: artifacts[strings.TrimPrefix(path, "/v1/admin/artifacts/")]})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			args := []string{"--base-url", server.URL, "--token", "synthetic-key", "-o", "json", "admin", "dns", "availability", "set", "app.example.test", "--edge", "edge-a", "--edge", "edge-b", "--wait=false"}
			var stdout, stderr bytes.Buffer
			if err := runWithStreams(append(slicesClone(args), "--dry-run"), &stdout, &stderr); err != nil {
				t.Fatal(err)
			}
			if creates != 0 || releases != 0 {
				t.Fatal("dry run performed a write")
			}
			stdout.Reset()
			err := runWithStreams(args, &stdout, &stderr)
			if mismatch {
				if err == nil || releases != 0 {
					t.Fatal("changed preview was activated", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err, stderr.String())
			}
			var result dnsAvailabilityResult
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if !result.ProducerActivated || result.RoutingAcceptanceComplete || creates != 2 || releases != 1 {
				t.Fatal("configuration was mislabeled or duplicated", result, creates, releases)
			}
			stdout.Reset()
			if err := runWithStreams(args, &stdout, &stderr); err != nil {
				t.Fatal(err)
			}
			if creates != 2 || releases != 1 {
				t.Fatal("retry duplicated configuration")
			}
			client, _ := NewClient(server.URL, "synthetic-key")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			client.context = ctx
			cli := &CLI{stderr: &stderr}
			_, err = cli.setDNSAvailability(ctx, client, "app.example.test", result.Policy, dnsAvailabilityOptions{Wait: true})
			if err == nil {
				t.Fatal("missing serving publication reported success")
			}
		})
	}
}

func slicesClone(values []string) []string { return append([]string(nil), values...) }

func TestAvailabilityTransportRetainsTLSVerificationAndRejectsRedirects(t *testing.T) {
	var received atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		writeJSONResponse(t, w, map[string]any{"ok": true})
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer redirect.Close()
	client, _ := NewClient(redirect.URL, "synthetic-key")
	if err := configureAvailabilityTransport(client, ""); err != nil {
		t.Fatal(err)
	}
	if err := client.doJSON("GET", "/", nil, nil); err == nil || received.Load() != 0 {
		t.Fatal("redirect leaked privileged API request", err)
	}
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1) }))
	defer tlsServer.Close()
	client, _ = NewClient(tlsServer.URL, "synthetic-key")
	if err := configureAvailabilityTransport(client, "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	if err := client.doJSON("GET", "/", nil, nil); err == nil || !strings.Contains(err.Error(), "certificate") || received.Load() != 0 {
		t.Fatal("explicit IP bypassed certificate verification", err)
	}
	if configureAvailabilityTransport(client, "not-an-IP") == nil {
		t.Fatal("accepted non-IP override")
	}
}
