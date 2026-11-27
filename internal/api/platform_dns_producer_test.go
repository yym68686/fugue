package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/testfixture/celldns"
)

func dnsProducerServer(t *testing.T) (*Server, *sql.DB, platformproducer.Policy, platformPublicationFixture) {
	t.Helper()
	_, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	keys := celldns.Keys()
	s.bundleSigningKey, s.bundleSigningKeyID = keys.PrimaryKey, keys.PrimaryKeyID
	req, policies, activations, inputs, sources := celldns.CompletePublication(t, true, true)
	celldns.RetainGlobalOnly(&req)
	previous := req.PreviousTrafficPublication
	var compiled platformconfig.CompileResult
	for _, input := range inputs {
		if input.Lineage.InputSnapshotDigest == previous.Parent.Metadata["input_snapshot_digest"] {
			compiled = input
		}
	}
	if compiled.Lineage.InputSnapshotDigest == "" {
		t.Fatal("actual normalized global compiler input missing")
	}
	state := platformPublicationFixture{}
	add := func(r model.PlatformArtifactRelease) {
		state.releases = append(state.releases, r)
		state.lanes = append(state.lanes, model.PlatformReleaseLane{LaneKey: r.LaneKey, ArtifactKind: r.ArtifactKind, ScopeKey: r.ScopeKey, ReleaseChannel: r.ReleaseChannel, ActiveReleaseID: r.ID, FencingToken: r.FencingToken, Version: 1})
	}
	for i, a := range policies {
		if a.ID == previous.ProducerPolicy.ArtifactID {
			state.artifacts = append(state.artifacts, a)
			add(activations[i])
		}
	}
	state.artifacts = append(state.artifacts, previous.Parent, previous.Route, previous.TLS, previous.DNS)
	state.artifacts = append(state.artifacts, sources...)
	r := celldns.PreviousPublication(*previous)
	add(r)
	now := time.Now().UTC()
	lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{ID: "source-lkg", ArtifactID: previous.Parent.ID, ArtifactKind: previous.Parent.ArtifactKind, Scope: previous.Parent.Scope, ScopeKey: previous.Parent.ScopeKey, SchemaVersion: previous.Parent.SchemaVersion, Generation: previous.Parent.Generation, GenerationSequence: previous.Parent.GenerationSequence, ContentHash: previous.Parent.ContentHash, ArtifactProvenance: previous.Parent.Provenance, VerifiedByReleaseID: r.ID, VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64), ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}, keys)
	if err != nil {
		t.Fatal(err)
	}
	state.lkgs = append(state.lkgs, lkg)
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "dns-producer", PublicationRole: platformconfig.PublicationRoleCellDNS, AuthorityCellID: "cell-dns", TargetScope: "authority-cell:cell-dns", Mode: "shadow", InputSource: platformproducer.DNSProducerInputSource, IntervalSeconds: 30, RefreshSeconds: 120,
		DNSSource: &platformproducer.DNSProducerSource{ProducerPolicy: *previous.ProducerPolicy, PreviousTopology: req.Intent.RouteAuthorityTransition.PreviousTopology, DNSNodeIDs: []string{"dns-a"}}}
	raw, _ := json.Marshal(p)
	content := map[string]any{}
	json.Unmarshal(raw, &content)
	a := celldns.Sign(t, model.PlatformArtifact{SchemaVersion: model.PlatformArtifactSchemaVersionV1, ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{Key: "platform-config-producer:cell-dns"}, Generation: p.Generation, Content: content}, "dns-producer-policy", 1)
	state.artifacts = append(state.artifacts, a)
	add(model.PlatformArtifactRelease{ID: "dns-producer-activation", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, Generation: a.Generation, ReleaseChannel: "shadow", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now, LaneKey: platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, "shadow")})
	const fixture = "dns-producer"
	s.store = newStoreTestDatabase(t, fixture)
	s.store.ConfigurePlatformArtifactSigning(keys)
	db := openStoreTestSQL(t, fixture)
	state.seed(t, db)
	for _, input := range inputs {
		if err := s.store.EnsurePlatformCompilerInput(input.InputSnapshot, input.Lineage.InputSnapshotDigest); err != nil {
			t.Fatal(err)
		}
	}
	// Any live Kubernetes/Edge read is a regression: capture uses immutable input.
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("DNS source capture performed live network observation")
		w.WriteHeader(503)
	}))
	t.Cleanup(kube.Close)
	s.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL}, nil
	}
	return s, db, p, state
}

func TestDNSProducerWarmsOnlyExactPersistedOutput(t *testing.T) {
	s, _, policy, _ := dnsProducerServer(t)
	if _, err := s.reconcilePlatformConfigurationScope(t.Context(), "platform-config-producer:cell-dns", nil); err != nil {
		t.Fatal(err)
	}
	parent, _, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, policy.TargetScope, "shadow")
	if err != nil || !found {
		t.Fatal("missing fixture shadow", err)
	}
	child, err := s.consumerAssignmentChild(parent, model.PlatformArtifactKindDNSAnswerBundle)
	if err != nil {
		t.Fatal(err)
	}
	policy.Generation = model.NewID("warm-dns-producer") // A fresh cache key for this validation.
	s.warmDNSProducerPublication(t.Context(), policy, parent)
	var sawHit, decoded bool
	observe := func(stage platformproducer.DNSOutputValidationStage, _ time.Duration, result platformproducer.DNSOutputCacheResult) {
		sawHit = sawHit || result == platformproducer.DNSOutputCacheHit
		decoded = decoded || stage == platformproducer.DNSOutputChildDecode
	}
	if err := platformproducer.ValidateDNSProducerOutputObserved(policy, child, observe); err != nil || !sawHit || decoded {
		t.Fatal("persisted DNS output was not warmed for the transaction", sawHit, decoded, err)
	}
	sawHit, decoded = false, false
	child.Content["generation"] = "unapproved-generation-with-original-claimed-hash"
	if err := platformproducer.ValidateDNSProducerOutputObserved(policy, child, observe); err == nil || sawHit || !decoded {
		t.Fatal("warm cache masked changed content", sawHit, decoded, err)
	}
}

func TestDNSProducerRefusesUnverifiedChangedOrUnavailableSource(t *testing.T) {
	for _, scenario := range []string{"source activation", "full fence", "unverified", "expired LKG", "corrupt LKG", "unsigned input", "missing input", "missing producer dependency", "missing runtime input", "corrupt runtime input", "runtime input size", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			s, db, p, state := dnsProducerServer(t)
			ctx := context.Background()
			if _, err := s.captureDNSProducer(ctx, p); err != nil {
				t.Fatalf("uncorrupted source must pass: %v", err)
			}
			edit := func(query string, args ...any) {
				t.Helper()
				result, err := db.Exec(query, args...)
				if err != nil {
					t.Fatal(err)
				}
				if n, err := result.RowsAffected(); err != nil || n != 1 {
					t.Fatalf("expected one changed source record, got %d: %v", n, err)
				}
			}
			parent, err := s.store.GetPlatformArtifact(state.releases[1].ArtifactID)
			if err != nil {
				t.Fatal(err)
			}
			digest := parent.Metadata["runtime_input_snapshot_digest"]
			if digest == "" {
				digest = parent.Metadata["input_snapshot_digest"]
			}
			switch scenario {
			case "source activation":
				edit(`UPDATE fugue_platform_release_lanes SET fencing_token=fencing_token+1 WHERE lane_key=$1`, state.lanes[0].LaneKey)
			case "full fence":
				edit(`UPDATE fugue_platform_release_lanes SET fencing_token=fencing_token+1 WHERE lane_key=$1`, state.lanes[1].LaneKey)
			case "unverified":
				edit(`UPDATE fugue_platform_artifact_releases SET verification_state=$2 WHERE id=$1`, state.releases[1].ID, model.PlatformArtifactVerificationStateServingUnverified)
			case "expired LKG":
				edit(`UPDATE fugue_platform_lkg_snapshots SET expires_at=now()-interval '1 minute' WHERE id=$1`, state.lkgs[0].ID)
			case "corrupt LKG":
				edit(`UPDATE fugue_platform_lkg_snapshots SET snapshot_provenance_json=jsonb_set(snapshot_provenance_json,'{signature}','"corrupt"') WHERE id=$1`, state.lkgs[0].ID)
			case "unsigned input", "missing input":
				input, err := s.store.GetPlatformArtifactByIdentity(model.PlatformArtifactKindPlatformIntent, "global", parent.Metadata["intent_generation"])
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "unsigned input" {
					edit(`UPDATE fugue_platform_artifacts SET provenance_json=jsonb_set(provenance_json,'{signature}','"corrupt"') WHERE id=$1`, input.ID)
				} else {
					edit(`DELETE FROM fugue_platform_artifacts WHERE id=$1`, input.ID)
				}
			case "missing producer dependency":
				producer, err := s.store.GetPlatformArtifact(p.DNSSource.ProducerPolicy.ArtifactID)
				if err != nil {
					t.Fatal(err)
				}
				policy, err := platformproducer.Decode(producer)
				if err != nil {
					t.Fatal(err)
				}
				edit(`DELETE FROM fugue_platform_artifacts WHERE id=$1`, policy.DNSPolicyArtifactID)
			case "missing runtime input":
				edit(`DELETE FROM fugue_platform_input_hot_chunks WHERE content_hash=$1 AND ordinal=0`, digest)
			case "corrupt runtime input":
				edit(`UPDATE fugue_platform_input_hot_chunks SET data=decode('00','hex') WHERE content_hash=$1 AND ordinal=0`, digest)
			case "runtime input size":
				edit(`UPDATE fugue_platform_input_manifests SET size_bytes=size_bytes+1 WHERE content_hash=$1`, digest)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			before := convergenceStoreFacts(t, db)
			if _, err := s.captureDNSProducer(ctx, p); err == nil {
				t.Fatal("untrusted source accepted")
			}
			if after := convergenceStoreFacts(t, db); !reflect.DeepEqual(before, after) {
				t.Fatal("failed capture mutated source")
			}
		})
	}
}
