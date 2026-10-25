package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
	"fugue/internal/testfixture/celldns"
)

func dnsProducerServer(t *testing.T) (*Server, string, platformproducer.Policy, model.State) {
	t.Helper()
	_, s, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	keys := celldns.Keys()
	s.bundleSigningKey, s.bundleSigningKeyID = keys.PrimaryKey, keys.PrimaryKeyID
	req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	celldns.RetainGlobalOnly(&req)
	_, source := celldns.TransitionRequestWithSource(t)
	compiled, err := platformconfig.Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	previous := req.PreviousTrafficPublication
	state := model.State{}
	add := func(r model.PlatformArtifactRelease) {
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
		state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: r.LaneKey, ArtifactKind: r.ArtifactKind, ScopeKey: r.ScopeKey, ReleaseChannel: r.ReleaseChannel, ActiveReleaseID: r.ID, FencingToken: r.FencingToken, Version: 1})
	}
	for i, a := range policies {
		if a.ID == previous.ProducerPolicy.ArtifactID {
			state.PlatformArtifacts = append(state.PlatformArtifacts, a)
			add(activations[i])
		}
	}
	state.PlatformArtifacts = append(state.PlatformArtifacts, previous.Parent, previous.Route, previous.TLS, previous.DNS,
		celldns.Sign(t, compiled.IntentArtifact, "source-intent", 1), celldns.Sign(t, compiled.PolicyArtifact, "source-policy", 1))
	r := celldns.PreviousPublication(*previous)
	add(r)
	now := time.Now().UTC()
	lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{ID: "source-lkg", ArtifactID: previous.Parent.ID, ArtifactKind: previous.Parent.ArtifactKind, Scope: previous.Parent.Scope, ScopeKey: previous.Parent.ScopeKey, SchemaVersion: previous.Parent.SchemaVersion, Generation: previous.Parent.Generation, GenerationSequence: previous.Parent.GenerationSequence, ContentHash: previous.Parent.ContentHash, ArtifactProvenance: previous.Parent.Provenance, VerifiedByReleaseID: r.ID, VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64), ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}, keys)
	if err != nil {
		t.Fatal(err)
	}
	state.PlatformLKGSnapshots = append(state.PlatformLKGSnapshots, lkg)
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "dns-producer", PublicationRole: platformconfig.PublicationRoleCellDNS, AuthorityCellID: "cell-dns", TargetScope: "authority-cell:cell-dns", Mode: "shadow", InputSource: platformproducer.DNSProducerInputSource, IntervalSeconds: 30, RefreshSeconds: 120,
		DNSSource: &platformproducer.DNSProducerSource{ProducerPolicy: *previous.ProducerPolicy, PreviousTopology: req.Intent.RouteAuthorityTransition.PreviousTopology, DNSNodeIDs: []string{"dns-a"}}}
	raw, _ := json.Marshal(p)
	content := map[string]any{}
	json.Unmarshal(raw, &content)
	a := celldns.Sign(t, model.PlatformArtifact{SchemaVersion: model.PlatformArtifactSchemaVersionV1, ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{Key: "platform-config-producer:cell-dns"}, Generation: p.Generation, Content: content}, "dns-producer-policy", 1)
	state.PlatformArtifacts = append(state.PlatformArtifacts, a)
	add(model.PlatformArtifactRelease{ID: "dns-producer-activation", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, Generation: a.Generation, ReleaseChannel: "shadow", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now, LaneKey: platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, "shadow")})
	path := t.TempDir() + "/state.json"
	raw, _ = json.Marshal(state)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	s.store = store.New(path)
	s.store.ConfigurePlatformArtifactSigning(keys)
	if err := s.store.EnsurePlatformCompilerInput(compiled.InputSnapshot, compiled.Lineage.InputSnapshotDigest); err != nil {
		t.Fatal(err)
	}
	raw, _ = os.ReadFile(path)
	json.Unmarshal(raw, &state)
	// Any live Kubernetes/Edge read is a regression: capture uses immutable input.
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("DNS source capture performed live network observation")
		w.WriteHeader(503)
	}))
	t.Cleanup(kube.Close)
	s.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL}, nil
	}
	return s, path, p, state
}

func TestDNSProducerCaptureAndShadowAreIndependentAndReadOnlyForSources(t *testing.T) {
	s, path, p, before := dnsProducerServer(t)
	req, err := s.captureDNSProducer(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if req.RuntimeSnapshot.CapturedAt == nil || req.RuntimeSnapshot.CapturedAt.Year() != 2026 || req.RuntimeSnapshot.CapturedAt.Month() != time.August {
		t.Fatal("source timestamp was renewed")
	}
	if _, err := s.reconcilePlatformConfigurationScope(context.Background(), "platform-config-producer:cell-dns", nil); err != nil {
		t.Fatal(err)
	}
	a, r, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "shadow")
	if err != nil || !found || a.Content["publication_role"] != platformconfig.PublicationRoleCellDNS {
		t.Fatal("DNS shadow absent", err)
	}
	sets, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ArtifactReleaseID: r.ID})
	if err != nil || len(sets) != 1 || sets[0].ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || sets[0].RequiredCardinality != 1 {
		t.Fatal("DNS acquired other authority", sets, err)
	}
	if _, err := s.reconcilePlatformConfigurationScope(context.Background(), "platform-config-producer:cell-dns", nil); err != nil {
		t.Fatal(err)
	}
	_, repeat, _, _ := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "shadow")
	if repeat.ID != r.ID {
		t.Fatal("unchanged source republished")
	}
	var after model.State
	raw, _ := os.ReadFile(path)
	json.Unmarshal(raw, &after)
	if !reflect.DeepEqual(before.PlatformLKGSnapshots, after.PlatformLKGSnapshots) {
		t.Fatal("shadow changed positive LKG")
	}
	for _, lane := range before.PlatformReleaseLanes {
		found := false
		for _, current := range after.PlatformReleaseLanes {
			if lane.LaneKey == current.LaneKey {
				found = reflect.DeepEqual(lane, current)
			}
		}
		if !found {
			t.Fatal("source authority mutated")
		}
	}
}

func TestDNSProducerRefusesUnverifiedChangedOrUnavailableSource(t *testing.T) {
	for _, scenario := range []string{"source activation", "full fence", "unverified", "expired LKG", "corrupt LKG", "unsigned input", "missing input", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			s, path, p, state := dnsProducerServer(t)
			ctx := context.Background()
			switch scenario {
			case "source activation":
				state.PlatformReleaseLanes[0].FencingToken++
			case "full fence":
				state.PlatformReleaseLanes[1].FencingToken++
			case "unverified":
				state.PlatformArtifactReleases[1].VerificationState = model.PlatformArtifactVerificationStateServingUnverified
			case "expired LKG":
				state.PlatformLKGSnapshots[0].ExpiresAt = time.Now().Add(-time.Minute)
			case "corrupt LKG":
				state.PlatformLKGSnapshots[0].SnapshotProvenance.Signature = "corrupt"
			case "unsigned input":
				state.PlatformArtifacts[5].Provenance.Signature = "corrupt"
			case "missing input":
				state.PlatformArtifacts = append(state.PlatformArtifacts[:5], state.PlatformArtifacts[6:]...)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			raw, _ := json.Marshal(state)
			os.WriteFile(path, raw, 0600)
			_, err := s.captureDNSProducer(ctx, p)
			if err == nil {
				t.Fatal("untrusted source accepted")
			}
			after, _ := os.ReadFile(path)
			if string(raw) != string(after) {
				t.Fatal("failed capture mutated source")
			}
		})
	}
}
