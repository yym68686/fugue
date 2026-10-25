package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	neturl "net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
	"fugue/internal/testfixture/celldns"
)

func TestDNSRouteSourceAPIRequiresExactPrivateDNSAssignment(t *testing.T) {
	_, s, tenant, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	keys := celldns.Keys()
	s.bundleSigningKey, s.bundleSigningKeyID = keys.PrimaryKey, keys.PrimaryKeyID
	s.auth.PlatformComponentIdentityKeyring = edgeRouteIntentTestKeyring()
	req, policies, policyReleases := celldns.SourceAuthorizedRequest(t, true)
	compiled := celldns.Compile(t, req)
	state := model.State{PlatformArtifacts: policies, PlatformArtifactReleases: policyReleases}
	add := func(r model.PlatformArtifactRelease) {
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
		state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: r.LaneKey, ArtifactKind: r.ArtifactKind, ScopeKey: r.ScopeKey, ReleaseChannel: r.ReleaseChannel, ActiveReleaseID: r.ID, FencingToken: r.FencingToken, Version: 1})
	}
	for _, p := range req.CellRoutePublications {
		state.PlatformArtifacts = append(state.PlatformArtifacts, p.Parent, p.Route, p.TLS)
		add(celldns.Publication(p))
	}
	p := req.PreviousTrafficPublication
	state.PlatformArtifacts = append(state.PlatformArtifacts, p.Parent, p.Route, p.TLS, p.DNS)
	add(celldns.PreviousPublication(*p))
	dnsRelease := model.PlatformArtifactRelease{ID: "dns-shadow", ArtifactID: compiled.ReleaseArtifact.ID, ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: compiled.ReleaseArtifact.ScopeKey, Generation: compiled.ReleaseArtifact.Generation, ReleaseChannel: "shadow", Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 1, ReleasedAt: time.Now().Add(-time.Minute), LaneKey: platformsafety.ReleaseLaneKey(model.PlatformArtifactKindReleaseSet, compiled.ReleaseArtifact.ScopeKey, "shadow")}
	add(dnsRelease)
	state.PlatformArtifacts = append(state.PlatformArtifacts, compiled.ReleaseArtifact, compiled.DNSArtifact)
	topology, _, err := platformcontrol.DeclaredTrafficConsumerTopology(compiled.ReleaseArtifact)
	if err != nil {
		t.Fatal(err)
	}
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: compiled.ReleaseArtifact.ID, ArtifactReleaseID: dnsRelease.ID, ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, Scope: compiled.DNSArtifact.Scope, ScopeKey: compiled.DNSArtifact.ScopeKey, Generation: compiled.DNSArtifact.Generation, Revision: 1, PreparedAt: time.Now().UTC(), Topology: topology})
	if err != nil {
		t.Fatal(err)
	}
	state.ExpectedConsumerSets = append(state.ExpectedConsumerSets, set)
	path := t.TempDir() + "/state.json"
	raw, _ := json.Marshal(state)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	s.store = store.New(path)
	s.store.ConfigurePlatformArtifactSigning(keys)
	issue := func(component, node, authority, kind string) string {
		t.Helper()
		token, err := platformcontrol.IssuePlatformComponentIdentity(edgeRouteIntentTestKeyring(), platformcontrol.PlatformComponentIdentityClaims{CredentialID: "kubernetes:test-system:dns-account:pod-a", Component: component, NodeID: node, AuthorityID: authority, ScopeKey: "authority-cell:" + authority, ArtifactKinds: []string{kind}}, time.Now().UTC(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	token := issue(model.PlatformConsumerComponentDNSServer, "dns-a", "cell-dns", model.PlatformArtifactKindDNSAnswerBundle)
	base := "/v1/platform-state/consumers/artifacts/" + compiled.DNSArtifact.ID + "/route-sources"
	url := base + "?expected_consumer_set_id=" + set.ID
	read := func(token, url string, code int) model.PlatformConsumerDNSRouteSourcesResponse {
		t.Helper()
		res := performJSONRequest(t, s, http.MethodGet, url, token, nil)
		if res.Code != code {
			t.Fatalf("route source read %d want %d: %s", res.Code, code, res.Body.String())
		}
		var out model.PlatformConsumerDNSRouteSourcesResponse
		if code == http.StatusOK {
			mustDecodeJSON(t, res, &out)
			if res.Header().Get("Cache-Control") != "private, no-store" || res.Header().Get("ETag") != "\""+out.Snapshot.SelectionDigest+"\"" {
				t.Fatal("observation caching or digest binding lost")
			}
		} else if strings.Contains(res.Body.String(), policies[0].ID) {
			t.Fatal("unauthorized response disclosed a source")
		}
		return out
	}
	first := read(token, url, http.StatusOK)
	if first.Assignment.ExpectedConsumerSetID != set.ID || first.Assignment.ArtifactID != compiled.DNSArtifact.ID || len(first.Snapshot.Scopes) != 3 {
		t.Fatal("source response lost exact DNS assignment")
	}
	read(tenant, url, http.StatusUnauthorized)
	read(admin, url, http.StatusUnauthorized)
	read(issue(model.PlatformConsumerComponentEdgeWorker, "edge-a", "cell-a", model.PlatformArtifactKindEdgeRouteBundle), url, http.StatusForbidden)
	read(issue(model.PlatformConsumerComponentDNSServer, "dns-foreign", "cell-dns", model.PlatformArtifactKindDNSAnswerBundle), url, http.StatusNotFound)
	read(issue(model.PlatformConsumerComponentDNSServer, "dns-a", "cell-other", model.PlatformArtifactKindDNSAnswerBundle), url, http.StatusNotFound)
	read(token, base, http.StatusBadRequest)
	read(token, url+"&expected_consumer_set_id="+set.ID, http.StatusBadRequest)
	read(token, base+"?expected_consumer_set_id=foreign", http.StatusNotFound)
	read(token, strings.Replace(url, compiled.DNSArtifact.ID, req.CellRoutePublications[0].Parent.ID, 1), http.StatusNotFound)
	after, _ := os.ReadFile(path)
	if string(after) != string(raw) {
		t.Fatal("private read mutated configuration, runtime facts or LKG")
	}
	for i, channel := range []string{"gray", "full"} {
		serving := dnsRelease
		serving.ID, serving.ReleaseChannel = "dns-"+channel, channel
		serving.ReleasedAt = time.Now().Add(time.Duration(i-2) * time.Second)
		serving.LaneKey = platformsafety.ReleaseLaneKey(serving.ArtifactKind, serving.ScopeKey, channel)
		if channel == "gray" {
			serving.CanaryRuleRef = "cohort=complete"
		}
		add(serving)
		next, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: compiled.ReleaseArtifact.ID, ArtifactReleaseID: serving.ID, ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, Scope: compiled.DNSArtifact.Scope, ScopeKey: compiled.DNSArtifact.ScopeKey, Generation: compiled.DNSArtifact.Generation, Revision: int64(i + 2), PreparedAt: time.Now().UTC(), Topology: topology})
		if err != nil {
			t.Fatal(err)
		}
		state.ExpectedConsumerSets = append(state.ExpectedConsumerSets, next)
		raw, _ = json.Marshal(state)
		os.WriteFile(path, raw, 0600)
		url = base + "?expected_consumer_set_id=" + next.ID
		selected := read(token, url, http.StatusOK)
		if selected.Release.ID != serving.ID {
			t.Fatal("DNS-only serving source read selected another release")
		}
		assignment := performJSONRequest(t, s, http.MethodGet, "/v1/platform-state/consumers/assignment?serving_only=true", token, nil)
		if assignment.Code != http.StatusOK {
			t.Fatalf("DNS-only serving assignment failed: %d %s", assignment.Code, assignment.Body.String())
		}
		var response model.PlatformConsumerAssignmentResponse
		mustDecodeJSON(t, assignment, &response)
		if len(response.Assignments) != 1 || response.Assignments[0].ArtifactReleaseID != serving.ID || response.Assignments[0].ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle {
			t.Fatal("DNS-only authority acquired route membership or selected the wrong release")
		}
	}
	// Superseding the DNS assignment revokes this artifact's source access even
	// though its signed contents and routing sources remain valid.
	state.PlatformArtifactReleases[len(state.PlatformArtifactReleases)-1].Status = model.PlatformArtifactReleaseStatusSuperseded
	raw, _ = json.Marshal(state)
	os.WriteFile(path, raw, 0600)
	read(token, url, http.StatusNotFound)

	// A positive full DNS LKG remains an independent source-read authority after
	// that release is superseded by a pending gray successor.
	state.PlatformArtifactReleases[len(state.PlatformArtifactReleases)-1].VerificationState = model.PlatformArtifactVerificationStateVerified
	state.PlatformArtifactReleases[len(state.PlatformArtifactReleases)-1].VerifiedLKGGeneration = compiled.ReleaseArtifact.Generation
	verifiedAt := time.Now().UTC()
	state.PlatformArtifactReleases[len(state.PlatformArtifactReleases)-1].VerifiedAt = &verifiedAt
	lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{ID: "dns-retained-lkg", ArtifactID: compiled.ReleaseArtifact.ID, ArtifactKind: compiled.ReleaseArtifact.ArtifactKind, Scope: compiled.ReleaseArtifact.Scope, ScopeKey: compiled.ReleaseArtifact.ScopeKey, SchemaVersion: compiled.ReleaseArtifact.SchemaVersion, Generation: compiled.ReleaseArtifact.Generation, GenerationSequence: compiled.ReleaseArtifact.GenerationSequence, ContentHash: compiled.ReleaseArtifact.ContentHash, ArtifactProvenance: compiled.ReleaseArtifact.Provenance, VerifiedByReleaseID: state.PlatformArtifactReleases[len(state.PlatformArtifactReleases)-1].ID, VerificationEvidenceHash: "sha256:" + strings.Repeat("b", 64), ExpiresAt: time.Now().Add(time.Hour), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}, keys)
	if err != nil {
		t.Fatal(err)
	}
	state.PlatformLKGSnapshots = append(state.PlatformLKGSnapshots, lkg)
	raw, _ = json.Marshal(state)
	os.WriteFile(path, raw, 0600)
	retainedURL := "/v1/platform-state/consumers/artifacts/" + compiled.DNSArtifact.ID + "/retained-route-sources?expected_consumer_set_id=" + neturl.QueryEscape(state.ExpectedConsumerSets[len(state.ExpectedConsumerSets)-1].ID)
	retainedResponse := performJSONRequest(t, s, http.MethodGet, retainedURL, token, nil)
	if retainedResponse.Code != http.StatusOK {
		t.Fatalf("retained positive DNS LKG source read denied: %d %s", retainedResponse.Code, retainedResponse.Body.String())
	}
	var retained model.PlatformConsumerRetainedDNSRouteSourcesResponse
	mustDecodeJSON(t, retainedResponse, &retained)
	if retained.RetainedLKG.ID != lkg.ID || retained.Release.ID != lkg.VerifiedByReleaseID || retained.Assignment.ReleaseChannel != "full" {
		t.Fatal("retained response lost positive full LKG binding")
	}
	baseline, _ := json.Marshal(state)
	for _, scenario := range []string{"valid", "corrupt LKG", "expired LKG", "missing LKG", "foreign parent", "rolled back", "unverified", "wrong set", "foreign node", "foreign authority", "non DNS"} {
		t.Run("retained/"+scenario, func(t *testing.T) {
			var next model.State
			if err := json.Unmarshal(baseline, &next); err != nil {
				t.Fatal(err)
			}
			requestURL, requestToken := retainedURL, token
			code := http.StatusNotFound
			switch scenario {
			case "valid":
				code = http.StatusOK
			case "corrupt LKG":
				next.PlatformLKGSnapshots[0].SnapshotProvenance.Signature = "invalid"
			case "expired LKG":
				next.PlatformLKGSnapshots[0].ExpiresAt = time.Now().Add(-time.Minute)
				next.PlatformLKGSnapshots[0], err = platformsafety.SignPlatformLKGSnapshot(next.PlatformLKGSnapshots[0], keys)
				if err != nil {
					t.Fatal(err)
				}
			case "missing LKG":
				next.PlatformLKGSnapshots = nil
			case "foreign parent":
				next.PlatformLKGSnapshots[0].ArtifactID = "other-parent"
			case "rolled back":
				next.PlatformArtifactReleases[len(next.PlatformArtifactReleases)-1].Status = model.PlatformArtifactReleaseStatusRolledBack
			case "unverified":
				next.PlatformArtifactReleases[len(next.PlatformArtifactReleases)-1].VerificationState = "pending"
			case "wrong set":
				requestURL = strings.Replace(requestURL, next.ExpectedConsumerSets[len(next.ExpectedConsumerSets)-1].ID, "foreign", 1)
			case "foreign node":
				requestToken = issue(model.PlatformConsumerComponentDNSServer, "foreign-node", "cell-dns", model.PlatformArtifactKindDNSAnswerBundle)
			case "foreign authority":
				requestToken = issue(model.PlatformConsumerComponentDNSServer, "dns-a", "cell-other", model.PlatformArtifactKindDNSAnswerBundle)
			case "non DNS":
				requestToken = issue(model.PlatformConsumerComponentEdgeWorker, "edge-a", "cell-dns", model.PlatformArtifactKindEdgeRouteBundle)
				code = http.StatusForbidden
			}
			encoded, _ := json.Marshal(next)
			if err := os.WriteFile(path, encoded, 0600); err != nil {
				t.Fatal(err)
			}
			response := performJSONRequest(t, s, http.MethodGet, requestURL, requestToken, nil)
			if response.Code != code {
				t.Fatalf("retained authorization: %d want %d %s", response.Code, code, response.Body.String())
			}
			after, _ := os.ReadFile(path)
			if !reflect.DeepEqual(encoded, after) {
				t.Fatal("retained read modified production configuration or runtime state")
			}
		})
	}

}

func TestDNSRouteSourceReadRechecksAssignmentAfterObservation(t *testing.T) {
	for _, scenario := range []string{"unchanged", "fence", "expected set", "release", "revoked", "foreign snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			current := consumerArtifactLookup{Artifact: model.PlatformArtifact{ID: "dns", ContentHash: "digest"}, Assignment: model.PlatformConsumerAssignment{ArtifactID: "dns", FencingToken: 1, ExpectedConsumerSetID: "expected"}, Release: model.PlatformArtifactRelease{ID: "release"}}
			revoked := false
			lookup := func() (consumerArtifactLookup, error) {
				if revoked {
					return consumerArtifactLookup{}, store.ErrNotFound
				}
				return current, nil
			}
			observe := func(context.Context, model.PlatformArtifact) (model.PlatformDNSRouteSourceSnapshot, error) {
				snapshot := model.PlatformDNSRouteSourceSnapshot{DNSArtifactID: "dns", DNSArtifactDigest: "digest"}
				switch scenario {
				case "fence":
					current.Assignment.FencingToken++
				case "expected set":
					current.Assignment.ExpectedConsumerSetID = "replacement"
				case "release":
					current.Release.ID = "next-release"
				case "revoked":
					revoked = true
				case "foreign snapshot":
					snapshot.DNSArtifactID = "foreign"
				}
				return snapshot, nil
			}
			response, err := observeAssignedDNSRouteSources(context.Background(), lookup, observe)
			if scenario == "unchanged" {
				if err != nil || response.Snapshot.DNSArtifactID != "dns" {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, store.ErrConflict) || response.Snapshot.DNSArtifactID != "" {
				t.Fatal("changed assignment disclosed source snapshot", err)
			}
		})
	}
}
