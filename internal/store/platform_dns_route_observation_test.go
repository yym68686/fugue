package store

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/testfixture/celldns"
)

func dnsSourceObservationFixture(t *testing.T, transition bool) (model.State, model.PlatformArtifact) {
	t.Helper()
	req, policies, policyReleases := celldns.SourceAuthorizedRequest(t, transition)
	compiled := celldns.Compile(t, req)
	state := model.State{PlatformArtifacts: policies, PlatformArtifactReleases: policyReleases}
	add := func(parent, route, tls model.PlatformArtifact, r model.PlatformArtifactRelease) {
		state.PlatformArtifacts = append(state.PlatformArtifacts, parent, route, tls)
		r.VerificationState = model.PlatformArtifactVerificationStateVerified
		r.VerifiedLKGGeneration = parent.Generation
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
		state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: r.LaneKey, ArtifactKind: r.ArtifactKind, ScopeKey: r.ScopeKey, ReleaseChannel: r.ReleaseChannel, ActiveReleaseID: r.ID, FencingToken: r.FencingToken, Version: 1})
		lkg, err := buildPlatformLKGSnapshot(parent, r.ID, "sha256:"+strings.Repeat("a", 64), time.Now().UTC(), celldns.Keys())
		if err != nil {
			t.Fatal(err)
		}
		state.PlatformLKGSnapshots = append(state.PlatformLKGSnapshots, lkg)
	}
	for _, p := range req.CellRoutePublications {
		add(p.Parent, p.Route, p.TLS, celldns.Publication(p))
	}
	if p := req.PreviousTrafficPublication; p != nil {
		add(p.Parent, p.Route, p.TLS, celldns.PreviousPublication(*p))
	}
	return state, compiled.DNSArtifact
}

func TestDNSRouteSourceObservationIsBoundedReadOnlyAndSelectionSensitive(t *testing.T) {
	for _, transition := range []bool{false, true} {
		state, child := dnsSourceObservationFixture(t, transition)
		path := t.TempDir() + "/state.json"
		raw, _ := json.Marshal(state)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		s := New(path)
		s.ConfigurePlatformArtifactSigning(celldns.Keys())
		first, err := s.ObserveDNSRouteSources(context.Background(), child)
		if err != nil {
			t.Fatal(err)
		}
		if err := dnsroutesource.VerifySnapshot(first, child, celldns.Keys(), time.Now()); err != nil {
			t.Fatal("consumer rejected authentic source observation", err)
		}
		for _, scenario := range []string{"scope", "duplicate scope", "lane fence", "unselected release", "policy signature", "forged LKG", "DNS identity"} {
			raw, _ := json.Marshal(first)
			var forged model.PlatformDNSRouteSourceSnapshot
			json.Unmarshal(raw, &forged)
			switch scenario {
			case "scope":
				forged.Scopes[0].ScopeKey = "authority-cell:cell-foreign"
			case "duplicate scope":
				forged.Scopes[1] = forged.Scopes[0]
			case "lane fence":
				forged.Scopes[0].Lanes[0].FencingToken++
			case "unselected release":
				forged.Scopes[0].Publications[0].Release.ID = "unselected"
			case "policy signature":
				forged.Scopes[0].Publications[0].ProducerPolicy.Provenance.Signature = "forged"
			case "forged LKG":
				forged.Scopes[0].Publications[0].LKG.SnapshotProvenance.Signature = "forged"
			case "DNS identity":
				forged.DNSArtifactID = "foreign-dns"
			}
			forged.SelectionDigest, _ = dnsroutesource.SelectionDigest(forged)
			if dnsroutesource.VerifySnapshot(forged, child, celldns.Keys(), time.Now()) == nil {
				t.Fatal("self-computed selection digest authorized invalid source", scenario)
			}
		}
		second, err := s.ObserveDNSRouteSources(context.Background(), child)
		if err != nil || first.SelectionDigest != second.SelectionDigest || first.ObservedAt.Equal(second.ObservedAt) {
			t.Fatal("observation time changed authority digest", err)
		}
		if first.DNSArtifactID != child.ID || first.DNSArtifactDigest != child.ContentHash {
			t.Fatal("DNS binding lost")
		}
		for _, scope := range first.Scopes {
			if len(scope.Publications) != 1 || !slices.Equal(scope.Publications[0].Selections, []string{"full", "lkg"}) {
				t.Fatal("current full/LKG were duplicated or lost")
			}
		}
		after, _ := os.ReadFile(path)
		if string(after) != string(raw) {
			t.Fatal("source observation wrote runtime or configuration state")
		}
		// A new unverified full can coexist with the previous positive LKG.
		if err := s.withLockedState(true, func(st *model.State) error {
			lane := &st.PlatformReleaseLanes[0]
			index := platformArtifactReleaseIndex(st.PlatformArtifactReleases, lane.ActiveReleaseID)
			old := st.PlatformArtifactReleases[index]
			st.PlatformArtifactReleases[index].Status = model.PlatformArtifactReleaseStatusSuperseded
			next := old
			next.ID = "new-source-full"
			next.FencingToken++
			next.VerificationState = model.PlatformArtifactVerificationStateServingUnverified
			next.VerifiedLKGGeneration = ""
			next.ReleasedAt = time.Now().Add(-time.Second)
			lane.ActiveReleaseID = next.ID
			lane.FencingToken = next.FencingToken
			lane.Version++
			st.PlatformArtifactReleases = append(st.PlatformArtifactReleases, next)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		changed, err := s.ObserveDNSRouteSources(context.Background(), child)
		if err != nil || changed.SelectionDigest == first.SelectionDigest {
			t.Fatal("new source fence not observed", err)
		}
		if len(changed.Scopes[0].Publications) != 2 || changed.Scopes[0].Publications[1].Selections[0] != "lkg" {
			t.Fatal("positive predecessor discarded while new full unverified")
		}
		if err := s.withLockedState(true, func(st *model.State) error {
			i := platformArtifactReleaseIndex(st.PlatformArtifactReleases, "new-source-full")
			st.PlatformArtifactReleases[i].VerificationState = model.PlatformArtifactVerificationStateFailed
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		failed, err := s.ObserveDNSRouteSources(context.Background(), child)
		if err != nil {
			t.Fatal(err)
		}
		if len(failed.Scopes[0].Publications) != 1 || failed.Scopes[0].Publications[0].Selections[0] != "lkg" {
			t.Fatal("failed candidate admitted or LKG lost")
		}
	}
}

func TestDNSRouteSourceObservationRejectsUnauthorizedAndTamperedState(t *testing.T) {
	for _, scenario := range []string{"frozen", "fence drift", "route signature", "producer signature", "producer activation", "unapproved policy", "forged LKG", "no approved source", "expired LKG only", "historical release"} {
		t.Run(scenario, func(t *testing.T) {
			state, child := dnsSourceObservationFixture(t, false)
			lane := &state.PlatformReleaseLanes[0]
			ri := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lane.ActiveReleaseID)
			parentIndex := platformArtifactIndex(state.PlatformArtifacts, state.PlatformArtifactReleases[ri].ArtifactID)
			switch scenario {
			case "frozen":
				lane.Frozen = true
			case "fence drift":
				lane.FencingToken++
			case "route signature":
				state.PlatformArtifacts[platformArtifactIndex(state.PlatformArtifacts, "route-a")].Provenance.Signature = "forged"
			case "producer signature":
				state.PlatformArtifacts[0].Provenance.Signature = "forged"
			case "producer activation":
				state.PlatformArtifactReleases[0].ScopeKey = "foreign"
			case "unapproved policy":
				state.PlatformArtifacts[0].Content["generation"] = "new-policy"
				state.PlatformArtifacts[0] = celldns.Sign(t, state.PlatformArtifacts[0], state.PlatformArtifacts[0].ID, 1)
			case "forged LKG":
				state.PlatformLKGSnapshots[0].SnapshotProvenance.Signature = "forged"
			case "no approved source":
				delete(state.PlatformArtifacts[parentIndex].Metadata, "producer_policy_release_id")
				state.PlatformArtifacts[parentIndex] = celldns.Sign(t, state.PlatformArtifacts[parentIndex], state.PlatformArtifacts[parentIndex].ID, 1)
				state.PlatformLKGSnapshots = nil
			case "expired LKG only":
				lane.ActiveReleaseID = ""
				state.PlatformLKGSnapshots[0].ExpiresAt = time.Now().Add(-time.Second)
			case "historical release":
				lane.ActiveReleaseID = ""
				state.PlatformLKGSnapshots = nil
			}
			p := t.TempDir() + "/state.json"
			raw, _ := json.Marshal(state)
			os.WriteFile(p, raw, 0600)
			s := New(p)
			s.ConfigurePlatformArtifactSigning(celldns.Keys())
			if _, err := s.ObserveDNSRouteSources(context.Background(), child); err == nil {
				t.Fatal("unsafe source state returned")
			}
		})
	}
}

func TestDNSRouteSourceGrayPublicationBindsCohortAndApproval(t *testing.T) {
	state, child := dnsSourceObservationFixture(t, false)
	old := state.PlatformArtifactReleases[platformArtifactReleaseIndex(state.PlatformArtifactReleases, state.PlatformReleaseLanes[0].ActiveReleaseID)]
	gray := old
	gray.ID = "source-gray"
	gray.ReleaseChannel = "gray"
	gray.CanaryRuleRef = "cohort=complete"
	gray.LaneKey = platformsafety.ReleaseLaneKey(gray.ArtifactKind, gray.ScopeKey, "gray")
	gray.VerificationState = model.PlatformArtifactVerificationStateServingUnverified
	gray.VerifiedLKGGeneration = ""
	gray.ReleasedAt = time.Now().Add(-time.Second)
	state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, gray)
	state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: gray.LaneKey, ArtifactKind: gray.ArtifactKind, ScopeKey: gray.ScopeKey, ReleaseChannel: "gray", ActiveReleaseID: gray.ID, FencingToken: gray.FencingToken, Version: 1})
	p := t.TempDir() + "/state.json"
	raw, _ := json.Marshal(state)
	os.WriteFile(p, raw, 0600)
	s := New(p)
	s.ConfigurePlatformArtifactSigning(celldns.Keys())
	got, err := s.ObserveDNSRouteSources(context.Background(), child)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Scopes[0].Publications) != 2 || got.Scopes[0].Publications[0].Selections[0] != "gray" {
		t.Fatal("gray/full overlap missing")
	}
	want, err := dnsroutesource.SelectionDigest(got)
	if err != nil || want != got.SelectionDigest {
		t.Fatal("invalid selection digest")
	}
}
