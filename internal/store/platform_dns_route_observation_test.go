package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/testfixture/celldns"
)

func dnsSourceObservationFixture(t *testing.T, transition bool) (storeProjection, model.PlatformArtifact) {
	t.Helper()
	req, policies, policyReleases := celldns.SourceAuthorizedRequest(t, transition)
	compiled := celldns.Compile(t, req)
	state := storeProjection{PlatformArtifacts: policies, PlatformArtifactReleases: policyReleases}
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

func seedDNSObservationRows(t *testing.T, s *Store, state storeProjection) {
	t.Helper()
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, artifact := range state.PlatformArtifacts {
		if _, err := pgInsertPlatformArtifact(t.Context(), tx, artifact); err != nil {
			t.Fatal(err)
		}
	}
	for _, release := range state.PlatformArtifactReleases {
		if _, err := pgInsertPlatformArtifactRelease(t.Context(), tx, release); err != nil {
			t.Fatal(err)
		}
	}
	for _, lane := range state.PlatformReleaseLanes {
		_, err := tx.ExecContext(t.Context(), `INSERT INTO fugue_platform_release_lanes
			(lane_key,artifact_kind,scope_key,release_channel,active_release_id,fencing_token,version,frozen,updated_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,now())`, lane.LaneKey, lane.ArtifactKind, lane.ScopeKey, lane.ReleaseChannel,
			lane.ActiveReleaseID, lane.FencingToken, lane.Version, lane.Frozen)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, lkg := range state.PlatformLKGSnapshots {
		if _, err := pgUpsertPlatformLKGSnapshot(t.Context(), tx, lkg); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
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
			s := newStoreTestDatabase(t, "")
			s.ConfigurePlatformArtifactSigning(celldns.Keys())
			seedDNSObservationRows(t, s, state)
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
	s := newStoreTestDatabase(t, "")
	s.ConfigurePlatformArtifactSigning(celldns.Keys())
	seedDNSObservationRows(t, s, state)
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
