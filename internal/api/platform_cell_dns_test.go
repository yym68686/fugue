package api

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"fugue/internal/dnsfacts"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	"fugue/internal/routeprobe"
	"fugue/internal/store"
	"fugue/internal/testfixture/celldns"
)

func TestCellDNSAPICompilationRequiresCurrentSignedReferences(t *testing.T) {
	for _, scenario := range []string{"complete", "untrusted embedded artifact", "obsolete publication", "wrong fence", "invalid retained TLS"} {
		t.Run(scenario, func(t *testing.T) {
			_, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			req := celldns.Request(t)
			state := model.State{}
			for i := range req.CellRoutePublications {
				p := &req.CellRoutePublications[i]
				for _, a := range []*model.PlatformArtifact{&p.Parent, &p.Route, &p.TLS} {
					var err error
					*a, err = platformsafety.SignPlatformArtifact(*a, s.bundleKeyring())
					if err != nil {
						t.Fatal(err)
					}
					state.PlatformArtifacts = append(state.PlatformArtifacts, *a)
				}
				state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, celldns.Publication(*p))
				state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: celldns.Publication(*p).LaneKey, ArtifactKind: celldns.Publication(*p).ArtifactKind, ScopeKey: celldns.Publication(*p).ScopeKey, ReleaseChannel: celldns.Publication(*p).ReleaseChannel, ActiveReleaseID: celldns.Publication(*p).ID, FencingToken: celldns.Publication(*p).FencingToken, Version: 1, UpdatedAt: celldns.Publication(*p).ReleasedAt})
			}
			switch scenario {
			case "untrusted embedded artifact":
				req.CellRoutePublications[0].TLS.Provenance.Signature = "forged"
			case "obsolete publication":
				state.PlatformArtifactReleases[0].ID = "successor"
				state.PlatformReleaseLanes[0].ActiveReleaseID = "successor"
			case "invalid retained TLS":
				state.PlatformArtifacts[2].Status = model.PlatformArtifactStatusDraft
			case "wrong fence":

				req.CellRoutePublications[0].Reference.FencingToken++
				req.Intent.CellRoutePublications[0].FencingToken++
			}
			path := t.TempDir() + "/state.json"
			raw, _ := json.Marshal(state)
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			s.store = store.New(path)
			s.store.ConfigurePlatformArtifactSigning(s.bundleKeyring())
			request := platformConfigCompileRequest{CellRoutePublications: req.CellRoutePublications, Intent: req.Intent, Policy: req.Policy, RuntimeSnapshot: req.RuntimeSnapshot}
			response := performJSONRequest(t, s, http.MethodPost, "/v1/admin/platform-config/compile", admin, request)
			if scenario != "complete" {
				if response.Code != http.StatusConflict {
					t.Fatalf("untrusted reference response %d: %s", response.Code, response.Body.String())
				}
				artifacts, err := s.store.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 100})
				if err != nil || len(artifacts) != 6 {
					t.Fatal("rejected reference wrote artifacts", err, len(artifacts))
				}
				return
			}
			if response.Code != http.StatusCreated {
				t.Fatalf("compile failed %d: %s", response.Code, response.Body.String())
			}
			var rawResponse map[string]any
			json.Unmarshal(response.Body.Bytes(), &rawResponse)
			if rawResponse["route_artifact"] != nil || rawResponse["tls_artifact"] != nil || rawResponse["dns_artifact"] == nil {
				t.Fatal("compile response does not match DNS-only contract")
			}
			var result platformConfigCompileResponse
			json.Unmarshal(response.Body.Bytes(), &result)
			if _, err := decodePlatformDNSArtifact(result.DNSArtifact); err != nil {
				t.Fatal(err)
			}
			replay := performJSONRequest(t, s, http.MethodPost, "/v1/admin/platform-config/compile-from-artifacts", admin, platformConfigCompileArtifactsRequest{CellRoutePublications: req.CellRoutePublications, IntentArtifactID: result.IntentArtifact.ID, PolicyArtifactID: result.PolicyArtifact.ID, RuntimeSnapshot: req.RuntimeSnapshot})
			if replay.Code != http.StatusCreated {
				t.Fatalf("replay failed %d: %s", replay.Code, replay.Body.String())
			}
			var repeated platformConfigCompileResponse
			json.Unmarshal(replay.Body.Bytes(), &repeated)
			if repeated.ReleaseArtifact.ID != result.ReleaseArtifact.ID || repeated.DNSArtifact.ID != result.DNSArtifact.ID {
				t.Fatal("replay created a different DNS publication")
			}
		})
	}
}

func TestCellDNSRuntimeFactsKeepEachRouteAuthority(t *testing.T) {
	for _, transition := range []bool{false, true} {
		name := "neutral-only"
		if transition {
			name = "authority-transition"
		}
		t.Run(name, func(t *testing.T) { testCellDNSRuntimeFactsKeepEachRouteAuthority(t, transition) })
	}
}
func testCellDNSRuntimeFactsKeepEachRouteAuthority(t *testing.T, transition bool) {
	request := celldns.Request(t)
	if transition {
		request = celldns.TransitionRequest(t)
	}
	c := celldns.Compile(t, request)
	payload, err := decodePlatformDNSArtifact(c.DNSArtifact)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	source := dnsFactSource{parent: c.ReleaseArtifact, payload: payload, group: "cell-dns", claims: platformcontrol.PlatformComponentIdentityClaims{NodeID: "dns-a"}, cellBindings: map[string]*model.TrafficReleaseBinding{}}
	planDigest, _ := platformconfig.Digest(payload.ReadinessPlan)
	snapshot := dnsfacts.Snapshot{Schema: dnsfacts.Schema, NodeID: "dns-a", EdgeGroupID: "cell-dns", ParentDigest: c.ReleaseArtifact.ContentHash, PlanDigest: planDigest, ObservedAt: now, EvaluatedAt: now, CheckpointValidUntil: now.Add(time.Minute), Ready: true}
	for _, p := range payload.CellRoutePublications {
		d, _ := platformconfig.Digest(p.Reference)
		b, err := platformconfig.CellRoutePublicationBinding(p)
		if err != nil {
			t.Fatal(err)
		}
		source.cellBindings[d] = b
	}
	if transition {
		p := payload.PreviousTrafficPublication
		binding, err := platformconfig.PreviousTrafficPublicationBinding(*p, "edge-group-a")
		if err != nil {
			t.Fatal(err)
		}
		digest, _ := platformconfig.Digest(p.Reference)
		source.cellBindings[digest] = binding
	}
	for _, req := range payload.ReadinessPlan.Probes {
		proof := routeprobe.Proof{Digest: req.RouteDigest, Version: "observed-bundle", EdgeID: req.EdgeID, GroupID: req.EdgeGroupID, CheckedAt: now, ValidUntil: now.Add(20 * time.Second), TrafficRelease: source.cellBindings[req.CellPublicationDigest]}
		if transition && req.EdgeID == "edge-a" {
			old := req.PreviousAuthority
			proof.GroupID, proof.Digest, proof.TrafficRelease = old.EdgeGroupID, old.RouteDigest, source.cellBindings[old.PublicationDigest]
		}
		snapshot.Facts = append(snapshot.Facts, dnsfacts.Probe{ProbeID: req.ID, Ready: true, Proof: proof})
	}
	ids, ready, err := evaluateDNSRuntimeSnapshot(snapshot, source, now)
	if err != nil || !ready || len(ids) != len(payload.ReadinessPlan.Probes) {
		t.Fatal("exact referenced facts rejected", err)
	}
	if transition {
		raw, _ := json.Marshal(snapshot)
		var mixed dnsfacts.Snapshot
		json.Unmarshal(raw, &mixed)
		for i, fact := range mixed.Facts {
			if fact.Proof.EdgeID != "edge-a" {
				continue
			}
			for _, req := range payload.ReadinessPlan.Probes {
				if req.ID == fact.ProbeID {
					mixed.Facts[i].Proof.GroupID, mixed.Facts[i].Proof.Digest, mixed.Facts[i].Proof.TrafficRelease = req.EdgeGroupID, req.RouteDigest, source.cellBindings[req.CellPublicationDigest]
				}
			}
			break
		}
		if _, ready, err := evaluateDNSRuntimeSnapshot(mixed, source, now); err != nil || ready {
			t.Fatal("individually valid mixed-publication dependencies claimed readiness", err)
		}
	}
	for _, scenario := range []string{"other Cell", "DNS parent", "wrong fence", "expired", "missing Edge"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(snapshot)
			var changed dnsfacts.Snapshot
			json.Unmarshal(raw, &changed)
			proof := &changed.Facts[0].Proof
			switch scenario {
			case "other Cell":
				for _, fact := range changed.Facts {
					if fact.Proof.EdgeID != proof.EdgeID {
						proof.TrafficRelease = fact.Proof.TrafficRelease
						break
					}
				}
			case "DNS parent":
				proof.TrafficRelease.ReleaseSetID = c.ReleaseArtifact.ID
			case "wrong fence":
				proof.TrafficRelease.FencingToken++
			case "expired":
				proof.ValidUntil = now.Add(-time.Second)
			case "missing Edge":
				changed.Facts = changed.Facts[:1]
			}
			_, ready, err := evaluateDNSRuntimeSnapshot(changed, source, now)
			if ready || err == nil && scenario != "expired" && scenario != "missing Edge" {
				t.Fatal("invalid cross-Cell evidence accepted", err)
			}
		})
	}
}
