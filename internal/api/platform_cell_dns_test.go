package api

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/dnsfacts"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
	"fugue/internal/testfixture/celldns"
)

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
