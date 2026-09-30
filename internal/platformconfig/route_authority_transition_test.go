package platformconfig_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestDNSAuthorityTransitionCompilesExplicitAlternativesWithoutExtraVotes(t *testing.T) {
	req := celldns.TransitionRequest(t)
	before, _ := json.Marshal(req)
	c := celldns.Compile(t, req)
	if err := platformconfig.ValidateDNSCellPlan(c.DNSArtifact); err != nil {
		t.Fatal(err)
	}
	var plan platformconfig.DNSReadinessPlan
	raw, _ := json.Marshal(c.DNSArtifact.Content["readiness_plan"])
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Probes) != 4 || len(plan.Records) != 1 || len(plan.Records[0].Targets) != 2 {
		t.Fatal("authority alternatives invented physical endpoints", plan)
	}
	for _, p := range plan.Probes {
		if p.PreviousAuthority == nil || p.PreviousAuthority.EdgeGroupID == p.EdgeGroupID || p.PreviousAuthority.RouteDigest == p.RouteDigest {
			t.Fatal("previous proof lost exact authority binding", p)
		}
	}
	for _, target := range plan.Records[0].Targets {
		if !target.RequireSinglePublication {
			t.Fatal("whole-target source coherence missing")
		}
	}
	if platformconfig.DNSReadinessQuorum(plan.Records[0], func(target platformconfig.DNSReadinessTarget) bool { return target.EdgeID == "edge-a" }) {
		t.Fatal("two authorities counted as two physical members")
	}
	after, _ := json.Marshal(req)
	if string(before) != string(after) {
		t.Fatal("compiler mutated caller's transition")
	}
	p, err := platformconfig.DecodePreviousTrafficPublication(c.DNSArtifact)
	if err != nil || p == nil || !reflect.DeepEqual(p.Reference, req.PreviousTrafficPublication.Reference) {
		t.Fatal("lost retained predecessor", err)
	}
}

func TestDNSAuthorityTransitionRejectsChangedSignedBehaviorAndTopology(t *testing.T) {
	for name, change := range map[string]func(*platformconfig.CompileRequest){
		"unbound source":    func(r *platformconfig.CompileRequest) { r.PreviousTrafficPublication = nil },
		"undeclared source": func(r *platformconfig.CompileRequest) { r.Intent.RouteAuthorityTransition = nil },
		"source fence":      func(r *platformconfig.CompileRequest) { r.PreviousTrafficPublication.Reference.FencingToken++ },
		"invented previous node": func(r *platformconfig.CompileRequest) {
			r.Intent.RouteAuthorityTransition.PreviousTopology.Edges[0].ID = "invented-edge"
		},
		"previous risk rewrite": func(r *platformconfig.CompileRequest) {
			r.Intent.RouteAuthorityTransition.PreviousTopology.Edges[0].FailureDomains["provider"] = "other-provider"
		},
		"source input changed": func(r *platformconfig.CompileRequest) { r.PreviousTrafficPublication.Route.Content["routes"] = []any{} },
		"old DNS ownership missing": func(r *platformconfig.CompileRequest) {
			p := r.PreviousTrafficPublication
			p.DNS.Content["readiness_plan"] = map[string]any{"records": []any{}, "probes": []any{}}
			p.DNS = celldns.Sign(t, p.DNS, p.DNS.ID, p.DNS.GenerationSequence)
			p.Reference.DNSArtifactDigest = p.DNS.ContentHash
			r.Intent.RouteAuthorityTransition.PreviousPublication = p.Reference
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := celldns.TransitionRequest(t)
			change(&r)
			if _, err := platformconfig.Compile(r); err == nil {
				t.Fatal("unsafe transition compiled")
			}
		})
	}
	for field, value := range map[string]any{"upstream_url": "http://other-origin:8080", "tenant_id": "other-tenant", "cache_namespace": "other-generation", "deployment_generation": "release-new", "path_prefix": "/private", "enabled": false, "min_healthy_edge_nodes": 1} {
		t.Run(field, func(t *testing.T) {
			r := celldns.TransitionRequest(t)
			p := &r.CellRoutePublications[0]
			rows := p.Route.Content["routes"].([]any)
			rows[0].(map[string]any)[field] = value
			p.Route = celldns.Sign(t, p.Route, p.Route.ID, p.Route.GenerationSequence)
			p.Reference.RouteArtifactDigest = p.Route.ContentHash
			r.Intent.CellRoutePublications[0] = p.Reference
			if err := platformconfig.ValidateCellRoutePublication(*p); err != nil {
				t.Fatal("fixture should retain valid signed composition before behavior comparison", err)
			}
			if _, err := platformconfig.Compile(r); err == nil {
				t.Fatal("signed but changed route behavior accepted", field)
			}
		})
	}
}

func TestDNSAuthorityTransitionReplayRejectsAlteredAlternativeOrCoherence(t *testing.T) {
	for _, field := range []string{"publication_digest", "edge_group_id", "route_digest", "coherence"} {
		t.Run(field, func(t *testing.T) {
			c := celldns.Compile(t, celldns.TransitionRequest(t))
			plan := c.DNSArtifact.Content["readiness_plan"].(map[string]any)
			if field == "coherence" {
				plan["records"].([]any)[0].(map[string]any)["targets"].([]any)[0].(map[string]any)["require_single_publication"] = false
			} else {
				plan["probes"].([]any)[0].(map[string]any)["previous_authority"].(map[string]any)[field] = "changed"
			}
			if err := platformconfig.ValidateDNSCellPlan(c.DNSArtifact); err == nil {
				t.Fatal("edited transition plan replay accepted")
			}
		})
	}
}
