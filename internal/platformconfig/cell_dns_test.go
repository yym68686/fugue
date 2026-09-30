package platformconfig_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
	"fugue/internal/testfixture/celldns"
)

func TestCellDNSReferencesIndependentProjectionsAndPreservesQuorum(t *testing.T) {
	r := celldns.Request(t)
	before, _ := json.Marshal(r)
	c := celldns.Compile(t, r)
	if c.RouteArtifact.Content != nil || c.TLSArtifact.Content != nil || !reflect.DeepEqual(c.ReleaseSet.ArtifactKinds, []string{model.PlatformArtifactKindDNSAnswerBundle}) || len(c.ReleaseSet.ConsumerTopology.EdgeNodeIDs) != 0 || !reflect.DeepEqual(c.ReleaseSet.ConsumerTopology.DNSNodeIDs, []string{"dns-a"}) {
		t.Fatal("DNS publication acquired routing authority")
	}
	if err := platformconfig.ValidateDNSCellPlan(c.DNSArtifact); err != nil {
		t.Fatal(err)
	}
	var plan platformconfig.DNSReadinessPlan
	raw, _ := json.Marshal(c.DNSArtifact.Content["readiness_plan"])
	json.Unmarshal(raw, &plan)
	if len(plan.Probes) != 2 || len(plan.Records) != 1 || plan.Records[0].MinimumHealthyEdges != 2 || plan.Records[0].MinDistinctCells != 2 || plan.Records[0].MinDistinctDomains["provider"] != 2 {
		t.Fatalf("lost route quorum: %+v", plan)
	}
	if platformconfig.DNSReadinessQuorum(plan.Records[0], func(target platformconfig.DNSReadinessTarget) bool { return target.EdgeID == "edge-a" }) {
		t.Fatal("one physical Edge satisfied quorum")
	}
	if !platformconfig.DNSReadinessQuorum(plan.Records[0], func(platformconfig.DNSReadinessTarget) bool { return true }) {
		t.Fatal("complete quorum rejected")
	}
	if plan.Probes[0].RouteDigest == plan.Probes[1].RouteDigest {
		t.Fatal("independent upstream projections were merged")
	}
	for _, p := range r.CellRoutePublications {
		binding, err := platformconfig.CellRoutePublicationBinding(p)
		if err != nil {
			t.Fatal(err)
		}
		a := model.PlatformConsumerAssignment{ReleaseSetID: p.Parent.ID, ArtifactID: p.Route.ID, ArtifactKind: p.Route.ArtifactKind, ScopeKey: p.Parent.ScopeKey, ContentHash: p.Route.ContentHash, ExpectedGeneration: p.Route.Generation, GenerationSequence: p.Route.GenerationSequence, ExpectedConsumerSetID: "set", Revision: 1, ArtifactReleaseID: celldns.Publication(p).ID, FencingToken: celldns.Publication(p).FencingToken, ReleaseChannel: celldns.Publication(p).ReleaseChannel}
		projected, err := routeartifact.ProjectRelease(p.Parent, p.Route, a, celldns.Publication(p), celldns.Keys())
		if err != nil || !reflect.DeepEqual(binding, projected.TrafficRelease) {
			t.Fatal("DNS reference and actual Worker binding differ", err)
		}
	}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("compiler changed caller inputs")
	}
	slices.Reverse(r.Intent.CellRoutePublications)
	slices.Reverse(r.CellRoutePublications)
	slices.Reverse(r.RuntimeSnapshot.DNSEdgeEndpoints)
	r.CreatedAt = r.CreatedAt.Add(24 * time.Hour)
	replay := celldns.Compile(t, r)
	if !reflect.DeepEqual(c.DNSArtifact.Content, replay.DNSArtifact.Content) {
		t.Fatal("replay depends on enumeration or wall clock")
	}
}

func TestCellDNSRejectsUnboundOrInsufficientInputs(t *testing.T) {
	for name, mutate := range map[string]func(*platformconfig.CompileRequest){
		"shadow reference": func(r *platformconfig.CompileRequest) { r.Intent.CellRoutePublications[0].ReleaseChannel = "shadow" },
		"wrong fence": func(r *platformconfig.CompileRequest) {
			r.CellRoutePublications[0].Reference.FencingToken++
		},
		"wrong parent": func(r *platformconfig.CompileRequest) {
			r.CellRoutePublications[0].Parent.ID = "different"
		},
		"tampered route": func(r *platformconfig.CompileRequest) {
			r.CellRoutePublications[0].Route.Content["routes"] = []any{}
		},
		"foreign member": func(r *platformconfig.CompileRequest) { r.Intent.EdgeTopology.Edges[0].ID = "foreign" },
		"mixed endpoint": func(r *platformconfig.CompileRequest) { r.RuntimeSnapshot.DNSEdgeEndpoints[0].EdgeGroupID = "cell-b" },
		"extra reference": func(r *platformconfig.CompileRequest) {
			r.CellRoutePublications = append(r.CellRoutePublications, r.CellRoutePublications[0])
		},
		"one edge": func(r *platformconfig.CompileRequest) {
			r.RuntimeSnapshot.DNSEdgeEndpoints = r.RuntimeSnapshot.DNSEdgeEndpoints[:1]
		},
		"same provider": func(r *platformconfig.CompileRequest) {
			r.Intent.EdgeTopology.Edges[1].FailureDomains["provider"] = "provider-a"
		},
		"wrong capability": func(r *platformconfig.CompileRequest) { r.Intent.EdgeTopology.Edges[1].Capabilities = []string{"http"} },
		"wrong pool": func(r *platformconfig.CompileRequest) {
			r.Policy.EdgeSelectionConstraints[0].AllowedPoolIDs = []string{"pool-other"}
		},
		"foreign constraint owner": func(r *platformconfig.CompileRequest) { r.Policy.EdgeSelectionConstraints[0].TenantID = "tenant-other" },
		"own routes": func(r *platformconfig.CompileRequest) {
			r.Intent.Routes = []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080"}}
		},
		"route policy override": func(r *platformconfig.CompileRequest) { r.Policy.TLSReadiness = r.Policy.DNSReadiness },
		"mutable origins": func(r *platformconfig.CompileRequest) {
			r.RuntimeSnapshot.Origins = []platformconfig.OriginObservation{{Ref: "origin"}}
		},
		"legacy alias": func(r *platformconfig.CompileRequest) { r.Intent.EdgeTopology.Cells[0].LegacyGroupID = "edge-group-a" },
		"foreign DNS":  func(r *platformconfig.CompileRequest) { r.Intent.DNSConsumers[0].EdgeGroupID = "cell-a" },
	} {
		t.Run(name, func(t *testing.T) {
			r := celldns.Request(t)
			mutate(&r)
			if _, err := platformconfig.Compile(r); err == nil {
				t.Fatal("invalid independent DNS compiled")
			}
		})
	}
}

func TestCellDNSReplayRejectsEditedRequirements(t *testing.T) {
	for _, field := range []string{"minimum_healthy_edges", "targets", "cell_publication_digest", "route_digest"} {
		t.Run(field, func(t *testing.T) {
			c := celldns.Compile(t, celldns.Request(t))
			plan := c.DNSArtifact.Content["readiness_plan"].(map[string]any)
			switch field {
			case "minimum_healthy_edges":
				plan["records"].([]any)[0].(map[string]any)[field] = float64(1)
			case "targets":
				plan["records"].([]any)[0].(map[string]any)[field] = []any{}
			default:
				plan["probes"].([]any)[0].(map[string]any)[field] = "changed"
			}
			if platformconfig.ValidateDNSCellPlan(c.DNSArtifact) == nil {
				t.Fatal("edited proof requirements accepted")
			}
		})
	}
}

func TestCellDNSRetainsCellPolicyFloorWithoutDNSSelectionOverride(t *testing.T) {
	r := celldns.Request(t)
	r.Policy.EdgeSelectionConstraints = nil
	c := celldns.Compile(t, r)
	if err := platformconfig.ValidateDNSCellPlan(c.DNSArtifact); err != nil {
		t.Fatal(err)
	}
	var plan platformconfig.DNSReadinessPlan
	raw, _ := json.Marshal(c.DNSArtifact.Content["readiness_plan"])
	json.Unmarshal(raw, &plan)
	if plan.Records[0].MinimumHealthyEdges != 2 {
		t.Fatal("DNS lowered referenced Cell policy floor")
	}
	r.RuntimeSnapshot.DNSEdgeEndpoints = r.RuntimeSnapshot.DNSEdgeEndpoints[:1]
	if _, err := platformconfig.Compile(r); err == nil {
		t.Fatal("one physical Edge bypassed Cell floor")
	}
}

func TestCellDNSCannotDropPinnedRouteOrExcludedPhysicalEdge(t *testing.T) {
	for _, field := range []string{"excluded_edge_ids", "excluded_edge_group_ids", "dns_placement_edge_group_id", "min_healthy_edge_nodes"} {
		t.Run(field, func(t *testing.T) {
			r := celldns.Request(t)
			r.Policy.EdgeSelectionConstraints = nil
			p := &r.CellRoutePublications[0]
			route := p.Route.Content["routes"].([]any)[0].(map[string]any)
			switch field {
			case "excluded_edge_ids":
				route[field] = []any{"edge-a"}
			case "excluded_edge_group_ids":
				route[field] = []any{"cell-a"}
			case "dns_placement_edge_group_id":
				route[field] = "cell-other"
			case "min_healthy_edge_nodes":
				route[field] = float64(3)
			}
			p.Route = celldns.Sign(t, p.Route, p.Route.ID, 1)
			p.Reference.RouteArtifactDigest = p.Route.ContentHash
			r.Intent.CellRoutePublications[0] = p.Reference
			if _, err := platformconfig.Compile(r); err == nil {
				t.Fatalf("DNS ignored referenced route %s", field)
			}
		})
	}
}

func TestCellDNSPerRouteMinimumCannotLowerCellPolicyFloor(t *testing.T) {
	r := celldns.Request(t)
	r.Policy.EdgeSelectionConstraints = nil
	for i := range r.CellRoutePublications {
		p := &r.CellRoutePublications[i]
		p.Route.Content["routes"].([]any)[0].(map[string]any)["min_healthy_edge_nodes"] = float64(1)
		p.Route = celldns.Sign(t, p.Route, p.Route.ID, 1)
		p.Reference.RouteArtifactDigest = p.Route.ContentHash
		r.Intent.CellRoutePublications[i] = p.Reference
	}
	c := celldns.Compile(t, r)
	var plan platformconfig.DNSReadinessPlan
	raw, _ := json.Marshal(c.DNSArtifact.Content["readiness_plan"])
	json.Unmarshal(raw, &plan)
	if plan.Records[0].MinimumHealthyEdges != 2 {
		t.Fatal("route override lowered Cell minimum")
	}
}

func TestCellDNSInactiveDependenciesRespectOmitAndErrorPagePolicy(t *testing.T) {
	for _, scenario := range []string{"omit unavailable", "omit disabled", "error page", "mixed active", "missing dependency", "foreign owner", "route policy denied", "orphan query rule"} {
		t.Run(scenario, func(t *testing.T) {
			r := celldns.Request(t)
			r.Intent.DNS[0].RecordKind = model.EdgeDNSRecordKindPlatform
			r.Intent.DNS = append(r.Intent.DNS, platformconfig.DNSIntent{Hostname: "static.example.test", Type: "TXT", Values: []string{"retained"}, TTL: 60})
			if scenario == "omit unavailable" || scenario == "omit disabled" || scenario == "route policy denied" {
				r.Policy.DNSAnswerRules, r.RuntimeSnapshot.DNSSelections = nil, nil
			}
			if scenario == "error page" || scenario == "route policy denied" {
				r.Policy.DNSRouteStateConstraints = []platformconfig.DNSRouteStateConstraint{{RecordKind: model.EdgeDNSRecordKindPlatform, InactiveBehavior: "serve_error_page"}}
			}
			for i := range r.CellRoutePublications {
				p := &r.CellRoutePublications[i]
				route := p.Route.Content["routes"].([]any)[0].(map[string]any)
				if scenario != "mixed active" || i == 0 {
					route["enabled"], route["status"] = false, "unavailable"
					if scenario == "omit disabled" {
						route["status"] = "disabled"
					}
				}
				if scenario == "missing dependency" {
					route["hostname"] = "different.example.test"
				}
				if scenario == "foreign owner" {
					route["tenant_id"] = "foreign-tenant"
				}
				if scenario == "route policy denied" {
					route["route_policy"] = model.EdgeRoutePolicyRouteAOnly
				}
				p.Route = celldns.Sign(t, p.Route, p.Route.ID, 1)
				p.Reference.RouteArtifactDigest = p.Route.ContentHash
				r.Intent.CellRoutePublications[i] = p.Reference
			}
			compiled, err := platformconfig.Compile(r)
			if scenario == "mixed active" || scenario == "missing dependency" || scenario == "foreign owner" || scenario == "orphan query rule" {
				if err == nil {
					t.Fatal("missing, unauthorized or insufficient active dependencies accepted")
				}
				return
			}
			if err != nil {
				t.Fatal("known inactive dependencies blocked compilation", err)
			}
			if err := platformconfig.ValidateDNSCellPlan(compiled.DNSArtifact); err != nil {
				t.Fatal("offline replay rejected", err)
			}
			var payload struct {
				Plan    platformconfig.DNSReadinessPlan `json:"readiness_plan"`
				Records []platformconfig.DNSIntent      `json:"records"`
				Queries []platformconfig.DNSQueryView   `json:"query_views"`
			}
			raw, _ := json.Marshal(compiled.DNSArtifact.Content)
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(payload.Records, func(r platformconfig.DNSIntent) bool {
				return r.Hostname == "static.example.test" && r.Type == "TXT" && reflect.DeepEqual(r.Values, []string{"retained"})
			}) {
				t.Fatal("inactive route removed unrelated DNS data")
			}
			if scenario == "error page" {
				if len(payload.Plan.Records) != 1 || len(payload.Plan.Probes) != 2 || payload.Plan.Records[0].MinimumHealthyEdges != 2 {
					t.Fatal("error page lost exact quorum")
				}
				for _, p := range payload.Plan.Probes {
					if p.State != "unavailable" {
						t.Fatal("error page lost loaded-state proof")
					}
				}
			} else {
				if len(payload.Plan.Records) != 0 || len(payload.Plan.Probes) != 0 {
					t.Fatal("omitted route obtained proof authority")
				}
				for _, record := range payload.Records {
					if record.Hostname == "app.example.test" {
						t.Fatal("omitted route regained an answer")
					}
				}
				for _, view := range payload.Queries {
					for _, record := range view.Records {
						if record.Name == "app.example.test" {
							t.Fatal("omitted route regained query candidates")
						}
					}
				}
			}
		})
	}
}
