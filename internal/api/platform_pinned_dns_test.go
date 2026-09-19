package api

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"reflect"
	"testing"
	"time"
)

func pinnedDNSFixture() (platformproducer.ProjectionPolicyInput, []platformconfig.DNSConsumerIntent, []model.DNSNode) {
	probe := &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
	p := platformproducer.ProjectionPolicyInput{SchemaVersion: platformconfig.SchemaVersion, Generation: "policy", Scope: "global", Authorities: []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}}, Clients: []platformconfig.DNSClientPolicy{{NodeID: "dns-a", Rules: []platformconfig.DNSClientRule{}}}, DNSReadiness: probe, TLSReadiness: probe, Cohorts: []platformconfig.TrafficRolloutCohort{{ID: "edge-group-a", EdgeGroupIDs: []string{"edge-group-a"}}, {ID: "complete", EdgeGroupIDs: []string{"edge-group-a"}}}}
	c := []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: defaultEdgeDNSProbeLabel, ProbeTTL: 60}}
	nodes := []model.DNSNode{{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "8.8.8.8"}}
	return p, c, nodes
}

func TestPinnedDNSMatchesMigratedDeclarationsAndPreservesEvidence(t *testing.T) {
	p, c, nodes := pinnedDNSFixture()
	now := time.Now().UTC()
	edges := []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "1.1.1.1"}}
	makeDraft := func() platformIntentProjectionResponse {
		return platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "global"}, Policy: platformconfig.NormalizePolicySnapshot(platformconfig.PolicySnapshot{Generation: "policy", Scope: "global"})}
	}
	old := makeDraft()
	if err := projectDNSConsumerDeclarations(&old, nodes, map[string][]string{"edge-group-a": {"example.test"}}, 60, now); err != nil {
		t.Fatal(err)
	}
	if err := projectDNSAuthorityPolicies(&old, map[string]platformconfig.DNSAuthorityPolicy{"edge-group-a": p.Authorities[0]}); err != nil {
		t.Fatal(err)
	}
	if err := projectDNSClientPolicies(&old, map[string][]platformconfig.DNSClientRule{"edge-group-a": {}}); err != nil {
		t.Fatal(err)
	}
	if err := projectDNSReadiness(&old, edges, now); err != nil {
		t.Fatal(err)
	}
	pinned := makeDraft()
	if err := projectPinnedDNSInputs(&pinned, c, p, nil, nodes, nil, now); err != nil {
		t.Fatal(err)
	}
	if err := projectDNSReadinessWithPolicy(&pinned, edges, now, &p); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(old, pinned) {
		t.Fatalf("pinned declaration changed migration semantics\nold=%+v\nnew=%+v", old, pinned)
	}
	// Addresses, health, and old zone aliases are observations, not declarations.
	nodes[0].PublicIPv4 = "9.9.9.9"
	nodes[0].Healthy = false
	nodes = append(nodes, model.DNSNode{ID: "dns-a-old-zone", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "deleted.test", PublicIPv4: "8.8.4.4"})
	fresh := makeDraft()
	if err := projectPinnedDNSInputs(&fresh, c, p, nil, nodes, nil, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pinned.Intent, fresh.Intent) || fresh.RuntimeSnapshot.DNSConsumers[0].A[0] != "9.9.9.9" {
		t.Fatal("runtime state changed DNS intent")
	}
	// Explicit probe settings survive the endpoint capture without legacy defaults.
	q := *p.DNSReadiness
	q.FactFreshnessSeconds = 90
	p.DNSReadiness = &q
	if err := projectDNSReadinessWithPolicy(&fresh, edges, now.Add(time.Minute), &p); err != nil || fresh.Policy.DNSReadiness.FactFreshnessSeconds != 90 {
		t.Fatal("pinned probes overwritten", err)
	}
}

func TestPinnedDNSHostedZoneTemplatesAndOwnership(t *testing.T) {
	for _, scenario := range []string{"expanded", "removed", "no template", "missing node", "foreign node", "wrong group", "conflicting address", "invalid address"} {
		t.Run(scenario, func(t *testing.T) {
			p, c, nodes := pinnedDNSFixture()
			now := time.Now().UTC()
			hosted := []model.HostedZone{{ZoneName: "new.test", Status: model.HostedZoneStatusPendingDelegation}, {ZoneName: "suspended.test", Status: model.HostedZoneStatusSuspended}, {ZoneName: "deleted.test", Status: model.HostedZoneStatusDeleted}}
			templates := []platformproducer.HostedZoneTemplate{{NodeID: "dns-a", TemplateZone: "example.test"}}
			switch scenario {
			case "removed":
				hosted[0].Status = model.HostedZoneStatusDeleted
			case "no template":
				templates = nil
			case "missing node":
				nodes = nil
			case "foreign node":
				nodes[0].PhysicalNodeID = "other"
			case "wrong group":
				nodes[0].EdgeGroupID = "wrong"
			case "conflicting address":
				n := nodes[0]
				n.ID = "alias"
				n.PublicIPv4 = "9.9.9.9"
				nodes = append(nodes, n)
			case "invalid address":
				nodes[0].PublicIPv4 = "127.0.0.1"
			}
			draft := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "global"}, Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: "global"}}
			before, _ := json.Marshal(draft)
			err := projectPinnedDNSInputs(&draft, c, p, templates, nodes, hosted, now)
			if scenario == "expanded" || scenario == "removed" || scenario == "no template" {
				if err != nil {
					t.Fatal(err)
				}
				count := 1
				if scenario == "expanded" {
					count = 2
				}
				if len(draft.Intent.DNSConsumers[0].Zones) != count || len(draft.Policy.DNSAuthorities) != count {
					t.Fatal("hosted zone membership wrong")
				}
				for _, a := range draft.Policy.DNSAuthorities {
					if a.Nameservers[0] != "ns.example.test" || a.TTLSeconds != 60 {
						t.Fatal("template lost parameters")
					}
				}
			} else {
				after, _ := json.Marshal(draft)
				if err == nil || string(after) != string(before) {
					t.Fatal("invalid topology changed draft", err)
				}
			}
		})
	}
}
