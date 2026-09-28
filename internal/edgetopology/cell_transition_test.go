package edgetopology

import (
	"reflect"
	"testing"
)

func cellTransitionFixture() Intent {
	return Intent{
		SchemaVersion: SchemaVersion,
		Cells:         []AuthorityCell{{ID: "cell-a", LegacyGroupID: "edge-group-a"}, {ID: "cell-b", LegacyGroupID: "edge-group-b"}},
		Pools:         []ServingPool{{ID: "pool-shared"}},
		Edges: []Edge{
			{ID: "edge-a", AuthorityCellID: "cell-a", ServingPoolIDs: []string{"pool-shared"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "host-a"}, Labels: map[string]string{"country": "us"}},
			{ID: "edge-b", AuthorityCellID: "cell-b", ServingPoolIDs: []string{"pool-shared"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "host-b"}, Labels: map[string]string{"country": "us"}},
		},
	}
}

func TestCellTransitionBindsCompleteIntentsWithoutTrafficAuthority(t *testing.T) {
	previous := cellTransitionFixture()
	baseline := previous.Clone()
	next := previous.Clone()
	next.Cells[0].LegacyGroupID = ""
	plan, err := PlanCellTransition(previous, next)
	if err != nil || plan.AuthorizesTraffic || plan.CellID != "cell-a" || plan.PreviousAuthority != "edge-group-a" || plan.NextAuthority != "cell-a" ||
		!reflect.DeepEqual(plan.EdgeIDs, []string{"edge-a"}) || plan.PreviousDigest == plan.NextDigest || len(plan.PreviousDigest) != 71 || len(plan.NextDigest) != 71 {
		t.Fatalf("transition=%+v error=%v", plan, err)
	}
	if !reflect.DeepEqual(previous, baseline) {
		t.Fatal("planning mutated the serving topology")
	}
	if _, err := PlanCellTransition(next, next); err == nil {
		t.Fatal("completed transition was silently replayed")
	}
	if _, err := PlanCellTransition(next, previous); err == nil {
		t.Fatal("reverse alias installation was treated as neutral migration")
	}
}

func TestCellTransitionRejectsUnrelatedConfigurationChanges(t *testing.T) {
	for name, alter := range map[string]func(*Intent){
		"second cell":         func(v *Intent) { v.Cells[1].LegacyGroupID = "" },
		"different authority": func(v *Intent) { v.Cells[0].LegacyGroupID = "edge-group-other" },
		"changed risk":        func(v *Intent) { v.Edges[0].FailureDomains["host"] = "host-b" },
		"changed locality":    func(v *Intent) { v.Edges[0].Labels["country"] = "de" },
		"expanded capability": func(v *Intent) { v.Edges[0].Capabilities = []string{"http", "ssh", "tls"} },
		"reassigned edges":    func(v *Intent) { v.Edges[0].AuthorityCellID, v.Edges[1].AuthorityCellID = "cell-b", "cell-a" },
		"new pool": func(v *Intent) {
			v.Pools[0].ID = "pool-new"
			for i := range v.Edges {
				v.Edges[i].ServingPoolIDs = []string{"pool-new"}
			}
		},
		"removed edge": func(v *Intent) { v.Edges = v.Edges[:1] },
	} {
		t.Run(name, func(t *testing.T) {
			previous := cellTransitionFixture()
			next := previous.Clone()
			next.Cells[0].LegacyGroupID = ""
			alter(&next)
			if _, err := PlanCellTransition(previous, next); err == nil {
				t.Fatal("identity migration accepted an unrelated configuration change")
			}
		})
	}
}
