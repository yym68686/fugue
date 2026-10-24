package dnsroutesource

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/platformconfig"
)

func recoveryPlan() *platformconfig.DNSReadinessPlan {
	return &platformconfig.DNSReadinessPlan{
		Probes:  []platformconfig.DNSReadinessProbe{{ID: "a", EdgeID: "edge-a", EdgeGroupID: "cell-a", Address: "93.184.216.34", Hostname: "a.example.test", Path: "/", RouteDigest: "route-a", CellPublicationDigest: "publication-a"}, {ID: "b", EdgeID: "edge-b", EdgeGroupID: "cell-b", Address: "93.184.216.35", Hostname: "b.example.test", Path: "/", RouteDigest: "route-b", CellPublicationDigest: "publication-a"}},
		Records: []platformconfig.DNSReadinessRecord{{Hostname: "a.example.test", MinimumHealthyEdges: 1, Targets: []platformconfig.DNSReadinessTarget{{EdgeID: "edge-a", EdgeGroupID: "cell-a", Address: "93.184.216.34", Family: "A", RequireSinglePublication: true, FailureDomains: map[string]string{"provider": "provider-a"}, ProbeIDs: []string{"a"}}}}, {Hostname: "b.example.test", MinimumHealthyEdges: 1, Targets: []platformconfig.DNSReadinessTarget{{EdgeID: "edge-b", EdgeGroupID: "cell-b", Address: "93.184.216.35", Family: "A", ProbeIDs: []string{"b"}}}}},
	}
}
func TestRecoveryConstraintsAreLocalAndComplete(t *testing.T) {
	for _, mode := range []string{"unrelated", "publication", "order", "quorum", "dual stack", "distinct cells", "distinct domains", "failure domain", "single publication", "dependency", "missing probe"} {
		t.Run(mode, func(t *testing.T) {
			plan := recoveryPlan()
			original, _ := json.Marshal(plan)
			before, err := recoveryConstraints(plan)
			if err != nil {
				t.Fatal(err)
			}
			afterInput, _ := json.Marshal(plan)
			if string(afterInput) != string(original) {
				t.Fatal("signed plan mutated")
			}
			switch mode {
			case "unrelated":
				plan.Records[1].MinimumHealthyEdges++
			case "publication":
				for i := range plan.Probes {
					plan.Probes[i].CellPublicationDigest = "publication-next"
				}
			case "order":
				plan.Records[0], plan.Records[1] = plan.Records[1], plan.Records[0]
				plan.Probes[0], plan.Probes[1] = plan.Probes[1], plan.Probes[0]
			case "quorum":
				plan.Records[0].MinimumHealthyEdges++
			case "dual stack":
				plan.Records[0].RequireDualStack = true
			case "distinct cells":
				plan.Records[0].MinDistinctCells = 2
			case "distinct domains":
				plan.Records[0].MinDistinctDomains = map[string]int{"provider": 2}
			case "failure domain":
				plan.Records[0].Targets[0].FailureDomains["provider"] = "provider-other"
			case "single publication":
				plan.Records[0].Targets[0].RequireSinglePublication = false
			case "dependency":
				added := plan.Probes[0]
				added.ID = "extra"
				added.Path = "/admin"
				plan.Probes = append(plan.Probes, added)
				plan.Records[0].Targets[0].ProbeIDs = append(plan.Records[0].Targets[0].ProbeIDs, "extra")
			case "missing probe":
				plan.Records[0].Targets[0].ProbeIDs = []string{"missing"}
			}
			after, err := recoveryConstraints(plan)
			if mode == "missing probe" {
				if err == nil {
					t.Fatal("missing dependency accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "publication" || mode == "order" {
				if !reflect.DeepEqual(before, after) {
					t.Fatal("identity or iteration order changed constraints")
				}
				return
			}
			if mode == "unrelated" {
				if before["a"] != after["a"] || before["b"] == after["b"] {
					t.Fatal("unrelated record changed probe recovery")
				}
				return
			}
			if before["a"] == after["a"] || before["b"] != after["b"] {
				t.Fatal("relevant constraint was lost or affected unrelated record")
			}
		})
	}
}
