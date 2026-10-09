package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func physicalOrderFixture() CompileRequest {
	request := dnsQueryFixture()
	rule := &request.Policy.DNSAnswerRules[0]
	rule.SelectionMode = model.DNSAnswerPolicyKindPhysicalOrder
	rule.PhysicalOrder = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-b", "edge-a"}}
	rule.ScopedSelectionMode, rule.PreferredEdgeGroups, rule.FallbackEdgeGroups = "", nil, nil
	rule.ExplorationPercent, rule.ECSEnabled = 0, false
	fact := &request.RuntimeSnapshot.DNSSelections[0]
	fact.SelectedEdgeGroupID, fact.ScopedCandidates = "", nil
	rebindPlacement(&request)
	return request
}

func TestPhysicalOrderCompilesExactConfiguredOrder(t *testing.T) {
	request := physicalOrderFixture()
	before, _ := json.Marshal(request)
	compiled, err := Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	record := compiledQueryViews(t, compiled)[0].Records[0]
	if !reflect.DeepEqual(record.AnswerPolicy.PhysicalOrder, request.Policy.DNSAnswerRules[0].PhysicalOrder) || record.AnswerPolicy.PhysicalSelection != nil {
		t.Fatal("order changed or invented measurement evidence", record)
	}
	for _, candidate := range record.Candidates {
		if candidate.Healthy || candidate.RouteReady || candidate.TLSReady {
			t.Fatal("configuration fabricated readiness")
		}
	}
	after, _ := json.Marshal(request)
	if string(before) != string(after) {
		t.Fatal("compile mutated caller configuration")
	}
	normalized := normalizeDNSAnswerRules(request.Policy.DNSAnswerRules)
	normalized[0].PhysicalOrder.OrderedEdgeIDs[0] = "foreign"
	if request.Policy.DNSAnswerRules[0].PhysicalOrder.OrderedEdgeIDs[0] != "edge-b" {
		t.Fatal("order aliases caller configuration")
	}
}

func TestPhysicalOrderRejectsAmbiguousAuthority(t *testing.T) {
	for name, mutate := range map[string]func(*CompileRequest){
		"missing": func(request *CompileRequest) { request.Policy.DNSAnswerRules[0].PhysicalOrder = nil },
		"foreign": func(request *CompileRequest) {
			request.Policy.DNSAnswerRules[0].PhysicalOrder.OrderedEdgeIDs = []string{"other"}
		},
		"duplicate": func(request *CompileRequest) {
			request.Policy.DNSAnswerRules[0].PhysicalOrder.OrderedEdgeIDs = []string{"edge-b", "edge-b"}
		},
		"group": func(request *CompileRequest) {
			request.RuntimeSnapshot.DNSSelections[0].SelectedEdgeGroupID = "edge-group-a"
		},
		"exploration": func(request *CompileRequest) { request.Policy.DNSAnswerRules[0].ExplorationPercent = 5 },
		"geo":         func(request *CompileRequest) { request.Policy.DNSAnswerRules[0].ECSEnabled = true },
		"legacy":      func(request *CompileRequest) { request.Policy.DNSAnswerRules[0].SelectionMode = "geo" },
	} {
		t.Run(name, func(t *testing.T) {
			request := physicalOrderFixture()
			mutate(&request)
			rebindPlacement(&request)
			if _, err := Compile(request); err == nil {
				t.Fatal("accepted ambiguous authority")
			}
		})
	}
}
