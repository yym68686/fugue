package edgetopology

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func productionIntent(t *testing.T) Intent {
	t.Helper()
	raw, err := os.ReadFile("../../deploy/edge/topology.json")
	if err != nil {
		t.Fatal(err)
	}
	intent, err := Decode(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func TestProductionTopologyRetainsLegacyAuthorityBindings(t *testing.T) {
	intent := productionIntent(t)
	raw, err := os.ReadFile("../../deploy/releases/edge-groups.json")
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		Groups []struct {
			GroupID string `json:"groupId"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatal(err)
	}
	configured := make(map[string]struct{}, len(intent.Cells))
	for _, cell := range intent.Cells {
		configured[cell.LegacyGroupID] = struct{}{}
	}
	if len(configured) != len(registry.Groups) {
		t.Fatalf("topology and serving registry differ: cells=%d groups=%d", len(configured), len(registry.Groups))
	}
	for _, group := range registry.Groups {
		if _, ok := configured[group.GroupID]; !ok {
			t.Fatalf("serving group %q has no shadow authority cell", group.GroupID)
		}
	}
}

func TestTopologyRejectsAmbiguousOrCountryRisk(t *testing.T) {
	intent := productionIntent(t)
	intent.Edges[0].FailureDomains = map[string]string{"country": "us"}
	if err := intent.Validate(); err == nil || !strings.Contains(err.Error(), "failure domain") {
		t.Fatalf("country masquerading as a risk domain was accepted: %v", err)
	}
	intent = productionIntent(t)
	intent.Cells[1].LegacyGroupID = intent.Cells[0].LegacyGroupID
	if err := intent.Validate(); err == nil || !strings.Contains(err.Error(), "multiple authority cells") {
		t.Fatalf("ambiguous legacy group binding was accepted: %v", err)
	}
	intent = productionIntent(t)
	intent.Edges[1].ServingPoolIDs = []string{"pool-missing"}
	if err := intent.Validate(); err == nil || !strings.Contains(err.Error(), "unknown serving pool") {
		t.Fatalf("unknown serving pool was accepted: %v", err)
	}
	intent = productionIntent(t)
	intent.Edges[1].ID = intent.Edges[0].ID
	if err := intent.Validate(); err == nil {
		t.Fatal("duplicate Edge identity was accepted")
	}
}

func TestNeutralCellCanRetireLegacyGroupAlias(t *testing.T) {
	intent := productionIntent(t)
	intent.Cells[0].LegacyGroupID = ""
	if err := intent.Validate(); err != nil {
		t.Fatalf("neutral cell identity was rejected: %v", err)
	}
	audit := intent.Audit([]ObservedEdge{{ID: "vps-84c8f0a9", LegacyGroupID: "cell-public-a"}})
	if len(audit.MismatchedGroups) != 0 || len(audit.UnknownEdges) != 0 {
		t.Fatalf("neutral serving identity failed audit: %+v", audit)
	}
}

func TestDecodeRejectsUnknownAndTrailingFields(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/edge/topology.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range [][]byte{
		bytes.Replace(raw, []byte(`"schema_version"`), []byte(`"unknown_schema"`), 1),
		append(append([]byte(nil), raw...), []byte(`{"schema_version":"edge-topology/v1"}`)...),
	} {
		if _, err := Decode(bytes.NewReader(value)); err == nil {
			t.Fatal("invalid topology was accepted")
		}
	}
}

func TestAuditReportsDriftWithoutCreatingAuthorization(t *testing.T) {
	intent := productionIntent(t)
	observed := []ObservedEdge{
		{ID: "vps-591f4447", LegacyGroupID: "edge-group-country-de"},
		{ID: "new-edge", LegacyGroupID: "edge-group-country-us"},
	}
	got := intent.Audit(observed)
	if !reflect.DeepEqual(got.UnknownEdges, []string{"new-edge"}) ||
		!reflect.DeepEqual(got.MismatchedGroups, []string{"vps-591f4447"}) ||
		!reflect.DeepEqual(got.UnobservedEdges, []string{"bwg", "vps-84c8f0a9"}) {
		t.Fatalf("unexpected topology drift: %+v", got)
	}
}
