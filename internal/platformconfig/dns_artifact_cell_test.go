package platformconfig_test

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
	"reflect"
	"testing"
)

func TestDNSArtifactDecoderPreservesCellPlanRequirements(t *testing.T) {
	compiled := celldns.Compile(t, celldns.Request(t))
	artifact := compiled.DNSArtifact
	payload, err := platformconfig.DecodeDNSArtifact(artifact)
	if err != nil {
		t.Fatal(err)
	}
	var expected platformconfig.DNSReadinessPlan
	raw, err := json.Marshal(artifact.Content["readiness_plan"])
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(payload.ReadinessPlan, &expected) || payload.Policy.PublicationRole != platformconfig.PublicationRoleCellDNS || len(payload.CellRoutePublications) != 2 {
		t.Fatal("decoded Cell publication changed requirements")
	}
	raw, err = json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	var changed model.PlatformArtifact
	if err = json.Unmarshal(raw, &changed); err != nil {
		t.Fatal(err)
	}
	plan := changed.Content["readiness_plan"].(map[string]any)
	plan["probes"].([]any)[0].(map[string]any)["route_digest"] = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	if err = platformconfig.ValidateDNSCellPlan(changed); err == nil {
		t.Fatal("fixture mutation did not change bound Cell requirements")
	}
	if _, err = platformconfig.DecodeDNSArtifact(changed); err == nil {
		t.Fatal("combined decoder skipped Cell plan validation")
	}
}
