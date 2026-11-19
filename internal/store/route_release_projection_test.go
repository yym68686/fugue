package store

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
)

// Exercise the very SQL expression used by the repeatable-read route snapshot,
// not a Go approximation. Requires only an isolated PostgreSQL database.
func TestRouteReleaseProjectionPostgresParity(t *testing.T) {
	s := billingBatchPGStore(t)
	for _, input := range []string{`null`, `{}`, `{"image":"image","runtime_id":"runtime","replicas":1,"ports":[80],"env":{"secret":"never-export"},"command":["never-export"],"files":[{"content":"never-export"}]}`,
		`{"image":"image","runtime_id":"runtime","replicas":3,"ports":[443],"network_mode":"internal","workload_generation":"generation","continuity":{"zero_downtime":{"enabled":true,"mode":"safe","strategy":"never-export"}},"ssh":{},"workspace":{},"persistent_storage":{},"data":{},"failover":{},"node_loss":{},"volume_replication":{},"postgres":{}}`,
		`{"workspace":null,"continuity":{"zero_downtime":null}}`} {
		var raw []byte
		if err := s.db.QueryRow("SELECT "+routeBusinessReleaseSpecSQL+" FROM (SELECT $1::jsonb AS spec_snapshot_json) AS fixture", input).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var spec, got *model.AppSpec
		if err := json.Unmarshal([]byte(input), &spec); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		want := routeReleaseSpec(spec)
		if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(routeReleaseSpec(got), want) || strings.Contains(string(raw), "never-export") {
			t.Fatalf("projection mismatch: %s", raw)
		}
	}
}
