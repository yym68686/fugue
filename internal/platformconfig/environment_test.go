package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestImportEnvironmentPreservesAllStaticDNSRecords(t *testing.T) {
	types := []string{"A", "AAAA", "CAA", "CNAME", "MX", "NS", "SRV", "TXT"}
	values := []string{"192.0.2.4", "2001:db8::4", `0 issue "ca.example"`, "target.example", "10 mail.example.", "ns.example", "10 5 443 service.example.", "v=spf1 -all"}
	rows := make([]map[string]any, len(types))
	for i, kind := range types {
		rows[i] = map[string]any{"name": "records.example", "type": kind, "values": []string{values[i]}, "ttl": 300, "record_kind": "protected", "status": "active"}
	}
	rows[0]["values"] = []string{"192.0.2.4", "192.0.2.5"}
	for _, envelope := range []bool{false, true} {
		var input any = rows
		if envelope {
			input = map[string]any{"records": rows}
		}
		raw, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		result, err := ImportEnvironment(map[string]string{"FUGUE_DNS_STATIC_RECORDS_JSON": string(raw)}, "dns-import")
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Intent.DNS) != len(rows) {
			t.Fatalf("lost records: %+v", result.Intent.DNS)
		}
		byType := map[string]DNSIntent{}
		for _, row := range result.Intent.DNS {
			byType[row.Type] = row
		}
		for i, kind := range types {
			got := byType[kind]
			want := rows[i]["values"].([]string)
			if !reflect.DeepEqual(got.Values, want) || got.TTL != 300 || got.RecordKind != "protected" || got.Status != "active" {
				t.Fatalf("%s record changed: %+v", kind, got)
			}
		}
	}
}

func TestImportEnvironmentRejectsPartialOrUnsupportedInput(t *testing.T) {
	for name, raw := range map[string]string{
		"unknown envelope": `{"unknown":[]}`,
		"null envelope":    `{"records":null}`,
		"unknown field":    `[{"name":"a.example","type":"A","values":["192.0.2.1"],"new_option":true}]`,
		"mixed IP family":  `[{"name":"a.example","type":"A","values":["192.0.2.1","2001:db8::1"]}]`,
		"unsupported type": `[{"name":"a.example","type":"HTTPS","values":["1 ."]}]`,
		"empty value":      `[{"name":"a.example","type":"TXT","values":[" "]}]`,
		"trailing JSON":    `[] []`,
	} {
		t.Run(name, func(t *testing.T) {
			result, err := ImportEnvironment(map[string]string{"FUGUE_DNS_STATIC_RECORDS_JSON": raw}, "invalid-import")
			if err == nil || len(result.ImportedKeys) != 0 {
				t.Fatalf("partial import accepted: %+v %v", result, err)
			}
		})
	}
}

func TestImportEnvironmentRejectsUnknownRoutePolicy(t *testing.T) {
	for _, raw := range []string{
		`[{"hostname":"a.example","upstream_url":"http://origin","route_policy":"unknown"}]`,
		`[{"hostname":"a.example","upstream_url":"http://origin","edge_group_mode":"pinned"}]`,
		`[{"hostname":"a.example","upstream_url":"http://origin","status":"unknown"}]`,
		`[{"hostname":"a.example","upstream_url":"http://origin","unknown_field":true}]`,
		`[{"hostname":"a.example","upstream_url":"http://origin"},{"hostname":"a.example","upstream_url":"http://other"}]`,
	} {
		if _, err := ImportEnvironment(map[string]string{"FUGUE_PLATFORM_ROUTES_JSON": raw}, "invalid-routes"); err == nil {
			t.Fatalf("invalid routes accepted: %s", raw)
		}
	}
}
