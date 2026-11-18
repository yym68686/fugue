package platformconfig

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
)

// The independent full encoder is the compatibility oracle. Source decoding is
// typed (including alias merge/null semantics); behavior decoding is a generic
// map and must preserve all unknown fields inside policy and node views.
func handoffDecodedFields(raw []byte, source bool) (any, error) {
	if source {
		var value struct {
			Source *CellDNSPlanSource `json:"cell_dns_source"`
		}
		err := json.Unmarshal(raw, &value)
		return value, err
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	fields := map[string]any{}
	for _, key := range []string{"policy", "consumer_views", "query_views"} {
		fields[key] = value[key]
	}
	return fields, nil
}

func compareHandoffFields(t testing.TB, content map[string]any) {
	t.Helper()
	for _, source := range []bool{false, true} {
		full, fullErr := json.Marshal(content)
		projected, projectedErr := marshalDNSHandoffFields(content, source)
		if fmt.Sprint(fullErr) != fmt.Sprint(projectedErr) {
			t.Fatalf("encoder rejection changed: source=%t full=%v projected=%v", source, fullErr, projectedErr)
		}
		if fullErr != nil {
			continue
		}
		want, wantErr := handoffDecodedFields(full, source)
		got, gotErr := handoffDecodedFields(projected, source)
		if fmt.Sprint(wantErr) != fmt.Sprint(gotErr) || !reflect.DeepEqual(want, got) {
			t.Fatalf("handoff decode changed: source=%t full=%v projected=%v equal=%t", source, wantErr, gotErr, reflect.DeepEqual(want, got))
		}
	}
}

func TestDNSHandoffFieldsMatchFullEncoding(t *testing.T) {
	for index, content := range []map[string]any{
		nil, {},
		{"policy": nil, "consumer_views": nil, "cell_dns_source": nil},
		{"policy": map[string]any{"future_behavior": []any{true, "\xff", 9007199254740993.0}}, "query_views": []any{map[string]any{"future_view": "kept"}}},
		{"Policy": map[string]any{"future_behavior": true}, "QUERY_VIEWS": []any{true}},
		{"CELL_DNS_SOURCE": map[string]any{"intent": map[string]any{"generation": "first"}}, "cell_dns_source": map[string]any{"intent": map[string]any{"scope": "global"}}},
		{"CELL_DNS_SOURCE": map[string]any{"intent": map[string]any{"generation": "first"}}, "cell_dns_source": nil},
		{"cell_dnſ_ſource": false},
		{"Cell_DnS_Source": false, "cell_dns_source": nil},
		{"cell_dns_source": map[string]any{"intent": false}},
		{"ignored": math.NaN()}, {"ignored": math.Inf(1)},
		{"ignored": func() {}}, {"ignored": json.Number("invalid")},
		{"ignored": json.RawMessage(`invalid`)},
		{"policy": map[string]any{"invalid": func() {}}},
	} {
		t.Run(fmt.Sprint(index), func(t *testing.T) { compareHandoffFields(t, content) })
	}
	cycle := map[string]any{}
	cycle["self"] = cycle
	compareHandoffFields(t, cycle)
	var deep any
	for range 300 {
		deep = []any{deep}
	}
	compareHandoffFields(t, map[string]any{"ignored": deep})
	compareHandoffFields(t, map[string]any{"ignored": make([]any, 262144)})
	compareHandoffFields(t, map[string]any{"ignored": make([]any, 150000), "padding": strings.Repeat("x", 2<<20)})
}

type countedHandoffJSON struct {
	raw   json.RawMessage
	calls *int
}

func (value countedHandoffJSON) MarshalJSON() ([]byte, error) {
	(*value.calls)++
	return value.raw, nil
}

func TestDNSHandoffFieldsKeepIgnoredEncoderEffects(t *testing.T) {
	for _, source := range []bool{false, true} {
		for _, raw := range []string{`true`, `invalid`} {
			calls := 0
			content := map[string]any{"ignored": countedHandoffJSON{json.RawMessage(raw), &calls}}
			_, err := marshalDNSHandoffFields(content, source)
			if calls != 1 || (err != nil) != (raw == "invalid") {
				t.Fatalf("ignored encoder effects changed: source=%t calls=%d err=%v", source, calls, err)
			}
		}
	}
}

func TestDNSHandoffFieldsOwnDecodedUnknownBehavior(t *testing.T) {
	content := map[string]any{
		"policy":         map[string]any{"future_behavior": map[string]any{"enabled": true}},
		"consumer_views": []any{map[string]any{"future_view": []any{"first", "second"}}},
		"query_views":    []any{map[string]any{"records": []any{map[string]any{"future_record": true}}}},
		"ignored":        strings.Repeat("x", 256<<10),
	}
	before, _ := json.Marshal(content)
	raw, err := marshalDNSHandoffFields(content, false)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	result["policy"].(map[string]any)["future_behavior"].(map[string]any)["enabled"] = false
	result["consumer_views"].([]any)[0].(map[string]any)["future_view"].([]any)[0] = "changed"
	result["query_views"].([]any)[0].(map[string]any)["records"].([]any)[0].(map[string]any)["future_record"] = false
	after, _ := json.Marshal(content)
	if string(before) != string(after) {
		t.Fatal("decoded behavior aliases signed source content")
	}
}

func TestDNSHandoffDocumentsPreserveNodeBehaviorAndOwnership(t *testing.T) {
	content := map[string]any{
		"policy": map[string]any{
			"dns_query_policy":    map[string]any{"future": []any{map[string]any{"edge_group_id": "legacy", "bytes": "\xff"}}},
			"dns_authorities":     []any{map[string]any{"node_id": "first"}, map[string]any{"node_id": "second"}},
			"dns_client_policies": []any{map[string]any{"node_id": "first"}, map[string]any{"node_id": "second"}},
		},
		"consumer_views": []any{
			map[string]any{"node_id": "first", "edge_group_id": "legacy"},
			map[string]any{"node_id": "second", "edge_group_id": "legacy"},
		},
		"query_views": []any{
			map[string]any{"node_id": "first", "edge_group_id": "legacy", "records": []any{
				map[string]any{"name": "second.example.test", "future": []any{"one", "two"}},
				map[string]any{"name": "first.example.test", "record_kind": model.EdgeDNSRecordKindProbe, "edge_group_id": "legacy"},
			}},
			map[string]any{"node_id": "second", "edge_group_id": "legacy", "records": []any{false}},
		},
	}
	a := model.PlatformArtifact{Content: content}
	before, _ := json.Marshal(content)
	shared, _, ok := decodeDNSHandoffDocumentsOnce(a, a)
	if !ok {
		t.Fatal("ordinary JSON did not enter shared decode")
	}
	aliases := map[string]string{"legacy": "middle", "middle": "final"}
	for _, node := range []string{"first", "second", "unconfigured", "first"} {
		want, err := dnsHandoffBehavior(a, node, "authority", aliases)
		got, gotErr := shared.behavior(node, "authority", aliases)
		if fmt.Sprint(gotErr) != fmt.Sprint(err) || !reflect.DeepEqual(got, want) {
			t.Fatalf("node %q shared behavior differs: got %v want %v", node, gotErr, err)
		}
		if got != nil {
			got["dns_query_policy"].(map[string]any)["future"].([]any)[0].(map[string]any)["edge_group_id"] = "changed"
			got["query_views"].([]any)[0].(map[string]any)["edge_group_id"] = "changed"
		}
	}
	after, _ := json.Marshal(content)
	if string(after) != string(before) {
		t.Fatal("signed content was mutated")
	}
}

func TestDNSHandoffDocumentsKeepCustomEncoderOrder(t *testing.T) {
	calls := 0
	plain := model.PlatformArtifact{Content: map[string]any{}}
	custom := model.PlatformArtifact{Content: map[string]any{
		"ignored": countedHandoffJSON{json.RawMessage("invalid"), &calls},
	}}
	for _, pair := range [][2]model.PlatformArtifact{{plain, custom}, {custom, plain}} {
		if _, _, ok := decodeDNSHandoffDocumentsOnce(pair[0], pair[1]); ok || calls != 0 {
			t.Fatalf("reusable decode evaluated a custom encoder, calls=%d", calls)
		}
	}
	want := "DNS handoff policy missing"
	if err := compareDNSHandoffNode(plain, custom, "node", PlatformIntent{}, nil, nil); fmt.Sprint(err) != want || calls != 0 {
		t.Fatalf("predecessor rejection no longer short-circuits custom next encoding: %v calls=%d", err, calls)
	}
	if err := compareDNSHandoffNode(custom, plain, "node", PlatformIntent{}, nil, nil); err == nil || calls != 1 {
		t.Fatalf("custom predecessor encoding did not preserve error/effect: %v calls=%d", err, calls)
	}
}

func FuzzDNSHandoffFieldsMatchFullEncoding(f *testing.F) {
	for _, raw := range []string{`{}`, `{"policy":null}`, `{"cell_dns_source":false}`, `{"CELL_DNS_SOURCE":{"intent":{}},"cell_dns_source":null}`, `{"policy":{"future":true},"Policy":false}`} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		var content map[string]any
		if json.Unmarshal(raw, &content) != nil || content == nil {
			return
		}
		compareHandoffFields(t, content)
	})
}

func BenchmarkDNSHandoffFields(b *testing.B) {
	content := map[string]any{
		"policy":          map[string]any{"future_behavior": true},
		"cell_dns_source": map[string]any{"intent": map[string]any{"scope": "global"}},
		"consumer_views":  []any{map[string]any{"node_id": "dns-a"}},
		"query_views":     []any{map[string]any{"node_id": "dns-a"}},
		"retained":        map[string]any{"padding": strings.Repeat("x", 2<<20), "values": make([]any, 150000)},
	}
	for _, source := range []bool{false, true} {
		for _, projected := range []bool{false, true} {
			b.Run(fmt.Sprintf("source-%t/projected-%t", source, projected), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					var err error
					if projected {
						_, err = marshalDNSHandoffFields(content, source)
					} else {
						_, err = json.Marshal(content)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
