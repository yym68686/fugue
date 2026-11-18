package platformconfig

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
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
