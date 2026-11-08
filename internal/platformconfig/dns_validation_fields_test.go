package platformconfig

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

type dnsChildValidationView struct {
	Policy  PolicySnapshot `json:"policy"`
	Lineage Lineage        `json:"lineage"`
}

func dnsChildViewError(err error) string {
	if e, ok := err.(*json.UnmarshalTypeError); ok {
		// Byte offsets change when ignored fields are omitted. The public
		// validators return their existing fixed error, never this offset.
		return fmt.Sprintf("%s/%s/%s/%s", e.Value, e.Type, e.Struct, e.Field)
	}
	return fmt.Sprint(err)
}

func checkDNSChildValidationView(t testing.TB, content map[string]any) {
	t.Helper()
	before, _ := json.Marshal(content)
	wantHash, wantErr := Digest(content)
	gotHash, full, native, gotErr := dnsValidationContentDigest(content)
	if gotHash != wantHash || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
		t.Fatalf("complete content digest changed: %s/%v %s/%v", wantHash, wantErr, gotHash, gotErr)
	}
	if gotErr != nil {
		return
	}
	subset, err := marshalDNSChildValidationFields(content, full, native)
	if err != nil {
		t.Fatal(err)
	}
	var old, next dnsChildValidationView
	a, b := json.Unmarshal(full, &old), json.Unmarshal(subset, &next)
	if dnsChildViewError(a) != dnsChildViewError(b) || !reflect.DeepEqual(old, next) {
		t.Fatalf("full typed view changed: %v/%v equal=%t", a, b, reflect.DeepEqual(old, next))
	}
	after, _ := json.Marshal(content)
	if string(before) != string(after) {
		t.Fatal("content mutated")
	}
}

func TestDNSChildValidationFieldsMatchFullEncoding(t *testing.T) {
	for _, padding := range []int{0, (64 << 10) - 200, 64 << 10, 256 << 10} {
		for _, value := range []any{nil, false, float64(1), "bad", []any{}, map[string]any{}, map[string]any{"schema_version": "bad"}} {
			for _, field := range []string{"policy", "lineage", "POLICY", "LiNeAgE"} {
				t.Run(fmt.Sprintf("%d/%s/%T", padding, field, value), func(t *testing.T) {
					checkDNSChildValidationView(t, map[string]any{"padding": strings.Repeat("x", padding), field: value})
				})
			}
		}
	}
	for _, content := range []map[string]any{
		nil, {}, {"padding": strings.Repeat("x", 64<<10)},
		{"padding": strings.Repeat("x", 64<<10), "lineage": false, "POLICY": false, "policy": nil},
		{"padding": strings.Repeat("x", 64<<10), "POLICY": map[string]any{"generation": "before"}, "policy": map[string]any{"scope": "after"}},
		{"padding": strings.Repeat("x", 64<<10), "LiNeAgE": map[string]any{"compiler_version": "before"}, "lineage": nil},
		{"padding": string([]byte{0xff, '<', '&'}), "number": math.Inf(1)},
	} {
		checkDNSChildValidationView(t, content)
	}
	// Real signed composition fixtures exercise the complete public validators
	// as well as their exact typed views on large native children.
	p := cachedCellPublicationFixture(t, 256<<10)
	for _, content := range []map[string]any{p.Parent.Content, p.Route.Content, p.TLS.Content} {
		checkDNSChildValidationView(t, content)
	}
	if err := ValidateCellRoutePublication(p); err != nil {
		t.Fatal(err)
	}
	p.Route.Content["padding"] = "changed without updating full SHA"
	if ValidateCellRoutePublication(p) == nil {
		t.Fatal("subset decoding bypassed full actual content digest")
	}
}

func TestDNSChildValidationFieldsKeepCustomEncoding(t *testing.T) {
	calls := 0
	content := map[string]any{"padding": strings.Repeat("x", 64<<10), "ignored": countedDNSPlanJSON{raw: []byte(`true`), calls: &calls}}
	_, full, native, err := dnsValidationContentDigest(content)
	if err != nil || calls != 1 {
		t.Fatalf("first complete encoder changed: %v calls=%d", err, calls)
	}
	raw, err := marshalDNSChildValidationFields(content, full, native)
	if err != nil || calls != 2 || string(raw) != string(full) {
		t.Fatalf("custom second complete encoding changed: %v calls=%d", err, calls)
	}
	for _, value := range []any{json.RawMessage(`{}`), 1, []string{"typed"}, map[string]string{"typed": "map"}} {
		remaining := 65536
		if dnsValidationNativeTree(value, 0, &remaining) {
			t.Fatalf("non-native value accepted: %T", value)
		}
	}
	cycle := map[string]any{}
	cycle["self"] = cycle
	remaining := 65536
	if dnsValidationNativeTree(cycle, 0, &remaining) {
		t.Fatal("cyclic value accepted")
	}
	remaining = 1
	if dnsValidationNativeTree(map[string]any{"value": true}, 0, &remaining) {
		t.Fatal("structure work budget ignored")
	}
}

type changingDNSChildJSON struct{ change func() }

func (v changingDNSChildJSON) MarshalJSON() ([]byte, error) {
	v.change()
	return []byte(`true`), nil
}

func TestDNSChildValidationFieldsKeepEncoderMutationErrors(t *testing.T) {
	content := map[string]any{"padding": strings.Repeat("x", 64<<10)}
	content["custom"] = changingDNSChildJSON{change: func() {
		content["custom"] = math.Inf(1)
	}}
	_, full, native, err := dnsValidationContentDigest(content)
	if err != nil || native {
		t.Fatalf("custom first encoder qualification: %v native=%t", err, native)
	}
	_, want := json.Marshal(content)
	_, got := marshalDNSChildValidationFields(content, full, native)
	if want == nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("second encoder mutation error changed: %v / %v", want, got)
	}
	// A later validation hook also cannot turn an ignored native field into
	// invalid JSON without retaining the original second-encoder rejection.
	content = map[string]any{"padding": strings.Repeat("x", 64<<10)}
	_, full, native, err = dnsValidationContentDigest(content)
	if err != nil || !native {
		t.Fatal("native fixture qualification failed")
	}
	content["ignored"] = math.NaN()
	_, want = json.Marshal(content)
	_, got = marshalDNSChildValidationFields(content, full, native)
	if want == nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("post-hash invalid value error changed: %v / %v", want, got)
	}
}

func FuzzDNSChildValidationFieldsMatchFullEncoding(f *testing.F) {
	f.Add([]byte(`{"policy":{},"lineage":null}`))
	f.Add([]byte(`{"POLICY":{"scope":"one"},"policy":{"scope":"two"},"lineage":false}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		var content map[string]any
		if json.Unmarshal(raw, &content) != nil || content == nil {
			return
		}
		content["ignored_padding"] = strings.Repeat("x", 64<<10)
		checkDNSChildValidationView(t, content)
	})
}
