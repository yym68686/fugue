package platformconfig_test

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

// Keep the original complete-envelope decoder independent of field projection.
func fullPreviousPublicationDecode(a model.PlatformArtifact) (*platformconfig.PreviousTrafficPublicationInput, error) {
	raw, err := json.Marshal(a.Content)
	if err != nil {
		return nil, fmt.Errorf("previous traffic publication cannot be decoded")
	}
	var payload struct {
		Previous *platformconfig.PreviousTrafficPublicationInput `json:"previous_traffic_publication"`
		Source   *platformconfig.CellDNSPlanSource               `json:"cell_dns_source"`
	}
	if json.Unmarshal(raw, &payload) != nil {
		return nil, fmt.Errorf("previous traffic publication cannot be decoded")
	}
	declared := payload.Source != nil && payload.Source.Intent.RouteAuthorityTransition != nil
	if (payload.Previous != nil) != declared {
		return nil, fmt.Errorf("previous publication requires explicit signed transition")
	}
	if payload.Previous != nil {
		if err := platformconfig.ValidatePreviousTrafficPublication(*payload.Previous); err != nil {
			return nil, err
		}
		if payload.Previous.Reference != payload.Source.Intent.RouteAuthorityTransition.PreviousPublication {
			return nil, fmt.Errorf("previous publication differs from intent")
		}
	}
	return payload.Previous, nil
}

func previousDecodeFixture(t testing.TB, transition bool, padding int) model.PlatformArtifact {
	t.Helper()
	req, _, _ := celldns.SourceAuthorizedRequest(t, transition)
	a := celldns.Compile(t, req).DNSArtifact
	a.Content["ignored_padding"] = strings.Repeat("x", padding)
	return a
}

func comparePreviousDecode(t testing.TB, a model.PlatformArtifact) {
	t.Helper()
	want, oldErr := fullPreviousPublicationDecode(a)
	got, newErr := platformconfig.DecodePreviousTrafficPublication(a)
	if fmt.Sprint(oldErr) != fmt.Sprint(newErr) || !reflect.DeepEqual(want, got) {
		t.Fatalf("previous publication decode changed: old=%v new=%v equal=%t", oldErr, newErr, reflect.DeepEqual(want, got))
	}
}

func TestPreviousPublicationDecodeMatchesFullEnvelope(t *testing.T) {
	for _, transition := range []bool{false, true} {
		original := previousDecodeFixture(t, transition, 256<<10)
		encoded, err := json.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		changes := []func(map[string]any){
			func(map[string]any) {},
			func(c map[string]any) { c["ignored"] = math.NaN() },
			func(c map[string]any) { c["ignored"] = func() {} },
			func(c map[string]any) { c["cell_dns_source"] = false },
			func(c map[string]any) { c["previous_traffic_publication"] = false },
			func(c map[string]any) { c["cell_dns_source"], c["previous_traffic_publication"] = nil, nil },
			func(c map[string]any) { c["CELL_DNS_SOURCE"] = c["cell_dns_source"]; delete(c, "cell_dns_source") },
			func(c map[string]any) { c["Cell_DnS_Source"] = false },
			func(c map[string]any) { c["cell_dnſ_ſource"] = false },
			func(c map[string]any) {
				c["PREVIOUS_TRAFFIC_PUBLICATION"] = c["previous_traffic_publication"]
				c["previous_traffic_publication"] = nil
			},
			func(c map[string]any) { c["PREVIOUS_TRAFFIC_PUBLICATION"] = map[string]any{"reference": false} },
			func(c map[string]any) { c["ignored"] = json.Number("invalid") },
			func(c map[string]any) { c["ignored"] = json.RawMessage(`invalid`) },
		}
		for i, change := range changes {
			t.Run(fmt.Sprintf("transition-%t/mutation-%d", transition, i), func(t *testing.T) {
				var a model.PlatformArtifact
				if err := json.Unmarshal(encoded, &a); err != nil {
					t.Fatal(err)
				}
				change(a.Content)
				comparePreviousDecode(t, a)
			})
		}
	}
	for _, content := range []map[string]any{nil, {}, {"cell_dns_source": nil}, {"previous_traffic_publication": nil}, {"ignored": "\xff"}} {
		comparePreviousDecode(t, model.PlatformArtifact{Content: content})
	}
}

type countedPreviousJSON struct {
	raw   json.RawMessage
	calls *int
}

func (c countedPreviousJSON) MarshalJSON() ([]byte, error) { (*c.calls)++; return c.raw, nil }

func TestPreviousPublicationDecodeKeepsIgnoredEncoderEffects(t *testing.T) {
	for _, raw := range []string{`true`, `invalid`} {
		calls := 0
		a := model.PlatformArtifact{Content: map[string]any{
			"ignored": countedPreviousJSON{raw: json.RawMessage(raw), calls: &calls},
			"padding": strings.Repeat("x", 256<<10),
		}}
		_, err := platformconfig.DecodePreviousTrafficPublication(a)
		if calls != 1 || (err != nil) != (raw == "invalid") {
			t.Fatalf("ignored custom encoder changed: calls=%d err=%v", calls, err)
		}
	}
	cycle := map[string]any{}
	cycle["self"] = cycle
	comparePreviousDecode(t, model.PlatformArtifact{Content: cycle})
	deep := any(nil)
	for range 300 {
		deep = []any{deep}
	}
	comparePreviousDecode(t, model.PlatformArtifact{Content: map[string]any{"ignored": deep}})
	comparePreviousDecode(t, model.PlatformArtifact{Content: map[string]any{"ignored": make([]any, 262144)}})
}

func FuzzPreviousPublicationDecodeMatchesFullEnvelope(f *testing.F) {
	for _, raw := range []string{
		`{}`, `{"cell_dns_source":null,"previous_traffic_publication":null}`,
		`{"CELL_DNS_SOURCE":{"intent":{}},"cell_dns_source":null}`,
		`{"PREVIOUS_TRAFFIC_PUBLICATION":{"reference":false},"cell_dns_source":false}`,
	} {
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
		content["ignored_padding"] = strings.Repeat("x", 64<<10)
		comparePreviousDecode(t, model.PlatformArtifact{Content: content})
	})
}

func TestPreviousPublicationDecodeOwnsReturnedContent(t *testing.T) {
	a := previousDecodeFixture(t, true, 256<<10)
	got, err := platformconfig.DecodePreviousTrafficPublication(a)
	if err != nil || got == nil {
		t.Fatal("valid transition fixture required", err)
	}
	before, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	a.Content["previous_traffic_publication"].(map[string]any)["route"].(map[string]any)["content"].(map[string]any)["caller_change"] = true
	after, _ := json.Marshal(got)
	if string(before) != string(after) {
		t.Fatal("caller mutation changed returned predecessor")
	}
	before, _ = json.Marshal(a)
	got.Route.Content["returned_change"] = true
	after, _ = json.Marshal(a)
	if string(before) != string(after) {
		t.Fatal("returned predecessor aliases caller content")
	}
}

func TestPreviousPublicationDecodeLargeNativeEnvelope(t *testing.T) {
	for _, transition := range []bool{false, true} {
		a := previousDecodeFixture(t, transition, 256<<10)
		// Exercise a large tree, independently of large string bodies.
		a.Content["ignored_values"] = make([]any, 150000)
		comparePreviousDecode(t, a)
	}
}

func BenchmarkPreviousPublicationLargeNativeEnvelope(b *testing.B) {
	a := previousDecodeFixture(b, true, 2<<20)
	a.Content["ignored_values"] = make([]any, 150000)
	for _, variant := range []struct {
		name string
		run  func(model.PlatformArtifact) (*platformconfig.PreviousTrafficPublicationInput, error)
	}{{"full", fullPreviousPublicationDecode}, {"current", platformconfig.DecodePreviousTrafficPublication}} {
		b.Run(variant.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := variant.run(a); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPreviousPublicationEnvelope(b *testing.B) {
	for _, transition := range []bool{false, true} {
		for _, padding := range []int{0, 256 << 10, 2 << 20} {
			a := previousDecodeFixture(b, transition, padding)
			for _, variant := range []struct {
				name string
				run  func(model.PlatformArtifact) (*platformconfig.PreviousTrafficPublicationInput, error)
			}{{"full", fullPreviousPublicationDecode}, {"current", platformconfig.DecodePreviousTrafficPublication}} {
				b.Run(fmt.Sprintf("transition-%t/padding-%d/%s", transition, padding, variant.name), func(b *testing.B) {
					if _, err := variant.run(a); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := variant.run(a); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
