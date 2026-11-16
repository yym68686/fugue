package platformconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// Keep the previous copying envelope as the compatibility and allocation oracle.
func copyingCellRoutePublications(raw []byte) ([]CellRoutePublicationInput, PolicySnapshot, error) {
	var payload struct {
		Publications json.RawMessage    `json:"cell_route_publications"`
		Policy       json.RawMessage    `json:"policy"`
		Source       *CellDNSPlanSource `json:"cell_dns_source"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, PolicySnapshot{}, fmt.Errorf("DNS artifact envelope cannot be decoded: %w", err)
	}
	return decodeCellRoutePublicationFields(payload.Publications, payload.Policy, payload.Source)
}

func checkCellPublicationDecode(t testing.TB, raw []byte) {
	t.Helper()
	want, wantPolicy, wantErr := copyingCellRoutePublications(raw)
	got, gotPolicy, gotErr := decodeCellRoutePublications(raw)
	if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(gotPolicy, wantPolicy) {
		t.Fatalf("borrowed envelope changed typed views or rejection: old=%v new=%v", wantErr, gotErr)
	}
	var oldType, newType *json.UnmarshalTypeError
	if errors.As(wantErr, &oldType) != errors.As(gotErr, &newType) || !reflect.DeepEqual(oldType, newType) {
		t.Fatal("borrowed envelope changed the wrapped type error")
	}
}

func TestCellPublicationBorrowedEnvelopePreservesDecoder(t *testing.T) {
	for _, raw := range []string{
		`null`, `{}`, `[]`, `false`, `{`,
		`{"policy":false,"cell_dns_source":false}`,
		`{"policy":{},"cell_route_publications":false}`,
		`{"cell_route_publications":[],"policy":null,"POLICY":{"scope":"test"}}`,
		`{"Policy":{"scope":"earlier"},"policy":{"generation":"later"}}`,
		`{"cell_dns_source":{},"CELL_DNS_SOURCE":null,"policy":false}`,
		`{"policy":false,"policy":{}}`,
		`{"policy":false,"cell_route_publications":false,"cell_dns_source":null}`,
	} {
		checkCellPublicationDecode(t, []byte(raw))
	}
	p := cachedCellPublicationFixture(t, 256<<10)
	raw, err := json.Marshal(map[string]any{"policy": map[string]any{"publication_role": PublicationRoleCellDNS, "authority_cell_id": "dns-authority"}, "cell_route_publications": []CellRoutePublicationInput{p}})
	if err != nil {
		t.Fatal(err)
	}
	checkCellPublicationDecode(t, raw)
	pubs, _, err := decodeCellRoutePublications(raw)
	if err != nil || len(pubs) != 1 {
		t.Fatal("invalid publication fixture", err)
	}
	before, _ := json.Marshal(pubs)
	clear(raw)
	after, _ := json.Marshal(pubs)
	if string(before) != string(after) {
		t.Fatal("returned publications retained borrowed JSON bytes")
	}
	pubs[0].Route.Content["padding"] = "changed"
	if p.Route.Content["padding"] == "changed" {
		t.Fatal("returned publications alias the source tree")
	}
}

func FuzzCellPublicationBorrowedEnvelope(f *testing.F) {
	f.Add([]byte(`{"policy":{},"cell_route_publications":[]}`))
	f.Add([]byte(`{"Policy":false,"policy":null,"cell_dns_source":false}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		checkCellPublicationDecode(t, raw)
	})
}

func BenchmarkCellPublicationEnvelope(b *testing.B) {
	for _, padding := range []int{0, 256 << 10} {
		p := cachedCellPublicationFixture(b, padding)
		raw, err := json.Marshal(map[string]any{"policy": map[string]any{"publication_role": PublicationRoleCellDNS, "authority_cell_id": "dns-authority"}, "cell_route_publications": []CellRoutePublicationInput{p}})
		if err != nil {
			b.Fatal(err)
		}
		for _, trial := range []struct {
			name string
			run  func([]byte) ([]CellRoutePublicationInput, PolicySnapshot, error)
		}{{"copying", copyingCellRoutePublications}, {"current", decodeCellRoutePublications}} {
			b.Run(fmt.Sprintf("padding-%d/%s", padding, trial.name), func(b *testing.B) {
				if _, _, err := trial.run(raw); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, _, err := trial.run(raw); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
