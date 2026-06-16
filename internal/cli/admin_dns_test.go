package cli

import "testing"

func TestDNSAnswerEdgeReadyAllowsDNSTargetInventoryCheck(t *testing.T) {
	t.Parallel()

	if !dnsAnswerEdgeReady([]string{"edge-group-country-us"}, nil, true) {
		t.Fatal("expected DNS target answers mapped to an edge group to pass inventory readiness")
	}
	if dnsAnswerEdgeReady(nil, nil, true) {
		t.Fatal("expected DNS target answers without edge group inventory to fail")
	}
}

func TestDNSAnswerEdgeReadyRequiresRouteReadyForHTTPRoutes(t *testing.T) {
	t.Parallel()

	routeReady := map[string]bool{"edge-group-country-de": true}
	if !dnsAnswerEdgeReady([]string{"edge-group-country-us", "edge-group-country-de"}, routeReady, false) {
		t.Fatal("expected HTTP route answer to pass when one answer edge group is route-ready")
	}
	if dnsAnswerEdgeReady([]string{"edge-group-country-us"}, routeReady, false) {
		t.Fatal("expected HTTP route answer to fail when no answer edge group is route-ready")
	}
}

func TestUniqueStringsPreserveOrder(t *testing.T) {
	t.Parallel()

	got := uniqueStringsPreserveOrder([]string{"51.38.126.103", "15.204.94.71", "51.38.126.103", "", "15.204.94.71"})
	want := []string{"51.38.126.103", "15.204.94.71"}
	if len(got) != len(want) {
		t.Fatalf("expected %d values, got %+v", len(want), got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("expected order %+v, got %+v", want, got)
		}
	}
}
