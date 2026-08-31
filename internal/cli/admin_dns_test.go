package cli

import (
	"strings"
	"testing"

	"fugue/internal/model"
)

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

func TestRouteReadyEdgeGroupsExpandsUnpinnedHealthyGroups(t *testing.T) {
	t.Parallel()

	got := routeReadyEdgeGroups(model.RouteExplainResponse{
		Route: &model.EdgeRouteBinding{
			Status:      model.EdgeRouteStatusActive,
			RoutePolicy: model.EdgeRoutePolicyEnabled,
			UpstreamURL: "http://fugue-fugue.fugue-system.svc.cluster.local:80",
		},
		HealthyEdgeGroups: map[string]bool{
			"edge-group-country-de": true,
			"edge-group-country-us": true,
		},
	})
	if !got["edge-group-country-de"] || !got["edge-group-country-us"] || len(got) != 2 {
		t.Fatalf("expected unpinned route to expand to both healthy groups, got %+v", got)
	}
}

func TestDNSAnswerCheckQueryHostnameUsesFugueZoneCNAMECandidate(t *testing.T) {
	t.Parallel()

	nodes := []model.DNSNode{{Zone: "fugue.pro"}}
	got := dnsAnswerCheckQueryHostnameFromCandidates("api.example.com", nodes, []string{
		"api.example.com",
		"d-shared.dns.fugue.pro.",
	})
	if got != "d-shared.dns.fugue.pro" {
		t.Fatalf("expected Fugue DNS target query name, got %q", got)
	}
}

func TestDNSAnswerCheckQueryHostnameKeepsServedHostname(t *testing.T) {
	t.Parallel()

	nodes := []model.DNSNode{{Zone: "fugue.pro"}}
	got := dnsAnswerCheckQueryHostnameFromCandidates("d-shared.dns.fugue.pro", nodes, []string{
		"other.example.com",
	})
	if got != "d-shared.dns.fugue.pro" {
		t.Fatalf("expected original served hostname, got %q", got)
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

func TestSummarizeAuthoritativeAnswerSetsReportsUSDEAnswerSplit(t *testing.T) {
	t.Parallel()

	consistent, sets, reasons := summarizeAuthoritativeAnswerSets([]dnsAnswerCheckNode{
		{DNSNodeID: "dns-us", EdgeGroupID: "edge-group-country-us", QueryOK: true, Answers: []string{}},
		{DNSNodeID: "dns-de", EdgeGroupID: "edge-group-country-de", QueryOK: true, Answers: []string{"15.204.94.71"}},
	})
	if consistent {
		t.Fatal("expected divergent US/DE answers to fail authoritative consistency")
	}
	if len(sets) != 2 {
		t.Fatalf("expected two authoritative answer sets, got %+v", sets)
	}
	joined := strings.Join(reasons, " | ")
	for _, want := range []string{"authoritative answer split", "edge-group-country-us", "edge-group-country-de", "<empty>", "15.204.94.71"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected split reason to contain %q, got %q", want, joined)
		}
	}
}

func TestSummarizeAuthoritativeAnswerSetsIgnoresAnswerOrder(t *testing.T) {
	t.Parallel()

	consistent, sets, reasons := summarizeAuthoritativeAnswerSets([]dnsAnswerCheckNode{
		{DNSNodeID: "dns-us", EdgeGroupID: "edge-group-country-us", QueryOK: true, Answers: []string{"2001:db8::1", "15.204.94.71"}},
		{DNSNodeID: "dns-de", EdgeGroupID: "edge-group-country-de", QueryOK: true, Answers: []string{"15.204.94.71", "2001:db8::1"}},
	})
	if !consistent {
		t.Fatalf("expected identical answer sets to be consistent, reasons=%v", reasons)
	}
	if len(sets) != 1 || len(sets[0].DNSNodeIDs) != 2 {
		t.Fatalf("expected one answer set shared by two nodes, got %+v", sets)
	}
}

func TestSummarizeAuthoritativeAnswerSetsFailsClosedOnQueryFailure(t *testing.T) {
	t.Parallel()

	consistent, _, reasons := summarizeAuthoritativeAnswerSets([]dnsAnswerCheckNode{
		{DNSNodeID: "dns-us", EdgeGroupID: "edge-group-country-us", QueryOK: false},
		{DNSNodeID: "dns-de", EdgeGroupID: "edge-group-country-de", QueryOK: true, Answers: []string{"15.204.94.71"}},
	})
	if consistent {
		t.Fatal("expected a failed authoritative query to fail consistency")
	}
	if joined := strings.Join(reasons, " | "); !strings.Contains(joined, "authoritative query failed on nodes: dns-us") {
		t.Fatalf("expected failed node evidence, got %q", joined)
	}
}

func TestProbeDNSAnswerHostRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	probe := probeDNSAnswerHost(t.Context(), "app.example.com", "not-an-ip")
	if probe.Pass || probe.Message == "" {
		t.Fatalf("expected invalid candidate IP to fail with evidence, got %+v", probe)
	}
}
