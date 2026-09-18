package platformconfig

import (
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func dnsQueryFixture() CompileRequest {
	r := dnsReadinessFixture()
	r.Intent.DNS[0].Application.IPv6Policy = "ipv4_only"
	r.Intent.DNSConsumers = []DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}
	r.RuntimeSnapshot.DNSConsumers = []DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", ObservedAt: *r.RuntimeSnapshot.CapturedAt, A: []string{"8.8.8.8"}}}
	r.Policy.DNSAnswerRules = []DNSAnswerRule{{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", SelectionMode: "latency_aware", ScopedSelectionMode: "latency_aware", PreferredEdgeGroups: []string{"edge-group-a"}, FallbackEdgeGroups: []string{"edge-group-b"}, TTLSeconds: 90, ECSEnabled: true, ExplorationPercent: 5, SwitchCooldownSeconds: 1800}}
	r.RuntimeSnapshot.DNSSelections = []DNSSelectionObservation{{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", SourceGeneration: "signed-source", SourceDigest: "sha256:" + strings.Repeat("a", 64), ObservedAt: *r.RuntimeSnapshot.CapturedAt, SelectedEdgeGroupID: "edge-group-b", RankingVersion: "rank-v1", RankingScope: "global", Candidates: []DNSSelectionCandidate{
		{IP: "93.184.216.34", EdgeID: "edge-a", EdgeGroupID: "edge-group-a", Country: "aa", Region: "region-a", Weight: 100, Priority: 0, Score: 200},
		{IP: "93.184.216.35", EdgeID: "edge-b", EdgeGroupID: "edge-group-b", Country: "bb", Region: "region-b", Weight: 150, Priority: 50, Score: 100},
	}}}
	r.RuntimeSnapshot.DNSSelections[0].ScopedCandidates = []DNSSelectionScope{{ScopeKey: "country:aa", Country: "aa", SelectedEdgeGroupID: "edge-group-a", Candidates: append([]DNSSelectionCandidate(nil), r.RuntimeSnapshot.DNSSelections[0].Candidates...)}}
	rebindPlacement(&r)
	return r
}

func compiledQueryViews(t *testing.T, r CompileResult) []DNSQueryView {
	t.Helper()
	raw, _ := json.Marshal(r.DNSArtifact.Content["query_views"])
	var views []DNSQueryView
	if err := json.Unmarshal(raw, &views); err != nil {
		t.Fatal(err)
	}
	return views
}

func TestDNSQueryCompilationSeparatesPolicyRankingAndReadiness(t *testing.T) {
	r := dnsQueryFixture()
	before, _ := json.Marshal(r)
	compiled, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	views := compiledQueryViews(t, compiled)
	if len(views) != 1 || len(views[0].Records) != 2 {
		t.Fatal("consumer query records incomplete")
	}
	query := views[0].Records[0]
	if query.Name != "app.example.test" || query.TTL != 90 || len(query.ValueExpirations) != 0 || query.AnswerPolicy.PolicyKind != "latency_aware" || query.AnswerPolicy.SelectedEdgeGroupID != "edge-group-b" || len(query.ScopedCandidates) != 1 {
		t.Fatalf("selection semantics lost: %+v", query)
	}
	for _, c := range query.Candidates {
		if c.Healthy || c.RouteReady || c.TLSReady || c.DNSEligible || c.ServingGeneration != "" {
			t.Fatal("compiler fabricated current readiness")
		}
	}
	rawRecords := flattenedRecords(t, compiled)
	if len(rawRecords[0].ValueExpirations) == 0 {
		t.Fatal("new query authorization overwrote original finite lease representation")
	}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("compiler mutated configuration input")
	}
	r.CreatedAt = time.Now().Add(time.Hour)
	replay, err := Compile(r)
	if err != nil || !reflect.DeepEqual(compiled.DNSArtifact.Content, replay.DNSArtifact.Content) {
		t.Fatal("query compilation used current time", err)
	}
	r.RuntimeSnapshot.DNSSelections[0].Candidates[0].Score = 50
	changed, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Lineage.IntentDigest != compiled.Lineage.IntentDigest || changed.Lineage.PolicyDigest != compiled.Lineage.PolicyDigest || changed.Lineage.InputSnapshotDigest == compiled.Lineage.InputSnapshotDigest {
		t.Fatal("ranking fact changed desired policy")
	}
	r.Policy.DNSAnswerRules[0].ECSEnabled = false
	rebindPlacement(&r)
	policyChanged, err := Compile(r)
	if err != nil || policyChanged.Lineage.PolicyDigest == changed.Lineage.PolicyDigest {
		t.Fatal("query controls did not version policy", err)
	}
}

func TestDNSQueryRejectsUnboundIncompleteOrFabricatedInputs(t *testing.T) {
	for name, mutate := range map[string]func(*CompileRequest){
		"missing source":       func(r *CompileRequest) { r.RuntimeSnapshot.DNSSelections = nil },
		"wrong source node":    func(r *CompileRequest) { r.RuntimeSnapshot.DNSSelections[0].NodeID = "foreign" },
		"wrong endpoint owner": func(r *CompileRequest) { r.RuntimeSnapshot.DNSSelections[0].Candidates[0].EdgeID = "foreign" },
		"future source": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSSelections[0].ObservedAt = r.RuntimeSnapshot.CapturedAt.Add(time.Second)
		},
		"duplicate source": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSSelections = append(r.RuntimeSnapshot.DNSSelections, r.RuntimeSnapshot.DNSSelections[0])
		},
		"unknown mode":          func(r *CompileRequest) { r.Policy.DNSAnswerRules[0].SelectionMode = "script" },
		"unbounded exploration": func(r *CompileRequest) { r.Policy.DNSAnswerRules[0].ExplorationPercent = 51 },
		"invalid ranking":       func(r *CompileRequest) { r.RuntimeSnapshot.DNSSelections[0].Candidates[0].Score = math.NaN() },
		"duplicate candidate": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSSelections[0].Candidates = append(r.RuntimeSnapshot.DNSSelections[0].Candidates, r.RuntimeSnapshot.DNSSelections[0].Candidates[0])
		},
		"foreign rule": func(r *CompileRequest) { r.Policy.DNSAnswerRules[0].Hostname = "foreign.example.test" },
	} {
		t.Run(name, func(t *testing.T) {
			r := dnsQueryFixture()
			mutate(&r)
			rebindPlacement(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("invalid query authorization accepted")
			}
		})
	}
	r := dnsQueryFixture()
	compiled, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	views := compiledQueryViews(t, compiled)
	global := flattenedRecords(t, compiled)
	var consumerViews []DNSConsumerView
	raw, _ := json.Marshal(compiled.DNSArtifact.Content["consumer_views"])
	json.Unmarshal(raw, &consumerViews)
	var plan DNSReadinessPlan
	raw, _ = json.Marshal(compiled.DNSArtifact.Content["readiness_plan"])
	json.Unmarshal(raw, &plan)
	for name, mutate := range map[string]func([]DNSQueryView){
		"forged healthy": func(v []DNSQueryView) { v[0].Records[0].Candidates[0].Healthy = true },
		"wrong policy":   func(v []DNSQueryView) { v[0].Records[0].AnswerPolicy.ECSEnabled = false },
		"wrong address":  func(v []DNSQueryView) { v[0].Records[0].Candidates[0].IP = "1.1.1.1" },
		"static change":  func(v []DNSQueryView) { v[0].Records[1].Values = []string{"9.9.9.9"} },
		"wrong consumer": func(v []DNSQueryView) { v[0].NodeID = "foreign" },
	} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(views)
			var changed []DNSQueryView
			json.Unmarshal(raw, &changed)
			mutate(changed)
			if err := ValidateDNSQueryViews(changed, global, consumerViews, &plan, r.Policy); err == nil {
				t.Fatal("invalid signed query payload accepted")
			}
		})
	}
	originalOrder := append([]DNSSelectionCandidate(nil), r.RuntimeSnapshot.DNSSelections[0].Candidates...)
	normalized := normalizeDNSSelections(r.RuntimeSnapshot.DNSSelections)
	if !slices.EqualFunc(normalized[0].Candidates, originalOrder, func(a, b DNSSelectionCandidate) bool { return a.IP == b.IP }) {
		t.Fatal("selection tie order changed")
	}
}
