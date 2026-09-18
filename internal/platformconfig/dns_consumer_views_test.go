package platformconfig

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"
)

func dnsViewRequest() CompileRequest {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	return CompileRequest{
		Intent: PlatformIntent{Generation: "intent-1", Scope: "global", DNSConsumers: []DNSConsumerIntent{
			{NodeID: "dns-a", EdgeGroupID: "group-a", Zones: []string{"child.example.test", "example.test"}, ProbeLabel: "probe", ProbeTTL: 60},
			{NodeID: "dns-b", EdgeGroupID: "group-b", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60},
		}, DNS: []DNSIntent{
			{Hostname: "app.example.test", Type: "A", Values: []string{"192.0.2.1"}, TTL: 60},
			{Hostname: "app.child.example.test", Type: "A", Values: []string{"192.0.2.2"}, TTL: 60},
		}},
		Policy: PolicySnapshot{Generation: "policy-1", Scope: "global", MinimumHealthyEdges: 1, MaxStaleSeconds: 3600},
		RuntimeSnapshot: RuntimeSnapshot{CapturedAt: &now, DNSConsumers: []DNSConsumerObservation{
			{NodeID: "dns-a", EdgeGroupID: "group-a", ObservedAt: now, A: []string{"8.8.8.8", "8.8.4.4"}},
			{NodeID: "dns-b", EdgeGroupID: "group-b", ObservedAt: now, A: []string{"9.9.9.9"}},
		}}, CreatedAt: now,
	}
}

func TestDNSConsumerViewsSeparatePhysicalOwnersAndZoneLeases(t *testing.T) {
	req := dnsViewRequest()
	expiry := req.CreatedAt.Add(time.Minute)
	req.Intent.DNS[0].ValueExpirations = map[string]time.Time{"192.0.2.1": expiry}
	views, err := CompileDNSConsumerViews(req.Intent.DNSConsumers, req.RuntimeSnapshot, req.Intent.DNS)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		node, group, zone, probe string
		hosts                    []string
	}{
		{"dns-a", "group-a", "example.test", "8.8.4.4", []string{"app.example.test", "probe.example.test"}},
		{"dns-a", "group-a", "child.example.test", "8.8.4.4", []string{"app.child.example.test", "probe.child.example.test"}},
		{"dns-b", "group-b", "example.test", "9.9.9.9", []string{"app.example.test", "app.child.example.test", "probe.example.test"}},
	} {
		records, err := MaterializeDNSConsumerView(req.Intent.DNS, views, tc.node, tc.group, tc.zone, req.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
		hosts := []string{}
		for _, r := range records {
			hosts = append(hosts, r.Hostname)
			if r.RecordKind == "probe" && r.Values[0] != tc.probe {
				t.Fatalf("wrong listener owner: %+v", r)
			}
		}
		if !reflect.DeepEqual(hosts, tc.hosts) {
			t.Fatalf("zone leaked records: %v != %v", hosts, tc.hosts)
		}
	}
	for _, identity := range [][3]string{{"missing", "group-a", "example.test"}, {"dns-a", "group-b", "example.test"}, {"dns-a", "group-a", "removed.example.test"}} {
		if _, err := MaterializeDNSConsumerView(req.Intent.DNS, views, identity[0], identity[1], identity[2], req.CreatedAt); err == nil {
			t.Fatal("unassigned view accepted")
		}
	}
	records, err := MaterializeDNSConsumerView(req.Intent.DNS, views, "dns-a", "group-a", "example.test", expiry.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if r.Hostname == "app.example.test" && len(r.Values) > 0 {
			t.Fatal("zone materialization renewed expired application address")
		}
	}
	if req.Intent.DNS[0].ValueExpirations["192.0.2.1"] != expiry {
		t.Fatal("expiry mutated")
	}
	req.Intent.DNSConsumers[0].Zones = []string{"example.test"}
	after, err := CompileDNSConsumerViews(req.Intent.DNSConsumers, req.RuntimeSnapshot, req.Intent.DNS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MaterializeDNSConsumerView(req.Intent.DNS, after, "dns-a", "group-a", "child.example.test", req.CreatedAt); err == nil {
		t.Fatal("deleted zone resurrected")
	}
}

func TestDNSConsumerCompileReplayAndDesiredFactSeparation(t *testing.T) {
	req := dnsViewRequest()
	before, _ := json.Marshal(req)
	first, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(req)
	if string(before) != string(after) {
		t.Fatal("compiler mutated input")
	}
	slices.Reverse(req.Intent.DNSConsumers)
	slices.Reverse(req.RuntimeSnapshot.DNSConsumers)
	for i := range req.Intent.DNSConsumers {
		slices.Reverse(req.Intent.DNSConsumers[i].Zones)
	}
	for i := range req.RuntimeSnapshot.DNSConsumers {
		slices.Reverse(req.RuntimeSnapshot.DNSConsumers[i].A)
	}
	replay, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, replay) {
		t.Fatal("input enumeration affected compiled artifacts")
	}
	req.RuntimeSnapshot.DNSConsumers[0].A = []string{"1.1.1.1"}
	changed, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Lineage.IntentDigest != changed.Lineage.IntentDigest || first.Lineage.InputSnapshotDigest == changed.Lineage.InputSnapshotDigest || reflect.DeepEqual(first.DNSArtifact.Content, changed.DNSArtifact.Content) {
		t.Fatal("endpoint observation incorrectly owns intent or omitted from artifact lineage")
	}
}

func TestDNSConsumerViewsRejectAmbiguousInputs(t *testing.T) {
	cases := map[string]func(*CompileRequest){
		"missing fact":       func(r *CompileRequest) { r.RuntimeSnapshot.DNSConsumers = r.RuntimeSnapshot.DNSConsumers[:1] },
		"orphan fact":        func(r *CompileRequest) { r.Intent.DNSConsumers = r.Intent.DNSConsumers[:1] },
		"wrong group":        func(r *CompileRequest) { r.RuntimeSnapshot.DNSConsumers[0].EdgeGroupID = "other" },
		"shared endpoint":    func(r *CompileRequest) { r.RuntimeSnapshot.DNSConsumers[1].A = []string{"8.8.8.8"} },
		"future observation": func(r *CompileRequest) { r.RuntimeSnapshot.DNSConsumers[0].ObservedAt = r.CreatedAt.Add(time.Second) },
		"private endpoint":   func(r *CompileRequest) { r.RuntimeSnapshot.DNSConsumers[0].A = []string{"10.0.0.1"} },
		"wrong family":       func(r *CompileRequest) { r.RuntimeSnapshot.DNSConsumers[0].AAAA = []string{"8.8.8.8"} },
		"duplicate zone":     func(r *CompileRequest) { r.Intent.DNSConsumers[0].Zones = []string{"example.test", "example.test"} },
		"duplicate identity": func(r *CompileRequest) { r.Intent.DNSConsumers[1].NodeID = "dns-a" },
		"probe collision":    func(r *CompileRequest) { r.Intent.DNS[0].Hostname = "probe.example.test" },
		"zero ttl":           func(r *CompileRequest) { r.Intent.DNSConsumers[0].ProbeTTL = 0 },
		"noncanonical zone":  func(r *CompileRequest) { r.Intent.DNSConsumers[0].Zones = []string{"Example.test."} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := dnsViewRequest()
			mutate(&r)
			if _, err := CompileDNSConsumerViews(r.Intent.DNSConsumers, r.RuntimeSnapshot, r.Intent.DNS); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
	r := dnsViewRequest()
	views, err := CompileDNSConsumerViews(r.Intent.DNSConsumers, r.RuntimeSnapshot, r.Intent.DNS)
	if err != nil {
		t.Fatal(err)
	}
	views[1].Records[0].Values = []string{"1.1.1.1"}
	if err := ValidateDNSConsumerViews(views, r.Intent.DNS); err == nil {
		t.Fatal("same process with inconsistent zone endpoint accepted")
	}
}
