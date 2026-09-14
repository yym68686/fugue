package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func flattenCompileFixture() CompileRequest {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	record := DNSIntent{Hostname: "alias.example", Type: "ALIAS", Values: []string{"target.example"}, TTL: 60, TenantID: "tenant", Flatten: &DNSFlattenIntent{Mode: "always", Target: "target.example", IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "min", FallbackPolicy: "stale_if_error"}}
	digest, _ := DNSFlattenInputDigest(record)
	return CompileRequest{Intent: PlatformIntent{Generation: "flatten", DNS: []DNSIntent{record}}, Policy: PolicySnapshot{Generation: "flatten-policy", MaxStaleSeconds: 300}, RuntimeSnapshot: RuntimeSnapshot{CapturedAt: &now, DNSFlatten: []DNSFlattenObservation{{InputDigest: digest, TenantID: "tenant", CheckedAt: now.Add(-time.Second), ObservedAt: now.Add(-time.Second), Status: "resolved", A: []string{"93.184.216.34"}, AAAA: []string{"2606:4700:4700::1111"}, TargetTTL: 120}}}}
}

func flattenedRecords(t *testing.T, r CompileResult) []DNSIntent {
	t.Helper()
	b, e := json.Marshal(r.DNSArtifact.Content["records"])
	if e != nil {
		t.Fatal(e)
	}
	var records []DNSIntent
	if e = json.Unmarshal(b, &records); e != nil {
		t.Fatal(e)
	}
	return records
}

func TestFlattenCompileUsesOnlyFixedFacts(t *testing.T) {
	r := flattenCompileFixture()
	before, _ := json.Marshal(r)
	a, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	records := flattenedRecords(t, a)
	if len(records) != 2 || records[0].Type != "A" || records[1].Type != "AAAA" || records[0].TTL != 60 || records[0].Flatten != nil {
		t.Fatalf("bad compiled DNS: %+v", records)
	}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("compiler mutated inputs")
	}
	r.CreatedAt = time.Now().Add(48 * time.Hour)
	b, err := Compile(r)
	if err != nil || !reflect.DeepEqual(a.DNSArtifact.Content, b.DNSArtifact.Content) {
		t.Fatal("wall clock changed compilation", err)
	}
	r.RuntimeSnapshot.DNSFlatten[0].A = []string{"93.184.216.35"}
	b, err = Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	if a.Lineage.IntentDigest != b.Lineage.IntentDigest || a.Lineage.PolicyDigest != b.Lineage.PolicyDigest || a.Lineage.InputSnapshotDigest == b.Lineage.InputSnapshotDigest || reflect.DeepEqual(a.DNSArtifact.Content, b.DNSArtifact.Content) {
		t.Fatal("facts and intent were not separated")
	}
}

func TestFlattenRejectsUnboundStaleOrUnsafeInputs(t *testing.T) {
	for name, mutate := range map[string]func(*CompileRequest){
		"missing":      func(r *CompileRequest) { r.RuntimeSnapshot.DNSFlatten = nil },
		"wrong digest": func(r *CompileRequest) { r.RuntimeSnapshot.DNSFlatten[0].InputDigest = "other" },
		"wrong tenant": func(r *CompileRequest) { r.RuntimeSnapshot.DNSFlatten[0].TenantID = "other" },
		"duplicate": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSFlatten = append(r.RuntimeSnapshot.DNSFlatten, r.RuntimeSnapshot.DNSFlatten[0])
		},
		"missing capture": func(r *CompileRequest) { r.RuntimeSnapshot.CapturedAt = nil },
		"future": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSFlatten[0].CheckedAt = r.RuntimeSnapshot.CapturedAt.Add(time.Second)
		},
		"expired success": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSFlatten[0].ObservedAt = r.RuntimeSnapshot.CapturedAt.Add(-301 * time.Second)
		},
		"unknown status":   func(r *CompileRequest) { r.RuntimeSnapshot.DNSFlatten[0].Status = "healthy" },
		"missing TTL":      func(r *CompileRequest) { r.RuntimeSnapshot.DNSFlatten[0].TargetTTL = 0 },
		"TTL expired":      func(r *CompileRequest) { r.RuntimeSnapshot.DNSFlatten[0].TargetTTL = 1 },
		"private address":  func(r *CompileRequest) { r.RuntimeSnapshot.DNSFlatten[0].A = []string{"10.0.0.1"} },
		"reserved address": func(r *CompileRequest) { r.RuntimeSnapshot.DNSFlatten[0].A = []string{"192.0.2.1"} },
		"wrong family":     func(r *CompileRequest) { r.RuntimeSnapshot.DNSFlatten[0].AAAA = []string{"93.184.216.34"} },
		"no answers": func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSFlatten[0].A = nil
			r.RuntimeSnapshot.DNSFlatten[0].AAAA = nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := flattenCompileFixture()
			mutate(&r)
			if _, err := Compile(r); err == nil {
				t.Fatal("invalid facts accepted")
			}
		})
	}
}

func TestFlattenPolicyControlsFallbackFamiliesAndTTL(t *testing.T) {
	for _, test := range []struct {
		name                 string
		configure            func(*CompileRequest)
		wantTTL, wantRecords int
		wantError            bool
	}{
		{"target", func(r *CompileRequest) { r.Intent.DNS[0].Flatten.TTLPolicy = "target" }, 119, 2, false},
		{"record missing target TTL", func(r *CompileRequest) {
			r.Intent.DNS[0].Flatten.TTLPolicy = "record"
			r.RuntimeSnapshot.DNSFlatten[0].TargetTTL = 0
		}, 60, 2, false},
		{"IPv4", func(r *CompileRequest) { r.Intent.DNS[0].Flatten.IPv4Policy = "ipv4_only" }, 60, 1, false},
		{"dual missing IPv6", func(r *CompileRequest) {
			r.Intent.DNS[0].Flatten.IPv4Policy = "dual_stack_required"
			r.RuntimeSnapshot.DNSFlatten[0].AAAA = nil
		}, 0, 0, true},
		{"conflicting families", func(r *CompileRequest) {
			r.Intent.DNS[0].Flatten.IPv4Policy = "ipv4_only"
			r.Intent.DNS[0].Flatten.IPv6Policy = "ipv6_only"
		}, 0, 0, true},
		{"stale bounded", func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSFlatten[0].Status = "stale"
			r.RuntimeSnapshot.DNSFlatten[0].ObservedAt = r.RuntimeSnapshot.CapturedAt.Add(-290 * time.Second)
		}, 10, 2, false},
		{"fail closed", func(r *CompileRequest) {
			r.RuntimeSnapshot.DNSFlatten[0].Status = "error"
			r.Intent.DNS[0].Flatten.FallbackPolicy = "fail_closed"
		}, 0, 0, true},
		{"empty unsupported", func(r *CompileRequest) { r.Intent.DNS[0].Flatten.FallbackPolicy = "empty_noerror" }, 0, 0, true},
		{"apex", func(r *CompileRequest) {
			r.Intent.DNS[0].Flatten.Mode = "apex"
			r.Intent.DNS[0].Flatten.Zone = "alias.example"
		}, 60, 2, false},
		{"nonapex", func(r *CompileRequest) {
			r.Intent.DNS[0].Flatten.Mode = "apex"
			r.Intent.DNS[0].Flatten.Zone = "example"
		}, 0, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := flattenCompileFixture()
			test.configure(&r)
			r.RuntimeSnapshot.DNSFlatten[0].InputDigest, _ = DNSFlattenInputDigest(r.Intent.DNS[0])
			result, err := Compile(r)
			if test.wantError {
				if err == nil {
					t.Fatal("invalid policy accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			records := flattenedRecords(t, result)
			if len(records) != test.wantRecords || records[0].TTL != test.wantTTL {
				t.Fatalf("unexpected output: %+v", records)
			}
		})
	}
}

func TestFlattenApexCanCoexistWithMXAndTXT(t *testing.T) {
	r := flattenCompileFixture()
	r.Intent.DNS = append(r.Intent.DNS, DNSIntent{Hostname: "alias.example", Type: "MX", Values: []string{"10 mail.example."}, TTL: 60}, DNSIntent{Hostname: "alias.example", Type: "TXT", Values: []string{"verification"}, TTL: 60})
	result, err := Compile(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(flattenedRecords(t, result)) != 4 {
		t.Fatal("flatten lost non-address apex RRsets")
	}
	r.Intent.DNS = append(r.Intent.DNS, DNSIntent{Hostname: "alias.example", Type: "A", Values: []string{"93.184.216.35"}, TTL: 60})
	if _, err := Compile(r); err == nil {
		t.Fatal("flatten result overwrote an existing A RRset")
	}
}
