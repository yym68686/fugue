package dnsserver

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSRecordRecoveryScopesIntentToTheAffectedHostname(t *testing.T) {
	view, plan, readiness, _, now := queryExecutionFixture()
	hostname := plan.Records[0].Hostname
	base := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Records: []platformconfig.DNSIntent{
		{Hostname: hostname, Type: "A", TTL: 60, AppID: "application", TenantID: "tenant", Values: []string{"192.0.2.11"}},
		{Hostname: "challenge.example.test", Type: "TXT", TTL: 30, Values: []string{"previous-value"}},
	}, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &readiness}}
	raw := mustDNSJSON(t, base)
	for _, scenario := range []string{"add unrelated", "remove unrelated", "change unrelated", "same host type", "address", "tenant", "application", "ttl", "remove target"} {
		t.Run(scenario, func(t *testing.T) {
			var previous, next dnsServingPayload
			json.Unmarshal(raw, &previous)
			json.Unmarshal(raw, &next)
			want := false
			switch scenario {
			case "add unrelated":
				next.Records = append(next.Records, platformconfig.DNSIntent{Hostname: "new.example.test", Type: "TXT", TTL: 30, Values: []string{"new-value"}})
				want = true
			case "remove unrelated":
				next.Records = next.Records[:1]
				want = true
			case "change unrelated":
				next.Records[1].Values = []string{"next-value"}
				want = true
			case "same host type":
				next.Records = append(next.Records, platformconfig.DNSIntent{Hostname: hostname, Type: "TXT", TTL: 30, Values: []string{"new-value"}})
			case "address":
				next.Records[0].Values = []string{"192.0.2.99"}
			case "tenant":
				next.Records[0].TenantID = "foreign"
			case "application":
				next.Records[0].AppID = "foreign"
			case "ttl":
				next.Records[0].TTL++
			case "remove target":
				next.Records = next.Records[1:]
			}
			nextRaw := mustDNSJSON(t, next)
			old := &dnsServingState{payload: previous, record: dnsServingCheckpoint{Candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "before", ReleasedAt: now.Add(-time.Minute)}}}}
			bridge := &dnsReleaseBridge{payload: next, candidate: dnsPlatformCandidate{Release: model.PlatformArtifactRelease{ID: "after", ReleasedAt: now}}}
			allowed := compatibleDNSReleaseProbes(old, bridge)
			for _, target := range plan.Records[0].Targets {
				for _, id := range target.ProbeIDs {
					if allowed[id] != want {
						t.Fatal("unrelated record intent vetoed recovery or target authority changed", scenario, allowed)
					}
				}
			}
			if dnsTransitionRecords(old, bridge)[hostname] != want {
				t.Fatal("per-record transition scope", scenario)
			}
			if !reflect.DeepEqual(raw, mustDNSJSON(t, previous)) || !reflect.DeepEqual(nextRaw, mustDNSJSON(t, next)) {
				t.Fatal("intent comparison mutated signed records")
			}
		})
	}
}
