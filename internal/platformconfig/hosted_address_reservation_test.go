package platformconfig

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestHostedPhysicalRosterSurvivesRepeatedCaptureButCannotGrantAdmission(t *testing.T) {
	pin := func(id string, fence int64) PreviousTrafficPublicationReference {
		d := "sha256:" + strings.Repeat("1", 64)
		return PreviousTrafficPublicationReference{ReleaseSetID: "parent-" + id, ReleaseSetDigest: d, ReleaseID: "full-" + id, FencingToken: fence,
			RouteArtifactID: "route-" + id, RouteArtifactDigest: d, TLSArtifactID: "tls-" + id, TLSArtifactDigest: d, DNSArtifactID: "dns-" + id, DNSArtifactDigest: d}
	}
	const group = "edge-group-test"
	full := DNSIntent{Hostname: "host.example.test", Type: "A", RecordKind: "hosted", TenantID: "tenant", AppID: "owner", TTL: 60, Values: []string{"1.1.1.1", "8.8.8.8"}}
	member := func(id, ip string) DNSReadinessTarget {
		return DNSReadinessTarget{EdgeID: id, EdgeGroupID: group, Family: "A", Address: ip, FailureDomains: map[string]string{"rack": id}, ProbeIDs: []string{"prior-path"}}
	}
	all := DNSReadinessRecord{Hostname: full.Hostname, MinimumHealthyEdges: 1, Targets: []DNSReadinessTarget{member("first", "1.1.1.1"), member("second", "8.8.8.8")}}
	active := all
	active.Targets = []DNSReadinessTarget{member("first", "1.1.1.1")}
	selected := full
	selected.Values = []string{"1.1.1.1"}
	holds := []HostedAddressHold{{"second", group, "8.8.8.8"}}
	first, err := CaptureHostedAddressReservation(pin("one", 10), full, all, nil, nil, selected, active, holds)
	if err != nil {
		t.Fatal(err)
	}
	snap := RuntimeSnapshot{HostedAddressReservations: []HostedAddressReservation{first}}
	out := ConfigurationRuntimeInput(snap)
	var buf bytes.Buffer
	err = WriteRuntimeSnapshot(&buf, out)
	raw, encErr := json.Marshal(out)
	if err != nil || encErr != nil || !bytes.Equal(raw, buf.Bytes()) {
		t.Fatal("runtime stream differs from original representation", err, encErr)
	}
	var retained RuntimeSnapshot
	if err = json.Unmarshal(raw, &retained); err != nil || len(retained.HostedAddressReservations) != 1 {
		t.Fatal("compact snapshot lost declaration", err)
	}
	old := retained.HostedAddressReservations[0]
	second, err := CaptureHostedAddressReservation(pin("two", 11), first.Record, active, holds, &old, selected, active, holds)
	if err != nil || !reflect.DeepEqual(first.Physical, second.Physical) || !reflect.DeepEqual(first.Record, second.Record) || second.Source.ReleaseID != "full-two" {
		t.Fatal("recapture lost its roster or source", err)
	}
	for _, tc := range []string{"missing_custody", "stale_fence", "same_release", "old_hold_missing", "new_hold_missing", "different_member", "changed_record", "reused_probe", "foreign_tenant", "legacy_snapshot_missing_member"} {
		t.Run(tc, func(t *testing.T) {
			var saved HostedAddressReservation
			if e := json.Unmarshal(raw, &retained); e != nil {
				t.Fatal(e)
			}
			saved = retained.HostedAddressReservations[0]
			previous := &saved
			source := pin("two", 11)
			prior := first.Record
			before, after := holds, holds
			switch tc {
			case "missing_custody", "legacy_snapshot_missing_member":
				previous = nil
			case "stale_fence":
				source.FencingToken = 9
			case "same_release":
				source.ReleaseID = first.Source.ReleaseID
			case "old_hold_missing":
				before = nil
			case "new_hold_missing":
				after = nil
			case "different_member":
				saved.Physical.Targets[1].EdgeID = "replacement"
			case "changed_record":
				saved.Record.TTL++
			case "reused_probe":
				saved.Physical.Targets[0].ProbeIDs = []string{"stale"}
			case "foreign_tenant":
				prior.TenantID = "another"
			}
			if _, e := CaptureHostedAddressReservation(source, prior, active, before, previous, selected, active, after); e == nil {
				t.Fatal("invalid recapture accepted")
			}
		})
	}
	second.Physical.Targets[0].FailureDomains["rack"] = "modified"
	if reflect.DeepEqual(first.Physical, second.Physical) || all.Targets[0].FailureDomains["rack"] != "first" || len(first.Physical.Targets[0].ProbeIDs) != 0 || len(active.Targets) != 1 {
		t.Fatal("retention aliased source state or changed traffic eligibility")
	}
}

func TestRetainHostedAddressRecordOnlyForExactEffectiveHold(t *testing.T) {
	for _, scenario := range []string{"valid", "owner", "ttl", "quorum", "placement", "wrong_address", "wrong_edge", "expiry", "new_candidate", "duplicate", "extra_value", "hold_still_offered", "unexplained_loss", "no_active"} {
		t.Run(scenario, func(t *testing.T) {
			prior := DNSIntent{Hostname: "service.example.test", Type: "A", RecordKind: "hosted", AppID: "owner", TenantID: "tenant", Values: []string{"1.1.1.1", "8.8.8.8", "9.9.9.9"}, TTL: 60}
			next := prior
			next.Values = []string{"1.1.1.1", "8.8.8.8"}
			member := func(id, ip string) DNSReadinessTarget {
				return DNSReadinessTarget{EdgeID: id, EdgeGroupID: "edge-group-test", Address: ip, Family: "A", ProbeIDs: []string{"old"}}
			}
			before := DNSReadinessRecord{Hostname: prior.Hostname, MinimumHealthyEdges: 1, Targets: []DNSReadinessTarget{member("first", "1.1.1.1"), member("second", "8.8.8.8"), member("held", "9.9.9.9")}}
			after := before
			after.Targets = append([]DNSReadinessTarget(nil), before.Targets[:2]...)
			after.Targets[0].ProbeIDs = []string{"fresh-path"}
			holds := []HostedAddressHold{{"held", "edge-group-test", "9.9.9.9"}}
			switch scenario {
			case "owner":
				next.AppID = "other"
			case "ttl":
				next.TTL++
			case "quorum":
				after.MinimumHealthyEdges++
			case "placement":
				after.Targets[0].FailureDomains = map[string]string{"zone": "other"}
			case "wrong_address":
				holds[0].Address = "8.8.8.8"
			case "wrong_edge":
				holds[0].EdgeID = "second"
			case "expiry":
				prior.ValueExpirations = map[string]time.Time{"9.9.9.9": time.Now()}
			case "new_candidate":
				next.Values[0] = "8.8.4.4"
				after.Targets[0].Address = next.Values[0]
			case "duplicate":
				after.Targets = append(after.Targets, after.Targets[0])
			case "extra_value":
				next.Values = append(next.Values, "9.9.9.9")
			case "hold_still_offered":
				holds = append(holds, HostedAddressHold{"second", "edge-group-test", "8.8.8.8"})
			case "unexplained_loss":
				next.Values, after.Targets = next.Values[:1], after.Targets[:1]
			case "no_active":
				next.Values, after.Targets = nil, nil
			}
			got, err := RetainHostedAddressRecord(prior, next, before, after, holds)
			if scenario != "valid" {
				if err == nil || len(got.Values) != 0 {
					t.Fatal("accepted a change outside the exact hold", got)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, prior) || len(next.Values) != 2 || len(after.Targets) != 2 || after.Targets[0].ProbeIDs[0] != "fresh-path" {
				t.Fatal("address declaration or effective readiness was overwritten", got, err)
			}
			got.Values[0] = "changed"
			if prior.Values[0] == "changed" {
				t.Fatal("retained declaration aliased predecessor's address slice")
			}
		})
	}
}
