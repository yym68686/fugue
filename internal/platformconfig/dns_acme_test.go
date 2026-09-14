package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestACMECompileAndExpiryKeepIntentImmutable(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	request := CompileRequest{Intent: PlatformIntent{Generation: "acme", ACMEChallenges: []ACMEChallengeIntent{{ID: "one", Zone: "example", Hostname: "_acme-challenge.example", Value: "first", TTL: 60, ExpiresAt: now.Add(30 * time.Second)}, {ID: "two", Zone: "example", Hostname: "_acme-challenge.example", Value: "second", TTL: 60, ExpiresAt: now.Add(time.Minute)}}}, Policy: PolicySnapshot{Generation: "policy"}, RuntimeSnapshot: RuntimeSnapshot{CapturedAt: &now}}
	before, _ := json.Marshal(request)
	first, err := Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(request)
	if string(before) != string(after) {
		t.Fatal("mutated intent")
	}
	decode := func(result CompileResult) []DNSIntent {
		raw, _ := json.Marshal(result.DNSArtifact.Content["records"])
		var out []DNSIntent
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	records := decode(first)
	if len(records) != 1 || len(records[0].Values) != 2 || records[0].TTL != 30 || len(records[0].ValueExpirations) != 2 {
		t.Fatalf("lost expiry: %+v", records)
	}
	request.CreatedAt = now.Add(time.Hour)
	same, err := Compile(request)
	if err != nil || !reflect.DeepEqual(first.DNSArtifact.Content, same.DNSArtifact.Content) {
		t.Fatal("wall clock changed ACME output", err)
	}
	later := now.Add(30 * time.Second)
	request.RuntimeSnapshot.CapturedAt = &later
	second, err := Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	records = decode(second)
	if len(records) != 1 || len(records[0].Values) != 1 || records[0].Values[0] != "second" {
		t.Fatal("expired challenge retained")
	}
	if first.Lineage.IntentDigest != second.Lineage.IntentDigest || first.Lineage.PolicyDigest != second.Lineage.PolicyDigest || first.Lineage.InputSnapshotDigest == second.Lineage.InputSnapshotDigest {
		t.Fatal("expiry changed configuration")
	}
	last, err := DNSRecordsAt(records, now.Add(time.Minute))
	if err != nil || len(last) != 0 {
		t.Fatal("query-time expiry extended challenge", err)
	}
}

func TestACMEPermanentTXTAndMultipleOwners(t *testing.T) {
	now := time.Now().UTC()
	permanent := DNSIntent{Hostname: "_acme-challenge.example", Type: "TXT", Values: []string{"permanent"}, TTL: 60}
	challenges := []ACMEChallengeIntent{{ID: "a", Zone: "example", Hostname: permanent.Hostname, Value: "permanent", TTL: 30, ExpiresAt: now.Add(10 * time.Second)}, {ID: "b", Zone: "example", Hostname: permanent.Hostname, Value: "shared", TTL: 30, ExpiresAt: now.Add(10 * time.Second)}, {ID: "c", Zone: "example", Hostname: permanent.Hostname, Value: "shared", TTL: 30, ExpiresAt: now.Add(20 * time.Second)}}
	got, err := CompileACMEChallenges([]DNSIntent{permanent}, challenges, &now)
	if err != nil {
		t.Fatal(err)
	}
	if _, expires := got[0].ValueExpirations["permanent"]; expires {
		t.Fatal("permanent TXT acquired expiration")
	}
	if !got[0].ValueExpirations["shared"].Equal(now.Add(20 * time.Second)) {
		t.Fatal("shared challenge lost active owner")
	}
	got, err = DNSRecordsAt(got, now.Add(21*time.Second))
	if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0].Values, []string{"permanent"}) {
		t.Fatal("expired token deleted permanent TXT", err)
	}
}

func TestACMERejectsInvalidExpiryAndZone(t *testing.T) {
	now := time.Now().UTC()
	base := ACMEChallengeIntent{ID: "challenge", Zone: "example", Hostname: "_acme-challenge.example", Value: "token", TTL: 60, ExpiresAt: now.Add(time.Minute)}
	for _, mutate := range []func(*ACMEChallengeIntent){func(c *ACMEChallengeIntent) { c.Zone = "other.example" }, func(c *ACMEChallengeIntent) { c.ExpiresAt = time.Time{} }, func(c *ACMEChallengeIntent) { c.TTL = 3601 }, func(c *ACMEChallengeIntent) { c.Hostname = "example" }, func(c *ACMEChallengeIntent) { c.Value = "" }} {
		c := base
		mutate(&c)
		if _, err := CompileACMEChallenges(nil, []ACMEChallengeIntent{c}, &now); err == nil {
			t.Fatal("invalid challenge accepted")
		}
	}
	if _, err := CompileACMEChallenges(nil, []ACMEChallengeIntent{base}, nil); err == nil {
		t.Fatal("implicit wall clock accepted")
	}
	if _, err := CompileACMEChallenges(nil, []ACMEChallengeIntent{base, base}, &now); err == nil {
		t.Fatal("duplicate challenge accepted")
	}
}
