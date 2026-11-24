package dnsserver

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeartifact"
)

func TestScopedDNSMaterializationCannotMixProofsOrChangeUnselectedRecord(t *testing.T) {
	view, plan, policy, global, now := queryExecutionFixture()
	head := "sha256:" + strings.Repeat("d", 64)
	h := hostedDNSDecision{Host: plan.Probes[0].Hostname, Decision: head, SelectedAt: now.Add(-2 * time.Second), ValidUntil: now.Add(300 * time.Second)}
	for i := range plan.Records[0].Targets {
		plan.Records[0].Targets[i].RequireSinglePublication = true
	}
	scoped := slices.Clone(global)
	for i := range scoped {
		scoped[i].Proof.Kind = "hosted-route"
		scoped[i].Proof.Version = head
		scoped[i].Proof.ValidUntil = now.Add(45 * time.Second)
	}
	view.Records[0].TTL = 30
	if _, e := materializeDNSQueryScope(view, &plan, &policy, scoped, now, true, &h); e == nil {
		t.Fatal("unrelated TXT admitted into host selection")
	}
	view.Records = view.Records[:1]
	out, e := materializeDNSQueryScope(view, &plan, &policy, scoped, now, true, &h)
	if e != nil || len(out) != 1 || !reflect.DeepEqual(out[0].Values, view.Records[0].Values) || out[0].TTL != 30 {
		t.Fatal("qualified address set or TTL changed", e, out)
	}
	// Old facts cannot enter the scoped path, or vice versa. A change of
	// decision on a single path is enough to lose the two-edge quorum.
	for _, edit := range []func(*dnsReadinessFact){
		func(f *dnsReadinessFact) { f.Proof.Version = "other" },
		func(f *dnsReadinessFact) { f.Proof.Kind = "" },
		func(f *dnsReadinessFact) { f.Proof.TrafficRelease = &model.TrafficReleaseBinding{} },
		func(f *dnsReadinessFact) { f.Proof.CheckedAt = h.SelectedAt.Add(-time.Nanosecond) },
		func(f *dnsReadinessFact) { f.Proof.ValidUntil = now },
		func(f *dnsReadinessFact) { f.Proof.GroupID += "-other" },
		func(f *dnsReadinessFact) { f.Proof.State = "excluded" },
		func(f *dnsReadinessFact) { f.Proof.Digest = head },
		func(f *dnsReadinessFact) { f.ProbeID = scoped[1].ProbeID },
	} {
		v := slices.Clone(scoped)
		edit(&v[0])
		rows, e := materializeDNSQueryScope(view, &plan, &policy, v, now, true, &h)
		if e != nil || len(rows[0].Values) != 0 {
			t.Fatal("partial or borrowed evidence passed", e)
		}
	}
	rows, e := materializeDNSQueries(view, &plan, &policy, scoped, now)
	if e != nil || len(rows[0].Values) != 0 {
		t.Fatal("global evaluator accepted scoped result", e)
	}
	h.ValidUntil = now.Add(12 * time.Second)
	rows, e = materializeDNSQueryScope(view, &plan, &policy, scoped, now, true, &h)
	if e != nil || rows[0].TTL != 12 {
		t.Fatal("lease exceeded host authority", e)
	}
	if _, e := materializeDNSQueryScope(view, &plan, &policy, scoped, h.ValidUntil, true, &h); e == nil {
		t.Fatal("expired host decision allowed")
	}
	if view.Records[0].TTL != 30 || len(view.Records[0].Candidates) != 2 {
		t.Fatal("changed shared source view")
	}
}

func TestHostedDNSRecoveryNeverBorrowsAnotherDecisionOrCountsTwoZonesTwice(t *testing.T) {
	_, _, policy, source, at := queryExecutionFixture()
	avail := availabilityTestPolicy()
	p := routeartifact.HostedSelectedPacket{Decision: "decision-a", Parent: "full", Delivery: routeartifact.HostedDelivery{Host: "owned.example.test"}}
	s := new(Service)
	run := func(at time.Time, ready bool) bool {
		f := source[0]
		f.Ready = ready
		f.Proof.CheckedAt = at
		f.probeObservedAt = at
		f.Proof.ValidUntil = at.Add(time.Minute)
		a, b := routeartifact.HostedRecipient{Zone: "zone-a"}, routeartifact.HostedRecipient{Zone: "zone-b"}
		in := map[routeartifact.HostedRecipient][]dnsReadinessFact{a: {f}, b: {f}}
		s.hostedDNSRecovery(p, &policy, in, at)
		if !reflect.DeepEqual(in[a], in[b]) {
			t.Fatal("zone changed a physical observation")
		}
		return dnsAvailabilityRecovered(avail, in[a][0], time.Minute, at)
	}
	if run(at, true) || run(at, true) || !run(at.Add(12*time.Second), true) {
		t.Fatal("did not count distinct physical checks")
	}
	p.Decision = "decision-b"
	if run(at.Add(15*time.Second), true) {
		t.Fatal("new decision inherited recovery")
	}
	if run(at.Add(16*time.Second), false) || run(at.Add(27*time.Second), true) {
		t.Fatal("negative result did not reset recovery")
	}
	if !run(at.Add(39*time.Second), true) {
		t.Fatal("healthy decision never recovered")
	}
}
