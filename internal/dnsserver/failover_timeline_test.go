package dnsserver

import (
	"reflect"
	"testing"
	"time"

	"github.com/miekg/dns"
)

func TestDNSFailoverTimelineBoundsLastPrimaryAnswerByOriginalProofExpiry(t *testing.T) {
	view, plan, policy, initial, started := queryExecutionFixture()
	plan.Records[0].MinimumHealthyEdges = 1
	primaryExpires := started.Add(2 * time.Second)
	for index := range initial {
		if initial[index].Proof.EdgeID == "edge-a" {
			initial[index].Proof.ValidUntil = primaryExpires
		}
	}
	original := append([]dnsReadinessFact(nil), initial...)
	failedAt := started.Add(time.Second)
	failed := append([]dnsReadinessFact(nil), initial...)
	for index := range failed {
		if failed[index].Proof.EdgeID == "edge-a" {
			failed[index].Ready, failed[index].Reason = false, "probe_unavailable"
		}
	}
	retained := retainValidDNSReadinessFacts(&plan, &policy, initial, failed, failedAt)
	answerAt := func(at time.Time) []dns.RR {
		materialized, err := materializeDNSQueries(view, &plan, &policy, retained, at)
		if err != nil {
			t.Fatal(err)
		}
		answers, err := executeDNSQueryRecord(materialized[0], dnsGeoHint{}, at)
		if err != nil {
			t.Fatal(err)
		}
		return answers
	}
	lastPrimary := answerAt(failedAt)
	if len(lastPrimary) != 1 || lastPrimary[0].(*dns.A).A.String() != "8.8.8.8" || lastPrimary[0].Header().Ttl > 1 {
		t.Fatal("last primary answer exceeded its original proof lease", lastPrimary)
	}
	firstBackup := answerAt(primaryExpires.Add(time.Millisecond))
	if len(firstBackup) != 1 || firstBackup[0].(*dns.A).A.String() != "9.9.9.9" {
		t.Fatal("expired primary prevented an independently proven backup", firstBackup)
	}
	for _, fact := range retained {
		if fact.Proof.EdgeID == "edge-a" && !fact.Proof.ValidUntil.Equal(primaryExpires) {
			t.Fatal("retry renewed failed endpoint proof")
		}
	}
	if !reflect.DeepEqual(initial, original) || plan.Records[0].MinimumHealthyEdges != 1 || policy.FactFreshnessSeconds == 0 {
		t.Fatal("timeline altered its original facts or policy")
	}
	t.Logf("failure_observed=%s last_primary_answer=%s primary_proof_expires=%s first_backup_answer=%s ttl=%d recursive_cache=not_simulated existing_connections=not_migrated", failedAt.Format(time.RFC3339Nano), failedAt.Format(time.RFC3339Nano), primaryExpires.Format(time.RFC3339Nano), primaryExpires.Add(time.Millisecond).Format(time.RFC3339Nano), lastPrimary[0].Header().Ttl)
}
