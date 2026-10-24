package dnsserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"fugue/internal/platformconsumer"
	"github.com/miekg/dns"
)

func TestDNSSnapshotFailureRetainsExactReplayThroughJoinedStandbyError(t *testing.T) {
	state, _ := businessUnavailableServingState(t)
	before, _ := json.Marshal(state.zones["example.test"].records)
	err := probeDNSServingSnapshot(state)
	var failure *dnsSnapshotProbeFailure
	if !errors.As(err, &failure) || !strings.Contains(err.Error(), "name_digest=") || strings.Contains(err.Error(), "example.test") || failure.response.Rcode != dns.RcodeServerFailure {
		t.Fatal("snapshot failure lost its exact query", err)
	}
	service := &Service{}
	journal := decisionTestJournal(t)
	service.decisionAudit.Store(journal)
	service.recordDNSSnapshotFailure(errors.Join(err, platformconsumer.ErrDNSBackendNotSelected))
	select {
	case pending := <-journal.queue:
		journal.append(pending)
	default:
		t.Fatal("standby reporting rejection hid the candidate failure")
	}
	if journal.dropped.Load() != 0 || len(*journal.records.Load()) != 1 {
		t.Fatal("failed to retain bounded snapshot evidence")
	}
	receipt := (*journal.records.Load())[0]
	if receipt.Transport != "snapshot" || receipt.WriteSucceeded || receipt.Publication.Loaded != nil || receipt.Publication.LKG != nil || receipt.Publication.ServingLKG || receipt.Publication.Desired == nil || receipt.Publication.Outcome != "snapshot_probe_failed" || !receipt.ObservedAt.Equal(failure.observedAt) {
		t.Fatal("local candidate execution misrepresented as public serving evidence", receipt)
	}
	result, replayErr := ReplayDNSDecision(receipt)
	if replayErr != nil || !result.Matched || result.RCode != dns.RcodeServerFailure || len(result.Records) != 1 || result.Records[0].SelectionResult != "business_no_ready_endpoint" {
		t.Fatal("candidate failure does not replay with its original reason", result, replayErr)
	}
	after, _ := json.Marshal(state.zones["example.test"].records)
	if !bytes.Equal(before, after) {
		t.Fatal("diagnostics modified candidate policy or facts")
	}
}

func TestDNSSnapshotSuccessDoesNotCreateFailureEvidence(t *testing.T) {
	state, _ := decisionTestState(t)
	if err := probeDNSServingSnapshot(state); err != nil {
		t.Fatal(err)
	}
	service := &Service{}
	journal := decisionTestJournal(t)
	service.decisionAudit.Store(journal)
	for _, err := range []error{nil, errors.New("unrelated error"), platformconsumer.ErrDNSBackendNotSelected} {
		service.recordDNSSnapshotFailure(err)
	}
	if len(journal.queue) != 0 || journal.dropped.Load() != 0 {
		t.Fatal("unrelated outcomes created snapshot evidence")
	}
}
