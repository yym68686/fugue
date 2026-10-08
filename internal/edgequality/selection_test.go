package edgequality

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func selectionFixture(t *testing.T) (Receipt, DNSBinding) {
	t.Helper()
	snapshot := fixture()
	snapshot.Scope = "global"
	for index := range snapshot.Observations {
		snapshot.Observations[index].Scope = snapshot.Scope
	}
	receipt, err := Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	return receipt, DNSBinding{ReceiptID: "actual-answer-1", LoadedDigest: digest, PolicyDigest: digest,
		Hostname: snapshot.Hostname, Scope: snapshot.Scope, CurrentEdgeID: snapshot.CurrentEdgeID,
		ObservedAt: snapshot.CapturedAt.Add(-time.Second), ReplayMatched: true, WriteSucceeded: true}
}

func TestCompilePhysicalSelectionRequiresBoundEvidence(t *testing.T) {
	receipt, binding := selectionFixture(t)
	selection, err := CompileSelection(receipt, binding, receipt.Snapshot.CapturedAt)
	if err != nil || selection.PrimaryEdgeID != "edge-b" || len(selection.OrderedEdgeIDs) != 2 || selection.EvidenceDigest != receipt.Digest {
		t.Fatal(selection, err)
	}
	if err := model.ValidateDNSPhysicalSelection(selection); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		edit func(*DNSBinding)
	}{
		{"unmatched_replay", func(value *DNSBinding) { value.ReplayMatched = false }},
		{"failed_write", func(value *DNSBinding) { value.WriteSucceeded = false }},
		{"wrong_hostname", func(value *DNSBinding) { value.Hostname = "other.example.test" }},
		{"wrong_scope", func(value *DNSBinding) { value.Scope = "asn:other" }},
		{"wrong_edge", func(value *DNSBinding) { value.CurrentEdgeID = "sibling" }},
		{"stale", func(value *DNSBinding) { value.ObservedAt = value.ObservedAt.Add(-time.Hour) }},
		{"future", func(value *DNSBinding) { value.ObservedAt = value.ObservedAt.Add(time.Hour) }},
		{"missing_publication", func(value *DNSBinding) { value.LoadedDigest = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			modified := binding
			test.edit(&modified)
			if _, err := CompileSelection(receipt, modified, receipt.Snapshot.CapturedAt); err == nil {
				t.Fatal("accepted unbound evidence")
			}
		})
	}
	if _, err := CompileSelection(receipt, binding, receipt.Snapshot.CapturedAt.Add(time.Hour)); err == nil {
		t.Fatal("compiled stale evidence")
	}
}

func TestCompilePhysicalSelectionCannotPromoteIncompleteShadow(t *testing.T) {
	for _, mutation := range []func(*Snapshot){
		func(value *Snapshot) { value.Blockers = []string{"capacity_limit_unknown"} },
		func(value *Snapshot) { value.Observations = nil },
		func(value *Snapshot) {
			value.Candidates[0].RouteProofVerified = false
			value.Candidates[1].RouteProofVerified = false
		},
	} {
		receipt, binding := selectionFixture(t)
		mutation(&receipt.Snapshot)
		modified, err := Capture(receipt.Snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := CompileSelection(modified, binding, receipt.Snapshot.CapturedAt); err == nil {
			t.Fatal("promoted incomplete evidence")
		}
	}
}

func TestCompilePhysicalSelectionPreservesCooldownAndFastFailure(t *testing.T) {
	receipt, binding := selectionFixture(t)
	now := receipt.Snapshot.CapturedAt
	receipt.Snapshot.LastSwitchAt = &now
	held, err := Capture(receipt.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := CompileSelection(held, binding, now)
	if err != nil || selection.PrimaryEdgeID != "edge-a" {
		t.Fatal("ignored switch cooldown", selection, err)
	}
	receipt.Snapshot.Candidates[0].HardGates = []string{"route_unready"}
	failed, err := Capture(receipt.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	selection, err = CompileSelection(failed, binding, now)
	if err != nil || selection.PrimaryEdgeID != "edge-b" || len(selection.OrderedEdgeIDs) != 1 {
		t.Fatal("failure did not bypass comparative cooldown", selection, err)
	}
}
