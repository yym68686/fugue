package edgequality

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func historicalFixture(t *testing.T) (Snapshot, DNSBinding) {
	t.Helper()
	old := failureAwareFixture()
	old.Policy.Version = ComparableDeliveryPolicyVersion
	receipt, err := Capture(old)
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	binding := DNSBinding{ReceiptID: "original-answer", LoadedDigest: digest, PolicyDigest: digest, Hostname: old.Hostname,
		Scope: old.Scope, CurrentEdgeID: old.CurrentEdgeID, ObservedAt: old.CapturedAt, ReplayMatched: true, WriteSucceeded: true}
	selection, err := CompileSelection(receipt, binding, old.CapturedAt)
	if err != nil || selection.PrimaryEdgeID != "edge-b" {
		t.Fatal(selection, err)
	}
	snapshot := old
	snapshot.Policy = DefaultPairedContinuityPolicy()
	snapshot.CapturedAt = old.CapturedAt.Add(48 * time.Hour)
	snapshot.Observations = nil
	snapshot.Candidates = append([]Candidate(nil), old.Candidates...)
	for i := range snapshot.Candidates {
		at := snapshot.CapturedAt
		snapshot.Candidates[i].ProofObservedAt = &at
	}
	snapshot.HistoricalEvidence = &HistoricalEvidence{SourceArtifactID: "verified-parent", InputDigest: digest, Selection: selection, Receipt: receipt}
	binding.ObservedAt = snapshot.CapturedAt
	binding.ReceiptID = "fresh-actual-answer"
	return snapshot, binding
}

func TestHistoricalQualityReplaysOriginalPolicyWithoutNewProbeRounds(t *testing.T) {
	snapshot, binding := historicalFixture(t)
	for i := 0; i < 3; i++ {
		receipt, err := Capture(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Result.QualityState != "retained" || receipt.Result.History == nil || receipt.Result.ProposedEdgeID != "edge-b" || len(receipt.Result.Comparisons) != 0 {
			t.Fatal("old comparison lost or converted to new measurements", receipt.Result)
		}
		selection, err := CompileSelection(receipt, binding, snapshot.CapturedAt)
		if err != nil || selection.History.PolicyVersion != ComparableDeliveryPolicyVersion || !selection.QualityObservedAt.Equal(snapshot.HistoricalEvidence.Selection.CapturedAt) {
			t.Fatal("history renewed, reinterpreted or publication blocked", selection, err)
		}
		raw, _ := json.Marshal(receipt)
		var replayed Receipt
		if json.Unmarshal(raw, &replayed) != nil {
			t.Fatal("round trip")
		}
		if _, err := Replay(replayed); err != nil {
			t.Fatal("history not restart replayable", err)
		}
		var reordered any
		_ = json.Unmarshal(raw, &reordered)
		raw, _ = json.Marshal(reordered)
		_ = json.Unmarshal(raw, &replayed)
		if _, err := Replay(replayed); err != nil {
			t.Fatal("JSON export changed original legacy digest", err)
		}
		snapshot.CapturedAt = snapshot.CapturedAt.Add(time.Hour)
		binding.ObservedAt = snapshot.CapturedAt
		for j := range snapshot.Candidates {
			at := snapshot.CapturedAt
			snapshot.Candidates[j].ProofObservedAt = &at
		}
	}
}

func TestHistoricalOrderReadmitsRecoveredUnmeasuredCandidate(t *testing.T) {
	snapshot, binding := historicalFixture(t)
	candidate := snapshot.Candidates[0]
	candidate.EdgeID = "edge-c"
	snapshot.Candidates = append(snapshot.Candidates, candidate)
	for _, suspended := range []bool{true, false} {
		snapshot.Candidates[1].ServingBlockedReason = ""
		if suspended {
			snapshot.Candidates[1].ServingBlockedReason = "dns_admission_suspended"
		}
		receipt, err := Capture(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		selection, err := CompileSelection(receipt, binding, snapshot.CapturedAt)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"edge-b", "edge-a", "edge-c"}
		if suspended {
			want = []string{"edge-a", "edge-c"}
		}
		if !reflect.DeepEqual(selection.OrderedEdgeIDs, want) {
			t.Fatal("recovery required new measurements or bypassed suspension", selection)
		}
	}
}

func TestFreshComparableQualityTakesPrecedenceOverHistory(t *testing.T) {
	historical, _ := historicalFixture(t)
	fresh := pairedAlgorithmFixture()
	shift := historical.CapturedAt.Sub(fresh.CapturedAt)
	fresh.CapturedAt = historical.CapturedAt
	fresh.Candidates = historical.Candidates
	fresh.HistoricalEvidence = historical.HistoricalEvidence
	for i := range fresh.Observations {
		fresh.Observations[i].ObservedAt = fresh.Observations[i].ObservedAt.Add(shift)
	}
	// Direct comparator fixture, as in network_continuity_test.go: complete
	// attested report validation has independent signed-protocol coverage.
	result := evaluateNetwork(fresh)
	if result.QualityState != "measured" || result.History != nil || result.ProposedEdgeID != "edge-b" {
		t.Fatal("old history replaced fresh comparable evidence", result)
	}
}

func TestHistoricalQualityCannotBypassCurrentGatesOrChangeContext(t *testing.T) {
	for _, scenario := range []string{"suspended", "route changed", "route stale", "wrong scope", "wrong path", "forged winner", "nested history"} {
		t.Run(scenario, func(t *testing.T) {
			snapshot, _ := historicalFixture(t)
			switch scenario {
			case "suspended":
				snapshot.Candidates[1].ServingBlockedReason = "dns_admission_suspended"
			case "route changed":
				snapshot.Candidates[1].RouteGeneration = "different-route"
			case "route stale":
				at := snapshot.CapturedAt.Add(-time.Hour)
				snapshot.Candidates[1].ProofObservedAt = &at
			case "wrong scope":
				snapshot.Scope = "tcp_peer:198.51.100.0/24"
			case "wrong path":
				snapshot.PathPrefix = "/other/"
			case "forged winner":
				snapshot.HistoricalEvidence.Selection.OrderedEdgeIDs = []string{"edge-a", "edge-b"}
				snapshot.HistoricalEvidence.Selection.PrimaryEdgeID = "edge-a"
			case "nested history":
				snapshot.HistoricalEvidence.Receipt.Snapshot.HistoricalEvidence = &HistoricalEvidence{}
			}
			receipt, err := Capture(snapshot)
			if err == nil && receipt.Result.History != nil && receipt.Result.ProposedEdgeID == "edge-b" {
				t.Fatal("history bypassed guard", receipt.Result)
			}
		})
	}
}

func TestSharedHistoricalOrderRequiresEveryServiceOwner(t *testing.T) {
	for _, complete := range []bool{true, false} {
		children := []Receipt{}
		var binding DNSBinding
		for i, host := range []string{"first.example.test", "second.example.test"} {
			snapshot, currentBinding := historicalFixture(t)
			old := snapshot.HistoricalEvidence.Receipt.Snapshot
			old.Hostname, old.DNSHostname = host, "alias.example.test"
			for j := range old.Observations {
				old.Observations[j].Hostname = host
			}
			original, err := Capture(old)
			if err != nil {
				t.Fatal(err)
			}
			originalBinding := currentBinding
			originalBinding.Hostname, originalBinding.ObservedAt = "alias.example.test", old.CapturedAt
			selection, err := CompileSelection(original, originalBinding, old.CapturedAt)
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Hostname, snapshot.DNSHostname = host, "alias.example.test"
			snapshot.HistoricalEvidence.Receipt, snapshot.HistoricalEvidence.Selection = original, selection
			snapshot.ActualDNSReceipt = json.RawMessage(`{"shared":"actual-dns"}`)
			if !complete && i == 1 {
				snapshot.HistoricalEvidence = nil
			}
			child, err := Capture(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			children = append(children, child)
			binding = currentBinding
			binding.Hostname = "alias.example.test"
		}
		root, err := ServiceConsensusSnapshot("alias.example.test", children)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := Capture(root)
		if err != nil {
			t.Fatal(err)
		}
		selection, err := CompileSelection(receipt, binding, root.CapturedAt)
		if err != nil {
			t.Fatal(err)
		}
		if complete && (selection.PrimaryEdgeID != "edge-b" || selection.History == nil) {
			t.Fatal("shared history lost", selection)
		}
		if !complete && (selection.PrimaryEdgeID != "edge-a" || selection.History != nil) {
			t.Fatal("one owner lent quality evidence to another", selection)
		}
	}
}
