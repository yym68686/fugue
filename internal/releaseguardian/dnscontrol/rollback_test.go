package dnscontrol

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
)

type rollbackJournalFixture struct {
	intent  productioncontrol.Intent
	payload []byte
	state   productioncontrol.ResourceState
	claim   productioncontrol.Claim
	result  *productioncontrol.ExecutionResult
	receipt error
}

func (f rollbackJournalFixture) ReadIntent(context.Context, string) (productioncontrol.Intent, []byte, error) {
	return f.intent, f.payload, nil
}
func (f rollbackJournalFixture) ReadResource(context.Context, productioncontrol.Scope) (productioncontrol.ResourceState, error) {
	return f.state, nil
}
func (f rollbackJournalFixture) ReadExecution(context.Context, productioncontrol.Scope, string) (productioncontrol.Claim, *productioncontrol.ExecutionResult, error) {
	return f.claim, f.result, nil
}
func (f rollbackJournalFixture) CheckAppliedGlobalProducerReceipt(context.Context, productioncontrol.Claim, productioncontrol.ExecutionResult, int64) error {
	return f.receipt
}

func TestDNSRollbackRequiresCurrentSettledAppliedVersion(t *testing.T) {
	now := time.Now().UTC()
	sources, before, after, predecessor := testSources(t, now)
	submission, err := Draft("fixture", "original-dns-change", "https://api.example.test", predecessor, sources, before, after, now)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := submission.Intent.Digest()
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope("fixture")
	result := &productioncontrol.ExecutionResult{Outcome: productioncontrol.Applied,
		Verified:       productioncontrol.Version{Generation: submission.Intent.Generation + 1, Digest: submission.Intent.TargetDigest},
		EvidenceDigest: "sha256:" + strings.Repeat("a", 64)}
	fixture := rollbackJournalFixture{intent: submission.Intent, payload: submission.Payload,
		state: productioncontrol.ResourceState{Baseline: productioncontrol.Baseline{Scope: scope},
			LastVerified: result.Verified, LastFence: 1, LastIntent: digest},
		claim: productioncontrol.Claim{Scope: scope, Fence: 1, IntentDigest: digest, ApprovalDigest: "sha256:" + strings.Repeat("b", 64)}, result: result}

	source, err := readRollbackSource(t.Context(), fixture, "fixture", digest)
	if err != nil {
		t.Fatal(err)
	}
	inverse := source.change()
	originalPayload, err := Inspect(submission)
	if err != nil {
		t.Fatal(err)
	}
	if inverse.EdgeID != originalPayload.ChangedEdge || inverse.Before == nil || inverse.After != nil ||
		*inverse.Before != *originalPayload.AfterHold || inverse.After != originalPayload.BeforeHold {
		t.Fatalf("unexpected inverse transition: %+v", inverse)
	}
	reverseDNS, err := planNextDNS(sources.NextDNS, sources.Static, sources.NextProducer, inverse)
	if err != nil {
		t.Fatal(err)
	}
	if _, stillHeld := reverseDNS.Content["offline_edges"]; stillHeld {
		t.Fatal("inverse policy draft retained the original offline hold")
	}
	if source.head.LastVerified != result.Verified || source.binding != (productioncontrol.GlobalProducerRollbackRef{
		IntentDigest: digest, ApprovalDigest: fixture.claim.ApprovalDigest, EvidenceDigest: result.EvidenceDigest}) {
		t.Fatal("rollback did not bind the settled predecessor")
	}
	reversed := originalPayload
	reversed.Reverses = &source.binding
	reversed.PreviousProducer = originalPayload.NextProducer
	reversed.PreviousDNS = originalPayload.NextDNS
	reversed.Precondition.PreviousPolicy.FencingToken++
	reversed.BeforeHold, reversed.AfterHold = reversed.AfterHold, reversed.BeforeHold
	if err := source.verifyInverse(reversed); err != nil {
		t.Fatal(err)
	}
	reversed.AfterHold = reversed.BeforeHold
	if err := source.verifyInverse(reversed); err == nil {
		t.Fatal("non-inverse hold accepted")
	}

	for name, mutate := range map[string]func(*rollbackJournalFixture){
		"no result":   func(f *rollbackJournalFixture) { f.result = nil },
		"not applied": func(f *rollbackJournalFixture) { f.result.Outcome = productioncontrol.NotApplied },
		"no receipt":  func(f *rollbackJournalFixture) { f.receipt = errors.New("no receipt") },
		"pending":     func(f *rollbackJournalFixture) { f.state.Pending = digest },
		"wrong current value": func(f *rollbackJournalFixture) {
			f.state.LastVerified.Digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		},
		"wrong claim fence": func(f *rollbackJournalFixture) { f.claim.Fence++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := fixture
			changed.state = fixture.state
			changed.claim = fixture.claim
			if fixture.result != nil {
				resultCopy := *fixture.result
				changed.result = &resultCopy
			}
			mutate(&changed)
			if _, err := readRollbackSource(t.Context(), changed, "fixture", digest); err == nil {
				t.Fatal("unsafe rollback source accepted")
			}
		})
	}
}
