package dnscontrol

import (
	"context"
	"errors"
	"math"
	"reflect"
	"time"

	"fugue/internal/platformproducer"
	"fugue/internal/productioncontrol"
)

// rollbackJournal is the read-only surface needed to prove that a candidate
// rollback reverses the currently settled DNS operation, not a stale request.
type rollbackJournal interface {
	ReadIntent(context.Context, string) (productioncontrol.Intent, []byte, error)
	ReadResource(context.Context, productioncontrol.Scope) (productioncontrol.ResourceState, error)
	ReadExecution(context.Context, productioncontrol.Scope, string) (productioncontrol.Claim, *productioncontrol.ExecutionResult, error)
	CheckAppliedGlobalProducerReceipt(context.Context, productioncontrol.Claim, productioncontrol.ExecutionResult, int64) error
}

type rollbackSource struct {
	original Payload
	head     productioncontrol.ResourceState
	binding  productioncontrol.GlobalProducerRollbackRef
}

func readRollbackSource(ctx context.Context, journal rollbackJournal, cluster, originalDigest string) (rollbackSource, error) {
	if journal == nil || cluster == "" || !platformproducer.ValidDigest(originalDigest) {
		return rollbackSource{}, errors.New("DNS rollback requires a persisted original intent")
	}
	intent, raw, err := journal.ReadIntent(ctx, originalDigest)
	if err != nil {
		return rollbackSource{}, err
	}
	submission := productioncontrol.Submission{Intent: intent, Payload: raw}
	actual, digestErr := intent.Digest()
	payload, inspectErr := Inspect(submission)
	if digestErr != nil || actual != originalDigest || inspectErr != nil || intent.ClusterID != cluster ||
		intent.Generation >= math.MaxInt64-1 {
		return rollbackSource{}, errors.New("DNS rollback source is not an exact native producer intent")
	}
	scope := Scope(cluster)
	state, err := journal.ReadResource(ctx, scope)
	if err != nil {
		return rollbackSource{}, err
	}
	claim, result, err := journal.ReadExecution(ctx, scope, originalDigest)
	if err != nil || result == nil || claim.Scope != scope || claim.IntentDigest != originalDigest ||
		claim.Fence > state.LastFence || state.Scope != scope || state.Pending != "" ||
		result.Outcome != productioncontrol.Applied || result.Verified.Digest != intent.TargetDigest ||
		result.Verified.Generation != intent.Generation+1 || state.LastVerified != result.Verified ||
		claim.Fence < 1 || !platformproducer.ValidDigest(claim.ApprovalDigest) || !platformproducer.ValidDigest(result.EvidenceDigest) {
		return rollbackSource{}, errors.New("DNS rollback requires the exact currently settled Applied version")
	}
	if err := journal.CheckAppliedGlobalProducerReceipt(ctx, claim, *result, state.LastFence); err != nil {
		return rollbackSource{}, err
	}
	return rollbackSource{payload, state, productioncontrol.GlobalProducerRollbackRef{
		IntentDigest: originalDigest, ApprovalDigest: claim.ApprovalDigest, EvidenceDigest: result.EvidenceDigest,
	}}, nil
}

func (s rollbackSource) change() HoldChange {
	return HoldChange{EdgeID: s.original.ChangedEdge, Before: s.original.AfterHold, After: s.original.BeforeHold}
}

func (s rollbackSource) verifyInverse(p Payload) error {
	change := s.change()
	if p.Reverses == nil || *p.Reverses != s.binding || p.Origin != s.original.Origin ||
		p.PreviousProducer != s.original.NextProducer || p.PreviousDNS != s.original.NextDNS || p.Static != s.original.Static ||
		p.Precondition.PreviousPolicy.FencingToken != s.head.LastVerified.Generation ||
		p.ChangedEdge != change.EdgeID || !reflect.DeepEqual(p.BeforeHold, change.Before) || !reflect.DeepEqual(p.AfterHold, change.After) {
		return errors.New("DNS rollback is not the exact inverse of the current settled operation")
	}
	return nil
}

// DraftRollback first proves the journal predecessor, then prepares the exact
// inverse as immutable artifacts and repeats the normal read-only DNS shadow.
// It never dispatches or records approval; callers review and approve the
// returned fresh submission through the ordinary control chain.
func (r *Reader) DraftRollback(ctx context.Context, journal *productioncontrol.Journal, preparer *DraftPreparer, cluster, originalDigest, request string, now func() time.Time) (productioncontrol.Submission, error) {
	if r == nil || journal == nil || preparer == nil || preparer.reader != r || now == nil {
		return productioncontrol.Submission{}, errors.New("DNS rollback requires separate observation and draft profiles")
	}
	source, err := readRollbackSource(ctx, journal, cluster, originalDigest)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	if r.origin != source.original.Origin {
		return productioncontrol.Submission{}, errors.New("DNS rollback origin differs from the original operation")
	}
	// Validate the request token before making any immutable draft writes.
	if _, err := productioncontrol.NewIntent(cluster, Resource, request, productioncontrol.DNSMembership,
		source.head.LastVerified.Generation, source.head.LastVerified.Digest, source.head.LastVerified.Digest,
		source.head.LastVerified.Digest, []byte("rollback draft preflight")); err != nil {
		return productioncontrol.Submission{}, err
	}
	observed, err := preparer.prepare(ctx, source.change(), &source, now)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	check, err := readRollbackSource(ctx, journal, cluster, originalDigest)
	if err != nil || check.head != source.head || check.binding != source.binding {
		return productioncontrol.Submission{}, errors.New("DNS rollback predecessor changed during inverse shadow")
	}
	return draft(cluster, request, r.origin, check.head, observed.Sources, observed.Before, observed.After, now(), &check)
}

// RevalidateRollback checks persisted provenance and the typed inverse before
// the executor can reserve a claim. The journal and provider recheck the same
// source under the write fence.
func RevalidateRollback(ctx context.Context, j *productioncontrol.Journal, cluster string, p Payload) error {
	if p.Reverses == nil {
		return nil
	}
	if j == nil {
		return errors.New("DNS rollback requires the production journal")
	}
	source, err := readRollbackSource(ctx, j, cluster, p.Reverses.IntentDigest)
	if err != nil {
		return err
	}
	return source.verifyInverse(p)
}
