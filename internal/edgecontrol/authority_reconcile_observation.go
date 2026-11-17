package edgecontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"sync"
	"time"

	"fugue/internal/model"
)

const authorityReconcileLimit = 24
const authorityReconcileActiveLimit = 4

// These receipts describe the original reconciliation only. Reading them does
// not fetch inputs, take a store lock, replay a check, or grant authority.
type authorityReconcileObservations struct {
	mu        sync.Mutex
	sequence  uint64
	completed uint64
	failed    uint64
	dropped   uint64
	active    map[uint64]*authorityReconcileReceipt
	receipts  []authorityReconcileReceipt
	failures  []authorityReconcileReceipt
}

type authorityReconcilePublication struct {
	Generation string `json:"generation_hash,omitempty"`
	Parent     string `json:"release_set_hash,omitempty"`
	Route      string `json:"route_artifact_hash,omitempty"`
	Release    string `json:"release_hash,omitempty"`
	Channel    string `json:"channel,omitempty"`
	Fence      int64  `json:"fencing_token"`
}

type authorityReconcilePhase struct {
	Name       string    `json:"phase"`
	StartedAt  time.Time `json:"started_at"`
	DurationMS int64     `json:"duration_ms"`
	ErrorClass string    `json:"error_class,omitempty"`
}

type authorityReconcileReceipt struct {
	Sequence           uint64                        `json:"sequence"`
	StartedAt          time.Time                     `json:"started_at"`
	FinishedAt         time.Time                     `json:"finished_at,omitempty"`
	Outcome            string                        `json:"outcome"`
	ErrorClass         string                        `json:"error_class,omitempty"`
	Source             authorityReconcilePublication `json:"source"`
	Rechecked          authorityReconcilePublication `json:"rechecked"`
	Phases             []authorityReconcilePhase     `json:"phases"`
	Published          int                           `json:"published"`
	Failed             int                           `json:"failed"`
	PublicationFailure string                        `json:"publication_failure,omitempty"`
}

type authorityReconcileTrace struct {
	owner   *authorityReconcileObservations
	receipt *authorityReconcileReceipt
	phaseAt time.Time
}

func (o *authorityReconcileObservations) begin() *authorityReconcileTrace {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.active) >= authorityReconcileActiveLimit {
		o.dropped++
		return nil
	}
	if o.active == nil {
		o.active = make(map[uint64]*authorityReconcileReceipt)
	}
	o.sequence++
	r := &authorityReconcileReceipt{Sequence: o.sequence, StartedAt: time.Now().UTC(), Outcome: "in_flight"}
	o.active[r.Sequence] = r
	return &authorityReconcileTrace{owner: o, receipt: r}
}

func (t *authorityReconcileTrace) phase(name string) {
	if t == nil {
		return
	}
	switch name {
	case "route_fetch", "inventory_read", "cache_check", "compile", "route_recheck", "publish", "cache_store":
	default:
		return
	}
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	now := time.Now()
	t.completePhase(now)
	if len(t.receipt.Phases) < 7 {
		t.receipt.Phases = append(t.receipt.Phases, authorityReconcilePhase{Name: name, StartedAt: now.UTC()})
		t.phaseAt = now
	}
}

func (t *authorityReconcileTrace) completePhase(now time.Time) {
	if !t.phaseAt.IsZero() {
		t.receipt.Phases[len(t.receipt.Phases)-1].DurationMS = max(int64(0), now.Sub(t.phaseAt).Milliseconds())
	}
}

func authorityReconcileError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	default:
		return RouteIntentFailureCode(err)
	}
}

func (t *authorityReconcileTrace) phaseError(err error) {
	if t == nil || err == nil {
		return
	}
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	if n := len(t.receipt.Phases); n > 0 {
		t.receipt.Phases[n-1].ErrorClass = authorityReconcileError(err)
	}
}

func (t *authorityReconcileTrace) inputChanged() {
	if t == nil {
		return
	}
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	if n := len(t.receipt.Phases); n > 0 {
		t.receipt.Phases[n-1].ErrorClass = "input_changed"
	}
}

func authorityReconcilePublicationFailure(code string) string {
	switch code {
	case GroupAuthorityFailureCandidateRead, GroupAuthorityFailureCandidateCAS, GroupAuthorityFailureSigning,
		GroupAuthorityFailurePublicationCAS, GroupAuthorityFailurePublishedPointerCAS, GroupAuthorityFailureRecoveryEpochCAS,
		GroupAuthorityFailureAuditTailCAS, GroupAuthorityFailurePublicationValidation, GroupAuthorityFailurePendingRetention,
		GroupShadowFailureInventoryRead, GroupShadowFailureInventoryInvalid, GroupShadowFailureNoHealthyActive,
		GroupShadowFailureNoRoutableRoutes, GroupShadowFailureCompile, GroupShadowFailureLedgerRead, GroupShadowFailureLedgerCAS:
		return code
	default:
		return "unclassified"
	}
}

func authorityReconcileHash(value string) string {
	if value == "" {
		return ""
	}
	if len(value) > 4096 {
		return "identity_exceeds_budget"
	}
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (t *authorityReconcileTrace) source(snapshot model.EdgeRouteIntentSnapshot, rechecked bool) {
	if t == nil {
		return
	}
	value := authorityReconcilePublication{Generation: authorityReconcileHash(snapshot.Generation)}
	if b := snapshot.TrafficRelease; b != nil {
		channel := b.ReleaseChannel
		switch channel {
		case "shadow", "gray", "full":
		default:
			channel = "unrecognized"
		}
		value.Parent, value.Route, value.Release = authorityReconcileHash(b.ReleaseSetID), authorityReconcileHash(b.RouteArtifactID), authorityReconcileHash(b.ReleaseID)
		value.Channel, value.Fence = channel, b.FencingToken
	}
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	if rechecked {
		t.receipt.Rechecked = value
	} else {
		t.receipt.Source = value
	}
}

func cloneAuthorityReconcileReceipt(r authorityReconcileReceipt) authorityReconcileReceipt {
	r.Phases = slices.Clone(r.Phases)
	return r
}

func appendAuthorityReconcileReceipt(rows []authorityReconcileReceipt, r authorityReconcileReceipt) []authorityReconcileReceipt {
	if len(rows) == authorityReconcileLimit {
		copy(rows, rows[1:])
		rows = rows[:len(rows)-1]
	}
	return append(rows, cloneAuthorityReconcileReceipt(r))
}

func (t *authorityReconcileTrace) finish(batch AuthorityRuntimeBatch, skipped bool, err error) {
	if t == nil {
		return
	}
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	r := t.receipt
	t.completePhase(time.Now())
	r.FinishedAt = time.Now().UTC()
	r.Outcome, r.Published, r.Failed = "published", batch.Published.Published, batch.Published.Failed
	r.ErrorClass = authorityReconcileError(err)
	if err != nil {
		r.Outcome = "failed"
		if n := len(r.Phases); n > 0 && r.Phases[n-1].ErrorClass == "" {
			r.Phases[n-1].ErrorClass = r.ErrorClass
		}
	} else if batch.Published.Failed > 0 {
		r.Outcome = "publication_failed"
	} else if skipped {
		r.Outcome = "cached"
	}
	// Retain one fixed failure category, never arbitrary result bodies or IDs.
	for _, result := range batch.Published.Results {
		if result.Status == GroupAuthorityStatusFailed {
			r.PublicationFailure = authorityReconcilePublicationFailure(result.FailureCode)
			break
		}
	}
	delete(t.owner.active, r.Sequence)
	t.owner.completed++
	t.owner.receipts = appendAuthorityReconcileReceipt(t.owner.receipts, *r)
	if err != nil || batch.Published.Failed > 0 {
		t.owner.failed++
		t.owner.failures = appendAuthorityReconcileReceipt(t.owner.failures, *r)
	}
}

func (runtime *AuthorityRuntime) ReconcileObservationSnapshot(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o := &runtime.observations
	o.mu.Lock()
	defer o.mu.Unlock()
	clone := func(rows []authorityReconcileReceipt) []authorityReconcileReceipt {
		out := make([]authorityReconcileReceipt, len(rows))
		for i, r := range rows {
			out[i] = cloneAuthorityReconcileReceipt(r)
		}
		return out
	}
	active := make([]authorityReconcileReceipt, 0, len(o.active))
	for _, r := range o.active {
		copy := cloneAuthorityReconcileReceipt(*r)
		if n := len(copy.Phases); n > 0 {
			copy.Phases[n-1].DurationMS = max(int64(0), time.Since(copy.Phases[n-1].StartedAt).Milliseconds())
		}
		active = append(active, copy)
	}
	return map[string]any{"schema": "fugue.edge-control.reconcile.observation.v1", "observed_at": time.Now().UTC(),
		"completed": o.completed, "failed": o.failed, "dropped": o.dropped, "limit": authorityReconcileLimit,
		"evicted": o.completed - uint64(len(o.receipts)), "failures_evicted": o.failed - uint64(len(o.failures)),
		"active_limit": authorityReconcileActiveLimit, "active": active, "receipts": clone(o.receipts), "failures": clone(o.failures),
		"coverage": "bounded_original_local_reconciliations_not_end_to_end_delivery", "authorizes_traffic": false}, nil
}
