package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"fugue/internal/releaseguardian"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// frontRepairReviewedIntent is the exact input of the repair executor, not
// proof that the release lane approved it. The launcher must derive it from
// verified release, approval and preview artifacts, never from CLI overrides.
type frontRepairReviewedIntent struct {
	Group               string
	PlanDigest          string
	AuthorizationDigest string
	Nodes               []frontRepairNode
}

func (intent frontRepairReviewedIntent) matches(journal frontRepairJournal) bool {
	if intent.Group != journal.Plan.Before.GroupID || intent.PlanDigest != journal.Plan.Digest ||
		intent.AuthorizationDigest != journal.AuthorizationDigest || len(intent.Nodes) != len(journal.Plan.Members) {
		return false
	}
	nodes := make(map[string]frontRepairNode, len(intent.Nodes))
	for _, node := range intent.Nodes {
		if _, exists := nodes[node.Name]; exists {
			return false
		}
		nodes[node.Name] = node
	}
	for _, member := range journal.Plan.Members {
		if nodes[member.Node.Name] != member.Node {
			return false
		}
	}
	return true
}

type frontRepairOpened struct {
	journal   frontRepairJournal
	completed bool
}

// This operation is intentionally not wired to a production command. An
// approved launcher must bind the runtime and both leases to the same client
// and release; these helpers do not select or approve a repair.
func (r frontRepairRuntime) openReviewed(ctx context.Context, store frontRepairStore, intent frontRepairReviewedIntent) (frontRepairOpened, error) {
	if r.activator == nil || store.namespace != r.activator.config.Namespace || intent.Group != r.activator.config.GroupID {
		return frontRepairOpened{}, errors.New("Front repair runtime does not match the reviewed group")
	}
	return store.openReviewed(ctx, intent, func(ctx context.Context, before releaseguardian.CurrentAuthority, nodes []frontRepairNode) ([]frontRepairWitness, error) {
		observed, err := r.observe(ctx, before, nodes)
		if err != nil {
			return nil, err
		}
		return frontRepairWitnesses(observed), nil
	})
}

func frontRepairWitnesses(observed []frontRepairRuntimeMember) []frontRepairWitness {
	out := make([]frontRepairWitness, len(observed))
	for i := range observed {
		out[i] = observed[i].frontRepairWitness
	}
	return out
}

// Open the exact existing transaction before allowing any new writes. A
// completed journal is a historical receipt only, not a health assertion for
// the currently deployed release. Creating a fresh journal requires a new
// live observation matching every byte of the reviewed predecessor plan.
func (store frontRepairStore) openReviewed(ctx context.Context, intent frontRepairReviewedIntent,
	observe func(context.Context, releaseguardian.CurrentAuthority, []frontRepairNode) ([]frontRepairWitness, error)) (frontRepairOpened, error) {
	group := intent.Group
	if !exactSHA256Digest(intent.PlanDigest) || !exactSHA256Digest(intent.AuthorizationDigest) || len(intent.Nodes) < 2 || len(intent.Nodes) > 100 {
		return frontRepairOpened{}, errors.New("Front repair reviewed intent is invalid")
	}
	if err := store.requireHeld(ctx, group); err != nil {
		return frontRepairOpened{}, err
	}
	// The shared name can also hold an ordinary promotion. Decode errors
	// or conflicts at that name always stop repair, even on a repeat run.
	journal, err := store.load(ctx, group)
	if !apierrors.IsNotFound(err) {
		if err != nil || !intent.matches(journal) {
			return frontRepairOpened{}, errors.Join(errors.New("Front repair cannot resume a different durable transaction"), err)
		}
		return frontRepairOpened{journal: journal}, nil
	}
	completion, err := store.loadCompletion(ctx, group, intent.AuthorizationDigest)
	if !apierrors.IsNotFound(err) {
		if err != nil || !intent.matches(completion) {
			return frontRepairOpened{}, errors.Join(errors.New("Front repair cannot use a different completion receipt"), err)
		}
		return frontRepairOpened{journal: completion, completed: true}, nil
	}
	if observe == nil {
		return frontRepairOpened{}, errors.New("Front repair has no live observation adapter")
	}
	if err := store.authority.RequireNoActivatedTransition(ctx, group); err != nil {
		return frontRepairOpened{}, err
	}
	current, uid, rv, err := store.authority.LoadCurrent(ctx, group)
	if err != nil {
		return frontRepairOpened{}, err
	}
	witnesses, err := observe(ctx, current, intent.Nodes)
	if err != nil {
		return frontRepairOpened{}, err
	}
	now := store.locks.now().UTC()
	for _, witness := range witnesses {
		if witness.ObservedAt.IsZero() || witness.ObservedAt.After(now) || now.Sub(witness.ObservedAt) >= 20*time.Second {
			return frontRepairOpened{}, fmt.Errorf("Front repair prerequisite observation on %s expired", witness.Node.Name)
		}
	}
	plan, err := planPartialFrontRollbackRepair(current, string(uid), rv, intent.Nodes, witnesses)
	if err != nil || plan.Digest != intent.PlanDigest {
		return frontRepairOpened{}, errors.Join(errors.New("Front repair live predecessor differs from the approved preview"), err)
	}
	journal, err = sealFrontRepairJournal(plan, intent.AuthorizationDigest, now)
	if err != nil {
		return frontRepairOpened{}, err
	}
	if err := store.create(ctx, journal); err != nil {
		return frontRepairOpened{}, err
	}
	return frontRepairOpened{journal: journal}, nil
}
