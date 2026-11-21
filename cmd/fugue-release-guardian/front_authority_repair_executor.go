package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgegroupfront"
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

const (
	frontRepairRunBudget         = 95 * time.Second
	frontRepairNodeClockBound    = 4 * time.Second
	frontRepairFrontFollowBudget = 6 * time.Second
)

// runReviewed obtains and releases the existing Worker component and Front
// authority Leases using the same Kubernetes client as the runtime observer.
// It is only for a launcher that has already selected an immutable release.
func (r frontRepairRuntime) runReviewed(ctx context.Context, release declarativerelease.PlanRelease, intent frontRepairReviewedIntent) (frontRepairOpened, error) {
	return r.runReviewedWithReleaseCheck(ctx, release, intent, nil)
}

func (r frontRepairRuntime) runReviewedWithReleaseCheck(ctx context.Context, release declarativerelease.PlanRelease,
	intent frontRepairReviewedIntent, recheck func(context.Context, frontRepairStore) error) (_ frontRepairOpened, err error) {
	if r.activator == nil || release.Workload.Namespace != r.activator.config.Namespace || intent.Group != r.activator.config.GroupID {
		return frontRepairOpened{}, errors.New("Front repair release does not bind the exact runtime namespace and group")
	}
	lockCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	locks, err := acquireFrontRepairLeases(lockCtx, r.activator.client, release, intent.Group, r.activator.now)
	cancel()
	if err != nil {
		return frontRepairOpened{}, err
	}
	defer func() { err = errors.Join(err, locks.release(ctx)) }()
	store, err := newFrontRepairStore(locks)
	if err != nil {
		return frontRepairOpened{}, err
	}
	if recheck != nil {
		if err := recheck(ctx, store); err != nil {
			return frontRepairOpened{}, err
		}
	}
	return r.executeReviewed(ctx, store, intent)
}

// executeReviewed keeps preparation, per-node writes and pointer commit in
// one group-fenced transaction. Only the verified declaration launcher may
// invoke this function with leases from the selected release. No production
// command invokes it yet.
func (r frontRepairRuntime) executeReviewed(ctx context.Context, store frontRepairStore, intent frontRepairReviewedIntent) (frontRepairOpened, error) {
	ctx, cancel := context.WithTimeout(ctx, frontRepairRunBudget)
	defer cancel()
	opened, err := r.openReviewed(ctx, store, intent)
	if err != nil || opened.completed {
		return opened, err
	}
	journal := opened.journal
	before, group := journal.Plan.Before, intent.Group
	preparations := make(map[string]edgegroupfront.PublicationPreparation)
	for {
		if err := store.requireHeld(ctx, group); err != nil {
			return opened, err
		}
		members, err := r.observe(ctx, before, intent.Nodes)
		if err != nil {
			return opened, err
		}
		witnesses := frontRepairWitnesses(members)
		progress, err := journal.resume(witnesses, store.locks.now().UTC())
		if err != nil {
			return opened, err
		}
		if err := store.requireRepairPredecessor(ctx, journal, progress.Converged); err != nil {
			return opened, err
		}
		if progress.Converged {
			if err := store.commit(ctx, group, journal.Digest, witnesses); err != nil {
				return opened, err
			}
			// Confirm the committed group pointer and live roster again before
			// archiving and releasing the shared fence.
			final, err := r.observe(ctx, before, intent.Nodes)
			if err != nil {
				return opened, err
			}
			if err := store.finish(ctx, journal, frontRepairWitnesses(final)); err != nil {
				return opened, err
			}
			opened.completed = true
			return opened, nil
		}
		now := store.locks.now().UTC()
		preparedThisPass := false
		for _, member := range members {
			name := member.Node.Name
			request, pending := progress.Remaining[name]
			if !pending || request.Expected.ActiveSlot == request.Committed.Slot {
				continue
			}
			p, present := preparations[name]
			if present {
				if !frontRepairPreparationReady(p, request.Expected, before, now) {
					return opened, fmt.Errorf("Front repair preparation on %s expired; no automatic refresh", name)
				}
				continue
			}
			var receipt edgegroupfront.PublicationPreparation
			raw, execErr := r.issueRepairNodeCommand(ctx, store, member, request, true)
			if execErr != nil || decodeStrictJSON(raw, &receipt) != nil ||
				!frontRepairPreparationReady(receipt, request.Expected, before, store.locks.now().UTC()) {
				return opened, errors.Join(fmt.Errorf("Front repair cannot confirm non-serving preparation on %s", name), execErr)
			}
			preparations[name] = receipt
			preparedThisPass = true
			break // reobserve the entire cohort after every node command
		}
		if preparedThisPass {
			continue
		}
		if !progress.WorkersReady {
			if err := waitFrontRepairObservation(ctx); err != nil {
				return opened, fmt.Errorf("Front repair Workers did not converge on one healthy signed publication: %w", err)
			}
			continue
		}
		checked, err := store.writePlan(ctx, group, journal.Digest, witnesses, preparations)
		if err != nil {
			return opened, err
		}
		// No-traffic-change generation bumps first. A divergent member is not
		// switched if the already-serving members could not be fenced.
		member, request, found := nextFrontRepairMember(members, checked)
		if !found {
			return opened, errors.New("Front repair has no observed member for an outstanding activation")
		}
		raw, execErr := r.issueRepairNodeCommand(ctx, store, member, request, false)
		var receipt edgegroupfront.ActivationReceipt
		acknowledged := execErr == nil && decodeStrictJSON(raw, &receipt) == nil && receipt.Schema == edgegroupfront.ActivationReceiptSchemaV1 &&
			receipt.PreviousExists && receipt.Previous != nil && receipt.Previous.UpdatedAt.Equal(request.Expected.UpdatedAt) &&
			*receipt.Previous == request.Expected && exactSHA256Digest(receipt.StateDigest) && request.MatchesActivation(receipt.Current)
		// Local disk and live Front are the proof, never a successful exec.
		// If left at the predecessor the same command is not issued again.
		if err := r.requireRepairApplied(ctx, member, request, journal.CreatedAt); err != nil {
			return opened, errors.Join(fmt.Errorf("Front repair activation on %s is unconfirmed (acknowledged=%t)", member.Node.Name, acknowledged), execErr, err)
		}
	}
}

func (store frontRepairStore) requireRepairPredecessor(ctx context.Context, journal frontRepairJournal, converged bool) error {
	plan, group := journal.Plan, journal.Plan.Before.GroupID
	stored, err := store.load(ctx, group)
	if err != nil || stored.Digest != journal.Digest {
		return errors.Join(errors.New("Front repair journal changed during execution"), err)
	}
	if err := store.authority.RequireNoActivatedTransition(ctx, group); err != nil {
		return err
	}
	actual, uid, rv, err := store.authority.LoadCurrent(ctx, group)
	if err != nil || string(uid) != plan.CurrentUID {
		return errors.Join(errors.New("Front repair authority identity changed during execution"), err)
	}
	if actual == plan.Before && rv == plan.CurrentResourceVersion || converged && actual == journal.committedAuthority() {
		return nil
	}
	return errors.New("Front repair authority no longer proves the exact predecessor or completed pointer")
}

func nextFrontRepairMember(members []frontRepairRuntimeMember, progress frontRepairProgress) (frontRepairRuntimeMember, edgegroupfront.ActivationRepairCASRequest, bool) {
	// observe sorts on Node name; preserve the order within each class.
	for _, switchSlot := range []bool{false, true} {
		for _, member := range members {
			request, ok := progress.Remaining[member.Node.Name]
			if ok && (request.Expected.ActiveSlot != request.Committed.Slot) == switchSlot {
				return member, request, true
			}
		}
	}
	return frontRepairRuntimeMember{}, edgegroupfront.ActivationRepairCASRequest{}, false
}

func (r frontRepairRuntime) issueRepairNodeCommand(ctx context.Context, store frontRepairStore, member frontRepairRuntimeMember,
	request edgegroupfront.ActivationRepairCASRequest, prepare bool) ([]byte, error) {
	if err := r.requireSameMember(ctx, member); err != nil {
		return nil, err
	}
	a := r.activator
	// A short remote write deadline only protects the lock if the node's
	// clock agrees. Bracket the read; a slow response is not a usable proof.
	clockCtx, cancelClock := context.WithTimeout(ctx, 3*time.Second)
	start := time.Now()
	clockReply, clockErr := a.executor.Exec(clockCtx, a.config.Namespace, member.worker.Name, "edge", "date", "-u", "+%s")
	end := time.Now()
	cancelClock()
	epoch, parseErr := strconv.ParseInt(strings.TrimSpace(string(clockReply)), 10, 64)
	nodeTime := time.Unix(epoch, 0)
	if clockErr != nil || parseErr != nil || nodeTime.Before(end.Add(-frontRepairNodeClockBound)) ||
		nodeTime.Add(time.Second).After(start.Add(frontRepairNodeClockBound)) {
		return nil, errors.Join(errors.New("Front repair cannot establish the pinned node clock bound"), clockErr, parseErr)
	}
	if err := r.requireSameMember(ctx, member); err != nil {
		return nil, err
	}
	if now := store.locks.now().UTC(); member.ObservedAt.IsZero() || member.ObservedAt.After(now) || now.Sub(member.ObservedAt) >= 20*time.Second {
		return nil, errors.New("Front repair node observation expired before exec")
	}
	var tool frontRepairTool
	var staged frontRepairToolExecutor
	if r.nodeTools != nil {
		var err error
		tool, err = r.stagedToolForMember(ctx, member)
		if err != nil {
			return nil, err
		}
		var ok bool
		staged, ok = a.executor.(frontRepairToolExecutor)
		if !ok {
			return nil, errors.New("Front repair does not have the approved temporary tool transport")
		}
	}
	writeCtx, cancel, err := store.locks.mutationContext(ctx, a.config.GroupID)
	if err != nil {
		return nil, err
	}
	deadline, ok := writeCtx.Deadline()
	if !ok || writeCtx.Err() != nil {
		cancel()
		return nil, errors.New("Front repair node command has no live write deadline")
	}
	flag := "--repair-request"
	if prepare {
		flag = "--repair-prepare-request"
	}
	raw, err := json.Marshal(edgegroupfront.ActivationRepairWrite{
		Schema: edgegroupfront.ActivationRepairWriteSchemaV1, Request: request, ExpiresAt: deadline.UTC(),
	})
	if err != nil {
		cancel()
		return nil, err
	}
	envelope := base64.RawURLEncoding.EncodeToString(raw)
	var receipt []byte
	if staged != nil {
		receipt, err = staged.execFrontRepairTool(writeCtx, a.config.Namespace, member.worker.Name, tool, flag, envelope)
	} else {
		// Used by the isolated executor rehearsal. The protected path
		// requires staged tools even if the retained image contains a CAS.
		receipt, err = a.executor.Exec(writeCtx, a.config.Namespace, member.worker.Name, "edge",
			frontActivationCASBinary, "--state-file", frontActivationStatePath, flag, envelope)
	}
	cancel()
	if err != nil {
		// Kubernetes exec cancellation does not prove that the process exited.
		// Do not allow the caller to release the leases before a detached
		// process would reject its exact short write deadline. If the whole
		// executor is killed, the unrenewed group Lease remains in place.
		if remaining := time.Until(deadline.Add(frontRepairNodeClockBound)); remaining > 0 {
			timer := time.NewTimer(remaining)
			<-timer.C
		}
	}
	return receipt, err
}

func (r frontRepairRuntime) requireRepairApplied(ctx context.Context, member frontRepairRuntimeMember,
	request edgegroupfront.ActivationRepairCASRequest, created time.Time) error {
	observe, cancel := context.WithTimeout(ctx, frontRepairFrontFollowBudget)
	defer cancel()
	a := r.activator
	for {
		if err := r.requireSameMember(observe, member); err != nil {
			return err
		}
		disk, err := a.readActivation(observe, member.worker.Name)
		if err != nil || !request.MatchesActivation(disk) || disk.UpdatedAt.Before(created) {
			return errors.Join(errors.New("Front repair disk activation did not advance exactly"), err)
		}
		var live observedFront
		probeErr := r.read(observe, repairHealthEndpoint(member.front.Status.PodIP, frontHealthPort, "/readyz"), &live)
		if probeErr == nil && frontRepairLiveMatches(live, disk) {
			return r.requireSameMember(observe, member)
		}
		if probeErr == nil && !frontRepairLiveMatches(live, request.Expected) {
			return errors.New("live Front reported a different activation during repair")
		}
		if err := waitFrontRepairObservation(observe); err != nil {
			return errors.Join(errors.New("live Front did not follow the exact repaired activation"), probeErr, err)
		}
	}
}

func waitFrontRepairObservation(ctx context.Context) error {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
