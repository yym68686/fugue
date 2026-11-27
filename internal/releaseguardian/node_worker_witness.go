package releaseguardian

import (
	"context"
	"errors"
	"net/netip"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/productioncontrol"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The release job owns the TLS sockets. Guardian cross-checks these references
// against the live Front and can use them after its own process restarts.
type NodeWorkerConnection struct {
	FrontUID  string    `json:"front_uid"`
	ID        string    `json:"id"`
	StartedAt time.Time `json:"started_at"`
	Slot      string    `json:"slot"`
	Target    string    `json:"target"`
}

func (c NodeWorkerConnection) Valid(j edgegroupfront.NodeWorkerJournal, now time.Time) bool {
	t, err := netip.ParseAddrPort(c.Target)
	return err == nil && t.Port() != 0 && t.Addr().Zone() == "" && c.FrontUID == j.Recipient.PodUID &&
		len(c.ID) > 0 && len(c.ID) <= 128 && !c.StartedAt.IsZero() && !c.StartedAt.After(now) && (c.Slot == "a" || c.Slot == "b")
}

func (c NodeWorkerConnection) Same(other NodeWorkerConnection) bool {
	return c.FrontUID == other.FrontUID && c.ID == other.ID && c.StartedAt.Equal(other.StartedAt) && c.Slot == other.Slot && c.Target == other.Target
}

type NodeWorkerArm struct {
	Schema        string               `json:"schema"`
	JournalDigest string               `json:"journal_digest"`
	Held          NodeWorkerConnection `json:"held"`
	OpenedAt      time.Time            `json:"opened_at"`
	Deadline      time.Time            `json:"deadline"`
}

type NodeWorkerWitness struct {
	Schema        string                `json:"schema"`
	JournalDigest string                `json:"journal_digest"`
	Held          NodeWorkerConnection  `json:"held"`
	Fresh         NodeWorkerConnection  `json:"fresh"`
	ProbeFresh    *NodeWorkerConnection `json:"probe_fresh,omitempty"`
	ObservedAt    time.Time             `json:"observed_at"`
}

const (
	NodeWorkerArmSchema     = "edge-node-worker-arm/v1"
	NodeWorkerWitnessSchema = "edge-node-worker-connection-witness/v1"
)

func (arm NodeWorkerArm) Validate(j edgegroupfront.NodeWorkerJournal, now time.Time) error {
	window := arm.Deadline.Sub(arm.OpenedAt)
	if arm.Schema != NodeWorkerArmSchema || arm.JournalDigest != j.Digest ||
		arm.OpenedAt.Before(j.CreatedAt) || arm.OpenedAt.After(now) || window < 20*time.Second || window > 5*time.Minute ||
		!arm.Held.Valid(j, arm.OpenedAt) || arm.Held.Slot != j.Expected.ActiveSlot {
		return errors.New("node Worker arm lacks a bounded held connection for this journal")
	}
	return nil
}

func (w NodeWorkerWitness) Validate(j edgegroupfront.NodeWorkerJournal, arm NodeWorkerArm, on edgegroupfront.ActivationState, now time.Time) error {
	if w.Schema != NodeWorkerWitnessSchema || w.JournalDigest != j.Digest ||
		!w.Held.Same(arm.Held) || !w.Fresh.Valid(j, w.ObservedAt) ||
		w.Fresh.ID == w.Held.ID || w.Fresh.Slot != j.Candidate.Slot || w.Fresh.Target == w.Held.Target ||
		w.Fresh.StartedAt.Before(on.UpdatedAt) || w.ObservedAt.Before(on.UpdatedAt) || w.ObservedAt.After(now) ||
		!w.ObservedAt.Before(arm.Deadline) || j.Progress(on) != edgegroupfront.NodeWorkerActivated {
		return errors.New("node Worker witness does not prove both a retained and a new connection")
	}
	if !w.matchesProbe(j, on) {
		return errors.New("node Worker witness does not prove a distinct fresh connection from the pinned probe")
	}
	return nil
}

func (w NodeWorkerWitness) matchesProbe(j edgegroupfront.NodeWorkerJournal, on edgegroupfront.ActivationState) bool {
	p := w.ProbeFresh
	if j.ProbeServiceUID == "" {
		return p == nil
	}
	return p != nil && p.Valid(j, w.ObservedAt) && !p.StartedAt.Before(on.UpdatedAt) &&
		p.ID != w.Held.ID && p.ID != w.Fresh.ID && p.Slot == w.Fresh.Slot && p.Target == w.Fresh.Target
}

func NodeWorkerArmName(digest string) string     { return nodeWorkerInputName("armed", digest) }
func NodeWorkerWitnessName(digest string) string { return nodeWorkerInputName("connected", digest) }
func NodeWorkerResultName(digest string) string  { return nodeWorkerInputName("result", digest) }

func (store *AuthorityStore) RecordNativeNodeWorkerArm(ctx context.Context, fence NodeWorkerFence, arm NodeWorkerArm, now time.Time) error {
	j := fence.Journal
	if !validNativeWorkerClaim(store.namespace, j.Recipient.Group, fence.Claim) || !now.Before(arm.Deadline) {
		return errors.New("recording an arm requires a valid native claim and open observation window")
	}
	if err := arm.Validate(j, now); err != nil {
		return err
	}
	if err := store.requireNodeWorkerFence(ctx, fence); err != nil {
		return err
	}
	return store.createImmutable(ctx, NodeWorkerArmName(j.Digest), j.Recipient.Group, "node-worker-arm", "arm.json", arm)
}

func (store *AuthorityStore) RecordNativeNodeWorkerWitness(ctx context.Context, fence NodeWorkerFence, arm NodeWorkerArm,
	witness NodeWorkerWitness, state edgegroupfront.ActivationState, now time.Time) error {
	j := fence.Journal
	if !validNativeWorkerClaim(store.namespace, j.Recipient.Group, fence.Claim) {
		return errors.New("recording a live witness requires a valid native claim")
	}
	if err := store.requireNodeWorkerFence(ctx, fence); err != nil {
		return err
	}
	existing, found, err := store.ReadNodeWorkerArm(ctx, j, now)
	if err != nil || !found || !existing.Held.Same(arm.Held) ||
		!existing.OpenedAt.Equal(arm.OpenedAt) || !existing.Deadline.Equal(arm.Deadline) {
		return errors.Join(errors.New("live witness has no matching native arm"), err)
	}
	if err := witness.Validate(j, existing, state, now); err != nil {
		return err
	}
	return store.createImmutable(ctx, NodeWorkerWitnessName(j.Digest), j.Recipient.Group, "node-worker-witness", "witness.json", witness)
}

func nodeWorkerInputName(phase, digest string) string {
	if !digestPattern.MatchString(digest) {
		return ""
	}
	return "fugue-node-worker-" + phase + "-" + digest[7:39]
}

func (store *AuthorityStore) ReadNodeWorkerArm(ctx context.Context, j edgegroupfront.NodeWorkerJournal, now time.Time) (NodeWorkerArm, bool, error) {
	var a NodeWorkerArm
	found, err := store.readNodeWorkerInput(ctx, j, NodeWorkerArmName(j.Digest), "node-worker-arm", "arm.json", &a)
	if err == nil && found {
		err = a.Validate(j, now)
	}
	return a, found && err == nil, err
}

func (store *AuthorityStore) ReadNodeWorkerWitness(ctx context.Context, j edgegroupfront.NodeWorkerJournal) (NodeWorkerWitness, bool, error) {
	var w NodeWorkerWitness
	found, err := store.readNodeWorkerInput(ctx, j, NodeWorkerWitnessName(j.Digest), "node-worker-witness", "witness.json", &w)
	return w, found, err
}

func (store *AuthorityStore) readNodeWorkerInput(ctx context.Context, j edgegroupfront.NodeWorkerJournal, name, kind, key string, output any) (bool, error) {
	if name == "" || !groupPattern.MatchString(j.Recipient.Group) {
		return false, errors.New("node Worker release input lookup is invalid")
	}
	object, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if object.UID == "" || object.ResourceVersion == "" || object.DeletionTimestamp != nil || object.Immutable == nil || !*object.Immutable ||
		len(object.Data) != 1 || len(object.BinaryData) != 0 || object.Labels["fugue.pro/group"] != j.Recipient.Group ||
		object.Labels["fugue.pro/authority-kind"] != kind || len(object.Data[key]) > 16<<10 ||
		decodeStrict([]byte(object.Data[key]), output) != nil {
		return false, errors.New("invalid immutable node Worker release input")
	}
	return true, nil
}

type NodeWorkerResult struct {
	Schema        string                         `json:"schema"`
	JournalDigest string                         `json:"journal_digest"`
	Outcome       string                         `json:"outcome"`
	Activation    edgegroupfront.ActivationState `json:"activation"`
	Witness       *NodeWorkerWitness             `json:"witness,omitempty"`
	DrainStart    *time.Time                     `json:"drain_start,omitempty"`
	NativeClaim   *productioncontrol.Claim       `json:"native_claim,omitempty"`
}

const nodeWorkerResultSchema = "edge-node-worker-result/v1"

func (store *AuthorityStore) RecordNodeWorkerResult(ctx context.Context, fence NodeWorkerFence, state edgegroupfront.ActivationState, outcome string, w *NodeWorkerWitness) error {
	return store.recordNodeWorkerResult(ctx, fence, state, outcome, w, nil)
}

// RecordNodeWorkerDrainStart is only called once the Guardian has actually
// verified live cutover on both ingress listeners. Storing the start and
// witness in the same immutable result closes the restart/lost-reply gap.
func (store *AuthorityStore) RecordNodeWorkerDrainStart(ctx context.Context, fence NodeWorkerFence, state edgegroupfront.ActivationState, w NodeWorkerWitness, observedAt time.Time) error {
	start := observedAt.UTC()
	return store.recordNodeWorkerResult(ctx, fence, state, "verified", &w, &start)
}

func (store *AuthorityStore) recordNodeWorkerResult(ctx context.Context, fence NodeWorkerFence, state edgegroupfront.ActivationState, outcome string, w *NodeWorkerWitness, start *time.Time) error {
	j := fence.Journal
	result := NodeWorkerResult{
		Schema: nodeWorkerResultSchema, JournalDigest: j.Digest, Outcome: outcome, Activation: state, Witness: w, DrainStart: start,
	}
	if fence.Claim.Fence > 0 {
		result.NativeClaim = &fence.Claim
	}
	if !result.matches(j) {
		return errors.New("node Worker result does not follow from its durable state")
	}
	current, found, err := store.LoadNodeWorker(ctx, j.Recipient.Group)
	if err != nil || !found || current != fence {
		return errors.Join(errors.New("node Worker fence changed before recording the result"), err)
	}
	return store.createImmutable(ctx, NodeWorkerResultName(j.Digest), j.Recipient.Group, "node-worker-result", "result.json", result)
}

func (r NodeWorkerResult) matches(j edgegroupfront.NodeWorkerJournal) bool {
	if r.Schema != nodeWorkerResultSchema || r.JournalDigest != j.Digest {
		return false
	}
	progress := j.Progress(r.Activation)
	switch r.Outcome {
	case "verified":
		return progress == edgegroupfront.NodeWorkerActivated && r.Witness != nil &&
			r.Witness.Schema == NodeWorkerWitnessSchema && r.Witness.JournalDigest == j.Digest &&
			r.Witness.matchesProbe(j, r.Activation) &&
			(r.DrainStart == nil || j.ProbeServiceUID != "" && !r.DrainStart.Before(r.Witness.ObservedAt) &&
				!r.DrainStart.Before(r.Activation.UpdatedAt))
	case "reverted":
		return progress == edgegroupfront.NodeWorkerReverted && r.Witness == nil && r.DrainStart == nil
	case "expired_without_cutover":
		return progress == edgegroupfront.NodeWorkerPending && r.Witness == nil && r.DrainStart == nil
	}
	return false
}

// MatchesJournal validates an immutable terminal receipt against its sealed
// journal. Readers still need to prove the ownership and identity of both.
func (r NodeWorkerResult) MatchesJournal(j edgegroupfront.NodeWorkerJournal) bool {
	return r.matches(j)
}

// MatchesExecution validates the exact provider claim. Legacy results have
// no native execution identity and cannot settle a native journal entry.
func (r NodeWorkerResult) MatchesExecution(claim productioncontrol.Claim) bool {
	if claim.Fence == 0 {
		return r.NativeClaim == nil
	}
	return r.NativeClaim != nil && *r.NativeClaim == claim
}

func (store *AuthorityStore) ReadNodeWorkerResult(ctx context.Context, j edgegroupfront.NodeWorkerJournal) (NodeWorkerResult, bool, error) {
	var r NodeWorkerResult
	found, err := store.readNodeWorkerInput(ctx, j, NodeWorkerResultName(j.Digest), "node-worker-result", "result.json", &r)
	if found && (!r.matches(j) || (r.NativeClaim != nil && !validNativeWorkerClaim(store.namespace, j.Recipient.Group, *r.NativeClaim))) {
		return r, false, errors.New("node Worker result binds another transaction")
	}
	return r, found, err
}
