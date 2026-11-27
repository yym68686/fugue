package edgegroupfront

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/netip"
	"regexp"
	"syscall"
	"time"
)

const (
	NodeWorkerJournalSchema = "edge-node-worker-advance/v1"
	NodeWorkerWriteSchema   = "edge-node-worker-write/v1"
)

var (
	nodeWorkerUID       = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	nodeWorkerComponent = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
)

// NodeWorkerJournal binds a single physical listener handoff to the observed
// local predecessor. It cannot change the group's publication or select a
// different node, and must be projected from the Guardian's group fence.
type NodeWorkerJournal struct {
	Schema               string                  `json:"schema"`
	Recipient            NativeRecipient         `json:"recipient"`
	NodeUID              string                  `json:"node_uid"`
	ServiceUID           string                  `json:"service_uid"`
	ProbeServiceUID      string                  `json:"probe_service_uid,omitempty"`
	FixedTransport       bool                    `json:"fixed_transport,omitempty"`
	FixedRotation        bool                    `json:"fixed_rotation,omitempty"`
	PredecessorWorkerUID string                  `json:"predecessor_worker_uid"`
	CandidateWorkerUID   string                  `json:"candidate_worker_uid"`
	PredecessorProcess   string                  `json:"predecessor_process"`
	CandidateProcess     string                  `json:"candidate_process"`
	CandidateComponent   string                  `json:"candidate_component"`
	Expected             ActivationState         `json:"expected"`
	Candidate            ActivationRepairRelease `json:"candidate"`
	ReleaseDigest        string                  `json:"release_digest"`
	CreatedAt            time.Time               `json:"created_at"`
	Digest               string                  `json:"digest"`
}

type NodeWorkerWrite struct {
	Schema        string          `json:"schema"`
	Recipient     NativeRecipient `json:"recipient"`
	JournalDigest string          `json:"journal_digest"`
	Operation     string          `json:"operation"`
	ExpiresAt     time.Time       `json:"expires_at"`
}

func (j NodeWorkerJournal) sealedDigest() (string, error) {
	if j.Schema != NodeWorkerJournalSchema || validateActivationState(j.Expected) != nil || j.FixedRotation && !j.FixedTransport ||
		j.Recipient.Group != j.Expected.GroupID || j.Recipient.Node == "" ||
		!activationDigestPattern.MatchString(j.Recipient.Incarnation) || !activationDigestPattern.MatchString(j.ReleaseDigest) ||
		!activationDigestPattern.MatchString(j.PredecessorProcess) || !activationDigestPattern.MatchString(j.CandidateProcess) ||
		j.PredecessorProcess == j.CandidateProcess || len(j.CandidateComponent) > 63 || !nodeWorkerComponent.MatchString(j.CandidateComponent) ||
		!j.Candidate.valid() || j.Expected.Generation >= math.MaxUint64-1 ||
		// The current Worker uses the inactive slot to fetch publication
		// preparation. Do not grant a same-slot handoff via this protocol.
		j.Candidate.Slot == j.Expected.ActiveSlot || j.Candidate.WorkerSourceCommit == j.Expected.WorkerSourceCommit ||
		j.Candidate.WorkerImageDigest == j.Expected.WorkerImageDigest ||
		j.CreatedAt.IsZero() || j.CreatedAt.Before(j.Expected.UpdatedAt) {
		return "", errors.New("node Worker journal has an invalid predecessor, candidate or recipient")
	}
	seen := map[string]bool{}
	ids := []string{j.Recipient.PodUID, j.NodeUID, j.ServiceUID, j.PredecessorWorkerUID, j.CandidateWorkerUID}
	if j.ProbeServiceUID != "" {
		ids = append(ids, j.ProbeServiceUID)
	} else if j.FixedTransport {
		return "", errors.New("fixed Worker handoff requires the independent probe ingress")
	}
	for _, id := range ids {
		if !nodeWorkerUID.MatchString(id) || seen[id] {
			return "", errors.New("node Worker journal lacks distinct physical identities")
		}
		seen[id] = true
	}
	j.Digest = ""
	raw, err := json.Marshal(j)
	if err != nil {
		return "", err
	}
	return nativeDigest(raw), nil
}

// CandidateHTTPSPort is derived from the physical transport contract; no
// release declaration or connection witness may supply an arbitrary port.
func (j NodeWorkerJournal) CandidateHTTPSPort() uint16 {
	if !j.FixedTransport {
		return 38443
	}
	if j.Candidate.Slot == "a" {
		return 18443
	}
	return 28443
}

// NodeWorkerProcessDigest pins the Edge and Caddy container incarnations,
// not just their Pod (which may still have the same UID after a restart).
func NodeWorkerProcessDigest(edgeContainerID, caddyContainerID string) string {
	return nativeDigest([]byte("fugue-node-worker-process/v1\n" + edgeContainerID + "\n" + caddyContainerID))
}

func (j NodeWorkerJournal) Seal() (NodeWorkerJournal, error) {
	digest, err := j.sealedDigest()
	if err != nil {
		return NodeWorkerJournal{}, err
	}
	j.Digest = digest
	return j, nil
}

func ReadNodeWorkerJournal(raw []byte) (NodeWorkerJournal, error) {
	var j NodeWorkerJournal
	if len(raw) == 0 || len(raw) > 16<<10 || json.Unmarshal(raw, &j) != nil {
		return j, errors.New("node Worker journal is unreadable or oversized")
	}
	wire, err := json.Marshal(j)
	if err != nil || !bytes.Equal(raw, wire) {
		return j, errors.New("node Worker journal is not canonical")
	}
	sealed, err := j.Seal()
	if err != nil || sealed.Digest != j.Digest {
		return j, errors.New("node Worker journal digest or scope is invalid")
	}
	return j, nil
}

type NodeWorkerProgress string

const (
	NodeWorkerPending   NodeWorkerProgress = "pending"
	NodeWorkerActivated NodeWorkerProgress = "activated"
	NodeWorkerReverted  NodeWorkerProgress = "reverted"
	NodeWorkerConflict  NodeWorkerProgress = "conflict"
)

// NodeWorkerSelectionPlan projects the two exact successor states, apart
// from their execution timestamps. The rollback state restores the old Worker
// at a fresh generation; it is not a replay of the predecessor pointer.
type NodeWorkerSelectionPlan struct {
	AdvanceDigest      string `json:"advance_digest"`
	AdvanceGeneration  uint64 `json:"advance_generation"`
	RollbackDigest     string `json:"rollback_digest"`
	RollbackGeneration uint64 `json:"rollback_generation"`
}

func (j NodeWorkerJournal) SelectionPlan() (NodeWorkerSelectionPlan, error) {
	if sealed, err := j.Seal(); err != nil || sealed.Digest != j.Digest {
		return NodeWorkerSelectionPlan{}, errors.New("node Worker selection plan requires a sealed journal")
	}
	advance, rollback := j.target(), j.rollbackTarget()
	digestState := func(state ActivationState) (string, error) {
		raw, err := json.Marshal(activationWithoutTime(state))
		if err != nil {
			return "", err
		}
		return nativeDigest(raw), nil
	}
	advanceDigest, err := digestState(advance)
	if err != nil {
		return NodeWorkerSelectionPlan{}, err
	}
	rollbackDigest, err := digestState(rollback)
	if err != nil {
		return NodeWorkerSelectionPlan{}, err
	}
	return NodeWorkerSelectionPlan{AdvanceDigest: advanceDigest, AdvanceGeneration: advance.Generation,
		RollbackDigest: rollbackDigest, RollbackGeneration: rollback.Generation}, nil
}

// Progress distinguishes a safe retry from a lost successful response and
// from a third-party change. "Activated" says nothing about route readiness
// or connection survival; the Guardian must re-observe both before closing.
func (j NodeWorkerJournal) Progress(state ActivationState) NodeWorkerProgress {
	if sealed, err := j.Seal(); err != nil || sealed.Digest != j.Digest || validateActivationState(state) != nil {
		return NodeWorkerConflict
	}
	if sameRepairActivation(j.Expected, state) {
		return NodeWorkerPending
	}
	if !state.UpdatedAt.Before(j.CreatedAt) {
		switch activationWithoutTime(state) {
		case j.target():
			return NodeWorkerActivated
		case j.rollbackTarget():
			return NodeWorkerReverted
		}
	}
	return NodeWorkerConflict
}

func (j NodeWorkerJournal) target() ActivationState {
	c := j.Candidate
	reason := "advance node Worker using journal "
	if j.FixedRotation {
		reason = fixedWorkerAdvanceReason
	}
	return ActivationState{
		Schema: ActivationStateSchemaV1, GroupID: j.Recipient.Group, Generation: j.Expected.Generation + 1,
		ActiveSlot: c.Slot, PreviousSlot: j.Expected.ActiveSlot, BundleGeneration: c.BundleGeneration,
		WorkerSourceCommit: c.WorkerSourceCommit, WorkerImageDigest: c.WorkerImageDigest,
		Authority: ActivationAuthority, Operation: ActivationOperationPromote, Reason: reason + j.Digest,
	}
}

func (j NodeWorkerJournal) rollbackTarget() ActivationState {
	s := activationWithoutTime(j.Expected)
	s.Generation += 2
	s.PreviousSlot = j.Candidate.Slot
	s.Operation = ActivationOperationRollback
	s.RollbackOfGeneration = s.Generation - 1
	reason := "restore node Worker using journal "
	if j.FixedRotation {
		reason = fixedWorkerRestoreReason
	}
	s.Reason = reason + j.Digest
	return s
}

// ExecuteNodeWorkerWrite uses only the fixed local Front configuration and
// the kubelet's read-only journal projection. A CLI digest cannot create
// authority. The release writer must separately hold the group Lease and
// prove the Service, processes and signed publication before issuing commit
// or restoring the retained Worker.
func ExecuteNodeWorkerWrite(cfg Config, local NativeRecipient, raw []byte, w NodeWorkerWrite) (any, error) {
	return executeNodeWorkerWrite(cfg, local, raw, w, time.Now)
}

func executeNodeWorkerWrite(cfg Config, local NativeRecipient, raw []byte, w NodeWorkerWrite, clock func() time.Time) (any, error) {
	j, err := ReadNodeWorkerJournal(raw)
	if err != nil {
		return nil, err
	}
	if w.Schema != NodeWorkerWriteSchema || w.Recipient != local || j.Recipient != local || j.Digest != w.JournalDigest ||
		cfg.EdgeID != local.Node || cfg.EdgeGroupID != local.Group || cfg.WorkerTransport == "" ||
		(w.Operation != "prepare" && w.Operation != "commit" && w.Operation != "rollback") {
		return nil, errors.New("node Worker write lacks its exact projected journal and recipient")
	}
	if err := cfg.prepareWorkerTransport(); err != nil {
		return nil, err
	}
	// v1 requires two static image pins. Fixed rotation still requires the
	// exact authorized journal and the two registered ports, but records the
	// image change under the activation lock instead of restarting Front.
	var transport workerTransportDeclaration
	if err := json.Unmarshal([]byte(cfg.WorkerTransport), &transport); err != nil {
		return nil, err
	}
	candidate := workerImagePin{j.Candidate.Slot, j.Candidate.WorkerSourceCommit, j.Candidate.WorkerImageDigest}
	previous := workerImagePin{j.Expected.ActiveSlot, j.Expected.WorkerSourceCommit, j.Expected.WorkerImageDigest}
	prior, priorOK := cfg.workerTargets[previous]
	targets, nextOK := cfg.workerTargets[candidate]
	endpoint, endpointErr := netip.ParseAddrPort(targets.HTTPSAddress)
	modeMatches := !j.FixedTransport && transport.Schema == "fugue.worker-transport/v1" && transport.Candidate == candidate ||
		j.FixedTransport && transport.Schema == "fugue.worker-transport-fixed/v1"
	if cfg.fixedWorkerRotation {
		prior, targets = cfg.Slots[previous.Slot], cfg.Slots[candidate.Slot]
		endpoint, endpointErr = netip.ParseAddrPort(targets.HTTPSAddress)
		priorOK, nextOK, modeMatches = true, true, j.FixedTransport
	}
	if j.FixedRotation != cfg.fixedWorkerRotation || !priorOK || !nextOK || prior == targets || !modeMatches ||
		endpointErr != nil || endpoint.Port() != j.CandidateHTTPSPort() {
		return nil, errors.New("node Worker journal is not bound to the Front's retained and candidate listeners")
	}
	if err := validateActivationPath(cfg.ActiveSlotFile); err != nil {
		return nil, err
	}
	started := clock().UTC()
	if started.Before(j.CreatedAt) || !started.Before(w.ExpiresAt) || w.ExpiresAt.Sub(started) > 15*time.Second {
		return nil, errors.New("node Worker write deadline is missing, expired or excessive")
	}
	path := cfg.ActiveSlotFile
	lock, err := acquireActivationLockMode(path, syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errors.Join(ErrActivationCASConflict, err)
		}
		return nil, err
	}
	defer lock.Close()
	current, found, err := ReadActivationState(path)
	expected := NodeWorkerPending
	if w.Operation == "rollback" {
		expected = NodeWorkerActivated
	}
	if err != nil || !found || j.Progress(current) != expected {
		return nil, errors.Join(ErrActivationCASConflict, err)
	}
	var priorBinding NodeWorkerJournal
	if cfg.fixedWorkerRotation {
		selected, record, ok := cfg.activationWorkerTargets(current)
		want := prior
		if w.Operation == "rollback" {
			want = targets
		}
		if !ok || selected != want || (w.Operation == "rollback" && record.Digest != j.Digest) {
			return nil, errors.Join(ErrActivationCASConflict, errors.New("current Worker activation has no exact fixed listener binding"))
		}
		priorBinding = record
	}
	now := clock().UTC()
	if now.Before(started) || now.Before(current.UpdatedAt) || !now.Before(w.ExpiresAt) {
		return nil, errors.New("node Worker write deadline expired after acquiring activation lock")
	}
	deadline := w.ExpiresAt
	guard := func() error {
		checked := clock().UTC()
		if checked.Before(now) || !checked.Before(deadline) {
			return errors.New("node Worker write deadline expired before atomic replace")
		}
		return nil
	}
	c := j.Candidate
	if w.Operation == "prepare" {
		if prior, found, err := ReadPublicationPreparation(path); err != nil {
			return nil, err
		} else if found && prior.CreatedAt.Before(j.CreatedAt) && prior.Matches(current, prior.TargetSlot, prior.WorkerSourceCommit, now) {
			return nil, errors.Join(ErrActivationCASConflict, errors.New("live publication preparation predates the node Worker journal"))
		}
		return writePublicationPreparation(path, current, c.Slot, c.BundleGeneration, c.WorkerSourceCommit, c.WorkerImageDigest, now, guard)
	}
	if w.Operation == "rollback" {
		next := j.rollbackTarget()
		next.UpdatedAt = now
		return commitActivationStateGuarded(path, current, true, next, guard)
	}
	p, exists, err := ReadPublicationPreparation(path)
	if err != nil || !exists || p.CreatedAt.Before(j.CreatedAt) || !p.Matches(current, c.Slot, c.WorkerSourceCommit, now) ||
		p.BundleGeneration != c.BundleGeneration || p.WorkerImageDigest != c.WorkerImageDigest ||
		!now.Add(5*time.Second).Before(p.ExpiresAt) {
		return nil, errors.Join(errors.New("node Worker activation lacks exact unexpired signed-publication preparation"), err)
	}
	if p.ExpiresAt.Before(deadline) {
		deadline = p.ExpiresAt
	}
	if cfg.fixedWorkerRotation {
		if err := cfg.stageFixedWorkerBinding(priorBinding, j, guard); err != nil {
			return nil, err
		}
	}
	next := j.target()
	next.UpdatedAt = now
	return commitActivationStateGuarded(path, current, true, next, guard)
}
