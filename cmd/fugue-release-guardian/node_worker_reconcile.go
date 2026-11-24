package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var errNodeWorkerWriteUnknown = errors.New("node Worker native write result is unknown; retain the lease until expiry")

// Reconcile only a journal that the release lane has already authorized. A
// Kubernetes event alone cannot create a journal or arm a public cutover.
func (a *frontAuthorityActivator) reconcileNodeWorker(ctx context.Context, store *releaseguardian.AuthorityStore) (handled bool, change string, resultErr error) {
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	group := a.config.GroupID
	fence, found, err := store.LoadNodeWorker(ctx, group)
	if err != nil || !found {
		return err != nil, "", err
	}
	j := fence.Journal
	_, terminal, err := store.ReadNodeWorkerResult(ctx, j)
	if err != nil {
		return true, "", err
	}
	if !terminal {
		_, armed, err := store.ReadNodeWorkerArm(ctx, j, a.now())
		if err != nil || !armed && a.now().Before(j.CreatedAt.Add(5*time.Minute)) {
			return true, "", err
		}
	}

	lease, err := a.acquireLease(ctx)
	if err != nil {
		return true, "", err
	}
	defer func() {
		if !errors.Is(resultErr, errNodeWorkerWriteUnknown) {
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer stop()
			resultErr = errors.Join(resultErr, lease.release(cleanup))
		}
	}()
	current, found, err := store.LoadNodeWorker(ctx, group)
	if err != nil || !found || current != fence {
		return true, "", errors.Join(errors.New("node Worker transaction was replaced before reconciliation"), err)
	}
	if err := store.RequireNoActivatedTransition(ctx, group); err != nil {
		return true, "", err
	}
	if _, exists, err := store.ReadNodeWorkerResult(ctx, j); err != nil {
		return true, "", err
	} else if exists {
		if err := store.FinalizeNodeWorker(ctx, fence); err != nil {
			return true, "", err
		}
		return true, "released_terminal_fence", nil
	}
	arm, armed, err := store.ReadNodeWorkerArm(ctx, j, a.now())
	if err != nil {
		return true, "", err
	}
	// To recover a failed cutover, observe the original Worker even when
	// the candidate failed its readiness or has disappeared.
	runtime, err := a.observeNodeWorkerRuntime(ctx, j, false, false)
	if err != nil {
		return true, "", err
	}
	state, err := a.readNodeWorkerSelection(ctx, j, runtime)
	if err != nil {
		return true, "", err
	}
	now := a.now().UTC()
	switch j.Progress(state) {
	case edgegroupfront.NodeWorkerPending:
		if !armed || !now.Before(arm.Deadline) {
			return true, "expired_without_cutover", store.RecordNodeWorkerResult(ctx, fence, state, "expired_without_cutover", nil)
		}
		if err := store.EnsureNodeWorkerProjection(ctx, fence); err != nil {
			return true, "", err
		}
		return true, "", a.prepareOrActivateNodeWorker(ctx, store, lease, fence, arm, state)
	case edgegroupfront.NodeWorkerActivated:
		if !armed {
			return true, "", errors.New("node Worker activation has no surviving authorization; preserve the mutation fence")
		}
		witness, available, witnessErr := store.ReadNodeWorkerWitness(ctx, j)
		if witnessErr == nil && available {
			witnessErr = a.verifyNodeWorkerWitness(ctx, store, j, arm, witness, state)
			if witnessErr == nil {
				return true, "verified", store.RecordNodeWorkerResult(ctx, fence, state, "verified", &witness)
			}
		}
		candidateStillRunning := runtime.next.pod.UID != "" &&
			edgegroupfront.NodeWorkerProcessDigest(runtime.next.edgeID, runtime.next.caddyID) == j.CandidateProcess
		if now.Before(arm.Deadline) && candidateStillRunning {
			return true, "", witnessErr
		}
		if err := a.verifyLocalWorkerServing(ctx, j, runtime.prior, j.Expected.BundleGeneration); err != nil {
			return true, "", errors.Join(errors.New("node Worker verification failed but the retained listener cannot safely resume"), err)
		}
		if err := a.requireSameNodeWorkerRuntime(ctx, j, state, runtime, false); err != nil {
			return true, "", err
		}
		if err := store.EnsureNodeWorkerProjection(ctx, fence); err != nil {
			return true, "", err
		}
		return true, "rollback_requested", a.nativeNodeWorkerWrite(ctx, lease, fence, runtime, "rollback", time.Time{}, nil)
	case edgegroupfront.NodeWorkerReverted:
		runtime, err := a.observeNodeWorkerRuntime(ctx, j, false, true)
		if err != nil {
			return true, "", err
		}
		if err := a.verifyLocalWorkerServing(ctx, j, runtime.prior, j.Expected.BundleGeneration); err != nil {
			return true, "", err
		}
		return true, "reverted", store.RecordNodeWorkerResult(ctx, fence, state, "reverted", nil)
	default:
		return true, "", edgegroupfront.ErrActivationCASConflict
	}
}

func (a *frontAuthorityActivator) prepareOrActivateNodeWorker(ctx context.Context, store *releaseguardian.AuthorityStore, lease *heldAuthorityLease,
	fence releaseguardian.NodeWorkerFence, arm releaseguardian.NodeWorkerArm, state edgegroupfront.ActivationState) error {
	j := fence.Journal
	runtime, err := a.observeNodeWorkerRuntime(ctx, j, true, true)
	if err != nil {
		return err
	}
	if arm.Held.Target != runtime.prior.target || !a.nodeWorkerConnectionsLive(ctx, runtime, arm.Held) {
		return errors.New("release job's original TLS connection is no longer present on the retained listener")
	}
	if err := store.VerifyNodeWorkerArtifact(ctx, j, runtime.next.pod); err != nil {
		return err
	}
	if err := a.requireSameNodeWorkerRuntime(ctx, j, state, runtime, true); err != nil {
		return err
	}
	var prep edgegroupfront.PublicationPreparation
	if err := a.nativeNodeWorkerWrite(ctx, lease, fence, runtime, "prepare", arm.Deadline, &prep); err != nil {
		return err
	}
	if !prep.Matches(state, j.Candidate.Slot, j.Candidate.WorkerSourceCommit, a.now().UTC()) ||
		prep.WorkerImageDigest != j.Candidate.WorkerImageDigest || prep.BundleGeneration != j.Candidate.BundleGeneration {
		return errors.New("node Worker preparation receipt did not match the exact signed publication and candidate")
	}
	if err := a.verifyLocalWorkerServing(ctx, j, runtime.prior, j.Expected.BundleGeneration); err != nil {
		return fmt.Errorf("retained Worker cannot serve before cutover: %w", err)
	}
	if err := a.verifyLocalWorkerServing(ctx, j, runtime.next, j.Candidate.BundleGeneration); err != nil {
		return fmt.Errorf("candidate Worker has not proved its signed publication and TLS canary: %w", err)
	}
	if !a.nodeWorkerConnectionsLive(ctx, runtime, arm.Held) {
		return errors.New("original TLS connection disappeared before node Worker cutover")
	}
	if err := a.requireSameNodeWorkerRuntime(ctx, j, state, runtime, true); err != nil {
		return err
	}
	return a.nativeNodeWorkerWrite(ctx, lease, fence, runtime, "commit", arm.Deadline, nil)
}

func (a *frontAuthorityActivator) checkNodeWorkerProjection(ctx context.Context, j edgegroupfront.NodeWorkerJournal, runtime nodeWorkerRuntime) error {
	wire, err := json.Marshal(j)
	if err != nil {
		return err
	}
	digest, err := edgegroupfront.ProjectedJournalDigest(wire)
	if err != nil {
		return err
	}
	raw, execErr := a.executor.Exec(ctx, a.config.Namespace, runtime.front.Name, "edge-front", frontActivationCASBinary, "--worker-journal")
	var projected edgegroupfront.NativeProjection
	if execErr != nil || decodeStrictJSON(raw, &projected) != nil || projected.Recipient != j.Recipient || projected.Digest != digest {
		return errors.Join(errors.New("exact Front process has not mounted the current node Worker journal"), execErr)
	}
	return nil
}

func (a *frontAuthorityActivator) readNodeWorkerSelection(ctx context.Context, j edgegroupfront.NodeWorkerJournal, runtime nodeWorkerRuntime) (edgegroupfront.ActivationState, error) {
	s, err := a.readActivation(ctx, runtime.prior.pod.Name)
	if err != nil {
		return s, err
	}
	var live nodeWorkerFrontHealth
	if err := readAuthorityRuntimeJSON(ctx, repairHealthEndpoint(runtime.front.Status.PodIP, frontHealthPort, "/healthz"), &live); err != nil ||
		!frontRepairLiveMatches(live.observedFront, s) {
		return s, errors.Join(errors.New("public Front does not match the durable node activation"), err)
	}
	target := runtime.prior.target
	if j.Progress(s) == edgegroupfront.NodeWorkerActivated {
		// Derive from the exact Front host even if its selected Worker
		// replica is gone. Never dial an address from the health response.
		target = net.JoinHostPort(runtime.front.Status.HostIP, "38443")
	}
	if live.Target != target {
		return s, errors.New("public Front did not select the journal's exact physical listener")
	}
	return s, nil
}

func (a *frontAuthorityActivator) requireSameNodeWorkerRuntime(ctx context.Context, j edgegroupfront.NodeWorkerJournal, state edgegroupfront.ActivationState,
	runtime nodeWorkerRuntime, needCandidate bool) error {
	fresh, err := a.observeNodeWorkerRuntime(ctx, j, needCandidate, needCandidate)
	if err != nil || !runtime.sameBinding(fresh, needCandidate) {
		return errors.Join(errors.New("node Worker physical binding changed during observation"), err)
	}
	check, err := a.readNodeWorkerSelection(ctx, j, fresh)
	if err != nil || check != state {
		return errors.Join(errors.New("node Worker serving pointer drifted before the operation"), err)
	}
	return nil
}

func (a *frontAuthorityActivator) verifyLocalWorkerServing(ctx context.Context, j edgegroupfront.NodeWorkerJournal, worker nodeWorkerProcess, bundle string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var h authorityWorkerHealth
	if err := readAuthorityRuntimeJSON(probeCtx, repairHealthEndpoint(worker.pod.Status.PodIP, workerHealthPort, "/healthz"), &h); err != nil ||
		!preparedWorkerPublicationReady(h, releaseguardian.FrontAuthorityTarget{GroupID: j.Recipient.Group, FrontBundleGeneration: bundle}) ||
		h.CaddyLastError != "" || h.StaleCache == nil || *h.StaleCache || h.MaxStaleExceeded || !a.now().Add(10*time.Second).Before(h.BundleValidUntil) {
		return errors.Join(errors.New("Caddy did not apply a fresh signed route publication"), err)
	}
	code, payload, _, err := requestCandidateRoute(probeCtx, worker.target, a.config.RouteHost, a.config.RoutePath)
	if err != nil || code != http.StatusOK || shaDigest(payload) != a.config.RouteBodyDigest {
		return errors.Join(errors.New("exact Worker listener did not pass the established TLS route canary"), err)
	}
	return nil
}

func (a *frontAuthorityActivator) verifyNodeWorkerWitness(ctx context.Context, store *releaseguardian.AuthorityStore, j edgegroupfront.NodeWorkerJournal,
	arm releaseguardian.NodeWorkerArm, witness releaseguardian.NodeWorkerWitness, state edgegroupfront.ActivationState) error {
	if err := witness.Validate(j, arm, state, a.now().UTC()); err != nil {
		return err
	}
	r, err := a.observeNodeWorkerRuntime(ctx, j, true, true)
	if err != nil {
		return err
	}
	if witness.Held.Target != r.prior.target || witness.Fresh.Target != r.next.target ||
		!a.nodeWorkerConnectionsLive(ctx, r, witness.Held, witness.Fresh) {
		return errors.New("both TLS connections were not observed on their expected physical Workers")
	}
	if err := store.VerifyNodeWorkerArtifact(ctx, j, r.next.pod); err != nil {
		return err
	}
	if err := a.verifyLocalWorkerServing(ctx, j, r.next, j.Candidate.BundleGeneration); err != nil {
		return err
	}
	return a.requireSameNodeWorkerRuntime(ctx, j, state, r, true)
}

func (a *frontAuthorityActivator) nativeNodeWorkerWrite(ctx context.Context, lease *heldAuthorityLease, fence releaseguardian.NodeWorkerFence,
	runtime nodeWorkerRuntime, op string, until time.Time, receipt any) error {
	j := fence.Journal
	if err := a.checkNodeWorkerProjection(ctx, j, runtime); err != nil {
		return err
	}
	object, err := lease.client.CoordinationV1().Leases(lease.namespace).Get(ctx, lease.name, metav1.GetOptions{})
	if err != nil || string(object.UID) != lease.uid || object.ResourceVersion != lease.rv || valueOrEmpty(object.Spec.HolderIdentity) != lease.holder ||
		object.Spec.RenewTime == nil || object.Spec.LeaseDurationSeconds == nil {
		return errors.Join(errors.New("node Worker lost its exact group mutation Lease"), err)
	}
	now := a.now().UTC()
	deadline := now.Add(10 * time.Second)
	fencedAt := object.Spec.RenewTime.Time.Add(time.Duration(*object.Spec.LeaseDurationSeconds)*time.Second - 2*time.Second)
	if fencedAt.Before(deadline) {
		deadline = fencedAt
	}
	if !until.IsZero() && until.Before(deadline) {
		deadline = until
	}
	if !now.Add(time.Second).Before(deadline) {
		return errors.New("node Worker has no guarded write window left")
	}
	args, err := json.Marshal(edgegroupfront.NodeWorkerWrite{
		Schema: edgegroupfront.NodeWorkerWriteSchema, Recipient: j.Recipient, JournalDigest: j.Digest, Operation: op, ExpiresAt: deadline,
	})
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	raw, err := a.executor.Exec(writeCtx, a.config.Namespace, runtime.front.Name, "edge-front", frontActivationCASBinary,
		"--node-worker-advance", base64.RawURLEncoding.EncodeToString(args))
	if err != nil {
		return errors.Join(errNodeWorkerWriteUnknown, err)
	}
	if receipt != nil {
		return decodeStrictJSON(raw, receipt)
	}
	var activated edgegroupfront.ActivationReceipt
	want := edgegroupfront.NodeWorkerActivated
	if op == "rollback" {
		want = edgegroupfront.NodeWorkerReverted
	}
	if err := decodeStrictJSON(raw, &activated); err != nil || activated.Schema != edgegroupfront.ActivationReceiptSchemaV1 ||
		activated.GroupID != j.Recipient.Group || j.Progress(activated.Current) != want {
		return errors.Join(errNodeWorkerWriteUnknown, err)
	}
	return nil
}

func (a *frontAuthorityActivator) nodeWorkerConnectionsLive(ctx context.Context, runtime nodeWorkerRuntime, refs ...releaseguardian.NodeWorkerConnection) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	path := repairHealthEndpoint(runtime.front.Status.PodIP, frontHealthPort, "/edge/tcp-connections")
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, path, nil)
	if err != nil {
		return false
	}
	reader := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := reader.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	const limit = 4 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil || len(body) > limit || resp.StatusCode != http.StatusOK {
		return false
	}
	return nodeWorkerConnectionInventoryMatches(body, string(runtime.front.UID), refs)
}

func nodeWorkerConnectionInventoryMatches(raw []byte, uid string, refs []releaseguardian.NodeWorkerConnection) bool {
	var view struct {
		Count  int `json:"count"`
		Active []struct {
			ID        string    `json:"id"`
			Protocol  string    `json:"protocol"`
			StartedAt time.Time `json:"started_at"`
			Slot      string    `json:"slot"`
			Target    string    `json:"target"`
		} `json:"active"`
	}
	if len(refs) == 0 || len(refs) > 2 || json.Unmarshal(raw, &view) != nil || view.Count != len(view.Active) || view.Count > 16_384 {
		return false
	}
	for _, ref := range refs {
		matches := 0
		for _, c := range view.Active {
			fact := releaseguardian.NodeWorkerConnection{FrontUID: uid, ID: c.ID, StartedAt: c.StartedAt, Slot: c.Slot, Target: c.Target}
			if c.Protocol == "https" && fact.Same(ref) {
				matches++
			}
		}
		if matches != 1 {
			return false
		}
	}
	return true
}
