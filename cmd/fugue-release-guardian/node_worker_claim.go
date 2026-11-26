package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	"k8s.io/client-go/kubernetes"
)

// The release job uses stdin inside the running Guardian container. The
// command accepts no kubeconfig, authority endpoint, pod command or alternate
// state file from the input. Claiming alone cannot arm traffic.
func runNodeWorkerClaim(args []string, in io.Reader, out io.Writer) error {
	if len(args) != 0 || os.Getenv("FUGUE_RELEASE_GUARDIAN_ROLE") != "guardian" ||
		os.Getenv("FUGUE_RELEASE_GUARDIAN_MODE") != string(releaseguardian.ModeWrite) ||
		os.Getenv("FUGUE_RELEASE_GUARDIAN_POD_UID") == "" || in == nil || out == nil {
		return errors.New("node Worker claim requires the normal write Guardian and one canonical stdin journal")
	}
	raw, err := readClaimStdin(in)
	if err != nil {
		return err
	}
	j, err := edgegroupfront.ReadNodeWorkerJournal(raw)
	if err != nil {
		return err
	}
	targets, err := parseTargets(os.Getenv("FUGUE_RELEASE_GUARDIAN_TARGETS"))
	if err != nil || len(targets) == 0 {
		return errors.Join(errors.New("release Guardian has no configured namespace"), err)
	}
	ns := targets[0].Namespace
	for _, target := range targets {
		if ns != target.Namespace {
			return errors.New("node Worker claim cannot span namespaces")
		}
	}
	config, err := loadKubeConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	store, err := releaseguardian.NewAuthorityStore(client, ns)
	if err != nil {
		return err
	}
	runtime, err := newAuthorityRuntimeWithActivators(store, client, config, ns,
		os.Getenv("FUGUE_RELEASE_GUARDIAN_AUTHORITY_GROUPS"), os.Getenv("FUGUE_RELEASE_GUARDIAN_AUTHORITY_ACTIVATORS"))
	if err != nil {
		return err
	}
	if runtime == nil || runtime.fronts[j.Recipient.Group] == nil {
		return errors.New("node Worker claim is not for an enrolled authority group")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	component, err := store.FindNodeWorkerComponent(ctx, targets, j.Recipient.Group)
	if err != nil {
		return err
	}
	fence, err := runtime.fronts[j.Recipient.Group].claimNodeWorker(ctx, store, component, os.Getenv("FUGUE_RELEASE_GUARDIAN_POD_UID"), j)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(fence)
}

// Seal emits canonical wire data for the release job so it can retain the
// journal digest before requesting a claim. It does not read Kubernetes or
// grant authority. node-worker-claim still proves the exact live binding.
func runNodeWorkerSeal(args []string, in io.Reader, out io.Writer) error {
	if len(args) != 0 || in == nil || out == nil {
		return errors.New("node-worker-seal requires one unsigned stdin journal")
	}
	raw, err := readClaimStdin(in)
	if err != nil {
		return err
	}
	var j edgegroupfront.NodeWorkerJournal
	if err := decodeStrictJSON(raw, &j); err != nil || j.Digest != "" {
		return errors.Join(errors.New("node Worker input must be an unsigned journal"), err)
	}
	sealed, err := j.Seal()
	if err != nil {
		return err
	}
	// The claim command requires this exact object, without a trailing newline.
	canonical, err := json.Marshal(sealed)
	if err != nil {
		return err
	}
	_, err = out.Write(canonical)
	return err
}

func readClaimStdin(in io.Reader) ([]byte, error) {
	const limit = 16 << 10
	raw, err := io.ReadAll(io.LimitReader(in, limit+1))
	if err != nil || len(raw) > limit {
		return nil, errors.Join(errors.New("node Worker claim input is unreadable or oversized"), err)
	}
	return raw, nil
}

func waitNodeWorkerComponent(ctx context.Context, recent func() bool, wait, interval time.Duration,
	acquire func() (releaseguardian.NodeWorkerComponentHold, error)) (releaseguardian.NodeWorkerComponentHold, error) {
	deadline := time.Now().Add(wait)
	for {
		if err := ctx.Err(); err != nil {
			return releaseguardian.NodeWorkerComponentHold{}, err
		}
		if !recent() {
			return releaseguardian.NodeWorkerComponentHold{}, errors.New("new node Worker journal was not made from recent observations")
		}
		hold, err := acquire()
		if !errors.Is(err, releaseguardian.ErrNodeWorkerComponentBusy) {
			return hold, err
		}
		left := time.Until(deadline)
		if left <= 0 {
			return hold, err
		}
		timer := time.NewTimer(min(interval, left))
		select {
		case <-ctx.Done():
			timer.Stop()
			return hold, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}

func (a *frontAuthorityActivator) claimNodeWorker(ctx context.Context, store *releaseguardian.AuthorityStore,
	component releaseguardian.NodeWorkerComponent, podUID string, j edgegroupfront.NodeWorkerJournal) (fence releaseguardian.NodeWorkerFence, resultErr error) {
	// Ordinary slot-template writes hold this component Lease, then take the
	// group Lease for Front activation. Keep the same order. After the
	// immutable journal is claimed, ordinary writers see it under their lock.
	componentHold, err := waitNodeWorkerComponent(ctx, func() bool {
		now := a.now().UTC()
		return !j.CreatedAt.After(now) && j.CreatedAt.Add(2*time.Minute).After(now)
	}, 10*time.Second, time.Second, func() (releaseguardian.NodeWorkerComponentHold, error) {
		return releaseguardian.AcquireNodeWorkerComponent(ctx, a.client, component, podUID, j.Digest, a.now().UTC())
	})
	if err != nil {
		return fence, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		resultErr = errors.Join(resultErr, componentHold.Release(cleanup, a.now()))
	}()
	lease, err := a.acquireLease(ctx)
	if err != nil {
		return fence, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		resultErr = errors.Join(resultErr, lease.release(cleanup))
	}()
	r, err := a.observeNodeWorkerRuntime(ctx, j, true, true)
	if err != nil {
		return fence, err
	}
	if err := a.requireWorkerRetirementPreflight(ctx, j, r); err != nil {
		return fence, err
	}
	raw, execErr := a.executor.Exec(ctx, a.config.Namespace, r.front.Name, "edge-front", frontActivationCASBinary, "--worker-recipient")
	var id edgegroupfront.NativeRecipient
	if execErr != nil || decodeStrictJSON(raw, &id) != nil || id != j.Recipient {
		return fence, errors.Join(errors.New("new node Worker journal lacks the exact running Front recipient"), execErr)
	}
	state, err := a.readNodeWorkerSelection(ctx, j, r)
	if err != nil || j.Progress(state) != edgegroupfront.NodeWorkerPending {
		return fence, errors.Join(errors.New("node Worker serving pointer is not the exact observed predecessor"), err)
	}
	if _, alreadyFinished, err := store.ReadNodeWorkerResult(ctx, j); err != nil || alreadyFinished {
		return fence, errors.Join(errors.New("node Worker journal was already finalized"), err)
	}
	if err := store.VerifyNodeWorkerClaimArtifact(ctx, j, r.next.pod, component); err != nil {
		return fence, err
	}
	if err := a.requireSameNodeWorkerRuntime(ctx, j, state, r, true); err != nil {
		return fence, err
	}
	fence, err = store.ClaimNodeWorker(ctx, j)
	if err != nil {
		return fence, err
	}
	return fence, store.EnsureNodeWorkerProjection(ctx, fence)
}
