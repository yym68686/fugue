package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	"fugue/internal/routeprobe"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
)

type nodeWorkerWatchReport struct {
	Phase         string                            `json:"phase"`
	JournalDigest string                            `json:"journal_digest"`
	Activation    edgegroupfront.ActivationState    `json:"activation"`
	RouteDigest   string                            `json:"route_digest"`
	Connections   releaseguardian.NodeWorkerWitness `json:"connections"`
	Receipt       releaseguardian.NodeWorkerResult  `json:"guardian_result"`
}

type nodeWorkerWatchReady struct {
	Phase         string                               `json:"phase"`
	JournalDigest string                               `json:"journal_digest"`
	Held          releaseguardian.NodeWorkerConnection `json:"held"`
	Deadline      time.Time                            `json:"deadline"`
	RouteDigest   string                               `json:"route_digest"`
}

// This observer uses the same runtime checks as a claim. The observer's
// credentials and transport allow only reads, even while the old writer
// drives a separately armed transaction.
func runNodeWorkerWatch(parent context.Context, args []string, in io.Reader, out io.Writer) error {
	if out == nil {
		return errors.New("node-worker-watch requires an output stream")
	}
	var j edgegroupfront.NodeWorkerJournal
	var group string
	switch {
	case len(args) == 0 && in != nil:
		raw, err := readClaimStdin(in)
		if err != nil {
			return err
		}
		j, err = edgegroupfront.ReadNodeWorkerJournal(raw)
		if err != nil {
			return err
		}
		group = j.Recipient.Group
	case len(args) == 4 && args[0] == "--claimed" && frontRepairIdentity(args[1]) &&
		frontRepairIdentity(args[2]) && exactSHA256Digest(args[3]):
		group = args[1]
	default:
		return errors.New("usage: node-worker-watch [--claimed GROUP NODE CANDIDATE_RELEASE_DIGEST]; without arguments, supply the sealed journal on stdin")
	}
	cfg, err := loadKubeConfig()
	if err != nil {
		return err
	}
	reader := rest.CopyConfig(cfg)
	reader.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return nodeWorkerShadowObjects{next: next}
	})
	targets, store, front, err := loadNodeWorkerAuthority(reader, cfg, group)
	if err != nil {
		return err
	}
	front.executor = nodeWorkerShadowExec{next: front.executor, namespace: front.config.Namespace}
	if len(args) != 0 {
		j, err = front.waitForClaimedNodeWorkerWatch(parent, store, args[2], args[3])
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	component, err := store.FindNodeWorkerComponent(ctx, targets, j.Recipient.Group)
	if err != nil {
		return err
	}
	preflight, err := front.preflightNodeWorker(ctx, store, component, j)
	if err != nil {
		return err
	}
	if _, err := front.observeNodeWorkerRollback(ctx, j, preflight); err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	report, err := front.watchNodeWorker(ctx, store, j, preflight, encoder)
	if err != nil {
		return err
	}
	return encoder.Encode(report)
}

func (a *frontAuthorityActivator) waitForClaimedNodeWorkerWatch(ctx context.Context, store *releaseguardian.AuthorityStore,
	node, releaseDigest string) (edgegroupfront.NodeWorkerJournal, error) {
	var journal edgegroupfront.NodeWorkerJournal
	err := wait.PollUntilContextTimeout(ctx, time.Second, 10*time.Minute, true, func(ctx context.Context) (bool, error) {
		fence, found, err := store.LoadNodeWorker(ctx, a.config.GroupID)
		if err != nil || !found {
			return false, err
		}
		j := fence.Journal
		if j.Recipient.Node != node || j.ReleaseDigest != releaseDigest || !a.recentNodeWorkerJournal(j) {
			return false, nil
		}
		journal = j
		return true, nil
	})
	if err != nil {
		err = fmt.Errorf("observe the claimed handoff for the requested node and candidate release: %w", err)
	}
	return journal, err
}

type nodeWorkerWatchSocket struct {
	tls  *nodeWorkerTLS
	fact nodeWorkerIngressFact
}

func (a *frontAuthorityActivator) watchNodeWorker(ctx context.Context, store *releaseguardian.AuthorityStore, j edgegroupfront.NodeWorkerJournal,
	preflight nodeWorkerPreflight, events *json.Encoder) (nodeWorkerWatchReport, error) {
	runtime := preflight.runtime
	publicAddress, probeAddress, err := nodeWorkerIngressAddresses(runtime, j)
	if err != nil {
		return nodeWorkerWatchReport{}, err
	}
	held, previous, err := a.openObservedNodeWorkerSocket(ctx, runtime, j, publicAddress, j.Expected.ActiveSlot, runtime.prior.target)
	if err != nil {
		return nodeWorkerWatchReport{}, err
	}
	defer held.tls.close()
	if err := a.requireSameNodeWorkerRuntime(ctx, j, preflight.activation, runtime, true); err != nil {
		return nodeWorkerWatchReport{}, err
	}
	opened := a.now().UTC()
	arm := releaseguardian.NodeWorkerArm{
		Schema: releaseguardian.NodeWorkerArmSchema, JournalDigest: j.Digest,
		Held: held.fact.Connection, OpenedAt: opened, Deadline: opened.Add(150 * time.Second),
	}
	if err := arm.Validate(j, opened); err != nil {
		return nodeWorkerWatchReport{}, err
	}
	watchCtx, cancel := context.WithDeadline(ctx, arm.Deadline)
	defer cancel()
	if err := events.Encode(nodeWorkerWatchReady{
		Phase: "observing", JournalDigest: j.Digest, Held: arm.Held, Deadline: arm.Deadline, RouteDigest: previous.Digest,
	}); err != nil {
		return nodeWorkerWatchReport{}, err
	}
	poll := time.NewTicker(2 * time.Second)
	defer poll.Stop()
	var activationSeen *edgegroupfront.ActivationState
	for {
		if err := a.proveWatchedNodeWorkerSocket(watchCtx, runtime, j, held); err != nil {
			return nodeWorkerWatchReport{}, err
		}
		current, err := a.observeNodeWorkerRuntime(watchCtx, j, true, true)
		if err != nil {
			return nodeWorkerWatchReport{}, err
		}
		// The same journal pins all physical identities and the original
		// socket. A service IP or destination change is still a mismatch.
		if err := requireWatchedNodeWorkerEndpoints(j, runtime, current, publicAddress, probeAddress); err != nil {
			return nodeWorkerWatchReport{}, err
		}
		state, err := a.readNodeWorkerSelection(watchCtx, j, current)
		if err != nil && !errors.Is(err, errNodeWorkerFrontReloading) {
			return nodeWorkerWatchReport{}, err
		}
		switch j.Progress(state) {
		case edgegroupfront.NodeWorkerActivated:
			if activationSeen != nil && *activationSeen != state {
				return nodeWorkerWatchReport{}, errors.New("durable activation changed while the Front reloaded")
			}
			if err == nil {
				return a.watchActivatedNodeWorker(watchCtx, store, j, current, state, arm, held, previous, publicAddress, probeAddress)
			}
			activationSeen = &state
		case edgegroupfront.NodeWorkerPending:
			if activationSeen != nil {
				return nodeWorkerWatchReport{}, errors.New("Front returned to the predecessor after advancing the durable activation")
			}
			if _, found, err := store.ReadNodeWorkerResult(watchCtx, j); err != nil || found {
				return nodeWorkerWatchReport{}, errors.Join(errors.New("Guardian ended the watched handoff before advancing"), err)
			}
		default:
			return nodeWorkerWatchReport{}, errors.New("Front rolled back or departed from the watched journal")
		}
		select {
		case <-watchCtx.Done():
			return nodeWorkerWatchReport{}, errors.Join(errors.New("Front did not cut over during the held TLS observation"), watchCtx.Err())
		case <-poll.C:
		}
	}
}

func nodeWorkerIngressAddresses(runtime nodeWorkerRuntime, j edgegroupfront.NodeWorkerJournal) (netip.AddrPort, netip.AddrPort, error) {
	if len(runtime.service.Spec.ExternalIPs) != 1 {
		return netip.AddrPort{}, netip.AddrPort{}, errors.New("public Front Service lacks an exact address")
	}
	ip, err := netip.ParseAddr(runtime.service.Spec.ExternalIPs[0])
	if err != nil {
		return netip.AddrPort{}, netip.AddrPort{}, err
	}
	public := netip.AddrPortFrom(ip, 443)
	if j.ProbeServiceUID == "" {
		return public, netip.AddrPort{}, nil
	}
	if string(runtime.probe.UID) != j.ProbeServiceUID || len(runtime.probe.Spec.Ports) != 1 || runtime.probe.Spec.Ports[0].Port != 15443 {
		return netip.AddrPort{}, netip.AddrPort{}, errors.New("approved Front probe listener is missing")
	}
	return public, netip.AddrPortFrom(ip, 15443), nil
}

func requireWatchedNodeWorkerEndpoints(j edgegroupfront.NodeWorkerJournal, before, current nodeWorkerRuntime,
	publicAddress, probeAddress netip.AddrPort) error {
	public, probe, err := nodeWorkerIngressAddresses(current, j)
	if err != nil || public != publicAddress || probe != probeAddress || current.prior.target != before.prior.target ||
		current.next.target != before.next.target || current.front.Status.PodIP != before.front.Status.PodIP {
		return errors.Join(errors.New("Front endpoints changed during the handoff watch"), err)
	}
	return nil
}

func (a *frontAuthorityActivator) openObservedNodeWorkerSocket(ctx context.Context, runtime nodeWorkerRuntime, j edgegroupfront.NodeWorkerJournal,
	address netip.AddrPort, slot, target string) (nodeWorkerWatchSocket, routeprobe.Proof, error) {
	socket, err := openNodeWorkerTLS(ctx, address, a.config.RouteHost, nil)
	if err != nil {
		return nodeWorkerWatchSocket{}, routeprobe.Proof{}, err
	}
	proof, err := socket.proof(ctx, a.config.RoutePath, j.Recipient.Node, j.Recipient.Group)
	if err != nil {
		socket.close()
		return nodeWorkerWatchSocket{}, routeprobe.Proof{}, err
	}
	if err := socket.serving(ctx, a.config.RoutePath, a.config.RouteBodyDigest); err != nil {
		socket.close()
		return nodeWorkerWatchSocket{}, routeprobe.Proof{}, err
	}
	fact, err := observeNodeWorkerIngressFact(ctx, runtime, j, socket, slot, target)
	if err != nil {
		socket.close()
		return nodeWorkerWatchSocket{}, routeprobe.Proof{}, err
	}
	return nodeWorkerWatchSocket{socket, fact}, proof, nil
}

func (a *frontAuthorityActivator) proveWatchedNodeWorkerSocket(ctx context.Context, runtime nodeWorkerRuntime,
	j edgegroupfront.NodeWorkerJournal, socket nodeWorkerWatchSocket) error {
	if err := socket.tls.serving(ctx, a.config.RoutePath, a.config.RouteBodyDigest); err != nil {
		return err
	}
	fresh, err := observeNodeWorkerIngressFact(ctx, runtime, j, socket.tls, socket.fact.Connection.Slot, socket.fact.Connection.Target)
	if err != nil || !fresh.Connection.Same(socket.fact.Connection) || fresh.Source != socket.fact.Source {
		return errors.Join(errors.New("a watched Front socket changed before the Guardian result"), err)
	}
	return nil
}

func (a *frontAuthorityActivator) watchActivatedNodeWorker(ctx context.Context, store *releaseguardian.AuthorityStore, j edgegroupfront.NodeWorkerJournal,
	runtime nodeWorkerRuntime, state edgegroupfront.ActivationState, arm releaseguardian.NodeWorkerArm, held nodeWorkerWatchSocket,
	previous routeprobe.Proof, publicAddress, probeAddress netip.AddrPort) (nodeWorkerWatchReport, error) {
	fresh, current, err := a.openObservedNodeWorkerSocket(ctx, runtime, j, publicAddress, j.Candidate.Slot, runtime.next.target)
	if err != nil {
		return nodeWorkerWatchReport{}, fmt.Errorf("verify new public connection after cutover: %w", err)
	}
	defer fresh.tls.close()
	sockets := []nodeWorkerWatchSocket{held, fresh}
	var probeRef *releaseguardian.NodeWorkerConnection
	if probeAddress.IsValid() {
		probe, proof, err := a.openObservedNodeWorkerSocket(ctx, runtime, j, probeAddress, j.Candidate.Slot, runtime.next.target)
		if err != nil {
			return nodeWorkerWatchReport{}, fmt.Errorf("verify new independent probe connection after cutover: %w", err)
		}
		defer probe.tls.close()
		if !sameNodeWorkerProof(current, proof) {
			return nodeWorkerWatchReport{}, errors.New("new public and probe sockets disagree on the route proof")
		}
		probeRef = &probe.fact.Connection
		sockets = append(sockets, probe)
	}
	after, err := fresh.tls.proof(ctx, a.config.RoutePath, j.Recipient.Node, j.Recipient.Group)
	if err != nil || !sameNodeWorkerProof(current, after) || previous.Digest != current.Digest {
		return nodeWorkerWatchReport{}, errors.Join(errors.New("route proof changed across the watched handoff"), err)
	}
	w := releaseguardian.NodeWorkerWitness{
		Schema: releaseguardian.NodeWorkerWitnessSchema, JournalDigest: j.Digest,
		Held: held.fact.Connection, Fresh: fresh.fact.Connection, ProbeFresh: probeRef, ObservedAt: a.now().UTC(),
	}
	if err := w.Validate(j, arm, state, a.now().UTC()); err != nil {
		return nodeWorkerWatchReport{}, err
	}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		result, found, err := store.ReadNodeWorkerResult(ctx, j)
		if err != nil {
			return nodeWorkerWatchReport{}, err
		}
		for _, socket := range sockets {
			if err := a.proveWatchedNodeWorkerSocket(ctx, runtime, j, socket); err != nil {
				return nodeWorkerWatchReport{}, err
			}
		}
		live, err := a.observeNodeWorkerRuntime(ctx, j, true, true)
		if err != nil {
			return nodeWorkerWatchReport{}, err
		}
		if err := requireWatchedNodeWorkerEndpoints(j, runtime, live, publicAddress, probeAddress); err != nil {
			return nodeWorkerWatchReport{}, err
		}
		selected, err := a.readNodeWorkerSelection(ctx, j, live)
		if err != nil || selected != state {
			return nodeWorkerWatchReport{}, errors.Join(errors.New("Front activation changed while awaiting the Guardian result"), err)
		}
		if found && result.Outcome == "verified" {
			for _, socket := range sockets[1:] {
				proof, err := socket.tls.proof(ctx, a.config.RoutePath, j.Recipient.Node, j.Recipient.Group)
				if err != nil || proof.Digest != previous.Digest {
					return nodeWorkerWatchReport{}, errors.Join(errors.New("candidate route no longer matches the retained route at acknowledgement"), err)
				}
				if err := a.proveWatchedNodeWorkerSocket(ctx, live, j, socket); err != nil {
					return nodeWorkerWatchReport{}, err
				}
			}
		}
		if err := a.requireSameNodeWorkerRuntime(ctx, j, state, live, true); err != nil {
			return nodeWorkerWatchReport{}, err
		}
		if found {
			resultArm, armed, err := store.ReadNodeWorkerArm(ctx, j, a.now().UTC())
			if err != nil || !armed {
				return nodeWorkerWatchReport{}, errors.Join(errors.New("Guardian result lacks its immutable arm"), err)
			}
			if err := matchWatchedNodeWorkerReceipt(j, state, w, resultArm, result, a.now().UTC()); err != nil {
				return nodeWorkerWatchReport{}, err
			}
			return nodeWorkerWatchReport{
				Phase: "verified", JournalDigest: j.Digest, Activation: state, RouteDigest: current.Digest, Connections: w, Receipt: result,
			}, nil
		}
		select {
		case <-ctx.Done():
			return nodeWorkerWatchReport{}, errors.Join(errors.New("Guardian did not confirm the watched cutover"), ctx.Err())
		case <-tick.C:
		}
	}
}

func matchWatchedNodeWorkerReceipt(j edgegroupfront.NodeWorkerJournal, state edgegroupfront.ActivationState,
	w releaseguardian.NodeWorkerWitness, arm releaseguardian.NodeWorkerArm, result releaseguardian.NodeWorkerResult, now time.Time) error {
	if !result.MatchesJournal(j) || result.Outcome != "verified" || result.Activation != state ||
		(j.ProbeServiceUID != "" && result.DrainStart == nil) {
		return errors.New("Guardian did not verify the activation witnessed on the held sockets")
	}
	if err := arm.Validate(j, now); err != nil {
		return err
	}
	if err := result.Witness.Validate(j, arm, state, now); err != nil {
		return err
	}
	if result.Witness.Held.Target != w.Held.Target || result.Witness.Fresh.Target != w.Fresh.Target {
		return errors.New("Guardian verified different physical Worker listeners")
	}
	return nil
}
