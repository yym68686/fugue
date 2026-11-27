package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
)

func TestNodeWorkerWatchAttributesRealFrontSocketsAcrossSelection(t *testing.T) {
	const domain = "example.com"
	old := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "retained")
	}))
	defer old.Close()
	next := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "candidate")
	}))
	defer next.Close()
	reserve := func() net.Listener {
		l, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Close() })
		return l
	}
	tlsListener, healthListener := reserve(), reserve()
	public, health := tlsListener.Addr().String(), healthListener.Addr().String()
	_, _, j := nodeWorkerRuntimeFixture(t)
	path := filepath.Join(t.TempDir(), "activation.json")
	oldTarget, nextTarget := old.Listener.Addr().String(), next.Listener.Addr().String()
	cfg := edgegroupfront.Config{
		HTTPSListenAddr: public, HealthAddr: health, HTTPMode: edgegroupfront.HTTPModeDisabled,
		EdgeGroupID: j.Recipient.Group, DefaultSlot: j.Expected.ActiveSlot, ActiveSlotFile: path, RequireActivationState: true,
		Slots: map[string]edgegroupfront.SlotTargets{j.Expected.ActiveSlot: {HTTPSAddress: oldTarget}, j.Candidate.Slot: {HTTPSAddress: nextTarget}},
	}
	apply := func(req edgegroupfront.ActivationCASRequest) {
		t.Helper()
		if _, err := edgegroupfront.ApplyActivationCAS(path, req, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	apply(edgegroupfront.ActivationCASRequest{
		GroupID: j.Recipient.Group, ExpectedSlot: j.Expected.ActiveSlot, TargetSlot: j.Expected.ActiveSlot, InitialGeneration: j.Expected.Generation,
		BundleGeneration: j.Expected.BundleGeneration, WorkerSourceCommit: j.Expected.WorkerSourceCommit, WorkerImageDigest: j.Expected.WorkerImageDigest,
		Operation: edgegroupfront.ActivationOperationInit, Reason: j.Expected.Reason,
	})
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	done := make(chan error, 1)
	_ = tlsListener.Close()
	_ = healthListener.Close()
	go func() { done <- edgegroupfront.NewService(cfg, log.New(io.Discard, "", 0)).Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Front exited with %v", err)
		}
	}()
	roots := old.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	dial := func() (*nodeWorkerTLS, error) {
		return openNodeWorkerTLS(ctx, netip.MustParseAddrPort(public), domain, roots)
	}
	var held *nodeWorkerTLS
	for held == nil {
		var err error
		held, err = dial()
		if err != nil {
			select {
			case <-ctx.Done():
				t.Fatalf("Front did not start: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	defer held.close()
	observe := func(socket *nodeWorkerTLS, body string) releaseguardian.NodeWorkerConnection {
		t.Helper()
		if err := socket.serving(ctx, "/ready", shaDigest([]byte(body))); err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+health+"/edge/tcp-connections", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := nodeWorkerInventoryClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("Front inventory returned %d: %v", resp.StatusCode, err)
		}
		inventory, err := decodeNodeWorkerInventory(raw)
		if err != nil {
			t.Fatal(err)
		}
		fact, found, err := nodeWorkerInventoryFact(inventory, j.Recipient.PodUID, socket.source, socket.destination)
		if err != nil || !found {
			t.Fatalf("Front did not attribute the exact socket: %v", err)
		}
		return fact
	}
	before := observe(held, "retained")
	apply(edgegroupfront.ActivationCASRequest{
		GroupID: j.Recipient.Group, ExpectedGeneration: j.Expected.Generation, ExpectedSlot: j.Expected.ActiveSlot, TargetSlot: j.Candidate.Slot,
		BundleGeneration: j.Candidate.BundleGeneration, WorkerSourceCommit: j.Candidate.WorkerSourceCommit, WorkerImageDigest: j.Candidate.WorkerImageDigest,
		Operation: edgegroupfront.ActivationOperationPromote, Reason: "advance node Worker using journal " + j.Digest,
	})
	fresh, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.close()
	after := observe(fresh, "candidate")
	if before.Slot != j.Expected.ActiveSlot || before.Target != oldTarget || !before.Same(observe(held, "retained")) ||
		after.Slot != j.Candidate.Slot || after.Target != nextTarget || after.ID == before.ID || !after.StartedAt.After(before.StartedAt) {
		t.Fatalf("Front did not retain the original socket while selecting the candidate for new sockets: old=%+v new=%+v", before, after)
	}
}

func watchedNodeWorkerActivation(j edgegroupfront.NodeWorkerJournal) edgegroupfront.ActivationState {
	return edgegroupfront.ActivationState{
		Schema: edgegroupfront.ActivationStateSchemaV1, GroupID: j.Recipient.Group, Generation: j.Expected.Generation + 1,
		ActiveSlot: j.Candidate.Slot, PreviousSlot: j.Expected.ActiveSlot, BundleGeneration: j.Candidate.BundleGeneration,
		WorkerSourceCommit: j.Candidate.WorkerSourceCommit, WorkerImageDigest: j.Candidate.WorkerImageDigest,
		Authority: edgegroupfront.ActivationAuthority, Operation: edgegroupfront.ActivationOperationPromote,
		Reason: "advance node Worker using journal " + j.Digest, UpdatedAt: j.CreatedAt.Add(20 * time.Second),
	}
}

func TestNodeWorkerWatchOnlyToleratesTheExactPredecessorDuringReload(t *testing.T) {
	_, _, journal := nodeWorkerRuntimeFixture(t)
	state := watchedNodeWorkerActivation(journal)
	const hostIP = "192.0.2.7"
	predecessorTarget := net.JoinHostPort(hostIP, "28443")
	candidateTarget := net.JoinHostPort(hostIP, strconv.Itoa(int(journal.CandidateHTTPSPort())))
	health := func(activation edgegroupfront.ActivationState, target string) nodeWorkerFrontHealth {
		return nodeWorkerFrontHealth{observedFront: observedFront{
			Status: "ok", Generation: activation.Generation, ActiveSlot: activation.ActiveSlot, RouteAuthority: activation.Authority,
			BundleGeneration: activation.BundleGeneration, WorkerSourceCommit: activation.WorkerSourceCommit,
			WorkerImageDigest: activation.WorkerImageDigest,
		}, Target: target}
	}
	if err := checkNodeWorkerLiveSelection(journal, state, health(journal.Expected, predecessorTarget), predecessorTarget, hostIP); !errors.Is(err, errNodeWorkerFrontReloading) {
		t.Fatalf("exact predecessor may remain briefly during reload: %v", err)
	}
	if err := checkNodeWorkerLiveSelection(journal, state, health(state, candidateTarget), predecessorTarget, hostIP); err != nil {
		t.Fatalf("exact candidate must converge: %v", err)
	}
	for label, incorrect := range map[string]nodeWorkerFrontHealth{
		"predecessor on a different listener": health(journal.Expected, candidateTarget),
		"candidate on a different listener":   health(state, predecessorTarget),
		"empty or unreadable health":          {},
	} {
		if err := checkNodeWorkerLiveSelection(journal, state, incorrect, predecessorTarget, hostIP); err == nil || errors.Is(err, errNodeWorkerFrontReloading) {
			t.Errorf("%s must fail closed: %v", label, err)
		}
	}
}

func TestNodeWorkerWatchVerifiesIndependentReceiptAndDrain(t *testing.T) {
	_, _, journal := nodeWorkerRuntimeFixture(t)
	journal.ProbeServiceUID = "00000000-0000-4000-8000-000000000006"
	journal.Digest = ""
	journal, err := journal.Seal()
	if err != nil {
		t.Fatal(err)
	}
	state := watchedNodeWorkerActivation(journal)
	now := state.UpdatedAt.Add(10 * time.Second)
	conn := func(id, slot, target string, started time.Time) releaseguardian.NodeWorkerConnection {
		return releaseguardian.NodeWorkerConnection{
			FrontUID: journal.Recipient.PodUID, ID: id, Slot: slot, Target: target, StartedAt: started,
		}
	}
	const predecessor, candidate = "192.0.2.7:28443", "192.0.2.7:38443"
	legacyProbe := conn("legacy-probe", "a", candidate, state.UpdatedAt.Add(time.Second))
	observedProbe := conn("observed-probe", "a", candidate, state.UpdatedAt.Add(time.Second))
	arm := releaseguardian.NodeWorkerArm{
		Schema: releaseguardian.NodeWorkerArmSchema, JournalDigest: journal.Digest,
		Held: conn("legacy-held", "b", predecessor, journal.CreatedAt), OpenedAt: journal.CreatedAt.Add(time.Second),
		Deadline: journal.CreatedAt.Add(150 * time.Second),
	}
	w := releaseguardian.NodeWorkerWitness{
		Schema: releaseguardian.NodeWorkerWitnessSchema, JournalDigest: journal.Digest,
		Held: arm.Held, Fresh: conn("legacy-fresh", "a", candidate, state.UpdatedAt.Add(time.Second)),
		ProbeFresh: &legacyProbe, ObservedAt: now.Add(-time.Second),
	}
	observed := w
	observed.Held = conn("observed-held", "b", predecessor, journal.CreatedAt)
	observed.Fresh = conn("observed-fresh", "a", candidate, state.UpdatedAt.Add(time.Second))
	observed.ProbeFresh = &observedProbe
	result := releaseguardian.NodeWorkerResult{
		Schema: "edge-node-worker-result/v1", JournalDigest: journal.Digest, Outcome: "verified", Activation: state,
		Witness: &w, DrainStart: &now,
	}
	if err := matchWatchedNodeWorkerReceipt(journal, state, observed, arm, result, now); err != nil {
		t.Fatalf("expected Guardian and read-only watcher to use independent sockets for the same listeners: %v", err)
	}
	for label, alter := range map[string]func(*releaseguardian.NodeWorkerResult){
		"wrong activation": func(r *releaseguardian.NodeWorkerResult) { r.Activation.UpdatedAt = now },
		"wrong journal":    func(r *releaseguardian.NodeWorkerResult) { r.JournalDigest = "unrelated-journal" },
		"unproven drain":   func(r *releaseguardian.NodeWorkerResult) { r.DrainStart = nil },
		"outcome failure":  func(r *releaseguardian.NodeWorkerResult) { r.Outcome = "reverted" },
		"reopened predecessor": func(r *releaseguardian.NodeWorkerResult) {
			copied := *r.Witness
			copied.Held.ID = "different-from-arm"
			r.Witness = &copied
		},
		"unrelated candidate listener": func(r *releaseguardian.NodeWorkerResult) {
			copied := *r.Witness
			probe := *copied.ProbeFresh
			probe.Target = "192.0.2.8:38443"
			copied.Fresh.Target, copied.ProbeFresh = probe.Target, &probe
			r.Witness = &copied
		},
	} {
		incorrect := result
		alter(&incorrect)
		if err := matchWatchedNodeWorkerReceipt(journal, state, observed, arm, incorrect, now); err == nil {
			t.Errorf("accepted %s", label)
		}
	}
}

func TestNodeWorkerWatchedEndpointsStayPinned(t *testing.T) {
	a, _, journal := nodeWorkerRuntimeFixture(t)
	runtime, err := a.observeNodeWorkerRuntime(t.Context(), journal, true, true)
	if err != nil {
		t.Fatal(err)
	}
	public, probe, err := nodeWorkerIngressAddresses(runtime, journal)
	if err != nil {
		t.Fatal(err)
	}
	for label, alter := range map[string]func(*nodeWorkerRuntime){
		"public Service IP": func(r *nodeWorkerRuntime) { r.service.Spec.ExternalIPs = []string{"192.0.2.8"} },
		"Front Pod IP":      func(r *nodeWorkerRuntime) { r.front.Status.PodIP = "10.0.0.10" },
		"old Worker":        func(r *nodeWorkerRuntime) { r.prior.target = "192.0.2.7:18443" },
		"new Worker":        func(r *nodeWorkerRuntime) { r.next.target = "192.0.2.7:18443" },
		"unexpected probe": func(r *nodeWorkerRuntime) {
			r.probe.Spec.Ports = []corev1.ServicePort{{Port: 15443}}
		},
	} {
		changed := runtime
		alter(&changed)
		if label == "unexpected probe" {
			journal.ProbeServiceUID = "00000000-0000-4000-8000-000000000007"
		}
		if err := requireWatchedNodeWorkerEndpoints(journal, runtime, changed, public, probe); err == nil {
			t.Errorf("accepted a changed %s", label)
		}
		journal.ProbeServiceUID = ""
	}
}
