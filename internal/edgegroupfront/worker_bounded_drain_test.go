package edgegroupfront

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func drainFixture(t *testing.T) (Config, WorkerDrainGrant, *Service) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "activation.json")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	first, err := ApplyActivationCAS(path, activationRequest(ActivationOperationInit, 0, "a", "a", "bundle-a", 0), start.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	req := activationRequest(ActivationOperationPromote, 1, "a", "b", "bundle-b", 0)
	// Fixtures for ordinary activation use one image digest; a drain needs
	// two distinct physical transports pinned to different images.
	req.WorkerImageDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	next, err := ApplyActivationCAS(path, req, start)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		ActiveSlotFile: path, EdgeGroupID: first.Current.GroupID, EdgeID: "fixture-node",
		WorkerDrainGrantPath:   filepath.Join(filepath.Dir(path), "drain.json"),
		WorkerDrainFrontUID:    "01234567-89ab-cdef-0123-456789abcdef",
		RequireActivationState: true, WorkerTransport: "fixture-pinned-transport",
		workerTargets: map[workerImagePin]SlotTargets{
			workerPin(first.Current): {HTTPSAddress: "127.0.0.1:18443"},
			workerPin(next.Current):  {HTTPSAddress: "127.0.0.1:28443"},
		},
	}
	g := WorkerDrainGrant{
		Schema: WorkerDrainSchema, FrontUID: cfg.WorkerDrainFrontUID, Node: cfg.EdgeID,
		Previous: first.Current, Serving: next.Current, Start: start, Deadline: start.Add(WorkerDrainLimit), HealthyAt: start,
	}
	return cfg, g, NewService(cfg, log.New(io.Discard, "", 0))
}

func writeDrainFixture(t *testing.T, cfg Config, g WorkerDrainGrant) {
	t.Helper()
	b, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.WorkerDrainGrantPath, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func addDrainFixture(t *testing.T, s *Service, worker ActivationState, withUpstream bool) (*workerDrainConnection, net.Conn, net.Conn) {
	t.Helper()
	d, client := net.Pipe()
	c := &workerDrainConnection{worker: workerPin(worker), generation: worker.Generation, downstream: d, cancel: func() {}}
	var backend net.Conn
	if withUpstream {
		c.upstream, backend = net.Pipe()
		t.Cleanup(func() { _ = c.upstream.Close(); _ = backend.Close() })
	}
	t.Cleanup(func() { _ = d.Close(); _ = client.Close() })
	if s.workerDrainConnections == nil {
		s.workerDrainConnections = make(map[*workerDrainConnection]struct{})
	}
	s.workerDrainConnections[c] = struct{}{}
	return c, client, backend
}

func TestWorkerDrainClosesOnlyExactPreviousTransportAtDeadline(t *testing.T) {
	cfg, g, s := drainFixture(t)
	old, client, backend := addDrainFixture(t, s, g.Previous, true)
	pending, _, _ := addDrainFixture(t, s, g.Previous, false)
	promoted, _, _ := addDrainFixture(t, s, g.Serving, true)
	reused := g.Previous
	reused.WorkerImageDigest = g.Serving.WorkerImageDigest
	unrelated, _, _ := addDrainFixture(t, s, reused, true)
	writeDrainFixture(t, cfg, g)
	if err := s.reconcileWorkerDrain(cfg, g.Deadline.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if old.stopping || pending.stopping || s.workerDrainObservation.Remaining != 2 {
		t.Fatal("closed or missed an old connection before deadline")
	}
	if err := s.reconcileWorkerDrain(cfg, g.Deadline); err != nil {
		t.Fatal(err)
	}
	if old.stopping {
		t.Fatal("stale health witness authorized a closure")
	}
	g.HealthyAt = g.Deadline
	writeDrainFixture(t, cfg, g)
	if err := s.reconcileWorkerDrain(cfg, g.Deadline); err != nil {
		t.Fatal(err)
	}
	if !old.stopping || !pending.stopping || promoted.stopping || unrelated.stopping {
		t.Fatal("incorrect deadline target")
	}
	for _, endpoint := range []net.Conn{client, backend} {
		_ = endpoint.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := endpoint.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("endpoint not closed: %v", err)
		}
	}
	if s.attachDrainUpstream(pending, nil) {
		t.Fatal("in-flight old dial attached after closure")
	}
	if err := s.reconcileWorkerDrain(cfg, g.Deadline.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if s.workerDrainObservation.Forced != 2 {
		t.Fatal("counted a repeated command")
	}
	s.forgetDrainConnection(old)
	s.forgetDrainConnection(pending)
	if err := s.reconcileWorkerDrain(cfg, g.Deadline.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.reconcileWorkerDrain(cfg, g.Deadline.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if s.workerDrainObservation.Outcome != "closed_at_deadline" {
		t.Fatal("forced completion not attributed")
	}
}

func TestWorkerDrainQuietCompletionAndRollbackFence(t *testing.T) {
	cfg, g, s := drainFixture(t)
	writeDrainFixture(t, cfg, g)
	if err := s.reconcileWorkerDrain(cfg, g.Start); err != nil {
		t.Fatal(err)
	}
	if err := s.reconcileWorkerDrain(cfg, g.Start.Add(workerDrainQuiet-time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	if s.workerDrainObservation.Outcome != "" {
		t.Fatal("reported empty before quiet window")
	}
	if err := s.reconcileWorkerDrain(cfg, g.Start.Add(workerDrainQuiet)); err != nil {
		t.Fatal(err)
	}
	if s.workerDrainObservation.Outcome != "drained_naturally" {
		t.Fatal("did not finish naturally")
	}

	s = NewService(cfg, log.New(io.Discard, "", 0))
	old, _, _ := addDrainFixture(t, s, g.Previous, true)
	if _, err := ApplyActivationCAS(cfg.ActiveSlotFile, activationRequest(ActivationOperationRollback, 2, "b", "a", "bundle-a", 2), g.Deadline.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	g.HealthyAt = g.Deadline
	writeDrainFixture(t, cfg, g)
	if err := s.reconcileWorkerDrain(cfg, g.Deadline); err == nil || old.stopping {
		t.Fatal("delayed deadline command crossed rollback")
	}
}
