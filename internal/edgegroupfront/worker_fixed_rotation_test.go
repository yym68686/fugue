package edgegroupfront

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func fixedRotationFixture(t *testing.T) (Config, NodeWorkerJournal) {
	t.Helper()
	cfg, j := nodeWorkerFixture(t, "b", 916)
	var d workerTransportDeclaration
	if err := json.Unmarshal([]byte(cfg.WorkerTransport), &d); err != nil {
		t.Fatal(err)
	}
	d.Schema, d.Candidate, d.HTTP, d.HTTPS = fixedWorkerRotationSchema, workerImagePin{}, 0, 0
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	cfg.WorkerTransport = string(raw)
	j.FixedTransport, j.FixedRotation = true, true
	j.ProbeServiceUID = "00000000-0000-4000-8000-000000000006"
	j, err = j.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return cfg, j
}

func selectedRotation(t *testing.T, cfg Config, want string) ActivationState {
	t.Helper()
	if err := cfg.prepareWorkerTransport(); err != nil {
		t.Fatal(err)
	}
	slot, activation, targets, ok := NewService(cfg, nil).selectedWorker(cfg)
	if slot != want || !ok || activation == nil || targets != cfg.Slots[want] {
		t.Fatalf("selected %q %+v %t, want %q", slot, targets, ok, want)
	}
	return *activation
}

func TestFixedRotationPersistsAcrossInterruptedSwitchAndRollback(t *testing.T) {
	cfg, first := fixedRotationFixture(t)
	selectedRotation(t, cfg, "b")
	if got, err := ObserveFixedWorkerRotation(cfg, first.Recipient); err != nil || got != first.Expected {
		t.Fatalf("preflight failed to observe the exact B activation: %+v %v", got, err)
	}
	legacy := cfg
	legacy.WorkerTransport = strings.Replace(legacy.WorkerTransport, fixedWorkerRotationSchema, "fugue.worker-transport-fixed/v1", 1)
	if _, err := testNodeWorkerWrite(t, legacy, first, "prepare", first.CreatedAt); err == nil {
		t.Fatal("legacy Front accepted a rotation journal")
	}
	if _, err := testNodeWorkerWrite(t, cfg, first, "prepare", first.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cfg.fixedWorkerBindingsPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prepare updated listener bindings: %v", err)
	}

	// Commit's first rename succeeds; inject loss before the activation CAS.
	wire, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	now := first.CreatedAt.Add(time.Second)
	deadline := now.Add(10 * time.Second)
	checks := 0
	_, err = executeNodeWorkerWrite(cfg, first.Recipient, wire,
		NodeWorkerWrite{NodeWorkerWriteSchema, first.Recipient, first.Digest, "commit", deadline},
		func() time.Time {
			checks++
			if checks >= 4 {
				return deadline
			}
			return now
		})
	if err == nil {
		t.Fatal("interrupted commit succeeded")
	}
	if _, err := os.Stat(cfg.fixedWorkerBindingsPath()); err != nil {
		t.Fatal("injected interruption did not reach the first rename:", err)
	}
	selectedRotation(t, cfg, "b")
	if _, err := testNodeWorkerWrite(t, cfg, first, "commit", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	active := selectedRotation(t, cfg, "a")
	if first.Progress(active) != NodeWorkerActivated || active.WorkerImageDigest != first.Candidate.WorkerImageDigest {
		t.Fatal("new A was not selected after the durable CAS")
	}

	second := first
	second.Expected, second.CreatedAt = active, active.UpdatedAt.Add(time.Second)
	second.PredecessorProcess, second.PredecessorWorkerUID = first.CandidateProcess, first.CandidateWorkerUID
	second.CandidateProcess, second.CandidateWorkerUID = NodeWorkerProcessDigest("second-edge", "second-caddy"), "00000000-0000-4000-8000-000000000008"
	second.Candidate = ActivationRepairRelease{"b", "next-b.p1", strings.Repeat("e", 40), "sha256:" + strings.Repeat("e", 64)}
	second, err = second.Seal()
	if err != nil {
		t.Fatal(err)
	}
	for i, op := range []string{"prepare", "commit", "rollback"} {
		if _, err := testNodeWorkerWrite(t, cfg, second, op, second.CreatedAt.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("second rotation %s: %v", op, err)
		}
		want := []string{"a", "b", "a"}[i]
		if live := selectedRotation(t, cfg, want); second.Progress(live) != []NodeWorkerProgress{NodeWorkerPending, NodeWorkerActivated, NodeWorkerReverted}[i] {
			t.Fatalf("second rotation %s selected the wrong release: %+v", op, live)
		}
	}
	if err := os.Remove(cfg.fixedWorkerBindingsPath()); err != nil {
		t.Fatal(err)
	}
	if err := cfg.prepareWorkerTransport(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := NewService(cfg, nil).selectedWorker(cfg); ok {
		t.Fatal("fell back to a stale port after loss of the rotation binding")
	}
	if _, err := ObserveFixedWorkerRotation(cfg, first.Recipient); err == nil {
		t.Fatal("preflight accepted a serving activation without its durable binding")
	}
}

func TestFixedRotationDrainOnlyRetainsAndClosesExactPreviousRelease(t *testing.T) {
	cfg, j := fixedRotationFixture(t)
	cfg.WorkerDrainGrantPath, cfg.WorkerDrainFrontUID = cfg.ActiveSlotFile+".grant", j.Recipient.PodUID
	for i, op := range []string{"prepare", "commit"} {
		if _, err := testNodeWorkerWrite(t, cfg, j, op, j.CreatedAt.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	active := selectedRotation(t, cfg, "a")
	if err := cfg.prepareWorkerTransport(); err != nil {
		t.Fatal(err)
	}
	s := NewService(cfg, nil)
	old, _, _ := addDrainFixture(t, s, j.Expected, true)
	fresh, _, _ := addDrainFixture(t, s, active, true)
	start := active.UpdatedAt
	g := WorkerDrainGrant{Schema: WorkerDrainSchema, FrontUID: cfg.WorkerDrainFrontUID, Node: cfg.EdgeID,
		Previous: j.Expected, Serving: active, Start: start, Deadline: start.Add(WorkerDrainLimit), HealthyAt: start}
	writeDrainFixture(t, cfg, g)
	if err := s.reconcileWorkerDrain(cfg, g.Deadline.Add(-time.Second)); err != nil || old.stopping || fresh.stopping {
		t.Fatalf("connection lost before the deadline: %v", err)
	}
	g.HealthyAt = g.Deadline
	wrong := g
	wrong.Previous.BundleGeneration = "different-publication"
	writeDrainFixture(t, cfg, wrong)
	if err := s.reconcileWorkerDrain(cfg, g.Deadline); err == nil || old.stopping {
		t.Fatalf("unrelated grant was allowed to close the old Worker: %v", err)
	}
	writeDrainFixture(t, cfg, g)
	if err := s.reconcileWorkerDrain(cfg, g.Deadline); err != nil || !old.stopping || fresh.stopping {
		t.Fatalf("did not close only the old Worker at deadline: %v", err)
	}
}
