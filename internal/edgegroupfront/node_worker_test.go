package edgegroupfront

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func nodeWorkerFixture(t *testing.T, slot string, generation uint64) (Config, NodeWorkerJournal) {
	t.Helper()
	cfg, transport := transportFixture()
	cfg.ActiveSlotFile = filepath.Join(t.TempDir(), "activation.json")
	if slot == "a" {
		transport.Candidate.Slot = "b"
	}
	raw, err := json.Marshal(transport)
	if err != nil {
		t.Fatal(err)
	}
	cfg.WorkerTransport = string(raw)
	now := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	pin := transport.Retained[0]
	if slot == "b" {
		pin = transport.Retained[1]
	}
	state, err := ApplyActivationCAS(cfg.ActiveSlotFile, ActivationCASRequest{Operation: ActivationOperationInit,
		GroupID: cfg.EdgeGroupID, ExpectedSlot: slot, TargetSlot: slot, InitialGeneration: generation,
		BundleGeneration: "prior.p21.r1", WorkerSourceCommit: pin.Source, WorkerImageDigest: pin.Image,
		Reason: "observed mixed production baseline"}, now)
	if err != nil {
		t.Fatal(err)
	}
	uid := func(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i) }
	j, err := (NodeWorkerJournal{Schema: NodeWorkerJournalSchema,
		Recipient: NativeRecipient{PodUID: uid(1), Node: cfg.EdgeID, Group: cfg.EdgeGroupID, Incarnation: "sha256:" + strings.Repeat("1", 64)},
		NodeUID:   uid(2), ServiceUID: uid(3), PredecessorWorkerUID: uid(4), CandidateWorkerUID: uid(5),
		PredecessorProcess: NodeWorkerProcessDigest("old-edge", "old-caddy"), CandidateProcess: NodeWorkerProcessDigest("new-edge", "new-caddy"), CandidateComponent: "test-candidate",
		Expected: state.Current, Candidate: ActivationRepairRelease{transport.Candidate.Slot, "current.p45.r1", transport.Candidate.Source, transport.Candidate.Image},
		ReleaseDigest: "sha256:" + strings.Repeat("a", 64), CreatedAt: now.Add(time.Second)}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	return cfg, j
}

func testNodeWorkerWrite(t *testing.T, cfg Config, j NodeWorkerJournal, op string, now time.Time) (any, error) {
	t.Helper()
	raw, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}
	return executeNodeWorkerWrite(cfg, j.Recipient, raw, NodeWorkerWrite{NodeWorkerWriteSchema, j.Recipient, j.Digest, op, now.Add(10 * time.Second)}, func() time.Time { return now })
}

func TestNodeWorkerAdvancesOnlyObservedMemberAndRecoversLostResponse(t *testing.T) {
	primary, plan := nodeWorkerFixture(t, "b", 912)
	secondary, secondaryPlan := nodeWorkerFixture(t, "b", 912)
	held, heldPlan := nodeWorkerFixture(t, "a", 913)
	now := plan.CreatedAt
	if _, err := testNodeWorkerWrite(t, primary, plan, "commit", now); err == nil {
		t.Fatal("accepted commit before publication preparation")
	}
	p, err := testNodeWorkerWrite(t, primary, plan, "prepare", now)
	if err != nil || plan.Progress(plan.Expected) != NodeWorkerPending {
		t.Fatalf("preparation failed: %v", err)
	}
	again, err := testNodeWorkerWrite(t, primary, plan, "prepare", now.Add(time.Second))
	if err != nil || again != p {
		t.Fatalf("prepare replay renewed or changed the grant: %v", err)
	}
	for cfg, want := range map[*Config]ActivationState{&primary: plan.Expected, &secondary: secondaryPlan.Expected, &held: heldPlan.Expected} {
		state, found, err := ReadActivationState(cfg.ActiveSlotFile)
		if err != nil || !found || !sameRepairActivation(state, want) {
			t.Fatalf("prepare changed a serving pointer: %+v err=%v", state, err)
		}
	}
	if _, err := testNodeWorkerWrite(t, primary, plan, "commit", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	// Discard the response as if the Guardian disconnected during exec.
	state, found, err := ReadActivationState(primary.ActiveSlotFile)
	if err != nil || !found || state.Generation != 913 || state.ActiveSlot != "a" || plan.Progress(state) != NodeWorkerActivated {
		t.Fatalf("fresh observations cannot recover the completed CAS: %+v err=%v", state, err)
	}
	if err := primary.prepareWorkerTransport(); err != nil {
		t.Fatal(err)
	}
	_, _, targets, selected := (&Service{}).selectedWorker(primary)
	if !selected || targets.HTTPSAddress != "127.0.0.1:38443" {
		t.Fatalf("new connections would reach the old A listener: %+v", targets)
	}
	for cfg, want := range map[*Config]ActivationState{&secondary: secondaryPlan.Expected, &held: heldPlan.Expected} {
		state, found, err := ReadActivationState(cfg.ActiveSlotFile)
		if err != nil || !found || !sameRepairActivation(state, want) {
			t.Fatalf("CAS changed a different member: %+v err=%v", state, err)
		}
	}
	if _, err := testNodeWorkerWrite(t, primary, plan, "commit", now.Add(3*time.Second)); !errors.Is(err, ErrActivationCASConflict) {
		t.Fatalf("blind replay did not conflict: %v", err)
	}
	state.Reason = "a separate actor changed the activation"
	if plan.Progress(state) != NodeWorkerConflict {
		t.Fatal("mistook a different write at the same generation for this journal")
	}
	if _, err := testNodeWorkerWrite(t, primary, plan, "rollback", now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	state, found, err = ReadActivationState(primary.ActiveSlotFile)
	if err != nil || !found || state.Generation != 914 || state.RollbackOfGeneration != 913 || plan.Progress(state) != NodeWorkerReverted {
		t.Fatalf("fresh observations cannot recover a lost compensation reply: %+v err=%v", state, err)
	}
	_, _, targets, selected = (&Service{}).selectedWorker(primary)
	if !selected || targets.HTTPSAddress != "127.0.0.1:28443" {
		t.Fatalf("compensation did not reselect the retained B listener: %+v", targets)
	}
	if _, err := testNodeWorkerWrite(t, primary, plan, "commit", now.Add(4*time.Second)); !errors.Is(err, ErrActivationCASConflict) {
		t.Fatalf("stale commit after compensation did not conflict: %v", err)
	}
}

func TestNodeWorkerFixedTransportPromotesAndRevertsExactRetainedWorker(t *testing.T) {
	for _, slot := range []string{"a", "b"} {
		t.Run(slot, func(t *testing.T) {
			cfg, j := nodeWorkerFixture(t, slot, 912)
			var transport workerTransportDeclaration
			if err := json.Unmarshal([]byte(cfg.WorkerTransport), &transport); err != nil {
				t.Fatal(err)
			}
			next := 0
			if slot == "a" {
				next = 1
			}
			pin := transport.Retained[next]
			j.FixedTransport, j.ProbeServiceUID = true, "00000000-0000-4000-8000-000000000006"
			j.Candidate.Slot, j.Candidate.WorkerSourceCommit, j.Candidate.WorkerImageDigest = pin.Slot, pin.Source, pin.Image
			j, err := j.Seal()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := testNodeWorkerWrite(t, cfg, j, "prepare", j.CreatedAt); err == nil {
				t.Fatal("accepted an overlap transport for a fixed-port journal")
			}
			transport.Schema, transport.Candidate, transport.HTTP, transport.HTTPS = "fugue.worker-transport-fixed/v1", workerImagePin{}, 0, 0
			raw, err := json.Marshal(transport)
			if err != nil {
				t.Fatal(err)
			}
			cfg.WorkerTransport = string(raw)
			for i, op := range []string{"prepare", "commit", "rollback"} {
				if _, err := testNodeWorkerWrite(t, cfg, j, op, j.CreatedAt.Add(time.Duration(i)*time.Second)); err != nil {
					t.Fatalf("%s: %v", op, err)
				}
				got, _, err := ReadActivationState(cfg.ActiveSlotFile)
				want := []NodeWorkerProgress{NodeWorkerPending, NodeWorkerActivated, NodeWorkerReverted}[i]
				if err != nil || j.Progress(got) != want {
					t.Fatalf("%s: got %+v, want %s (err %v)", op, got, want, err)
				}
			}
		})
	}
}

func TestNodeWorkerRejectsWrongJournalRecipientDeadlineOrPin(t *testing.T) {
	cfg, plan := nodeWorkerFixture(t, "b", 912)
	now := plan.CreatedAt
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	w := NodeWorkerWrite{NodeWorkerWriteSchema, plan.Recipient, plan.Digest, "prepare", now.Add(10 * time.Second)}
	for name, mutate := range map[string]func(*NodeWorkerWrite, *NativeRecipient, *Config) []byte{
		"another container incarnation": func(_ *NodeWorkerWrite, id *NativeRecipient, _ *Config) []byte {
			id.Incarnation = "sha256:" + strings.Repeat("b", 64)
			return raw
		},
		"missing projection": func(_ *NodeWorkerWrite, _ *NativeRecipient, _ *Config) []byte { return nil },
		"noncanonical": func(_ *NodeWorkerWrite, _ *NativeRecipient, _ *Config) []byte {
			return append(bytes.Clone(raw), '\n')
		},
		"expired": func(w *NodeWorkerWrite, _ *NativeRecipient, _ *Config) []byte { w.ExpiresAt = now; return raw },
		"excessive": func(w *NodeWorkerWrite, _ *NativeRecipient, _ *Config) []byte {
			w.ExpiresAt = now.Add(time.Minute)
			return raw
		},
		"changed candidate port": func(_ *NodeWorkerWrite, _ *NativeRecipient, c *Config) []byte {
			c.WorkerTransport = strings.Replace(c.WorkerTransport, "38443", "38081", 1)
			return raw
		},
		"foreign node": func(_ *NodeWorkerWrite, _ *NativeRecipient, c *Config) []byte { c.EdgeID = "other"; return raw },
	} {
		t.Run(name, func(t *testing.T) {
			write, id, c := w, plan.Recipient, cfg
			projection := mutate(&write, &id, &c)
			if _, err := executeNodeWorkerWrite(c, id, projection, write, func() time.Time { return now }); err == nil {
				t.Fatal("accepted malformed or unbound request")
			}
		})
	}
	if _, err := os.Stat(cfg.ActiveSlotFile + ".prepared"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected request changed preparation: %v", err)
	}
	if _, err := testNodeWorkerWrite(t, cfg, plan, "prepare", now); err != nil {
		t.Fatal(err)
	}
	w.Operation = "commit"
	n := 0
	if _, err := executeNodeWorkerWrite(cfg, plan.Recipient, raw, w, func() time.Time {
		n++
		if n == 3 {
			return w.ExpiresAt
		}
		return now
	}); err == nil {
		t.Fatal("expired executor reached atomic replacement")
	}
	if _, err := testNodeWorkerWrite(t, cfg, plan, "commit", now.Add(PublicationPreparationLifetime-5*time.Second)); err == nil {
		t.Fatal("commit consumed a preparation about to expire")
	}
	state, _, err := ReadActivationState(cfg.ActiveSlotFile)
	if err != nil || plan.Progress(state) != NodeWorkerPending {
		t.Fatalf("failed commit touched serving state: %+v err=%v", state, err)
	}
}
