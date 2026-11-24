package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dr "fugue/internal/declarativerelease"
	st "fugue/internal/edgegroupfront"
)

func TestProjectedNativeCommandReportsLostWriteWithoutRepeatingCutover(t *testing.T) {
	dir := t.TempDir()
	state, journal := filepath.Join(dir, "activation.json"), filepath.Join(dir, "journal.json")
	now := time.Now().UTC()
	canonical := func(v any) []byte {
		t.Helper()
		b, err := dr.CanonicalJSON(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	digest := func(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }
	const group = "edge-group-example"
	release := func(slot string) st.ActivationRepairRelease {
		return st.ActivationRepairRelease{Slot: slot, BundleGeneration: "bundle-" + slot, WorkerSourceCommit: strings.Repeat(slot, 40), WorkerImageDigest: "sha256:" + strings.Repeat(slot, 64)}
	}
	a, b := release("a"), release("b")
	init := st.ActivationCASRequest{GroupID: group, ExpectedSlot: "a", TargetSlot: "a", BundleGeneration: a.BundleGeneration, WorkerSourceCommit: a.WorkerSourceCommit, WorkerImageDigest: a.WorkerImageDigest, Operation: st.ActivationOperationInit, Reason: "fixture initial Front"}
	if _, err := st.ApplyActivationCAS(state, init, now.Add(-2*time.Second)); err != nil {
		t.Fatal(err)
	}
	promote := init
	promote.TargetSlot, promote.Operation, promote.ExpectedGeneration = "b", st.ActivationOperationPromote, 1
	promote.BundleGeneration, promote.WorkerImageDigest, promote.WorkerSourceCommit = b.BundleGeneration, b.WorkerImageDigest, b.WorkerSourceCommit
	prior, err := st.ApplyActivationCAS(state, promote, now.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	r := st.ActivationRepairCASRequest{Expected: st.ActivationState(prior.Current), Committed: b, Previous: a, CommittedGeneration: 2}
	before := map[string]any{"GroupID": group, "CurrentFrontGeneration": 2, "PreviousFrontGeneration": 1, "CurrentWorkerSlot": b.Slot, "CurrentBundleGeneration": b.BundleGeneration, "CurrentWorkerSourceSHA": b.WorkerSourceCommit, "CurrentWorkerImageDigest": b.WorkerImageDigest, "PreviousWorkerSlot": a.Slot, "PreviousBundleGeneration": a.BundleGeneration, "PreviousWorkerSourceSHA": a.WorkerSourceCommit, "PreviousWorkerImageDigest": a.WorkerImageDigest}
	var members []any
	fp := digest([]byte("immutable release"))
	scope := st.NativeJournalScope{Schema: st.NativeJournalSchema, Authorization: fp, Targets: map[string]st.NativePermit{}}
	reqDigest, err := st.NativeRequestDigest(r)
	if err != nil {
		t.Fatal(err)
	}
	for i, node := range []string{"edge-a", "edge-b"} {
		id := st.NativeRecipient{Node: node, Group: group, PodUID: fmt.Sprintf("00000000-0000-0000-0000-%012d", i), Incarnation: digest([]byte(node))}
		scope.Targets[node] = st.NativePermit{Recipient: id, RequestDigest: reqDigest}
		members = append(members, map[string]any{"Node": map[string]string{"Name": node, "UID": node + "-uid"}, "FrontPodUID": node + "-front", "TargetWorkerPodUID": node + "-worker", "Activation": r.Expected})
	}
	plan := map[string]any{"Before": before, "Members": members, "CurrentUID": "current", "CurrentResourceVersion": "8", "TargetGeneration": 4, "Digest": ""}
	plan["Digest"] = digest(canonical(plan))
	j := map[string]any{"Schema": "edge-front-authority-repair/v1", "Plan": plan, "Ordinary": scope, "CreatedAt": now, "AuthorizationDigest": fp, "Digest": ""}
	r.JournalDigest = digest(canonical(j))
	j["Digest"] = r.JournalDigest
	projected := canonical(j)
	id := scope.Targets["edge-a"].Recipient
	identify := func() (st.NativeRecipient, error) { return id, nil }
	w := st.NativeWrite{Schema: st.NativeWriteSchema, Recipient: id, Operation: "commit", Write: st.ActivationRepairWrite{Schema: st.ActivationRepairWriteSchemaV1, Request: r, ExpiresAt: now.Add(12 * time.Second)}}
	var out bytes.Buffer
	run := func() error {
		raw, e := json.Marshal(w)
		if e != nil {
			t.Fatal(e)
		}
		out.Reset()
		return runNativeWorkerAt([]string{"--worker-convergence", base64.RawURLEncoding.EncodeToString(raw)}, &out, identify, journal, state)
	}
	if run() == nil {
		t.Fatal("absent projection allowed a write")
	}
	if err := os.WriteFile(journal, projected, 0444); err != nil {
		t.Fatal(err)
	}
	if err := runNativeWorkerAt([]string{"--worker-journal"}, &out, identify, journal, state); err != nil {
		t.Fatal(err)
	}
	var ack st.NativeProjection
	if err := json.Unmarshal(out.Bytes(), &ack); err != nil || ack.Recipient != id || ack.Digest != digest(projected) {
		t.Fatal("wrong projected content or process")
	}
	id.Incarnation = digest([]byte("restarted"))
	if run() == nil {
		t.Fatal("restarted process consumed original command")
	}
	id = w.Recipient
	if current, _, e := st.ReadActivationState(state); e != nil || current != prior.Current {
		t.Fatal("preflight changed serving state", e)
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	out.Reset() // simulate a lost exec reply: resolve from the durable file
	current, ok, err := st.ReadActivationState(state)
	if err != nil || !ok || !r.MatchesActivation(st.ActivationState(current)) || current.Generation != 4 || current.ActiveSlot != prior.Current.ActiveSlot {
		t.Fatal("wrong durable postcondition", current, err)
	}
	if run() == nil {
		t.Fatal("repeated CAS wrote again")
	}
	if after, _, e := st.ReadActivationState(state); e != nil || after != current {
		t.Fatal("replay changed durable receipt", e)
	}
}

func TestNodeWorkerCommandBindsFixedProjectionAndRecipient(t *testing.T) {
	dir := t.TempDir()
	state, journal := filepath.Join(dir, "activation.json"), filepath.Join(dir, "journal.json")
	now := time.Now().UTC()
	const group = "edge-group-example"
	digest := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }
	source := func(c string) string { return strings.Repeat(c, 40) }
	uid := func(i int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", i) }
	previous, err := st.ApplyActivationCAS(state, st.ActivationCASRequest{
		Operation: st.ActivationOperationInit, GroupID: group, ExpectedSlot: "b", TargetSlot: "b",
		InitialGeneration: 912, BundleGeneration: "fixture.p10.r1", WorkerSourceCommit: source("b"),
		WorkerImageDigest: digest("b"), Reason: "verified fixture predecessor",
	}, now.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	id := st.NativeRecipient{PodUID: uid(1), Node: "edge-pilot", Group: group, Incarnation: digest("f")}
	j, err := (st.NodeWorkerJournal{
		Schema: st.NodeWorkerJournalSchema, Recipient: id, NodeUID: uid(2), ServiceUID: uid(3),
		PredecessorWorkerUID: uid(4), CandidateWorkerUID: uid(5), Expected: previous.Current,
		PredecessorProcess: digest("a"), CandidateProcess: digest("c"), CandidateComponent: "test-candidate",
		Candidate:     st.ActivationRepairRelease{Slot: "a", BundleGeneration: "fixture.p11.r1", WorkerSourceCommit: source("c"), WorkerImageDigest: digest("c")},
		ReleaseDigest: digest("d"), CreatedAt: now,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	cfg := st.Config{NodeHost: "127.0.0.1", EdgeID: id.Node, EdgeGroupID: group, ActiveSlotFile: state, RequireActivationState: true,
		Slots:           map[string]st.SlotTargets{"a": {HTTPAddress: "127.0.0.1:18080", HTTPSAddress: "127.0.0.1:18443"}, "b": {HTTPAddress: "127.0.0.1:28080", HTTPSAddress: "127.0.0.1:28443"}},
		WorkerTransport: fmt.Sprintf(`{"schema":"fugue.worker-transport/v1","group":%q,"node":%q,"retained":[{"slot":"a","source":%q,"image":%q},{"slot":"b","source":%q,"image":%q}],"candidate":{"slot":"a","source":%q,"image":%q},"candidate_http_port":38080,"candidate_https_port":38443}`, group, id.Node, source("a"), digest("a"), source("b"), digest("b"), source("c"), digest("c")),
	}
	encode := func(v any) []byte {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	var out bytes.Buffer
	w := st.NodeWorkerWrite{Schema: st.NodeWorkerWriteSchema, Recipient: id, JournalDigest: j.Digest, Operation: "prepare", ExpiresAt: now.Add(10 * time.Second)}
	run := func(body []byte) error {
		out.Reset()
		return runNativeNodeWorker([]string{"--node-worker-advance", base64.RawURLEncoding.EncodeToString(body)}, &out, func() (st.NativeRecipient, error) { return id, nil }, cfg, journal)
	}
	if err := run(encode(w)); err == nil {
		t.Fatal("CLI wrote without a projection")
	}
	if err := os.WriteFile(journal, encode(j), 0444); err != nil {
		t.Fatal(err)
	}
	if err := run(append(encode(w), ' ')); err == nil {
		t.Fatal("CLI accepted a noncanonical envelope")
	}
	id.Incarnation = digest("e")
	if err := run(encode(w)); err == nil {
		t.Fatal("CLI accepted a different process incarnation")
	}
	id = j.Recipient
	if err := run(encode(w)); err != nil {
		t.Fatal(err)
	}
	var prep st.PublicationPreparation
	if err := json.Unmarshal(out.Bytes(), &prep); err != nil || prep.WorkerImageDigest != digest("c") {
		t.Fatalf("wrong preparation receipt: %+v err=%v", prep, err)
	}
	w.Operation = "commit"
	if err := run(encode(w)); err != nil {
		t.Fatal(err)
	}
	current, _, err := st.ReadActivationState(state)
	if err != nil || j.Progress(current) != st.NodeWorkerActivated {
		t.Fatalf("native write is not recoverable: %+v err=%v", current, err)
	}
}
