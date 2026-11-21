package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/frontrepairstate"
)

func TestRunRepairRequiresExactBoundedWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activation.json")
	now := time.Now().UTC()
	release := func(slot string) frontrepairstate.ActivationRepairRelease {
		return frontrepairstate.ActivationRepairRelease{Slot: slot, BundleGeneration: "fixture-" + slot, WorkerSourceCommit: strings.Repeat(slot, 40), WorkerImageDigest: "sha256:" + strings.Repeat(slot, 64)}
	}
	previous, committed := release("a"), release("b")
	init := edgegroupfront.ActivationCASRequest{GroupID: "edge-group-fixture", ExpectedSlot: "a", TargetSlot: "a", BundleGeneration: previous.BundleGeneration,
		WorkerSourceCommit: previous.WorkerSourceCommit, WorkerImageDigest: previous.WorkerImageDigest, Operation: edgegroupfront.ActivationOperationInit, Reason: "fixture initial Front"}
	if _, err := edgegroupfront.ApplyActivationCAS(path, init, now.Add(-2*time.Second)); err != nil {
		t.Fatal(err)
	}
	promote := init
	promote.Operation, promote.ExpectedGeneration, promote.TargetSlot = edgegroupfront.ActivationOperationPromote, 1, "b"
	promote.BundleGeneration, promote.WorkerSourceCommit, promote.WorkerImageDigest = committed.BundleGeneration, committed.WorkerSourceCommit, committed.WorkerImageDigest
	activated, err := edgegroupfront.ApplyActivationCAS(path, promote, now.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	write := frontrepairstate.ActivationRepairWrite{Schema: frontrepairstate.ActivationRepairWriteSchemaV1, ExpiresAt: now.Add(-time.Second),
		Request: frontrepairstate.ActivationRepairCASRequest{JournalDigest: "sha256:" + strings.Repeat("d", 64), Expected: frontrepairstate.ActivationState(activated.Current),
			CommittedGeneration: 2, Committed: committed, Previous: previous}}
	args := func(extraJSON string) []string {
		t.Helper()
		raw, err := json.Marshal(write)
		if err != nil {
			t.Fatal(err)
		}
		if extraJSON != "" {
			raw = append(raw[:len(raw)-1], extraJSON...)
		}
		return []string{"--state-file", path, "--repair-request", base64.RawURLEncoding.EncodeToString(raw)}
	}
	if err := run(args(""), &bytes.Buffer{}); err == nil {
		t.Fatal("expired repair was not rejected")
	}
	if err := run(append(args(""), "--repair-prepare-request", "invalid"), &bytes.Buffer{}); err == nil {
		t.Fatal("combined repair modes were not rejected")
	}
	write.ExpiresAt = time.Now().Add(10 * time.Second)
	if err := run(args(`,"unknown":true}`), &bytes.Buffer{}); err == nil {
		t.Fatal("unknown repair request data was not rejected")
	}
	if err := run(append(args(""), "--group", "edge-other"), &bytes.Buffer{}); err == nil {
		t.Fatal("repair mode accepted legacy activation flags")
	}
	if state, _, err := edgegroupfront.ReadActivationState(path); err != nil || state != activated.Current {
		t.Fatalf("invalid request changed Front: state=%+v err=%v", state, err)
	}
	var result bytes.Buffer
	if err := run(args(""), &result); err != nil {
		t.Fatal(err)
	}
	var receipt edgegroupfront.ActivationReceipt
	if err := json.Unmarshal(result.Bytes(), &receipt); err != nil || !write.Request.MatchesActivation(frontrepairstate.ActivationState(receipt.Current)) || receipt.Current.ActiveSlot != activated.Current.ActiveSlot {
		t.Fatalf("repair did not return an exact no-cutover rebase receipt: %+v err=%v", receipt, err)
	}
	if err := run(args(""), &result); err == nil {
		t.Fatal("repair command accepted replay of an old predecessor")
	}
}
