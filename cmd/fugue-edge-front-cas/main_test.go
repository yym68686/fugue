package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
)

func TestRunEmitsBoundedActivationReceipt(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "activation.json")
	args := []string{
		"--state-file", stateFile,
		"--group", "edge-group-country-de",
		"--expected-generation", "0",
		"--expected-slot", "a",
		"--target-slot", "a",
		"--bundle-generation", "bundle-de-a",
		"--worker-source-commit", "0123456789abcdef0123456789abcdef01234567",
		"--worker-image-digest", "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"--operation", "initialize",
		"--reason", "initialize DE front authority",
	}
	var output bytes.Buffer
	if err := run(args, &output, time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	var receipt edgegroupfront.ActivationReceipt
	if err := json.Unmarshal(output.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Schema != edgegroupfront.ActivationReceiptSchemaV1 || receipt.GroupID != "edge-group-country-de" || receipt.Current.Generation != 1 || receipt.Current.ActiveSlot != "a" {
		t.Fatalf("unexpected command receipt: %+v", receipt)
	}
}

func TestRunPreparesPublicationWithoutActivatingTraffic(t *testing.T) {
	now := time.Now().UTC()
	path := filepath.Join(t.TempDir(), "activation.json")
	args := []string{"--state-file", path, "--group", "edge-group-fixture", "--expected-generation", "0", "--expected-slot", "a", "--target-slot", "a", "--bundle-generation", "old.p1.r0", "--worker-source-commit", "0123456789abcdef0123456789abcdef01234567", "--worker-image-digest", "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "--operation", "initialize", "--reason", "initialize publication preparation fixture"}
	if err := run(args, &bytes.Buffer{}, now); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	for i, a := range args {
		switch a {
		case "--expected-generation":
			args[i+1] = "1"
		case "--target-slot":
			args[i+1] = "b"
		case "--bundle-generation":
			args[i+1] = "next.p2.r4"
		case "--operation":
			args[i+1] = "promote"
		}
	}
	guarded := append(append([]string{}, args...), "--require-publication-preparation")
	if err := run(guarded, &bytes.Buffer{}, now); err == nil {
		t.Fatal("guarded CLI activated without preparation")
	}
	var out bytes.Buffer
	if err := run(append(append([]string{}, args...), "--prepare-publication"), &out, now); err != nil {
		t.Fatal(err)
	}
	var p edgegroupfront.PublicationPreparation
	if json.Unmarshal(out.Bytes(), &p) != nil || p.Schema != edgegroupfront.PublicationPreparationSchema || p.TargetSlot != "b" {
		t.Fatal("invalid preparation receipt")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("prepare CLI changed public traffic")
	}
	if err := run(guarded, &bytes.Buffer{}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	s, _, err := edgegroupfront.ReadActivationState(path)
	if err != nil || s.ActiveSlot != "b" {
		t.Fatal("prepared activation failed")
	}
}

func TestRunInitializesAnExplicitExistingCellFence(t *testing.T) {
	args := []string{"--state-file", filepath.Join(t.TempDir(), "activation.json"), "--group", "cell-member-test", "--expected-generation", "0", "--initial-generation", "9", "--expected-slot", "a", "--target-slot", "a", "--bundle-generation", "verified-private-bundle", "--worker-source-commit", "0123456789abcdef0123456789abcdef01234567", "--worker-image-digest", "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "--operation", "initialize", "--reason", "verified private member enrollment"}
	var output bytes.Buffer
	if err := run(args, &output, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var receipt edgegroupfront.ActivationReceipt
	if err := json.Unmarshal(output.Bytes(), &receipt); err != nil || receipt.Current.Generation != 9 || receipt.PreviousExists {
		t.Fatal("explicit initial fence not reflected in receipt", receipt, err)
	}
}

func TestRunRepairRequiresExactBoundedWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activation.json")
	now := time.Now().UTC()
	release := func(slot string) edgegroupfront.ActivationRepairRelease {
		return edgegroupfront.ActivationRepairRelease{Slot: slot, BundleGeneration: "fixture-" + slot, WorkerSourceCommit: strings.Repeat(slot, 40), WorkerImageDigest: "sha256:" + strings.Repeat(slot, 64)}
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
	write := edgegroupfront.ActivationRepairWrite{Schema: edgegroupfront.ActivationRepairWriteSchemaV1, ExpiresAt: now.Add(-time.Second),
		Request: edgegroupfront.ActivationRepairCASRequest{JournalDigest: "sha256:" + strings.Repeat("d", 64), Expected: activated.Current,
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
	if err := run(args(""), &bytes.Buffer{}, now); err == nil {
		t.Fatal("expired repair was not rejected")
	}
	write.ExpiresAt = time.Now().Add(10 * time.Second)
	if err := run(args(`,"unknown":true}`), &bytes.Buffer{}, now); err == nil {
		t.Fatal("unknown repair request data was not rejected")
	}
	if err := run(append(args(""), "--group", "edge-other"), &bytes.Buffer{}, now); err == nil {
		t.Fatal("repair mode accepted legacy activation flags")
	}
	if state, _, err := edgegroupfront.ReadActivationState(path); err != nil || state != activated.Current {
		t.Fatalf("invalid request changed Front: state=%+v err=%v", state, err)
	}
	var result bytes.Buffer
	if err := run(args(""), &result, now); err != nil {
		t.Fatal(err)
	}
	var receipt edgegroupfront.ActivationReceipt
	if err := json.Unmarshal(result.Bytes(), &receipt); err != nil || !write.Request.MatchesActivation(receipt.Current) || receipt.Current.ActiveSlot != activated.Current.ActiveSlot {
		t.Fatalf("repair did not return an exact no-cutover rebase receipt: %+v err=%v", receipt, err)
	}
	if err := run(args(""), &result, now); err == nil {
		t.Fatal("repair command accepted replay of an old predecessor")
	}
}
