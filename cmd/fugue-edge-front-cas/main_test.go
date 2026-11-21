package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
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
