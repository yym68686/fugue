package main

import (
	"bytes"
	"encoding/json"
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
