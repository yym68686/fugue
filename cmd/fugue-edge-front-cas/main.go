// Command fugue-edge-front-cas performs one group-local, file-backed front
// activation CAS. It owns no rollout state machine and makes no Kubernetes or
// control-plane calls; the declarative release entry supplies every expected
// and target value and consumes the immutable JSON receipt.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"fugue/internal/edgegroupfront"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, time.Now().UTC()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer, now time.Time) error {
	if stdout == nil {
		return errors.New("stdout is nil")
	}
	flags := flag.NewFlagSet("fugue-edge-front-cas", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	stateFile := flags.String("state-file", "", "absolute group-local activation state file")
	groupID := flags.String("group", "", "exact edge group id")
	expectedGeneration := flags.Uint64("expected-generation", 0, "exact current activation generation")
	initialGeneration := flags.Uint64("initial-generation", 0, "verified existing Cell fence for a fresh activation file; zero defaults to one")
	expectedSlot := flags.String("expected-slot", "", "exact current slot")
	targetSlot := flags.String("target-slot", "", "target slot")
	bundleGeneration := flags.String("bundle-generation", "", "verified worker bundle generation")
	workerSourceCommit := flags.String("worker-source-commit", "", "exact 40-hex worker source commit")
	workerImageDigest := flags.String("worker-image-digest", "", "exact immutable worker image digest")
	operation := flags.String("operation", "", "initialize, promote, or rollback")
	rollbackOfGeneration := flags.Uint64("rollback-of-generation", 0, "exact failed generation for rollback")
	reason := flags.String("reason", "", "auditable activation reason")
	prepare := flags.Bool("prepare-publication", false, "prepare signed current publication without changing Front activation")
	requirePreparation := flags.Bool("require-publication-preparation", false, "require exact unexpired preparation before activation")
	repairRequest := flags.String("repair-request", "", "base64url of an exact short-lived journaled repair write")
	repairPrepare := flags.String("repair-prepare-request", "", "base64url of an exact short-lived journaled inactive Worker preparation")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("invalid edge front CAS arguments")
	}
	repairMode := ""
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "repair-request", "repair-prepare-request":
			repairMode = f.Name
		}
	})
	if repairMode != "" {
		if flags.NFlag() != 2 || *stateFile == "" {
			return errors.New("Front repair requires state-file and exactly one repair command")
		}
		if repairMode == "repair-prepare-request" {
			return runRepair(*stateFile, *repairPrepare, true, stdout)
		}
		return runRepair(*stateFile, *repairRequest, false, stdout)
	}
	request := edgegroupfront.ActivationCASRequest{
		RequirePublicationPreparation: *requirePreparation,
		GroupID:                       *groupID, ExpectedGeneration: *expectedGeneration, InitialGeneration: *initialGeneration, ExpectedSlot: *expectedSlot, TargetSlot: *targetSlot,
		BundleGeneration: *bundleGeneration, WorkerSourceCommit: *workerSourceCommit, WorkerImageDigest: *workerImageDigest,
		Operation: *operation, RollbackOfGeneration: *rollbackOfGeneration, Reason: *reason,
	}
	var receipt any
	var err error
	if *prepare {
		receipt, err = edgegroupfront.PreparePublication(*stateFile, request, now)
	} else {
		receipt, err = edgegroupfront.ApplyActivationCAS(*stateFile, request, now)
	}
	if err != nil {
		return err
	}
	return writeReceipt(stdout, receipt)
}

func runRepair(path, encoded string, prepare bool, stdout io.Writer) error {
	if len(encoded) == 0 || len(encoded) > 16<<10 {
		return errors.New("Front repair request size is invalid")
	}
	for _, c := range encoded {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return errors.New("Front repair request encoding is invalid")
		}
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return errors.New("Front repair request encoding is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var write edgegroupfront.ActivationRepairWrite
	if decoder.Decode(&write) != nil || decoder.Decode(&struct{}{}) != io.EOF || write.Schema != edgegroupfront.ActivationRepairWriteSchemaV1 {
		return errors.New("Front repair request schema is invalid")
	}
	if prepare {
		receipt, err := edgegroupfront.PrepareActivationRepairPublication(path, write.Request, write.ExpiresAt)
		if err != nil {
			return err
		}
		return writeReceipt(stdout, receipt)
	}
	receipt, err := edgegroupfront.ApplyActivationRepairCAS(path, write.Request, write.ExpiresAt)
	if err != nil {
		return err
	}
	return writeReceipt(stdout, receipt)
}

func writeReceipt(stdout io.Writer, receipt any) error {
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(receipt)
}
