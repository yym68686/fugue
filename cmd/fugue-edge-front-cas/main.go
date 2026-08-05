// Command fugue-edge-front-cas performs one group-local, file-backed front
// activation CAS. It owns no rollout state machine and makes no Kubernetes or
// control-plane calls; the declarative release entry supplies every expected
// and target value and consumes the immutable JSON receipt.
package main

import (
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
	expectedSlot := flags.String("expected-slot", "", "exact current slot")
	targetSlot := flags.String("target-slot", "", "target slot")
	bundleGeneration := flags.String("bundle-generation", "", "verified worker bundle generation")
	workerSourceCommit := flags.String("worker-source-commit", "", "exact 40-hex worker source commit")
	workerImageDigest := flags.String("worker-image-digest", "", "exact immutable worker image digest")
	operation := flags.String("operation", "", "initialize, promote, or rollback")
	rollbackOfGeneration := flags.Uint64("rollback-of-generation", 0, "exact failed generation for rollback")
	reason := flags.String("reason", "", "auditable activation reason")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("invalid edge front CAS arguments")
	}
	receipt, err := edgegroupfront.ApplyActivationCAS(*stateFile, edgegroupfront.ActivationCASRequest{
		GroupID: *groupID, ExpectedGeneration: *expectedGeneration, ExpectedSlot: *expectedSlot, TargetSlot: *targetSlot,
		BundleGeneration: *bundleGeneration, WorkerSourceCommit: *workerSourceCommit, WorkerImageDigest: *workerImageDigest,
		Operation: *operation, RollbackOfGeneration: *rollbackOfGeneration, Reason: *reason,
	}, now)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(receipt)
}
