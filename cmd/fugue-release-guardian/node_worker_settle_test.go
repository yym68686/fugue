package main

import (
	"testing"

	"fugue/internal/productioncontrol"
)

func TestNodeWorkerSettlementBindsBothLiveReadbacksAndResult(t *testing.T) {
	intent := productioncontrol.Intent{Generation: 3, CurrentDigest: "current", TargetDigest: "candidate", RollbackDigest: "rollback"}
	for _, tc := range []struct {
		label   string
		outcome productioncontrol.Outcome
		version productioncontrol.Version
	}{
		{"verified", productioncontrol.Applied, productioncontrol.Version{Generation: 4, Digest: "candidate"}},
		{"reverted", productioncontrol.Restored, productioncontrol.Version{Generation: 4, Digest: "rollback"}},
		{"expired_without_cutover", productioncontrol.NotApplied, productioncontrol.Version{Generation: 3, Digest: "current"}},
	} {
		t.Run(tc.label, func(t *testing.T) {
			result, err := terminalFrontWorkerVersion(intent, tc.label)
			if err != nil || result.Outcome != tc.outcome || result.Verified != tc.version {
				t.Fatalf("wrong verified head: %+v, %v", result, err)
			}
			checkpoint := nodeWorkerCheckpoint{Result: result, ObservationDigest: "serving", AuthorityDigest: "closed"}
			witness := productioncontrol.ExecutionWitness{
				Outcome: tc.outcome, VerifiedDigest: tc.version.Digest, Generation: tc.version.Generation,
				ObservationDigest: checkpoint.ObservationDigest, AuthorityDigest: checkpoint.AuthorityDigest,
			}
			if err := checkNodeWorkerTerminalWitness(witness, checkpoint); err != nil {
				t.Fatal(err)
			}
			changed := witness
			changed.ObservationDigest = "old-serving"
			if err := checkNodeWorkerTerminalWitness(changed, checkpoint); err == nil {
				t.Fatal("accepted changed serving readback")
			}
			changed = witness
			changed.AuthorityDigest = "open"
			if err := checkNodeWorkerTerminalWitness(changed, checkpoint); err == nil {
				t.Fatal("accepted changed write authority readback")
			}
			changed = witness
			changed.Generation++
			if err := checkNodeWorkerTerminalWitness(changed, checkpoint); err == nil {
				t.Fatal("accepted a different verified version")
			}
		})
	}
	if _, err := terminalFrontWorkerVersion(intent, "unknown"); err == nil {
		t.Fatal("unknown result was classified as not applied")
	}
}
