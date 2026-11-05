package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// ErrCompilerInputArchiveRetirementSafetyUnavailable means that replacement
// storage alone cannot prove a safe retirement. There is deliberately no flag,
// environment opt-in or operator-supplied boolean that bypasses this gate.
var ErrCompilerInputArchiveRetirementSafetyUnavailable = fmt.Errorf("compiler input retirement requires enforced hot dependency, reactivation and reader safety proofs: %w", ErrConflict)

func restrictCompilerInputArchiveRetirement(report CompilerInputArchiveRetirementReport) CompilerInputArchiveRetirementReport {
	// The internal storage plan checks complete replacements and writer fences.
	// Preserve that narrower result without describing it as permission to
	// remove the original relation or as already reclaimed disk space.
	report.ReplacementCoverageComplete = report.PlanSHA256 != "" && report.InputCount == report.CoveredInputCount && report.BlockedInputCount == 0 && report.OrphanChunkRows == 0
	report.CanRetire = false
	report.Applied = false
	report.ReclaimableRelationBytes = 0
	report.SafetyBlockers = []string{
		"full-hot-dependency-closure-and-local-readback-not-enforced",
		"persistent-reactivation-admission-not-enforced",
		"current-and-rollback-reader-compatibility-not-proven",
	}
	if report.PlanSHA256 != "" {
		// A blocked public plan cannot reuse the hash of the narrower storage
		// plan. This binds the missing prerequisites to the reviewable result.
		binding := struct {
			Schema      string   `json:"schema"`
			StoragePlan string   `json:"storage_plan_sha256"`
			Blockers    []string `json:"safety_blockers"`
		}{"fugue.compiler-input-retirement-readiness/v1", report.PlanSHA256, report.SafetyBlockers}
		encoded, _ := json.Marshal(binding)
		sum := sha256.Sum256(encoded)
		report.PlanSHA256 = hex.EncodeToString(sum[:])
	}
	return report
}
