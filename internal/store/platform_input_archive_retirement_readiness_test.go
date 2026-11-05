package store

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestCompilerInputArchiveRetirementRejectsApplyBeforeDatabaseOrObjectAccess(t *testing.T) {
	options := CompilerInputArchiveRetirementOptions{
		Apply: true, ExpectedPlanSHA256: strings.Repeat("a", 64),
		CurrentReaderRelease: strings.Repeat("b", 40), RollbackReaderRelease: strings.Repeat("c", 40),
		ArchiveRetentionPolicySHA256: strings.Repeat("d", 64),
	}
	// An intentionally invalid DSN would fail differently if the function
	// attempted a connection. Caller attestations must never enable TRUNCATE.
	report, err := RunCompilerInputArchiveRetirement(t.Context(), "invalid-database-address", options)
	if !errors.Is(err, ErrCompilerInputArchiveRetirementSafetyUnavailable) || !errors.Is(err, ErrConflict) || report.Applied || report.CanRetire || report.PlanSHA256 != "" || len(report.SafetyBlockers) != 3 {
		t.Fatal("retirement bypassed independent safety prerequisites", report, err)
	}
	options.ExpectedPlanSHA256 = ""
	if _, err = RunCompilerInputArchiveRetirement(t.Context(), "invalid-database-address", options); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("invalid apply syntax lost its distinct refusal", err)
	}
}

func TestCompilerInputArchiveRetirementCoverageDoesNotConferAuthority(t *testing.T) {
	storage := CompilerInputArchiveRetirementReport{
		CanRetire: true, Applied: true, PlanSHA256: strings.Repeat("a", 64),
		InputCount: 1, CoveredInputCount: 1, ReclaimableRelationBytes: 4096,
		LegacyTotalRelationBytes: 4096, ReclaimableLogicalBytes: 123,
		Inputs: []CompilerInputArchiveRetirementInput{{Digest: "sha256:" + strings.Repeat("b", 64), ReplacementVerified: true}},
	}
	got := restrictCompilerInputArchiveRetirement(storage)
	if got.CanRetire || got.Applied || got.ReclaimableRelationBytes != 0 || !got.ReplacementCoverageComplete || len(got.SafetyBlockers) != 3 || got.PlanSHA256 == storage.PlanSHA256 || len(got.PlanSHA256) != 64 {
		t.Fatal("replacement receipts were promoted to destructive authority", got)
	}
	if got.LegacyTotalRelationBytes != storage.LegacyTotalRelationBytes || got.ReclaimableLogicalBytes != storage.ReclaimableLogicalBytes || !reflect.DeepEqual(got.Inputs, storage.Inputs) {
		t.Fatal("refusal discarded replacement/size evidence")
	}
	if other := restrictCompilerInputArchiveRetirement(storage); !reflect.DeepEqual(got, other) {
		t.Fatal("readiness binding is not deterministic")
	}
	storage.CanRetire = false // a missing write fence does not undo verified coverage
	if other := restrictCompilerInputArchiveRetirement(storage); !other.ReplacementCoverageComplete || other.CanRetire {
		t.Fatal("replacement coverage was conflated with other retirement gates")
	}
	storage.PlanSHA256 = strings.Repeat("c", 64)
	if other := restrictCompilerInputArchiveRetirement(storage); other.PlanSHA256 == got.PlanSHA256 {
		t.Fatal("different storage inventories share a readiness binding")
	}
}
