package api

import (
	"slices"

	"fugue/internal/platformconfig"
)

const runtimePreviewSummarySchema = "fugue.runtime-preview-summary/v1"
const runtimePreviewOmittedPhysicalEvidence = "runtime_snapshot.dns_selections[].physical_evidence"

type platformRuntimePreviewSummary struct {
	Schema                       string `json:"schema"`
	FullSnapshotDigest           string `json:"full_snapshot_digest"`
	OmittedPhysicalEvidenceCount int    `json:"omitted_physical_evidence_count"`
	OmittedPhysicalEvidenceBytes int64  `json:"omitted_physical_evidence_bytes"`
}

// A projection of a completed strict capture, never an alternative collector.
// The producer and compiler continue consuming the complete runtime snapshot.
func summarizeRuntimePreview(result *platformIntentProjectionResponse) error {
	digest, err := platformconfig.RuntimeSnapshotDigest(result.RuntimeSnapshot)
	if err != nil {
		return err
	}
	summary := &platformRuntimePreviewSummary{Schema: runtimePreviewSummarySchema, FullSnapshotDigest: digest}
	// A capture may share observations with immutable caches. Only change the
	// response's own slice, leaving the original evidence bytes and facts intact.
	result.RuntimeSnapshot.DNSSelections = slices.Clone(result.RuntimeSnapshot.DNSSelections)
	for i := range result.RuntimeSnapshot.DNSSelections {
		fact := &result.RuntimeSnapshot.DNSSelections[i]
		if len(fact.PhysicalEvidence) > 0 {
			summary.OmittedPhysicalEvidenceCount++
			summary.OmittedPhysicalEvidenceBytes += int64(len(fact.PhysicalEvidence))
			fact.PhysicalEvidence = nil
		}
	}
	result.RuntimeSummary = summary
	result.MigrationReady = false
	result.OmittedRuntimeFields = append(slices.Clone(result.OmittedRuntimeFields), runtimePreviewOmittedPhysicalEvidence)
	return nil
}
