package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupusage"
	"fugue/internal/model"
)

func TestRenderBackupUsageSeparatesBillableAndPhysicalBytes(t *testing.T) {
	physicalBytes := int64(248)
	physicalObjects := 5
	usage := backupusage.Usage{
		Provider:            model.DataBackendProviderCloudflareR2,
		BillableBytes:       100,
		PhysicalBytes:       &physicalBytes,
		PhysicalObjectCount: &physicalObjects,
		MarkupPercent:       5,
		EffectiveMultiplier: 1.05,
		Reconciliation: &backupusage.Reconciliation{
			Status:                   backupusage.ReconciliationStatusDrift,
			BackendCount:             1,
			MeasuredBackendCount:     1,
			ReferencedBytes:          178,
			UnreferencedBytes:        70,
			OrphanedBytes:            70,
			OrphanedObjectCount:      1,
			MissingActiveObjectCount: 0,
			SizeMismatchCount:        2,
			ObservedAt:               time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC),
			Message:                  "R2 physical inventory differs from durable backup metadata",
		},
	}
	var output bytes.Buffer
	if err := renderBackupUsage(&output, usage); err != nil {
		t.Fatalf("render backup usage: %v", err)
	}
	for _, want := range []string{
		"billable_bytes: 100",
		"physical_bytes: 248",
		"physical_object_count: 5",
		"reconciliation_status: drift",
		"orphaned_bytes: 70",
		"orphaned_objects: 1",
		"size_mismatches: 2",
		"reconciled_at: 2026-07-30T12:00:00Z",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("backup usage output missing %q:\n%s", want, output.String())
		}
	}
}

func TestRenderBackupUsageDoesNotRenderUnavailablePhysicalBytesAsZero(t *testing.T) {
	usage := backupusage.Usage{
		Provider:            model.DataBackendProviderCloudflareR2,
		MarkupPercent:       5,
		EffectiveMultiplier: 1.05,
		Reconciliation: &backupusage.Reconciliation{
			Status:               backupusage.ReconciliationStatusUnavailable,
			BackendCount:         1,
			MeasuredBackendCount: 0,
			ObservedAt:           time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC),
		},
	}
	var output bytes.Buffer
	if err := renderBackupUsage(&output, usage); err != nil {
		t.Fatalf("render backup usage: %v", err)
	}
	if !strings.Contains(output.String(), "physical_bytes: unavailable") || !strings.Contains(output.String(), "physical_object_count: unavailable") {
		t.Fatalf("unavailable physical measurement was not explicit:\n%s", output.String())
	}
}
