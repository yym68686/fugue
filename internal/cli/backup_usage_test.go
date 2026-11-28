package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	backupusagecontract "fugue/internal/backupusage/contract"
	"fugue/internal/model"
)

func TestGetBackupUsageDecodesWireContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/backups/usage" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"provider":"cloudflare_r2","billable_bytes":19,"physical_bytes":0,"physical_object_count":0,"reconciliation":{"status":"complete","backend_count":1}}}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	usage, err := client.GetBackupUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage.BillableBytes != 19 || usage.PhysicalBytes == nil || *usage.PhysicalBytes != 0 ||
		usage.PhysicalObjectCount == nil || *usage.PhysicalObjectCount != 0 ||
		usage.Reconciliation == nil || usage.Reconciliation.Status != backupusagecontract.ReconciliationStatusComplete ||
		usage.Reconciliation.BackendCount != 1 {
		t.Fatalf("unexpected decoded usage: %+v", usage)
	}
}

func TestRenderBackupUsageSeparatesBillableAndPhysicalBytes(t *testing.T) {
	physicalBytes := int64(248)
	physicalObjects := 5
	usage := backupusagecontract.Usage{
		Provider:            model.DataBackendProviderCloudflareR2,
		BillableBytes:       100,
		PhysicalBytes:       &physicalBytes,
		PhysicalObjectCount: &physicalObjects,
		MarkupPercent:       5,
		EffectiveMultiplier: 1.05,
		Reconciliation: &backupusagecontract.Reconciliation{
			Status:                   backupusagecontract.ReconciliationStatusDrift,
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
	usage := backupusagecontract.Usage{
		Provider:            model.DataBackendProviderCloudflareR2,
		MarkupPercent:       5,
		EffectiveMultiplier: 1.05,
		Reconciliation: &backupusagecontract.Reconciliation{
			Status:               backupusagecontract.ReconciliationStatusUnavailable,
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
