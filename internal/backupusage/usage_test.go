package backupusage

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestFromModelPreservesBillingLedgerWithoutInventingPhysicalTotals(t *testing.T) {
	updatedAt := time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)
	usage := FromModel(model.BackupUsage{
		TenantID:              "tenant_a",
		BackendID:             "backend_a",
		Provider:              model.DataBackendProviderCloudflareR2,
		BillableBytes:         123,
		CloudflareR2PriceCode: "cloudflare-r2-standard-storage",
		MarkupPercent:         5,
		EffectiveMultiplier:   1.05,
		Currency:              "USD",
		UpdatedAt:             updatedAt,
	})
	if usage.TenantID != "tenant_a" || usage.BackendID != "backend_a" || usage.BillableBytes != 123 || !usage.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("billing ledger was not preserved: %+v", usage)
	}
	if usage.PhysicalBytes != nil || usage.PhysicalObjectCount != nil || usage.Reconciliation != nil {
		t.Fatalf("model conversion invented physical measurement: %+v", usage)
	}
}
