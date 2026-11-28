package backupusage

import (
	"fugue/internal/backupusage/contract"
	"fugue/internal/model"
)

const (
	ReconciliationStatusComplete    = contract.ReconciliationStatusComplete
	ReconciliationStatusReconciling = contract.ReconciliationStatusReconciling
	ReconciliationStatusDrift       = contract.ReconciliationStatusDrift
	ReconciliationStatusPartial     = contract.ReconciliationStatusPartial
	ReconciliationStatusUnavailable = contract.ReconciliationStatusUnavailable
)

type Usage = contract.Usage
type Reconciliation = contract.Reconciliation

func FromModel(usage model.BackupUsage) Usage {
	return Usage{
		TenantID:              usage.TenantID,
		BackendID:             usage.BackendID,
		Provider:              usage.Provider,
		BillableBytes:         usage.BillableBytes,
		CloudflareR2PriceCode: usage.CloudflareR2PriceCode,
		MarkupPercent:         usage.MarkupPercent,
		EffectiveMultiplier:   usage.EffectiveMultiplier,
		Currency:              usage.Currency,
		UpdatedAt:             usage.UpdatedAt,
	}
}
