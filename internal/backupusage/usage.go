package backupusage

import (
	"time"

	"fugue/internal/model"
)

const (
	ReconciliationStatusComplete    = "complete"
	ReconciliationStatusReconciling = "reconciling"
	ReconciliationStatusDrift       = "drift"
	ReconciliationStatusPartial     = "partial"
	ReconciliationStatusUnavailable = "unavailable"
)

// Usage keeps the existing billing ledger fields while adding an independently
// measured physical R2 inventory. Physical totals are omitted whenever the
// reconciliation could not measure every visible backend.
type Usage struct {
	TenantID              string          `json:"tenant_id,omitempty"`
	BackendID             string          `json:"backend_id,omitempty"`
	Provider              string          `json:"provider"`
	BillableBytes         int64           `json:"billable_bytes"`
	PhysicalBytes         *int64          `json:"physical_bytes,omitempty"`
	PhysicalObjectCount   *int            `json:"physical_object_count,omitempty"`
	CloudflareR2PriceCode string          `json:"cloudflare_r2_price_code,omitempty"`
	MarkupPercent         int             `json:"markup_percent"`
	EffectiveMultiplier   float64         `json:"effective_multiplier"`
	Currency              string          `json:"currency"`
	UpdatedAt             time.Time       `json:"updated_at"`
	Reconciliation        *Reconciliation `json:"reconciliation,omitempty"`
}

// Reconciliation describes exact object-level correspondence between durable
// artifact metadata and the measured R2 namespace.
type Reconciliation struct {
	Status                      string    `json:"status"`
	BackendCount                int       `json:"backend_count"`
	MeasuredBackendCount        int       `json:"measured_backend_count"`
	ExpectedObjectCount         int       `json:"expected_object_count"`
	ReferencedObjectCount       int       `json:"referenced_object_count"`
	ReferencedBytes             int64     `json:"referenced_bytes"`
	ActiveObjectCount           int       `json:"active_object_count"`
	ActiveBytes                 int64     `json:"active_bytes"`
	PendingDeletionObjectCount  int       `json:"pending_deletion_object_count"`
	PendingDeletionBytes        int64     `json:"pending_deletion_bytes"`
	UnreferencedObjectCount     int       `json:"unreferenced_object_count"`
	UnreferencedBytes           int64     `json:"unreferenced_bytes"`
	ProvisionalObjectCount      int       `json:"provisional_object_count"`
	ProvisionalBytes            int64     `json:"provisional_bytes"`
	OrphanedObjectCount         int       `json:"orphaned_object_count"`
	OrphanedBytes               int64     `json:"orphaned_bytes"`
	MissingActiveObjectCount    int       `json:"missing_active_object_count"`
	OverdueDeletionObjectCount  int       `json:"overdue_deletion_object_count"`
	OverdueDeletionBytes        int64     `json:"overdue_deletion_bytes"`
	LingeringDeletedObjectCount int       `json:"lingering_deleted_object_count"`
	LingeringDeletedBytes       int64     `json:"lingering_deleted_bytes"`
	DuplicateReferenceCount     int       `json:"duplicate_reference_count"`
	InvalidReferenceCount       int       `json:"invalid_reference_count"`
	SizeMismatchCount           int       `json:"size_mismatch_count"`
	UnresolvedBackendCount      int       `json:"unresolved_backend_count"`
	ObservedAt                  time.Time `json:"observed_at"`
	Message                     string    `json:"message,omitempty"`
}

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
