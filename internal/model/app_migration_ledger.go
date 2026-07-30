package model

import (
	"strings"
	"time"
)

// AppMigrationLedger is the durable, per-application cutover record.  It is
// stored as a versioned operation-evidence payload so older databases do not
// need a destructive schema migration.  A ledger entry is an observation of a
// migration attempt; it is never used as runtime status for the app.
type AppMigrationLedger struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	TenantID      string `json:"tenant_id"`
	ProjectID     string `json:"project_id,omitempty"`
	AppID         string `json:"app_id"`
	OperationID   string `json:"operation_id"`

	OldRuntimeID string `json:"old_runtime_id,omitempty"`
	NewRuntimeID string `json:"new_runtime_id"`
	OldClusterID string `json:"old_cluster_id"`
	NewClusterID string `json:"new_cluster_id"`

	ImageRef               string   `json:"image_ref,omitempty"`
	ImageReplicationStatus string   `json:"image_replication_status"`
	ImageReplicationResult string   `json:"image_replication_result,omitempty"`
	RuntimeObjectStatus    string   `json:"runtime_object_status"`
	RuntimeObjectResult    string   `json:"runtime_object_result,omitempty"`
	EndpointRequired       bool     `json:"endpoint_required"`
	EndpointStatus         string   `json:"endpoint_status"`
	EndpointResult         string   `json:"endpoint_result,omitempty"`
	EndpointReady          *bool    `json:"endpoint_ready,omitempty"`
	PhysicalReplicas       *int     `json:"physical_replicas,omitempty"`
	DesiredReplicas        int      `json:"desired_replicas"`
	Generation             int64    `json:"generation,omitempty"`
	ObservedGeneration     int64    `json:"observed_generation,omitempty"`
	InvariantViolations    []string `json:"invariant_violations,omitempty"`

	CutoverStatus         string `json:"cutover_status"`
	OldArtifactsProtected bool   `json:"old_artifacts_protected"`
	FailureReason         string `json:"failure_reason,omitempty"`
	OperatorType          string `json:"operator_type,omitempty"`
	OperatorID            string `json:"operator_id,omitempty"`
	EvidenceSource        string `json:"evidence_source,omitempty"`
	AssociatedOperationID string `json:"associated_operation_id,omitempty"`

	ObservedAt  time.Time `json:"observed_at"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	RetainUntil time.Time `json:"retain_until"`
}

const (
	AppMigrationLedgerSchemaVersion = 1

	AppMigrationCutoverPending   = "pending"
	AppMigrationCutoverVerified  = "verified"
	AppMigrationCutoverCompleted = "completed"
	AppMigrationCutoverBlocked   = "blocked"
	AppMigrationCutoverFailed    = "failed"

	AppMigrationEvidenceUnknown       = "unknown"
	AppMigrationEvidenceVerified      = "verified"
	AppMigrationEvidenceMissing       = "missing"
	AppMigrationEvidenceFailed        = "failed"
	AppMigrationEvidenceReady         = "ready"
	AppMigrationEvidenceCreated       = "created"
	AppMigrationEvidenceNotApplicable = "not_applicable"
)

// NormalizeAppMigrationLedger fills stable defaults while retaining explicit
// false/zero evidence.  In particular, it never turns an omitted pointer into
// an authoritative negative observation.
func NormalizeAppMigrationLedger(in AppMigrationLedger, now time.Time) AppMigrationLedger {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	in.SchemaVersion = AppMigrationLedgerSchemaVersion
	in.ID = strings.TrimSpace(in.ID)
	in.TenantID = strings.TrimSpace(in.TenantID)
	in.ProjectID = strings.TrimSpace(in.ProjectID)
	in.AppID = strings.TrimSpace(in.AppID)
	in.OperationID = strings.TrimSpace(in.OperationID)
	in.OldRuntimeID = strings.TrimSpace(in.OldRuntimeID)
	in.NewRuntimeID = strings.TrimSpace(in.NewRuntimeID)
	in.OldClusterID = strings.TrimSpace(in.OldClusterID)
	in.NewClusterID = strings.TrimSpace(in.NewClusterID)
	in.ImageRef = strings.TrimSpace(in.ImageRef)
	in.ImageReplicationStatus = strings.TrimSpace(in.ImageReplicationStatus)
	in.ImageReplicationResult = strings.TrimSpace(in.ImageReplicationResult)
	in.RuntimeObjectStatus = strings.TrimSpace(in.RuntimeObjectStatus)
	in.RuntimeObjectResult = strings.TrimSpace(in.RuntimeObjectResult)
	in.EndpointStatus = strings.TrimSpace(in.EndpointStatus)
	in.EndpointResult = strings.TrimSpace(in.EndpointResult)
	in.CutoverStatus = strings.TrimSpace(in.CutoverStatus)
	in.FailureReason = strings.TrimSpace(in.FailureReason)
	in.OperatorType = strings.TrimSpace(in.OperatorType)
	in.OperatorID = strings.TrimSpace(in.OperatorID)
	in.EvidenceSource = strings.TrimSpace(in.EvidenceSource)
	in.AssociatedOperationID = strings.TrimSpace(in.AssociatedOperationID)
	if in.AssociatedOperationID == "" {
		in.AssociatedOperationID = in.OperationID
	}
	if in.CutoverStatus == "" {
		in.CutoverStatus = AppMigrationCutoverPending
	}
	if in.ImageReplicationStatus == "" {
		in.ImageReplicationStatus = AppMigrationEvidenceUnknown
	}
	if in.RuntimeObjectStatus == "" {
		in.RuntimeObjectStatus = AppMigrationEvidenceUnknown
	}
	if in.EndpointStatus == "" {
		if in.EndpointRequired {
			in.EndpointStatus = AppMigrationEvidenceUnknown
		} else {
			in.EndpointStatus = AppMigrationEvidenceNotApplicable
		}
	}
	if in.ObservedAt.IsZero() {
		in.ObservedAt = now
	} else {
		in.ObservedAt = in.ObservedAt.UTC()
	}
	if in.CreatedAt.IsZero() {
		in.CreatedAt = now
	} else {
		in.CreatedAt = in.CreatedAt.UTC()
	}
	if in.UpdatedAt.IsZero() {
		in.UpdatedAt = now
	} else {
		in.UpdatedAt = in.UpdatedAt.UTC()
	}
	if in.RetainUntil.IsZero() {
		in.RetainUntil = in.CreatedAt.Add(90 * 24 * time.Hour)
	} else {
		in.RetainUntil = in.RetainUntil.UTC()
	}
	if in.RetainUntil.Before(in.CreatedAt.Add(90 * 24 * time.Hour)) {
		in.RetainUntil = in.CreatedAt.Add(90 * 24 * time.Hour)
	}
	if in.PhysicalReplicas != nil && *in.PhysicalReplicas < 0 {
		value := 0
		in.PhysicalReplicas = &value
	}
	if in.DesiredReplicas < 0 {
		in.DesiredReplicas = 0
	}
	in.InvariantViolations = normalizeStringSet(in.InvariantViolations)
	return in
}

func normalizeStringSet(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
