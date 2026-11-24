package model

// RecoveredBackupRetirementRequest binds an operator's restore evidence to an
// exact replacement. The digest is a reference, not server-verified evidence.
type RecoveredBackupRetirementRequest struct {
	ReplacementArtifactID string `json:"replacement_artifact_id"`
	RestoreEvidenceSHA256 string `json:"restore_evidence_sha256,omitempty"`
	Reason                string `json:"reason,omitempty"`
	Confirmation          string `json:"confirmation,omitempty"`
}

type RecoveredBackupRetirement struct {
	ArtifactID            string   `json:"artifact_id"`
	ReplacementArtifactID string   `json:"replacement_artifact_id"`
	Retired               bool     `json:"retired"`
	Blockers              []string `json:"blockers"`
	Confirmation          string   `json:"confirmation,omitempty"`
	AuditID               string   `json:"audit_id,omitempty"`
}
