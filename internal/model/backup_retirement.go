package model

// RecoveredBackupRetirementRequest binds an exact replacement and optional
// operator receipt reference. A digest is not server-verified restore evidence.
type RecoveredBackupRetirementRequest struct {
	ReplacementArtifactID string `json:"replacement_artifact_id"`
	RestoreEvidenceSHA256 string `json:"restore_evidence_sha256,omitempty"`
	Reason                string `json:"reason,omitempty"`
	Confirmation          string `json:"confirmation,omitempty"`
}

type RecoveredBackupRetirement struct {
	ArtifactID            string   `json:"artifact_id"`
	ReplacementArtifactID string   `json:"replacement_artifact_id"`
	RetainedArtifactIDs   []string `json:"retained_artifact_ids"`
	RestoreEvidenceStatus string   `json:"restore_evidence_status"`
	Retired               bool     `json:"retired"`
	Blockers              []string `json:"blockers"`
	Confirmation          string   `json:"confirmation,omitempty"`
	AuditID               string   `json:"audit_id,omitempty"`
}
