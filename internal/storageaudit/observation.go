package storageaudit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"
)

const (
	OwnershipLock     = "storage-lifecycle-audit/owner/v1"
	ObservationSchema = "fugue.storage-lifecycle.audit.v1"
	CapacitySemantics = "one bounded source page, not a global inventory; declared bytes are not reclaimable physical bytes; unknown is not eligible; audit never deletes or throttles"
)

// Policy v1 deliberately supports observation only. Neither age nor the
// absence of a known reference confers deletion authority.
type Policy struct {
	Version                 int       `json:"version"`
	Mode                    string    `json:"mode"`
	EffectiveFrom           time.Time `json:"effective_from"`
	IntervalSeconds         int       `json:"interval_seconds"`
	SourcePageRows          int       `json:"source_page_rows"`
	HistoryDays             int       `json:"history_days"`
	MinimumVerifiedVersions int       `json:"minimum_verified_versions"`
	UnusedUploadHours       int       `json:"unused_upload_hours"`
	UnreferencedUploadDays  int       `json:"unreferenced_upload_days"`
	WarningFreePercent      int       `json:"warning_free_percent"`
	PressureFreePercent     int       `json:"pressure_free_percent"`
	CriticalFreePercent     int       `json:"critical_free_percent"`
}

func ParsePolicy(raw []byte) (Policy, error) {
	var p Policy
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if len(raw) > 4096 || d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF ||
		p.Version != 1 || (p.Mode != "disabled" && p.Mode != "audit") || p.EffectiveFrom.Year() <= 1 ||
		p.IntervalSeconds < 300 || p.IntervalSeconds > 3600 || p.SourcePageRows < 1 || p.SourcePageRows > 128 ||
		p.HistoryDays < 7 || p.HistoryDays > 90 || p.MinimumVerifiedVersions < 3 || p.MinimumVerifiedVersions > 100 ||
		p.UnusedUploadHours < 24 || p.UnusedUploadHours > 720 || p.UnreferencedUploadDays < 30 || p.UnreferencedUploadDays > 365 ||
		p.CriticalFreePercent < 5 || p.CriticalFreePercent >= p.PressureFreePercent ||
		p.PressureFreePercent >= p.WarningFreePercent || p.WarningFreePercent > 50 {
		return Policy{}, errors.New("invalid or unsupported storage lifecycle policy")
	}
	p.EffectiveFrom = p.EffectiveFrom.UTC()
	return p, nil
}

func (p Policy) Digest() string {
	p.EffectiveFrom = p.EffectiveFrom.UTC()
	canonical, err := json.Marshal(p)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:])
}

type RelationAllocation struct {
	Relation   string `json:"relation"`
	Present    bool   `json:"present"`
	HeapBytes  int64  `json:"heap_bytes"`
	TableBytes int64  `json:"table_bytes"` // includes TOAST; NOT additive to heap
	IndexBytes int64  `json:"index_bytes"`
	TotalBytes int64  `json:"total_bytes"`
}

type SourcePage struct {
	Rows                 int   `json:"rows"`
	DeclaredBytes        int64 `json:"declared_bytes"`
	ReferencedRows       int   `json:"observed_referenced_rows"`
	ReferencedBytes      int64 `json:"observed_referenced_declared_bytes"`
	UnknownRows          int   `json:"unknown_rows"`
	UnknownBytes         int64 `json:"unknown_declared_bytes"`
	LegacyRows           int   `json:"predating_contract_rows"`
	OlderThan30DaysRows  int   `json:"older_than_30_days_rows"`
	OlderThan30DaysBytes int64 `json:"older_than_30_days_declared_bytes"`
	PageAtEnd            bool  `json:"page_at_end"`
	ReferencesComplete   bool  `json:"references_complete"`
	RetirementAuthorized bool  `json:"retirement_authorized"`
	EligibleRows         int   `json:"eligible_rows"`
}

type Sample struct {
	ObservedAt    time.Time            `json:"observed_at"`
	PolicySHA256  string               `json:"policy_sha256"`
	DatabaseBytes int64                `json:"database_bytes"`
	Relations     []RelationAllocation `json:"relations"`
	SourcePage    SourcePage           `json:"source_page"`
}

func isLifecycleRelation(relation string) bool {
	switch relation {
	case "fugue_source_uploads", "fugue_platform_artifacts", "fugue_platform_artifact_contents",
		"fugue_audit_events", "fugue_platform_input_shared_packs", "fugue_platform_input_shared_objects",
		"fugue_platform_input_component_refs", "fugue_platform_input_component_recipes",
		"fugue_platform_input_separated_refs", "fugue_platform_input_components", "fugue_platform_quality_evidence":
		return true
	default:
		return false
	}
}

func (s Sample) valid() bool {
	if s.ObservedAt.IsZero() || s.DatabaseBytes < 0 || len(s.Relations) != 11 || len(s.PolicySHA256) != sha256.Size*2 {
		return false
	}
	if _, err := hex.DecodeString(s.PolicySHA256); err != nil {
		return false
	}
	seen := make(map[string]struct{}, len(s.Relations))
	for _, r := range s.Relations {
		if _, duplicate := seen[r.Relation]; duplicate || !isLifecycleRelation(r.Relation) || r.HeapBytes < 0 ||
			r.TableBytes < 0 || r.IndexBytes < 0 || r.TotalBytes < 0 {
			return false
		}
		seen[r.Relation] = struct{}{}
	}
	p := s.SourcePage
	return p.Rows >= 0 && p.Rows <= 128 && p.ReferencedRows >= 0 && p.ReferencedRows <= p.Rows &&
		p.UnknownRows == p.Rows-p.ReferencedRows && p.DeclaredBytes >= 0 && p.ReferencedBytes >= 0 &&
		p.ReferencedBytes <= p.DeclaredBytes && p.UnknownBytes == p.DeclaredBytes-p.ReferencedBytes &&
		p.LegacyRows >= 0 && p.LegacyRows <= p.Rows && p.OlderThan30DaysRows >= 0 && p.OlderThan30DaysRows <= p.Rows &&
		p.OlderThan30DaysBytes >= 0 && p.OlderThan30DaysBytes <= p.DeclaredBytes &&
		!p.ReferencesComplete && !p.RetirementAuthorized && p.EligibleRows == 0
}

type Observation struct {
	Schema             string    `json:"schema"`
	Policy             Policy    `json:"policy"`
	PolicySHA256       string    `json:"policy_sha256"`
	PolicyReadFailed   bool      `json:"policy_read_failed"`
	LastAttemptAt      time.Time `json:"last_attempt_at"`
	LastRunFailed      bool      `json:"last_run_failed"`
	FailureCategory    string    `json:"failure_category"`
	Failures           uint64    `json:"process_failures"`
	DurationSeconds    float64   `json:"last_duration_seconds"`
	Sample             *Sample   `json:"last_successful_sample,omitempty"`
	ExecutionSupported bool      `json:"execution_supported"`
	CapacitySemantics  string    `json:"capacity_semantics"`
}

func (p Progress) Observation() Observation {
	o := Observation{Schema: ObservationSchema, Policy: p.Policy, PolicySHA256: p.PolicySHA256,
		LastAttemptAt: p.LastAttemptAt.Time, LastRunFailed: p.LastFailed, Failures: uint64(p.Failures),
		DurationSeconds: p.Duration, Sample: p.Sample, CapacitySemantics: CapacitySemantics}
	if o.LastRunFailed {
		o.FailureCategory = "bounded_read_failed"
	}
	return o
}
