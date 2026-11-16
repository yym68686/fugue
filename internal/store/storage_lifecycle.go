package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// V1 deliberately supports observation only. Enforce is NOT an alias for audit:
// it is refused until complete retention authorities and writer fencing exist.
// Neither age nor the absence of a known reference confers deletion authority.
type StorageLifecyclePolicy struct {
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

func ParseStorageLifecyclePolicy(raw []byte) (StorageLifecyclePolicy, error) {
	var p StorageLifecyclePolicy
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if len(raw) > 4096 || d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF ||
		p.Version != 1 || (p.Mode != "disabled" && p.Mode != "audit") || p.EffectiveFrom.Year() <= 1 ||
		p.IntervalSeconds < 300 || p.IntervalSeconds > 3600 || p.SourcePageRows < 1 || p.SourcePageRows > 128 ||
		p.HistoryDays < 7 || p.HistoryDays > 90 || p.MinimumVerifiedVersions < 3 || p.MinimumVerifiedVersions > 100 ||
		p.UnusedUploadHours < 24 || p.UnusedUploadHours > 720 || p.UnreferencedUploadDays < 30 || p.UnreferencedUploadDays > 365 ||
		p.CriticalFreePercent < 5 || p.CriticalFreePercent >= p.PressureFreePercent ||
		p.PressureFreePercent >= p.WarningFreePercent || p.WarningFreePercent > 50 {
		return StorageLifecyclePolicy{}, errors.New("invalid or unsupported storage lifecycle policy")
	}
	p.EffectiveFrom = p.EffectiveFrom.UTC()
	return p, nil
}

type StorageRelationAllocation struct {
	Relation   string `json:"relation"`
	Present    bool   `json:"present"`
	HeapBytes  int64  `json:"heap_bytes"`
	TableBytes int64  `json:"table_bytes"` // includes TOAST; NOT additive to heap
	IndexBytes int64  `json:"index_bytes"`
	TotalBytes int64  `json:"total_bytes"`
}

type StorageSourceAuditPage struct {
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

type StorageLifecycleSample struct {
	ObservedAt    time.Time                   `json:"observed_at"`
	PolicySHA256  string                      `json:"policy_sha256"`
	DatabaseBytes int64                       `json:"database_bytes"`
	Relations     []StorageRelationAllocation `json:"relations"`
	SourcePage    StorageSourceAuditPage      `json:"source_page"`
}

type StorageLifecycleObservation struct {
	Schema             string                  `json:"schema"`
	Policy             StorageLifecyclePolicy  `json:"policy"`
	PolicySHA256       string                  `json:"policy_sha256"`
	PolicyReadFailed   bool                    `json:"policy_read_failed"`
	LastAttemptAt      time.Time               `json:"last_attempt_at"`
	LastRunFailed      bool                    `json:"last_run_failed"`
	FailureCategory    string                  `json:"failure_category"`
	Failures           uint64                  `json:"process_failures"`
	DurationSeconds    float64                 `json:"last_duration_seconds"`
	Sample             *StorageLifecycleSample `json:"last_successful_sample,omitempty"`
	ExecutionSupported bool                    `json:"execution_supported"`
	CapacitySemantics  string                  `json:"capacity_semantics"`
}

type storageLifecycleState struct {
	mu          sync.Mutex
	observation StorageLifecycleObservation
	cursor      string // private: never expose tenant/upload identities as metrics
}

func (s *Store) StorageLifecycleObservation() StorageLifecycleObservation {
	s.lifecycle.mu.Lock()
	defer s.lifecycle.mu.Unlock()
	o := s.lifecycle.observation
	o.Schema = "fugue.storage-lifecycle.audit.v1"
	o.CapacitySemantics = "one bounded source page, not a global inventory; declared bytes are not reclaimable physical bytes; unknown is not eligible; audit never deletes or throttles"
	if o.Sample != nil {
		copy := *o.Sample
		copy.Relations = append([]StorageRelationAllocation(nil), copy.Relations...)
		o.Sample = &copy
	}
	return o
}

// A separate loop keeps both metrics scrapes and runtime observations entirely
// in memory. There is no schema migration, data rewrite, body read or DELETE.
func (s *Store) RunStorageLifecycleAudit(ctx context.Context, storagePolicyPath string) {
	if storagePolicyPath == "" || s.db == nil {
		return
	}
	path := filepath.Join(filepath.Dir(storagePolicyPath), "storage-lifecycle.json")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		s.storageLifecycleTick(ctx, path, time.Now().UTC())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Store) storageLifecycleTick(ctx context.Context, path string, now time.Time) {
	f, err := os.Open(path)
	var raw []byte
	if err == nil {
		raw, err = io.ReadAll(io.LimitReader(f, 4097))
		_ = f.Close()
	}
	var p StorageLifecyclePolicy
	if err == nil {
		p, err = ParseStorageLifecyclePolicy(raw)
	}
	s.lifecycle.mu.Lock()
	o := &s.lifecycle.observation
	if err != nil {
		// Missing policies support reader-only rollback. Invalid/missing config
		// cannot leave the last policy active, including between samples.
		o.PolicyReadFailed = !os.IsNotExist(err)
		o.Policy = StorageLifecyclePolicy{Mode: "disabled"}
		o.PolicySHA256 = ""
		s.lifecycle.cursor = ""
		s.lifecycle.mu.Unlock()
		return
	}
	canonical, _ := json.Marshal(p)
	hash := sha256.Sum256(canonical)
	digest := hex.EncodeToString(hash[:])
	changed := digest != o.PolicySHA256
	if changed {
		s.lifecycle.cursor = ""
	}
	o.Policy, o.PolicySHA256, o.PolicyReadFailed = p, digest, false
	if p.Mode != "audit" || now.Before(p.EffectiveFrom) || (!changed && now.Sub(o.LastAttemptAt) < time.Duration(p.IntervalSeconds)*time.Second) {
		s.lifecycle.mu.Unlock()
		return
	}
	cursor := s.lifecycle.cursor
	o.LastAttemptAt = now
	s.lifecycle.mu.Unlock()
	started := time.Now()
	sample, next, auditErr := s.collectStorageLifecycleSample(ctx, p, digest, cursor, now)
	s.lifecycle.mu.Lock()
	defer s.lifecycle.mu.Unlock()
	o = &s.lifecycle.observation
	o.DurationSeconds = time.Since(started).Seconds()
	o.LastRunFailed, o.FailureCategory = auditErr != nil, ""
	if auditErr != nil {
		o.Failures++
		o.FailureCategory = "bounded_read_failed" // never driver messages, SQL or credentials
		return                                    // do not advance a failed page or publish a partial sample
	}
	o.Sample = &sample
	s.lifecycle.cursor = next
}

// Fixed allowlist: callers/configuration cannot add tables or select bodies.
const storageLifecycleRelationSQL = `
WITH requested(name) AS (VALUES
 ('fugue_source_uploads'), ('fugue_platform_artifacts'), ('fugue_platform_artifact_contents'),
 ('fugue_audit_events'), ('fugue_platform_input_shared_packs'), ('fugue_platform_input_shared_objects'),
 ('fugue_platform_input_component_refs'), ('fugue_platform_input_component_recipes'),
 ('fugue_platform_input_separated_refs'), ('fugue_platform_input_components'))
SELECT r.name,c.oid IS NOT NULL,COALESCE(pg_relation_size(c.oid),0),
 COALESCE(pg_table_size(c.oid),0),COALESCE(pg_indexes_size(c.oid),0),COALESCE(pg_total_relation_size(c.oid),0)
FROM requested r LEFT JOIN pg_namespace n ON n.nspname=current_schema()
LEFT JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=r.name AND c.relkind='r'
ORDER BY r.name`

// Known positive references only. This is NOT the complete retention graph.
// No negative result from these sources is ever classified as discardable.
const storageLifecycleSourceSQL = `
WITH page AS MATERIALIZED (
 SELECT id,tenant_id,created_at,size_bytes FROM fugue_source_uploads
 WHERE id > $1 ORDER BY id LIMIT $2
), refs AS MATERIALIZED (
 SELECT p.id FROM page p JOIN fugue_app_database_import_jobs j ON j.source_upload_id=p.id AND j.tenant_id=p.tenant_id
 UNION
 SELECT p.id FROM fugue_apps a CROSS JOIN LATERAL
 (VALUES(a.source_json->>'upload_id'),(a.source_json->'origin_source'->>'upload_id'),(a.source_json->'build_source'->>'upload_id')) v(id)
 JOIN page p ON p.id=v.id AND p.tenant_id=a.tenant_id
 UNION
 SELECT p.id FROM fugue_operations o CROSS JOIN LATERAL
 (VALUES(o.desired_source_json->>'upload_id'),(o.desired_source_json->'desired_source'->>'upload_id'),(o.desired_source_json->'desired_origin_source'->>'upload_id')) v(id)
 JOIN page p ON p.id=v.id AND p.tenant_id=o.tenant_id
)
SELECT p.id,p.created_at,p.size_bytes,EXISTS(SELECT 1 FROM refs r WHERE r.id=p.id)
FROM page p ORDER BY p.id`

func (s *Store) collectStorageLifecycleSample(ctx context.Context, p StorageLifecyclePolicy, digest, cursor string, now time.Time) (sample StorageLifecycleSample, next string, err error) {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return sample, "", err
	}
	defer tx.Rollback()
	for _, q := range []string{`SET LOCAL statement_timeout='2s'`, `SET LOCAL lock_timeout='100ms'`, `SET LOCAL temp_file_limit='0'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			return sample, "", err
		}
	}
	sample.ObservedAt, sample.PolicySHA256 = now, digest
	if err = tx.QueryRowContext(ctx, `SELECT pg_database_size(current_database())`).Scan(&sample.DatabaseBytes); err != nil {
		return sample, "", err
	}
	rows, err := tx.QueryContext(ctx, storageLifecycleRelationSQL)
	if err != nil {
		return sample, "", err
	}
	for rows.Next() {
		var r StorageRelationAllocation
		if err = rows.Scan(&r.Relation, &r.Present, &r.HeapBytes, &r.TableBytes, &r.IndexBytes, &r.TotalBytes); err != nil {
			break
		}
		sample.Relations = append(sample.Relations, r)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return sample, "", err
	}
	rows, err = tx.QueryContext(ctx, storageLifecycleSourceSQL, cursor, p.SourcePageRows+1)
	if err != nil {
		return sample, "", err
	}
	page := &sample.SourcePage
	page.PageAtEnd = true
	for rows.Next() {
		var id string
		var at time.Time
		var size int64
		var referenced bool
		if err = rows.Scan(&id, &at, &size, &referenced); err != nil {
			break
		}
		if page.Rows == p.SourcePageRows {
			page.PageAtEnd = false // sentinel is NOT counted; next page re-reads it
			break
		}
		if size < 0 || size > math.MaxInt64-page.DeclaredBytes {
			err = errors.New("invalid source size")
			break
		}
		next = id
		page.Rows++
		page.DeclaredBytes += size
		if referenced {
			page.ReferencedRows++
			page.ReferencedBytes += size
		} else {
			page.UnknownRows++
			page.UnknownBytes += size
		}
		if at.Before(p.EffectiveFrom) {
			page.LegacyRows++
		}
		if at.Before(now.Add(-30 * 24 * time.Hour)) {
			page.OlderThan30DaysRows++
			page.OlderThan30DaysBytes += size
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return sample, "", err
	}
	if page.PageAtEnd {
		next = "" // new RR next sweep; never combine pages into a global proof
	}
	return sample, next, tx.Commit()
}
