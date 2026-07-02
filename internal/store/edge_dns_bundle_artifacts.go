package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type EdgeDNSBundleArtifact struct {
	ScopeKey          string
	Zone              string
	DNSNodeID         string
	EdgeGroupID       string
	AnswerIPs         []string
	RouteAAnswerIPs   []string
	Version           string
	ETag              string
	SourceFingerprint string
	Bundle            model.EdgeDNSBundle
	GeneratedAt       time.Time
	ValidUntil        time.Time
	ActivatedAt       time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (s *Store) GetEdgeDNSBundleArtifact(scopeKey string) (EdgeDNSBundleArtifact, error) {
	scopeKey = strings.TrimSpace(scopeKey)
	if scopeKey == "" {
		return EdgeDNSBundleArtifact{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetEdgeDNSBundleArtifact(scopeKey)
	}

	s.edgeDNSBundleArtifactMu.Lock()
	defer s.edgeDNSBundleArtifactMu.Unlock()
	artifact, ok := s.edgeDNSBundleArtifacts[scopeKey]
	if !ok {
		return EdgeDNSBundleArtifact{}, ErrNotFound
	}
	return cloneEdgeDNSBundleArtifact(artifact), nil
}

func (s *Store) UpsertEdgeDNSBundleArtifact(artifact EdgeDNSBundleArtifact) error {
	normalized, err := normalizeEdgeDNSBundleArtifact(artifact)
	if err != nil {
		return err
	}
	if s.usingDatabase() {
		return s.pgUpsertEdgeDNSBundleArtifact(normalized)
	}

	s.edgeDNSBundleArtifactMu.Lock()
	defer s.edgeDNSBundleArtifactMu.Unlock()
	if s.edgeDNSBundleArtifacts == nil {
		s.edgeDNSBundleArtifacts = make(map[string]EdgeDNSBundleArtifact)
	}
	if existing, ok := s.edgeDNSBundleArtifacts[normalized.ScopeKey]; ok && !existing.CreatedAt.IsZero() {
		normalized.CreatedAt = existing.CreatedAt
	}
	s.edgeDNSBundleArtifacts[normalized.ScopeKey] = cloneEdgeDNSBundleArtifact(normalized)
	return nil
}

func normalizeEdgeDNSBundleArtifact(artifact EdgeDNSBundleArtifact) (EdgeDNSBundleArtifact, error) {
	artifact.ScopeKey = strings.TrimSpace(artifact.ScopeKey)
	artifact.Zone = strings.TrimSpace(strings.ToLower(artifact.Zone))
	artifact.DNSNodeID = strings.TrimSpace(artifact.DNSNodeID)
	artifact.EdgeGroupID = strings.TrimSpace(artifact.EdgeGroupID)
	artifact.AnswerIPs = uniqueSortedStoreStrings(artifact.AnswerIPs)
	artifact.RouteAAnswerIPs = uniqueSortedStoreStrings(artifact.RouteAAnswerIPs)
	artifact.Version = strings.TrimSpace(firstNonEmptyStoreString(artifact.Version, artifact.Bundle.Version))
	artifact.ETag = strings.TrimSpace(artifact.ETag)
	artifact.SourceFingerprint = strings.TrimSpace(artifact.SourceFingerprint)
	if artifact.ScopeKey == "" || artifact.Version == "" || strings.TrimSpace(artifact.Bundle.Version) == "" {
		return EdgeDNSBundleArtifact{}, ErrInvalidInput
	}
	if artifact.GeneratedAt.IsZero() {
		artifact.GeneratedAt = artifact.Bundle.GeneratedAt
	}
	if artifact.ValidUntil.IsZero() {
		artifact.ValidUntil = artifact.Bundle.ValidUntil
	}
	now := time.Now().UTC()
	if artifact.GeneratedAt.IsZero() {
		artifact.GeneratedAt = now
	}
	if artifact.ActivatedAt.IsZero() {
		artifact.ActivatedAt = now
	}
	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = now
	}
	if artifact.UpdatedAt.IsZero() {
		artifact.UpdatedAt = now
	}
	return artifact, nil
}

func (s *Store) pgGetEdgeDNSBundleArtifact(scopeKey string) (EdgeDNSBundleArtifact, error) {
	if err := s.ensureDatabaseReady(); err != nil {
		return EdgeDNSBundleArtifact{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return scanEdgeDNSBundleArtifact(s.db.QueryRowContext(ctx, `
SELECT scope_key, zone, dns_node_id, edge_group_id, answer_ips_json, route_a_answer_ips_json,
	version, etag, source_fingerprint, bundle_json, generated_at, valid_until, activated_at, created_at, updated_at
FROM fugue_edge_dns_bundle_artifacts
WHERE scope_key = $1
`, scopeKey))
}

func (s *Store) pgUpsertEdgeDNSBundleArtifact(artifact EdgeDNSBundleArtifact) error {
	if err := s.ensureDatabaseReady(); err != nil {
		return err
	}
	answerIPsJSON, err := marshalJSON(artifact.AnswerIPs)
	if err != nil {
		return err
	}
	routeAAnswerIPsJSON, err := marshalJSON(artifact.RouteAAnswerIPs)
	if err != nil {
		return err
	}
	bundleJSON, err := marshalJSON(artifact.Bundle)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = s.db.ExecContext(ctx, `
INSERT INTO fugue_edge_dns_bundle_artifacts (
	scope_key, zone, dns_node_id, edge_group_id, answer_ips_json, route_a_answer_ips_json,
	version, etag, source_fingerprint, bundle_json, generated_at, valid_until, activated_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6,
	$7, $8, $9, $10, $11, $12, $13, $14, $15
)
ON CONFLICT (scope_key) DO UPDATE SET
	zone = EXCLUDED.zone,
	dns_node_id = EXCLUDED.dns_node_id,
	edge_group_id = EXCLUDED.edge_group_id,
	answer_ips_json = EXCLUDED.answer_ips_json,
	route_a_answer_ips_json = EXCLUDED.route_a_answer_ips_json,
	version = EXCLUDED.version,
	etag = EXCLUDED.etag,
	source_fingerprint = EXCLUDED.source_fingerprint,
	bundle_json = EXCLUDED.bundle_json,
	generated_at = EXCLUDED.generated_at,
	valid_until = EXCLUDED.valid_until,
	activated_at = EXCLUDED.activated_at,
	updated_at = EXCLUDED.updated_at
`, artifact.ScopeKey, artifact.Zone, artifact.DNSNodeID, artifact.EdgeGroupID, answerIPsJSON, routeAAnswerIPsJSON,
		artifact.Version, artifact.ETag, artifact.SourceFingerprint, bundleJSON, artifact.GeneratedAt, nullTime(artifact.ValidUntil), artifact.ActivatedAt, artifact.CreatedAt, artifact.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert edge dns bundle artifact: %w", mapDBErr(err))
	}
	return nil
}

func scanEdgeDNSBundleArtifact(row interface {
	Scan(dest ...any) error
}) (EdgeDNSBundleArtifact, error) {
	var artifact EdgeDNSBundleArtifact
	var answerIPsJSON, routeAAnswerIPsJSON, bundleJSON []byte
	var validUntil sql.NullTime
	if err := row.Scan(
		&artifact.ScopeKey,
		&artifact.Zone,
		&artifact.DNSNodeID,
		&artifact.EdgeGroupID,
		&answerIPsJSON,
		&routeAAnswerIPsJSON,
		&artifact.Version,
		&artifact.ETag,
		&artifact.SourceFingerprint,
		&bundleJSON,
		&artifact.GeneratedAt,
		&validUntil,
		&artifact.ActivatedAt,
		&artifact.CreatedAt,
		&artifact.UpdatedAt,
	); err != nil {
		return EdgeDNSBundleArtifact{}, mapDBErr(err)
	}
	if err := json.Unmarshal(answerIPsJSON, &artifact.AnswerIPs); err != nil {
		return EdgeDNSBundleArtifact{}, fmt.Errorf("decode edge dns artifact answer ips: %w", err)
	}
	if err := json.Unmarshal(routeAAnswerIPsJSON, &artifact.RouteAAnswerIPs); err != nil {
		return EdgeDNSBundleArtifact{}, fmt.Errorf("decode edge dns artifact route-a answer ips: %w", err)
	}
	if err := json.Unmarshal(bundleJSON, &artifact.Bundle); err != nil {
		return EdgeDNSBundleArtifact{}, fmt.Errorf("decode edge dns artifact bundle: %w", err)
	}
	if validUntil.Valid {
		artifact.ValidUntil = validUntil.Time
	}
	return artifact, nil
}

func cloneEdgeDNSBundleArtifact(artifact EdgeDNSBundleArtifact) EdgeDNSBundleArtifact {
	out := artifact
	out.AnswerIPs = append([]string(nil), artifact.AnswerIPs...)
	out.RouteAAnswerIPs = append([]string(nil), artifact.RouteAAnswerIPs...)
	raw, err := json.Marshal(artifact.Bundle)
	if err == nil {
		_ = json.Unmarshal(raw, &out.Bundle)
	}
	return out
}

func uniqueSortedStoreStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
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
	sort.Strings(out)
	return out
}

func firstNonEmptyStoreString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
