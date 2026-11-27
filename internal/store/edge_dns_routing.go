package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) ListEdgeDNSRoutingDecisions(hostname string) ([]model.EdgeDNSRoutingDecision, error) {
	hostname = normalizeEdgePerformanceHostname(hostname)
	return s.pgListEdgeDNSRoutingDecisions(hostname)
}

func (s *Store) UpsertEdgeDNSRoutingDecisions(decisions []model.EdgeDNSRoutingDecision) error {
	if len(decisions) == 0 {
		return nil
	}
	return s.pgUpsertEdgeDNSRoutingDecisions(decisions)
}

func (s *Store) pgListEdgeDNSRoutingDecisions(hostname string) ([]model.EdgeDNSRoutingDecision, error) {
	if err := s.ensureDatabaseReady(); err != nil {
		return nil, err
	}
	query := `
SELECT hostname, scope_key, country, region, asn, selected_edge_group_id, previous_edge_group_id,
	reason, score, sample_count, switched_at, cooldown_until, created_at, updated_at
FROM fugue_edge_dns_routing_decisions
WHERE 1=1
`
	args := []any{}
	if hostname != "" {
		args = append(args, hostname)
		query += fmt.Sprintf(" AND hostname = $%d", len(args))
	}
	query += " ORDER BY hostname ASC, scope_key ASC"
	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, fmt.Errorf("list edge dns routing decisions: %w", err)
	}
	defer rows.Close()
	var decisions []model.EdgeDNSRoutingDecision
	for rows.Next() {
		var decision model.EdgeDNSRoutingDecision
		var switchedAt, cooldownUntil sql.NullTime
		if err := rows.Scan(
			&decision.Hostname,
			&decision.ScopeKey,
			&decision.Country,
			&decision.Region,
			&decision.ASN,
			&decision.SelectedEdgeGroupID,
			&decision.PreviousEdgeGroupID,
			&decision.Reason,
			&decision.Score,
			&decision.SampleCount,
			&switchedAt,
			&cooldownUntil,
			&decision.CreatedAt,
			&decision.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan edge dns routing decision: %w", err)
		}
		if switchedAt.Valid {
			decision.SwitchedAt = switchedAt.Time
		}
		if cooldownUntil.Valid {
			decision.CooldownUntil = cooldownUntil.Time
		}
		decisions = append(decisions, decision)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate edge dns routing decisions: %w", err)
	}
	return decisions, nil
}

func (s *Store) pgUpsertEdgeDNSRoutingDecisions(decisions []model.EdgeDNSRoutingDecision) error {
	if err := s.ensureDatabaseReady(); err != nil {
		return err
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin edge dns routing decision transaction: %w", err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	for _, decision := range decisions {
		normalized, ok := normalizeEdgeDNSRoutingDecisionForStore(decision, now)
		if !ok {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_edge_dns_routing_decisions (
	hostname, scope_key, country, region, asn, selected_edge_group_id, previous_edge_group_id,
	reason, score, sample_count, switched_at, cooldown_until, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
)
ON CONFLICT (hostname, scope_key) DO UPDATE SET
	country = EXCLUDED.country,
	region = EXCLUDED.region,
	asn = EXCLUDED.asn,
	selected_edge_group_id = EXCLUDED.selected_edge_group_id,
	previous_edge_group_id = EXCLUDED.previous_edge_group_id,
	reason = EXCLUDED.reason,
	score = EXCLUDED.score,
	sample_count = EXCLUDED.sample_count,
	switched_at = EXCLUDED.switched_at,
	cooldown_until = EXCLUDED.cooldown_until,
	updated_at = EXCLUDED.updated_at
`,
			normalized.Hostname,
			normalized.ScopeKey,
			normalized.Country,
			normalized.Region,
			normalized.ASN,
			normalized.SelectedEdgeGroupID,
			normalized.PreviousEdgeGroupID,
			normalized.Reason,
			normalized.Score,
			normalized.SampleCount,
			nullTime(normalized.SwitchedAt),
			nullTime(normalized.CooldownUntil),
			normalized.CreatedAt,
			normalized.UpdatedAt,
		); err != nil {
			return fmt.Errorf("upsert edge dns routing decision: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit edge dns routing decision transaction: %w", err)
	}
	return nil
}

func normalizeEdgeDNSRoutingDecisionForStore(decision model.EdgeDNSRoutingDecision, now time.Time) (model.EdgeDNSRoutingDecision, bool) {
	decision.Hostname = normalizeEdgePerformanceHostname(decision.Hostname)
	decision.ScopeKey = strings.TrimSpace(strings.ToLower(decision.ScopeKey))
	decision.Country = normalizeEdgeMetadataValue(decision.Country)
	decision.Region = normalizeEdgeMetadataValue(decision.Region)
	decision.ASN = normalizeEdgeMetadataValue(decision.ASN)
	decision.SelectedEdgeGroupID = normalizeEdgeGroupID(decision.SelectedEdgeGroupID)
	decision.PreviousEdgeGroupID = normalizeEdgeGroupID(decision.PreviousEdgeGroupID)
	decision.Reason = strings.TrimSpace(decision.Reason)
	if decision.Hostname == "" || decision.ScopeKey == "" || decision.SelectedEdgeGroupID == "" {
		return model.EdgeDNSRoutingDecision{}, false
	}
	if decision.CreatedAt.IsZero() {
		decision.CreatedAt = now
	}
	if decision.UpdatedAt.IsZero() {
		decision.UpdatedAt = now
	}
	if decision.SwitchedAt.IsZero() {
		decision.SwitchedAt = decision.UpdatedAt
	}
	return decision, true
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}
