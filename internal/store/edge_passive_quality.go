package store

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
)

// Read across domains in one database scan, bounded per physical edge so the
// incumbent cannot occupy the entire batch. No recovery or service probes.
func (s *Store) ListPassiveEdgeNetworkSamples(ctx context.Context, edgeIDs []string, since time.Time, limit int) ([]model.EdgeNetworkSample, error) {
	if len(edgeIDs) > 32 || since.IsZero() || limit < 1 || limit > 1024 {
		return nil, ErrInvalidInput
	}
	samples := []model.EdgeNetworkSample{}
	if len(edgeIDs) == 0 {
		return samples, nil
	}
	wanted := make(map[string]bool, len(edgeIDs))
	for _, id := range edgeIDs {
		if id == "" || wanted[id] {
			return nil, ErrInvalidInput
		}
		wanted[id] = true
	}
	if s.usingDatabase() {
		rows, err := s.db.QueryContext(ctx, `SELECT sample_json FROM (
 SELECT edge_id, sample_json, row_number() OVER (PARTITION BY edge_id ORDER BY observed_at DESC, id) AS position
 FROM fugue_edge_network_samples
 WHERE edge_id = ANY($1::text[]) AND observed_at >= $2 AND sample_json->>'source' = 'public_front_tcp_info_v1'
) AS observations WHERE position <= $3 ORDER BY edge_id, position`, edgeIDs, since, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			var sample model.EdgeNetworkSample
			if err := rows.Scan(&raw); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(raw, &sample); err != nil {
				return nil, err
			}
			samples = append(samples, sample)
		}
		return samples, rows.Err()
	}
	byEdge := make(map[string][]model.EdgeNetworkSample)
	err := s.withLockedState(false, func(state *model.State) error {
		for _, sample := range state.EdgeNetworkSamples {
			if err := ctx.Err(); err != nil {
				return err
			}
			if wanted[sample.EdgeID] && sample.Source == "public_front_tcp_info_v1" && !sample.ObservedAt.Before(since) {
				byEdge[sample.EdgeID] = append(byEdge[sample.EdgeID], sample)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, edgeID := range slices.Sorted(slices.Values(edgeIDs)) {
		edge := byEdge[edgeID]
		slices.SortFunc(edge, func(a, b model.EdgeNetworkSample) int {
			if order := b.ObservedAt.Compare(a.ObservedAt); order != 0 {
				return order
			}
			return strings.Compare(a.ID, b.ID)
		})
		samples = append(samples, edge[:min(limit, len(edge))]...)
	}
	return samples, nil
}
