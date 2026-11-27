package store

import (
	"context"
	"encoding/json"
	"time"

	"fugue/internal/model"
)

// Read across domains in one database scan, bounded per physical edge so the
// incumbent cannot occupy the entire batch. No recovery or service probes.
func (s *Store) ListPassiveEdgeNetworkSamples(ctx context.Context, edgeIDs []string, since, before time.Time, limit int) ([]model.EdgeNetworkSample, error) {
	if len(edgeIDs) > 32 || since.IsZero() || !before.After(since) || limit < 1 || limit > 1024 {
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
	rows, err := s.db.QueryContext(ctx, `SELECT sample_json FROM (
 SELECT edge_id, sample_json, row_number() OVER (PARTITION BY edge_id ORDER BY (sample_json->>'source' = $4) DESC, observed_at DESC, id) AS position
 FROM fugue_edge_network_samples
 WHERE edge_id = ANY($1::text[]) AND observed_at >= $2 AND observed_at < $5 AND sample_json->>'source' IN ('public_front_tcp_info_v1', $4)
) AS observations WHERE position <= $3 ORDER BY edge_id, position`, edgeIDs, since, limit, model.EdgeNetworkPassiveFrontSource, before)
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
