package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"fugue/internal/model"
)

func (s *Store) RecordEdgeNetworkSamples(ctx context.Context, samples []model.EdgeNetworkSample, pruneBefore time.Time) error {
	if len(samples) > 32 {
		return fmt.Errorf("network observation batch exceeds bound")
	}
	for _, sample := range samples {
		if err := model.ValidateEdgeNetworkSample(sample); err != nil {
			return err
		}
	}
	if s.usingDatabase() {
		transaction, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer transaction.Rollback()
		if !pruneBefore.IsZero() {
			if _, err := transaction.ExecContext(ctx, `DELETE FROM fugue_edge_network_samples WHERE observed_at < $1`, pruneBefore); err != nil {
				return err
			}
		}
		for _, sample := range samples {
			raw, err := json.Marshal(sample)
			if err != nil {
				return err
			}
			if _, err := transaction.ExecContext(ctx, `INSERT INTO fugue_edge_network_samples (edge_id, id, hostname, observed_at, sample_json) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (edge_id, id) DO NOTHING`, sample.EdgeID, sample.ID, sample.Hostname, sample.ObservedAt, raw); err != nil {
				return err
			}
		}
		return transaction.Commit()
	}
	return s.withLockedState(true, func(state *model.State) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		retained := []model.EdgeNetworkSample{}
		seen := map[string]bool{}
		for _, sample := range state.EdgeNetworkSamples {
			if !pruneBefore.IsZero() && sample.ObservedAt.Before(pruneBefore) {
				continue
			}
			retained = append(retained, sample)
			seen[sample.EdgeID+"\x00"+sample.ID] = true
		}
		for _, sample := range samples {
			key := sample.EdgeID + "\x00" + sample.ID
			if !seen[key] {
				retained = append(retained, sample)
				seen[key] = true
			}
		}
		state.EdgeNetworkSamples = retained
		return nil
	})
}

func (s *Store) ListEdgeNetworkSamples(ctx context.Context, hostname string, since time.Time, limit int) ([]model.EdgeNetworkSample, error) {
	if hostname == "" || limit < 1 || limit > 4096 || since.IsZero() {
		return nil, fmt.Errorf("bounded hostname network observation query required")
	}
	samples := []model.EdgeNetworkSample{}
	if s.usingDatabase() {
		rows, err := s.db.QueryContext(ctx, `SELECT sample_json FROM fugue_edge_network_samples WHERE hostname = $1 AND observed_at >= $2 ORDER BY observed_at DESC, edge_id, id LIMIT $3`, hostname, since, limit)
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
	err := s.withLockedState(false, func(state *model.State) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, sample := range state.EdgeNetworkSamples {
			if sample.Hostname == hostname && !sample.ObservedAt.Before(since) {
				samples = append(samples, sample)
			}
		}
		return nil
	})
	sort.Slice(samples, func(left, right int) bool {
		if !samples[left].ObservedAt.Equal(samples[right].ObservedAt) {
			return samples[left].ObservedAt.After(samples[right].ObservedAt)
		}
		if samples[left].EdgeID != samples[right].EdgeID {
			return samples[left].EdgeID < samples[right].EdgeID
		}
		return samples[left].ID < samples[right].ID
	})
	if len(samples) > limit {
		samples = samples[:limit]
	}
	return samples, err
}
