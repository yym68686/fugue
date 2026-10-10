package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"fugue/internal/clientmeasurement"
	"fugue/internal/model"
)

func (s *Store) RecordEdgeClientProbeReport(ctx context.Context, report model.EdgeClientProbeReport) error {
	if _, err := clientmeasurement.ValidateReport(report); err != nil {
		return err
	}
	raw, err := json.Marshal(report)
	if err != nil || len(raw) > 128<<10 {
		return errors.New("client measurement report exceeds bound")
	}
	permit := report.Plan.Permits[0]
	if s.usingDatabase() {
		var retained []byte
		if err := s.db.QueryRowContext(ctx, `INSERT INTO fugue_edge_network_samples (edge_id, id, hostname, observed_at, sample_json) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (edge_id, id) DO UPDATE SET sample_json = fugue_edge_network_samples.sample_json RETURNING sample_json`, "client-probe-report", report.Plan.RoundID, "client-probe:"+permit.Hostname, permit.IssuedAt, raw).Scan(&retained); err != nil {
			return err
		}
		var existing model.EdgeClientProbeReport
		if json.Unmarshal(retained, &existing) != nil || !reflect.DeepEqual(existing, report) {
			return errors.New("client measurement round was already retained with different outcomes")
		}
		return nil
	}
	return s.withLockedState(true, func(state *model.State) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		retained := make([]model.EdgeClientProbeReport, 0, len(state.EdgeClientProbeReports)+1)
		for _, existing := range state.EdgeClientProbeReports {
			if existing.Plan.RoundID == report.Plan.RoundID {
				if !reflect.DeepEqual(existing, report) {
					return errors.New("client measurement round was already retained with different outcomes")
				}
				return nil
			}
			if len(existing.Plan.Permits) > 0 && existing.Plan.Permits[0].IssuedAt.After(permit.IssuedAt.Add(-time.Hour)) {
				retained = append(retained, existing)
			}
		}
		state.EdgeClientProbeReports = append(retained, report)
		return nil
	})
}

func (s *Store) ListEdgeClientProbeReports(ctx context.Context, hostname string, since time.Time, limit int) ([]model.EdgeClientProbeReport, error) {
	if hostname == "" || since.IsZero() || limit < 1 || limit > 256 {
		return nil, errors.New("bounded client measurement query required")
	}
	reports := []model.EdgeClientProbeReport{}
	if s.usingDatabase() {
		rows, err := s.db.QueryContext(ctx, `SELECT sample_json FROM fugue_edge_network_samples WHERE hostname = $1 AND observed_at >= $2 ORDER BY observed_at DESC, edge_id, id LIMIT $3`, "client-probe:"+hostname, since, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			var report model.EdgeClientProbeReport
			if err := rows.Scan(&raw); err != nil {
				return nil, err
			}
			if json.Unmarshal(raw, &report) != nil {
				return nil, errors.New("stored client measurement invalid")
			}
			reports = append(reports, report)
		}
		return reports, rows.Err()
	}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, report := range state.EdgeClientProbeReports {
			if len(report.Plan.Permits) > 0 && report.Plan.Permits[0].Hostname == hostname && !report.Plan.Permits[0].IssuedAt.Before(since) {
				reports = append(reports, report)
			}
		}
		return ctx.Err()
	})
	sort.Slice(reports, func(left, right int) bool {
		return reports[left].Plan.Permits[0].IssuedAt.After(reports[right].Plan.Permits[0].IssuedAt)
	})
	if len(reports) > limit {
		reports = reports[:limit]
	}
	return reports, err
}
