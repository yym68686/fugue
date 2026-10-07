package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fugue/internal/imagecachepolicy"
	"fugue/internal/model"
)

func (s *Store) GetImageOrphanPolicy() (model.ImageOrphanPolicy, error) {
	p := imagecachepolicy.DefaultOrphanPolicy()
	if !s.usingDatabase() {
		err := s.withLockedState(false, func(st *model.State) error {
			if st.ImageOrphanPolicy != nil {
				p = *st.ImageOrphanPolicy
			}
			return nil
		})
		return p, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT body FROM fugue_image_orphan_policies ORDER BY generation DESC LIMIT 1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	err = json.Unmarshal(raw, &p)
	return p, err
}
func (s *Store) UpdateImageOrphanPolicy(p model.ImageOrphanPolicy, expected int64, actor string) (model.ImageOrphanPolicy, error) {
	p = imagecachepolicy.NormalizeOrphanPolicy(p)
	if expected < 0 {
		return p, ErrInvalidInput
	}
	if err := imagecachepolicy.ValidateOrphanPolicy(p); err != nil {
		return p, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	p.Generation = expected + 1
	p.UpdatedAt = time.Now().UTC()
	p.UpdatedBy = actor
	if !s.usingDatabase() {
		err := s.withLockedState(true, func(st *model.State) error {
			g := int64(0)
			if st.ImageOrphanPolicy != nil {
				g = st.ImageOrphanPolicy.Generation
			}
			if g != expected {
				return ErrConflict
			}
			st.ImageOrphanPolicy = &p
			st.ImageOrphanPolicyHistory = append(st.ImageOrphanPolicyHistory, p)
			return nil
		})
		return p, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return p, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(315609238744292)`); err != nil {
		return p, err
	}
	var g int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(generation),0) FROM fugue_image_orphan_policies`).Scan(&g); err != nil {
		return p, err
	}
	if g != expected {
		return p, ErrConflict
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return p, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_image_orphan_policies(generation,body) VALUES($1,$2)`, p.Generation, raw); err != nil {
		return p, err
	}
	return p, tx.Commit()
}
func (s *Store) ListImageOrphanDecisions() ([]model.ImageOrphanDecision, error) {
	out := []model.ImageOrphanDecision{}
	if !s.usingDatabase() {
		err := s.withLockedState(false, func(st *model.State) error { out = append(out, st.ImageOrphanDecisions...); return nil })
		return out, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT body FROM fugue_image_orphan_decisions ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var d model.ImageOrphanDecision
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &d); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Persist observations in one transaction; immutable transition events retain
// retirement/revocation evidence even when a current decision is superseded.
func (s *Store) SaveImageOrphanDecisions(items []model.ImageOrphanDecision) error {
	if len(items) == 0 {
		return nil
	}
	if !s.usingDatabase() {
		return s.withLockedState(true, func(st *model.State) error {
			idx := map[string]int{}
			for i, d := range st.ImageOrphanDecisions {
				idx[d.ID] = i
			}
			for _, d := range items {
				if i, ok := idx[d.ID]; ok {
					st.ImageOrphanDecisions[i] = d
				} else {
					idx[d.ID] = len(st.ImageOrphanDecisions)
					st.ImageOrphanDecisions = append(st.ImageOrphanDecisions, d)
				}
			}
			return nil
		})
	}
	raw, err := json.Marshal(items)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `WITH incoming AS (SELECT value body FROM jsonb_array_elements($1::jsonb)) INSERT INTO fugue_image_orphan_events(decision_id,body) SELECT i.body->>'id',i.body FROM incoming i LEFT JOIN fugue_image_orphan_decisions d ON d.id=i.body->>'id' WHERE d.id IS NULL OR d.body->>'state' IS DISTINCT FROM i.body->>'state' OR d.body->>'policy_generation' IS DISTINCT FROM i.body->>'policy_generation'`, raw); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_image_orphan_decisions(id,body) SELECT value->>'id',value FROM jsonb_array_elements($1::jsonb) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body`, raw); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) ImageOrphanContext(now time.Time) (model.ImageOrphanPolicy, map[string]model.ImageOrphanDecision, bool, string, error) {
	p, err := s.GetImageOrphanPolicy()
	if err != nil {
		return p, nil, false, "", err
	}
	items, err := s.ListImageOrphanDecisions()
	if err != nil {
		return p, nil, false, "", err
	}
	byID := map[string]model.ImageOrphanDecision{}
	for _, d := range items {
		byID[d.ID] = d
	}
	nodes, err := s.ListImageCacheNodeInventories(model.ImageCacheNodeInventoryFilter{})
	if err != nil {
		return p, nil, false, "", err
	}
	updaters, err := s.ListNodeUpdaters("", true)
	if err != nil {
		return p, nil, false, "", err
	}
	ok, reason := imagecachepolicy.OrphanCoverage(p, nodes, updaters, now)
	return p, byID, ok, reason, nil
}
