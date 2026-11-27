package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"fugue/internal/tlsissuance"
)

const tlsIssuancePolicyKey = "tls-issuance/v1/policy"

func (s *Store) ReadTLSIssuancePolicy(ctx context.Context) (tlsissuance.Policy, error) {
	var p tlsissuance.Policy
	raw, err := s.readTLSIssuanceRecord(ctx, tlsIssuancePolicyKey)
	if err != nil || len(raw) == 0 {
		return p, err
	}
	err = json.Unmarshal(raw, &p)
	if err == nil {
		err = p.Validate()
	}
	return p, err
}

func (s *Store) PutTLSIssuancePolicy(ctx context.Context, p tlsissuance.Policy) error {
	if err := p.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	return s.updateTLSIssuanceRecord(ctx, tlsIssuancePolicyKey, func(raw []byte) ([]byte, error) {
		var old tlsissuance.Policy
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &old); err != nil {
				return nil, err
			}
		}
		next, _ := json.Marshal(p)
		if p.Generation == old.Generation {
			previous, _ := json.Marshal(old)
			if string(previous) == string(next) {
				return nil, nil
			}
		}
		if p.Generation != old.Generation+1 {
			return nil, ErrConflict
		}
		return next, nil
	})
}

func tlsIssuanceStateKey(_ string, directory, host string) string {
	// Changing issuer ownership must not reset a CA/domain retry barrier or
	// create another account. The authority generation fences ownership.
	return fmt.Sprintf("tls-issuance/v1/state/%x", sha256.Sum256([]byte(directory+"\x00"+host)))
}

func (s *Store) ReadTLSIssuanceState(ctx context.Context, issuer, directory, host string) (tlsissuance.State, error) {
	var state tlsissuance.State
	raw, err := s.readTLSIssuanceRecord(ctx, tlsIssuanceStateKey(issuer, directory, host))
	if err == nil && len(raw) > 0 {
		err = json.Unmarshal(raw, &state)
	}
	return state, err
}

func (s *Store) UpdateTLSIssuanceState(ctx context.Context, issuer, directory, host string, fn func(*tlsissuance.State) error) error {
	return s.updateTLSIssuanceRecord(ctx, tlsIssuanceStateKey(issuer, directory, host), func(raw []byte) ([]byte, error) {
		var state tlsissuance.State
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &state); err != nil {
				return nil, err
			}
		}
		if err := fn(&state); err != nil {
			return nil, err
		}
		return json.Marshal(state)
	})
}

// These are private issuance records, separate from serving configuration,
// positive LKG, diagnostic observations and their retention/compaction rules.
func (s *Store) readTLSIssuanceRecord(ctx context.Context, key string) ([]byte, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return []byte(value), err
}

func (s *Store) updateTLSIssuanceRecord(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var locked bool
	if err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, key).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return ErrConflict
	}
	var value string
	err = tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, key).Scan(&value)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	next, err := fn([]byte(value))
	if err != nil {
		return err
	}
	if len(next) > 1<<20 {
		return ErrInvalidInput
	}
	if next != nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_meta(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=NOW()`, key, string(next))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
