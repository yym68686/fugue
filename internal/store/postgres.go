package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	postgresStateTableSQL = `
CREATE TABLE IF NOT EXISTS fugue_state (
    id SMALLINT PRIMARY KEY,
    state JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`
	postgresBootstrapLockID int64 = 315609238744281
)

func (s *Store) ensureDatabaseReady() error {
	s.dbInitMu.Lock()
	defer s.dbInitMu.Unlock()

	if s.dbReady {
		return nil
	}
	if strings.TrimSpace(s.databaseURL) == "" {
		return fmt.Errorf("database url is empty")
	}

	if s.db == nil {
		db, err := sql.Open("pgx", s.databaseURL)
		if err != nil {
			return fmt.Errorf("open postgres: %w", err)
		}
		s.db = db
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, postgresStateTableSQL); err != nil {
		return fmt.Errorf("create fugue_state table: %w", err)
	}
	if err := s.bootstrapDatabaseState(ctx); err != nil {
		return err
	}

	s.dbReady = true
	return nil
}

func (s *Store) bootstrapDatabaseState(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bootstrap transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", postgresBootstrapLockID); err != nil {
		return fmt.Errorf("acquire postgres advisory lock: %w", err)
	}

	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM fugue_state WHERE id = 1)").Scan(&exists); err != nil {
		return fmt.Errorf("check fugue_state bootstrap row: %w", err)
	}
	if !exists {
		state, imported, err := s.loadLegacyFileState()
		if err != nil {
			return err
		}
		if !imported {
			state = model.State{}
		}
		ensureDefaults(&state)
		if err := s.writeDatabaseStateTx(ctx, tx, state); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bootstrap transaction: %w", err)
	}
	return nil
}

func (s *Store) loadLegacyFileState() (model.State, bool, error) {
	if strings.TrimSpace(s.path) == "" {
		return model.State{}, false, nil
	}

	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return model.State{}, false, nil
		}
		return model.State{}, false, fmt.Errorf("read legacy state file: %w", err)
	}
	if len(data) == 0 {
		return model.State{}, false, nil
	}

	var state model.State
	if err := json.Unmarshal(data, &state); err != nil {
		return model.State{}, false, fmt.Errorf("unmarshal legacy state file: %w", err)
	}
	return state, true, nil
}

func (s *Store) withDatabaseState(write bool, fn func(*model.State) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: !write})
	if err != nil {
		return fmt.Errorf("begin postgres transaction: %w", err)
	}
	defer tx.Rollback()

	state, err := s.readDatabaseStateTx(ctx, tx, write)
	if err != nil {
		return err
	}
	ensureDefaults(&state)

	if err := fn(&state); err != nil {
		return err
	}
	if write {
		if err := s.writeDatabaseStateTx(ctx, tx, state); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit postgres transaction: %w", err)
	}
	return nil
}

func (s *Store) readDatabaseStateTx(ctx context.Context, tx *sql.Tx, write bool) (model.State, error) {
	query := "SELECT state FROM fugue_state WHERE id = 1"
	if write {
		query += " FOR UPDATE"
	}

	var raw []byte
	err := tx.QueryRowContext(ctx, query).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.State{}, nil
		}
		return model.State{}, fmt.Errorf("read postgres state: %w", err)
	}
	if len(raw) == 0 {
		return model.State{}, nil
	}

	var state model.State
	if err := json.Unmarshal(raw, &state); err != nil {
		return model.State{}, fmt.Errorf("unmarshal postgres state: %w", err)
	}
	return state, nil
}

func (s *Store) writeDatabaseStateTx(ctx context.Context, tx *sql.Tx, state model.State) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal postgres state: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_state (id, state, updated_at)
VALUES (1, $1, NOW())
ON CONFLICT (id)
DO UPDATE SET state = EXCLUDED.state, updated_at = NOW()
`, data); err != nil {
		return fmt.Errorf("write postgres state: %w", err)
	}
	return nil
}
