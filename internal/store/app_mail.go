package store

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"fugue/internal/model"
)

func (s *Store) GetMailPlatformConfig() (model.MailPlatformConfig, error) {
	var out model.MailPlatformConfig
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var raw []byte
		if err := s.db.QueryRowContext(ctx, "SELECT body FROM fugue_mail_platform_config WHERE id='platform'").Scan(&raw); err != nil {
			return out, mapDBErr(err)
		}
		err := json.Unmarshal(raw, &out)
		return out, err
	}
	err := s.withLockedState(false, func(st *model.State) error {
		if st.MailPlatformConfig == nil {
			return ErrNotFound
		}
		out = *st.MailPlatformConfig
		return nil
	})
	return out, err
}

func (s *Store) SaveMailPlatformConfig(config model.MailPlatformConfig) error {
	if config.MailflareAppID == "" {
		return ErrInvalidInput
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		raw, err := json.Marshal(config)
		if err != nil {
			return err
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('fugue-mail-platform',0))"); err != nil {
			return err
		}
		var blocked bool
		if err = tx.QueryRowContext(ctx, `SELECT (EXISTS(SELECT 1 FROM fugue_app_mail_bindings WHERE body->>'state' <> 'revoked') AND $1 = false) OR (EXISTS(SELECT 1 FROM fugue_app_mail_bindings) AND EXISTS(SELECT 1 FROM fugue_mail_platform_config WHERE body->>'mailflare_app_id' <> $2))`, config.Enabled, config.MailflareAppID).Scan(&blocked); err != nil {
			return err
		}
		if blocked {
			return ErrConflict
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO fugue_mail_platform_config(id,body) VALUES('platform',$1::jsonb) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body", string(raw)); err != nil {
			return mapDBErr(err)
		}
		return tx.Commit()
	}
	return s.withLockedState(true, func(st *model.State) error {
		if st.MailPlatformConfig != nil && st.MailPlatformConfig.MailflareAppID != config.MailflareAppID && len(st.AppMailBindings) > 0 {
			return ErrConflict
		}
		if !config.Enabled {
			for _, b := range st.AppMailBindings {
				if b.State != "revoked" {
					return ErrConflict
				}
			}
		}
		st.MailPlatformConfig = &config
		return nil
	})
}

func (s *Store) GetAppMailBinding(appID string) (model.AppMailBinding, error) {
	var out model.AppMailBinding
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var raw []byte
		if err := s.db.QueryRowContext(ctx, "SELECT body FROM fugue_app_mail_bindings WHERE app_id=$1", appID).Scan(&raw); err != nil {
			return out, mapDBErr(err)
		}
		err := json.Unmarshal(raw, &out)
		return out, err
	}
	err := s.withLockedState(false, func(st *model.State) error {
		for _, v := range st.AppMailBindings {
			if v.AppID == appID {
				out = v
				return nil
			}
		}
		return ErrNotFound
	})
	return out, err
}

// Exact CAS; retries cannot overwrite a concurrent revocation. Identity fields
// cannot change, so a project transfer needs explicit suspension/migration.
func (s *Store) SaveAppMailBinding(v model.AppMailBinding, expected uint64) (model.AppMailBinding, error) {
	if v.AppID == "" || v.TenantID == "" || v.ProjectID == "" || v.Hostname == "" || v.LocalPart == "" || expected >= 9007199254740991 || (v.State != "active" && v.State != "suspended" && v.State != "revoked") {
		return v, ErrInvalidInput
	}
	prepare := func(prior *model.AppMailBinding) error {
		if prior != nil {
			if prior.Generation != expected || prior.TenantID != v.TenantID || prior.ProjectID != v.ProjectID || prior.Hostname != v.Hostname || prior.LocalPart != v.LocalPart || (prior.State == "revoked" && v.State != "revoked") {
				return ErrConflict
			}
			v.CreatedAt = prior.CreatedAt
		} else if expected != 0 || v.State != "active" {
			return ErrConflict
		}
		v.Generation = expected + 1
		v.UpdatedAt = time.Now().UTC()
		if v.CreatedAt.IsZero() {
			v.CreatedAt = v.UpdatedAt
		}
		return nil
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return v, err
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('fugue-mail-platform',0))"); err != nil {
			return v, err
		}
		if v.State == "active" {
			var enabled bool
			if err = tx.QueryRowContext(ctx, "SELECT COALESCE((SELECT (body->>'enabled')::boolean FROM fugue_mail_platform_config WHERE id='platform'),false)").Scan(&enabled); err != nil {
				return v, err
			}
			if !enabled {
				return v, ErrConflict
			}
		}
		if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "fugue-mail:"+v.AppID); err != nil {
			return v, err
		}
		var raw []byte
		err = tx.QueryRowContext(ctx, "SELECT body FROM fugue_app_mail_bindings WHERE app_id=$1 FOR UPDATE", v.AppID).Scan(&raw)
		var prior *model.AppMailBinding
		if err == nil {
			prior = &model.AppMailBinding{}
			if err = json.Unmarshal(raw, prior); err != nil {
				return v, err
			}
		} else if mapDBErr(err) != ErrNotFound {
			return v, err
		}
		if err = prepare(prior); err != nil {
			return v, err
		}
		raw, err = json.Marshal(v)
		if err != nil {
			return v, err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO fugue_app_mail_bindings(app_id,generation,body) VALUES($1,$2,$3::jsonb) ON CONFLICT(app_id) DO UPDATE SET generation=EXCLUDED.generation,body=EXCLUDED.body", v.AppID, v.Generation, string(raw))
		if err != nil {
			return v, mapDBErr(err)
		}
		return v, tx.Commit()
	}
	err := s.withLockedState(true, func(st *model.State) error {
		if v.State == "active" && (st.MailPlatformConfig == nil || !st.MailPlatformConfig.Enabled) {
			return ErrConflict
		}
		for i := range st.AppMailBindings {
			if st.AppMailBindings[i].AppID == v.AppID {
				if err := prepare(&st.AppMailBindings[i]); err != nil {
					return err
				}
				st.AppMailBindings[i] = v
				return nil
			}
		}
		if err := prepare(nil); err != nil {
			return err
		}
		st.AppMailBindings = append(st.AppMailBindings, v)
		return nil
	})
	return v, err
}

func (s *Store) ListAppMailBindings(after string, limit int) ([]model.AppMailBinding, error) {
	if limit < 1 || limit > 100 {
		return nil, ErrInvalidInput
	}
	out := []model.AppMailBinding{}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rows, err := s.db.QueryContext(ctx, "SELECT body FROM fugue_app_mail_bindings WHERE app_id > $1 ORDER BY app_id LIMIT $2", after, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			var v model.AppMailBinding
			if err = rows.Scan(&raw); err != nil {
				return nil, err
			}
			if err = json.Unmarshal(raw, &v); err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, rows.Err()
	}
	err := s.withLockedState(false, func(st *model.State) error {
		for _, v := range st.AppMailBindings {
			if v.AppID > after {
				out = append(out, v)
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].AppID < out[j].AppID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, err
}
