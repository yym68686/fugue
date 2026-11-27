package store

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"fugue/internal/model"
)

func (s *Store) GetMailPlatformConfig() (model.MailPlatformConfig, error) {
	var out model.MailPlatformConfig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var raw []byte
	if err := s.db.QueryRowContext(ctx, "SELECT body FROM fugue_mail_platform_config WHERE id='platform'").Scan(&raw); err != nil {
		return out, mapDBErr(err)
	}
	err := json.Unmarshal(raw, &out)
	return out, err
}

func (s *Store) SaveMailPlatformConfig(config model.MailPlatformConfig) error {
	if config.MailflareAppID == "" {
		return ErrInvalidInput
	}
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

func (s *Store) GetAppMailBinding(appID string) (model.AppMailBinding, error) {
	var out model.AppMailBinding
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var raw []byte
	if err := s.db.QueryRowContext(ctx, "SELECT body FROM fugue_app_mail_bindings WHERE app_id=$1", appID).Scan(&raw); err != nil {
		return out, mapDBErr(err)
	}
	err := json.Unmarshal(raw, &out)
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
			v.ProviderDNSRecords = prior.ProviderDNSRecords
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

// Provider observations are independent of user lifecycle generations. Exact
// identity/generation CAS prevents a delayed response from changing revoked DNS.
func (s *Store) SaveAppMailProviderDNS(binding model.AppMailBinding, records []model.AppMailDNSRecord) error {
	apply := func(current *model.AppMailBinding) error {
		if current.AppID != binding.AppID || current.TenantID != binding.TenantID || current.ProjectID != binding.ProjectID || current.Hostname != binding.Hostname || current.Generation != binding.Generation || current.State != "active" || binding.State != "active" {
			return ErrConflict
		}
		if !reflect.DeepEqual(current.ProviderDNSRecords, binding.ProviderDNSRecords) && !reflect.DeepEqual(current.ProviderDNSRecords, records) {
			return ErrConflict
		}
		current.ProviderDNSRecords = records
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "fugue-mail:"+binding.AppID); err != nil {
		return err
	}
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT body FROM fugue_app_mail_bindings WHERE app_id=$1 FOR UPDATE", binding.AppID).Scan(&raw); err != nil {
		return mapDBErr(err)
	}
	var current model.AppMailBinding
	if err = json.Unmarshal(raw, &current); err != nil {
		return err
	}
	unchanged := reflect.DeepEqual(current.ProviderDNSRecords, records)
	if err = apply(&current); err != nil {
		return err
	}
	if unchanged {
		return tx.Commit()
	}
	raw, err = json.Marshal(current)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE fugue_app_mail_bindings SET body=$2::jsonb WHERE app_id=$1", binding.AppID, string(raw)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ListAppMailBindings(after string, limit int) ([]model.AppMailBinding, error) {
	if limit < 1 || limit > 100 {
		return nil, ErrInvalidInput
	}
	out := []model.AppMailBinding{}
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
