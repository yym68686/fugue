package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

type sqlScanner interface {
	Scan(dest ...any) error
}

func (s *Store) pgListTenants() ([]model.Tenant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, slug, status, created_at, updated_at
FROM fugue_tenants
ORDER BY created_at ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()

	tenants := make([]model.Tenant, 0)
	for rows.Next() {
		tenant, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		tenants = append(tenants, tenant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenants: %w", err)
	}
	return tenants, nil
}

func (s *Store) pgGetTenant(id string) (model.Tenant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tenant, err := scanTenant(s.db.QueryRowContext(ctx, `
SELECT id, name, slug, status, created_at, updated_at
FROM fugue_tenants
WHERE id = $1
`, id))
	if err != nil {
		return model.Tenant{}, mapDBErr(err)
	}
	return tenant, nil
}

func (s *Store) pgCreateTenant(name string) (model.Tenant, error) {
	tenant := model.Tenant{
		ID:        model.NewID("tenant"),
		Name:      name,
		Slug:      model.Slugify(name),
		Status:    "active",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Tenant{}, fmt.Errorf("begin create tenant transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_tenants (id, name, slug, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
`, tenant.ID, tenant.Name, tenant.Slug, tenant.Status, tenant.CreatedAt, tenant.UpdatedAt); err != nil {
		return model.Tenant{}, mapDBErr(err)
	}
	if _, err := s.pgEnsureTenantBillingRecordTx(ctx, tx, tenant.ID, true, tenant.CreatedAt); err != nil {
		return model.Tenant{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Tenant{}, fmt.Errorf("commit create tenant transaction: %w", err)
	}
	return tenant, nil
}

func (s *Store) pgDeleteTenant(id string) (model.Tenant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Tenant{}, fmt.Errorf("begin delete tenant transaction: %w", err)
	}
	defer tx.Rollback()

	tenant, err := scanTenant(tx.QueryRowContext(ctx, `
SELECT id, name, slug, status, created_at, updated_at
FROM fugue_tenants
WHERE id = $1
FOR UPDATE
`, id))
	if err != nil {
		return model.Tenant{}, mapDBErr(err)
	}

	if _, err := tx.ExecContext(ctx, `
DELETE FROM fugue_audit_events
WHERE tenant_id = $1
`, id); err != nil {
		return model.Tenant{}, fmt.Errorf("delete tenant audit events: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
DELETE FROM fugue_tenants
WHERE id = $1
`, id); err != nil {
		return model.Tenant{}, fmt.Errorf("delete tenant: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return model.Tenant{}, fmt.Errorf("commit delete tenant transaction: %w", err)
	}
	return tenant, nil
}

func (s *Store) pgListProjects(tenantID string) ([]model.Project, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT id, tenant_id, name, slug, description, created_at, updated_at
FROM fugue_projects
WHERE tenant_id = $1
ORDER BY created_at ASC
`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()

	projects := make([]model.Project, 0)
	for rows.Next() {
		project, err := scanProject(rows)
		if err != nil {
			return nil, err
		}
		projects = append(projects, project)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate projects: %w", err)
	}
	return projects, nil
}

func (s *Store) pgGetProject(id string) (model.Project, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	project, err := scanProject(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, name, slug, description, created_at, updated_at
FROM fugue_projects
WHERE id = $1
`, id))
	if err != nil {
		return model.Project{}, mapDBErr(err)
	}
	return project, nil
}

func (s *Store) pgCreateProject(tenantID, name, description string) (model.Project, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Project{}, fmt.Errorf("begin create project transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
	if err != nil {
		return model.Project{}, err
	}
	if !exists {
		return model.Project{}, ErrNotFound
	}

	project := model.Project{
		ID:          model.NewID("project"),
		TenantID:    tenantID,
		Name:        name,
		Slug:        model.Slugify(name),
		Description: strings.TrimSpace(description),
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_projects (id, tenant_id, name, slug, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
`, project.ID, project.TenantID, project.Name, project.Slug, project.Description, project.CreatedAt, project.UpdatedAt); err != nil {
		return model.Project{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.Project{}, fmt.Errorf("commit create project transaction: %w", err)
	}
	return project, nil
}

func (s *Store) pgUpdateProject(id string, name, description *string) (model.Project, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Project{}, fmt.Errorf("begin update project transaction: %w", err)
	}
	defer tx.Rollback()

	project, err := scanProject(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, slug, description, created_at, updated_at
FROM fugue_projects
WHERE id = $1
FOR UPDATE
`, id))
	if err != nil {
		return model.Project{}, mapDBErr(err)
	}

	changed := false
	if name != nil {
		trimmedName := strings.TrimSpace(*name)
		if trimmedName == "" {
			return model.Project{}, ErrInvalidInput
		}
		slug := model.Slugify(trimmedName)
		var exists bool
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_projects
	WHERE tenant_id = $1
	  AND lower(slug) = lower($2)
	  AND id <> $3
)
`, project.TenantID, slug, project.ID).Scan(&exists); err != nil {
			return model.Project{}, fmt.Errorf("check project slug conflict: %w", err)
		}
		if exists {
			return model.Project{}, ErrConflict
		}
		if project.Name != trimmedName || project.Slug != slug {
			project.Name = trimmedName
			project.Slug = slug
			changed = true
		}
	}

	if description != nil {
		trimmedDescription := strings.TrimSpace(*description)
		if project.Description != trimmedDescription {
			project.Description = trimmedDescription
			changed = true
		}
	}

	if changed {
		project.UpdatedAt = time.Now().UTC()
		if _, err := tx.ExecContext(ctx, `
UPDATE fugue_projects
SET tenant_id = $2,
	name = $3,
	slug = $4,
	description = $5,
	created_at = $6,
	updated_at = $7
WHERE id = $1
`, project.ID, project.TenantID, project.Name, project.Slug, project.Description, project.CreatedAt, project.UpdatedAt); err != nil {
			return model.Project{}, mapDBErr(err)
		}
	}

	if err := tx.Commit(); err != nil {
		return model.Project{}, fmt.Errorf("commit update project transaction: %w", err)
	}
	return project, nil
}

func (s *Store) pgDeleteProject(id string) (model.Project, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Project{}, fmt.Errorf("begin delete project transaction: %w", err)
	}
	defer tx.Rollback()

	project, err := scanProject(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, slug, description, created_at, updated_at
FROM fugue_projects
WHERE id = $1
FOR UPDATE
`, id))
	if err != nil {
		return model.Project{}, mapDBErr(err)
	}

	if live, err := s.pgProjectHasLiveResourcesTx(ctx, tx, project.ID); err != nil {
		return model.Project{}, err
	} else if live {
		return model.Project{}, ErrConflict
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_projects WHERE id = $1`, project.ID); err != nil {
		return model.Project{}, fmt.Errorf("delete project %s: %w", project.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return model.Project{}, fmt.Errorf("commit delete project transaction: %w", err)
	}
	return project, nil
}

func (s *Store) pgEnsureDefaultProject(tenantID string) (model.Project, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Project{}, false, fmt.Errorf("begin ensure default project transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
	if err != nil {
		return model.Project{}, false, err
	}
	if !exists {
		return model.Project{}, false, ErrNotFound
	}

	project, err := scanProject(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, slug, description, created_at, updated_at
FROM fugue_projects
WHERE tenant_id = $1
  AND slug = 'default'
`, tenantID))
	if err == nil {
		if err := tx.Commit(); err != nil {
			return model.Project{}, false, fmt.Errorf("commit ensure default project transaction: %w", err)
		}
		return project, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.Project{}, false, mapDBErr(err)
	}

	now := time.Now().UTC()
	project, err = scanProject(tx.QueryRowContext(ctx, `
INSERT INTO fugue_projects (id, tenant_id, name, slug, description, created_at, updated_at)
VALUES ($1, $2, 'default', 'default', 'default project', $3, $4)
RETURNING id, tenant_id, name, slug, description, created_at, updated_at
`, model.NewID("project"), tenantID, now, now))
	if err != nil {
		return model.Project{}, false, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.Project{}, false, fmt.Errorf("commit ensure default project transaction: %w", err)
	}
	return project, true, nil
}

func (s *Store) pgListAPIKeys(tenantID string, platformAdmin bool) ([]model.APIKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at
FROM fugue_api_keys
`
	args := make([]any, 0, 1)
	if !platformAdmin {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	defer rows.Close()

	keys := make([]model.APIKey, 0)
	for rows.Next() {
		key, err := scanAPIKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, redactAPIKey(key))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate api keys: %w", err)
	}
	return keys, nil
}

func (s *Store) pgGetAPIKey(id string) (model.APIKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key, err := scanAPIKey(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at
FROM fugue_api_keys
WHERE id = $1
`, id))
	if err != nil {
		return model.APIKey{}, mapDBErr(err)
	}
	return key, nil
}

func (s *Store) pgCreateAPIKey(tenantID, label string, scopes []string) (model.APIKey, string, error) {
	secret := model.NewSecret("fugue_pk")
	key := model.APIKey{
		ID:        model.NewID("apikey"),
		TenantID:  tenantID,
		Label:     label,
		Prefix:    model.SecretPrefix(secret),
		Hash:      model.HashSecret(secret),
		Status:    model.APIKeyStatusActive,
		Scopes:    model.NormalizeScopes(scopes),
		CreatedAt: time.Now().UTC(),
	}
	scopesJSON, err := marshalJSON(key.Scopes)
	if err != nil {
		return model.APIKey{}, "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.APIKey{}, "", fmt.Errorf("begin create api key transaction: %w", err)
	}
	defer tx.Rollback()

	if tenantID != "" {
		exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
		if err != nil {
			return model.APIKey{}, "", err
		}
		if !exists {
			return model.APIKey{}, "", ErrNotFound
		}
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_api_keys (id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL)
`, key.ID, nullIfEmpty(key.TenantID), key.Label, key.Prefix, key.Hash, key.Status, scopesJSON, key.CreatedAt); err != nil {
		return model.APIKey{}, "", mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.APIKey{}, "", fmt.Errorf("commit create api key transaction: %w", err)
	}

	return redactAPIKey(key), secret, nil
}

func (s *Store) pgUpdateAPIKey(id string, label *string, scopes *[]string) (model.APIKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.APIKey{}, fmt.Errorf("begin update api key transaction: %w", err)
	}
	defer tx.Rollback()

	current, err := scanAPIKey(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at
FROM fugue_api_keys
WHERE id = $1
FOR UPDATE
`, id))
	if err != nil {
		return model.APIKey{}, mapDBErr(err)
	}
	updated, err := applyAPIKeyUpdates(current, label, scopes)
	if err != nil {
		return model.APIKey{}, err
	}
	scopesJSON, err := marshalJSON(updated.Scopes)
	if err != nil {
		return model.APIKey{}, err
	}

	key, err := scanAPIKey(tx.QueryRowContext(ctx, `
UPDATE fugue_api_keys
SET label = $2,
	scopes_json = $3
WHERE id = $1
RETURNING id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at
`, id, updated.Label, scopesJSON))
	if err != nil {
		return model.APIKey{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.APIKey{}, fmt.Errorf("commit update api key transaction: %w", err)
	}
	return redactAPIKey(key), nil
}

func (s *Store) pgRotateAPIKey(id string, label *string, scopes *[]string) (model.APIKey, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.APIKey{}, "", fmt.Errorf("begin rotate api key transaction: %w", err)
	}
	defer tx.Rollback()

	current, err := scanAPIKey(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at
FROM fugue_api_keys
WHERE id = $1
FOR UPDATE
`, id))
	if err != nil {
		return model.APIKey{}, "", mapDBErr(err)
	}
	updated, err := applyAPIKeyUpdates(current, label, scopes)
	if err != nil {
		return model.APIKey{}, "", err
	}

	secret := model.NewSecret("fugue_pk")
	updated.Prefix = model.SecretPrefix(secret)
	updated.Hash = model.HashSecret(secret)

	scopesJSON, err := marshalJSON(updated.Scopes)
	if err != nil {
		return model.APIKey{}, "", err
	}
	key, err := scanAPIKey(tx.QueryRowContext(ctx, `
UPDATE fugue_api_keys
SET label = $2,
	prefix = $3,
	hash = $4,
	scopes_json = $5
WHERE id = $1
RETURNING id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at
`, id, updated.Label, updated.Prefix, updated.Hash, scopesJSON))
	if err != nil {
		return model.APIKey{}, "", mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.APIKey{}, "", fmt.Errorf("commit rotate api key transaction: %w", err)
	}
	return redactAPIKey(key), secret, nil
}

func (s *Store) pgDisableAPIKey(id string) (model.APIKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	key, err := scanAPIKey(s.db.QueryRowContext(ctx, `
UPDATE fugue_api_keys
SET status = $2,
	disabled_at = COALESCE(disabled_at, $3)
WHERE id = $1
RETURNING id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at
`, id, model.APIKeyStatusDisabled, now))
	if err != nil {
		return model.APIKey{}, mapDBErr(err)
	}
	return redactAPIKey(key), nil
}

func (s *Store) pgEnableAPIKey(id string) (model.APIKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key, err := scanAPIKey(s.db.QueryRowContext(ctx, `
UPDATE fugue_api_keys
SET status = $2,
	disabled_at = NULL
WHERE id = $1
RETURNING id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at
`, id, model.APIKeyStatusActive))
	if err != nil {
		return model.APIKey{}, mapDBErr(err)
	}
	return redactAPIKey(key), nil
}

func (s *Store) pgDeleteAPIKey(id string) (model.APIKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key, err := scanAPIKey(s.db.QueryRowContext(ctx, `
DELETE FROM fugue_api_keys
WHERE id = $1
RETURNING id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at
`, id))
	if err != nil {
		return model.APIKey{}, mapDBErr(err)
	}
	return redactAPIKey(key), nil
}

func (s *Store) pgAuthenticateAPIKey(secret string) (model.Principal, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key, err := scanAPIKey(s.db.QueryRowContext(ctx, `
UPDATE fugue_api_keys
SET last_used_at = NOW()
WHERE hash = $1
  AND status = $2
RETURNING id, tenant_id, label, prefix, hash, status, scopes_json, created_at, last_used_at, disabled_at
`, model.HashSecret(secret), model.APIKeyStatusActive))
	if err != nil {
		return model.Principal{}, mapDBErr(err)
	}

	scopes := make(map[string]struct{}, len(key.Scopes))
	for _, scope := range key.Scopes {
		scopes[scope] = struct{}{}
	}
	return model.Principal{
		ActorType: model.ActorTypeAPIKey,
		ActorID:   key.ID,
		TenantID:  key.TenantID,
		Scopes:    scopes,
	}, nil
}

func (s *Store) pgListEnrollmentTokens(tenantID string) ([]model.EnrollmentToken, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, expires_at, used_at, created_at, last_used_at
FROM fugue_enrollment_tokens
WHERE tenant_id = $1
ORDER BY created_at ASC
`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list enrollment tokens: %w", err)
	}
	defer rows.Close()

	tokens := make([]model.EnrollmentToken, 0)
	for rows.Next() {
		token, err := scanEnrollmentToken(rows)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, redactEnrollmentToken(token))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate enrollment tokens: %w", err)
	}
	return tokens, nil
}

func (s *Store) pgCreateEnrollmentToken(tenantID, label string, ttl time.Duration) (model.EnrollmentToken, string, error) {
	secret := model.NewSecret("fugue_enroll")
	token := model.EnrollmentToken{
		ID:        model.NewID("enroll"),
		TenantID:  tenantID,
		Label:     label,
		Prefix:    model.SecretPrefix(secret),
		Hash:      model.HashSecret(secret),
		ExpiresAt: time.Now().UTC().Add(ttl),
		CreatedAt: time.Now().UTC(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.EnrollmentToken{}, "", fmt.Errorf("begin create enrollment token transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
	if err != nil {
		return model.EnrollmentToken{}, "", err
	}
	if !exists {
		return model.EnrollmentToken{}, "", ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_enrollment_tokens (id, tenant_id, label, prefix, hash, expires_at, used_at, created_at, last_used_at)
VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, NULL)
`, token.ID, token.TenantID, token.Label, token.Prefix, token.Hash, token.ExpiresAt, token.CreatedAt); err != nil {
		return model.EnrollmentToken{}, "", mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.EnrollmentToken{}, "", fmt.Errorf("commit create enrollment token transaction: %w", err)
	}
	return redactEnrollmentToken(token), secret, nil
}

func (s *Store) pgListNodeKeys(tenantID string, platformAdmin bool) ([]model.NodeKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, label, prefix, hash, status, created_at, updated_at, last_used_at, revoked_at
FROM fugue_node_keys
`
	args := make([]any, 0, 1)
	if !platformAdmin {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list node keys: %w", err)
	}
	defer rows.Close()

	keys := make([]model.NodeKey, 0)
	for rows.Next() {
		key, err := scanNodeKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, redactNodeKey(key))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate node keys: %w", err)
	}
	return keys, nil
}

func (s *Store) pgGetNodeKey(id string) (model.NodeKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key, err := scanNodeKey(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, status, created_at, updated_at, last_used_at, revoked_at
FROM fugue_node_keys
WHERE id = $1
`, id))
	if err != nil {
		return model.NodeKey{}, mapDBErr(err)
	}
	return key, nil
}

func (s *Store) pgAuthenticateNodeKey(secret string) (model.NodeKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.NodeKey{}, fmt.Errorf("begin authenticate node key transaction: %w", err)
	}
	defer tx.Rollback()

	key, err := scanNodeKey(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, status, created_at, updated_at, last_used_at, revoked_at
FROM fugue_node_keys
WHERE hash = $1
FOR UPDATE
`, model.HashSecret(secret)))
	if err != nil {
		return model.NodeKey{}, mapDBErr(err)
	}
	if key.RevokedAt != nil || key.Status == model.NodeKeyStatusRevoked {
		return model.NodeKey{}, ErrConflict
	}

	now := time.Now().UTC()
	key.LastUsedAt = &now
	key.UpdatedAt = now
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_node_keys
SET last_used_at = $2, updated_at = $3
WHERE id = $1
`, key.ID, key.LastUsedAt, key.UpdatedAt); err != nil {
		return model.NodeKey{}, fmt.Errorf("update node key last_used_at: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return model.NodeKey{}, fmt.Errorf("commit authenticate node key transaction: %w", err)
	}
	return redactNodeKey(key), nil
}

func (s *Store) pgCreateNodeKey(tenantID, label string) (model.NodeKey, string, error) {
	label = defaultNodeKeyLabel(label)
	secret := model.NewSecret("fugue_nk")
	now := time.Now().UTC()
	key := model.NodeKey{
		ID:        model.NewID("nodekey"),
		TenantID:  tenantID,
		Label:     label,
		Prefix:    model.SecretPrefix(secret),
		Hash:      model.HashSecret(secret),
		Status:    model.NodeKeyStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.NodeKey{}, "", fmt.Errorf("begin create node key transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
	if err != nil {
		return model.NodeKey{}, "", err
	}
	if !exists {
		return model.NodeKey{}, "", ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_node_keys (id, tenant_id, label, prefix, hash, status, created_at, updated_at, last_used_at, revoked_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL)
`, key.ID, key.TenantID, key.Label, key.Prefix, key.Hash, key.Status, key.CreatedAt, key.UpdatedAt); err != nil {
		return model.NodeKey{}, "", mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.NodeKey{}, "", fmt.Errorf("commit create node key transaction: %w", err)
	}
	return redactNodeKey(key), secret, nil
}

func (s *Store) pgRevokeNodeKey(id string) (model.NodeKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.NodeKey{}, fmt.Errorf("begin revoke node key transaction: %w", err)
	}
	defer tx.Rollback()

	key, err := scanNodeKey(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, status, created_at, updated_at, last_used_at, revoked_at
FROM fugue_node_keys
WHERE id = $1
FOR UPDATE
`, id))
	if err != nil {
		return model.NodeKey{}, mapDBErr(err)
	}

	if key.RevokedAt == nil {
		now := time.Now().UTC()
		key.Status = model.NodeKeyStatusRevoked
		key.RevokedAt = &now
		key.UpdatedAt = now
		if _, err := tx.ExecContext(ctx, `
UPDATE fugue_node_keys
SET status = $2, revoked_at = $3, updated_at = $4
WHERE id = $1
`, key.ID, key.Status, key.RevokedAt, key.UpdatedAt); err != nil {
			return model.NodeKey{}, fmt.Errorf("update node key revoke: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return model.NodeKey{}, fmt.Errorf("commit revoke node key transaction: %w", err)
	}
	return redactNodeKey(key), nil
}

func (s *Store) pgBootstrapNode(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.NodeKey, model.Runtime, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", fmt.Errorf("begin bootstrap node transaction: %w", err)
	}
	defer tx.Rollback()

	key, err := scanNodeKey(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, status, created_at, updated_at, last_used_at, revoked_at
FROM fugue_node_keys
WHERE hash = $1
FOR UPDATE
`, model.HashSecret(secret)))
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", mapDBErr(err)
	}
	if key.RevokedAt != nil || key.Status == model.NodeKeyStatusRevoked {
		return model.NodeKey{}, model.Runtime{}, "", ErrConflict
	}

	now := time.Now().UTC()
	key.LastUsedAt = &now
	key.UpdatedAt = now
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_node_keys
SET last_used_at = $2, updated_at = $3
WHERE id = $1
`, key.ID, key.LastUsedAt, key.UpdatedAt); err != nil {
		return model.NodeKey{}, model.Runtime{}, "", fmt.Errorf("update node key last_used_at: %w", err)
	}

	endpoint = strings.TrimSpace(endpoint)
	explicitFingerprint := strings.TrimSpace(machineFingerprint) != ""
	machineName = normalizedMachineName(machineName, runtimeName, endpoint)
	machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, runtimeName, endpoint)
	fingerprintHash := model.HashSecret(machineFingerprint)

	runtime, found, err := s.pgFindRuntimeByFingerprintTx(ctx, tx, key.TenantID, fingerprintHash, true)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", err
	}
	var matchingRuntimes []model.Runtime
	if explicitFingerprint {
		matchingRuntimes, err = s.pgListRuntimesByFingerprintTx(ctx, tx, fingerprintHash, true)
		if err != nil {
			return model.NodeKey{}, model.Runtime{}, "", err
		}
	}
	if !found && explicitFingerprint {
		runtime, found, err = s.pgFindRuntimeCandidateTx(ctx, tx, key.TenantID, key.ID, model.RuntimeTypeExternalOwned, machineName, runtimeName, endpoint)
		if err != nil {
			return model.NodeKey{}, model.Runtime{}, "", err
		}
	}
	transferByFingerprint := explicitFingerprint && len(matchingRuntimes) > 0 && (runtime.ID == "" || runtime.FingerprintHash != fingerprintHash || runtime.TenantID != key.TenantID)
	if explicitFingerprint {
		keepRuntimeID := ""
		if runtime.FingerprintHash == fingerprintHash && runtime.TenantID == key.TenantID {
			keepRuntimeID = runtime.ID
		}
		for _, conflict := range matchingRuntimes {
			if conflict.ID == keepRuntimeID {
				continue
			}
			if err := s.pgDetachRuntimeOwnershipTx(ctx, tx, &conflict, now); err != nil {
				return model.NodeKey{}, model.Runtime{}, "", err
			}
		}
	}

	runtimeSecret := model.NewSecret("fugue_rt")

	if found {
		if transferByFingerprint {
			if err := s.pgDeleteRuntimeAccessGrantsTx(ctx, tx, runtime.ID); err != nil {
				return model.NodeKey{}, model.Runtime{}, "", err
			}
			runtime.AccessMode = model.RuntimeAccessModePrivate
		}
		runtime.Type = model.RuntimeTypeExternalOwned
		runtime.Status = model.RuntimeStatusActive
		runtime.TenantID = key.TenantID
		runtime.Endpoint = endpoint
		runtime.Labels = cloneMap(labels)
		runtime.NodeKeyID = key.ID
		runtime.AgentKeyPrefix = model.SecretPrefix(runtimeSecret)
		runtime.AgentKeyHash = model.HashSecret(runtimeSecret)
		runtime.LastSeenAt = &now
		runtime.LastHeartbeatAt = &now
		runtime.UpdatedAt = now
		applyRuntimeIdentity(&runtime, machineName, machineFingerprint, model.RuntimeTypeExternalOwned, endpoint, labels, key.ID, now)
		if err := s.pgUpdateRuntimeTx(ctx, tx, runtime); err != nil {
			return model.NodeKey{}, model.Runtime{}, "", err
		}
		if err := tx.Commit(); err != nil {
			return model.NodeKey{}, model.Runtime{}, "", fmt.Errorf("commit bootstrap node transaction: %w", err)
		}
		return redactNodeKey(key), runtime, runtimeSecret, nil
	}

	name, err := s.pgNextAvailableRuntimeNameTx(ctx, tx, key.TenantID, runtimeName)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", err
	}
	runtime = model.Runtime{
		ID:              model.NewID("runtime"),
		TenantID:        key.TenantID,
		Name:            name,
		Type:            model.RuntimeTypeExternalOwned,
		AccessMode:      model.RuntimeAccessModePrivate,
		PoolMode:        model.RuntimePoolModeDedicated,
		Status:          model.RuntimeStatusActive,
		Endpoint:        endpoint,
		Labels:          cloneMap(labels),
		NodeKeyID:       key.ID,
		AgentKeyPrefix:  model.SecretPrefix(runtimeSecret),
		AgentKeyHash:    model.HashSecret(runtimeSecret),
		LastSeenAt:      &now,
		LastHeartbeatAt: &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyRuntimeIdentity(&runtime, machineName, machineFingerprint, model.RuntimeTypeExternalOwned, endpoint, labels, key.ID, now)
	if err := s.pgInsertRuntimeTx(ctx, tx, runtime); err != nil {
		return model.NodeKey{}, model.Runtime{}, "", err
	}

	if err := tx.Commit(); err != nil {
		return model.NodeKey{}, model.Runtime{}, "", fmt.Errorf("commit bootstrap node transaction: %w", err)
	}
	return redactNodeKey(key), runtime, runtimeSecret, nil
}

func (s *Store) pgBootstrapClusterNode(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.NodeKey, model.Runtime, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, fmt.Errorf("begin bootstrap cluster node transaction: %w", err)
	}
	defer tx.Rollback()

	key, err := scanNodeKey(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, status, created_at, updated_at, last_used_at, revoked_at
FROM fugue_node_keys
WHERE hash = $1
FOR UPDATE
`, model.HashSecret(secret)))
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, mapDBErr(err)
	}
	if key.RevokedAt != nil || key.Status == model.NodeKeyStatusRevoked {
		return model.NodeKey{}, model.Runtime{}, ErrConflict
	}

	now := time.Now().UTC()
	key.LastUsedAt = &now
	key.UpdatedAt = now
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_node_keys
SET last_used_at = $2, updated_at = $3
WHERE id = $1
`, key.ID, key.LastUsedAt, key.UpdatedAt); err != nil {
		return model.NodeKey{}, model.Runtime{}, fmt.Errorf("update node key last_used_at: %w", err)
	}

	endpoint = strings.TrimSpace(endpoint)
	explicitFingerprint := strings.TrimSpace(machineFingerprint) != ""
	machineName = normalizedMachineName(machineName, runtimeName, endpoint)
	machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, runtimeName, endpoint)
	fingerprintHash := model.HashSecret(machineFingerprint)

	runtime, found, err := s.pgFindRuntimeByFingerprintTx(ctx, tx, key.TenantID, fingerprintHash, true)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, err
	}
	var matchingRuntimes []model.Runtime
	if explicitFingerprint {
		matchingRuntimes, err = s.pgListRuntimesByFingerprintTx(ctx, tx, fingerprintHash, true)
		if err != nil {
			return model.NodeKey{}, model.Runtime{}, err
		}
	}
	if !found && explicitFingerprint {
		runtime, found, err = s.pgFindRuntimeCandidateTx(ctx, tx, key.TenantID, key.ID, model.RuntimeTypeManagedOwned, machineName, runtimeName, endpoint)
		if err != nil {
			return model.NodeKey{}, model.Runtime{}, err
		}
	}
	transferByFingerprint := explicitFingerprint && len(matchingRuntimes) > 0 && (runtime.ID == "" || runtime.FingerprintHash != fingerprintHash || runtime.TenantID != key.TenantID)
	if explicitFingerprint {
		keepRuntimeID := ""
		if runtime.FingerprintHash == fingerprintHash && runtime.TenantID == key.TenantID {
			keepRuntimeID = runtime.ID
		}
		for _, conflict := range matchingRuntimes {
			if conflict.ID == keepRuntimeID {
				continue
			}
			if err := s.pgDetachRuntimeOwnershipTx(ctx, tx, &conflict, now); err != nil {
				return model.NodeKey{}, model.Runtime{}, err
			}
		}
	}

	if found {
		if transferByFingerprint {
			if err := s.pgDeleteRuntimeAccessGrantsTx(ctx, tx, runtime.ID); err != nil {
				return model.NodeKey{}, model.Runtime{}, err
			}
			runtime.AccessMode = model.RuntimeAccessModePrivate
		}
		runtime.Type = model.RuntimeTypeManagedOwned
		runtime.Status = model.RuntimeStatusActive
		runtime.TenantID = key.TenantID
		runtime.Endpoint = endpoint
		runtime.Labels = cloneMap(labels)
		runtime.NodeKeyID = key.ID
		runtime.AgentKeyPrefix = ""
		runtime.AgentKeyHash = ""
		runtime.LastSeenAt = &now
		runtime.LastHeartbeatAt = &now
		runtime.UpdatedAt = now
		applyRuntimeIdentity(&runtime, machineName, machineFingerprint, model.RuntimeTypeManagedOwned, endpoint, labels, key.ID, now)
		if err := s.pgUpdateRuntimeTx(ctx, tx, runtime); err != nil {
			return model.NodeKey{}, model.Runtime{}, err
		}
		if err := tx.Commit(); err != nil {
			return model.NodeKey{}, model.Runtime{}, fmt.Errorf("commit bootstrap cluster node transaction: %w", err)
		}
		return redactNodeKey(key), runtime, nil
	}

	name, err := s.pgNextAvailableRuntimeNameTx(ctx, tx, key.TenantID, runtimeName)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, err
	}
	runtime = model.Runtime{
		ID:              model.NewID("runtime"),
		TenantID:        key.TenantID,
		Name:            name,
		Type:            model.RuntimeTypeManagedOwned,
		AccessMode:      model.RuntimeAccessModePrivate,
		PoolMode:        model.RuntimePoolModeDedicated,
		Status:          model.RuntimeStatusActive,
		Endpoint:        endpoint,
		Labels:          cloneMap(labels),
		NodeKeyID:       key.ID,
		LastSeenAt:      &now,
		LastHeartbeatAt: &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyRuntimeIdentity(&runtime, machineName, machineFingerprint, model.RuntimeTypeManagedOwned, endpoint, labels, key.ID, now)
	if err := s.pgInsertRuntimeTx(ctx, tx, runtime); err != nil {
		return model.NodeKey{}, model.Runtime{}, err
	}

	if err := tx.Commit(); err != nil {
		return model.NodeKey{}, model.Runtime{}, fmt.Errorf("commit bootstrap cluster node transaction: %w", err)
	}
	return redactNodeKey(key), runtime, nil
}

func (s *Store) pgCreateRuntime(tenantID, name, runtimeType, endpoint string, labels map[string]string) (model.Runtime, string, error) {
	secret := model.NewSecret("fugue_rt")
	now := time.Now().UTC()
	runtime := model.Runtime{
		ID:             model.NewID("runtime"),
		TenantID:       tenantID,
		Name:           name,
		Type:           runtimeType,
		AccessMode:     normalizeRuntimeAccessMode(runtimeType, ""),
		PoolMode:       model.NormalizeRuntimePoolMode(runtimeType, ""),
		Status:         model.RuntimeStatusPending,
		Endpoint:       endpoint,
		Labels:         cloneMap(labels),
		AgentKeyPrefix: model.SecretPrefix(secret),
		AgentKeyHash:   model.HashSecret(secret),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if runtimeType == model.RuntimeTypeManagedShared || runtimeType == model.RuntimeTypeManagedOwned {
		runtime.Status = model.RuntimeStatusActive
	}
	backfillRuntimeMetadata(&runtime, model.Machine{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Runtime{}, "", fmt.Errorf("begin create runtime transaction: %w", err)
	}
	defer tx.Rollback()

	if tenantID != "" {
		exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
		if err != nil {
			return model.Runtime{}, "", err
		}
		if !exists {
			return model.Runtime{}, "", ErrNotFound
		}
	}
	if err := s.pgInsertRuntimeTx(ctx, tx, runtime); err != nil {
		return model.Runtime{}, "", err
	}
	if err := tx.Commit(); err != nil {
		return model.Runtime{}, "", fmt.Errorf("commit create runtime transaction: %w", err)
	}
	return runtime, secret, nil
}

func (s *Store) pgConsumeEnrollmentToken(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Runtime, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Runtime{}, "", fmt.Errorf("begin consume enrollment token transaction: %w", err)
	}
	defer tx.Rollback()

	token, err := scanEnrollmentToken(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, expires_at, used_at, created_at, last_used_at
FROM fugue_enrollment_tokens
WHERE hash = $1
FOR UPDATE
`, model.HashSecret(secret)))
	if err != nil {
		return model.Runtime{}, "", mapDBErr(err)
	}
	now := time.Now().UTC()
	if token.UsedAt != nil || token.ExpiresAt.Before(now) {
		return model.Runtime{}, "", ErrConflict
	}
	token.UsedAt = &now
	token.LastUsedAt = &now
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_enrollment_tokens
SET used_at = $2, last_used_at = $3
WHERE id = $1
`, token.ID, token.UsedAt, token.LastUsedAt); err != nil {
		return model.Runtime{}, "", fmt.Errorf("update enrollment token usage: %w", err)
	}

	endpoint = strings.TrimSpace(endpoint)
	explicitFingerprint := strings.TrimSpace(machineFingerprint) != ""
	machineName = normalizedMachineName(machineName, runtimeName, endpoint)
	machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, runtimeName, endpoint)
	fingerprintHash := model.HashSecret(machineFingerprint)

	runtime, found, err := s.pgFindRuntimeByFingerprintTx(ctx, tx, token.TenantID, fingerprintHash, true)
	if err != nil {
		return model.Runtime{}, "", err
	}
	if !found && explicitFingerprint {
		runtime, found, err = s.pgFindRuntimeCandidateTx(ctx, tx, token.TenantID, "", model.RuntimeTypeExternalOwned, machineName, runtimeName, endpoint)
		if err != nil {
			return model.Runtime{}, "", err
		}
	}

	runtimeSecret := model.NewSecret("fugue_rt")

	if found {
		runtime.Type = model.RuntimeTypeExternalOwned
		runtime.Status = model.RuntimeStatusActive
		runtime.Endpoint = endpoint
		runtime.Labels = cloneMap(labels)
		runtime.AgentKeyPrefix = model.SecretPrefix(runtimeSecret)
		runtime.AgentKeyHash = model.HashSecret(runtimeSecret)
		runtime.LastSeenAt = &now
		runtime.LastHeartbeatAt = &now
		runtime.UpdatedAt = now
		applyRuntimeIdentity(&runtime, machineName, machineFingerprint, model.RuntimeTypeExternalOwned, endpoint, labels, runtime.NodeKeyID, now)
		if err := s.pgUpdateRuntimeTx(ctx, tx, runtime); err != nil {
			return model.Runtime{}, "", err
		}
		if err := tx.Commit(); err != nil {
			return model.Runtime{}, "", fmt.Errorf("commit consume enrollment token transaction: %w", err)
		}
		return runtime, runtimeSecret, nil
	}

	name, err := s.pgNextAvailableRuntimeNameTx(ctx, tx, token.TenantID, runtimeName)
	if err != nil {
		return model.Runtime{}, "", err
	}
	runtime = model.Runtime{
		ID:              model.NewID("runtime"),
		TenantID:        token.TenantID,
		Name:            name,
		Type:            model.RuntimeTypeExternalOwned,
		AccessMode:      model.RuntimeAccessModePrivate,
		PoolMode:        model.RuntimePoolModeDedicated,
		Status:          model.RuntimeStatusActive,
		Endpoint:        endpoint,
		Labels:          cloneMap(labels),
		AgentKeyPrefix:  model.SecretPrefix(runtimeSecret),
		AgentKeyHash:    model.HashSecret(runtimeSecret),
		LastSeenAt:      &now,
		LastHeartbeatAt: &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyRuntimeIdentity(&runtime, machineName, machineFingerprint, model.RuntimeTypeExternalOwned, endpoint, labels, "", now)
	if err := s.pgInsertRuntimeTx(ctx, tx, runtime); err != nil {
		return model.Runtime{}, "", err
	}

	if err := tx.Commit(); err != nil {
		return model.Runtime{}, "", fmt.Errorf("commit consume enrollment token transaction: %w", err)
	}
	return runtime, runtimeSecret, nil
}

func (s *Store) pgAuthenticateRuntimeKey(secret string) (model.Runtime, model.Principal, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Runtime{}, model.Principal{}, fmt.Errorf("begin authenticate runtime transaction: %w", err)
	}
	defer tx.Rollback()

	runtime, err := scanRuntime(tx.QueryRowContext(ctx, `
UPDATE fugue_runtimes
SET last_seen_at = NOW(),
	last_heartbeat_at = NOW(),
	status = $2,
	updated_at = NOW()
WHERE agent_key_hash = $1
RETURNING id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
`, model.HashSecret(secret), model.RuntimeStatusActive))
	if err != nil {
		return model.Runtime{}, model.Principal{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.Runtime{}, model.Principal{}, fmt.Errorf("commit authenticate runtime transaction: %w", err)
	}
	return runtime, model.Principal{
		ActorType: model.ActorTypeRuntime,
		ActorID:   runtime.ID,
		TenantID:  runtime.TenantID,
		Scopes: map[string]struct{}{
			"runtime.agent": {},
		},
	}, nil
}

func (s *Store) pgUpdateRuntimeHeartbeat(runtimeID, endpoint string) (model.Runtime, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Runtime{}, fmt.Errorf("begin update heartbeat transaction: %w", err)
	}
	defer tx.Rollback()

	runtime, err := scanRuntime(tx.QueryRowContext(ctx, `
UPDATE fugue_runtimes
SET last_heartbeat_at = NOW(),
	last_seen_at = NOW(),
	status = $2,
	updated_at = NOW(),
	endpoint = CASE WHEN $3 <> '' THEN $3 ELSE endpoint END
WHERE id = $1
RETURNING id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
`, runtimeID, model.RuntimeStatusActive, endpoint))
	if err != nil {
		return model.Runtime{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.Runtime{}, fmt.Errorf("commit update heartbeat transaction: %w", err)
	}
	return runtime, nil
}

func (s *Store) pgMarkRuntimeOfflineStale(after time.Duration) (int, error) {
	if after <= 0 {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin mark stale runtimes transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
SELECT id
FROM fugue_runtimes
WHERE type <> $1
  AND type <> $4
  AND status <> $2
  AND (last_heartbeat_at IS NULL OR last_heartbeat_at < $3)
FOR UPDATE
`, model.RuntimeTypeManagedShared, model.RuntimeStatusOffline, time.Now().UTC().Add(-after), model.RuntimeTypeManagedOwned)
	if err != nil {
		return 0, fmt.Errorf("query stale runtimes: %w", err)
	}
	defer rows.Close()

	runtimeIDs := make([]string, 0)
	for rows.Next() {
		var runtimeID string
		if err := rows.Scan(&runtimeID); err != nil {
			return 0, fmt.Errorf("scan stale runtime id: %w", err)
		}
		runtimeIDs = append(runtimeIDs, runtimeID)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate stale runtimes: %w", err)
	}
	for _, runtimeID := range runtimeIDs {
		if _, err := tx.ExecContext(ctx, `
UPDATE fugue_runtimes
SET status = $2, updated_at = NOW()
WHERE id = $1
`, runtimeID, model.RuntimeStatusOffline); err != nil {
			return 0, fmt.Errorf("mark runtime %s offline stale: %w", runtimeID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit mark stale runtimes transaction: %w", err)
	}
	return len(runtimeIDs), nil
}

func (s *Store) pgListNodes(tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	return s.pgListRuntimesByFilter(tenantID, platformAdmin, true)
}

func (s *Store) pgListRuntimes(tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	return s.pgListRuntimesByFilter(tenantID, platformAdmin, false)
}

func (s *Store) pgListRuntimesByNodeKey(nodeKeyID, tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE node_key_id = $1
`
	args := []any{nodeKeyID}
	if !platformAdmin {
		query += ` AND tenant_id = $2`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runtimes by node key: %w", err)
	}
	defer rows.Close()

	runtimes := make([]model.Runtime, 0)
	for rows.Next() {
		runtime, err := scanRuntime(rows)
		if err != nil {
			return nil, err
		}
		runtimes = append(runtimes, runtime)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtimes by node key: %w", err)
	}
	return runtimes, nil
}

func (s *Store) pgListRuntimesByFilter(tenantID string, platformAdmin bool, nodesOnly bool) ([]model.Runtime, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes AS r
`
	args := make([]any, 0, 3)
	clauses := make([]string, 0, 2)
	if nodesOnly {
		clauses = append(clauses, fmt.Sprintf("type IN ($%d, $%d)", len(args)+1, len(args)+2))
		args = append(args, model.RuntimeTypeExternalOwned, model.RuntimeTypeManagedOwned)
	}
	if !platformAdmin {
		tenantArg := len(args) + 1
		sharedTypeArg := tenantArg + 1
		platformSharedArg := sharedTypeArg + 1
		clauses = append(clauses, fmt.Sprintf("(r.tenant_id = $%d OR r.type = $%d OR r.access_mode = $%d OR EXISTS (SELECT 1 FROM fugue_runtime_access_grants AS g WHERE g.runtime_id = r.id AND g.tenant_id = $%d))", tenantArg, sharedTypeArg, platformSharedArg, tenantArg))
		args = append(args, tenantID, model.RuntimeTypeManagedShared, model.RuntimeAccessModePlatformShared)
	}
	if len(clauses) > 0 {
		query += ` WHERE ` + strings.Join(clauses, " AND ")
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runtimes: %w", err)
	}
	defer rows.Close()

	runtimes := make([]model.Runtime, 0)
	for rows.Next() {
		runtime, err := scanRuntime(rows)
		if err != nil {
			return nil, err
		}
		runtimes = append(runtimes, runtime)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtimes: %w", err)
	}
	return runtimes, nil
}

func (s *Store) pgGetRuntime(id string) (model.Runtime, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runtime, err := scanRuntime(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE id = $1
`, id))
	if err != nil {
		return model.Runtime{}, mapDBErr(err)
	}
	return runtime, nil
}

func (s *Store) pgDetachRuntimeOwnership(runtimeID string) (model.Runtime, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Runtime{}, fmt.Errorf("begin detach runtime ownership transaction: %w", err)
	}
	defer tx.Rollback()

	runtime, err := s.pgGetRuntimeTx(ctx, tx, runtimeID, true)
	if err != nil {
		return model.Runtime{}, err
	}
	if err := s.pgDetachRuntimeOwnershipTx(ctx, tx, &runtime, time.Now().UTC()); err != nil {
		return model.Runtime{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Runtime{}, fmt.Errorf("commit detach runtime ownership transaction: %w", err)
	}
	return runtime, nil
}

func (s *Store) pgFindManagedOwnedRuntimeTx(ctx context.Context, tx *sql.Tx, nodeKeyID, runtimeName string) (model.Runtime, bool, error) {
	runtime, err := scanRuntime(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE type = $1
  AND node_key_id = $2
  AND lower(name) = lower($3)
FOR UPDATE
`, model.RuntimeTypeManagedOwned, nodeKeyID, strings.TrimSpace(runtimeName)))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Runtime{}, false, nil
		}
		return model.Runtime{}, false, fmt.Errorf("find managed-owned runtime by node key %s and name %s: %w", nodeKeyID, runtimeName, err)
	}
	return runtime, true, nil
}

func (s *Store) pgGetRuntimeTx(ctx context.Context, tx *sql.Tx, id string, forUpdate bool) (model.Runtime, error) {
	query := `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE id = $1
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	runtime, err := scanRuntime(tx.QueryRowContext(ctx, query, id))
	if err != nil {
		return model.Runtime{}, mapDBErr(err)
	}
	return runtime, nil
}

func (s *Store) pgFindRuntimeByFingerprintTx(ctx context.Context, tx *sql.Tx, tenantID, fingerprintHash string, forUpdate bool) (model.Runtime, bool, error) {
	if strings.TrimSpace(fingerprintHash) == "" {
		return model.Runtime{}, false, nil
	}
	query := `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE tenant_id = $1
  AND fingerprint_hash = $2
ORDER BY updated_at DESC, created_at DESC
LIMIT 1
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	runtime, err := scanRuntime(tx.QueryRowContext(ctx, query, tenantID, fingerprintHash))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Runtime{}, false, nil
		}
		return model.Runtime{}, false, err
	}
	return runtime, true, nil
}

func (s *Store) pgListRuntimesByFingerprintTx(ctx context.Context, tx *sql.Tx, fingerprintHash string, forUpdate bool) ([]model.Runtime, error) {
	if strings.TrimSpace(fingerprintHash) == "" {
		return nil, nil
	}
	query := `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE fingerprint_hash = $1
ORDER BY updated_at DESC, created_at DESC
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, fingerprintHash)
	if err != nil {
		return nil, fmt.Errorf("query runtimes by fingerprint: %w", err)
	}
	defer rows.Close()

	runtimes := make([]model.Runtime, 0)
	for rows.Next() {
		runtime, err := scanRuntime(rows)
		if err != nil {
			return nil, err
		}
		runtimes = append(runtimes, runtime)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtimes by fingerprint: %w", err)
	}
	return runtimes, nil
}

func (s *Store) pgDeleteRuntimeAccessGrantsTx(ctx context.Context, tx *sql.Tx, runtimeID string) error {
	if strings.TrimSpace(runtimeID) == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM fugue_runtime_access_grants
WHERE runtime_id = $1
`, runtimeID); err != nil {
		return fmt.Errorf("delete runtime access grants for %s: %w", runtimeID, err)
	}
	return nil
}

func (s *Store) pgDetachRuntimeOwnershipTx(ctx context.Context, tx *sql.Tx, runtime *model.Runtime, now time.Time) error {
	if runtime == nil {
		return nil
	}
	if err := s.pgDeleteRuntimeAccessGrantsTx(ctx, tx, runtime.ID); err != nil {
		return err
	}
	runtime.AccessMode = model.RuntimeAccessModePrivate
	runtime.PoolMode = model.RuntimePoolModeDedicated
	runtime.Status = model.RuntimeStatusOffline
	runtime.NodeKeyID = ""
	runtime.ClusterNodeName = ""
	runtime.FingerprintPrefix = ""
	runtime.FingerprintHash = ""
	runtime.AgentKeyPrefix = ""
	runtime.AgentKeyHash = ""
	runtime.UpdatedAt = now
	return s.pgUpdateRuntimeTx(ctx, tx, *runtime)
}

func (s *Store) pgFindRuntimeCandidateTx(ctx context.Context, tx *sql.Tx, tenantID, nodeKeyID, runtimeType, machineName, runtimeName, endpoint string) (model.Runtime, bool, error) {
	query := `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE tenant_id = $1
  AND type = $2
`
	args := []any{tenantID, runtimeType}
	if strings.TrimSpace(nodeKeyID) != "" {
		query += ` AND node_key_id = $3`
		args = append(args, nodeKeyID)
	}
	query += ` ORDER BY updated_at DESC, created_at DESC FOR UPDATE`

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return model.Runtime{}, false, fmt.Errorf("query runtime candidates: %w", err)
	}
	defer rows.Close()

	bestScore := 0
	var best model.Runtime
	for rows.Next() {
		runtime, err := scanRuntime(rows)
		if err != nil {
			return model.Runtime{}, false, err
		}
		score := 0
		if endpoint != "" && strings.EqualFold(strings.TrimSpace(runtime.Endpoint), strings.TrimSpace(endpoint)) {
			score++
		}
		if runtimeName != "" && strings.EqualFold(runtime.Name, strings.TrimSpace(runtimeName)) {
			score += 4
		}
		if runtimeName != "" && strings.EqualFold(runtime.ClusterNodeName, strings.TrimSpace(runtimeName)) {
			score += 4
		}
		if machineName != "" && strings.EqualFold(runtime.MachineName, strings.TrimSpace(machineName)) {
			score += 2
		}
		if score > bestScore {
			bestScore = score
			best = runtime
		}
	}
	if err := rows.Err(); err != nil {
		return model.Runtime{}, false, fmt.Errorf("iterate runtime candidates: %w", err)
	}
	if bestScore == 0 {
		return model.Runtime{}, false, nil
	}
	return best, true, nil
}

func (s *Store) pgInsertRuntimeTx(ctx context.Context, tx *sql.Tx, runtime model.Runtime) error {
	labelsJSON, err := marshalNullableJSON(runtime.Labels)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
`, runtime.ID, nullIfEmpty(runtime.TenantID), runtime.Name, runtime.MachineName, runtime.Type, normalizeRuntimeAccessMode(runtime.Type, runtime.AccessMode), model.NormalizeRuntimePoolMode(runtime.Type, runtime.PoolMode), runtime.ConnectionMode, runtime.Status, runtime.Endpoint, labelsJSON, nullIfEmpty(runtime.NodeKeyID), runtime.ClusterNodeName, runtime.FingerprintPrefix, runtime.FingerprintHash, runtime.AgentKeyPrefix, runtime.AgentKeyHash, runtime.LastSeenAt, runtime.LastHeartbeatAt, runtime.CreatedAt, runtime.UpdatedAt); err != nil {
		return mapDBErr(err)
	}
	return nil
}

func (s *Store) pgUpdateRuntimeTx(ctx context.Context, tx *sql.Tx, runtime model.Runtime) error {
	labelsJSON, err := marshalNullableJSON(runtime.Labels)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_runtimes
SET name = $2,
	machine_name = $3,
	type = $4,
	access_mode = $5,
	pool_mode = $6,
	connection_mode = $7,
	status = $8,
	endpoint = $9,
	labels_json = $10,
	node_key_id = $11,
	cluster_node_name = $12,
	fingerprint_prefix = $13,
	fingerprint_hash = $14,
	agent_key_prefix = $15,
	agent_key_hash = $16,
	last_seen_at = $17,
	last_heartbeat_at = $18,
	updated_at = $19
WHERE id = $1
`, runtime.ID, runtime.Name, runtime.MachineName, runtime.Type, normalizeRuntimeAccessMode(runtime.Type, runtime.AccessMode), model.NormalizeRuntimePoolMode(runtime.Type, runtime.PoolMode), runtime.ConnectionMode, runtime.Status, runtime.Endpoint, labelsJSON, nullIfEmpty(runtime.NodeKeyID), runtime.ClusterNodeName, runtime.FingerprintPrefix, runtime.FingerprintHash, runtime.AgentKeyPrefix, runtime.AgentKeyHash, runtime.LastSeenAt, runtime.LastHeartbeatAt, runtime.UpdatedAt); err != nil {
		return fmt.Errorf("update runtime %s: %w", runtime.ID, err)
	}
	return nil
}

func (s *Store) pgEnsureManagedSharedLocationLabels(labels map[string]string) (model.Runtime, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Runtime{}, false, fmt.Errorf("begin ensure managed shared location transaction: %w", err)
	}
	defer tx.Rollback()

	runtimeObj, err := scanRuntime(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE id = $1
FOR UPDATE
`, "runtime_managed_shared"))
	if err != nil {
		return model.Runtime{}, false, mapDBErr(err)
	}
	if len(labels) == 0 || len(runtimepkg.PlacementNodeSelector(runtimeObj)) > 0 {
		return runtimeObj, false, nil
	}

	runtimeLabels := cloneMap(runtimeObj.Labels)
	if runtimeLabels == nil {
		runtimeLabels = map[string]string{}
	}
	for key, value := range labels {
		runtimeLabels[key] = value
	}
	runtimeObj.Labels = runtimeLabels
	runtimeObj.UpdatedAt = time.Now().UTC()
	if err := s.pgUpdateRuntimeTx(ctx, tx, runtimeObj); err != nil {
		return model.Runtime{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return model.Runtime{}, false, fmt.Errorf("commit ensure managed shared location transaction: %w", err)
	}
	return runtimeObj, true, nil
}

func (s *Store) pgListApps(tenantID string, platformAdmin bool) ([]model.App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
FROM fugue_apps
`
	args := make([]any, 0, 1)
	if !platformAdmin {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list apps: %w", err)
	}
	defer rows.Close()

	apps := make([]model.App, 0)
	for rows.Next() {
		app, err := scanApp(rows)
		if err != nil {
			return nil, err
		}
		normalizeAppStatusForRead(&app)
		if err := s.pgHydrateAppBackingServices(ctx, &app); err != nil {
			return nil, err
		}
		if isDeletedApp(app) {
			continue
		}
		apps = append(apps, app)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate apps: %w", err)
	}
	return apps, nil
}

func (s *Store) pgGetApp(id string) (model.App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app, err := scanApp(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
FROM fugue_apps
WHERE id = $1
`, id))
	if err != nil {
		return model.App{}, mapDBErr(err)
	}
	normalizeAppStatusForRead(&app)
	if err := s.pgHydrateAppBackingServices(ctx, &app); err != nil {
		return model.App{}, err
	}
	if isDeletedApp(app) {
		return model.App{}, ErrNotFound
	}
	return app, nil
}

func (s *Store) pgGetAppByHostname(hostname string) (model.App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app, err := scanApp(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
FROM fugue_apps
WHERE lower(route_json->>'hostname') = lower($1)
`, hostname))
	if err != nil {
		if !errors.Is(mapDBErr(err), ErrNotFound) {
			return model.App{}, mapDBErr(err)
		}
		app, err = s.pgGetVerifiedAppByCustomDomainHostname(ctx, hostname)
		if err != nil {
			return model.App{}, err
		}
	}
	normalizeAppStatusForRead(&app)
	if err := s.pgHydrateAppBackingServices(ctx, &app); err != nil {
		return model.App{}, err
	}
	return app, nil
}

func (s *Store) pgCreateApp(tenantID, projectID, name, description string, spec model.AppSpec, source *model.AppSource, route *model.AppRoute) (model.App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.App{}, fmt.Errorf("begin create app transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
	if err != nil {
		return model.App{}, err
	}
	if !exists {
		return model.App{}, ErrNotFound
	}
	projectOK, err := s.pgProjectBelongsToTenantTx(ctx, tx, projectID, tenantID)
	if err != nil {
		return model.App{}, err
	}
	if !projectOK {
		return model.App{}, ErrNotFound
	}
	if err := normalizeAppSpecResources(&spec); err != nil {
		return model.App{}, err
	}
	if err := validateManagedPostgresSpecForAppName(name, spec.Postgres); err != nil {
		return model.App{}, err
	}
	visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, spec.RuntimeID, tenantID)
	if err != nil {
		return model.App{}, err
	}
	if !visible {
		return model.App{}, ErrNotFound
	}
	runtimeType, err := s.pgRuntimeTypeTx(ctx, tx, spec.RuntimeID)
	if err != nil {
		return model.App{}, err
	}
	if err := validateWorkspaceSpecForRuntime(spec, runtimeType); err != nil {
		return model.App{}, err
	}
	if err := validateFailoverSpec(spec); err != nil {
		return model.App{}, err
	}
	if err := validateManagedPostgresRuntimeSpec(spec.RuntimeID, derefPostgresSpec(spec.Postgres)); err != nil {
		return model.App{}, err
	}
	if spec.Failover != nil {
		visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, spec.Failover.TargetRuntimeID, tenantID)
		if err != nil {
			return model.App{}, err
		}
		if !visible {
			return model.App{}, ErrNotFound
		}
		targetRuntimeType, err := s.pgRuntimeTypeTx(ctx, tx, spec.Failover.TargetRuntimeID)
		if err != nil {
			return model.App{}, err
		}
		if err := validateFailoverTargetRuntimeType(targetRuntimeType); err != nil {
			return model.App{}, err
		}
	}
	for _, runtimeID := range managedPostgresReferencedRuntimeIDs(spec.RuntimeID, derefPostgresSpec(spec.Postgres)) {
		visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, runtimeID, tenantID)
		if err != nil {
			return model.App{}, err
		}
		if !visible {
			return model.App{}, ErrNotFound
		}
		runtimeType, err := s.pgRuntimeTypeTx(ctx, tx, runtimeID)
		if err != nil {
			return model.App{}, err
		}
		if err := validateFailoverTargetRuntimeType(runtimeType); err != nil {
			return model.App{}, err
		}
	}

	now := time.Now().UTC()
	allowPendingImport := source != nil && isQueuedImportSourceType(source.Type) && strings.TrimSpace(spec.Image) == ""
	phase := "created"
	if allowPendingImport {
		phase = "importing"
	}
	app := model.App{
		ID:          model.NewID("app"),
		TenantID:    tenantID,
		ProjectID:   projectID,
		Name:        name,
		Description: strings.TrimSpace(description),
		Source:      cloneAppSource(source),
		Route:       cloneAppRoute(route),
		Spec:        spec,
		Status: model.AppStatus{
			Phase:           phase,
			CurrentReplicas: 0,
			UpdatedAt:       now,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}
	var ownedService *model.BackingService
	var binding *model.ServiceBinding
	if app.Spec.Postgres != nil {
		service, appBinding := ownedManagedPostgresResources(app)
		service.Name = s.pgNextAvailableBackingServiceNameTx(ctx, tx, tenantID, projectID, service.Name)
		ownedService = &service
		binding = &appBinding
		app.Spec.Postgres = nil
	}
	sourceJSON, err := marshalNullableJSON(app.Source)
	if err != nil {
		return model.App{}, err
	}
	routeJSON, err := marshalNullableJSON(app.Route)
	if err != nil {
		return model.App{}, err
	}
	specJSON, err := marshalJSON(app.Spec)
	if err != nil {
		return model.App{}, err
	}
	statusJSON, err := marshalJSON(app.Status)
	if err != nil {
		return model.App{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_apps (id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`, app.ID, app.TenantID, app.ProjectID, app.Name, app.Description, sourceJSON, routeJSON, specJSON, statusJSON, app.CreatedAt, app.UpdatedAt); err != nil {
		return model.App{}, mapDBErr(err)
	}
	if ownedService != nil {
		if err := s.pgInsertBackingServiceTx(ctx, tx, *ownedService); err != nil {
			return model.App{}, err
		}
		if err := s.pgInsertServiceBindingTx(ctx, tx, *binding); err != nil {
			return model.App{}, err
		}
		app.BackingServices = []model.BackingService{cloneBackingService(*ownedService)}
		app.Bindings = []model.ServiceBinding{cloneServiceBinding(*binding)}
	}
	if err := tx.Commit(); err != nil {
		return model.App{}, fmt.Errorf("commit create app transaction: %w", err)
	}
	return app, nil
}

func (s *Store) pgUpdateAppRoute(id string, route model.AppRoute) (model.App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.App{}, fmt.Errorf("begin update app route transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, id, true)
	if err != nil {
		return model.App{}, mapDBErr(err)
	}
	if isDeletedApp(app) {
		return model.App{}, ErrNotFound
	}

	route.Hostname = strings.TrimSpace(strings.ToLower(route.Hostname))
	route.BaseDomain = strings.TrimSpace(strings.ToLower(route.BaseDomain))
	route.PublicURL = strings.TrimSpace(route.PublicURL)
	if route.Hostname == "" {
		return model.App{}, ErrInvalidInput
	}
	if route.BaseDomain == "" && app.Route != nil {
		route.BaseDomain = strings.TrimSpace(strings.ToLower(app.Route.BaseDomain))
	}
	if route.PublicURL == "" {
		route.PublicURL = "https://" + route.Hostname
	}
	if route.ServicePort <= 0 {
		if app.Route != nil && app.Route.ServicePort > 0 {
			route.ServicePort = app.Route.ServicePort
		}
		if route.ServicePort <= 0 {
			route.ServicePort = firstPositiveSpecPort(app.Spec.Ports)
		}
		if route.ServicePort <= 0 {
			route.ServicePort = 80
		}
	}

	app.Route = cloneAppRoute(&route)
	app.UpdatedAt = time.Now().UTC()
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.App{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.App{}, fmt.Errorf("commit update app route transaction: %w", err)
	}
	normalizeAppStatusForRead(&app)
	if err := s.pgHydrateAppBackingServices(context.Background(), &app); err != nil {
		return model.App{}, err
	}
	return app, nil
}

func (s *Store) pgUpdateAppImageMirrorLimit(id string, limit int) (model.App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.App{}, fmt.Errorf("begin update app image mirror limit transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, id, true)
	if err != nil {
		return model.App{}, mapDBErr(err)
	}
	if isDeletedApp(app) {
		return model.App{}, ErrNotFound
	}

	app.Spec.ImageMirrorLimit = limit
	app.UpdatedAt = time.Now().UTC()
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.App{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.App{}, fmt.Errorf("commit update app image mirror limit transaction: %w", err)
	}
	normalizeAppStatusForRead(&app)
	if err := s.pgHydrateAppBackingServices(context.Background(), &app); err != nil {
		return model.App{}, err
	}
	return app, nil
}

func (s *Store) pgPurgeApp(id string) (model.App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.App{}, fmt.Errorf("begin purge app transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, id, true)
	if err != nil {
		return model.App{}, mapDBErr(err)
	}
	normalizeAppStatusForRead(&app)
	if app.Status.CurrentReplicas > 0 || strings.TrimSpace(app.Status.CurrentRuntimeID) != "" {
		return model.App{}, ErrConflict
	}
	if !isDeletedApp(app) {
		phase := strings.TrimSpace(strings.ToLower(app.Status.Phase))
		if strings.TrimSpace(app.Spec.Image) != "" || (phase != "importing" && phase != "failed") {
			return model.App{}, ErrConflict
		}
	}

	if err := s.pgDeleteServiceBindingsByAppTx(ctx, tx, app.ID); err != nil {
		return model.App{}, err
	}
	if err := s.pgDeleteOwnedBackingServicesByAppTx(ctx, tx, app.ID); err != nil {
		return model.App{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_apps WHERE id = $1`, app.ID); err != nil {
		return model.App{}, fmt.Errorf("delete app %s: %w", app.ID, err)
	}
	if err := s.pgDeleteAppDomainsByAppTx(ctx, tx, app.ID); err != nil {
		return model.App{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.App{}, fmt.Errorf("commit purge app transaction: %w", err)
	}
	return app, nil
}

func (s *Store) pgReserveIdempotencyRecord(scope, tenantID, key, requestHash string) (model.IdempotencyRecord, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.IdempotencyRecord{}, false, fmt.Errorf("begin reserve idempotency transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	record := model.IdempotencyRecord{
		Scope:       scope,
		TenantID:    tenantID,
		Key:         key,
		RequestHash: requestHash,
		Status:      model.IdempotencyStatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	result, err := tx.ExecContext(ctx, `
INSERT INTO fugue_idempotency_keys (scope, tenant_id, key, request_hash, status, app_id, operation_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '', '', $6, $7)
ON CONFLICT DO NOTHING
`, record.Scope, record.TenantID, record.Key, record.RequestHash, record.Status, record.CreatedAt, record.UpdatedAt)
	if err != nil {
		return model.IdempotencyRecord{}, false, mapDBErr(err)
	}

	if rowsAffected, _ := result.RowsAffected(); rowsAffected == 1 {
		if err := tx.Commit(); err != nil {
			return model.IdempotencyRecord{}, false, fmt.Errorf("commit reserve idempotency transaction: %w", err)
		}
		return record, true, nil
	}

	record, err = s.pgGetIdempotencyRecordTx(ctx, tx, scope, tenantID, key, true)
	if err != nil {
		return model.IdempotencyRecord{}, false, mapDBErr(err)
	}
	if record.RequestHash != requestHash {
		return model.IdempotencyRecord{}, false, ErrIdempotencyMismatch
	}
	if err := tx.Commit(); err != nil {
		return model.IdempotencyRecord{}, false, fmt.Errorf("commit read idempotency transaction: %w", err)
	}
	return record, false, nil
}

func (s *Store) pgCompleteIdempotencyRecord(scope, tenantID, key, appID, operationID string) (model.IdempotencyRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	record, err := scanIdempotencyRecord(s.db.QueryRowContext(ctx, `
UPDATE fugue_idempotency_keys
SET status = $4,
	app_id = $5,
	operation_id = $6,
	updated_at = $7
WHERE scope = $1
  AND tenant_id = $2
  AND key = $3
RETURNING scope, tenant_id, key, request_hash, status, app_id, operation_id, created_at, updated_at
`, scope, tenantID, key, model.IdempotencyStatusCompleted, appID, operationID, now))
	if err != nil {
		return model.IdempotencyRecord{}, mapDBErr(err)
	}
	return record, nil
}

func (s *Store) pgRepairAppStatuses() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin repair app statuses transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
FROM fugue_apps
FOR UPDATE
`)
	if err != nil {
		return fmt.Errorf("list apps for status repair: %w", err)
	}
	apps := make([]model.App, 0)
	for rows.Next() {
		app, err := scanApp(rows)
		if err != nil {
			rows.Close()
			return err
		}
		apps = append(apps, app)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate apps for status repair: %w", err)
	}
	rows.Close()

	for _, app := range apps {
		if !repairFailedAppPhase(&app) {
			continue
		}
		if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit repair app statuses transaction: %w", err)
	}
	return nil
}

func (s *Store) pgReleaseIdempotencyRecord(scope, tenantID, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.db.ExecContext(ctx, `
DELETE FROM fugue_idempotency_keys
WHERE scope = $1
  AND tenant_id = $2
  AND key = $3
  AND status = $4
`, scope, tenantID, key, model.IdempotencyStatusPending)
	if err != nil {
		return fmt.Errorf("release idempotency record: %w", err)
	}
	return nil
}

func (s *Store) pgCreateOperation(op model.Operation) (model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, fmt.Errorf("begin create operation transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, op.AppID, false)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	if app.TenantID != op.TenantID {
		return model.Operation{}, ErrNotFound
	}

	switch op.Type {
	case model.OperationTypeImport:
		if op.DesiredSpec == nil || op.DesiredSource == nil {
			return model.Operation{}, ErrInvalidInput
		}
		if !isQueuedImportSourceType(op.DesiredSource.Type) {
			return model.Operation{}, ErrInvalidInput
		}
		if err := normalizeAppSpecResources(op.DesiredSpec); err != nil {
			return model.Operation{}, err
		}
		if err := validateManagedPostgresSpecForAppName(app.Name, op.DesiredSpec.Postgres); err != nil {
			return model.Operation{}, err
		}
		visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, op.DesiredSpec.RuntimeID, op.TenantID)
		if err != nil {
			return model.Operation{}, err
		}
		if !visible {
			return model.Operation{}, ErrNotFound
		}
		runtimeType, err := s.pgRuntimeTypeTx(ctx, tx, op.DesiredSpec.RuntimeID)
		if err != nil {
			return model.Operation{}, err
		}
		if err := validateWorkspaceSpecForRuntime(*op.DesiredSpec, runtimeType); err != nil {
			return model.Operation{}, err
		}
		if err := validateFailoverSpec(*op.DesiredSpec); err != nil {
			return model.Operation{}, err
		}
		if err := validateManagedPostgresRuntimeSpec(op.DesiredSpec.RuntimeID, derefPostgresSpec(op.DesiredSpec.Postgres)); err != nil {
			return model.Operation{}, err
		}
		if op.DesiredSpec.Failover != nil {
			visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, op.DesiredSpec.Failover.TargetRuntimeID, op.TenantID)
			if err != nil {
				return model.Operation{}, err
			}
			if !visible {
				return model.Operation{}, ErrNotFound
			}
			targetRuntimeType, err := s.pgRuntimeTypeTx(ctx, tx, op.DesiredSpec.Failover.TargetRuntimeID)
			if err != nil {
				return model.Operation{}, err
			}
			if err := validateFailoverTargetRuntimeType(targetRuntimeType); err != nil {
				return model.Operation{}, err
			}
		}
		for _, runtimeID := range managedPostgresReferencedRuntimeIDs(op.DesiredSpec.RuntimeID, derefPostgresSpec(op.DesiredSpec.Postgres)) {
			visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, runtimeID, op.TenantID)
			if err != nil {
				return model.Operation{}, err
			}
			if !visible {
				return model.Operation{}, ErrNotFound
			}
			runtimeType, err := s.pgRuntimeTypeTx(ctx, tx, runtimeID)
			if err != nil {
				return model.Operation{}, err
			}
			if err := validateFailoverTargetRuntimeType(runtimeType); err != nil {
				return model.Operation{}, err
			}
		}
		op.TargetRuntimeID = op.DesiredSpec.RuntimeID
	case model.OperationTypeDeploy:
		if op.DesiredSpec == nil {
			return model.Operation{}, ErrInvalidInput
		}
		if err := normalizeAppSpecResources(op.DesiredSpec); err != nil {
			return model.Operation{}, err
		}
		if err := validateManagedPostgresSpecForAppName(app.Name, op.DesiredSpec.Postgres); err != nil {
			return model.Operation{}, err
		}
		visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, op.DesiredSpec.RuntimeID, op.TenantID)
		if err != nil {
			return model.Operation{}, err
		}
		if !visible {
			return model.Operation{}, ErrNotFound
		}
		runtimeType, err := s.pgRuntimeTypeTx(ctx, tx, op.DesiredSpec.RuntimeID)
		if err != nil {
			return model.Operation{}, err
		}
		if err := validateWorkspaceSpecForRuntime(*op.DesiredSpec, runtimeType); err != nil {
			return model.Operation{}, err
		}
		if err := validateFailoverSpec(*op.DesiredSpec); err != nil {
			return model.Operation{}, err
		}
		if err := validateManagedPostgresRuntimeSpec(op.DesiredSpec.RuntimeID, derefPostgresSpec(op.DesiredSpec.Postgres)); err != nil {
			return model.Operation{}, err
		}
		if op.DesiredSpec.Failover != nil {
			visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, op.DesiredSpec.Failover.TargetRuntimeID, op.TenantID)
			if err != nil {
				return model.Operation{}, err
			}
			if !visible {
				return model.Operation{}, ErrNotFound
			}
			targetRuntimeType, err := s.pgRuntimeTypeTx(ctx, tx, op.DesiredSpec.Failover.TargetRuntimeID)
			if err != nil {
				return model.Operation{}, err
			}
			if err := validateFailoverTargetRuntimeType(targetRuntimeType); err != nil {
				return model.Operation{}, err
			}
		}
		for _, runtimeID := range managedPostgresReferencedRuntimeIDs(op.DesiredSpec.RuntimeID, derefPostgresSpec(op.DesiredSpec.Postgres)) {
			visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, runtimeID, op.TenantID)
			if err != nil {
				return model.Operation{}, err
			}
			if !visible {
				return model.Operation{}, ErrNotFound
			}
			runtimeType, err := s.pgRuntimeTypeTx(ctx, tx, runtimeID)
			if err != nil {
				return model.Operation{}, err
			}
			if err := validateFailoverTargetRuntimeType(runtimeType); err != nil {
				return model.Operation{}, err
			}
		}
		op.TargetRuntimeID = op.DesiredSpec.RuntimeID
	case model.OperationTypeScale:
		if op.DesiredReplicas == nil || *op.DesiredReplicas < 0 {
			return model.Operation{}, ErrInvalidInput
		}
		op.TargetRuntimeID = app.Spec.RuntimeID
	case model.OperationTypeDelete:
		if strings.TrimSpace(app.Spec.RuntimeID) == "" {
			return model.Operation{}, ErrInvalidInput
		}
		op.SourceRuntimeID = app.Spec.RuntimeID
		op.TargetRuntimeID = app.Spec.RuntimeID
	case model.OperationTypeMigrate:
		if op.TargetRuntimeID == "" {
			return model.Operation{}, ErrInvalidInput
		}
		visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, op.TargetRuntimeID, op.TenantID)
		if err != nil {
			return model.Operation{}, err
		}
		if !visible {
			return model.Operation{}, ErrNotFound
		}
		if hasPersistentWorkspace(app) {
			return model.Operation{}, ErrInvalidInput
		}
		if appHasManagedPostgresService(app) {
			targetRuntimeType, err := s.pgRuntimeTypeTx(ctx, tx, op.TargetRuntimeID)
			if err != nil {
				return model.Operation{}, err
			}
			if err := validateFailoverTargetRuntimeType(targetRuntimeType); err != nil {
				return model.Operation{}, err
			}
		}
		op.SourceRuntimeID = app.Spec.RuntimeID
	case model.OperationTypeFailover:
		var inFlightCount int
		if err := tx.QueryRowContext(ctx, `
	SELECT COUNT(1)
FROM fugue_operations
WHERE app_id = $1
  AND status IN ($2, $3, $4)
`, app.ID, model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent).Scan(&inFlightCount); err != nil {
			return model.Operation{}, fmt.Errorf("count in-flight app operations: %w", err)
		}
		if inFlightCount > 0 {
			return model.Operation{}, ErrConflict
		}
		targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
		if targetRuntimeID == "" && app.Spec.Failover != nil {
			targetRuntimeID = strings.TrimSpace(app.Spec.Failover.TargetRuntimeID)
		}
		if targetRuntimeID == "" {
			return model.Operation{}, ErrInvalidInput
		}
		visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, targetRuntimeID, op.TenantID)
		if err != nil {
			return model.Operation{}, err
		}
		if !visible {
			return model.Operation{}, ErrNotFound
		}
		targetRuntimeType, err := s.pgRuntimeTypeTx(ctx, tx, targetRuntimeID)
		if err != nil {
			return model.Operation{}, err
		}
		if err := validateFailoverTargetRuntimeType(targetRuntimeType); err != nil {
			return model.Operation{}, err
		}
		sourceRuntimeType, err := s.pgRuntimeTypeTx(ctx, tx, app.Spec.RuntimeID)
		if err != nil {
			return model.Operation{}, err
		}
		if err := validateFailoverTargetRuntimeType(sourceRuntimeType); err != nil {
			return model.Operation{}, err
		}
		if strings.TrimSpace(app.Spec.RuntimeID) == targetRuntimeID {
			return model.Operation{}, ErrInvalidInput
		}
		op.SourceRuntimeID = app.Spec.RuntimeID
		op.TargetRuntimeID = targetRuntimeID
	case model.OperationTypeDatabaseSwitchover:
		var inFlightCount int
		if err := tx.QueryRowContext(ctx, `
	SELECT COUNT(1)
	FROM fugue_operations
	WHERE app_id = $1
	  AND status IN ($2, $3, $4)
	`, app.ID, model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent).Scan(&inFlightCount); err != nil {
			return model.Operation{}, fmt.Errorf("count in-flight app operations: %w", err)
		}
		if inFlightCount > 0 {
			return model.Operation{}, ErrConflict
		}
		targetRuntimeID := strings.TrimSpace(op.TargetRuntimeID)
		if targetRuntimeID == "" {
			return model.Operation{}, ErrInvalidInput
		}
		visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, targetRuntimeID, op.TenantID)
		if err != nil {
			return model.Operation{}, err
		}
		if !visible {
			return model.Operation{}, ErrNotFound
		}
		postgresSpec := OwnedManagedPostgresSpec(app)
		if postgresSpec == nil {
			return model.Operation{}, ErrInvalidInput
		}
		sourceRuntimeID := strings.TrimSpace(postgresSpec.RuntimeID)
		if sourceRuntimeID == "" {
			sourceRuntimeID = strings.TrimSpace(app.Spec.RuntimeID)
		}
		if sourceRuntimeID == "" {
			return model.Operation{}, ErrInvalidInput
		}
		targetRuntimeType, err := s.pgRuntimeTypeTx(ctx, tx, targetRuntimeID)
		if err != nil {
			return model.Operation{}, err
		}
		if err := validateFailoverTargetRuntimeType(targetRuntimeType); err != nil {
			return model.Operation{}, err
		}
		sourceRuntimeType, err := s.pgRuntimeTypeTx(ctx, tx, sourceRuntimeID)
		if err != nil {
			return model.Operation{}, err
		}
		if err := validateFailoverTargetRuntimeType(sourceRuntimeType); err != nil {
			return model.Operation{}, err
		}
		if sourceRuntimeID == targetRuntimeID {
			return model.Operation{}, ErrInvalidInput
		}
		op.SourceRuntimeID = sourceRuntimeID
		op.TargetRuntimeID = targetRuntimeID
		if op.DesiredSpec == nil {
			op.DesiredSpec = cloneAppSpec(&app.Spec)
		}
	default:
		return model.Operation{}, ErrInvalidInput
	}

	now := time.Now().UTC()
	op.DesiredSpec = cloneAppSpec(op.DesiredSpec)
	op.DesiredSource = cloneAppSource(op.DesiredSource)
	billing, err := s.pgEnsureTenantBillingRecordTx(ctx, tx, app.TenantID, true, now)
	if err != nil {
		return model.Operation{}, err
	}
	billingState, err := s.pgLoadTenantBillingStateTx(ctx, tx, app.TenantID)
	if err != nil {
		return model.Operation{}, err
	}
	currentTotal, nextTotal, err := projectedTenantManagedTotalsWithBilling(&billingState, app, op, billing)
	if err != nil {
		return model.Operation{}, err
	}
	accrueTenantBillingWithCommittedStorage(&billing, currentTotal.StorageGibibytes, now)
	if err := s.pgUpdateTenantBillingRecordTx(ctx, tx, billing); err != nil {
		return model.Operation{}, err
	}
	effectiveBilling := billing
	nextEnvelope, envelopeChanged := nextManagedEnvelope(effectiveBilling, currentTotal, nextTotal)
	if envelopeChanged {
		effectiveBilling.ManagedCap = nextEnvelope
	}
	if err := validateManagedOperationBilling(effectiveBilling, currentTotal, nextTotal); err != nil {
		return model.Operation{}, err
	}
	if envelopeChanged {
		billing.ManagedCap = nextEnvelope
		billing.UpdatedAt = now
		if err := s.pgUpdateTenantBillingRecordTx(ctx, tx, billing); err != nil {
			return model.Operation{}, err
		}
		if err := s.pgInsertTenantBillingEventTx(ctx, tx, newTenantBillingConfigUpdatedEvent(
			app.TenantID,
			nextEnvelope,
			billing.BalanceMicroCents,
			now,
			map[string]string{"source": "auto-expand"},
		)); err != nil {
			return model.Operation{}, err
		}
	}
	op.ID = model.NewID("op")
	op.Status = model.OperationStatusPending
	op.ExecutionMode = model.ExecutionModeManaged
	op.ResultMessage = defaultInFlightOperationMessage(op)
	op.CreatedAt = now
	op.UpdatedAt = now

	desiredSpecJSON, err := marshalNullableJSON(op.DesiredSpec)
	if err != nil {
		return model.Operation{}, err
	}
	desiredSourceJSON, err := marshalNullableJSON(op.DesiredSource)
	if err != nil {
		return model.Operation{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_operations (id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, '', '', '', $15, $16, NULL, NULL)
`, op.ID, op.TenantID, op.Type, op.Status, op.ExecutionMode, op.RequestedByType, op.RequestedByID, op.AppID, op.SourceRuntimeID, op.TargetRuntimeID, intPointerValue(op.DesiredReplicas), desiredSpecJSON, desiredSourceJSON, op.ResultMessage, op.CreatedAt, op.UpdatedAt); err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	if err := applyInFlightOperationToAppModel(&app, &op); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.Operation{}, err
	}
	if err := s.notifyOperationTx(ctx, tx, op.ID); err != nil {
		return model.Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Operation{}, fmt.Errorf("commit create operation transaction: %w", err)
	}
	return op, nil
}

func (s *Store) pgListOperations(tenantID string, platformAdmin bool) ([]model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
FROM fugue_operations
`
	args := make([]any, 0, 1)
	if !platformAdmin {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list operations: %w", err)
	}
	defer rows.Close()

	ops := make([]model.Operation, 0)
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operations: %w", err)
	}
	return ops, nil
}

func (s *Store) pgListOperationsByApp(tenantID string, platformAdmin bool, appID string) ([]model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
FROM fugue_operations
WHERE app_id = $1
`
	args := []any{appID}
	if !platformAdmin {
		query += ` AND tenant_id = $2`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list operations by app: %w", err)
	}
	defer rows.Close()

	ops := make([]model.Operation, 0)
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operations by app: %w", err)
	}
	return ops, nil
}

func (s *Store) pgListActiveOperations() ([]model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
FROM fugue_operations
WHERE status IN ($1, $2, $3)
ORDER BY created_at ASC, id ASC
`, model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent)
	if err != nil {
		return nil, fmt.Errorf("list active operations: %w", err)
	}
	defer rows.Close()

	ops := make([]model.Operation, 0)
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active operations: %w", err)
	}
	return ops, nil
}

func (s *Store) pgGetOperation(id string) (model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	op, err := scanOperation(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
FROM fugue_operations
WHERE id = $1
`, id))
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	return op, nil
}

func (s *Store) pgTryClaimPendingOperation(id string) (model.Operation, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, false, fmt.Errorf("begin try-claim operation transaction: %w", err)
	}
	defer tx.Rollback()

	op, err := scanOperation(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
FROM fugue_operations
WHERE id = $1
  AND status = $2
FOR UPDATE SKIP LOCKED
`, id, model.OperationStatusPending))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Operation{}, false, nil
		}
		return model.Operation{}, false, fmt.Errorf("try claim pending operation %s: %w", id, err)
	}

	now := time.Now().UTC()
	if op.Type == model.OperationTypeImport {
		op.Status = model.OperationStatusRunning
		op.ExecutionMode = model.ExecutionModeManaged
		op.StartedAt = &now
		op.ResultMessage = defaultInFlightOperationMessage(op)
	} else {
		runtimeType, err := s.pgRuntimeTypeTx(ctx, tx, op.TargetRuntimeID)
		if err != nil {
			return model.Operation{}, false, err
		}
		if runtimeType == model.RuntimeTypeExternalOwned {
			op.Status = model.OperationStatusWaitingAgent
			op.ExecutionMode = model.ExecutionModeAgent
			op.AssignedRuntimeID = op.TargetRuntimeID
			op.ResultMessage = "task dispatched to external runtime agent"
		} else {
			op.Status = model.OperationStatusRunning
			op.ExecutionMode = model.ExecutionModeManaged
			op.StartedAt = &now
			op.ResultMessage = defaultInFlightOperationMessage(op)
		}
	}
	op.UpdatedAt = now

	if err := s.pgUpdateOperationTx(ctx, tx, op); err != nil {
		return model.Operation{}, false, err
	}

	app, err := s.pgGetAppTx(ctx, tx, op.AppID, true)
	if err != nil {
		return model.Operation{}, false, mapDBErr(err)
	}
	if err := applyInFlightOperationToAppModel(&app, &op); err != nil {
		return model.Operation{}, false, err
	}
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.Operation{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return model.Operation{}, false, fmt.Errorf("commit try-claim operation %s: %w", id, err)
	}
	return op, true, nil
}

func (s *Store) pgClaimNextPendingOperation() (model.Operation, bool, error) {
	return s.pgClaimNextPendingOperationWithFilter("")
}

func (s *Store) pgClaimNextPendingForegroundOperation() (model.Operation, bool, error) {
	return s.pgClaimNextPendingOperationWithFilter(`
  AND NOT (type = $2 AND requested_by_id = $3)
`, model.OperationTypeImport, model.OperationRequestedByGitHubSyncController)
}

func (s *Store) pgClaimNextPendingGitHubSyncImportOperation() (model.Operation, bool, error) {
	return s.pgClaimNextPendingOperationWithFilter(`
  AND type = $2
  AND requested_by_id = $3
`, model.OperationTypeImport, model.OperationRequestedByGitHubSyncController)
}

func (s *Store) pgClaimNextPendingOperationWithFilter(extraWhere string, args ...any) (model.Operation, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, false, fmt.Errorf("begin claim operation transaction: %w", err)
	}
	defer tx.Rollback()

	queryArgs := append([]any{model.OperationStatusPending}, args...)
	query := `
WITH next_op AS (
	SELECT id
	FROM fugue_operations
	WHERE status = $1
` + extraWhere + `
	ORDER BY created_at ASC
	FOR UPDATE SKIP LOCKED
	LIMIT 1
)
SELECT o.id, o.tenant_id, o.type, o.status, o.execution_mode, o.requested_by_type, o.requested_by_id, o.app_id, o.source_runtime_id, o.target_runtime_id, o.desired_replicas, o.desired_spec_json, o.desired_source_json, o.result_message, o.manifest_path, o.assigned_runtime_id, o.error_message, o.created_at, o.updated_at, o.started_at, o.completed_at
FROM fugue_operations o
JOIN next_op n ON n.id = o.id
`
	op, err := scanOperation(tx.QueryRowContext(ctx, query, queryArgs...))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Operation{}, false, nil
		}
		return model.Operation{}, false, fmt.Errorf("claim next pending operation: %w", err)
	}

	now := time.Now().UTC()
	if op.Type == model.OperationTypeImport {
		op.Status = model.OperationStatusRunning
		op.ExecutionMode = model.ExecutionModeManaged
		op.StartedAt = &now
		op.ResultMessage = defaultInFlightOperationMessage(op)
	} else {
		runtimeType, err := s.pgRuntimeTypeTx(ctx, tx, op.TargetRuntimeID)
		if err != nil {
			return model.Operation{}, false, err
		}
		if runtimeType == model.RuntimeTypeExternalOwned {
			op.Status = model.OperationStatusWaitingAgent
			op.ExecutionMode = model.ExecutionModeAgent
			op.AssignedRuntimeID = op.TargetRuntimeID
			op.ResultMessage = "task dispatched to external runtime agent"
		} else {
			op.Status = model.OperationStatusRunning
			op.ExecutionMode = model.ExecutionModeManaged
			op.StartedAt = &now
		}
	}
	op.UpdatedAt = now

	app, err := s.pgGetAppTx(ctx, tx, op.AppID, true)
	if err != nil {
		return model.Operation{}, false, mapDBErr(err)
	}
	if err := applyInFlightOperationToAppModel(&app, &op); err != nil {
		return model.Operation{}, false, err
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_operations
SET status = $2,
	execution_mode = $3,
	assigned_runtime_id = $4,
	result_message = $5,
	started_at = $6,
	updated_at = $7
WHERE id = $1
`, op.ID, op.Status, op.ExecutionMode, op.AssignedRuntimeID, op.ResultMessage, op.StartedAt, op.UpdatedAt); err != nil {
		return model.Operation{}, false, fmt.Errorf("update claimed operation %s: %w", op.ID, err)
	}
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.Operation{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return model.Operation{}, false, fmt.Errorf("commit claim operation transaction: %w", err)
	}
	return op, true, nil
}

func (s *Store) pgDispatchOperationToRuntime(id, runtimeID string) (model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, fmt.Errorf("begin dispatch operation transaction: %w", err)
	}
	defer tx.Rollback()

	op, err := s.pgGetOperationTx(ctx, tx, id, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	now := time.Now().UTC()
	op.Status = model.OperationStatusWaitingAgent
	op.ExecutionMode = model.ExecutionModeAgent
	op.AssignedRuntimeID = runtimeID
	op.ResultMessage = "task dispatched to external runtime agent"
	op.UpdatedAt = now

	app, err := s.pgGetAppTx(ctx, tx, op.AppID, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	if err := applyInFlightOperationToAppModel(&app, &op); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateOperationTx(ctx, tx, op); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Operation{}, fmt.Errorf("commit dispatch operation transaction: %w", err)
	}
	return op, nil
}

func (s *Store) pgCompleteOperation(id, runtimeID, manifestPath, message string, desiredSpec *model.AppSpec, desiredSource *model.AppSource) (model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, fmt.Errorf("begin complete operation transaction: %w", err)
	}
	defer tx.Rollback()

	op, err := s.pgGetOperationTx(ctx, tx, id, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	if runtimeID != "" && op.AssignedRuntimeID != runtimeID {
		return model.Operation{}, ErrNotFound
	}
	if !operationCanTransitionToCompleted(op) {
		return model.Operation{}, ErrConflict
	}
	if desiredSpec != nil {
		op.DesiredSpec = cloneAppSpec(desiredSpec)
	}
	if desiredSource != nil {
		op.DesiredSource = cloneAppSource(desiredSource)
	}

	now := time.Now().UTC()
	op.Status = model.OperationStatusCompleted
	op.UpdatedAt = now
	op.CompletedAt = &now
	op.ManifestPath = manifestPath
	op.ResultMessage = strings.TrimSpace(message)
	if op.StartedAt == nil {
		op.StartedAt = &now
	}

	app, err := s.pgGetAppTx(ctx, tx, op.AppID, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	if op.Type == model.OperationTypeDeploy || op.Type == model.OperationTypeDatabaseSwitchover {
		if err := s.pgApplyDesiredSpecBackingServicesTx(ctx, tx, &app, op.DesiredSpec); err != nil {
			return model.Operation{}, err
		}
	}
	if err := applyOperationToAppModel(&app, &op); err != nil {
		return model.Operation{}, err
	}

	if err := s.pgUpdateOperationTx(ctx, tx, op); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.Operation{}, err
	}
	if op.Type == model.OperationTypeDelete {
		if err := s.pgDeleteAppDomainsByAppTx(ctx, tx, app.ID); err != nil {
			return model.Operation{}, err
		}
		if err := s.pgDeleteServiceBindingsByAppTx(ctx, tx, app.ID); err != nil {
			return model.Operation{}, err
		}
		if err := s.pgDeleteOwnedBackingServicesByAppTx(ctx, tx, app.ID); err != nil {
			return model.Operation{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return model.Operation{}, fmt.Errorf("commit complete operation transaction: %w", err)
	}
	return op, nil
}

func (s *Store) pgFailOperation(id, message string) (model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, fmt.Errorf("begin fail operation transaction: %w", err)
	}
	defer tx.Rollback()

	op, err := s.pgGetOperationTx(ctx, tx, id, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}

	now := time.Now().UTC()
	op.Status = model.OperationStatusFailed
	op.UpdatedAt = now
	op.CompletedAt = &now
	op.ErrorMessage = strings.TrimSpace(message)
	if op.StartedAt == nil {
		op.StartedAt = &now
	}

	app, err := s.pgGetAppTx(ctx, tx, op.AppID, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	applyFailedOperationToAppModel(&app, &op)

	if err := s.pgUpdateOperationTx(ctx, tx, op); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Operation{}, fmt.Errorf("commit fail operation transaction: %w", err)
	}
	return op, nil
}

func (s *Store) pgListAssignedOperations(runtimeID string) ([]model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
FROM fugue_operations
WHERE assigned_runtime_id = $1
  AND status = $2
ORDER BY created_at ASC
`, runtimeID, model.OperationStatusWaitingAgent)
	if err != nil {
		return nil, fmt.Errorf("list assigned operations: %w", err)
	}
	defer rows.Close()

	ops := make([]model.Operation, 0)
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate assigned operations: %w", err)
	}
	return ops, nil
}

func (s *Store) pgRequeueManagedOperation(id, message string) (model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, fmt.Errorf("begin requeue operation transaction: %w", err)
	}
	defer tx.Rollback()

	op, err := s.pgGetOperationTx(ctx, tx, id, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	if !isRequeueableManagedOperation(op) {
		return model.Operation{}, ErrConflict
	}

	app, err := s.pgGetAppTx(ctx, tx, op.AppID, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}

	requeueManagedOperationState(&op, message)
	if err := applyInFlightOperationToAppModel(&app, &op); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateOperationTx(ctx, tx, op); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.Operation{}, err
	}
	if err := s.notifyOperationTx(ctx, tx, op.ID); err != nil {
		return model.Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Operation{}, fmt.Errorf("commit requeue operation transaction: %w", err)
	}
	return op, nil
}

func (s *Store) pgRequeueInFlightManagedOperations(message string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin bulk requeue transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
SELECT id
FROM fugue_operations
WHERE status = $1
  AND execution_mode = $2
ORDER BY created_at ASC
FOR UPDATE
`, model.OperationStatusRunning, model.ExecutionModeManaged)
	if err != nil {
		return 0, fmt.Errorf("list in-flight managed operations: %w", err)
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan in-flight managed operation id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate in-flight managed operations: %w", err)
	}
	rows.Close()

	for _, id := range ids {
		op, err := s.pgGetOperationTx(ctx, tx, id, true)
		if err != nil {
			return 0, mapDBErr(err)
		}
		if !isRequeueableManagedOperation(op) {
			continue
		}
		app, err := s.pgGetAppTx(ctx, tx, op.AppID, true)
		if err != nil {
			return 0, mapDBErr(err)
		}
		requeueManagedOperationState(&op, message)
		if err := applyInFlightOperationToAppModel(&app, &op); err != nil {
			return 0, err
		}
		if err := s.pgUpdateOperationTx(ctx, tx, op); err != nil {
			return 0, err
		}
		if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
			return 0, err
		}
		if err := s.notifyOperationTx(ctx, tx, op.ID); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit bulk requeue transaction: %w", err)
	}
	return len(ids), nil
}

func (s *Store) pgAppendAuditEvent(event model.AuditEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if event.ID == "" {
		event.ID = model.NewID("audit")
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	metadataJSON, err := marshalNullableJSON(event.Metadata)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO fugue_audit_events (id, tenant_id, actor_type, actor_id, action, target_type, target_id, metadata_json, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
`, event.ID, nullIfEmpty(event.TenantID), event.ActorType, event.ActorID, event.Action, event.TargetType, event.TargetID, metadataJSON, event.CreatedAt)
	if err != nil {
		return fmt.Errorf("append audit event: %w", err)
	}
	return nil
}

func (s *Store) pgListAuditEvents(tenantID string, platformAdmin bool) ([]model.AuditEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, actor_type, actor_id, action, target_type, target_id, metadata_json, created_at
FROM fugue_audit_events
`
	args := make([]any, 0, 1)
	if !platformAdmin {
		query += ` WHERE tenant_id = $1 OR tenant_id IS NULL`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at DESC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list audit events: %w", err)
	}
	defer rows.Close()

	events := make([]model.AuditEvent, 0)
	for rows.Next() {
		event, err := scanAuditEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}
	return events, nil
}

func (s *Store) pgTenantExistsTx(ctx context.Context, tx *sql.Tx, tenantID string) (bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fugue_tenants WHERE id = $1)`, tenantID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check tenant %s exists: %w", tenantID, err)
	}
	return exists, nil
}

func (s *Store) pgProjectBelongsToTenantTx(ctx context.Context, tx *sql.Tx, projectID, tenantID string) (bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_projects
	WHERE id = $1 AND tenant_id = $2
)
`, projectID, tenantID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check project %s belongs to tenant %s: %w", projectID, tenantID, err)
	}
	return exists, nil
}

func (s *Store) pgProjectHasLiveResourcesTx(ctx context.Context, tx *sql.Tx, projectID string) (bool, error) {
	var hasApps bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_apps
	WHERE project_id = $1
	  AND lower(COALESCE(status_json->>'phase', '')) <> 'deleted'
)
`, projectID).Scan(&hasApps); err != nil {
		return false, fmt.Errorf("check live apps for project %s: %w", projectID, err)
	}
	if hasApps {
		return true, nil
	}

	var hasServices bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_backing_services
	WHERE project_id = $1
)
`, projectID).Scan(&hasServices); err != nil {
		return false, fmt.Errorf("check backing services for project %s: %w", projectID, err)
	}
	return hasServices, nil
}

func (s *Store) pgRuntimeVisibleToTenantTx(ctx context.Context, tx *sql.Tx, runtimeID, tenantID string) (bool, error) {
	if strings.TrimSpace(runtimeID) == "" {
		return false, nil
	}

	var runtimeType string
	var accessMode string
	var owner sql.NullString
	if err := tx.QueryRowContext(ctx, `
SELECT type, access_mode, tenant_id
FROM fugue_runtimes
WHERE id = $1
`, runtimeID).Scan(&runtimeType, &accessMode, &owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("query runtime %s access: %w", runtimeID, err)
	}
	if runtimeType == model.RuntimeTypeManagedShared {
		return true, nil
	}
	if owner.Valid && owner.String == tenantID && tenantID != "" {
		return true, nil
	}
	if normalizeRuntimeAccessMode(runtimeType, accessMode) == model.RuntimeAccessModePlatformShared {
		return true, nil
	}
	if strings.TrimSpace(tenantID) == "" {
		return false, nil
	}

	var granted bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_runtime_access_grants
	WHERE runtime_id = $1
	  AND tenant_id = $2
)
`, runtimeID, tenantID).Scan(&granted); err != nil {
		return false, fmt.Errorf("query runtime %s grant for tenant %s: %w", runtimeID, tenantID, err)
	}
	return granted, nil
}

func (s *Store) pgRuntimeTypeTx(ctx context.Context, tx *sql.Tx, runtimeID string) (string, error) {
	if strings.TrimSpace(runtimeID) == "" {
		return "", nil
	}
	var runtimeType string
	if err := tx.QueryRowContext(ctx, `SELECT type FROM fugue_runtimes WHERE id = $1`, runtimeID).Scan(&runtimeType); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("query runtime %s type: %w", runtimeID, err)
	}
	return runtimeType, nil
}

func (s *Store) pgRuntimeVisibleToTenant(runtimeID, tenantID string, platformAdmin bool) (bool, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return false, ErrInvalidInput
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if platformAdmin {
		var exists bool
		if err := s.db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_runtimes
	WHERE id = $1
)
`, runtimeID).Scan(&exists); err != nil {
			return false, fmt.Errorf("check runtime %s exists: %w", runtimeID, err)
		}
		return exists, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin runtime visibility transaction: %w", err)
	}
	defer tx.Rollback()

	visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, runtimeID, tenantID)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit runtime visibility transaction: %w", err)
	}
	return visible, nil
}

func (s *Store) pgListRuntimeAccessGrants(runtimeID string) ([]model.RuntimeAccessGrant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var exists bool
	if err := s.db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_runtimes
	WHERE id = $1
)
`, runtimeID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check runtime %s exists: %w", runtimeID, err)
	}
	if !exists {
		return nil, ErrNotFound
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT runtime_id, tenant_id, created_at, updated_at
FROM fugue_runtime_access_grants
WHERE runtime_id = $1
ORDER BY created_at ASC
`, runtimeID)
	if err != nil {
		return nil, fmt.Errorf("list runtime access grants: %w", err)
	}
	defer rows.Close()

	grants := make([]model.RuntimeAccessGrant, 0)
	for rows.Next() {
		grant, err := scanRuntimeAccessGrant(rows)
		if err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtime access grants: %w", err)
	}
	return grants, nil
}

func (s *Store) pgGrantRuntimeAccess(runtimeID, ownerTenantID, granteeTenantID string) (model.RuntimeAccessGrant, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.RuntimeAccessGrant{}, fmt.Errorf("begin grant runtime access transaction: %w", err)
	}
	defer tx.Rollback()

	runtimeObj, err := s.pgGetRuntimeTx(ctx, tx, runtimeID, true)
	if err != nil {
		return model.RuntimeAccessGrant{}, err
	}
	if runtimeObj.TenantID == "" || runtimeObj.TenantID != ownerTenantID {
		return model.RuntimeAccessGrant{}, ErrNotFound
	}
	if runtimeObj.Type == model.RuntimeTypeManagedShared {
		return model.RuntimeAccessGrant{}, ErrInvalidInput
	}
	if granteeTenantID == runtimeObj.TenantID {
		return model.RuntimeAccessGrant{}, ErrInvalidInput
	}

	granteeExists, err := s.pgTenantExistsTx(ctx, tx, granteeTenantID)
	if err != nil {
		return model.RuntimeAccessGrant{}, err
	}
	if !granteeExists {
		return model.RuntimeAccessGrant{}, ErrNotFound
	}

	now := time.Now().UTC()
	grant, err := scanRuntimeAccessGrant(tx.QueryRowContext(ctx, `
INSERT INTO fugue_runtime_access_grants (runtime_id, tenant_id, created_at, updated_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (runtime_id, tenant_id) DO UPDATE SET
	updated_at = EXCLUDED.updated_at
RETURNING runtime_id, tenant_id, created_at, updated_at
`, runtimeID, granteeTenantID, now, now))
	if err != nil {
		return model.RuntimeAccessGrant{}, fmt.Errorf("grant runtime access: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return model.RuntimeAccessGrant{}, fmt.Errorf("commit grant runtime access transaction: %w", err)
	}
	return grant, nil
}

func (s *Store) pgRevokeRuntimeAccess(runtimeID, ownerTenantID, granteeTenantID string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin revoke runtime access transaction: %w", err)
	}
	defer tx.Rollback()

	runtimeObj, err := s.pgGetRuntimeTx(ctx, tx, runtimeID, true)
	if err != nil {
		return false, err
	}
	if runtimeObj.TenantID == "" || runtimeObj.TenantID != ownerTenantID {
		return false, ErrNotFound
	}

	result, err := tx.ExecContext(ctx, `
DELETE FROM fugue_runtime_access_grants
WHERE runtime_id = $1
  AND tenant_id = $2
`, runtimeID, granteeTenantID)
	if err != nil {
		return false, fmt.Errorf("revoke runtime access: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit revoke runtime access transaction: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	return rowsAffected > 0, nil
}

func (s *Store) pgSetRuntimeAccessMode(runtimeID, ownerTenantID, accessMode string) (model.Runtime, error) {
	accessMode = strings.TrimSpace(accessMode)
	switch accessMode {
	case model.RuntimeAccessModePrivate, model.RuntimeAccessModePlatformShared:
	default:
		return model.Runtime{}, ErrInvalidInput
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Runtime{}, fmt.Errorf("begin set runtime access mode transaction: %w", err)
	}
	defer tx.Rollback()

	runtimeObj, err := s.pgGetRuntimeTx(ctx, tx, runtimeID, true)
	if err != nil {
		return model.Runtime{}, err
	}
	if runtimeObj.TenantID == "" || runtimeObj.TenantID != ownerTenantID {
		return model.Runtime{}, ErrNotFound
	}
	if runtimeObj.Type == model.RuntimeTypeManagedShared {
		return model.Runtime{}, ErrInvalidInput
	}

	runtimeObj.AccessMode = normalizeRuntimeAccessMode(runtimeObj.Type, accessMode)
	runtimeObj.UpdatedAt = time.Now().UTC()
	if err := s.pgUpdateRuntimeTx(ctx, tx, runtimeObj); err != nil {
		return model.Runtime{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Runtime{}, fmt.Errorf("commit set runtime access mode transaction: %w", err)
	}
	return runtimeObj, nil
}

func (s *Store) pgSetRuntimePoolMode(runtimeID, poolMode string) (model.Runtime, error) {
	poolMode = strings.TrimSpace(poolMode)
	switch poolMode {
	case model.RuntimePoolModeDedicated, model.RuntimePoolModeInternalShared:
	default:
		return model.Runtime{}, ErrInvalidInput
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Runtime{}, fmt.Errorf("begin set runtime pool mode transaction: %w", err)
	}
	defer tx.Rollback()

	runtimeObj, err := s.pgGetRuntimeTx(ctx, tx, runtimeID, true)
	if err != nil {
		return model.Runtime{}, err
	}
	if runtimeObj.Type != model.RuntimeTypeManagedOwned || runtimeObj.TenantID == "" {
		return model.Runtime{}, ErrInvalidInput
	}

	runtimeObj.PoolMode = model.NormalizeRuntimePoolMode(runtimeObj.Type, poolMode)
	runtimeObj.UpdatedAt = time.Now().UTC()
	if err := s.pgUpdateRuntimeTx(ctx, tx, runtimeObj); err != nil {
		return model.Runtime{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Runtime{}, fmt.Errorf("commit set runtime pool mode transaction: %w", err)
	}
	return runtimeObj, nil
}

func (s *Store) pgNextAvailableRuntimeNameTx(ctx context.Context, tx *sql.Tx, tenantID, requested string) (string, error) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		requested = "node"
	}
	name := requested
	suffix := 2
	for {
		var exists bool
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_runtimes
	WHERE COALESCE(tenant_id, '') = COALESCE($1, '')
	  AND lower(name) = lower($2)
)
`, nullIfEmpty(tenantID), name).Scan(&exists); err != nil {
			return "", fmt.Errorf("check runtime name %s exists: %w", name, err)
		}
		if !exists {
			return name, nil
		}
		name = fmt.Sprintf("%s-%d", requested, suffix)
		suffix++
	}
}

func (s *Store) pgGetAppTx(ctx context.Context, tx *sql.Tx, id string, forUpdate bool) (model.App, error) {
	query := `
	SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
	FROM fugue_apps
WHERE id = $1
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	app, err := scanApp(tx.QueryRowContext(ctx, query, id))
	if err != nil {
		return model.App{}, err
	}
	if err := s.pgHydrateAppBackingServicesWithQueryer(ctx, tx, &app); err != nil {
		return model.App{}, err
	}
	return app, nil
}

func (s *Store) pgGetIdempotencyRecordTx(ctx context.Context, tx *sql.Tx, scope, tenantID, key string, forUpdate bool) (model.IdempotencyRecord, error) {
	query := `
SELECT scope, tenant_id, key, request_hash, status, app_id, operation_id, created_at, updated_at
FROM fugue_idempotency_keys
WHERE scope = $1
  AND tenant_id = $2
  AND key = $3
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	record, err := scanIdempotencyRecord(tx.QueryRowContext(ctx, query, scope, tenantID, key))
	if err != nil {
		return model.IdempotencyRecord{}, err
	}
	return record, nil
}

func (s *Store) pgGetOperationTx(ctx context.Context, tx *sql.Tx, id string, forUpdate bool) (model.Operation, error) {
	query := `
SELECT id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
FROM fugue_operations
WHERE id = $1
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	op, err := scanOperation(tx.QueryRowContext(ctx, query, id))
	if err != nil {
		return model.Operation{}, err
	}
	return op, nil
}

func (s *Store) pgUpdateOperationTx(ctx context.Context, tx *sql.Tx, op model.Operation) error {
	desiredSpecJSON, err := marshalNullableJSON(op.DesiredSpec)
	if err != nil {
		return err
	}
	desiredSourceJSON, err := marshalNullableJSON(op.DesiredSource)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_operations
SET tenant_id = $2,
	type = $3,
	status = $4,
	execution_mode = $5,
	requested_by_type = $6,
	requested_by_id = $7,
	app_id = $8,
	source_runtime_id = $9,
	target_runtime_id = $10,
	desired_replicas = $11,
	desired_spec_json = $12,
	desired_source_json = $13,
	result_message = $14,
	manifest_path = $15,
	assigned_runtime_id = $16,
	error_message = $17,
	created_at = $18,
	updated_at = $19,
	started_at = $20,
	completed_at = $21
WHERE id = $1
`, op.ID, op.TenantID, op.Type, op.Status, op.ExecutionMode, op.RequestedByType, op.RequestedByID, op.AppID, op.SourceRuntimeID, op.TargetRuntimeID, intPointerValue(op.DesiredReplicas), desiredSpecJSON, desiredSourceJSON, op.ResultMessage, op.ManifestPath, op.AssignedRuntimeID, op.ErrorMessage, op.CreatedAt, op.UpdatedAt, op.StartedAt, op.CompletedAt); err != nil {
		return fmt.Errorf("update operation %s: %w", op.ID, err)
	}
	return nil
}

func (s *Store) pgUpdateAppTx(ctx context.Context, tx *sql.Tx, app model.App) error {
	sourceJSON, err := marshalNullableJSON(app.Source)
	if err != nil {
		return err
	}
	routeJSON, err := marshalNullableJSON(app.Route)
	if err != nil {
		return err
	}
	specJSON, err := marshalJSON(app.Spec)
	if err != nil {
		return err
	}
	statusJSON, err := marshalJSON(app.Status)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_apps
SET tenant_id = $2,
	project_id = $3,
	name = $4,
	description = $5,
	source_json = $6,
	route_json = $7,
	spec_json = $8,
	status_json = $9,
	created_at = $10,
	updated_at = $11
WHERE id = $1
`, app.ID, app.TenantID, app.ProjectID, app.Name, app.Description, sourceJSON, routeJSON, specJSON, statusJSON, app.CreatedAt, app.UpdatedAt); err != nil {
		return fmt.Errorf("update app %s: %w", app.ID, err)
	}
	return nil
}

func scanTenant(scanner sqlScanner) (model.Tenant, error) {
	var tenant model.Tenant
	if err := scanner.Scan(&tenant.ID, &tenant.Name, &tenant.Slug, &tenant.Status, &tenant.CreatedAt, &tenant.UpdatedAt); err != nil {
		return model.Tenant{}, err
	}
	return tenant, nil
}

func scanProject(scanner sqlScanner) (model.Project, error) {
	var project model.Project
	var tenantID sql.NullString
	if err := scanner.Scan(&project.ID, &tenantID, &project.Name, &project.Slug, &project.Description, &project.CreatedAt, &project.UpdatedAt); err != nil {
		return model.Project{}, err
	}
	project.TenantID = tenantID.String
	return project, nil
}

func scanAPIKey(scanner sqlScanner) (model.APIKey, error) {
	var key model.APIKey
	var tenantID sql.NullString
	var status sql.NullString
	var scopesRaw []byte
	var lastUsedAt sql.NullTime
	var disabledAt sql.NullTime
	if err := scanner.Scan(&key.ID, &tenantID, &key.Label, &key.Prefix, &key.Hash, &status, &scopesRaw, &key.CreatedAt, &lastUsedAt, &disabledAt); err != nil {
		return model.APIKey{}, err
	}
	key.TenantID = tenantID.String
	key.Status = normalizeAPIKeyStatus(status.String)
	scopes, err := decodeJSONValue[[]string](scopesRaw)
	if err != nil {
		return model.APIKey{}, err
	}
	key.Scopes = scopes
	if lastUsedAt.Valid {
		key.LastUsedAt = &lastUsedAt.Time
	}
	if disabledAt.Valid {
		key.DisabledAt = &disabledAt.Time
	}
	normalizeAPIKeyForRead(&key)
	return key, nil
}

func scanEnrollmentToken(scanner sqlScanner) (model.EnrollmentToken, error) {
	var token model.EnrollmentToken
	var tenantID sql.NullString
	var usedAt sql.NullTime
	var lastUsedAt sql.NullTime
	if err := scanner.Scan(&token.ID, &tenantID, &token.Label, &token.Prefix, &token.Hash, &token.ExpiresAt, &usedAt, &token.CreatedAt, &lastUsedAt); err != nil {
		return model.EnrollmentToken{}, err
	}
	token.TenantID = tenantID.String
	if usedAt.Valid {
		token.UsedAt = &usedAt.Time
	}
	if lastUsedAt.Valid {
		token.LastUsedAt = &lastUsedAt.Time
	}
	return token, nil
}

func scanNodeKey(scanner sqlScanner) (model.NodeKey, error) {
	var key model.NodeKey
	var tenantID sql.NullString
	var lastUsedAt sql.NullTime
	var revokedAt sql.NullTime
	if err := scanner.Scan(&key.ID, &tenantID, &key.Label, &key.Prefix, &key.Hash, &key.Status, &key.CreatedAt, &key.UpdatedAt, &lastUsedAt, &revokedAt); err != nil {
		return model.NodeKey{}, err
	}
	key.TenantID = tenantID.String
	if lastUsedAt.Valid {
		key.LastUsedAt = &lastUsedAt.Time
	}
	if revokedAt.Valid {
		key.RevokedAt = &revokedAt.Time
	}
	return key, nil
}

func scanRuntime(scanner sqlScanner) (model.Runtime, error) {
	var runtime model.Runtime
	var tenantID sql.NullString
	var machineName sql.NullString
	var accessMode sql.NullString
	var poolMode sql.NullString
	var connectionMode sql.NullString
	var endpoint sql.NullString
	var labelsRaw []byte
	var nodeKeyID sql.NullString
	var clusterNodeName sql.NullString
	var fingerprintPrefix sql.NullString
	var fingerprintHash sql.NullString
	var agentKeyPrefix sql.NullString
	var agentKeyHash sql.NullString
	var lastSeenAt sql.NullTime
	var lastHeartbeatAt sql.NullTime
	if err := scanner.Scan(
		&runtime.ID,
		&tenantID,
		&runtime.Name,
		&machineName,
		&runtime.Type,
		&accessMode,
		&poolMode,
		&connectionMode,
		&runtime.Status,
		&endpoint,
		&labelsRaw,
		&nodeKeyID,
		&clusterNodeName,
		&fingerprintPrefix,
		&fingerprintHash,
		&agentKeyPrefix,
		&agentKeyHash,
		&lastSeenAt,
		&lastHeartbeatAt,
		&runtime.CreatedAt,
		&runtime.UpdatedAt,
	); err != nil {
		return model.Runtime{}, err
	}
	runtime.TenantID = tenantID.String
	runtime.MachineName = machineName.String
	runtime.AccessMode = normalizeRuntimeAccessMode(runtime.Type, accessMode.String)
	runtime.PoolMode = model.NormalizeRuntimePoolMode(runtime.Type, poolMode.String)
	runtime.ConnectionMode = connectionMode.String
	runtime.Endpoint = endpoint.String
	runtime.NodeKeyID = nodeKeyID.String
	runtime.ClusterNodeName = clusterNodeName.String
	runtime.FingerprintPrefix = fingerprintPrefix.String
	runtime.FingerprintHash = fingerprintHash.String
	runtime.AgentKeyPrefix = agentKeyPrefix.String
	runtime.AgentKeyHash = agentKeyHash.String
	labels, err := decodeJSONValue[map[string]string](labelsRaw)
	if err != nil {
		return model.Runtime{}, err
	}
	runtime.Labels = labels
	if lastSeenAt.Valid {
		runtime.LastSeenAt = &lastSeenAt.Time
	}
	if lastHeartbeatAt.Valid {
		runtime.LastHeartbeatAt = &lastHeartbeatAt.Time
	}
	return runtime, nil
}

func scanRuntimeAccessGrant(scanner sqlScanner) (model.RuntimeAccessGrant, error) {
	var grant model.RuntimeAccessGrant
	if err := scanner.Scan(&grant.RuntimeID, &grant.TenantID, &grant.CreatedAt, &grant.UpdatedAt); err != nil {
		return model.RuntimeAccessGrant{}, err
	}
	return grant, nil
}

func scanApp(scanner sqlScanner) (model.App, error) {
	var app model.App
	var sourceRaw []byte
	var routeRaw []byte
	var specRaw []byte
	var statusRaw []byte
	if err := scanner.Scan(&app.ID, &app.TenantID, &app.ProjectID, &app.Name, &app.Description, &sourceRaw, &routeRaw, &specRaw, &statusRaw, &app.CreatedAt, &app.UpdatedAt); err != nil {
		return model.App{}, err
	}
	source, err := decodeJSONPointer[model.AppSource](sourceRaw)
	if err != nil {
		return model.App{}, err
	}
	route, err := decodeJSONPointer[model.AppRoute](routeRaw)
	if err != nil {
		return model.App{}, err
	}
	spec, err := decodeJSONValue[model.AppSpec](specRaw)
	if err != nil {
		return model.App{}, err
	}
	status, err := decodeJSONValue[model.AppStatus](statusRaw)
	if err != nil {
		return model.App{}, err
	}
	app.Source = source
	app.Route = route
	app.Spec = spec
	app.Status = status
	model.ApplyAppSpecDefaults(&app.Spec)
	return app, nil
}

func scanIdempotencyRecord(scanner sqlScanner) (model.IdempotencyRecord, error) {
	var record model.IdempotencyRecord
	if err := scanner.Scan(
		&record.Scope,
		&record.TenantID,
		&record.Key,
		&record.RequestHash,
		&record.Status,
		&record.AppID,
		&record.OperationID,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return model.IdempotencyRecord{}, err
	}
	return record, nil
}

func scanOperation(scanner sqlScanner) (model.Operation, error) {
	var op model.Operation
	var desiredReplicas sql.NullInt64
	var desiredSpecRaw []byte
	var desiredSourceRaw []byte
	var startedAt sql.NullTime
	var completedAt sql.NullTime
	if err := scanner.Scan(&op.ID, &op.TenantID, &op.Type, &op.Status, &op.ExecutionMode, &op.RequestedByType, &op.RequestedByID, &op.AppID, &op.SourceRuntimeID, &op.TargetRuntimeID, &desiredReplicas, &desiredSpecRaw, &desiredSourceRaw, &op.ResultMessage, &op.ManifestPath, &op.AssignedRuntimeID, &op.ErrorMessage, &op.CreatedAt, &op.UpdatedAt, &startedAt, &completedAt); err != nil {
		return model.Operation{}, err
	}
	if desiredReplicas.Valid {
		value := int(desiredReplicas.Int64)
		op.DesiredReplicas = &value
	}
	desiredSpec, err := decodeJSONPointer[model.AppSpec](desiredSpecRaw)
	if err != nil {
		return model.Operation{}, err
	}
	model.ApplyAppSpecDefaults(desiredSpec)
	desiredSource, err := decodeJSONPointer[model.AppSource](desiredSourceRaw)
	if err != nil {
		return model.Operation{}, err
	}
	op.DesiredSpec = desiredSpec
	op.DesiredSource = desiredSource
	if startedAt.Valid {
		op.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		op.CompletedAt = &completedAt.Time
	}
	return op, nil
}

func scanAuditEvent(scanner sqlScanner) (model.AuditEvent, error) {
	var event model.AuditEvent
	var tenantID sql.NullString
	var metadataRaw []byte
	if err := scanner.Scan(&event.ID, &tenantID, &event.ActorType, &event.ActorID, &event.Action, &event.TargetType, &event.TargetID, &metadataRaw, &event.CreatedAt); err != nil {
		return model.AuditEvent{}, err
	}
	event.TenantID = tenantID.String
	metadata, err := decodeJSONValue[map[string]string](metadataRaw)
	if err != nil {
		return model.AuditEvent{}, err
	}
	event.Metadata = metadata
	return event, nil
}

func applyOperationToAppModel(app *model.App, op *model.Operation) error {
	now := time.Now().UTC()
	switch op.Type {
	case model.OperationTypeImport:
		// Import only prepares a build artifact; the deploy operation applies it.
	case model.OperationTypeDeploy:
		if op.DesiredSpec == nil {
			return ErrInvalidInput
		}
		app.Spec = *op.DesiredSpec
		if app.Route != nil {
			app.Route.ServicePort = firstPositiveSpecPort(app.Spec.Ports)
		}
		if op.DesiredSource != nil {
			app.Source = cloneAppSource(op.DesiredSource)
		}
		app.Status.Phase = "deployed"
		app.Status.CurrentRuntimeID = app.Spec.RuntimeID
		app.Status.CurrentReplicas = app.Spec.Replicas
		if op.ExecutionMode != model.ExecutionModeManaged {
			app.Status.CurrentReleaseStartedAt = nil
			app.Status.CurrentReleaseReadyAt = nil
		}
	case model.OperationTypeScale:
		if op.DesiredReplicas == nil {
			return ErrInvalidInput
		}
		app.Spec.Replicas = *op.DesiredReplicas
		if *op.DesiredReplicas == 0 {
			app.Status.Phase = "disabled"
		} else {
			app.Status.Phase = "scaled"
		}
		app.Status.CurrentRuntimeID = app.Spec.RuntimeID
		app.Status.CurrentReplicas = *op.DesiredReplicas
		if *op.DesiredReplicas == 0 || op.ExecutionMode != model.ExecutionModeManaged {
			app.Status.CurrentReleaseStartedAt = nil
			app.Status.CurrentReleaseReadyAt = nil
		}
	case model.OperationTypeDelete:
		app.Name = deletedAppName(app.Name, op.ID)
		app.Route = nil
		app.Spec.Replicas = 0
		app.Status.Phase = "deleted"
		app.Status.CurrentRuntimeID = ""
		app.Status.CurrentReplicas = 0
		app.Status.CurrentReleaseStartedAt = nil
		app.Status.CurrentReleaseReadyAt = nil
	case model.OperationTypeMigrate:
		if op.TargetRuntimeID == "" {
			return ErrInvalidInput
		}
		app.Spec.RuntimeID = op.TargetRuntimeID
		app.Status.Phase = "migrated"
		app.Status.CurrentRuntimeID = op.TargetRuntimeID
		app.Status.CurrentReplicas = app.Spec.Replicas
		if op.ExecutionMode != model.ExecutionModeManaged {
			app.Status.CurrentReleaseStartedAt = nil
			app.Status.CurrentReleaseReadyAt = nil
		}
	case model.OperationTypeFailover:
		if op.TargetRuntimeID == "" {
			return ErrInvalidInput
		}
		app.Spec.RuntimeID = op.TargetRuntimeID
		app.Status.Phase = "failed-over"
		app.Status.CurrentRuntimeID = op.TargetRuntimeID
		app.Status.CurrentReplicas = app.Spec.Replicas
		if op.ExecutionMode != model.ExecutionModeManaged {
			app.Status.CurrentReleaseStartedAt = nil
			app.Status.CurrentReleaseReadyAt = nil
		}
	case model.OperationTypeDatabaseSwitchover:
		if op.DesiredSpec == nil {
			return ErrInvalidInput
		}
		app.Spec = *op.DesiredSpec
	default:
		return ErrInvalidInput
	}
	if op.Type != model.OperationTypeDatabaseSwitchover {
		app.Status.LastOperationID = op.ID
		app.Status.LastMessage = op.ResultMessage
	}
	app.Status.UpdatedAt = now
	app.UpdatedAt = now
	return nil
}

func applyInFlightOperationToAppModel(app *model.App, op *model.Operation) error {
	if op.Type == model.OperationTypeDatabaseSwitchover {
		return nil
	}
	phase, err := inFlightOperationPhase(*op)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	app.Status.Phase = phase
	app.Status.LastOperationID = op.ID
	app.Status.LastMessage = effectiveInFlightOperationMessage(*op)
	app.Status.UpdatedAt = now
	app.UpdatedAt = now
	return nil
}

func applyFailedOperationToAppModel(app *model.App, op *model.Operation) {
	if op.Type == model.OperationTypeDatabaseSwitchover {
		return
	}
	now := time.Now().UTC()
	app.Status.Phase = failedPhaseForApp(*app)
	app.Status.LastOperationID = op.ID
	app.Status.LastMessage = strings.TrimSpace(op.ErrorMessage)
	app.Status.UpdatedAt = now
	app.UpdatedAt = now
}

func sortOperationsByCreatedAt(ops []model.Operation) {
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].CreatedAt.Before(ops[j].CreatedAt)
	})
}
