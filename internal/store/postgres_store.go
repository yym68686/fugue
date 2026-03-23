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

	_, err := s.db.ExecContext(ctx, `
INSERT INTO fugue_tenants (id, name, slug, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
`, tenant.ID, tenant.Name, tenant.Slug, tenant.Status, tenant.CreatedAt, tenant.UpdatedAt)
	if err != nil {
		return model.Tenant{}, mapDBErr(err)
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

func (s *Store) pgListAPIKeys(tenantID string, platformAdmin bool) ([]model.APIKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, label, prefix, hash, scopes_json, created_at, last_used_at
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

func (s *Store) pgCreateAPIKey(tenantID, label string, scopes []string) (model.APIKey, string, error) {
	secret := model.NewSecret("fugue_pk")
	key := model.APIKey{
		ID:        model.NewID("apikey"),
		TenantID:  tenantID,
		Label:     label,
		Prefix:    model.SecretPrefix(secret),
		Hash:      model.HashSecret(secret),
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
INSERT INTO fugue_api_keys (id, tenant_id, label, prefix, hash, scopes_json, created_at, last_used_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)
`, key.ID, nullIfEmpty(key.TenantID), key.Label, key.Prefix, key.Hash, scopesJSON, key.CreatedAt); err != nil {
		return model.APIKey{}, "", mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.APIKey{}, "", fmt.Errorf("commit create api key transaction: %w", err)
	}

	return redactAPIKey(key), secret, nil
}

func (s *Store) pgAuthenticateAPIKey(secret string) (model.Principal, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key, err := scanAPIKey(s.db.QueryRowContext(ctx, `
UPDATE fugue_api_keys
SET last_used_at = NOW()
WHERE hash = $1
RETURNING id, tenant_id, label, prefix, hash, scopes_json, created_at, last_used_at
`, model.HashSecret(secret)))
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

func (s *Store) pgBootstrapNode(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.NodeKey, model.Runtime, string, model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, fmt.Errorf("begin bootstrap node transaction: %w", err)
	}
	defer tx.Rollback()

	key, err := scanNodeKey(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, status, created_at, updated_at, last_used_at, revoked_at
FROM fugue_node_keys
WHERE hash = $1
FOR UPDATE
`, model.HashSecret(secret)))
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, mapDBErr(err)
	}
	if key.RevokedAt != nil || key.Status == model.NodeKeyStatusRevoked {
		return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, ErrConflict
	}

	now := time.Now().UTC()
	key.LastUsedAt = &now
	key.UpdatedAt = now
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_node_keys
SET last_used_at = $2, updated_at = $3
WHERE id = $1
`, key.ID, key.LastUsedAt, key.UpdatedAt); err != nil {
		return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, fmt.Errorf("update node key last_used_at: %w", err)
	}

	endpoint = strings.TrimSpace(endpoint)
	explicitFingerprint := strings.TrimSpace(machineFingerprint) != ""
	machineName = normalizedMachineName(machineName, runtimeName, endpoint)
	machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, runtimeName, endpoint)
	fingerprintHash := model.HashSecret(machineFingerprint)

	machine, found, err := s.pgFindMachineByFingerprintTx(ctx, tx, key.TenantID, fingerprintHash, true)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, err
	}
	if !found && explicitFingerprint {
		machine, found, err = s.pgFindMachineCandidateTx(ctx, tx, key.TenantID, key.ID, model.MachineConnectionModeAgent, machineName, runtimeName, endpoint)
		if err != nil {
			return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, err
		}
	}

	runtimeSecret := model.NewSecret("fugue_rt")

	if found {
		machine.Name = machineName
		machine.ConnectionMode = model.MachineConnectionModeAgent
		machine.Status = model.RuntimeStatusActive
		machine.Endpoint = endpoint
		machine.Labels = cloneMap(labels)
		machine.NodeKeyID = key.ID
		machine.FingerprintPrefix = model.SecretPrefix(machineFingerprint)
		machine.FingerprintHash = fingerprintHash
		machine.LastSeenAt = &now
		machine.UpdatedAt = now

		runtime, err := s.pgGetRuntimeTx(ctx, tx, machine.RuntimeID, true)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, err
		}
		if errors.Is(err, ErrNotFound) {
			name, err := s.pgNextAvailableRuntimeNameTx(ctx, tx, key.TenantID, runtimeName)
			if err != nil {
				return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, err
			}
			runtime = model.Runtime{
				ID:              model.NewID("runtime"),
				TenantID:        key.TenantID,
				Name:            name,
				Type:            model.RuntimeTypeExternalOwned,
				Status:          model.RuntimeStatusActive,
				Endpoint:        endpoint,
				Labels:          cloneMap(labels),
				NodeKeyID:       key.ID,
				AgentKeyPrefix:  model.SecretPrefix(runtimeSecret),
				AgentKeyHash:    model.HashSecret(runtimeSecret),
				LastHeartbeatAt: &now,
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			labelsJSON, err := marshalNullableJSON(runtime.Labels)
			if err != nil {
				return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, err
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
`, runtime.ID, nullIfEmpty(runtime.TenantID), runtime.Name, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, nullIfEmpty(runtime.NodeKeyID), runtime.AgentKeyPrefix, runtime.AgentKeyHash, runtime.LastHeartbeatAt, runtime.CreatedAt, runtime.UpdatedAt); err != nil {
				return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, mapDBErr(err)
			}
		} else {
			runtime.Type = model.RuntimeTypeExternalOwned
			runtime.Status = model.RuntimeStatusActive
			runtime.Endpoint = endpoint
			runtime.Labels = cloneMap(labels)
			runtime.NodeKeyID = key.ID
			runtime.AgentKeyPrefix = model.SecretPrefix(runtimeSecret)
			runtime.AgentKeyHash = model.HashSecret(runtimeSecret)
			runtime.LastHeartbeatAt = &now
			runtime.UpdatedAt = now
			labelsJSON, err := marshalNullableJSON(runtime.Labels)
			if err != nil {
				return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, err
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE fugue_runtimes
SET type = $2,
	status = $3,
	endpoint = $4,
	labels_json = $5,
	node_key_id = $6,
	agent_key_prefix = $7,
	agent_key_hash = $8,
	last_heartbeat_at = $9,
	updated_at = $10
WHERE id = $1
`, runtime.ID, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, nullIfEmpty(runtime.NodeKeyID), runtime.AgentKeyPrefix, runtime.AgentKeyHash, runtime.LastHeartbeatAt, runtime.UpdatedAt); err != nil {
				return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, fmt.Errorf("update external-owned runtime %s: %w", runtime.ID, err)
			}
		}

		machine.RuntimeID = runtime.ID
		machine.RuntimeName = runtime.Name
		machine.ClusterNodeName = ""
		if err := s.pgUpdateMachineTx(ctx, tx, machine); err != nil {
			return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, err
		}
		if err := tx.Commit(); err != nil {
			return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, fmt.Errorf("commit bootstrap node transaction: %w", err)
		}
		return redactNodeKey(key), runtime, runtimeSecret, redactMachine(machine), nil
	}

	name, err := s.pgNextAvailableRuntimeNameTx(ctx, tx, key.TenantID, runtimeName)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, err
	}
	runtime := model.Runtime{
		ID:              model.NewID("runtime"),
		TenantID:        key.TenantID,
		Name:            name,
		Type:            model.RuntimeTypeExternalOwned,
		Status:          model.RuntimeStatusActive,
		Endpoint:        endpoint,
		Labels:          cloneMap(labels),
		NodeKeyID:       key.ID,
		AgentKeyPrefix:  model.SecretPrefix(runtimeSecret),
		AgentKeyHash:    model.HashSecret(runtimeSecret),
		LastHeartbeatAt: &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	labelsJSON, err := marshalNullableJSON(runtime.Labels)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
`, runtime.ID, nullIfEmpty(runtime.TenantID), runtime.Name, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, nullIfEmpty(runtime.NodeKeyID), runtime.AgentKeyPrefix, runtime.AgentKeyHash, runtime.LastHeartbeatAt, runtime.CreatedAt, runtime.UpdatedAt); err != nil {
		return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, mapDBErr(err)
	}

	machine = model.Machine{
		ID:                model.NewID("machine"),
		TenantID:          key.TenantID,
		Name:              machineName,
		ConnectionMode:    model.MachineConnectionModeAgent,
		Status:            model.RuntimeStatusActive,
		Endpoint:          endpoint,
		Labels:            cloneMap(labels),
		NodeKeyID:         key.ID,
		RuntimeID:         runtime.ID,
		RuntimeName:       runtime.Name,
		FingerprintPrefix: model.SecretPrefix(machineFingerprint),
		FingerprintHash:   fingerprintHash,
		LastSeenAt:        &now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := s.pgInsertMachineTx(ctx, tx, machine); err != nil {
		return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, err
	}

	if err := tx.Commit(); err != nil {
		return model.NodeKey{}, model.Runtime{}, "", model.Machine{}, fmt.Errorf("commit bootstrap node transaction: %w", err)
	}
	return redactNodeKey(key), runtime, runtimeSecret, redactMachine(machine), nil
}

func (s *Store) pgBootstrapClusterNode(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.NodeKey, model.Runtime, model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, model.Machine{}, fmt.Errorf("begin bootstrap cluster node transaction: %w", err)
	}
	defer tx.Rollback()

	key, err := scanNodeKey(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, status, created_at, updated_at, last_used_at, revoked_at
FROM fugue_node_keys
WHERE hash = $1
FOR UPDATE
`, model.HashSecret(secret)))
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, model.Machine{}, mapDBErr(err)
	}
	if key.RevokedAt != nil || key.Status == model.NodeKeyStatusRevoked {
		return model.NodeKey{}, model.Runtime{}, model.Machine{}, ErrConflict
	}

	now := time.Now().UTC()
	key.LastUsedAt = &now
	key.UpdatedAt = now
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_node_keys
SET last_used_at = $2, updated_at = $3
WHERE id = $1
`, key.ID, key.LastUsedAt, key.UpdatedAt); err != nil {
		return model.NodeKey{}, model.Runtime{}, model.Machine{}, fmt.Errorf("update node key last_used_at: %w", err)
	}

	endpoint = strings.TrimSpace(endpoint)
	explicitFingerprint := strings.TrimSpace(machineFingerprint) != ""
	machineName = normalizedMachineName(machineName, runtimeName, endpoint)
	machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, runtimeName, endpoint)
	fingerprintHash := model.HashSecret(machineFingerprint)

	machine, found, err := s.pgFindMachineByFingerprintTx(ctx, tx, key.TenantID, fingerprintHash, true)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, model.Machine{}, err
	}
	if !found && explicitFingerprint {
		machine, found, err = s.pgFindMachineCandidateTx(ctx, tx, key.TenantID, key.ID, model.MachineConnectionModeCluster, machineName, runtimeName, endpoint)
		if err != nil {
			return model.NodeKey{}, model.Runtime{}, model.Machine{}, err
		}
	}

	if found {
		machine.Name = machineName
		machine.ConnectionMode = model.MachineConnectionModeCluster
		machine.Status = model.RuntimeStatusActive
		machine.Endpoint = endpoint
		machine.Labels = cloneMap(labels)
		machine.NodeKeyID = key.ID
		machine.FingerprintPrefix = model.SecretPrefix(machineFingerprint)
		machine.FingerprintHash = fingerprintHash
		machine.LastSeenAt = &now
		machine.UpdatedAt = now

		runtime, err := s.pgGetRuntimeTx(ctx, tx, machine.RuntimeID, true)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return model.NodeKey{}, model.Runtime{}, model.Machine{}, err
		}
		if errors.Is(err, ErrNotFound) {
			name, err := s.pgNextAvailableRuntimeNameTx(ctx, tx, key.TenantID, runtimeName)
			if err != nil {
				return model.NodeKey{}, model.Runtime{}, model.Machine{}, err
			}
			runtime = model.Runtime{
				ID:              model.NewID("runtime"),
				TenantID:        key.TenantID,
				Name:            name,
				Type:            model.RuntimeTypeManagedOwned,
				Status:          model.RuntimeStatusActive,
				Endpoint:        endpoint,
				Labels:          cloneMap(labels),
				NodeKeyID:       key.ID,
				LastHeartbeatAt: &now,
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			labelsJSON, err := marshalNullableJSON(runtime.Labels)
			if err != nil {
				return model.NodeKey{}, model.Runtime{}, model.Machine{}, err
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '', '', $9, $10, $11)
`, runtime.ID, nullIfEmpty(runtime.TenantID), runtime.Name, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, nullIfEmpty(runtime.NodeKeyID), runtime.LastHeartbeatAt, runtime.CreatedAt, runtime.UpdatedAt); err != nil {
				return model.NodeKey{}, model.Runtime{}, model.Machine{}, mapDBErr(err)
			}
		} else {
			runtime.Type = model.RuntimeTypeManagedOwned
			runtime.Status = model.RuntimeStatusActive
			runtime.Endpoint = endpoint
			runtime.Labels = cloneMap(labels)
			runtime.NodeKeyID = key.ID
			runtime.AgentKeyPrefix = ""
			runtime.AgentKeyHash = ""
			runtime.LastHeartbeatAt = &now
			runtime.UpdatedAt = now
			labelsJSON, err := marshalNullableJSON(runtime.Labels)
			if err != nil {
				return model.NodeKey{}, model.Runtime{}, model.Machine{}, err
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE fugue_runtimes
SET type = $2,
	status = $3,
	endpoint = $4,
	labels_json = $5,
	node_key_id = $6,
	agent_key_prefix = '',
	agent_key_hash = '',
	last_heartbeat_at = $7,
	updated_at = $8
WHERE id = $1
`, runtime.ID, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, nullIfEmpty(runtime.NodeKeyID), runtime.LastHeartbeatAt, runtime.UpdatedAt); err != nil {
				return model.NodeKey{}, model.Runtime{}, model.Machine{}, fmt.Errorf("update managed-owned runtime %s: %w", runtime.ID, err)
			}
		}

		machine.RuntimeID = runtime.ID
		machine.RuntimeName = runtime.Name
		machine.ClusterNodeName = runtime.Name
		if err := s.pgUpdateMachineTx(ctx, tx, machine); err != nil {
			return model.NodeKey{}, model.Runtime{}, model.Machine{}, err
		}
		if err := tx.Commit(); err != nil {
			return model.NodeKey{}, model.Runtime{}, model.Machine{}, fmt.Errorf("commit bootstrap cluster node transaction: %w", err)
		}
		return redactNodeKey(key), runtime, redactMachine(machine), nil
	}

	name, err := s.pgNextAvailableRuntimeNameTx(ctx, tx, key.TenantID, runtimeName)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, model.Machine{}, err
	}
	runtime := model.Runtime{
		ID:              model.NewID("runtime"),
		TenantID:        key.TenantID,
		Name:            name,
		Type:            model.RuntimeTypeManagedOwned,
		Status:          model.RuntimeStatusActive,
		Endpoint:        endpoint,
		Labels:          cloneMap(labels),
		NodeKeyID:       key.ID,
		LastHeartbeatAt: &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	labelsJSON, err := marshalNullableJSON(runtime.Labels)
	if err != nil {
		return model.NodeKey{}, model.Runtime{}, model.Machine{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '', '', $9, $10, $11)
`, runtime.ID, nullIfEmpty(runtime.TenantID), runtime.Name, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, nullIfEmpty(runtime.NodeKeyID), runtime.LastHeartbeatAt, runtime.CreatedAt, runtime.UpdatedAt); err != nil {
		return model.NodeKey{}, model.Runtime{}, model.Machine{}, mapDBErr(err)
	}

	machine = model.Machine{
		ID:                model.NewID("machine"),
		TenantID:          key.TenantID,
		Name:              machineName,
		ConnectionMode:    model.MachineConnectionModeCluster,
		Status:            model.RuntimeStatusActive,
		Endpoint:          endpoint,
		Labels:            cloneMap(labels),
		NodeKeyID:         key.ID,
		RuntimeID:         runtime.ID,
		RuntimeName:       runtime.Name,
		ClusterNodeName:   runtime.Name,
		FingerprintPrefix: model.SecretPrefix(machineFingerprint),
		FingerprintHash:   fingerprintHash,
		LastSeenAt:        &now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := s.pgInsertMachineTx(ctx, tx, machine); err != nil {
		return model.NodeKey{}, model.Runtime{}, model.Machine{}, err
	}

	if err := tx.Commit(); err != nil {
		return model.NodeKey{}, model.Runtime{}, model.Machine{}, fmt.Errorf("commit bootstrap cluster node transaction: %w", err)
	}
	return redactNodeKey(key), runtime, redactMachine(machine), nil
}

func (s *Store) pgCreateRuntime(tenantID, name, runtimeType, endpoint string, labels map[string]string) (model.Runtime, string, error) {
	secret := model.NewSecret("fugue_rt")
	now := time.Now().UTC()
	runtime := model.Runtime{
		ID:             model.NewID("runtime"),
		TenantID:       tenantID,
		Name:           name,
		Type:           runtimeType,
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
	labelsJSON, err := marshalNullableJSON(runtime.Labels)
	if err != nil {
		return model.Runtime{}, "", err
	}

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
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, $9, NULL, $10, $11)
`, runtime.ID, nullIfEmpty(runtime.TenantID), runtime.Name, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, runtime.AgentKeyPrefix, runtime.AgentKeyHash, runtime.CreatedAt, runtime.UpdatedAt); err != nil {
		return model.Runtime{}, "", mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.Runtime{}, "", fmt.Errorf("commit create runtime transaction: %w", err)
	}
	return runtime, secret, nil
}

func (s *Store) pgConsumeEnrollmentToken(secret, runtimeName, endpoint string, labels map[string]string, machineName, machineFingerprint string) (model.Runtime, string, model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Runtime{}, "", model.Machine{}, fmt.Errorf("begin consume enrollment token transaction: %w", err)
	}
	defer tx.Rollback()

	token, err := scanEnrollmentToken(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, label, prefix, hash, expires_at, used_at, created_at, last_used_at
FROM fugue_enrollment_tokens
WHERE hash = $1
FOR UPDATE
`, model.HashSecret(secret)))
	if err != nil {
		return model.Runtime{}, "", model.Machine{}, mapDBErr(err)
	}
	now := time.Now().UTC()
	if token.UsedAt != nil || token.ExpiresAt.Before(now) {
		return model.Runtime{}, "", model.Machine{}, ErrConflict
	}
	token.UsedAt = &now
	token.LastUsedAt = &now
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_enrollment_tokens
SET used_at = $2, last_used_at = $3
WHERE id = $1
`, token.ID, token.UsedAt, token.LastUsedAt); err != nil {
		return model.Runtime{}, "", model.Machine{}, fmt.Errorf("update enrollment token usage: %w", err)
	}

	endpoint = strings.TrimSpace(endpoint)
	explicitFingerprint := strings.TrimSpace(machineFingerprint) != ""
	machineName = normalizedMachineName(machineName, runtimeName, endpoint)
	machineFingerprint = normalizedMachineFingerprint(machineFingerprint, machineName, runtimeName, endpoint)
	fingerprintHash := model.HashSecret(machineFingerprint)

	machine, found, err := s.pgFindMachineByFingerprintTx(ctx, tx, token.TenantID, fingerprintHash, true)
	if err != nil {
		return model.Runtime{}, "", model.Machine{}, err
	}
	if !found && explicitFingerprint {
		machine, found, err = s.pgFindMachineCandidateTx(ctx, tx, token.TenantID, "", model.MachineConnectionModeAgent, machineName, runtimeName, endpoint)
		if err != nil {
			return model.Runtime{}, "", model.Machine{}, err
		}
	}

	runtimeSecret := model.NewSecret("fugue_rt")

	if found {
		machine.Name = machineName
		machine.ConnectionMode = model.MachineConnectionModeAgent
		machine.Status = model.RuntimeStatusActive
		machine.Endpoint = endpoint
		machine.Labels = cloneMap(labels)
		machine.FingerprintPrefix = model.SecretPrefix(machineFingerprint)
		machine.FingerprintHash = fingerprintHash
		machine.LastSeenAt = &now
		machine.UpdatedAt = now

		runtime, err := s.pgGetRuntimeTx(ctx, tx, machine.RuntimeID, true)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return model.Runtime{}, "", model.Machine{}, err
		}
		if errors.Is(err, ErrNotFound) {
			name, err := s.pgNextAvailableRuntimeNameTx(ctx, tx, token.TenantID, runtimeName)
			if err != nil {
				return model.Runtime{}, "", model.Machine{}, err
			}
			runtime = model.Runtime{
				ID:              model.NewID("runtime"),
				TenantID:        token.TenantID,
				Name:            name,
				Type:            model.RuntimeTypeExternalOwned,
				Status:          model.RuntimeStatusActive,
				Endpoint:        endpoint,
				Labels:          cloneMap(labels),
				AgentKeyPrefix:  model.SecretPrefix(runtimeSecret),
				AgentKeyHash:    model.HashSecret(runtimeSecret),
				LastHeartbeatAt: &now,
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			labelsJSON, err := marshalNullableJSON(runtime.Labels)
			if err != nil {
				return model.Runtime{}, "", model.Machine{}, err
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, $9, $10, $11, $12)
`, runtime.ID, nullIfEmpty(runtime.TenantID), runtime.Name, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, runtime.AgentKeyPrefix, runtime.AgentKeyHash, runtime.LastHeartbeatAt, runtime.CreatedAt, runtime.UpdatedAt); err != nil {
				return model.Runtime{}, "", model.Machine{}, mapDBErr(err)
			}
		} else {
			runtime.Type = model.RuntimeTypeExternalOwned
			runtime.Status = model.RuntimeStatusActive
			runtime.Endpoint = endpoint
			runtime.Labels = cloneMap(labels)
			runtime.AgentKeyPrefix = model.SecretPrefix(runtimeSecret)
			runtime.AgentKeyHash = model.HashSecret(runtimeSecret)
			runtime.LastHeartbeatAt = &now
			runtime.UpdatedAt = now
			labelsJSON, err := marshalNullableJSON(runtime.Labels)
			if err != nil {
				return model.Runtime{}, "", model.Machine{}, err
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE fugue_runtimes
SET type = $2,
	status = $3,
	endpoint = $4,
	labels_json = $5,
	agent_key_prefix = $6,
	agent_key_hash = $7,
	last_heartbeat_at = $8,
	updated_at = $9
WHERE id = $1
`, runtime.ID, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, runtime.AgentKeyPrefix, runtime.AgentKeyHash, runtime.LastHeartbeatAt, runtime.UpdatedAt); err != nil {
				return model.Runtime{}, "", model.Machine{}, fmt.Errorf("update enrolled runtime %s: %w", runtime.ID, err)
			}
		}

		machine.RuntimeID = runtime.ID
		machine.RuntimeName = runtime.Name
		machine.ClusterNodeName = ""
		if err := s.pgUpdateMachineTx(ctx, tx, machine); err != nil {
			return model.Runtime{}, "", model.Machine{}, err
		}
		if err := tx.Commit(); err != nil {
			return model.Runtime{}, "", model.Machine{}, fmt.Errorf("commit consume enrollment token transaction: %w", err)
		}
		return runtime, runtimeSecret, redactMachine(machine), nil
	}

	name, err := s.pgNextAvailableRuntimeNameTx(ctx, tx, token.TenantID, runtimeName)
	if err != nil {
		return model.Runtime{}, "", model.Machine{}, err
	}
	runtime := model.Runtime{
		ID:              model.NewID("runtime"),
		TenantID:        token.TenantID,
		Name:            name,
		Type:            model.RuntimeTypeExternalOwned,
		Status:          model.RuntimeStatusActive,
		Endpoint:        endpoint,
		Labels:          cloneMap(labels),
		AgentKeyPrefix:  model.SecretPrefix(runtimeSecret),
		AgentKeyHash:    model.HashSecret(runtimeSecret),
		LastHeartbeatAt: &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	labelsJSON, err := marshalNullableJSON(runtime.Labels)
	if err != nil {
		return model.Runtime{}, "", model.Machine{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, $9, $10, $11, $12)
`, runtime.ID, nullIfEmpty(runtime.TenantID), runtime.Name, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, runtime.AgentKeyPrefix, runtime.AgentKeyHash, runtime.LastHeartbeatAt, runtime.CreatedAt, runtime.UpdatedAt); err != nil {
		return model.Runtime{}, "", model.Machine{}, mapDBErr(err)
	}

	machine = model.Machine{
		ID:                model.NewID("machine"),
		TenantID:          token.TenantID,
		Name:              machineName,
		ConnectionMode:    model.MachineConnectionModeAgent,
		Status:            model.RuntimeStatusActive,
		Endpoint:          endpoint,
		Labels:            cloneMap(labels),
		RuntimeID:         runtime.ID,
		RuntimeName:       runtime.Name,
		FingerprintPrefix: model.SecretPrefix(machineFingerprint),
		FingerprintHash:   fingerprintHash,
		LastSeenAt:        &now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := s.pgInsertMachineTx(ctx, tx, machine); err != nil {
		return model.Runtime{}, "", model.Machine{}, err
	}

	if err := tx.Commit(); err != nil {
		return model.Runtime{}, "", model.Machine{}, fmt.Errorf("commit consume enrollment token transaction: %w", err)
	}
	return runtime, runtimeSecret, redactMachine(machine), nil
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
SET last_heartbeat_at = NOW(), status = $2, updated_at = NOW()
WHERE agent_key_hash = $1
RETURNING id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at
`, model.HashSecret(secret), model.RuntimeStatusActive))
	if err != nil {
		return model.Runtime{}, model.Principal{}, mapDBErr(err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_machines
SET status = $2,
	last_seen_at = NOW(),
	updated_at = NOW(),
	endpoint = CASE WHEN $3 <> '' THEN $3 ELSE endpoint END
WHERE runtime_id = $1
`, runtime.ID, model.RuntimeStatusActive, runtime.Endpoint); err != nil {
		return model.Runtime{}, model.Principal{}, fmt.Errorf("update machine heartbeat for runtime %s: %w", runtime.ID, err)
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
	status = $2,
	updated_at = NOW(),
	endpoint = CASE WHEN $3 <> '' THEN $3 ELSE endpoint END
WHERE id = $1
RETURNING id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at
`, runtimeID, model.RuntimeStatusActive, endpoint))
	if err != nil {
		return model.Runtime{}, mapDBErr(err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_machines
SET status = $2,
	last_seen_at = NOW(),
	updated_at = NOW(),
	endpoint = CASE WHEN $3 <> '' THEN $3 ELSE endpoint END
WHERE runtime_id = $1
`, runtime.ID, model.RuntimeStatusActive, runtime.Endpoint); err != nil {
		return model.Runtime{}, fmt.Errorf("update machine heartbeat for runtime %s: %w", runtime.ID, err)
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
		if _, err := tx.ExecContext(ctx, `
UPDATE fugue_machines
SET status = $2, updated_at = NOW()
WHERE runtime_id = $1
`, runtimeID, model.RuntimeStatusOffline); err != nil {
			return 0, fmt.Errorf("mark machine for runtime %s offline stale: %w", runtimeID, err)
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

func (s *Store) pgListMachines(tenantID string, platformAdmin bool) ([]model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, name, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, last_seen_at, created_at, updated_at
FROM fugue_machines
`
	args := make([]any, 0, 1)
	if !platformAdmin {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list machines: %w", err)
	}
	defer rows.Close()

	machines := make([]model.Machine, 0)
	for rows.Next() {
		machine, err := scanMachine(rows)
		if err != nil {
			return nil, err
		}
		machines = append(machines, redactMachine(machine))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate machines: %w", err)
	}
	return machines, nil
}

func (s *Store) pgListMachinesByNodeKey(nodeKeyID, tenantID string, platformAdmin bool) ([]model.Machine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, name, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, last_seen_at, created_at, updated_at
FROM fugue_machines
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
		return nil, fmt.Errorf("list machines by node key: %w", err)
	}
	defer rows.Close()

	machines := make([]model.Machine, 0)
	for rows.Next() {
		machine, err := scanMachine(rows)
		if err != nil {
			return nil, err
		}
		machines = append(machines, redactMachine(machine))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate machines by node key: %w", err)
	}
	return machines, nil
}

func (s *Store) pgListRuntimes(tenantID string, platformAdmin bool) ([]model.Runtime, error) {
	return s.pgListRuntimesByFilter(tenantID, platformAdmin, false)
}

func (s *Store) pgListRuntimesByFilter(tenantID string, platformAdmin bool, nodesOnly bool) ([]model.Runtime, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
`
	args := make([]any, 0, 3)
	clauses := make([]string, 0, 2)
	if nodesOnly {
		clauses = append(clauses, fmt.Sprintf("type IN ($%d, $%d)", len(args)+1, len(args)+2))
		args = append(args, model.RuntimeTypeExternalOwned, model.RuntimeTypeManagedOwned)
	}
	if !platformAdmin {
		clauses = append(clauses, fmt.Sprintf("(tenant_id = $%d OR type = $%d)", len(args)+1, len(args)+2))
		args = append(args, tenantID, model.RuntimeTypeManagedShared)
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
SELECT id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE id = $1
`, id))
	if err != nil {
		return model.Runtime{}, mapDBErr(err)
	}
	return runtime, nil
}

func (s *Store) pgFindManagedOwnedRuntimeTx(ctx context.Context, tx *sql.Tx, nodeKeyID, runtimeName string) (model.Runtime, bool, error) {
	runtime, err := scanRuntime(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at
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
SELECT id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at
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

func (s *Store) pgFindMachineByFingerprintTx(ctx context.Context, tx *sql.Tx, tenantID, fingerprintHash string, forUpdate bool) (model.Machine, bool, error) {
	if strings.TrimSpace(fingerprintHash) == "" {
		return model.Machine{}, false, nil
	}
	query := `
SELECT id, tenant_id, name, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, last_seen_at, created_at, updated_at
FROM fugue_machines
WHERE tenant_id = $1
  AND fingerprint_hash = $2
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	machine, err := scanMachine(tx.QueryRowContext(ctx, query, tenantID, fingerprintHash))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Machine{}, false, nil
		}
		return model.Machine{}, false, err
	}
	return machine, true, nil
}

func (s *Store) pgFindMachineCandidateTx(ctx context.Context, tx *sql.Tx, tenantID, nodeKeyID, connectionMode, machineName, runtimeName, endpoint string) (model.Machine, bool, error) {
	query := `
SELECT id, tenant_id, name, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, last_seen_at, created_at, updated_at
FROM fugue_machines
WHERE tenant_id = $1
  AND connection_mode = $2
`
	args := []any{tenantID, connectionMode}
	if strings.TrimSpace(nodeKeyID) != "" {
		query += ` AND node_key_id = $3`
		args = append(args, nodeKeyID)
	}
	query += ` ORDER BY updated_at DESC, created_at DESC FOR UPDATE`

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return model.Machine{}, false, fmt.Errorf("query machine candidates: %w", err)
	}
	defer rows.Close()

	bestScore := 0
	var best model.Machine
	for rows.Next() {
		machine, err := scanMachine(rows)
		if err != nil {
			return model.Machine{}, false, err
		}
		score := 0
		if endpoint != "" && strings.EqualFold(strings.TrimSpace(machine.Endpoint), strings.TrimSpace(endpoint)) {
			score++
		}
		if runtimeName != "" && strings.EqualFold(machine.RuntimeName, strings.TrimSpace(runtimeName)) {
			score += 4
		}
		if runtimeName != "" && strings.EqualFold(machine.ClusterNodeName, strings.TrimSpace(runtimeName)) {
			score += 4
		}
		if machineName != "" && strings.EqualFold(machine.Name, strings.TrimSpace(machineName)) {
			score += 2
		}
		if score > bestScore {
			bestScore = score
			best = machine
		}
	}
	if err := rows.Err(); err != nil {
		return model.Machine{}, false, fmt.Errorf("iterate machine candidates: %w", err)
	}
	if bestScore == 0 {
		return model.Machine{}, false, nil
	}
	return best, true, nil
}

func (s *Store) pgInsertMachineTx(ctx context.Context, tx *sql.Tx, machine model.Machine) error {
	labelsJSON, err := marshalNullableJSON(machine.Labels)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_machines (id, tenant_id, name, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, last_seen_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
`, machine.ID, nullIfEmpty(machine.TenantID), machine.Name, machine.ConnectionMode, machine.Status, machine.Endpoint, labelsJSON, nullIfEmpty(machine.NodeKeyID), machine.RuntimeID, machine.RuntimeName, machine.ClusterNodeName, machine.FingerprintPrefix, machine.FingerprintHash, machine.LastSeenAt, machine.CreatedAt, machine.UpdatedAt); err != nil {
		return mapDBErr(err)
	}
	return nil
}

func (s *Store) pgUpdateMachineTx(ctx context.Context, tx *sql.Tx, machine model.Machine) error {
	labelsJSON, err := marshalNullableJSON(machine.Labels)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_machines
SET name = $2,
	connection_mode = $3,
	status = $4,
	endpoint = $5,
	labels_json = $6,
	node_key_id = $7,
	runtime_id = $8,
	runtime_name = $9,
	cluster_node_name = $10,
	fingerprint_prefix = $11,
	fingerprint_hash = $12,
	last_seen_at = $13,
	updated_at = $14
WHERE id = $1
`, machine.ID, machine.Name, machine.ConnectionMode, machine.Status, machine.Endpoint, labelsJSON, nullIfEmpty(machine.NodeKeyID), machine.RuntimeID, machine.RuntimeName, machine.ClusterNodeName, machine.FingerprintPrefix, machine.FingerprintHash, machine.LastSeenAt, machine.UpdatedAt); err != nil {
		return fmt.Errorf("update machine %s: %w", machine.ID, err)
	}
	return nil
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
		return model.App{}, mapDBErr(err)
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
	visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, spec.RuntimeID, tenantID)
	if err != nil {
		return model.App{}, err
	}
	if !visible {
		return model.App{}, ErrNotFound
	}

	now := time.Now().UTC()
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
			Phase:           "created",
			CurrentReplicas: 0,
			UpdatedAt:       now,
		},
		CreatedAt: now,
		UpdatedAt: now,
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
	if err := tx.Commit(); err != nil {
		return model.App{}, fmt.Errorf("commit create app transaction: %w", err)
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
	case model.OperationTypeDeploy:
		if op.DesiredSpec == nil {
			return model.Operation{}, ErrInvalidInput
		}
		visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, op.DesiredSpec.RuntimeID, op.TenantID)
		if err != nil {
			return model.Operation{}, err
		}
		if !visible {
			return model.Operation{}, ErrNotFound
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
		op.SourceRuntimeID = app.Spec.RuntimeID
	default:
		return model.Operation{}, ErrInvalidInput
	}

	now := time.Now().UTC()
	op.DesiredSpec = cloneAppSpec(op.DesiredSpec)
	op.DesiredSource = cloneAppSource(op.DesiredSource)
	op.ID = model.NewID("op")
	op.Status = model.OperationStatusPending
	op.ExecutionMode = model.ExecutionModeManaged
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
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, '', '', '', '', $14, $15, NULL, NULL)
`, op.ID, op.TenantID, op.Type, op.Status, op.ExecutionMode, op.RequestedByType, op.RequestedByID, op.AppID, op.SourceRuntimeID, op.TargetRuntimeID, intPointerValue(op.DesiredReplicas), desiredSpecJSON, desiredSourceJSON, op.CreatedAt, op.UpdatedAt); err != nil {
		return model.Operation{}, mapDBErr(err)
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

func (s *Store) pgClaimNextPendingOperation() (model.Operation, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, false, fmt.Errorf("begin claim operation transaction: %w", err)
	}
	defer tx.Rollback()

	op, err := scanOperation(tx.QueryRowContext(ctx, `
WITH next_op AS (
	SELECT id
	FROM fugue_operations
	WHERE status = $1
	ORDER BY created_at ASC
	FOR UPDATE SKIP LOCKED
	LIMIT 1
)
SELECT o.id, o.tenant_id, o.type, o.status, o.execution_mode, o.requested_by_type, o.requested_by_id, o.app_id, o.source_runtime_id, o.target_runtime_id, o.desired_replicas, o.desired_spec_json, o.desired_source_json, o.result_message, o.manifest_path, o.assigned_runtime_id, o.error_message, o.created_at, o.updated_at, o.started_at, o.completed_at
FROM fugue_operations o
JOIN next_op n ON n.id = o.id
`, model.OperationStatusPending))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Operation{}, false, nil
		}
		return model.Operation{}, false, fmt.Errorf("claim next pending operation: %w", err)
	}

	now := time.Now().UTC()
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
	op.UpdatedAt = now

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

	if err := tx.Commit(); err != nil {
		return model.Operation{}, false, fmt.Errorf("commit claim operation transaction: %w", err)
	}
	return op, true, nil
}

func (s *Store) pgDispatchOperationToRuntime(id, runtimeID string) (model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	op, err := scanOperation(s.db.QueryRowContext(ctx, `
UPDATE fugue_operations
SET status = $2,
	execution_mode = $3,
	assigned_runtime_id = $4,
	result_message = $5,
	updated_at = $6
WHERE id = $1
RETURNING id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
`, id, model.OperationStatusWaitingAgent, model.ExecutionModeAgent, runtimeID, "task dispatched to external runtime agent", now))
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	return op, nil
}

func (s *Store) pgCompleteOperation(id, runtimeID, manifestPath, message string) (model.Operation, error) {
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
	if err := applyOperationToAppModel(&app, &op); err != nil {
		return model.Operation{}, err
	}

	if err := s.pgUpdateOperationTx(ctx, tx, op); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.Operation{}, err
	}

	if err := tx.Commit(); err != nil {
		return model.Operation{}, fmt.Errorf("commit complete operation transaction: %w", err)
	}
	return op, nil
}

func (s *Store) pgFailOperation(id, message string) (model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	op, err := scanOperation(s.db.QueryRowContext(ctx, `
UPDATE fugue_operations
SET status = $2,
	updated_at = $3,
	completed_at = $4,
	error_message = $5
WHERE id = $1
RETURNING id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
`, id, model.OperationStatusFailed, now, now, strings.TrimSpace(message)))
	if err != nil {
		return model.Operation{}, mapDBErr(err)
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

func (s *Store) pgRuntimeVisibleToTenantTx(ctx context.Context, tx *sql.Tx, runtimeID, tenantID string) (bool, error) {
	runtimeType, err := s.pgRuntimeTypeTx(ctx, tx, runtimeID)
	if err != nil {
		return false, err
	}
	if runtimeType == "" {
		return false, nil
	}
	if runtimeType == model.RuntimeTypeManagedShared {
		return true, nil
	}

	var owner sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM fugue_runtimes WHERE id = $1`, runtimeID).Scan(&owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("query runtime %s owner: %w", runtimeID, err)
	}
	return owner.String == tenantID, nil
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
	var scopesRaw []byte
	var lastUsedAt sql.NullTime
	if err := scanner.Scan(&key.ID, &tenantID, &key.Label, &key.Prefix, &key.Hash, &scopesRaw, &key.CreatedAt, &lastUsedAt); err != nil {
		return model.APIKey{}, err
	}
	key.TenantID = tenantID.String
	scopes, err := decodeJSONValue[[]string](scopesRaw)
	if err != nil {
		return model.APIKey{}, err
	}
	key.Scopes = scopes
	if lastUsedAt.Valid {
		key.LastUsedAt = &lastUsedAt.Time
	}
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
	var endpoint sql.NullString
	var labelsRaw []byte
	var nodeKeyID sql.NullString
	var agentKeyPrefix sql.NullString
	var agentKeyHash sql.NullString
	var lastHeartbeatAt sql.NullTime
	if err := scanner.Scan(&runtime.ID, &tenantID, &runtime.Name, &runtime.Type, &runtime.Status, &endpoint, &labelsRaw, &nodeKeyID, &agentKeyPrefix, &agentKeyHash, &lastHeartbeatAt, &runtime.CreatedAt, &runtime.UpdatedAt); err != nil {
		return model.Runtime{}, err
	}
	runtime.TenantID = tenantID.String
	runtime.Endpoint = endpoint.String
	runtime.NodeKeyID = nodeKeyID.String
	runtime.AgentKeyPrefix = agentKeyPrefix.String
	runtime.AgentKeyHash = agentKeyHash.String
	labels, err := decodeJSONValue[map[string]string](labelsRaw)
	if err != nil {
		return model.Runtime{}, err
	}
	runtime.Labels = labels
	if lastHeartbeatAt.Valid {
		runtime.LastHeartbeatAt = &lastHeartbeatAt.Time
	}
	return runtime, nil
}

func scanMachine(scanner sqlScanner) (model.Machine, error) {
	var machine model.Machine
	var tenantID sql.NullString
	var endpoint sql.NullString
	var labelsRaw []byte
	var nodeKeyID sql.NullString
	var runtimeID sql.NullString
	var runtimeName sql.NullString
	var clusterNodeName sql.NullString
	var fingerprintPrefix sql.NullString
	var fingerprintHash sql.NullString
	var lastSeenAt sql.NullTime
	if err := scanner.Scan(&machine.ID, &tenantID, &machine.Name, &machine.ConnectionMode, &machine.Status, &endpoint, &labelsRaw, &nodeKeyID, &runtimeID, &runtimeName, &clusterNodeName, &fingerprintPrefix, &fingerprintHash, &lastSeenAt, &machine.CreatedAt, &machine.UpdatedAt); err != nil {
		return model.Machine{}, err
	}
	machine.TenantID = tenantID.String
	machine.Endpoint = endpoint.String
	machine.NodeKeyID = nodeKeyID.String
	machine.RuntimeID = runtimeID.String
	machine.RuntimeName = runtimeName.String
	machine.ClusterNodeName = clusterNodeName.String
	machine.FingerprintPrefix = fingerprintPrefix.String
	machine.FingerprintHash = fingerprintHash.String
	labels, err := decodeJSONValue[map[string]string](labelsRaw)
	if err != nil {
		return model.Machine{}, err
	}
	machine.Labels = labels
	if lastSeenAt.Valid {
		machine.LastSeenAt = &lastSeenAt.Time
	}
	return machine, nil
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
	case model.OperationTypeDeploy:
		if op.DesiredSpec == nil {
			return ErrInvalidInput
		}
		app.Spec = *op.DesiredSpec
		if op.DesiredSource != nil {
			app.Source = cloneAppSource(op.DesiredSource)
		}
		app.Status.Phase = "deployed"
		app.Status.CurrentRuntimeID = app.Spec.RuntimeID
		app.Status.CurrentReplicas = app.Spec.Replicas
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
	case model.OperationTypeDelete:
		app.Name = deletedAppName(app.Name, op.ID)
		app.Route = nil
		app.Spec.Replicas = 0
		app.Status.Phase = "deleted"
		app.Status.CurrentRuntimeID = ""
		app.Status.CurrentReplicas = 0
	case model.OperationTypeMigrate:
		if op.TargetRuntimeID == "" {
			return ErrInvalidInput
		}
		app.Spec.RuntimeID = op.TargetRuntimeID
		app.Status.Phase = "migrated"
		app.Status.CurrentRuntimeID = op.TargetRuntimeID
		app.Status.CurrentReplicas = app.Spec.Replicas
	default:
		return ErrInvalidInput
	}
	app.Status.LastOperationID = op.ID
	app.Status.LastMessage = op.ResultMessage
	app.Status.UpdatedAt = now
	app.UpdatedAt = now
	return nil
}

func sortOperationsByCreatedAt(ops []model.Operation) {
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].CreatedAt.Before(ops[j].CreatedAt)
	})
}
