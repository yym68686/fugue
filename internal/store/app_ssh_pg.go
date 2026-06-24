package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

const sshKeySelectColumns = `
SELECT id, tenant_id, label, public_key, fingerprint, status, comment, created_at, updated_at, last_used_at
FROM fugue_ssh_keys
`

const appSSHEndpointSelectColumns = `
SELECT id, tenant_id, project_id, app_id, runtime_id, runtime_type, edge_group_id, hostname, public_port,
	target_namespace, target_service, target_host, target_port, ssh_user, status, status_reason,
	host_key_fingerprint, created_at, updated_at, released_at
FROM fugue_app_ssh_endpoints
`

func (s *Store) pgListSSHKeys(tenantID string, platformAdmin bool) ([]model.SSHKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := sshKeySelectColumns
	args := make([]any, 0, 1)
	if !platformAdmin {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list ssh keys: %w", err)
	}
	defer rows.Close()

	keys := make([]model.SSHKey, 0)
	for rows.Next() {
		key, err := scanSSHKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ssh keys: %w", err)
	}
	return keys, nil
}

func (s *Store) pgGetSSHKey(id string) (model.SSHKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key, err := scanSSHKey(s.db.QueryRowContext(ctx, sshKeySelectColumns+` WHERE id = $1`, id))
	if err != nil {
		return model.SSHKey{}, mapDBErr(err)
	}
	return key, nil
}

func (s *Store) pgCreateSSHKey(tenantID, label, publicKey, fingerprint string) (model.SSHKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	key, err := scanSSHKey(s.db.QueryRowContext(ctx, `
INSERT INTO fugue_ssh_keys (id, tenant_id, label, public_key, fingerprint, status, comment, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id, tenant_id, label, public_key, fingerprint, status, comment, created_at, updated_at, last_used_at
`, model.NewID("sshkey"), tenantID, label, publicKey, fingerprint, model.SSHKeyStatusActive, sshPublicKeyComment(publicKey), now, now))
	if err != nil {
		return model.SSHKey{}, mapDBErr(err)
	}
	return key, nil
}

func (s *Store) pgDeleteSSHKey(id string) (model.SSHKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.SSHKey{}, fmt.Errorf("begin delete ssh key transaction: %w", err)
	}
	defer tx.Rollback()

	key, err := scanSSHKey(tx.QueryRowContext(ctx, sshKeySelectColumns+` WHERE id = $1 FOR UPDATE`, id))
	if err != nil {
		return model.SSHKey{}, mapDBErr(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_ssh_keys WHERE id = $1`, id); err != nil {
		return model.SSHKey{}, fmt.Errorf("delete ssh key %s: %w", id, err)
	}

	rows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
FROM fugue_apps
WHERE tenant_id = $1
FOR UPDATE
`, key.TenantID)
	if err != nil {
		return model.SSHKey{}, fmt.Errorf("list apps for ssh key cleanup: %w", err)
	}
	apps := make([]model.App, 0)
	for rows.Next() {
		app, err := scanApp(rows)
		if err != nil {
			rows.Close()
			return model.SSHKey{}, err
		}
		apps = append(apps, app)
	}
	if err := rows.Close(); err != nil {
		return model.SSHKey{}, fmt.Errorf("close apps for ssh key cleanup: %w", err)
	}
	if err := rows.Err(); err != nil {
		return model.SSHKey{}, fmt.Errorf("iterate apps for ssh key cleanup: %w", err)
	}
	for _, app := range apps {
		if app.Spec.SSH == nil {
			continue
		}
		before := len(app.Spec.SSH.AuthorizedKeyIDs)
		app.Spec.SSH.AuthorizedKeyIDs = removeStringValue(app.Spec.SSH.AuthorizedKeyIDs, id)
		app.Spec.SSH = model.NormalizeAppSSHSpec(app.Spec.SSH)
		if before != len(app.Spec.SSH.AuthorizedKeyIDs) {
			app.UpdatedAt = time.Now().UTC()
			if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
				return model.SSHKey{}, err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return model.SSHKey{}, fmt.Errorf("commit delete ssh key transaction: %w", err)
	}
	return key, nil
}

func (s *Store) pgGetAppSSHEndpoint(appID string) (model.AppSSHEndpoint, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	endpoint, err := scanAppSSHEndpoint(s.db.QueryRowContext(ctx, appSSHEndpointSelectColumns+` WHERE app_id = $1`, appID))
	if err != nil {
		return model.AppSSHEndpoint{}, mapDBErr(err)
	}
	return normalizeAppSSHEndpointForRead(endpoint), nil
}

func (s *Store) pgUpsertAppSSHConfig(appID string, update AppSSHUpdate) (model.App, model.AppSSHEndpoint, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.App{}, model.AppSSHEndpoint{}, fmt.Errorf("begin upsert app ssh transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, appID, true)
	if err != nil {
		return model.App{}, model.AppSSHEndpoint{}, mapDBErr(err)
	}
	if isDeletedApp(app) {
		return model.App{}, model.AppSSHEndpoint{}, ErrNotFound
	}
	if err := s.pgValidateSSHKeysTx(ctx, tx, app.TenantID, update.AuthorizedKeyIDs); err != nil {
		return model.App{}, model.AppSSHEndpoint{}, err
	}
	authorizedKeys, err := s.pgResolveAppSSHAuthorizedKeysTx(ctx, tx, app.TenantID, update.AuthorizedKeyIDs, update.AuthorizedKeys)
	if err != nil {
		return model.App{}, model.AppSSHEndpoint{}, err
	}

	runtimeObj, runtimeFound, err := s.pgRuntimeByIDTx(ctx, tx, app.Spec.RuntimeID)
	if err != nil {
		return model.App{}, model.AppSSHEndpoint{}, err
	}
	status := model.AppSSHEndpointStatusPending
	statusReason := ""
	runtimeType := ""
	edgeGroupID := ""
	if runtimeFound {
		runtimeType = runtimeObj.Type
		edgeGroupID = edgeGroupIDForRuntime(runtimeObj)
		if runtimeObj.Type == model.RuntimeTypeExternalOwned {
			status = model.AppSSHEndpointStatusUnsupported
			statusReason = "external-owned runtimes do not support native ssh routes yet"
		}
	} else {
		status = model.AppSSHEndpointStatusUnavailable
		statusReason = "runtime is missing"
	}

	ssh := &model.AppSSHSpec{
		Enabled:            true,
		TargetPort:         update.TargetPort,
		User:               update.User,
		AuthorizedKeyIDs:   append([]string(nil), update.AuthorizedKeyIDs...),
		AuthorizedKeys:     authorizedKeys,
		AllowTCPForwarding: update.AllowTCPForwarding,
	}
	app.Spec.SSH = model.NormalizeAppSSHSpec(ssh)
	app.UpdatedAt = time.Now().UTC()
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.App{}, model.AppSSHEndpoint{}, err
	}

	endpoint, exists, err := s.pgGetAppSSHEndpointForUpdateTx(ctx, tx, appID)
	if err != nil {
		return model.App{}, model.AppSSHEndpoint{}, err
	}
	if !exists {
		port, err := s.pgAllocateAppSSHPublicPortTx(ctx, tx, update.PublicPortStart, update.PublicPortEnd, 0)
		if err != nil {
			return model.App{}, model.AppSSHEndpoint{}, err
		}
		now := time.Now().UTC()
		endpoint = model.AppSSHEndpoint{
			ID:         model.NewID("sshend"),
			TenantID:   app.TenantID,
			ProjectID:  app.ProjectID,
			AppID:      app.ID,
			PublicPort: port,
			CreatedAt:  now,
		}
	}
	endpoint.TenantID = app.TenantID
	endpoint.ProjectID = app.ProjectID
	endpoint.RuntimeID = app.Spec.RuntimeID
	endpoint.RuntimeType = runtimeType
	endpoint.EdgeGroupID = edgeGroupID
	endpoint.Hostname = strings.TrimSpace(update.Hostname)
	endpoint.TargetNamespace = runtimepkg.NamespaceForTenant(app.TenantID)
	endpoint.TargetService = runtimepkg.RuntimeAppServiceName(app)
	endpoint.TargetHost = endpoint.TargetService + "." + endpoint.TargetNamespace + ".svc.cluster.local"
	endpoint.TargetPort = app.Spec.SSH.TargetPort
	endpoint.User = app.Spec.SSH.User
	endpoint.Status = status
	endpoint.StatusReason = statusReason
	endpoint.ReleasedAt = nil
	endpoint.UpdatedAt = time.Now().UTC()
	if err := s.pgUpsertAppSSHEndpointTx(ctx, tx, endpoint); err != nil {
		return model.App{}, model.AppSSHEndpoint{}, err
	}

	if err := tx.Commit(); err != nil {
		return model.App{}, model.AppSSHEndpoint{}, fmt.Errorf("commit upsert app ssh transaction: %w", err)
	}
	normalizeAppStatusForRead(&app)
	if err := s.pgHydrateAppBackingServices(ctx, &app); err != nil {
		return model.App{}, model.AppSSHEndpoint{}, err
	}
	return app, normalizeAppSSHEndpointForRead(endpoint), nil
}

func (s *Store) pgDisableAppSSH(appID string) (model.App, model.AppSSHEndpoint, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.App{}, model.AppSSHEndpoint{}, fmt.Errorf("begin disable app ssh transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, appID, true)
	if err != nil {
		return model.App{}, model.AppSSHEndpoint{}, mapDBErr(err)
	}
	if isDeletedApp(app) {
		return model.App{}, model.AppSSHEndpoint{}, ErrNotFound
	}
	app.Spec.SSH = nil
	app.UpdatedAt = time.Now().UTC()
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.App{}, model.AppSSHEndpoint{}, err
	}

	endpoint, exists, err := s.pgGetAppSSHEndpointForUpdateTx(ctx, tx, appID)
	if err != nil {
		return model.App{}, model.AppSSHEndpoint{}, err
	}
	if exists {
		now := time.Now().UTC()
		endpoint.Status = model.AppSSHEndpointStatusDisabled
		endpoint.StatusReason = "ssh disabled"
		endpoint.ReleasedAt = &now
		endpoint.UpdatedAt = now
		if err := s.pgUpsertAppSSHEndpointTx(ctx, tx, endpoint); err != nil {
			return model.App{}, model.AppSSHEndpoint{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return model.App{}, model.AppSSHEndpoint{}, fmt.Errorf("commit disable app ssh transaction: %w", err)
	}
	normalizeAppStatusForRead(&app)
	if err := s.pgHydrateAppBackingServices(ctx, &app); err != nil {
		return model.App{}, model.AppSSHEndpoint{}, err
	}
	return app, normalizeAppSSHEndpointForRead(endpoint), nil
}

func (s *Store) pgRotateAppSSHPort(appID string, start, end int) (model.AppSSHEndpoint, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppSSHEndpoint{}, fmt.Errorf("begin rotate app ssh port transaction: %w", err)
	}
	defer tx.Rollback()

	endpoint, exists, err := s.pgGetAppSSHEndpointForUpdateTx(ctx, tx, appID)
	if err != nil {
		return model.AppSSHEndpoint{}, err
	}
	if !exists {
		return model.AppSSHEndpoint{}, ErrNotFound
	}
	port, err := s.pgAllocateAppSSHPublicPortTx(ctx, tx, start, end, endpoint.PublicPort)
	if err != nil {
		return model.AppSSHEndpoint{}, err
	}
	endpoint.PublicPort = port
	endpoint.UpdatedAt = time.Now().UTC()
	if err := s.pgUpsertAppSSHEndpointTx(ctx, tx, endpoint); err != nil {
		return model.AppSSHEndpoint{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.AppSSHEndpoint{}, fmt.Errorf("commit rotate app ssh port transaction: %w", err)
	}
	return normalizeAppSSHEndpointForRead(endpoint), nil
}

func (s *Store) pgListEdgeSSHRoutes(options AppSSHRouteOptions) ([]model.EdgeSSHRoute, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, appSSHEndpointSelectColumns+`
WHERE status IN ('pending', 'ready')
ORDER BY public_port ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list edge ssh routes: %w", err)
	}
	defer rows.Close()

	routes := make([]model.EdgeSSHRoute, 0)
	for rows.Next() {
		endpoint, err := scanAppSSHEndpoint(rows)
		if err != nil {
			return nil, err
		}
		route, ok := edgeSSHRouteFromEndpoint(endpoint, options)
		if ok {
			routes = append(routes, route)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate edge ssh routes: %w", err)
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].PublicPort < routes[j].PublicPort
	})
	return routes, nil
}

func (s *Store) pgValidateSSHKeysTx(ctx context.Context, tx *sql.Tx, tenantID string, keyIDs []string) error {
	for _, id := range keyIDs {
		var exists bool
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_ssh_keys
	WHERE id = $1
	  AND tenant_id = $2
	  AND status = $3
)
`, id, tenantID, model.SSHKeyStatusActive).Scan(&exists); err != nil {
			return fmt.Errorf("validate ssh key %s: %w", id, err)
		}
		if !exists {
			return ErrNotFound
		}
	}
	return nil
}

func (s *Store) pgResolveAppSSHAuthorizedKeysTx(ctx context.Context, tx *sql.Tx, tenantID string, keyIDs, inlineKeys []string) ([]string, error) {
	keys := make([]string, 0, len(keyIDs)+len(inlineKeys))
	for _, id := range keyIDs {
		var publicKey string
		if err := tx.QueryRowContext(ctx, `
SELECT public_key
FROM fugue_ssh_keys
WHERE id = $1
  AND tenant_id = $2
  AND status = $3
`, id, tenantID, model.SSHKeyStatusActive).Scan(&publicKey); err != nil {
			return nil, mapDBErr(err)
		}
		keys = append(keys, publicKey)
	}
	keys = append(keys, inlineKeys...)
	return model.NormalizeSSHPublicKeys(keys), nil
}

func (s *Store) pgRuntimeByIDTx(ctx context.Context, tx *sql.Tx, runtimeID string) (model.Runtime, bool, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return model.Runtime{}, false, nil
	}
	runtimeObj, err := s.pgGetRuntimeTx(ctx, tx, runtimeID, false)
	if err != nil {
		if err == sql.ErrNoRows || err == ErrNotFound {
			return model.Runtime{}, false, nil
		}
		return model.Runtime{}, false, mapDBErr(err)
	}
	return runtimeObj, true, nil
}

func (s *Store) pgGetAppSSHEndpointForUpdateTx(ctx context.Context, tx *sql.Tx, appID string) (model.AppSSHEndpoint, bool, error) {
	endpoint, err := scanAppSSHEndpoint(tx.QueryRowContext(ctx, appSSHEndpointSelectColumns+` WHERE app_id = $1 FOR UPDATE`, appID))
	if err != nil {
		if err == sql.ErrNoRows {
			return model.AppSSHEndpoint{}, false, nil
		}
		return model.AppSSHEndpoint{}, false, mapDBErr(err)
	}
	return normalizeAppSSHEndpointForRead(endpoint), true, nil
}

func (s *Store) pgAllocateAppSSHPublicPortTx(ctx context.Context, tx *sql.Tx, start, end, exclude int) (int, error) {
	start, end = normalizeAppSSHPortRange(start, end)
	rows, err := tx.QueryContext(ctx, `
SELECT public_port, status, released_at
FROM fugue_app_ssh_endpoints
WHERE public_port BETWEEN $1 AND $2
FOR UPDATE
`, start, end)
	if err != nil {
		return 0, fmt.Errorf("list allocated app ssh ports: %w", err)
	}
	defer rows.Close()

	used := map[int]struct{}{}
	now := time.Now().UTC()
	for rows.Next() {
		var (
			port       int
			status     string
			releasedAt sql.NullTime
		)
		if err := rows.Scan(&port, &status, &releasedAt); err != nil {
			return 0, err
		}
		if port == exclude {
			continue
		}
		if model.NormalizeAppSSHEndpointStatus(status) == model.AppSSHEndpointStatusReleased ||
			model.NormalizeAppSSHEndpointStatus(status) == model.AppSSHEndpointStatusDisabled {
			if releasedAt.Valid && now.Sub(releasedAt.Time) > time.Hour {
				continue
			}
		}
		used[port] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate allocated app ssh ports: %w", err)
	}
	for port := start; port <= end; port++ {
		if _, ok := used[port]; !ok {
			return port, nil
		}
	}
	return 0, ErrConflict
}

func (s *Store) pgUpsertAppSSHEndpointTx(ctx context.Context, tx *sql.Tx, endpoint model.AppSSHEndpoint) error {
	endpoint = normalizeAppSSHEndpointForRead(endpoint)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_app_ssh_endpoints (
	id, tenant_id, project_id, app_id, runtime_id, runtime_type, edge_group_id, hostname, public_port,
	target_namespace, target_service, target_host, target_port, ssh_user, status, status_reason,
	host_key_fingerprint, created_at, updated_at, released_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
ON CONFLICT (app_id) DO UPDATE SET
	tenant_id = EXCLUDED.tenant_id,
	project_id = EXCLUDED.project_id,
	runtime_id = EXCLUDED.runtime_id,
	runtime_type = EXCLUDED.runtime_type,
	edge_group_id = EXCLUDED.edge_group_id,
	hostname = EXCLUDED.hostname,
	public_port = EXCLUDED.public_port,
	target_namespace = EXCLUDED.target_namespace,
	target_service = EXCLUDED.target_service,
	target_host = EXCLUDED.target_host,
	target_port = EXCLUDED.target_port,
	ssh_user = EXCLUDED.ssh_user,
	status = EXCLUDED.status,
	status_reason = EXCLUDED.status_reason,
	host_key_fingerprint = EXCLUDED.host_key_fingerprint,
	updated_at = EXCLUDED.updated_at,
	released_at = EXCLUDED.released_at
`, endpoint.ID, endpoint.TenantID, endpoint.ProjectID, endpoint.AppID, endpoint.RuntimeID, endpoint.RuntimeType,
		endpoint.EdgeGroupID, endpoint.Hostname, endpoint.PublicPort, endpoint.TargetNamespace, endpoint.TargetService,
		endpoint.TargetHost, endpoint.TargetPort, endpoint.User, endpoint.Status, endpoint.StatusReason,
		endpoint.HostKeyFingerprint, endpoint.CreatedAt, endpoint.UpdatedAt, endpoint.ReleasedAt); err != nil {
		return mapDBErr(fmt.Errorf("upsert app ssh endpoint %s: %w", endpoint.AppID, err))
	}
	return nil
}

func scanSSHKey(scanner sqlScanner) (model.SSHKey, error) {
	var key model.SSHKey
	var lastUsedAt sql.NullTime
	if err := scanner.Scan(
		&key.ID,
		&key.TenantID,
		&key.Label,
		&key.PublicKey,
		&key.Fingerprint,
		&key.Status,
		&key.Comment,
		&key.CreatedAt,
		&key.UpdatedAt,
		&lastUsedAt,
	); err != nil {
		return model.SSHKey{}, err
	}
	if key.Status == "" {
		key.Status = model.SSHKeyStatusActive
	}
	if lastUsedAt.Valid {
		key.LastUsedAt = &lastUsedAt.Time
	}
	return key, nil
}

func scanAppSSHEndpoint(scanner sqlScanner) (model.AppSSHEndpoint, error) {
	var endpoint model.AppSSHEndpoint
	var releasedAt sql.NullTime
	if err := scanner.Scan(
		&endpoint.ID,
		&endpoint.TenantID,
		&endpoint.ProjectID,
		&endpoint.AppID,
		&endpoint.RuntimeID,
		&endpoint.RuntimeType,
		&endpoint.EdgeGroupID,
		&endpoint.Hostname,
		&endpoint.PublicPort,
		&endpoint.TargetNamespace,
		&endpoint.TargetService,
		&endpoint.TargetHost,
		&endpoint.TargetPort,
		&endpoint.User,
		&endpoint.Status,
		&endpoint.StatusReason,
		&endpoint.HostKeyFingerprint,
		&endpoint.CreatedAt,
		&endpoint.UpdatedAt,
		&releasedAt,
	); err != nil {
		return model.AppSSHEndpoint{}, err
	}
	if releasedAt.Valid {
		endpoint.ReleasedAt = &releasedAt.Time
	}
	return normalizeAppSSHEndpointForRead(endpoint), nil
}
