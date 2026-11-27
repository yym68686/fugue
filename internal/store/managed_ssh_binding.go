package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
)

func (stateStore *Store) GetManagedSSHBinding(tenantID, appID string) (model.ManagedSSHPublicBinding, error) {
	app, err := stateStore.GetApp(appID)
	if err != nil {
		return model.ManagedSSHPublicBinding{}, err
	}
	if app.TenantID != tenantID || isDeletedApp(app) {
		return model.ManagedSSHPublicBinding{}, ErrNotFound
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var raw []byte
	if err := stateStore.db.QueryRowContext(ctx, `SELECT body FROM fugue_managed_ssh_bindings WHERE app_id=$1 AND tenant_id=$2`, appID, tenantID).Scan(&raw); err != nil {
		var databaseError *pgconn.PgError
		if errors.As(err, &databaseError) && databaseError.Code == "42P01" {
			return model.ManagedSSHPublicBinding{}, ErrNotFound
		}
		return model.ManagedSSHPublicBinding{}, mapDBErr(err)
	}
	binding, err := decodeManagedSSHBinding(raw)
	if err == nil && (binding.AppID != appID || binding.TenantID != tenantID) {
		err = ErrConflict
	}
	return binding, err
}

func (stateStore *Store) PrepareManagedSSHBinding(tenantID, appID, identityID, expectedLegacyDigest, hostname string, start, end int) (model.ManagedSSHPublicBinding, error) {
	identity, err := stateStore.GetAppSSHIdentity(tenantID, appID)
	if err != nil {
		return model.ManagedSSHPublicBinding{}, err
	}
	if identity.ID != identityID {
		return model.ManagedSSHPublicBinding{}, ErrConflict
	}
	if !validManagedSSHHostname(hostname) || start < 1 || end > 65535 || end < start {
		return model.ManagedSSHPublicBinding{}, ErrInvalidInput
	}
	return stateStore.pgPrepareManagedSSHBinding(tenantID, appID, identityID, expectedLegacyDigest, hostname, start, end)
}

func newManagedSSHBinding(app model.App, identityID, expectedLegacyDigest, hostname string, start, end int, legacy model.AppSSHEndpoint, used map[int]bool) (model.ManagedSSHPublicBinding, error) {
	binding := model.ManagedSSHPublicBinding{ID: model.NewID("sshbinding"), TenantID: app.TenantID, AppID: app.ID, IdentityID: identityID, Hostname: hostname, RuntimeID: app.Spec.RuntimeID, CreatedAt: time.Now().UTC()}
	if legacy.ID != "" {
		if legacy.AppID != app.ID || legacy.TenantID != app.TenantID || expectedLegacyDigest != model.AppSSHRouteDigest(legacy) || used[legacy.PublicPort] {
			return binding, ErrConflict
		}
		binding.ID, binding.Hostname, binding.PublicPort = legacy.ID, legacy.Hostname, legacy.PublicPort
		binding.RuntimeID, binding.RuntimeType, binding.EdgeGroupID = legacy.RuntimeID, legacy.RuntimeType, legacy.EdgeGroupID
		binding.LegacyRouteDigest = expectedLegacyDigest
	} else {
		if expectedLegacyDigest != "" {
			return binding, ErrConflict
		}
		for port := start; port <= end; port++ {
			if !used[port] {
				binding.PublicPort = port
				break
			}
		}
		if binding.PublicPort == 0 {
			return binding, ErrConflict
		}
		binding.LegacyRouteDigest = model.AppSSHRouteDigest(model.AppSSHEndpoint{ID: binding.ID, TenantID: app.TenantID, AppID: app.ID, Hostname: hostname, PublicPort: binding.PublicPort, RuntimeID: binding.RuntimeID, Status: model.AppSSHEndpointStatusDisabled})
	}
	if !validManagedSSHBinding(binding) {
		return model.ManagedSSHPublicBinding{}, ErrInvalidInput
	}
	return binding, nil
}

func (stateStore *Store) pgPrepareManagedSSHBinding(tenantID, appID, identityID, expectedLegacyDigest, hostname string, start, end int) (model.ManagedSSHPublicBinding, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transaction, err := stateStore.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ManagedSSHPublicBinding{}, err
	}
	defer transaction.Rollback()
	app, err := stateStore.pgGetAppTx(ctx, transaction, appID, false)
	if err != nil {
		return model.ManagedSSHPublicBinding{}, mapDBErr(err)
	}
	if app.TenantID != tenantID || isDeletedApp(app) {
		return model.ManagedSSHPublicBinding{}, ErrNotFound
	}
	if _, err := transaction.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue.ssh-public-ports/v1',0))`); err != nil {
		return model.ManagedSSHPublicBinding{}, err
	}
	var raw []byte
	err = transaction.QueryRowContext(ctx, `SELECT body FROM fugue_managed_ssh_bindings WHERE app_id=$1`, appID).Scan(&raw)
	if err == nil {
		binding, err := decodeManagedSSHBinding(raw)
		if err != nil {
			return binding, err
		}
		if binding.TenantID != tenantID || binding.IdentityID != identityID {
			return binding, ErrConflict
		}
		return binding, transaction.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.ManagedSSHPublicBinding{}, err
	}
	legacy, err := scanAppSSHEndpoint(transaction.QueryRowContext(ctx, appSSHEndpointSelectColumns+` WHERE app_id=$1`, appID))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.ManagedSSHPublicBinding{}, err
	}
	used := map[int]bool{}
	rows, err := transaction.QueryContext(ctx, `SELECT public_port FROM fugue_app_ssh_endpoints WHERE app_id<>$1 UNION SELECT public_port FROM fugue_managed_ssh_bindings`, appID)
	if err != nil {
		return model.ManagedSSHPublicBinding{}, err
	}
	for rows.Next() {
		var port int
		if err := rows.Scan(&port); err != nil {
			rows.Close()
			return model.ManagedSSHPublicBinding{}, err
		}
		used[port] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return model.ManagedSSHPublicBinding{}, err
	}
	binding, err := newManagedSSHBinding(app, identityID, expectedLegacyDigest, hostname, start, end, legacy, used)
	if err != nil {
		return binding, err
	}
	raw, err = json.Marshal(binding)
	if err != nil {
		return binding, err
	}
	if _, err := transaction.ExecContext(ctx, `INSERT INTO fugue_managed_ssh_bindings(app_id,tenant_id,public_port,body) VALUES($1,$2,$3,$4::jsonb)`, appID, tenantID, binding.PublicPort, string(raw)); err != nil {
		return binding, mapDBErr(err)
	}
	return binding, transaction.Commit()
}

func validManagedSSHHostname(hostname string) bool {
	if net.ParseIP(hostname) != nil {
		return true
	}
	return hostname != "" && len(hostname) <= 253 && !strings.ContainsAny(hostname, "\x00\r\n\t /\\?#@:")
}

func validManagedSSHBinding(binding model.ManagedSSHPublicBinding) bool {
	return binding.ID != "" && binding.TenantID != "" && binding.AppID != "" && binding.IdentityID != "" && binding.PublicPort > 0 && binding.PublicPort <= 65535 && validManagedSSHHostname(binding.Hostname) && strings.HasPrefix(binding.LegacyRouteDigest, "sha256:") && !binding.CreatedAt.IsZero()
}

func decodeManagedSSHBinding(raw []byte) (model.ManagedSSHPublicBinding, error) {
	var binding model.ManagedSSHPublicBinding
	if err := json.Unmarshal(raw, &binding); err != nil {
		return binding, err
	}
	if !validManagedSSHBinding(binding) {
		return model.ManagedSSHPublicBinding{}, ErrConflict
	}
	return binding, nil
}
