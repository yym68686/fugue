package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
)

var ErrManagedSSHPublicationUnsupported = errors.New("managed SSH public activation requires PostgreSQL durable route projection")

func (stateStore *Store) ActivateManagedSSH(tenantID, appID string, revision, intentGeneration uint64, digest, fingerprint, bindingID, legacyRouteDigest, actor string) (managedssh.ControlState, error) {
	return stateStore.publishManagedSSHSlot(tenantID, appID, revision, intentGeneration, digest, fingerprint, bindingID, legacyRouteDigest, actor, "")
}

func (stateStore *Store) RollbackManagedSSH(tenantID, appID string, revision, intentGeneration uint64, digest, fingerprint, bindingID, legacyRouteDigest, actor, slotID string) (managedssh.ControlState, error) {
	if slotID == "" {
		return managedssh.ControlState{}, ErrInvalidInput
	}
	return stateStore.publishManagedSSHSlot(tenantID, appID, revision, intentGeneration, digest, fingerprint, bindingID, legacyRouteDigest, actor, slotID)
}

func (stateStore *Store) publishManagedSSHSlot(tenantID, appID string, revision, intentGeneration uint64, digest, fingerprint, bindingID, legacyRouteDigest, actor, rollbackSlot string) (managedssh.ControlState, error) {
	if !stateStore.usingDatabase() {
		return managedssh.ControlState{}, ErrManagedSSHPublicationUnsupported
	}
	intent, err := stateStore.GetManagedSSHIntent(tenantID, appID)
	if err != nil {
		return managedssh.ControlState{}, err
	}
	if !intent.Spec.Enabled || intent.Generation != intentGeneration {
		return managedssh.ControlState{}, ErrConflict
	}
	binding, err := stateStore.GetManagedSSHBinding(tenantID, appID)
	if err != nil {
		return managedssh.ControlState{}, err
	}
	if binding.ID != bindingID || binding.IdentityID != intent.Spec.IdentityID || binding.LegacyRouteDigest != legacyRouteDigest {
		return managedssh.ControlState{}, ErrConflict
	}
	state, err := stateStore.GetManagedSSHControl(tenantID, appID)
	if err != nil {
		return state, err
	}
	if state.Revision != revision || state.IntentGeneration != intentGeneration {
		return state, ErrConflict
	}
	slot := state.Candidate
	if rollbackSlot != "" {
		slot = nil
		for index := range state.Draining {
			if state.Draining[index].ID == rollbackSlot {
				slot = &state.Draining[index]
			}
		}
	}
	if slot == nil {
		return state, ErrConflict
	}
	keys, err := stateStore.ListSSHKeys(tenantID, false)
	if err != nil {
		return state, err
	}
	configuration := slot.Artifact.Configuration
	desired, err := managedssh.CompileConfiguration(intent, configuration.Identity, configuration.Target, keys, configuration.Generation, configuration.ValidUntil.Add(-managedssh.GrantLifetime))
	if err != nil || !reflect.DeepEqual(configuration, desired) {
		return state, ErrConflict
	}
	var activated managedssh.ControlState
	if rollbackSlot == "" {
		activated, err = state.ActivateCandidate(digest, fingerprint, legacyRouteDigest, actor, time.Now().UTC())
	} else {
		activated, err = state.RollbackSlot(rollbackSlot, digest, fingerprint, actor, time.Now().UTC())
	}
	if err != nil {
		return state, err
	}
	if err := stateStore.SaveManagedSSHControl(revision, activated); err != nil {
		return state, err
	}
	return activated, nil
}

func managedSSHEndpoint(binding model.ManagedSSHPublicBinding, app model.App, state managedssh.ControlState) (model.AppSSHEndpoint, error) {
	if state.Active == nil || binding.TenantID != state.TenantID || binding.AppID != state.AppID || binding.IdentityID != state.IdentityID || binding.LegacyRouteDigest != state.LegacyRouteDigest || app.ID != binding.AppID || app.TenantID != binding.TenantID || !state.Active.Verified(time.Now().UTC()) {
		return model.AppSSHEndpoint{}, ErrConflict
	}
	slot := state.Active
	return model.AppSSHEndpoint{ID: binding.ID, TenantID: binding.TenantID, ProjectID: app.ProjectID, AppID: binding.AppID, RuntimeID: binding.RuntimeID, RuntimeType: binding.RuntimeType, EdgeGroupID: binding.EdgeGroupID, Hostname: binding.Hostname, PublicPort: binding.PublicPort, TargetNamespace: slot.Namespace, TargetService: slot.ID, TargetHost: slot.ID + "." + slot.Namespace + ".svc.cluster.local", TargetPort: 2222, User: slot.Artifact.Configuration.Target.User, Status: model.AppSSHEndpointStatusPending, StatusReason: "managed SSH candidate activated; public endpoint observation pending", HostKeyFingerprint: slot.Artifact.Configuration.Identity.Fingerprint, CreatedAt: binding.CreatedAt, UpdatedAt: time.Now().UTC()}, nil
}

func (stateStore *Store) pgPublishManagedSSHRoute(ctx context.Context, transaction *sql.Tx, app model.App, state managedssh.ControlState) error {
	if _, err := transaction.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue.ssh-public-ports/v1',0))`); err != nil {
		return err
	}
	var raw []byte
	err := transaction.QueryRowContext(ctx, `SELECT body FROM fugue_managed_ssh_bindings WHERE app_id=$1 AND tenant_id=$2`, app.ID, app.TenantID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	binding, err := decodeManagedSSHBinding(raw)
	if err != nil {
		return err
	}
	endpoint, err := managedSSHEndpoint(binding, app, state)
	if err != nil {
		return err
	}
	if err := validateManagedSSHRouteAuthorization(ctx, transaction, state); err != nil {
		return err
	}
	if state.RouteGeneration == 1 {
		legacy, err := scanAppSSHEndpoint(transaction.QueryRowContext(ctx, appSSHEndpointSelectColumns+` WHERE app_id=$1 FOR UPDATE`, app.ID))
		if err == nil && model.AppSSHRouteDigest(legacy) != binding.LegacyRouteDigest {
			return ErrConflict
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	raw, err = json.Marshal(endpoint)
	if err != nil {
		return err
	}
	result, err := transaction.ExecContext(ctx, `INSERT INTO fugue_managed_ssh_routes(app_id,tenant_id,generation,artifact_digest,body) VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT(app_id) DO UPDATE SET generation=EXCLUDED.generation,artifact_digest=EXCLUDED.artifact_digest,body=EXCLUDED.body WHERE fugue_managed_ssh_routes.tenant_id=EXCLUDED.tenant_id AND fugue_managed_ssh_routes.generation<EXCLUDED.generation`, app.ID, app.TenantID, state.RouteGeneration, state.Active.Artifact.Digest, string(raw))
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrConflict
	}
	if _, err := transaction.ExecContext(ctx, `INSERT INTO fugue_managed_ssh_route_history(app_id,generation,artifact_digest,actor,activated_at,body) VALUES($1,$2,$3,$4,$5,$6::jsonb)`, app.ID, state.RouteGeneration, state.Active.Artifact.Digest, state.ActivationActor, state.ActivatedAt, string(raw)); err != nil {
		return err
	}
	return stateStore.pgUpsertAppSSHEndpointTx(ctx, transaction, endpoint)
}

func validateManagedSSHRouteAuthorization(ctx context.Context, transaction *sql.Tx, state managedssh.ControlState) error {
	var raw []byte
	if err := transaction.QueryRowContext(ctx, `SELECT body FROM fugue_managed_ssh_intents WHERE app_id=$1 AND tenant_id=$2 FOR SHARE`, state.AppID, state.TenantID).Scan(&raw); err != nil {
		return err
	}
	intent, err := decodeManagedSSHIntent(raw)
	if err != nil {
		return err
	}
	if intent.Generation != state.IntentGeneration || !intent.Spec.Enabled {
		return ErrConflict
	}
	rows, err := transaction.QueryContext(ctx, sshKeySelectColumns+` WHERE tenant_id=$1 ORDER BY id FOR SHARE`, state.TenantID)
	if err != nil {
		return err
	}
	keys := []model.SSHKey{}
	for rows.Next() {
		key, err := scanSSHKey(rows)
		if err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	configuration := state.Active.Artifact.Configuration
	desired, err := managedssh.CompileConfiguration(intent, configuration.Identity, configuration.Target, keys, configuration.Generation, configuration.ValidUntil.Add(-managedssh.GrantLifetime))
	if err != nil || !reflect.DeepEqual(configuration, desired) {
		return ErrConflict
	}
	return nil
}
