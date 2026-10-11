package store

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"sort"
	"syscall"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
)

func (stateStore *Store) GetManagedSSHIntent(tenantID, appID string) (model.ManagedSSHIntent, error) {
	app, err := stateStore.GetApp(appID)
	if err != nil {
		return model.ManagedSSHIntent{}, err
	}
	if app.TenantID != tenantID || isDeletedApp(app) {
		return model.ManagedSSHIntent{}, ErrNotFound
	}
	if stateStore.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var raw []byte
		if err := stateStore.db.QueryRowContext(ctx, `SELECT body FROM fugue_managed_ssh_intents WHERE tenant_id=$1 AND app_id=$2`, tenantID, appID).Scan(&raw); err != nil {
			return model.ManagedSSHIntent{}, mapDBErr(err)
		}
		intent, err := decodeManagedSSHIntent(raw)
		if err == nil && (intent.AppID != appID || intent.TenantID != tenantID) {
			err = ErrConflict
		}
		return intent, err
	}
	var result model.ManagedSSHIntent
	err = stateStore.withManagedSSHIntentFile(false, func(intents map[string]model.ManagedSSHIntent) error {
		intent, exists := intents[appID]
		if !exists || intent.TenantID != tenantID {
			return ErrNotFound
		}
		result = intent
		return nil
	})
	return result, err
}

func (stateStore *Store) PutManagedSSHIntent(tenantID, appID string, expected uint64, spec model.ManagedSSHIntentSpec) (model.ManagedSSHIntent, error) {
	if expected >= math.MaxInt64 || managedssh.ValidateIntent(spec) != nil {
		return model.ManagedSSHIntent{}, ErrInvalidInput
	}
	identity, err := stateStore.GetAppSSHIdentity(tenantID, appID)
	if err != nil {
		return model.ManagedSSHIntent{}, err
	}
	if spec.IdentityID != identity.ID {
		return model.ManagedSSHIntent{}, ErrConflict
	}
	keys, err := stateStore.ListSSHKeys(tenantID, false)
	if err != nil {
		return model.ManagedSSHIntent{}, err
	}
	available := map[string]bool{}
	for _, key := range keys {
		available[key.ID] = key.TenantID == tenantID && key.Status == model.SSHKeyStatusActive
	}
	if spec.Enabled {
		for _, identifier := range spec.AuthorizedKeyIDs {
			if !available[identifier] {
				return model.ManagedSSHIntent{}, ErrInvalidInput
			}
		}
	}
	if spec.AuthorizedKeyIDs == nil {
		spec.AuthorizedKeyIDs = []string{}
	}
	now := time.Now().UTC()
	intent := model.ManagedSSHIntent{TenantID: tenantID, AppID: appID, Generation: expected + 1, Spec: spec, CreatedAt: now, UpdatedAt: now}
	if stateStore.usingDatabase() {
		return stateStore.pgPutManagedSSHIntent(intent, expected)
	}
	err = stateStore.withManagedSSHIntentFile(true, func(intents map[string]model.ManagedSSHIntent) error {
		current, exists := intents[appID]
		if exists {
			if current.Generation != expected || current.TenantID != tenantID || current.Spec.IdentityID != spec.IdentityID {
				return ErrConflict
			}
			intent.CreatedAt = current.CreatedAt
		} else if expected != 0 {
			return ErrConflict
		}
		intents[appID] = intent
		return nil
	})
	return intent, err
}

func (stateStore *Store) ListManagedSSHIntents() ([]model.ManagedSSHIntent, error) {
	intents := []model.ManagedSSHIntent{}
	if stateStore.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rows, err := stateStore.db.QueryContext(ctx, `SELECT body FROM fugue_managed_ssh_intents ORDER BY app_id`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				return nil, err
			}
			intent, err := decodeManagedSSHIntent(raw)
			if err != nil {
				return nil, err
			}
			intents = append(intents, intent)
		}
		return intents, rows.Err()
	}
	err := stateStore.withManagedSSHIntentFile(false, func(records map[string]model.ManagedSSHIntent) error {
		for _, intent := range records {
			intents = append(intents, intent)
		}
		return nil
	})
	sort.Slice(intents, func(first, second int) bool { return intents[first].AppID < intents[second].AppID })
	return intents, err
}

func (stateStore *Store) pgPutManagedSSHIntent(intent model.ManagedSSHIntent, expected uint64) (model.ManagedSSHIntent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw, err := json.Marshal(intent)
	if err != nil {
		return model.ManagedSSHIntent{}, err
	}
	var result []byte
	if expected == 0 {
		err = stateStore.db.QueryRowContext(ctx, `INSERT INTO fugue_managed_ssh_intents(app_id,tenant_id,generation,body) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT DO NOTHING RETURNING body`, intent.AppID, intent.TenantID, intent.Generation, string(raw)).Scan(&result)
	} else {
		err = stateStore.db.QueryRowContext(ctx, `UPDATE fugue_managed_ssh_intents SET generation=$3, body=jsonb_set($4::jsonb,'{created_at}',body->'created_at') WHERE app_id=$1 AND tenant_id=$2 AND generation=$5 AND body->'spec'->>'identity_id'=$6 RETURNING body`, intent.AppID, intent.TenantID, intent.Generation, string(raw), expected, intent.Spec.IdentityID).Scan(&result)
	}
	if errors.Is(mapDBErr(err), ErrNotFound) {
		return model.ManagedSSHIntent{}, ErrConflict
	}
	if err != nil {
		return model.ManagedSSHIntent{}, mapDBErr(err)
	}
	return decodeManagedSSHIntent(result)
}

func decodeManagedSSHIntent(raw []byte) (model.ManagedSSHIntent, error) {
	var intent model.ManagedSSHIntent
	if err := json.Unmarshal(raw, &intent); err != nil {
		return intent, err
	}
	if intent.TenantID == "" || intent.AppID == "" || intent.Generation == 0 || intent.Generation > math.MaxInt64 || intent.CreatedAt.IsZero() || intent.UpdatedAt.Before(intent.CreatedAt) || managedssh.ValidateIntent(intent.Spec) != nil {
		return model.ManagedSSHIntent{}, ErrConflict
	}
	return intent, nil
}

func (stateStore *Store) withManagedSSHIntentFile(write bool, callback func(map[string]model.ManagedSSHIntent) error) error {
	path := stateStore.path + ".managed-ssh-intents.json"
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	intents := map[string]model.ManagedSSHIntent{}
	raw, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		var encoded map[string]json.RawMessage
		if err := json.Unmarshal(raw, &encoded); err != nil {
			return err
		}
		if encoded == nil {
			return ErrConflict
		}
		for appID, body := range encoded {
			intent, err := decodeManagedSSHIntent(body)
			if err != nil {
				return err
			}
			if intent.AppID != appID {
				return ErrConflict
			}
			intents[appID] = intent
		}
	}
	if err := callback(intents); err != nil {
		return err
	}
	if !write {
		return nil
	}
	raw, err = json.Marshal(intents)
	if err != nil {
		return err
	}
	return writeSSHIdentityFile(path, raw)
}
