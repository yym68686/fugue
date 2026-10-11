package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/sshidentity"
)

func (stateStore *Store) ConfigureSSHIdentityKeyring(keyring sshidentity.Keyring) {
	stateStore.sshIdentityKeyringMu.Lock()
	defer stateStore.sshIdentityKeyringMu.Unlock()
	stateStore.sshIdentityKeyring = keyring
	stateStore.sshIdentityKeyFile = ""
}

func (stateStore *Store) ConfigureSSHIdentityKeyFile(path string) {
	stateStore.sshIdentityKeyringMu.Lock()
	defer stateStore.sshIdentityKeyringMu.Unlock()
	stateStore.sshIdentityKeyFile = strings.TrimSpace(path)
}

func (stateStore *Store) identityKeyring() (sshidentity.Keyring, error) {
	stateStore.sshIdentityKeyringMu.RLock()
	defer stateStore.sshIdentityKeyringMu.RUnlock()
	if stateStore.sshIdentityKeyFile != "" {
		return sshidentity.LoadKeyring(stateStore.sshIdentityKeyFile)
	}
	if !stateStore.sshIdentityKeyring.Configured() {
		return sshidentity.Keyring{}, sshidentity.ErrKeyUnavailable
	}
	return stateStore.sshIdentityKeyring, nil
}

func (stateStore *Store) PrepareAppSSHIdentity(tenantID, appID string) (model.AppSSHIdentity, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appID) == "" {
		return model.AppSSHIdentity{}, ErrInvalidInput
	}
	if stateStore.usingDatabase() {
		return stateStore.pgPrepareAppSSHIdentity(tenantID, appID)
	}
	var identity model.AppSSHIdentity
	app, err := stateStore.GetApp(appID)
	if err != nil {
		return identity, err
	}
	if app.TenantID != tenantID || isDeletedApp(app) {
		return identity, ErrNotFound
	}
	keyring, err := stateStore.identityKeyring()
	if err != nil {
		return identity, err
	}
	err = stateStore.withSSHIdentityFile(true, func(records *[]model.AppSSHIdentityRecord) error {
		for _, record := range *records {
			if record.Identity.AppID == appID {
				if err := validateStoredSSHIdentity(record, tenantID, appID); err != nil {
					return err
				}
				if _, err := keyring.Signer(record); err != nil {
					return err
				}
				identity = record.Identity
				return nil
			}
		}
		record, err := keyring.Create(tenantID, appID, time.Now().UTC())
		if err != nil {
			return err
		}
		*records = append(*records, record)
		identity = record.Identity
		return nil
	})
	return identity, err
}

func (stateStore *Store) GetAppSSHIdentity(tenantID, appID string) (model.AppSSHIdentity, error) {
	record, err := stateStore.appSSHIdentityRecord(tenantID, appID)
	return record.Identity, err
}

func (stateStore *Store) AppSSHIdentityPrivateKey(tenantID, appID, identityID string) ([]byte, error) {
	if strings.TrimSpace(identityID) == "" {
		return nil, ErrInvalidInput
	}
	record, err := stateStore.appSSHIdentityRecord(tenantID, appID)
	if err != nil {
		return nil, err
	}
	if record.Identity.ID != identityID {
		return nil, ErrConflict
	}
	keyring, err := stateStore.identityKeyring()
	if err != nil {
		return nil, err
	}
	return keyring.PrivateKeyPEM(record)
}

func (stateStore *Store) appSSHIdentityRecord(tenantID, appID string) (model.AppSSHIdentityRecord, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appID) == "" {
		return model.AppSSHIdentityRecord{}, ErrInvalidInput
	}
	app, err := stateStore.GetApp(appID)
	if err != nil {
		return model.AppSSHIdentityRecord{}, err
	}
	if app.TenantID != tenantID || isDeletedApp(app) {
		return model.AppSSHIdentityRecord{}, ErrNotFound
	}
	if stateStore.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var raw []byte
		var publicRaw []byte
		err := stateStore.db.QueryRowContext(ctx, `SELECT anchor.public_body, identity.body
FROM fugue_app_ssh_identity_anchors anchor LEFT JOIN fugue_app_ssh_identities identity ON identity.app_id=anchor.app_id
WHERE anchor.app_id = $1 AND anchor.tenant_id = $2`, appID, tenantID).Scan(&publicRaw, &raw)
		if err != nil {
			return model.AppSSHIdentityRecord{}, mapDBErr(err)
		}
		return decodeAnchoredSSHIdentity(raw, publicRaw, tenantID, appID)
	}
	var result model.AppSSHIdentityRecord
	err = stateStore.withSSHIdentityFile(false, func(records *[]model.AppSSHIdentityRecord) error {
		for _, record := range *records {
			if record.Identity.AppID == appID {
				if err := validateStoredSSHIdentity(record, tenantID, appID); err != nil {
					return err
				}
				result = record
				return nil
			}
		}
		return ErrNotFound
	})
	return result, err
}

func (stateStore *Store) pgPrepareAppSSHIdentity(tenantID, appID string) (model.AppSSHIdentity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	transaction, err := stateStore.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AppSSHIdentity{}, err
	}
	defer transaction.Rollback()
	app, err := stateStore.pgGetAppTx(ctx, transaction, appID, true)
	if err != nil {
		return model.AppSSHIdentity{}, mapDBErr(err)
	}
	if app.TenantID != tenantID || isDeletedApp(app) {
		return model.AppSSHIdentity{}, ErrNotFound
	}
	keyring, err := stateStore.identityKeyring()
	if err != nil {
		return model.AppSSHIdentity{}, err
	}
	var raw, publicRaw []byte
	err = transaction.QueryRowContext(ctx, `SELECT anchor.public_body, identity.body
FROM fugue_app_ssh_identity_anchors anchor LEFT JOIN fugue_app_ssh_identities identity ON identity.app_id=anchor.app_id
WHERE anchor.app_id = $1`, appID).Scan(&publicRaw, &raw)
	if err == nil {
		record, err := decodeAnchoredSSHIdentity(raw, publicRaw, tenantID, appID)
		if err != nil {
			return model.AppSSHIdentity{}, err
		}
		if _, err := keyring.Signer(record); err != nil {
			return model.AppSSHIdentity{}, err
		}
		if err := transaction.Commit(); err != nil {
			return model.AppSSHIdentity{}, err
		}
		return record.Identity, nil
	}
	if !errors.Is(mapDBErr(err), ErrNotFound) {
		return model.AppSSHIdentity{}, err
	}
	record, err := keyring.Create(tenantID, appID, time.Now().UTC())
	if err != nil {
		return model.AppSSHIdentity{}, err
	}
	raw, err = json.Marshal(record)
	if err != nil {
		return model.AppSSHIdentity{}, err
	}
	publicRaw, err = json.Marshal(record.Identity)
	if err != nil {
		return model.AppSSHIdentity{}, err
	}
	if _, err = transaction.ExecContext(ctx, `INSERT INTO fugue_app_ssh_identity_anchors (app_id, tenant_id, identity_id, public_body)
VALUES ($1, $2, $3, $4::jsonb)`, appID, tenantID, record.Identity.ID, string(publicRaw)); err != nil {
		return model.AppSSHIdentity{}, fmt.Errorf("reserve SSH identity: %w", mapDBErr(err))
	}
	if _, err = transaction.ExecContext(ctx, `INSERT INTO fugue_app_ssh_identities (app_id, tenant_id, identity_id, body)
VALUES ($1, $2, $3, $4::jsonb)`, appID, tenantID, record.Identity.ID, string(raw)); err != nil {
		return model.AppSSHIdentity{}, fmt.Errorf("persist SSH identity: %w", mapDBErr(err))
	}
	if err := transaction.Commit(); err != nil {
		return model.AppSSHIdentity{}, err
	}
	return record.Identity, nil
}

func validateStoredSSHIdentity(record model.AppSSHIdentityRecord, tenantID, appID string) error {
	if record.Identity.TenantID != tenantID || record.Identity.AppID != appID || record.Ciphertext == "" || record.EncryptionKeyID == "" {
		return sshidentity.ErrIntegrity
	}
	if err := sshidentity.ValidatePublic(record.Identity); err != nil {
		return sshidentity.ErrIntegrity
	}
	return nil
}

func decodeStoredSSHIdentity(raw []byte, tenantID, appID string) (model.AppSSHIdentityRecord, error) {
	var record model.AppSSHIdentityRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return model.AppSSHIdentityRecord{}, sshidentity.ErrIntegrity
	}
	if err := validateStoredSSHIdentity(record, tenantID, appID); err != nil {
		return model.AppSSHIdentityRecord{}, err
	}
	return record, nil
}

func decodeAnchoredSSHIdentity(raw, publicRaw []byte, tenantID, appID string) (model.AppSSHIdentityRecord, error) {
	record, err := decodeStoredSSHIdentity(raw, tenantID, appID)
	if err != nil {
		return model.AppSSHIdentityRecord{}, err
	}
	var anchor model.AppSSHIdentity
	if json.Unmarshal(publicRaw, &anchor) != nil || !reflect.DeepEqual(anchor, record.Identity) {
		return model.AppSSHIdentityRecord{}, sshidentity.ErrIntegrity
	}
	return record, nil
}
