package sshmaintenance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/sshidentity"
)

type RetainedIdentity struct {
	Anchor model.AppSSHIdentity       `json:"anchor"`
	Record model.AppSSHIdentityRecord `json:"record"`
}

type Backup struct {
	Schema     string             `json:"schema"`
	CapturedAt time.Time          `json:"captured_at"`
	Digest     string             `json:"digest"`
	Identities []RetainedIdentity `json:"identities"`
}

type Entry struct {
	Identity     model.AppSSHIdentity `json:"identity"`
	RecordDigest string               `json:"record_digest"`
	FromKeyID    string               `json:"from_key_id"`
	ToKeyID      string               `json:"to_key_id"`
	Rewrap       bool                 `json:"rewrap"`
}

type Plan struct {
	Schema        string  `json:"schema"`
	BackupDigest  string  `json:"backup_digest"`
	KeyringDigest string  `json:"keyring_digest"`
	Retention     string  `json:"retention"`
	Entries       []Entry `json:"entries"`
	Digest        string  `json:"digest"`
}

var ErrPrecondition = errors.New("SSH identity maintenance precondition changed")
var ErrIntegrity = errors.New("SSH identity maintenance inventory failed integrity validation")
var digestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var actorPattern = regexp.MustCompile(`^[A-Za-z0-9_.:/@-]{1,256}$`)

func Digest(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ValidateBackup(backup Backup, keyring sshidentity.Keyring) error {
	if backup.Schema != "fugue.ssh-identity-backup/v1" || backup.CapturedAt.IsZero() || backup.CapturedAt.After(time.Now().Add(5*time.Second)) || backup.Identities == nil || len(backup.Identities) > 10000 || backup.Digest != Digest(backup.Identities) {
		return ErrIntegrity
	}
	previous := ""
	identifiers := map[string]bool{}
	for _, retained := range backup.Identities {
		anchor := retained.Anchor
		if sshidentity.ValidatePublic(anchor) != nil || !reflect.DeepEqual(anchor, retained.Record.Identity) || anchor.AppID <= previous || identifiers[anchor.ID] {
			return ErrIntegrity
		}
		if _, err := keyring.Signer(retained.Record); err != nil {
			return ErrIntegrity
		}
		previous, identifiers[anchor.ID] = anchor.AppID, true
	}
	return nil
}

func ReadInventory(ctx context.Context, transaction *sql.Tx, keyring sshidentity.Keyring) (Backup, error) {
	rows, err := transaction.QueryContext(ctx, `SELECT anchor.public_body, identity.body
FROM fugue_app_ssh_identity_anchors anchor FULL JOIN fugue_app_ssh_identities identity USING (app_id)
ORDER BY COALESCE(anchor.app_id, identity.app_id) LIMIT 10001`)
	if err != nil {
		return Backup{}, errors.New("SSH identity inventory is unavailable")
	}
	defer rows.Close()
	backup := Backup{Schema: "fugue.ssh-identity-backup/v1", CapturedAt: time.Now().UTC(), Identities: []RetainedIdentity{}}
	for rows.Next() {
		var anchorRaw, recordRaw []byte
		if rows.Scan(&anchorRaw, &recordRaw) != nil || len(anchorRaw) > 16384 || len(recordRaw) > 32768 || len(backup.Identities) >= 10000 {
			return Backup{}, ErrIntegrity
		}
		var retained RetainedIdentity
		if Decode(anchorRaw, &retained.Anchor) != nil || Decode(recordRaw, &retained.Record) != nil {
			return Backup{}, ErrIntegrity
		}
		backup.Identities = append(backup.Identities, retained)
	}
	if rows.Err() != nil {
		return Backup{}, errors.New("SSH identity inventory could not be completely read")
	}
	backup.Digest = Digest(backup.Identities)
	return backup, ValidateBackup(backup, keyring)
}

func BuildPlan(current, backup Backup, keyring sshidentity.Keyring, keyringDigest string) (Plan, error) {
	if ValidateBackup(current, keyring) != nil || ValidateBackup(backup, keyring) != nil || current.Digest != backup.Digest || !digestPattern.MatchString(keyringDigest) {
		return Plan{}, ErrPrecondition
	}
	plan := Plan{Schema: "fugue.ssh-identity-rewrap-plan/v1", BackupDigest: backup.Digest, KeyringDigest: keyringDigest, Retention: "retain-identities-and-ownership-until-explicit-decommission", Entries: []Entry{}}
	for _, retained := range current.Identities {
		rewrapped, err := keyring.Rewrap(retained.Record)
		if err != nil || !reflect.DeepEqual(rewrapped.Identity, retained.Anchor) {
			return Plan{}, ErrIntegrity
		}
		plan.Entries = append(plan.Entries, Entry{Identity: retained.Anchor, RecordDigest: Digest(retained.Record), FromKeyID: retained.Record.EncryptionKeyID, ToKeyID: rewrapped.EncryptionKeyID, Rewrap: retained.Record.EncryptionKeyID != rewrapped.EncryptionKeyID})
	}
	plan.Digest = Digest(plan)
	return plan, nil
}

func Apply(ctx context.Context, database *sql.DB, keyring sshidentity.Keyring, keyringDigest string, backup Backup, expected Plan, actor string) (int, error) {
	if !actorPattern.MatchString(actor) {
		return 0, errors.New("explicit canonical maintenance actor required")
	}
	transaction, err := database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, errors.New("SSH identity maintenance transaction unavailable")
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, `SET LOCAL lock_timeout = '2s'`); err != nil {
		return 0, errors.New("SSH identity lock bound unavailable")
	}
	if _, err := transaction.ExecContext(ctx, `SET LOCAL statement_timeout = '15s'`); err != nil {
		return 0, errors.New("SSH identity statement bound unavailable")
	}
	current, err := ReadInventory(ctx, transaction, keyring)
	if err != nil {
		return 0, err
	}
	plan, err := BuildPlan(current, backup, keyring, keyringDigest)
	if err != nil || !reflect.DeepEqual(plan, expected) {
		return 0, ErrPrecondition
	}
	changed := 0
	for index, entry := range plan.Entries {
		if !entry.Rewrap {
			continue
		}
		previous := current.Identities[index].Record
		updated, err := keyring.Rewrap(previous)
		if err != nil || updated.EncryptionKeyID != entry.ToKeyID || !reflect.DeepEqual(updated.Identity, entry.Identity) {
			return 0, ErrIntegrity
		}
		previousRaw, _ := json.Marshal(previous)
		updatedRaw, _ := json.Marshal(updated)
		result, err := transaction.ExecContext(ctx, `UPDATE fugue_app_ssh_identities SET body=$1::jsonb WHERE app_id=$2 AND tenant_id=$3 AND identity_id=$4 AND body=$5::jsonb`, string(updatedRaw), entry.Identity.AppID, entry.Identity.TenantID, entry.Identity.ID, string(previousRaw))
		if err != nil {
			return 0, errors.New("SSH identity rewrap failed; transaction rolled back")
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			return 0, ErrPrecondition
		}
		metadata, _ := json.Marshal(map[string]string{"identity_id": entry.Identity.ID, "fingerprint": entry.Identity.Fingerprint, "from_key_id": entry.FromKeyID, "to_key_id": entry.ToKeyID, "plan_digest": plan.Digest, "backup_digest": backup.Digest})
		if _, err := transaction.ExecContext(ctx, `INSERT INTO fugue_audit_events (id,tenant_id,actor_type,actor_id,action,target_type,target_id,metadata_json,created_at) VALUES ($1,$2,'maintenance',$3,'ssh.identity.rewrapped','ssh_identity',$4,$5::jsonb,$6)`, model.NewID("audit"), entry.Identity.TenantID, actor, entry.Identity.ID, string(metadata), time.Now().UTC()); err != nil {
			return 0, errors.New("SSH identity audit failed; transaction rolled back")
		}
		changed++
	}
	verified, err := ReadInventory(ctx, transaction, keyring)
	if err != nil || len(verified.Identities) != len(current.Identities) {
		return 0, ErrIntegrity
	}
	for index, retained := range verified.Identities {
		if !reflect.DeepEqual(retained.Anchor, current.Identities[index].Anchor) || retained.Record.EncryptionKeyID != plan.Entries[index].ToKeyID {
			return 0, ErrIntegrity
		}
	}
	if err := transaction.Commit(); err != nil {
		return 0, errors.New("SSH identity maintenance commit was not confirmed; re-plan before retry")
	}
	return changed, nil
}

func KeyUsage(backup Backup) []string {
	used := map[string]bool{}
	for _, retained := range backup.Identities {
		used[retained.Record.EncryptionKeyID] = true
	}
	keys := make([]string, 0, len(used))
	for key := range used {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
