package sshmaintenance

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"fugue/internal/sshidentity"
	"github.com/DATA-DOG/go-sqlmock"
)

func fixture(test *testing.T) (Backup, sshidentity.Keyring) {
	test.Helper()
	old, err := sshidentity.NewKeyring("storage-old", map[string][]byte{"storage-old": bytes.Repeat([]byte{23}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	current, err := sshidentity.NewKeyring("storage-new", map[string][]byte{"storage-old": bytes.Repeat([]byte{23}, 32), "storage-new": bytes.Repeat([]byte{29}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	record, err := old.Create("tenant", "app-fixture", time.Now().UTC())
	if err != nil {
		test.Fatal(err)
	}
	backup := Backup{Schema: "fugue.ssh-identity-backup/v1", CapturedAt: time.Now().UTC(), Identities: []RetainedIdentity{{Anchor: record.Identity, Record: record}}}
	backup.Digest = Digest(backup.Identities)
	return backup, current
}

func TestRewrapPlanBindsProtectedBackupAndNeverExposesCiphertext(test *testing.T) {
	backup, keyring := fixture(test)
	digest := "sha256:" + strings.Repeat("a", 64)
	plan, err := BuildPlan(backup, backup, keyring, digest)
	if err != nil || len(plan.Entries) != 1 || !plan.Entries[0].Rewrap || plan.Entries[0].FromKeyID != "storage-old" || plan.Entries[0].ToKeyID != "storage-new" || !reflect.DeepEqual(plan.Entries[0].Identity, backup.Identities[0].Anchor) {
		test.Fatal("rewrap plan changed logical identity or omitted storage-key transition", err)
	}
	raw, _ := json.Marshal(plan)
	if bytes.Contains(raw, []byte(backup.Identities[0].Record.Ciphertext)) || bytes.Contains(raw, []byte("ciphertext")) || bytes.Contains(raw, []byte("PRIVATE KEY")) {
		test.Fatal("public review plan exposed protected storage material")
	}
	changed := backup
	changed.Identities = append([]RetainedIdentity{}, backup.Identities...)
	changed.Identities[0].Record, err = keyring.Rewrap(changed.Identities[0].Record)
	if err != nil {
		test.Fatal(err)
	}
	changed.Digest = Digest(changed.Identities)
	if _, err := BuildPlan(changed, backup, keyring, digest); err == nil {
		test.Fatal("stale backup authorized changed identity inventory")
	}
	noop, err := BuildPlan(changed, changed, keyring, digest)
	if err != nil || noop.Entries[0].Rewrap {
		test.Fatal("already rewrapped identity needs no ciphertext churn", err)
	}
	changed.Identities[0].Anchor.AppID = "other-app"
	changed.Digest = Digest(changed.Identities)
	if ValidateBackup(changed, keyring) == nil {
		test.Fatal("ownership substitution accepted")
	}
}

func TestMaintenanceFilesRejectOverwriteAndInsecureInput(test *testing.T) {
	directory := test.TempDir()
	path := filepath.Join(directory, "snapshot.json")
	if WritePrivate(path, map[string]string{"value": "protected"}) != nil || WritePrivate(path, map[string]string{"value": "replacement"}) == nil {
		test.Fatal("maintenance backup overwrite was admitted")
	}
	if _, err := ReadPrivate(path, 1024); err != nil {
		test.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		test.Fatal(err)
	}
	if _, err := ReadPrivate(path, 1024); err == nil {
		test.Fatal("publicly readable encrypted backup admitted")
	}
	var value struct {
		Value string `json:"value"`
	}
	if Decode([]byte(`{"value":"one","extra":"unreviewed"}`), &value) == nil || Decode([]byte(`{"value":"one"}{}`), &value) == nil {
		test.Fatal("unknown fields or trailing plan data accepted")
	}
}

func TestRewrapRollsBackWhenAuditCannotPersist(test *testing.T) {
	backup, keyring := fixture(test)
	digest := "sha256:" + strings.Repeat("a", 64)
	plan, err := BuildPlan(backup, backup, keyring, digest)
	if err != nil {
		test.Fatal(err)
	}
	database, mock, err := sqlmock.New()
	if err != nil {
		test.Fatal(err)
	}
	defer database.Close()
	anchor, _ := json.Marshal(backup.Identities[0].Anchor)
	record, _ := json.Marshal(backup.Identities[0].Record)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SET LOCAL lock_timeout = '2s'`)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`SET LOCAL statement_timeout = '15s'`)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT anchor.public_body, identity.body").WillReturnRows(sqlmock.NewRows([]string{"public_body", "body"}).AddRow(anchor, record))
	mock.ExpectExec("UPDATE fugue_app_ssh_identities").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO fugue_audit_events").WillReturnError(context.DeadlineExceeded)
	mock.ExpectRollback()
	if count, err := Apply(context.Background(), database, keyring, digest, backup, plan, "operator-fixture"); err == nil || count != 0 || strings.Contains(err.Error(), backup.Identities[0].Record.Ciphertext) {
		test.Fatal("audit failure did not abort private-key rewrapping", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		test.Fatal(err)
	}
}
