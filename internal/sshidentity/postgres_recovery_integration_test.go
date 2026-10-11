package sshidentity

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"

	_ "github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/crypto/ssh"
)

func TestRestoredSSHIdentityMatchesAuthenticatedInventory(test *testing.T) {
	address := os.Getenv("FUGUE_TEST_SSH_RESTORED_DATABASE_URL")
	if address == "" {
		test.Skip("requires independently restored SSH identity tables in a disposable database")
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Hostname() != "127.0.0.1" || !strings.Contains(parsed.Path, "fugue_test") {
		test.Fatal("requires disposable loopback fugue_test database")
	}
	keyring, err := LoadKeyring(os.Getenv("FUGUE_TEST_SSH_KEYRING"))
	if err != nil {
		test.Fatal("independent protected keyring backup unavailable")
	}
	raw, err := os.ReadFile(os.Getenv("FUGUE_TEST_SSH_EXPECTED_IDENTITY"))
	if err != nil {
		test.Fatal("authenticated public identity inventory unavailable")
	}
	var expected struct {
		Identity model.AppSSHIdentity `json:"identity"`
	}
	if json.Unmarshal(raw, &expected) != nil || ValidatePublic(expected.Identity) != nil {
		test.Fatal("invalid public identity inventory")
	}
	database, err := sql.Open("pgx", address)
	if err != nil {
		test.Fatal("cannot open restored database")
	}
	defer database.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	transaction, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		test.Fatal("cannot begin read-only recovery verification")
	}
	defer transaction.Rollback()
	rows, err := transaction.QueryContext(ctx, `SELECT anchor.public_body, identity.body
FROM fugue_app_ssh_identity_anchors anchor FULL JOIN fugue_app_ssh_identities identity USING (app_id)
ORDER BY COALESCE(anchor.app_id, identity.app_id) LIMIT 10001`)
	if err != nil {
		test.Fatal("restored identity tables unavailable")
	}
	defer rows.Close()
	count, found := 0, false
	for rows.Next() {
		count++
		if count > 10000 {
			test.Fatal("recovery verification inventory exceeds bound")
		}
		var publicRaw, privateRaw []byte
		if rows.Scan(&publicRaw, &privateRaw) != nil {
			test.Fatal("restored identity row is incomplete")
		}
		var anchor model.AppSSHIdentity
		var record model.AppSSHIdentityRecord
		if json.Unmarshal(publicRaw, &anchor) != nil || json.Unmarshal(privateRaw, &record) != nil || !reflect.DeepEqual(anchor, record.Identity) {
			test.Fatal("restored ownership and encrypted identity differ")
		}
		signer, err := keyring.Signer(record)
		if err != nil || ssh.FingerprintSHA256(signer.PublicKey()) != anchor.Fingerprint {
			test.Fatal("restored private key does not match its retained identity")
		}
		if anchor.AppID == expected.Identity.AppID {
			if !reflect.DeepEqual(anchor, expected.Identity) {
				test.Fatal("backup identity differs from authenticated API inventory")
			}
			found = true
		}
	}
	if rows.Err() != nil || !found {
		test.Fatal("expected identity missing from independently restored backup")
	}
	test.Logf("verified %d restored encrypted identities and ownership anchors", count)
}
