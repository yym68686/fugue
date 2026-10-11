package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"fugue/internal/sshidentity"
)

func TestAppSSHIdentityPostgresIntegration(test *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		test.Skip("set disposable loopback FUGUE_TEST_DATABASE_URL")
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Hostname() != "127.0.0.1" || !strings.Contains(parsed.Path, "fugue_test") {
		test.Fatal("requires disposable loopback fugue_test database")
	}
	stateStore := New("", address)
	database, err := sql.Open("pgx", address)
	if err != nil {
		test.Fatal(err)
	}
	defer database.Close()
	stateStore.db = database
	transaction, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		test.Fatal(err)
	}
	defer transaction.Rollback()
	if _, err := stateStore.applyPostgresSchemaTx(context.Background(), transaction); err != nil {
		test.Fatal(err)
	}
	if err := transaction.Commit(); err != nil {
		test.Fatal(err)
	}
	for _, migrate := range []func(context.Context, string) error{
		schemamigrate.MigratePlatformState,
		schemamigrate.MigrateImageCacheManifestGraph,
		schemamigrate.MigrateEdgeInstanceFencing,
		schemamigrate.MigrateSourceUploadSessions,
	} {
		if err := migrate(context.Background(), address); err != nil {
			test.Fatal(err)
		}
	}
	if err := stateStore.Init(); err != nil {
		test.Fatal(err)
	}
	defer stateStore.db.Close()
	for attempt := 0; attempt < 2; attempt++ {
		if err := schemamigrate.MigrateSSHIdentity(context.Background(), address); err != nil {
			test.Fatal(err)
		}
	}
	keyring, err := sshidentity.NewKeyring("storage-v1", map[string][]byte{"storage-v1": bytes.Repeat([]byte{27}, 32)})
	if err != nil {
		test.Fatal(err)
	}
	stateStore.ConfigureSSHIdentityKeyring(keyring)
	tenant, err := stateStore.CreateTenant(model.NewID("identity-test"))
	if err != nil {
		test.Fatal(err)
	}
	defer stateStore.db.Exec("DELETE FROM fugue_tenants WHERE id=$1", tenant.ID)
	defer stateStore.db.Exec("DELETE FROM fugue_app_ssh_identity_anchors WHERE tenant_id=$1", tenant.ID)
	defer stateStore.db.Exec("DELETE FROM fugue_app_ssh_identities WHERE tenant_id=$1", tenant.ID)
	project, err := stateStore.CreateProject(tenant.ID, "workspaces", "")
	if err != nil {
		test.Fatal(err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "workspace", "", model.AppSpec{Image: "example/workspace:v1", Replicas: 1})
	if err != nil {
		test.Fatal(err)
	}
	const workers = 10
	identities := make(chan model.AppSSHIdentity, workers)
	failures := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			identity, err := stateStore.PrepareAppSSHIdentity(tenant.ID, app.ID)
			identities <- identity
			failures <- err
		}()
	}
	group.Wait()
	close(identities)
	close(failures)
	for err := range failures {
		if err != nil {
			test.Fatal(err)
		}
	}
	var expected model.AppSSHIdentity
	for identity := range identities {
		if expected.ID == "" {
			expected = identity
		}
		if !reflect.DeepEqual(identity, expected) {
			test.Fatal("concurrent database preparations selected different identities")
		}
	}
	identity, err := stateStore.GetAppSSHIdentity(tenant.ID, app.ID)
	if err != nil || !reflect.DeepEqual(identity, expected) {
		test.Fatal("stored identity changed", err)
	}
	defer stateStore.db.Exec("DELETE FROM fugue_managed_ssh_intents WHERE tenant_id=$1", tenant.ID)
	verifyManagedSSHIntentCAS(test, stateStore, app, identity.ID)
	defer stateStore.db.Exec("DELETE FROM fugue_managed_ssh_control WHERE tenant_id=$1", tenant.ID)
	defer stateStore.db.Exec("DELETE FROM fugue_managed_ssh_artifacts WHERE app_id=$1", app.ID)
	verifyManagedSSHControlStore(test, stateStore, app, identity)
	if _, err := stateStore.GetAppSSHIdentity("tenant_other", app.ID); !errors.Is(err, ErrNotFound) {
		test.Fatal("database tenant isolation failed", err)
	}
	if _, err := stateStore.AppSSHIdentityPrivateKey(tenant.ID, app.ID, expected.ID); err != nil {
		test.Fatal(err)
	}
	_, endpoint, err := stateStore.UpsertAppSSHConfig(app.ID, AppSSHUpdate{Enabled: true, Hostname: "ssh.example", PublicPortStart: 24000, PublicPortEnd: 25000, AuthorizedKeys: []string{testSSHPublicKey}})
	if err != nil {
		test.Fatal(err)
	}
	now := time.Now().UTC()
	observation := model.AppSSHObservation{ID: model.NewID("sshobs"), TenantID: app.TenantID, AppID: app.ID, IdentityID: expected.ID, EndpointID: endpoint.ID, RouteDigest: model.AppSSHRouteDigest(endpoint), Scope: "public_endpoint", Observer: "database-integration", Target: model.AppSSHPublicAddress(endpoint), Algorithm: expected.Algorithm, Fingerprint: expected.Fingerprint, KeyExchangeVerified: true, IdentityMatched: true, ObservedAt: now, ExpiresAt: now.Add(time.Minute), Reason: "identity_verified"}
	defer stateStore.db.Exec("DELETE FROM fugue_app_ssh_observations WHERE tenant_id=$1", tenant.ID)
	if err := stateStore.RecordAppSSHObservation(observation); err != nil {
		test.Fatal(err)
	}
	latest, err := stateStore.LatestAppSSHObservation(tenant.ID, app.ID)
	if err != nil || latest.ID != observation.ID || !latest.IdentityMatched {
		test.Fatal("database observation did not roundtrip", latest, err)
	}
	defer stateStore.db.Exec("DELETE FROM fugue_managed_ssh_bindings WHERE tenant_id=$1", tenant.ID)
	verifyManagedSSHBindingStore(test, stateStore, app, identity.ID, true)
	verifyManagedSSHRouteProjection(test, stateStore, app)
	verifyEdgeSSHAcknowledgementStore(test, stateStore)
	var original []byte
	if err := stateStore.db.QueryRow("SELECT body FROM fugue_app_ssh_identities WHERE app_id=$1", app.ID).Scan(&original); err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.db.Exec(`UPDATE fugue_app_ssh_identities SET body=jsonb_set(body, '{ciphertext}', '"corrupt"'::jsonb) WHERE app_id=$1`, app.ID); err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.PrepareAppSSHIdentity(tenant.ID, app.ID); !errors.Is(err, sshidentity.ErrIntegrity) {
		test.Fatal("database corruption regenerated identity", err)
	}
	if _, err := stateStore.db.Exec("UPDATE fugue_app_ssh_identities SET body=$1::jsonb WHERE app_id=$2", string(original), app.ID); err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.db.Exec("DELETE FROM fugue_app_ssh_identities WHERE app_id=$1", app.ID); err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.PrepareAppSSHIdentity(tenant.ID, app.ID); !errors.Is(err, sshidentity.ErrIntegrity) {
		test.Fatal("missing identity record regenerated a key", err)
	}
	if _, err := stateStore.db.Exec("INSERT INTO fugue_app_ssh_identities(app_id,tenant_id,identity_id,body) VALUES($1,$2,$3,$4::jsonb)", app.ID, tenant.ID, expected.ID, string(original)); err != nil {
		test.Fatal(err)
	}
	if _, err := stateStore.db.Exec("DELETE FROM fugue_apps WHERE id=$1", app.ID); err != nil {
		test.Fatal(err)
	}
	var retained int
	if err := stateStore.db.QueryRow("SELECT count(*) FROM fugue_app_ssh_identities WHERE app_id=$1", app.ID).Scan(&retained); err != nil || retained != 1 {
		test.Fatal("compute deletion cascaded to identity material", err)
	}
	if _, err := stateStore.GetAppSSHIdentity(tenant.ID, app.ID); !errors.Is(err, ErrNotFound) {
		test.Fatal("deleted application can still access retained identity", err)
	}
}
