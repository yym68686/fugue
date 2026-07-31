package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/backupadapter"
	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	"fugue/internal/backupobserver"
	"fugue/internal/model"
	"fugue/internal/store"
)

type backupObservationAPIFixture struct {
	storePath  string
	stateStore *store.Store
	server     *Server
	keyring    backupidentity.Keyring
	run        model.BackupRun
	spec       backupcontrol.BackupRunSpec
	token      string
}

func TestBackupObservationAPIIsExactReadOnlyAndRedacted(t *testing.T) {
	fixture := newBackupObservationAPIFixture(t)
	before, err := os.ReadFile(fixture.storePath)
	if err != nil {
		t.Fatalf("read store before observation: %v", err)
	}
	recorder := performJSONRequest(t, fixture.server, http.MethodGet, fixture.observationURL(), fixture.token, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("observation status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	after, err := os.ReadFile(fixture.storePath)
	if err != nil {
		t.Fatalf("read store after observation: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("GET observation mutated the legacy JSON store")
	}
	assertBackupObservationPrivateNoStore(t, recorder.Header())
	status, err := backupcontrol.DecodeBackupRunStatus(fixture.spec, recorder.Body.Bytes())
	if err != nil {
		t.Fatalf("decode strict observation status: %v body=%s", err, recorder.Body.String())
	}
	if status.RunID != fixture.run.ID || status.SpecDigest != fixture.spec.Digest ||
		status.CellKey != fixture.spec.CellKey || status.ObservedState != model.BackupRunStatusPending ||
		!status.ObservationOnly || status.ProductionWriteAllowed || !status.ValidUntil.After(status.ObservedAt) {
		t.Fatalf("observation contract drifted: %+v", status)
	}
	for _, forbidden := range []string{
		"private-backup-bucket", "https://s3.example.test", "observation-access-key",
		"observation-secret-key", "observation-session-token", "credential_secret_id",
		"ciphertext", "object_key", "manifest_object_key",
	} {
		if bytes.Contains(bytes.ToLower(recorder.Body.Bytes()), bytes.ToLower([]byte(forbidden))) {
			t.Fatalf("observation response leaked %q: %s", forbidden, recorder.Body.String())
		}
	}
}

func TestBackupObservationAPIInteroperatesWithIsolatedObserverClient(t *testing.T) {
	fixture := newBackupObservationAPIFixture(t)
	before, err := os.ReadFile(fixture.storePath)
	if err != nil {
		t.Fatalf("read store before observer client request: %v", err)
	}
	httpServer := httptest.NewServer(fixture.server.Handler())
	defer httpServer.Close()
	source, err := backupobserver.NewHTTPObservationSource(backupobserver.HTTPObservationSourceConfig{
		BaseURL:                   httpServer.URL,
		BearerToken:               fixture.token,
		RequestTimeout:            time.Second,
		MaxResponseBytes:          64 << 10,
		AllowInsecureHTTPForTests: true,
		Client:                    httpServer.Client(),
	})
	if err != nil {
		t.Fatalf("construct isolated observer client: %v", err)
	}
	status, err := source.Observe(context.Background(), fixture.spec)
	if err != nil {
		t.Fatalf("observe through real client/server boundary: %v", err)
	}
	if err := backupcontrol.ValidateBackupRunStatus(fixture.spec, status); err != nil ||
		status.RunID != fixture.run.ID || status.CellKey != fixture.spec.CellKey {
		t.Fatalf("observer client received an invalid status: %+v err=%v", status, err)
	}
	after, err := os.ReadFile(fixture.storePath)
	if err != nil {
		t.Fatalf("read store after observer client request: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("real observer client request mutated the legacy JSON store")
	}
}

func TestBackupObservationAPIRejectsIdentityQueryTenantAndCellDriftWithoutMutation(t *testing.T) {
	fixture := newBackupObservationAPIFixture(t)
	before, err := os.ReadFile(fixture.storePath)
	if err != nil {
		t.Fatalf("read store before rejected observations: %v", err)
	}
	otherCell := "backup/registry/ffffffffffffffff"
	wrongCellToken := fixture.issueToken(t, backupidentity.Claims{
		CredentialID: backupidentity.CredentialIDForCell(otherCell), RunID: fixture.run.ID,
		CellKey: otherCell, SpecDigest: fixture.spec.Digest,
	})
	wrongTenantToken := fixture.issueToken(t, backupidentity.Claims{
		CredentialID: backupidentity.CredentialIDForCell(fixture.spec.CellKey), RunID: fixture.run.ID,
		TenantID: "tenant-other", CellKey: fixture.spec.CellKey, SpecDigest: fixture.spec.Digest,
	})
	otherDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	tests := []struct {
		name   string
		target string
		token  string
		status int
	}{
		{name: "missing query", target: "/v1/backup-control/runs/" + fixture.run.ID + "/observation", token: fixture.token, status: http.StatusBadRequest},
		{name: "duplicate query", target: fixture.observationURL() + "&" + backupObservationSpecQuery(fixture.spec.Digest), token: fixture.token, status: http.StatusBadRequest},
		{name: "unknown query", target: fixture.observationURL() + "&other=value", token: fixture.token, status: http.StatusBadRequest},
		{name: "noncanonical query", target: "/v1/backup-control/runs/" + fixture.run.ID + "/observation?" + backupObservationSpecQuery(fixture.spec.Digest) + "%20", token: fixture.token, status: http.StatusBadRequest},
		{name: "wrong run", target: "/v1/backup-control/runs/run-other/observation?" + backupObservationSpecQuery(fixture.spec.Digest), token: fixture.token, status: http.StatusForbidden},
		{name: "wrong digest", target: "/v1/backup-control/runs/" + fixture.run.ID + "/observation?" + backupObservationSpecQuery(otherDigest), token: fixture.token, status: http.StatusForbidden},
		{name: "wrong cell", target: fixture.observationURL(), token: wrongCellToken, status: http.StatusForbidden},
		{name: "wrong tenant", target: fixture.observationURL(), token: wrongTenantToken, status: http.StatusNotFound},
		{name: "bootstrap credential", target: fixture.observationURL(), token: "bootstrap-secret", status: http.StatusUnauthorized},
		{name: "platform credential shape", target: fixture.observationURL(), token: "fugue_pc_v1.invalid.invalid.invalid", status: http.StatusUnauthorized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := performJSONRequest(t, fixture.server, http.MethodGet, test.target, test.token, nil)
			if recorder.Code != test.status {
				t.Fatalf("status=%d want=%d body=%s", recorder.Code, test.status, recorder.Body.String())
			}
			assertBackupObservationPrivateNoStore(t, recorder.Header())
			for _, forbidden := range []string{"private-backup-bucket", "observation-secret-key", "ciphertext"} {
				if strings.Contains(strings.ToLower(recorder.Body.String()), strings.ToLower(forbidden)) {
					t.Fatalf("rejected observation leaked %q: %s", forbidden, recorder.Body.String())
				}
			}
		})
	}

	withoutKeyring := NewServer(fixture.stateStore, auth.New(fixture.stateStore, "bootstrap-secret"), nil, ServerConfig{})
	recorder := performJSONRequest(t, withoutKeyring, http.MethodGet, fixture.observationURL(), fixture.token, nil)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("empty keyring status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	assertBackupObservationPrivateNoStore(t, recorder.Header())
	after, err := os.ReadFile(fixture.storePath)
	if err != nil {
		t.Fatalf("read store after rejected observations: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("rejected observation mutated the legacy JSON store")
	}
}

func TestBackupObservationAPIFailsClosedWhenBackendGenerationRotates(t *testing.T) {
	fixture := newBackupObservationAPIFixture(t)
	if _, err := fixture.stateStore.RotateBackupBackendCredentials(
		fixture.run.BackendID,
		"",
		true,
		model.DataBackendCredentials{
			AccessKeyID: "rotated-access-key", SecretAccessKey: "rotated-secret-key", Token: "rotated-session-token",
		},
	); err != nil {
		t.Fatalf("rotate backend credentials: %v", err)
	}
	before, err := os.ReadFile(fixture.storePath)
	if err != nil {
		t.Fatalf("read store before stale precondition: %v", err)
	}
	recorder := performJSONRequest(t, fixture.server, http.MethodGet, fixture.observationURL(), fixture.token, nil)
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "spec digest precondition failed") {
		t.Fatalf("stale generation status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	assertBackupObservationPrivateNoStore(t, recorder.Header())
	for _, forbidden := range []string{"rotated-access-key", "rotated-secret-key", "rotated-session-token"} {
		if strings.Contains(recorder.Body.String(), forbidden) {
			t.Fatalf("stale precondition response leaked %q: %s", forbidden, recorder.Body.String())
		}
	}
	after, err := os.ReadFile(fixture.storePath)
	if err != nil {
		t.Fatalf("read store after stale precondition: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("stale spec precondition mutated the legacy JSON store")
	}
}

func TestExactBackupObservationSpecDigestRejectsAmbiguousQueries(t *testing.T) {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for raw, accepted := range map[string]bool{
		backupObservationSpecQuery(digest): true,
		"":                                 false,
		"spec_digest=":                     false,
		"other=" + url.QueryEscape(digest): false,
		backupObservationSpecQuery(digest) + "&" + backupObservationSpecQuery(digest): false,
		backupObservationSpecQuery(digest) + "&other=value":                           false,
		"%73pec_digest=" + url.QueryEscape(digest):                                    false,
		backupObservationSpecQuery(digest) + "%20":                                    false,
		backupObservationSpecQuery(digest) + ";other=value":                           false,
	} {
		value, ok := exactBackupObservationSpecDigest(&url.URL{RawQuery: raw})
		if ok != accepted || (accepted && value != digest) {
			t.Fatalf("raw query %q = (%q,%t), want accepted=%t", raw, value, ok, accepted)
		}
	}
	if _, ok := exactBackupObservationSpecDigest(nil); ok {
		t.Fatal("nil URL unexpectedly produced a spec digest")
	}
}

func newBackupObservationAPIFixture(t *testing.T) backupObservationAPIFixture {
	t.Helper()
	clearBackupObservationAPIEnv(t)
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "backup-observation-api-encryption-key")
	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name: "observation backend", Provider: model.DataBackendProviderS3,
		Bucket: "private-backup-bucket", Region: "region-1", Endpoint: "https://s3.example.test", Prefix: "backups",
		Credentials: model.DataBackendCredentials{
			AccessKeyID: "observation-access-key", SecretAccessKey: "observation-secret-key", Token: "observation-session-token",
		},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		ID: "run-observation-1", BackendID: backend.ID, Trigger: model.BackupRunTriggerManual,
		Target: model.BackupTarget{Type: model.BackupTargetRegistry},
	})
	if err != nil {
		t.Fatalf("create backup run: %v", err)
	}
	backendObservation, err := stateStore.GetBackupBackendObservation(backend.ID, "", true)
	if err != nil {
		t.Fatalf("read backend observation: %v", err)
	}
	spec, err := backupadapter.BuildShadowSpec(run, backendObservation.Generation)
	if err != nil {
		t.Fatalf("build shadow spec: %v", err)
	}
	keyring := backupidentity.DeriveKeyring(strings.Repeat("i", 32), "backup-key-1", "", "", nil)
	authenticator := auth.New(stateStore, "bootstrap-secret")
	authenticator.BackupObserverIdentityKeyring = keyring
	fixture := backupObservationAPIFixture{
		storePath: storePath, stateStore: stateStore, keyring: keyring, run: run, spec: spec,
		server: NewServer(stateStore, authenticator, nil, ServerConfig{}),
	}
	fixture.token = fixture.issueToken(t, backupidentity.Claims{
		CredentialID: backupidentity.CredentialIDForCell(spec.CellKey), RunID: run.ID,
		TenantID: run.TenantID, CellKey: spec.CellKey, SpecDigest: spec.Digest,
	})
	return fixture
}

func (fixture backupObservationAPIFixture) issueToken(t *testing.T, claims backupidentity.Claims) string {
	t.Helper()
	token, err := backupidentity.Issue(fixture.keyring, claims, time.Now().UTC(), 5*time.Minute)
	if err != nil {
		t.Fatalf("issue backup observer token: %v", err)
	}
	return token
}

func (fixture backupObservationAPIFixture) observationURL() string {
	return "/v1/backup-control/runs/" + fixture.run.ID + "/observation?" + backupObservationSpecQuery(fixture.spec.Digest)
}

func backupObservationSpecQuery(digest string) string {
	return url.Values{"spec_digest": []string{digest}}.Encode()
}

func assertBackupObservationPrivateNoStore(t *testing.T, header http.Header) {
	t.Helper()
	cacheControl := header.Get("Cache-Control")
	if !strings.Contains(cacheControl, "private") || !strings.Contains(cacheControl, "no-store") || header.Get("Pragma") != "no-cache" {
		t.Fatalf("backup observation response is cacheable: %v", header)
	}
	if contentType := header.Get("Content-Type"); !strings.HasPrefix(contentType, "application/json") {
		t.Fatalf("backup observation response content type=%q, want application/json", contentType)
	}
}

func clearBackupObservationAPIEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"FUGUE_DATA_BACKEND_PROVIDER",
		"FUGUE_DATA_BACKEND_BUCKET",
		"FUGUE_DATA_BACKEND_ACCESS_KEY_ID",
		"FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY",
		"FUGUE_DATA_BACKEND_SESSION_TOKEN",
		"FUGUE_DATA_BACKEND_ENDPOINT",
		"FUGUE_DATA_R2_ACCOUNT_ID",
		"FUGUE_DATA_BACKEND_REGION",
		"FUGUE_DATA_BACKEND_PREFIX",
		"FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY",
	} {
		t.Setenv(key, "")
	}
}
