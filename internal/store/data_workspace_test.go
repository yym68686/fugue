package store

import (
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestDefaultDataBackendWithoutEnvUsesManagedBlobAPI(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.GetDataBackend(stateStore.DefaultDataBackendID(), "", true)
	if err != nil {
		t.Fatalf("get default backend: %v", err)
	}
	if backend.Provider != model.DataBackendProviderFugueManaged {
		t.Fatalf("expected unmanaged local default to be fugue-managed, got %+v", backend)
	}
	if !backend.Capabilities.FugueManagedBlobAPI || backend.Capabilities.S3Compatible {
		t.Fatalf("expected managed blob API capabilities, got %+v", backend.Capabilities)
	}
}

func TestSeedDefaultDataBackendFromEnvStoresEncryptedCredentials(t *testing.T) {
	t.Setenv("FUGUE_DATA_BACKEND_PROVIDER", model.DataBackendProviderCloudflareR2)
	t.Setenv("FUGUE_DATA_R2_ACCOUNT_ID", "acct123")
	t.Setenv("FUGUE_DATA_BACKEND_BUCKET", "fugue-data")
	t.Setenv("FUGUE_DATA_BACKEND_PREFIX", "prod")
	t.Setenv("FUGUE_DATA_BACKEND_ACCESS_KEY_ID", "access-key")
	t.Setenv("FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY", "secret-key")
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "test-encryption-key")

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.GetDataBackend(stateStore.DefaultDataBackendID(), "", true)
	if err != nil {
		t.Fatalf("get default backend: %v", err)
	}
	if backend.Provider != model.DataBackendProviderCloudflareR2 || backend.Bucket != "fugue-data" || backend.Prefix != "prod" {
		t.Fatalf("unexpected seeded backend: %+v", backend)
	}
	if backend.Endpoint != "https://acct123.r2.cloudflarestorage.com" {
		t.Fatalf("unexpected R2 endpoint %q", backend.Endpoint)
	}
	if backend.Credentials.AccessKeyID != "access-key" {
		t.Fatalf("expected redacted backend to keep access key id, got %+v", backend.Credentials)
	}
	if backend.Credentials.SecretAccessKey != "" {
		t.Fatalf("expected secret access key to be redacted from metadata, got %+v", backend.Credentials)
	}
	forUse, err := stateStore.GetDataBackendForUse(stateStore.DefaultDataBackendID(), "", true)
	if err != nil {
		t.Fatalf("get default backend for use: %v", err)
	}
	if forUse.Credentials.AccessKeyID != "access-key" || forUse.Credentials.SecretAccessKey != "secret-key" {
		t.Fatalf("expected unredacted credentials for backend use, got %+v", forUse.Credentials)
	}
	if forUse.CredentialSecretID == "" {
		t.Fatal("expected encrypted credential secret id")
	}
	rotated, err := stateStore.RotateDataBackendCredentials(stateStore.DefaultDataBackendID(), "", true, model.DataBackendCredentials{AccessKeyID: "rotated-access", SecretAccessKey: "rotated-secret"})
	if err != nil {
		t.Fatalf("rotate credentials: %v", err)
	}
	if rotated.Credentials.AccessKeyID != "rotated-access" || rotated.Credentials.SecretAccessKey != "" {
		t.Fatalf("expected rotated backend credentials to be redacted, got %+v", rotated.Credentials)
	}
	rotatedForUse, err := stateStore.GetDataBackendForUse(stateStore.DefaultDataBackendID(), "", true)
	if err != nil {
		t.Fatalf("get rotated backend for use: %v", err)
	}
	if rotatedForUse.Credentials.AccessKeyID != "rotated-access" || rotatedForUse.Credentials.SecretAccessKey != "rotated-secret" {
		t.Fatalf("expected rotated unredacted credentials for backend use, got %+v", rotatedForUse.Credentials)
	}
}

func clearDefaultDataBackendEnv(t *testing.T) {
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
