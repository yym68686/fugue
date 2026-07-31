package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestAppBackupStatusJSONRedactsAppSecretsFromOlderServers(t *testing.T) {
	t.Parallel()

	const (
		envSecret      = "must-not-leak-env-secret"
		postgresSecret = "must-not-leak-postgres-secret"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"replicas":1},"status":{"phase":"ready"},"created_at":"2026-07-29T00:00:00Z","updated_at":"2026-07-29T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/backups/status":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"replicas":1,"env":{"API_KEY":"` + envSecret + `"},"postgres":{"password":"` + postgresSecret + `"}},"status":{"phase":"ready"},"created_at":"2026-07-29T00:00:00Z","updated_at":"2026-07-29T00:00:00Z"},"policies":[],"artifacts":[],"posture":[]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"app", "backup", "status", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app backup status: %v stderr=%s", err, stderr.String())
	}
	output := stdout.String()
	for _, leaked := range []string{envSecret, postgresSecret} {
		if strings.Contains(output, leaked) {
			t.Fatalf("default JSON output leaked %q: %s", leaked, output)
		}
	}
	if strings.Count(output, redactedSecretValue) < 2 {
		t.Fatalf("expected env and postgres secrets to be redacted, got %s", output)
	}
}

func TestAppBackupStatusJSONRedactsDiagnosticAndReceiptSecrets(t *testing.T) {
	t.Parallel()

	const (
		basicSecret          = "basic-header-secret"
		bearerSecret         = "bearer-header-secret"
		xAPIKeySecret        = "x-api-key-secret"
		xAuthTokenSecret     = "x-auth-token-secret"
		urlPasswordSecret    = "url-password-secret"
		queryTokenSecret     = "query-token-secret"
		passwordSecret       = "password-assignment-secret"
		awsSecret            = "aws-secret-access-key-value"
		clientSecret         = "oauth-client-secret-value"
		cookieSecret         = "session-cookie-secret"
		receiptPassword      = "receipt-password-secret"
		receiptHeaderSecret  = "receipt-header-secret"
		workspaceResetSecret = "workspace-reset-secret"
		storageResetSecret   = "storage-reset-secret"
	)
	receiptJSON := `{"receipt_id":"receipt_public_123","event":"backup.upload.failed","error":"X-Auth-Token: ` + receiptHeaderSecret + `","headers":{"X-API-Key":"` + xAPIKeySecret + `"},"credentials":{"username":"backup-user","password":"` + receiptPassword + `"},"token_id":"token_record_123","token_prefix":"tok_public","api_key_id":"api_key_record_123","access_key_id":"access_key_record_123","credential_id":"credential_record_123","callback_url":"https://status.example/receipts/receipt_public_123"}`
	status := appBackupStatusResponse{
		App: model.App{
			ID:        "app_public_123",
			TenantID:  "tenant_public_123",
			ProjectID: "project_public_123",
			Name:      "demo",
			Source: &model.AppSource{
				Type:      "git",
				RepoURL:   "https://git-user:" + urlPasswordSecret + "@git.example/repo.git?access_token=" + queryTokenSecret,
				CommitSHA: "commit_public_123",
			},
			Spec: model.AppSpec{
				Replicas:  1,
				RuntimeID: "runtime_public_123",
				Env: map[string]string{
					"API_KEY": xAPIKeySecret,
				},
				Workspace: &model.AppWorkspaceSpec{
					MountPath:  "/workspace",
					ResetToken: workspaceResetSecret,
				},
				PersistentStorage: &model.AppPersistentStorageSpec{
					StorageSize: "10Gi",
					ResetToken:  storageResetSecret,
				},
			},
			Status: model.AppStatus{
				Phase:       "deployed",
				LastMessage: "event=backup.failed Authorization: Basic " + basicSecret,
				LastFailedOperation: &model.AppOperationFailure{
					ID:            "op_public_123",
					Type:          "backup",
					ErrorMessage:  "connect postgresql://backup-user:" + urlPasswordSecret + "@db.example/app",
					ResultMessage: "AWS_SECRET_ACCESS_KEY=" + awsSecret,
				},
				SourceSync: &model.AppSourceSyncStatus{
					Provider:         "github",
					LastErrorCode:    "webhook_failed",
					LastErrorMessage: `{"event":"sync.failed","authorization":"Bearer ` + bearerSecret + `","client_secret":"` + clientSecret + `"}`,
				},
			},
			StoredStatus: &model.AppStatus{
				Phase:       "deployed",
				LastMessage: "X-API-Key: " + xAPIKeySecret,
			},
			ObservedStatus: &model.AppObservedStatus{
				Phase:               "ready",
				RuntimeID:           "runtime_public_123",
				EvidenceSource:      "controller",
				EvidenceSources:     []string{"receipt=" + receiptJSON},
				Reason:              "deployment_ready",
				Message:             "Proxy-Authorization: Bearer " + bearerSecret,
				InvariantViolations: []string{"Password=" + passwordSecret},
			},
			BackingServices: []model.BackingService{{
				ID:          "service_public_123",
				TenantID:    "tenant_public_123",
				ProjectID:   "project_public_123",
				Name:        "postgres",
				Type:        "postgres",
				Provisioner: "cnpg",
				Status:      "ready",
				RuntimeStatus: &model.BackingServiceRuntimeStatus{
					Phase:   "ready",
					Message: "Set-Cookie: session=" + cookieSecret + "; HttpOnly",
				},
			}},
		},
		Policies: []model.BackupPolicy{{
			ID:             "policy_public_123",
			BackendID:      "backend_public_123",
			Name:           "hourly",
			DisabledReason: "X-Auth-Token: " + xAuthTokenSecret,
		}},
		Artifacts: []model.BackupArtifact{{
			ID:                "artifact_public_123",
			RunID:             "run_public_123",
			BackendID:         "backend_public_123",
			ObjectKey:         "apps/app_public_123/database.dump",
			ManifestObjectKey: "apps/app_public_123/manifest.json",
			SHA256:            "sha256_public_123",
			Manifest: model.BackupManifest{
				Metadata: map[string]string{
					"receipt":       receiptJSON,
					"receipt_id":    "receipt_public_123",
					"token_id":      "token_record_123",
					"api_key_id":    "api_key_record_123",
					"credential_id": "credential_record_123",
					"callback_url":  "https://status.example/receipts/receipt_public_123",
				},
				Invariants: map[string]string{
					"upload_event": "Authorization: Bearer " + bearerSecret,
				},
			},
		}},
		Posture: []model.BackupPosture{{
			PolicyID: "policy_public_123",
			Status:   "error",
			Message:  "DATABASE_URL=postgresql://backup-user:" + urlPasswordSecret + "@db.example/app",
		}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_public_123","tenant_id":"tenant_public_123","project_id":"project_public_123","name":"demo","spec":{"replicas":1},"status":{"phase":"ready"},"created_at":"2026-07-31T00:00:00Z","updated_at":"2026-07-31T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_public_123/backups/status":
			if err := json.NewEncoder(w).Encode(status); err != nil {
				t.Fatalf("encode status: %v", err)
			}
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"app", "backup", "status", "demo",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("run app backup status: %v stderr=%s", err, stderr.String())
	}
	output := stdout.String()
	for _, leaked := range []string{
		basicSecret,
		bearerSecret,
		xAPIKeySecret,
		xAuthTokenSecret,
		urlPasswordSecret,
		queryTokenSecret,
		passwordSecret,
		awsSecret,
		clientSecret,
		cookieSecret,
		receiptPassword,
		receiptHeaderSecret,
		workspaceResetSecret,
		storageResetSecret,
	} {
		if strings.Contains(output, leaked) {
			t.Fatalf("default backup status JSON leaked %q: %s", leaked, output)
		}
	}
	if strings.Count(output, redactedSecretValue) < 10 {
		t.Fatalf("expected explicit redaction markers across structured and diagnostic fields: %s", output)
	}
	for _, preserved := range []string{
		"app_public_123",
		"runtime_public_123",
		"op_public_123",
		"policy_public_123",
		"backend_public_123",
		"artifact_public_123",
		"run_public_123",
		"apps/app_public_123/database.dump",
		"sha256_public_123",
		"receipt_public_123",
		"token_record_123",
		"tok_public",
		"api_key_record_123",
		"access_key_record_123",
		"credential_record_123",
		"https://status.example/receipts/receipt_public_123",
	} {
		if !strings.Contains(output, preserved) {
			t.Fatalf("backup status redaction removed non-secret identifier %q: %s", preserved, output)
		}
	}
}

func TestBackupStatusDiagnosticRedactionCoversCredentialVariantsWithoutRedactingIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		secret string
	}{
		{name: "authorization basic", input: "Authorization: Basic basic-secret", secret: "basic-secret"},
		{name: "proxy authorization", input: "Proxy-Authorization: Bearer proxy-secret", secret: "proxy-secret"},
		{name: "api key header", input: "X-API-Key: api-secret", secret: "api-secret"},
		{name: "auth token header", input: "X-Auth-Token=auth-secret", secret: "auth-secret"},
		{name: "url userinfo", input: "postgresql://db-user:url-secret@db.example/app", secret: "url-secret"},
		{name: "dsn password", input: "Server=db.example;User Id=backup;Password=dsn-secret;Database=app", secret: "dsn-secret"},
		{name: "aws secret assignment", input: "AWS_SECRET_ACCESS_KEY=aws-secret", secret: "aws-secret"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			redacted := redactDiagnosticString(test.input)
			if strings.Contains(redacted, test.secret) {
				t.Fatalf("credential variant leaked %q in %q", test.secret, redacted)
			}
			if !strings.Contains(redacted, redactedSecretValue) {
				t.Fatalf("credential variant has no redaction marker: %q", redacted)
			}
		})
	}

	structured := `{"receipt_id":"receipt_public_123","token_id":"token_record_123","token_prefix":"tok_public","api_key_id":"api_key_record_123","access_key_id":"access_key_record_123","secret_id":"secret_record_123","credential_id":"credential_record_123","event":"backup.completed","callback_url":"https://status.example/receipts/receipt_public_123","token":"raw-token-secret"}`
	redacted := redactDiagnosticString(structured)
	if strings.Contains(redacted, "raw-token-secret") {
		t.Fatalf("structured token leaked: %s", redacted)
	}
	for _, preserved := range []string{
		"receipt_public_123",
		"token_record_123",
		"tok_public",
		"api_key_record_123",
		"access_key_record_123",
		"secret_record_123",
		"credential_record_123",
		"backup.completed",
		"https://status.example/receipts/receipt_public_123",
	} {
		if !strings.Contains(redacted, preserved) {
			t.Fatalf("structured redaction removed non-secret value %q: %s", preserved, redacted)
		}
	}
}
