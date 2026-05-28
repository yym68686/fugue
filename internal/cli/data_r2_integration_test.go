package cli

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/api"
	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestCloudflareR2DataWorkspacePushPullIntegration(t *testing.T) {
	cfg, ok := r2IntegrationConfigFromEnv(t)
	if !ok {
		return
	}
	defer cleanupR2IntegrationPrefix(t, cfg)

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("R2 Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := stateStore.CreateAPIKey(tenant.ID, "data", []string{"data.read", "data.write", "data.delete", "data.grant", "data.admin"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	backend, err := stateStore.CreateDataBackend(model.DataBackend{
		TenantID: tenant.ID,
		Name:     "r2-integration",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   cfg.bucket,
		Region:   "auto",
		Endpoint: cfg.endpoint,
		Prefix:   cfg.prefix,
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     cfg.accessKeyID,
			SecretAccessKey: cfg.secretAccessKey,
			Token:           cfg.sessionToken,
		},
		Capabilities: model.DataBackendCapabilitiesForProvider(model.DataBackendProviderCloudflareR2),
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	server := api.NewServer(stateStore, auth.New(stateStore, ""), nil, api.ServerConfig{})
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	sourceDir := filepath.Join(t.TempDir(), "r2-training-project")
	if err := os.MkdirAll(filepath.Join(sourceDir, "data"), 0o755); err != nil {
		t.Fatalf("create source data dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "data", "sample.txt"), []byte("training-data-r2-e2e"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "track", "./data")
	_, err = stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID:         tenant.ID,
		Name:             "r2-training-project",
		StorageBackendID: backend.ID,
		Assets:           []model.DataAsset{{Name: "data", Path: "./data"}},
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	pushOut := runDataCLIInDir(t, sourceDir, "--base-url", httpServer.URL, "--token", secret, "data", "push", "--version", "r2-e2e")
	if !strings.Contains(pushOut, "r2-e2e") {
		t.Fatalf("expected push output to mention r2-e2e, got %s", pushOut)
	}

	targetDir := filepath.Join(t.TempDir(), "r2-target")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("create target dir: %v", err)
	}
	runDataCLIInDir(t, targetDir, "--base-url", httpServer.URL, "--token", secret, "data", "workspace", "use", "r2-training-project")
	pullOut := runDataCLIInDir(t, targetDir, "--base-url", httpServer.URL, "--token", secret, "data", "pull")
	if !strings.Contains(pullOut, "Restored version r2-e2e") {
		t.Fatalf("expected pull output to mention restored r2-e2e, got %s", pullOut)
	}
	restored, err := os.ReadFile(filepath.Join(targetDir, "data", "sample.txt"))
	if err != nil {
		t.Fatalf("read restored R2 file: %v", err)
	}
	if string(restored) != "training-data-r2-e2e" {
		t.Fatalf("unexpected restored R2 content %q", string(restored))
	}
}

type r2IntegrationConfig struct {
	bucket          string
	endpoint        string
	prefix          string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

func r2IntegrationConfigFromEnv(t *testing.T) (r2IntegrationConfig, bool) {
	t.Helper()
	if os.Getenv("FUGUE_DATA_INTEGRATION_R2") != "1" {
		t.Skip("set FUGUE_DATA_INTEGRATION_R2=1 with R2 backend env vars to run")
		return r2IntegrationConfig{}, false
	}
	bucket := strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_BUCKET"))
	accessKey := strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_ACCESS_KEY_ID"))
	secretKey := strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY"))
	endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_ENDPOINT")), "/")
	if endpoint == "" {
		accountID := strings.TrimSpace(os.Getenv("FUGUE_DATA_R2_ACCOUNT_ID"))
		if accountID != "" {
			endpoint = fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)
		}
	}
	if bucket == "" || accessKey == "" || secretKey == "" || endpoint == "" {
		t.Skip("R2 integration requires bucket, access key, secret key, and account id or endpoint")
		return r2IntegrationConfig{}, false
	}
	prefix := strings.Trim(strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_PREFIX")), "/")
	prefix = path.Join(prefix, "integration", fmt.Sprintf("cli-e2e-%d", time.Now().UnixNano()))
	return r2IntegrationConfig{
		bucket:          bucket,
		endpoint:        endpoint,
		prefix:          prefix,
		accessKeyID:     accessKey,
		secretAccessKey: secretKey,
		sessionToken:    strings.TrimSpace(os.Getenv("FUGUE_DATA_BACKEND_SESSION_TOKEN")),
	}, true
}

func cleanupR2IntegrationPrefix(t *testing.T, cfg r2IntegrationConfig) {
	t.Helper()
	client := s3.New(s3.Options{
		Region:       "auto",
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.accessKeyID, cfg.secretAccessKey, cfg.sessionToken),
		UsePathStyle: true,
		BaseEndpoint: aws.String(cfg.endpoint),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var token *string
	for {
		resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(cfg.bucket), Prefix: aws.String(strings.Trim(cfg.prefix, "/") + "/"), ContinuationToken: token})
		if err != nil {
			t.Logf("cleanup R2 prefix list failed: %v", err)
			return
		}
		if len(resp.Contents) > 0 {
			objects := make([]types.ObjectIdentifier, 0, len(resp.Contents))
			for _, obj := range resp.Contents {
				objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
			}
			if _, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(cfg.bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)}}); err != nil {
				t.Logf("cleanup R2 prefix delete failed: %v", err)
				return
			}
		}
		if !aws.ToBool(resp.IsTruncated) {
			return
		}
		token = resp.NextContinuationToken
	}
}
