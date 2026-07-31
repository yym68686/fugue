package legacysource

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupadapter"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
	"fugue/internal/backupmaterializer/httpapi"
	"fugue/internal/backupmaterializer/localissuer"
	"fugue/internal/backupmaterializeridentity"
	"fugue/internal/backupmaterializeridentity/httpauth"
	"fugue/internal/model"
	"fugue/internal/store"
)

type integrationReviewer struct {
	result backupmaterializeridentity.ReviewResult
}

func (reviewer integrationReviewer) ReviewToken(
	context.Context,
	string,
	[]string,
) (backupmaterializeridentity.ReviewResult, error) {
	return reviewer.result, nil
}

func TestRealJSONStoreSourceToPrivateBundleIsReadOnly(t *testing.T) {
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "materializer-source-integration-encryption-key")
	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("initialize integration store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name: "materializer backend", Provider: model.DataBackendProviderS3,
		Bucket: "private-materializer-bucket", Region: "region-1", Endpoint: "https://s3.example.test", Prefix: "backups",
		Credentials: model.DataBackendCredentials{
			AccessKeyID: "materializer-access-key", SecretAccessKey: "materializer-secret-key", Token: "materializer-session-token",
		},
	})
	if err != nil {
		t.Fatalf("create integration backend: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		ID: "run-materializer-source-1", BackendID: backend.ID, Trigger: model.BackupRunTriggerManual,
		Target: model.BackupTarget{Type: model.BackupTargetRegistry},
	})
	if err != nil {
		t.Fatalf("create integration run: %v", err)
	}
	backendObservation, err := stateStore.GetBackupBackendObservation(backend.ID, "", true)
	if err != nil {
		t.Fatalf("read integration backend generation: %v", err)
	}
	spec, err := backupadapter.BuildShadowSpec(run, backendObservation.Generation)
	if err != nil {
		t.Fatalf("build integration desired spec: %v", err)
	}
	before, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store before materialization: %v", err)
	}

	readSnapshot := func(ctx context.Context, runID string) (Snapshot, error) {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, ErrSnapshotUnavailable
		}
		currentRun, readErr := stateStore.GetBackupRun(runID, "", true)
		if readErr != nil {
			return Snapshot{}, classifyStoreReadError(readErr)
		}
		currentBackend, readErr := stateStore.GetBackupBackendObservation(currentRun.BackendID, "", true)
		if readErr != nil {
			return Snapshot{}, classifyStoreReadError(readErr)
		}
		return Snapshot{Run: currentRun, BackendGeneration: currentBackend.Generation}, nil
	}
	source, err := New(readSnapshot)
	if err != nil {
		t.Fatalf("construct integration source: %v", err)
	}
	keyring := backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
	issuer, err := localissuer.New(keyring)
	if err != nil {
		t.Fatalf("construct integration issuer: %v", err)
	}
	now := time.Date(2026, 7, 30, 15, 30, 0, 0, time.UTC)
	inputHandler, err := httpapi.New(source, issuer, func() time.Time { return now })
	if err != nil {
		t.Fatalf("construct integration input handler: %v", err)
	}
	authenticator, err := httpauth.New(
		integrationReviewer{result: integrationReviewResult(spec.CellKey)},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("construct integration materializer auth: %v", err)
	}
	handler := authenticator.RequireGET(inputHandler)
	request := httptest.NewRequest(
		http.MethodGet,
		strings.Replace(httpapi.RoutePath, "{run}", run.ID, 1),
		nil,
	)
	request.SetPathValue("run", run.ID)
	request.Header.Set("Authorization", "Bearer test-header.test-materializer-payload.test-materializer-signature")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("integration bundle status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	bundle, err := backupmaterializer.DecodeObserverInputBundle(recorder.Body.Bytes(), keyring, now.Add(time.Minute))
	if err != nil || bundle.DesiredSpec != spec || bundle.CellKey != spec.CellKey || bundle.RunID != run.ID {
		t.Fatalf("integration bundle drifted: bundle=%#v err=%v", bundle, err)
	}
	after, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store after materialization: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("materializer source or issuer mutated the legacy JSON store")
	}
	for _, forbidden := range []string{
		"private-materializer-bucket", "https://s3.example.test", "materializer-access-key",
		"materializer-secret-key", "materializer-session-token", "test-materializer-payload",
	} {
		if strings.Contains(recorder.Body.String(), forbidden) {
			t.Fatalf("private bundle boundary leaked %q: %s", forbidden, recorder.Body.String())
		}
	}
	if recorder.Header().Get("Cache-Control") != "private, no-store, max-age=0" || recorder.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("integration bundle response is cacheable: %v", recorder.Header())
	}
}

func classifyStoreReadError(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return ErrSnapshotNotFound
	case errors.Is(err, store.ErrInvalidInput), errors.Is(err, store.ErrConflict):
		return ErrSnapshotConflict
	default:
		return ErrSnapshotUnavailable
	}
}

func integrationReviewResult(cellKey string) backupmaterializeridentity.ReviewResult {
	serviceAccountName := backupmaterializeridentity.ServiceAccountNameForCell(cellKey)
	return backupmaterializeridentity.ReviewResult{
		Authenticated: true,
		Audiences:     []string{backupmaterializeridentity.Audience},
		Username: "system:serviceaccount:" + backupmaterializeridentity.ServiceAccountNamespace + ":" +
			serviceAccountName,
		UID: "11111111-1111-4111-8111-111111111111",
		Groups: []string{
			"system:serviceaccounts",
			"system:serviceaccounts:" + backupmaterializeridentity.ServiceAccountNamespace,
			"system:authenticated",
		},
		Extra: map[string][]string{
			"authentication.kubernetes.io/credential-id": {"JTI=33333333-3333-4333-8333-333333333333"},
			"authentication.kubernetes.io/pod-name":      {serviceAccountName + "-6d4f7c8b9f-abcde"},
			"authentication.kubernetes.io/pod-uid":       {"22222222-2222-4222-8222-222222222222"},
		},
	}
}
