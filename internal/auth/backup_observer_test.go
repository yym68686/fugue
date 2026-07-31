package auth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupidentity"
)

const (
	authBackupCell   = "backup/registry/0123456789abcdef"
	authBackupDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func TestRequireBackupObserverBindsOnlyDedicatedGETIdentity(t *testing.T) {
	t.Parallel()
	keyring := backupObserverAuthTestKeyring()
	token, err := backupidentity.Issue(keyring, backupidentity.Claims{
		CredentialID: backupidentity.CredentialIDForCell(authBackupCell),
		RunID:        "run-1", CellKey: authBackupCell, SpecDigest: authBackupDigest,
	}, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("issue backup observer identity: %v", err)
	}
	authenticator := &Authenticator{BackupObserverIdentityKeyring: keyring}
	handler := authenticator.RequireBackupObserver(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, ok := BackupObserverIdentityFromContext(r.Context())
		if !ok || claims.RunID != "run-1" || claims.CellKey != authBackupCell || claims.SpecDigest != authBackupDigest {
			t.Fatalf("verified backup observer claims are missing or drifted: %+v ok=%t", claims, ok)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/backup-control/runs/run-1/observation", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusNoContent, recorder.Code, recorder.Body.String())
	}
	assertBackupObserverNoStoreHeaders(t, recorder)
}

func TestRequireBackupObserverRejectsOtherCredentialsAndMissingKeyring(t *testing.T) {
	t.Parallel()
	for name, authenticator := range map[string]*Authenticator{
		"configured": {BackupObserverIdentityKeyring: backupObserverAuthTestKeyring()},
		"empty":      {},
	} {
		t.Run(name, func(t *testing.T) {
			for _, token := range []string{"tenant-api-key", "fugue_pc_v1.invalid.invalid.invalid", "fugue_wk_invalid", ""} {
				handler := authenticator.RequireBackupObserver(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					t.Fatal("handler ran for a non-backup-observer credential")
				}))
				req := httptest.NewRequest(http.MethodGet, "/v1/backup-control/runs/run-1/observation", nil)
				if token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, req)
				if recorder.Code != http.StatusUnauthorized {
					t.Fatalf("token %q: status=%d body=%s", token, recorder.Code, recorder.Body.String())
				}
				assertBackupObserverNoStoreHeaders(t, recorder)
				var response struct {
					Error string `json:"error"`
					Code  string `json:"code"`
				}
				if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || response.Error != "unauthorized" || response.Code != "auth_required" {
					t.Fatalf("token %q: unexpected error response %+v err=%v", token, response, err)
				}
			}
		})
	}
}

func TestRequireBackupObserverRejectsMutationMethodsBeforeHandler(t *testing.T) {
	t.Parallel()
	keyring := backupObserverAuthTestKeyring()
	token, err := backupidentity.Issue(keyring, backupidentity.Claims{
		CredentialID: backupidentity.CredentialIDForCell(authBackupCell),
		RunID:        "run-1", CellKey: authBackupCell, SpecDigest: authBackupDigest,
	}, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("issue backup observer identity: %v", err)
	}
	handler := (&Authenticator{BackupObserverIdentityKeyring: keyring}).RequireBackupObserver(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("GET-only identity reached a mutation handler")
	}))
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead} {
		req := httptest.NewRequest(method, "/v1/backup-control/runs/run-1/observation", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusMethodNotAllowed || !strings.Contains(recorder.Body.String(), "GET-only") {
			t.Fatalf("method %s: status=%d body=%s", method, recorder.Code, recorder.Body.String())
		}
		assertBackupObserverNoStoreHeaders(t, recorder)
	}
}

func backupObserverAuthTestKeyring() backupidentity.Keyring {
	return backupidentity.DeriveKeyring(strings.Repeat("b", 32), "backup-key-1", "", "", nil)
}

func assertBackupObserverNoStoreHeaders(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Header().Get("Cache-Control") != "private, no-store, max-age=0" || recorder.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("backup observer response is cacheable: %v", recorder.Header())
	}
}
