package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestBackupRunListCursorPaginationIsStableAndFilterBound(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	base := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 6; i++ {
		createdAt := base.Add(time.Duration(i/2) * time.Minute)
		if _, err := stateStore.CreateBackupRun(model.BackupRun{
			ID:        fmt.Sprintf("backup_run_page_%02d", i),
			Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
			Trigger:   model.BackupRunTriggerManual,
			Status:    model.BackupRunStatusPending,
			CreatedAt: createdAt,
		}); err != nil {
			t.Fatalf("create run %d: %v", i, err)
		}
	}

	type pageInfo struct {
		HasNextPage bool   `json:"has_next_page"`
		Limit       int    `json:"limit"`
		NextCursor  string `json:"next_cursor"`
	}
	type response struct {
		Runs     []model.BackupRun `json:"runs"`
		PageInfo pageInfo          `json:"page_info"`
	}

	seen := make(map[string]struct{})
	cursor := ""
	firstCursor := ""
	for {
		target := "/v1/backups/runs?limit=2&status=blocked"
		if cursor != "" {
			target += "&cursor=" + url.QueryEscape(cursor)
		}
		recorder := performJSONRequest(t, server, http.MethodGet, target, "bootstrap-secret", nil)
		if recorder.Code != http.StatusOK {
			t.Fatalf("list page status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		var payload response
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode page: %v", err)
		}
		if payload.PageInfo.Limit != 2 {
			t.Fatalf("unexpected page limit: %+v", payload.PageInfo)
		}
		if len(payload.Runs) > 2 {
			t.Fatalf("page exceeded limit: %d", len(payload.Runs))
		}
		for _, run := range payload.Runs {
			if _, duplicate := seen[run.ID]; duplicate {
				t.Fatalf("duplicate run %s", run.ID)
			}
			seen[run.ID] = struct{}{}
		}
		if !payload.PageInfo.HasNextPage {
			break
		}
		if payload.PageInfo.NextCursor == "" {
			t.Fatal("has_next_page without next_cursor")
		}
		if firstCursor == "" {
			firstCursor = payload.PageInfo.NextCursor
		}
		cursor = payload.PageInfo.NextCursor
	}
	if len(seen) != 6 {
		t.Fatalf("pagination omitted runs: got %d", len(seen))
	}

	changedFilter := performJSONRequest(t, server, http.MethodGet,
		"/v1/backups/runs?limit=2&status=failed&cursor="+url.QueryEscape(firstCursor),
		"bootstrap-secret", nil)
	if changedFilter.Code != http.StatusBadRequest {
		t.Fatalf("filter-bound cursor status=%d body=%s", changedFilter.Code, changedFilter.Body.String())
	}
}

func TestBackupListsRejectOversizedServerPageInsteadOfSilentlyTruncating(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	for _, path := range []string{
		"/v1/backups/runs?limit=501",
		"/v1/backups/artifacts?limit=501",
	} {
		recorder := performJSONRequest(t, server, http.MethodGet, path, "bootstrap-secret", nil)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("expected %s to return 400, got %d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestBackupArtifactListCursorPaginationReturnsEveryArtifact(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	base := time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		if _, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
			ID: fmt.Sprintf("backup_artifact_page_%02d", i),
			Target: model.BackupTarget{
				Type: model.BackupTargetControlPlaneDatabase,
			},
			Kind:      model.BackupArtifactKindControlPlanePGDump,
			Status:    model.BackupArtifactStatusActive,
			CreatedAt: base.Add(time.Duration(i/2) * time.Minute),
		}); err != nil {
			t.Fatalf("create artifact %d: %v", i, err)
		}
	}

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	seen := make(map[string]struct{})
	cursor := ""
	for {
		target := "/v1/backups/artifacts?limit=2"
		if cursor != "" {
			target += "&cursor=" + url.QueryEscape(cursor)
		}
		recorder := performJSONRequest(t, server, http.MethodGet, target, "bootstrap-secret", nil)
		if recorder.Code != http.StatusOK {
			t.Fatalf("list artifact page status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		var payload struct {
			Artifacts []model.BackupArtifact `json:"artifacts"`
			PageInfo struct {
				HasNextPage bool   `json:"has_next_page"`
				NextCursor  string `json:"next_cursor"`
			} `json:"page_info"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode artifact page: %v", err)
		}
		for _, artifact := range payload.Artifacts {
			if _, duplicate := seen[artifact.ID]; duplicate {
				t.Fatalf("duplicate artifact %s", artifact.ID)
			}
			seen[artifact.ID] = struct{}{}
		}
		if !payload.PageInfo.HasNextPage {
			break
		}
		if payload.PageInfo.NextCursor == "" {
			t.Fatal("artifact page omitted next cursor")
		}
		cursor = payload.PageInfo.NextCursor
	}
	if len(seen) != 5 {
		t.Fatalf("pagination omitted artifacts: got %d", len(seen))
	}
}
