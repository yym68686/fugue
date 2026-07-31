package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"fugue/internal/model"
)

type testBackupPageInfo struct {
	HasNextPage bool   `json:"has_next_page"`
	Limit       int    `json:"limit"`
	NextCursor  string `json:"next_cursor,omitempty"`
}

func TestBackupRunListLimitAggregatesAcrossServerPages(t *testing.T) {
	t.Parallel()

	const total = 663
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/backups/runs" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		requests++
		cursor := r.URL.Query().Get("cursor")
		limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil {
			http.Error(w, "missing limit", http.StatusBadRequest)
			return
		}
		start := 0
		wantLimit := 500
		if cursor == "runs-page-2" {
			start = 500
			wantLimit = 163
		} else if cursor != "" {
			http.Error(w, "unexpected cursor", http.StatusBadRequest)
			return
		}
		if limit != wantLimit {
			http.Error(w, fmt.Sprintf("limit=%d want=%d", limit, wantLimit), http.StatusBadRequest)
			return
		}
		end := min(start+limit, total)
		runs := make([]model.BackupRun, 0, end-start)
		for i := start; i < end; i++ {
			runs = append(runs, model.BackupRun{ID: fmt.Sprintf("backup_run_%04d", i)})
		}
		page := testBackupPageInfo{Limit: limit, HasNextPage: end < total}
		if page.HasNextPage {
			page.NextCursor = "runs-page-2"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"runs": runs, "page_info": page})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"backup", "run", "ls", "--limit", strconv.Itoa(total),
	}, &stdout, &stderr); err != nil {
		t.Fatalf("list paginated runs: %v stderr=%s", err, stderr.String())
	}
	var payload struct {
		Runs []model.BackupRun `json:"runs"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if len(payload.Runs) != total || requests != 2 {
		t.Fatalf("got runs=%d requests=%d, want runs=%d requests=2", len(payload.Runs), requests, total)
	}
}

func TestBackupArtifactListAllAggregatesEveryServerPage(t *testing.T) {
	t.Parallel()

	const total = 1019
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/backups/artifacts" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		if r.URL.Query().Get("include_deleted") != "true" {
			http.Error(w, "include_deleted filter was not preserved", http.StatusBadRequest)
			return
		}
		requests++
		if r.URL.Query().Get("limit") != "500" {
			http.Error(w, "page limit must be 500", http.StatusBadRequest)
			return
		}
		cursor := r.URL.Query().Get("cursor")
		start := 0
		switch cursor {
		case "":
		case "artifacts-page-2":
			start = 500
		case "artifacts-page-3":
			start = 1000
		default:
			http.Error(w, "unexpected cursor", http.StatusBadRequest)
			return
		}
		end := min(start+500, total)
		artifacts := make([]model.BackupArtifact, 0, end-start)
		for i := start; i < end; i++ {
			artifacts = append(artifacts, model.BackupArtifact{ID: fmt.Sprintf("backup_artifact_%04d", i)})
		}
		page := testBackupPageInfo{Limit: 500, HasNextPage: end < total}
		if end == 500 {
			page.NextCursor = "artifacts-page-2"
		} else if end == 1000 {
			page.NextCursor = "artifacts-page-3"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"artifacts": artifacts, "page_info": page})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"backup", "artifact", "ls", "--all", "--include-deleted",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("list all paginated artifacts: %v stderr=%s", err, stderr.String())
	}
	var payload struct {
		Artifacts []model.BackupArtifact `json:"artifacts"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if len(payload.Artifacts) != total || requests != 3 {
		t.Fatalf("got artifacts=%d requests=%d, want artifacts=%d requests=3", len(payload.Artifacts), requests, total)
	}
}

func TestBackupListFailsLoudlyWhenLegacyServerCannotProveAnotherPage(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("limit") != "500" {
			http.Error(w, "expected first page limit 500", http.StatusBadRequest)
			return
		}
		runs := make([]model.BackupRun, 500)
		for i := range runs {
			runs[i].ID = fmt.Sprintf("legacy_run_%04d", i)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"runs": runs})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"backup", "run", "ls", "--limit", "501",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "omitted backup run pagination metadata") {
		t.Fatalf("expected explicit legacy pagination error, got %v stderr=%s", err, stderr.String())
	}
	if requests != 1 || stdout.Len() != 0 {
		t.Fatalf("legacy pagination requests=%d stdout=%q", requests, stdout.String())
	}
}

func TestBackupListLegacyServerRemainsCompatibleWithinOneRequestedPage(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") != "20" {
			http.Error(w, "expected legacy page limit 20", http.StatusBadRequest)
			return
		}
		artifacts := make([]model.BackupArtifact, 20)
		for i := range artifacts {
			artifacts[i].ID = fmt.Sprintf("legacy_artifact_%02d", i)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"artifacts": artifacts})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"backup", "artifact", "ls",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("legacy one-page list: %v stderr=%s", err, stderr.String())
	}
	var payload struct {
		Artifacts []model.BackupArtifact `json:"artifacts"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if len(payload.Artifacts) != 20 {
		t.Fatalf("legacy one-page artifacts=%d, want 20", len(payload.Artifacts))
	}
}
