package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestBackupRunWaitPollsInsteadOfServerWait(t *testing.T) {
	var mu sync.Mutex
	postSeen := false
	getCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backups/runs":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode backup run request: %v", err)
			}
			if body["wait"] != false {
				t.Fatalf("expected CLI wait to send wait=false to API, got %+v", body)
			}
			mu.Lock()
			postSeen = true
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"run":{"id":"backup_run_test","target":{"type":"control-plane-db"},"status":"pending","trigger":"manual","created_at":"2026-06-13T12:00:00Z","updated_at":"2026-06-13T12:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backups/runs/backup_run_test":
			mu.Lock()
			getCount++
			mu.Unlock()
			_, _ = w.Write([]byte(`{"run":{"id":"backup_run_test","target":{"type":"control-plane-db"},"status":"succeeded","trigger":"manual","bytes_written":123,"artifact_count":1,"created_at":"2026-06-13T12:00:00Z","updated_at":"2026-06-13T12:00:02Z","finished_at":"2026-06-13T12:00:02Z"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--color", "never",
		"admin", "backup", "run", "--wait",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin backup --wait: %v stderr=%s", err, stderr.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if !postSeen {
		t.Fatal("expected backup run POST")
	}
	if getCount == 0 {
		t.Fatal("expected CLI to poll backup run")
	}
	if !bytes.Contains(stdout.Bytes(), []byte("status: succeeded")) {
		t.Fatalf("expected succeeded run output, got %s", stdout.String())
	}
}
