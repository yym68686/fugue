package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNodeUpdateTaskShowUsesExactBoundedLookup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/node-update-tasks" || r.URL.Query().Get("task_id") != "task-sample" || r.URL.Query().Get("limit") != "1" || r.URL.Query().Get("details") != "true" {
			t.Errorf("unexpected lookup: %s %s", r.Method, r.URL)
		}
		_, _ = w.Write([]byte(`{"tasks":[{"id":"task-sample","status":"completed"}]}`))
	}))
	defer server.Close()
	var out, errout bytes.Buffer
	if err := runWithStreams([]string{"--base-url", server.URL, "--token", "test", "admin", "node-updater", "task", "show", "task-sample", "--json"}, &out, &errout); err != nil {
		t.Fatalf("%v %s", err, errout.String())
	}
}
