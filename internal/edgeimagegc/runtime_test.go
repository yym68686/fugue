package edgeimagegc

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInventoryRequiresAllEvidenceAndFollowsPagination(t *testing.T) {
	calls := 0
	fail := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path == "/apis/apps/v1/controllerrevisions" && fail {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if r.URL.Query().Get("continue") == "" {
			fmt.Fprint(w, `{"metadata":{"continue":"page2"},"items":[{"data":{"record":"{\"image\":\"registry.example/system:rollback\"}"}}]}`)
		} else {
			fmt.Fprint(w, `{"metadata":{},"items":[]}`)
		}
	}))
	defer server.Close()
	runtime := &CommandRuntime{APIURL: server.URL, Client: server.Client(), Namespace: "control", runCRI: func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "images" {
			return []byte(`{"images":[]}`), nil
		}
		return []byte(`{"containers":[]}`), nil
	}}
	evidence, err := runtime.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 18 || !evidence.Protected["registry.example/system:rollback"] {
		t.Fatalf("incomplete history: calls=%d protection=%v", calls, evidence.Protected)
	}
	fail = true
	if _, err := runtime.Observe(context.Background()); err == nil {
		t.Fatal("forbidden history accepted")
	}
	runtime.runCRI = func(context.Context, ...string) ([]byte, error) { return []byte(`{}`), nil }
	if _, err := runtime.Observe(context.Background()); err == nil {
		t.Fatal("missing runtime inventory accepted")
	}
}
