package diagnosticprobe

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"fugue/internal/livediagnostics"
)

func TestObjectWatchStartsFromObservedVersionAndProjectsFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Fatal("unexpected mutation")
		}
		if r.URL.Query().Get("watch") == "true" {
			if r.URL.Query().Get("resourceVersion") != "42" || r.URL.Query().Get("fieldSelector") != "metadata.name=sample" {
				t.Error("watch did not bind observed object/version")
			}
			fmt.Fprintln(w, `{"type":"MODIFIED","object":{"metadata":{"namespace":"ns","name":"sample","resourceVersion":"43"},"status":{"phase":"second"},"spec":{"private":"hidden"}}}`)
		} else {
			fmt.Fprint(w, `{"metadata":{"namespace":"ns","name":"sample","resourceVersion":"42"},"status":{"phase":"first"}}`)
		}
	}))
	defer server.Close()
	k := &kubeReader{client: server.Client(), base: server.URL}
	v, err := k.objectChanges(context.Background(), livediagnostics.ProbeRequest{}, Collector{Resource: "pods", Namespace: "ns", ObjectName: "sample", CaptureSeconds: 3, Fields: []string{"status"}})
	if err != nil {
		t.Fatal(err)
	}
	rows := v.(map[string]any)["events"].([]any)
	if len(rows) != 1 {
		t.Fatal("missing watch event")
	}
	object := rows[0].(map[string]any)["object"].(map[string]any)
	if object["spec"] != nil || object["status"].(map[string]any)["phase"] != "second" {
		t.Fatal("incorrect projection", object)
	}
}
