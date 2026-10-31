package objectstorage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBucketInventoryPaginationIsReadOnly(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Fatal("inventory mutated provider")
		}
		calls++
		result := map[string]any{"buckets": []map[string]string{{"name": "bucket-two"}}}
		if r.URL.Query().Get("cursor") == "" {
			result = map[string]any{"buckets": []map[string]string{{"name": "bucket-one"}}, "cursor": "next"}
		}
		json.NewEncoder(w).Encode(map[string]any{"success": true, "result": result})
	}))
	defer server.Close()
	c := New("test-account", "secret")
	c.BaseURL = server.URL
	c.HTTP = server.Client()
	names, err := c.ListBuckets(context.Background())
	if err != nil || len(names) != 2 || calls != 2 {
		t.Fatalf("names %v calls %d err %v", names, calls, err)
	}
}
