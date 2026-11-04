package objectstorage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestArchiveProtectionRefusesExternalPolicyAndVerifiesLock(t *testing.T) {
	external := true
	locked := false
	mutations := 0
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rules := []any{}
		if strings.HasSuffix(r.URL.Path, "/lifecycle") {
			if external {
				rules = append(rules, map[string]any{"id": "external", "enabled": true})
			} else {
				rules = append(rules, map[string]any{"id": "abort-incomplete", "enabled": true, "conditions": map[string]string{"prefix": ""}, "abortMultipartUploadsTransition": map[string]any{"condition": map[string]any{"type": "Age", "maxAge": 604800}}})
			}
		} else {
			if r.Method == http.MethodPut {
				mutations++
				locked = true
			}
			if locked {
				rules = append(rules, map[string]any{"id": archiveLockRule, "enabled": true, "prefix": "", "condition": map[string]string{"type": "Indefinite"}})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "result": map[string]any{"rules": rules}})
	}))
	defer fake.Close()
	c := New(strings.Repeat("a", 32), "synthetic-token")
	c.BaseURL = fake.URL
	c.HTTP = fake.Client()
	if _, err := c.ProtectArchiveBucket(context.Background(), "dedicated-archive"); err == nil || mutations != 0 {
		t.Fatal("external lifecycle modified")
	}
	external = false
	first, err := c.ProtectArchiveBucket(context.Background(), "dedicated-archive")
	if err != nil || len(first) != 64 || mutations != 1 {
		t.Fatal("protection not verified", err)
	}
	second, err := c.ProtectArchiveBucket(context.Background(), "dedicated-archive")
	if err != nil || first != second || mutations != 1 {
		t.Fatal("idempotent protection changed", err)
	}
}
