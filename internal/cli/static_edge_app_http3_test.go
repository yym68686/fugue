package cli

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStaticEdgeAppHTTP3OverridePreservesOtherRoutes(t *testing.T) {
	original := map[string]any{"match": []any{map[string]any{"host": []any{"other.example.test"}}}, "handle": []any{map[string]any{"handler": "static_response", "status_code": 200}}}
	server := map[string]any{"listen": []any{":443"}, "routes": []any{original}}
	before, _ := json.Marshal(original)
	staticEdgeAppSetHTTP3Port(server, "new.example.test", 31443)
	routes := server["routes"].([]any)
	after, _ := json.Marshal(routes[1])
	if string(before) != string(after) {
		t.Fatal("other app route changed")
	}
	raw, _ := json.Marshal(routes[0])
	if !strings.Contains(string(raw), "new.example.test") || !strings.Contains(string(raw), "31443") || !strings.Contains(string(raw), "deferred\":true") {
		t.Fatal("wrong public QUIC advertisement", string(raw))
	}
}
