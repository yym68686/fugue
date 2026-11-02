package cli

import "fmt"

// A separate serving generation may own TCP/443 and a different public QUIC
// listener. Never change another hostname's advertisement or the listeners.
func staticEdgeAppSetHTTP3Port(server map[string]any, hostname string, port int) {
	routes, _ := server["routes"].([]any)
	route := map[string]any{"match": []any{map[string]any{"host": []any{hostname}}}, "handle": []any{map[string]any{"handler": "headers", "response": map[string]any{"deferred": true, "set": map[string]any{"Alt-Svc": []any{fmt.Sprintf("h3=\":%d\"; ma=86400", port)}}}}}}
	server["routes"] = append([]any{route}, routes...)
}
