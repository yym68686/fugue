package fuguechart_test

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"text/template"
)

// Exercise the actual online-lookup guard without requiring a live API server.
// The remaining chart rendering is covered by TestControlPlanePostgres*.
func TestOnlineChartCannotReenableExplicitlyDisabledBarman(t *testing.T) {
	source, err := os.ReadFile("templates/control-plane-postgres-cluster.yaml")
	if err != nil {
		t.Fatal(err)
	}
	guard, _, ok := strings.Cut(string(source), "{{- if lt")
	if !ok {
		t.Fatal("cannot locate database validation boundary")
	}
	for _, disabled := range []bool{false, true} {
		for _, backupEnabled := range []bool{false, true} {
			live := map[string]any{}
			if disabled {
				live["metadata"] = map[string]any{"annotations": map[string]any{"fugue.pro/barman-disabled": "policy-uid"}}
			}
			funcs := template.FuncMap{
				"lookup":  func(...string) map[string]any { return live },
				"include": func(string, any) string { return "sample-db" },
				"dict":    func() map[string]any { return map[string]any{} },
				"default": func(fallback, value any) any {
					if value == nil {
						return fallback
					}
					return value
				},
				"get": func(m map[string]any, key string) any { return m[key] },
				"dig": func(a, b string, fallback any, m map[string]any) any {
					if parent, ok := m[a].(map[string]any); ok && parent[b] != nil {
						return parent[b]
					}
					return fallback
				},
				"fail": func(message string) (string, error) { return "", fmt.Errorf("%s", message) },
			}
			tmpl, err := template.New("guard").Funcs(funcs).Parse(guard + "{{- end }}")
			if err != nil {
				t.Fatal(err)
			}
			data := map[string]any{"Release": map[string]any{"Namespace": "platform-test"}, "Values": map[string]any{"controlPlanePostgres": map[string]any{"enabled": true, "backup": map[string]any{"enabled": backupEnabled}}}}
			var output bytes.Buffer
			err = tmpl.Execute(&output, data)
			if (err != nil) != (disabled && backupEnabled) {
				t.Fatalf("disabled=%v backupEnabled=%v: unexpected guard result: %v", disabled, backupEnabled, err)
			}
			if err != nil && !strings.Contains(err.Error(), "explicitly disabled") {
				t.Fatal(err)
			}
		}
	}
}
