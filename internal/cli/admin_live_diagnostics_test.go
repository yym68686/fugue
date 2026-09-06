package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUnifiedDiagnosticsReplacesAdminEntryPoint(t *testing.T) {
	t.Parallel()
	root := newCLI(io.Discard, io.Discard).newRootCommand()
	admin, _, err := root.Find([]string{"admin"})
	if err != nil {
		t.Fatal(err)
	}
	for _, cmd := range admin.Commands() {
		if cmd.Name() == "diagnostics" {
			t.Fatal("removed admin diagnostics command is still registered")
		}
	}
	for _, target := range []string{"app", "platform", "node-process"} {
		for _, action := range []string{"start", "list", "show", "report", "cancel"} {
			cmd, remaining, err := root.Find([]string{"diagnostics", target, action})
			if err != nil || len(remaining) != 0 || cmd.CommandPath() != "fugue diagnostics "+target+" "+action {
				t.Fatalf("missing %s %s: command=%v remaining=%v err=%v", target, action, cmd, remaining, err)
			}
			for _, flag := range []string{"direct-kubernetes", "kubeconfig", "kube-context", "control-namespace", "release-instance"} {
				present := cmd.InheritedFlags().Lookup(flag) != nil
				if present != (target != "app") {
					t.Errorf("%s %s flag %s present=%v", target, action, flag, present)
				}
			}
		}
	}
}

func TestUnifiedDiagnosticsPlatformStartSendsPlatformMemoryProbe(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != platformDiagnosticsPath {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var request platformDiagnosticStartRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.Target.Type != "platform_component" || request.Target.Component != "api" || request.Kind != "memory-profile" || request.DurationSeconds != 300 || request.SampleIntervalMilliseconds != 500 {
			t.Fatalf("unexpected request: %+v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"session":{"id":"diagnostic-test","kind":"memory-profile","status":"queued","target":{"type":"platform_component","component":"api","namespace":"fugue-system","pod":"api-1","container":"api","node":"node-1"},"control_path":"api","duration_seconds":300,"frequency_hz":19,"sample_interval_milliseconds":500,"created_at":"2026-09-05T00:00:00Z"}}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL, "--token", "bootstrap", "--json", "diagnostics", "platform", "start",
		"--component", "api", "--kind", "memory-profile", "--duration", "300", "--sample-interval-ms", "500",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run diagnostics start: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"control_path": "api"`) {
		t.Fatalf("unexpected output: %s", stdout.String())
	}
}
