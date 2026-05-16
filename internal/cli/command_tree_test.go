package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRootHelpListsSemanticCommands(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{"--help"}, &stdout, &stderr); err != nil {
		t.Fatalf("run help: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"Fugue is a semantic CLI over the Fugue control-plane API.",
		"curl -fsSL https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.sh | sh",
		"https://fugue.pro/app/api-keys",
		"https://app.example.com/app/api-keys",
		"Use a tenant API key for normal deploys. Use a platform-admin/bootstrap key only for admin commands.",
		"export FUGUE_API_KEY=<copied-access-key>",
		"Base URL defaults to FUGUE_BASE_URL, then FUGUE_API_URL, then https://api.fugue.pro.",
		"Web Base URL defaults to FUGUE_WEB_BASE_URL, then APP_BASE_URL, then a best-effort guess from the API base URL.",
		"Tenant is auto-selected when your key only sees one tenant.",
		"Deploy and create flows default to the \"default\" project when you do not pass --project.",
		"App and operation JSON output redacts secrets by default. Pass --show-secrets only when you explicitly need raw values.",
		"Pass --json as a shortcut for --output json, and use --output-file to mirror stdout into a local file.",
		"Diagnostic commands redact sensitive values by default. Pass --redact=false together with --confirm-raw-output only when you explicitly need unredacted evidence.",
		"FUGUE_SKIP_UPDATE_CHECK",
		"Shortcut for --output json",
		"Also write stdout output to a local file",
		"Redact sensitive values in diagnostic output",
		"Required together with --redact=false to allow unredacted output",
		"Show the Fugue CLI build version",
		"deploy",
		"app",
		"tenant",
		"project",
		"runtime",
		"service",
		"workflow",
		"logs",
		"debug",
		"source-upload",
		"version",
		"upgrade",
		"api",
		"diagnose",
		"web",
		"operation",
		"admin",
		"deploy inspect .",
		"deploy github owner/repo",
		"fugue app overview my-app",
		"fugue app source show my-app",
		"fugue source-upload show upload_123",
		"fugue app failover policy set my-app --app-to runtime-b",
		"fugue app service attach my-app postgres",
		"fugue app deploy my-app",
		"fugue app build my-app",
		"fugue app command set my-app --command \"python app.py\"",
		"fugue app config put my-app /app/config.yaml --from-file config.yaml",
		"fugue app storage set my-app --size 10Gi --mount /data",
		"fugue app domain primary set my-app www.example.com",
		"fugue service postgres create app-db --runtime shared",
		"fugue version --check-latest",
		"fugue upgrade",
		"fugue operation ls --app my-app",
		"fugue operation show op_123 --show-secrets",
		"fugue runtime doctor shared",
		"fugue admin runtime access show shared",
		"fugue admin node-updater task ls --status pending",
		"fugue admin discovery bundle show",
		"fugue project overview",
		"fugue project images usage",
		"fugue admin cluster status",
		"fugue admin cluster pods --namespace kube-system",
		"fugue admin cluster workload show kube-system deployment coredns",
		"fugue admin cluster dns resolve api.github.com --server 10.43.0.10",
		"fugue admin cluster net websocket my-app --path /ws",
		"fugue deploy github owner/repo --service-env-file gateway=.env.gateway --service-env-file runtime=.env.runtime",
		"fugue tenant ls",
		"fugue api request GET /v1/apps",
		"fugue workflow run ./signup.yaml --json",
		"fugue diagnose fs my-app --path /workspace/data --json",
		"fugue logs collect my-app --request-id req_123 --since 30m --json",
		"fugue logs query my-app --request-id req_123 --since 30m --status 200 --json",
		"fugue debug bundle my-app --request-id req_123 --archive ./bundle.zip --json",
		"fugue diagnose timing -- app overview my-app",
		"fugue admin users ls",
		"fugue web diagnose admin-users",
		"Use fugue CLI and the current FUGUE_API_KEY to deploy this project.",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected help output to contain %q, got %q", want, out)
		}
	}
	for _, unwanted := range []string{
		"\n  template      ",
		"fugue app continuity enable my-app --app-to runtime-b",
		"fugue app redeploy my-app",
		"fugue admin runtime access shared",
		"fugue project usage",
	} {
		if strings.Contains(out, unwanted) {
			t.Fatalf("expected help output to omit %q, got %q", unwanted, out)
		}
	}
}

func TestInvestigationHelpDocsDescribeNewWorkflows(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "workflow group",
			args: []string{"workflow", "--help"},
			want: []string{
				"Run declarative HTTP investigation workflows with step-to-step extraction, per-step timing, and stable machine-readable output.",
				"fugue workflow run ./signup.yaml --json",
				"./workflow-result.json",
			},
		},
		{
			name: "workflow run",
			args: []string{"workflow", "run", "--help"},
			want: []string{
				"Execute one YAML or JSON workflow file and emit request summaries, response summaries, extracted variables, status checks, and failure classification for every step.",
				"fugue workflow run ./signup.yaml --output-file ./signup-run.json",
				"--confirm-raw-output",
			},
		},
		{
			name: "logs group",
			args: []string{"logs", "--help"},
			want: []string{
				"Collect correlated investigation evidence without dropping into separate runtime, builder, or control-plane log tools.",
				"fugue logs collect my-app --request-id req_123 --since 30m --json",
				"./evidence.json",
			},
		},
		{
			name: "logs collect",
			args: []string{"logs", "collect", "--help"},
			want: []string{
				"Collect workload, build, and control-plane log fragments plus an app/operation timeline into one correlated evidence document.",
				"--workflow-file",
				"fugue logs collect my-app --operation op_deploy_123 --workflow-file ./signup.yaml --output-file ./evidence.json",
			},
		},
		{
			name: "logs query",
			args: []string{"logs", "query", "--help"},
			want: []string{
				"Query runtime log entries through the app runtime log stream and normalize them into stable machine-readable fields.",
				"--request-id",
				"fugue logs query my-app --request-id req_123 --since 30m --json",
			},
		},
		{
			name: "debug group",
			args: []string{"debug", "--help"},
			want: []string{
				"Export shareable investigation bundles and bundle-friendly evidence manifests.",
				"fugue debug bundle my-app --request-id req_123 --archive ./bundle.zip --json",
			},
		},
		{
			name: "debug bundle",
			args: []string{"debug", "bundle", "--help"},
			want: []string{
				"Create a single zip archive that contains the collected evidence JSON, timeline, snapshots, warnings, and per-source log files for one app investigation.",
				"--archive",
				"./bundle-manifest.json",
			},
		},
		{
			name: "diagnose group",
			args: []string{"diagnose", "--help"},
			want: []string{
				"Run higher-level troubleshooting workflows that combine multiple low-level probes into one diagnosis.",
				"fugue diagnose timing -- app overview my-app",
				"fugue diagnose fs my-app --path /workspace/data --json",
			},
		},
		{
			name: "diagnose fs",
			args: []string{"diagnose", "fs", "--help"},
			want: []string{
				"Diagnose app filesystem failures by combining app phase, runtime pod selection, container readiness, raw exec errors, recent events, and related log evidence into one report.",
				"--path",
				"fugue diagnose fs my-app --source persistent --path data --json",
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if err := runWithStreams(tc.args, &stdout, &stderr); err != nil {
				t.Fatalf("run help %v: %v", tc.args, err)
			}

			out := stdout.String()
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Fatalf("expected help output for %v to contain %q, got %q", tc.args, want, out)
				}
			}
		})
	}
}

func TestVisibleCommandsHaveLongAndExamples(t *testing.T) {
	t.Parallel()

	root := newCLI(io.Discard, io.Discard).newRootCommand()
	missing := undocumentedCommandsReport(root)
	if len(missing) != 0 {
		t.Fatalf("expected help docs for all visible commands:\n%s", strings.Join(missing, "\n"))
	}
}

func TestRunTenantListShowsVisibleTenants(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/tenants" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme","status":"active","updated_at":"2026-04-02T00:00:00Z"},{"id":"tenant_999","name":"Beta","slug":"beta","status":"active","updated_at":"2026-04-03T00:00:00Z"}]}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"tenant", "ls",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run tenant ls: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"TENANT", "SLUG", "Acme", "acme", "Beta", "beta"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunDeployImageSupportsIntentFlags(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("port: 8080\n"), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	secretPath := filepath.Join(t.TempDir(), "app.env")
	if err := os.WriteFile(secretPath, []byte("TOKEN=secret\n"), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	settingsPath := filepath.Join(t.TempDir(), "settings.yaml")
	if err := os.WriteFile(settingsPath, []byte("theme: prod\n"), 0o644); err != nil {
		t.Fatalf("write settings file: %v", err)
	}

	var gotRequest importImageRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-image":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode image import request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","name":"demo"},"operation":{"id":"op_123"}}`))
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
		"deploy", "image", "ghcr.io/example/demo:latest",
		"--name", "demo",
		"--background",
		"--command", "python app.py",
		"--file", "/app/config.yaml=" + configPath,
		"--secret-file", "/app/.env:600=" + secretPath,
		"--storage-mode", "movable_rwo",
		"--storage-size", "20Gi",
		"--storage-class", "fast",
		"--mount", "/data",
		"--mount-file", "/app/settings.yaml=" + settingsPath,
		"--managed-postgres",
		"--postgres-database", "appdb",
		"--postgres-user", "app_user",
		"--postgres-password", "secret",
		"--postgres-storage-size", "5Gi",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy image: %v", err)
	}

	if gotRequest.NetworkMode != model.AppNetworkModeBackground {
		t.Fatalf("expected background network mode, got %+v", gotRequest)
	}
	if gotRequest.StartupCommand == nil || *gotRequest.StartupCommand != "python app.py" {
		t.Fatalf("expected startup command to be forwarded, got %+v", gotRequest.StartupCommand)
	}
	if len(gotRequest.Files) != 2 {
		t.Fatalf("expected two declarative files, got %+v", gotRequest.Files)
	}
	fileByPath := map[string]model.AppFile{}
	for _, appFile := range gotRequest.Files {
		fileByPath[appFile.Path] = appFile
	}
	if fileByPath["/app/config.yaml"].Content != "port: 8080\n" || fileByPath["/app/config.yaml"].Secret {
		t.Fatalf("unexpected config file payload %+v", fileByPath["/app/config.yaml"])
	}
	if fileByPath["/app/.env"].Content != "TOKEN=secret\n" || !fileByPath["/app/.env"].Secret || fileByPath["/app/.env"].Mode != 0o600 {
		t.Fatalf("unexpected secret file payload %+v", fileByPath["/app/.env"])
	}
	if gotRequest.PersistentStorage == nil || gotRequest.PersistentStorage.Mode != model.AppPersistentStorageModeMovableRWO || gotRequest.PersistentStorage.StorageSize != "20Gi" || gotRequest.PersistentStorage.StorageClassName != "fast" {
		t.Fatalf("unexpected persistent storage payload %+v", gotRequest.PersistentStorage)
	}
	if len(gotRequest.PersistentStorage.Mounts) != 2 {
		t.Fatalf("expected two persistent storage mounts, got %+v", gotRequest.PersistentStorage.Mounts)
	}
	mountByPath := map[string]model.AppPersistentStorageMount{}
	for _, mount := range gotRequest.PersistentStorage.Mounts {
		mountByPath[mount.Path] = mount
	}
	if mountByPath["/data"].Kind != model.AppPersistentStorageMountKindDirectory {
		t.Fatalf("expected /data directory mount, got %+v", mountByPath["/data"])
	}
	if mountByPath["/app/settings.yaml"].Kind != model.AppPersistentStorageMountKindFile || mountByPath["/app/settings.yaml"].SeedContent != "theme: prod\n" {
		t.Fatalf("unexpected file mount %+v", mountByPath["/app/settings.yaml"])
	}
	if gotRequest.Postgres == nil || gotRequest.Postgres.Database != "appdb" || gotRequest.Postgres.User != "app_user" || gotRequest.Postgres.Password != "secret" || gotRequest.Postgres.StorageSize != "5Gi" {
		t.Fatalf("unexpected managed postgres payload %+v", gotRequest.Postgres)
	}
	if got := stdout.String(); got != "app_id=app_123\noperation_id=op_123\n" {
		t.Fatalf("unexpected stdout %q", got)
	}
}

func TestRunAppCommandSetUsesStartupCommandPatch(t *testing.T) {
	t.Parallel()

	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_123":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode app patch body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "command", "set", "demo",
		"--command", "python app.py",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app command set: %v", err)
	}

	if gotBody["startup_command"] != "python app.py" {
		t.Fatalf("expected startup_command patch, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "startup_command=python app.py"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppCommandSetArgsUsesDeploySpec(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		Spec model.AppSpec `json:"spec"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/deploy":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode deploy body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "command", "set", "demo",
		"--command", "python app.py",
		"--arg=--reload",
		"--arg", "8080",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app command set args: %v", err)
	}

	if strings.Join(gotBody.Spec.Command, " ") != "sh -lc python app.py" {
		t.Fatalf("expected startup command in deploy spec, got %+v", gotBody.Spec.Command)
	}
	if strings.Join(gotBody.Spec.Args, " ") != "--reload 8080" {
		t.Fatalf("expected args in deploy spec, got %+v", gotBody.Spec.Args)
	}
	if out := stdout.String(); !strings.Contains(out, "args=--reload 8080") {
		t.Fatalf("expected stdout to include args, got %q", out)
	}
}

func TestRunAppNetworkSetResolvesPeerAppNames(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		Spec model.AppSpec `json:"spec"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[
				{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},
				{"id":"app_api","tenant_id":"tenant_123","project_id":"project_123","name":"api","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}
			]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/deploy":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode deploy body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "network", "set", "demo",
		"--mode", "internal",
		"--egress-dns", "off",
		"--egress-allow-app", "api:443",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app network set: %v", err)
	}

	if gotBody.Spec.NetworkMode != model.AppNetworkModeInternal {
		t.Fatalf("expected internal network mode, got %q", gotBody.Spec.NetworkMode)
	}
	if gotBody.Spec.NetworkPolicy == nil || gotBody.Spec.NetworkPolicy.Egress == nil {
		t.Fatalf("expected egress policy, got %+v", gotBody.Spec.NetworkPolicy)
	}
	egress := gotBody.Spec.NetworkPolicy.Egress
	if egress.AllowDNS {
		t.Fatalf("expected egress DNS disabled, got %+v", egress)
	}
	if len(egress.AllowApps) != 1 || egress.AllowApps[0].AppID != "app_api" || len(egress.AllowApps[0].Ports) != 1 || egress.AllowApps[0].Ports[0] != 443 {
		t.Fatalf("expected resolved peer app id and port, got %+v", egress.AllowApps)
	}
}

func TestRunEnvGeneratedSetUsesDeploySpec(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		Spec model.AppSpec `json:"spec"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/deploy":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode deploy body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "env", "generated", "set", "demo", "SESSION_SECRET",
		"--encoding", "hex",
		"--length", "48",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run env generated set: %v", err)
	}

	spec, ok := gotBody.Spec.GeneratedEnv["SESSION_SECRET"]
	if !ok {
		t.Fatalf("expected generated env spec, got %+v", gotBody.Spec.GeneratedEnv)
	}
	if spec.Generate != model.AppGeneratedEnvGenerateRandom || spec.Encoding != model.AppGeneratedEnvEncodingHex || spec.Length != 48 {
		t.Fatalf("unexpected generated env spec %+v", spec)
	}
}

func TestRunAppStorageSetBuildsPersistentStorageSpec(t *testing.T) {
	t.Parallel()

	settingsPath := filepath.Join(t.TempDir(), "settings.yaml")
	if err := os.WriteFile(settingsPath, []byte("theme: prod\n"), 0o644); err != nil {
		t.Fatalf("write settings file: %v", err)
	}

	var gotBody struct {
		Spec model.AppSpec `json:"spec"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"persistent_storage":{"mode":"shared_project_rwx","storage_class_name":"fugue-rwx","shared_sub_path":"sessions/demo","mounts":[{"kind":"directory","path":"/workspace","mode":493}]}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/deploy":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode deploy body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "storage", "set", "demo",
		"--mode", "movable_rwo",
		"--size", "20Gi",
		"--class", "fast",
		"--mount", "/data",
		"--mount-file", "/app/settings.yaml=" + settingsPath,
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app storage set: %v", err)
	}

	if gotBody.Spec.Workspace != nil {
		t.Fatalf("expected workspace to be cleared, got %+v", gotBody.Spec.Workspace)
	}
	if gotBody.Spec.PersistentStorage == nil || gotBody.Spec.PersistentStorage.Mode != model.AppPersistentStorageModeMovableRWO || gotBody.Spec.PersistentStorage.StorageSize != "20Gi" || gotBody.Spec.PersistentStorage.StorageClassName != "fast" {
		t.Fatalf("unexpected persistent storage spec %+v", gotBody.Spec.PersistentStorage)
	}
	if got := gotBody.Spec.PersistentStorage.SharedSubPath; got != "" {
		t.Fatalf("expected movable RWO storage set to clear shared subpath, got %q", got)
	}
	if len(gotBody.Spec.PersistentStorage.Mounts) != 3 {
		t.Fatalf("expected three mounts, got %+v", gotBody.Spec.PersistentStorage.Mounts)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "storage_mode=persistent_storage", "persistent_mode=movable_rwo", "storage_size=20Gi"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppDatabaseSwitchoverUsesDatabaseEndpoint(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1,"postgres":{"runtime_id":"runtime_a","database":"app","user":"app","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1,"postgres":{"runtime_id":"runtime_a","database":"app","user":"app","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/switchover":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode switchover body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123","type":"database-switchover","status":"pending"}}`))
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
		"app", "db", "switchover", "demo", "runtime-b",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db switchover: %v", err)
	}

	if gotBody["target_runtime_id"] != "runtime_b" {
		t.Fatalf("expected target_runtime_id runtime_b, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "target_runtime_id=runtime_b"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunProjectEditPatchesMetadata(t *testing.T) {
	t.Parallel()

	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"old","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/projects/project_123":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode patch project body: %v", err)
			}
			_, _ = w.Write([]byte(`{"project":{"id":"project_123","tenant_id":"tenant_123","name":"demo-v2","slug":"demo-v2","description":"new description","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-03T00:00:00Z"}}`))
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
		"project", "edit", "demo",
		"--name", "demo-v2",
		"--description", "new description",
		"--default-runtime-id", "runtime_vps",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run project edit: %v", err)
	}

	if gotBody["name"] != "demo-v2" || gotBody["description"] != "new description" || gotBody["default_runtime_id"] != "runtime_vps" {
		t.Fatalf("unexpected project patch body %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"project=demo-v2", "tenant=Acme", "description=new description"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunProjectMoveQueuesEligibleApps(t *testing.T) {
	t.Parallel()

	var migrateBodies []map[string]any
	var serviceMigrateBodies []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"managed-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[
{"id":"app_web","tenant_id":"tenant_123","project_id":"project_123","name":"web","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"current_runtime_id":"runtime_a"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},
{"id":"app_session","tenant_id":"tenant_123","project_id":"project_123","name":"session","spec":{"runtime_id":"runtime_a","replicas":1,"persistent_storage":{"mode":"shared_project_rwx","storage_size":"1Gi","mounts":[{"kind":"directory","path":"/workspace"}]}},"status":{"current_runtime_id":"runtime_a"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},
{"id":"app_worker","tenant_id":"tenant_123","project_id":"project_123","name":"worker","spec":{"runtime_id":"runtime_a","replicas":1,"persistent_storage":{"mode":"movable_rwo","storage_size":"1Gi","mounts":[{"kind":"directory","path":"/workspace"}]}},"status":{"current_runtime_id":"runtime_a"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},
{"id":"app_data","tenant_id":"tenant_123","project_id":"project_123","name":"data","spec":{"runtime_id":"runtime_a","replicas":1,"persistent_storage":{"storage_size":"1Gi","mounts":[{"kind":"directory","path":"/data"}]}},"status":{"current_runtime_id":"runtime_a"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}
]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(`{"backing_services":[
{"id":"service_db","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},
{"id":"service_owned","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_web","name":"web-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"web","user":"web","service_name":"web-postgres"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}
]}`))
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/backing-services/") && strings.HasSuffix(r.URL.Path, "/migrate"):
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode service migrate body: %v", err)
			}
			serviceMigrateBodies = append(serviceMigrateBodies, body)
			_, _ = w.Write([]byte(`{"backing_service":{"id":"service_db","tenant_id":"tenant_123","project_id":"project_123","name":"main-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"demo","user":"demo","service_name":"demo-postgres"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"already_current":false,"operation":{"id":"op_service_db","tenant_id":"tenant_123","app_id":"app_web","service_id":"service_db","type":"database-switchover","status":"pending","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","target_runtime_id":"runtime_b","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_service_db":
			_, _ = w.Write([]byte(`{"operation":{"id":"op_service_db","tenant_id":"tenant_123","app_id":"app_web","service_id":"service_db","type":"database-switchover","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","target_runtime_id":"runtime_b","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/apps/") && strings.HasSuffix(r.URL.Path, "/migrate"):
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode migrate body: %v", err)
			}
			migrateBodies = append(migrateBodies, body)
			appID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/apps/"), "/migrate")
			_, _ = w.Write([]byte(fmt.Sprintf(`{"operation":{"id":"op_%s","tenant_id":"tenant_123","app_id":%q,"type":"migrate","status":"pending","target_runtime_id":"runtime_b","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`, appID, appID)))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/operations/op_app_"):
			opID := strings.TrimPrefix(r.URL.Path, "/v1/operations/")
			appID := strings.TrimPrefix(opID, "op_")
			_, _ = w.Write([]byte(fmt.Sprintf(`{"operation":{"id":%q,"tenant_id":"tenant_123","app_id":%q,"type":"migrate","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","target_runtime_id":"runtime_b","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`, opID, appID)))
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
		"project", "move", "demo",
		"--to", "runtime-b",
		"--skip-blocked",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run project move: %v", err)
	}

	if len(migrateBodies) != 3 {
		t.Fatalf("expected three migrate requests, got %d", len(migrateBodies))
	}
	if len(serviceMigrateBodies) != 1 {
		t.Fatalf("expected one service migrate request, got %d", len(serviceMigrateBodies))
	}
	if serviceMigrateBodies[0]["target_runtime_id"] != "runtime_b" {
		t.Fatalf("expected service target_runtime_id runtime_b, got %+v", serviceMigrateBodies[0])
	}
	for _, body := range migrateBodies {
		if body["target_runtime_id"] != "runtime_b" {
			t.Fatalf("expected target_runtime_id runtime_b, got %+v", body)
		}
	}
	out := stdout.String()
	for _, want := range []string{"project=demo", "target_runtime_id=runtime_b", "candidate_apps=3", "candidate_services=1", "updated_services=1", "queued_operations=4", "skipped_apps=1", "skipped_app=data", "blocked by persistent storage"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppMoveLocalizesOwnedManagedPostgresBeforeMigratingApp(t *testing.T) {
	t.Parallel()

	var calls []string
	migrated := false
	localized := false
	appPayload := func(appRuntimeID, databaseRuntimeID string) string {
		return fmt.Sprintf(`{"app":{"id":"app_web","tenant_id":"tenant_123","project_id":"project_123","name":"web","spec":{"runtime_id":%q,"image":"ghcr.io/example/web:latest","replicas":1},"status":{"phase":"deployed","current_runtime_id":%q,"current_replicas":1},"backing_services":[{"id":"app-postgres-app_web","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_web","name":"web","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":%q,"database":"web","user":"web","service_name":"web-postgres"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`, appRuntimeID, appRuntimeID, databaseRuntimeID)
	}
	currentAppPayload := func() string {
		switch {
		case migrated:
			return appPayload("runtime_b", "runtime_b")
		case localized:
			return appPayload("runtime_a", "runtime_b")
		default:
			return appPayload("runtime_a", "runtime_a")
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_a","tenant_id":"tenant_123","name":"runtime-a","type":"managed-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"managed-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_web","tenant_id":"tenant_123","project_id":"project_123","name":"web","spec":{"runtime_id":"runtime_a","image":"ghcr.io/example/web:latest","replicas":1},"status":{"current_runtime_id":"runtime_a"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_web":
			_, _ = w.Write([]byte(currentAppPayload()))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_web/migrate":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode migrate body: %v", err)
			}
			if body["target_runtime_id"] != "runtime_b" {
				t.Fatalf("expected migrate target runtime_b, got %+v", body)
			}
			calls = append(calls, "migrate")
			_, _ = w.Write([]byte(`{"operation":{"id":"op_migrate","tenant_id":"tenant_123","app_id":"app_web","type":"migrate","status":"pending","target_runtime_id":"runtime_b","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_migrate":
			migrated = true
			_, _ = w.Write([]byte(`{"operation":{"id":"op_migrate","tenant_id":"tenant_123","app_id":"app_web","type":"migrate","status":"completed","target_runtime_id":"runtime_b","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_web/database/localize":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode database localize body: %v", err)
			}
			if body["target_runtime_id"] != "runtime_b" {
				t.Fatalf("expected database target runtime_b, got %+v", body)
			}
			if _, ok := body["target_node_name"]; ok {
				t.Fatalf("app move should not pin database localize to a node, got %+v", body)
			}
			calls = append(calls, "localize")
			_, _ = w.Write([]byte(`{"operation":{"id":"op_database","tenant_id":"tenant_123","app_id":"app_web","type":"database-localize","status":"pending","target_runtime_id":"runtime_b","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_database":
			localized = true
			_, _ = w.Write([]byte(`{"operation":{"id":"op_database","tenant_id":"tenant_123","app_id":"app_web","type":"database-localize","status":"completed","target_runtime_id":"runtime_b","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"app", "move", "web",
		"--to", "runtime-b",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app move: %v stderr=%s", err, stderr.String())
	}

	if got := strings.Join(calls, ","); got != "localize,migrate" {
		t.Fatalf("expected database localize before app migration, got %s", got)
	}
	if !strings.Contains(stdout.String(), `"current_runtime_id": "runtime_b"`) {
		t.Fatalf("expected final app on runtime_b, got %s", stdout.String())
	}
}

func TestRunDeployGitHubSubcommandNormalizesOwnerRepo(t *testing.T) {
	t.Parallel()

	var gotRequest importGitHubRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-github":
			_, _ = w.Write([]byte(`{"repository":{"repo_url":"https://github.com/example/demo","repo_visibility":"public","repo_owner":"example","repo_name":"demo","branch":"main","default_app_name":"demo"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-github":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode import github request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","name":"demo"},"operation":{"id":"op_123"}}`))
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
		"deploy", "github", "example/demo",
		"--branch", "main",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy github: %v", err)
	}

	if gotRequest.RepoURL != "https://github.com/example/demo" {
		t.Fatalf("expected normalized repo url, got %q", gotRequest.RepoURL)
	}
	if gotRequest.Branch != "main" {
		t.Fatalf("expected branch main, got %q", gotRequest.Branch)
	}
	if gotRequest.Name != "demo" {
		t.Fatalf("expected default app name demo, got %q", gotRequest.Name)
	}
	if got := stdout.String(); got != "app_id=app_123\noperation_id=op_123\n" {
		t.Fatalf("unexpected stdout %q", got)
	}
}

func TestRunDeployGitHubWaitShowsMissingImageDiagnosis(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-github":
			_, _ = w.Write([]byte(`{"repository":{"repo_url":"https://github.com/example/demo","repo_visibility":"public","repo_owner":"example","repo_name":"demo","branch":"main","default_app_name":"demo"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-github":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","name":"demo"},"operation":{"id":"op_import","app_id":"app_123","type":"import","status":"pending"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_import":
			_, _ = w.Write([]byte(`{"operation":{
				"id":"op_import",
				"tenant_id":"tenant_123",
				"app_id":"app_123",
				"type":"import",
				"status":"completed",
				"result_message":"queued deploy operation op_deploy",
				"desired_source":{
					"type":"github-private",
					"repo_url":"https://github.com/example/demo",
					"build_strategy":"dockerfile",
					"compose_service":"runtime",
					"resolved_image_ref":"registry.example.com/demo-managed:sha256"
				},
				"created_at":"2026-04-02T00:00:00Z",
				"updated_at":"2026-04-02T00:01:00Z"
			}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{
				"id":"app_123",
				"tenant_id":"tenant_123",
				"project_id":"project_123",
				"name":"demo",
				"route":{"public_url":"https://demo.example.com"},
				"source":{
					"type":"github-private",
					"repo_url":"https://github.com/example/demo",
					"build_strategy":"dockerfile",
					"compose_service":"runtime",
					"resolved_image_ref":"registry.example.com/demo-managed:sha256"
				},
				"spec":{"image":"registry.example.com/demo-runtime:sha256","runtime_id":"runtime_managed_shared","replicas":1},
				"status":{"phase":"degraded","current_runtime_id":"runtime_managed_shared","current_replicas":1},
				"created_at":"2026-04-02T00:00:00Z",
				"updated_at":"2026-04-02T00:03:00Z"
			}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if got := r.URL.Query().Get("app_id"); got != "app_123" {
				t.Fatalf("expected app_id filter app_123, got %q", got)
			}
			_, _ = w.Write([]byte(`{"operations":[
				{
					"id":"op_deploy",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"deploy",
					"status":"completed",
					"result_message":"deployed revision 14",
					"desired_spec":{"image":"registry.example.com/demo-runtime:sha256","runtime_id":"runtime_managed_shared","replicas":1},
					"desired_source":{
						"type":"github-private",
						"repo_url":"https://github.com/example/demo",
						"build_strategy":"dockerfile",
						"compose_service":"runtime",
						"resolved_image_ref":"registry.example.com/demo-managed:sha256"
					},
					"created_at":"2026-04-02T00:02:00Z",
					"updated_at":"2026-04-02T00:03:00Z"
				},
				{
					"id":"op_import",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"import",
					"status":"completed",
					"result_message":"queued deploy operation op_deploy",
					"desired_source":{
						"type":"github-private",
						"repo_url":"https://github.com/example/demo",
						"build_strategy":"dockerfile",
						"compose_service":"runtime",
						"resolved_image_ref":"registry.example.com/demo-managed:sha256"
					},
					"created_at":"2026-04-02T00:00:00Z",
					"updated_at":"2026-04-02T00:01:00Z"
				}
			]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":1,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[{"image_ref":"registry.example.com/demo-managed:sha256","runtime_image_ref":"registry.example.com/demo-runtime:sha256","status":"missing","current":true}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app.kubernetes.io/name=demo","container":"demo","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-9f8d7c6b5","parent":{"kind":"Deployment","name":"demo"},"revision":"14","desired_replicas":1,"current_replicas":1,"ready_replicas":0,"available_replicas":0,"containers":[{"name":"demo","image":"registry.example.com/demo-runtime:sha256"}],"pods":[{"namespace":"tenant-123","name":"demo-9f8d7c6b5-abc12","phase":"Pending","ready":false,"node_name":"gcp1","containers":[{"name":"demo","image":"registry.example.com/demo-runtime:sha256","ready":false,"restart_count":0,"state":"waiting","reason":"ErrImagePull","message":"manifest unknown"}]}],"warnings":[]}],"warnings":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/control-plane":
			_, _ = w.Write([]byte(`{"control_plane":{"namespace":"fugue-system"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/pods":
			_, _ = w.Write([]byte(`{"cluster_pods":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/audit-events":
			_, _ = w.Write([]byte(`{"audit_events":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
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
		"deploy", "github", "example/demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy github with wait: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"app_id=app_123",
		"operation_id=op_import",
		"url=https://demo.example.com",
		"diagnosis",
		"category=runtime-image-missing",
		`summary=build op_import queued deploy op_deploy, but managed image "registry.example.com/demo-managed:sha256" is missing from registry inventory`,
		"registry_image_status=missing",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppCreateStagesGitHubSource(t *testing.T) {
	t.Parallel()

	var gotRequest createAppRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_default","tenant_id":"tenant_123","name":"default","slug":"default","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode create app request: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_default","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"ports":[8080]},"status":{"phase":"importing"},"source":{"type":"github-public","repo_url":"example/demo","repo_branch":"main","build_strategy":"buildpacks"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"app", "create", "demo",
		"--github", "example/demo",
		"--branch", "main",
		"--build", "buildpacks",
		"--port", "8080",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app create: %v", err)
	}

	if gotRequest.Name != "demo" || gotRequest.TenantID != "tenant_123" || gotRequest.ProjectID != "" {
		t.Fatalf("unexpected create request %+v", gotRequest)
	}
	if gotRequest.Source == nil || gotRequest.Source.Type != model.AppSourceTypeGitHubPublic || gotRequest.Source.RepoURL != "example/demo" || gotRequest.Source.RepoBranch != "main" {
		t.Fatalf("unexpected staged source %+v", gotRequest.Source)
	}
	if gotRequest.Spec.Ports[0] != 8080 {
		t.Fatalf("expected service port 8080, got %+v", gotRequest.Spec.Ports)
	}
	out := stdout.String()
	for _, want := range []string{"app=demo", "phase=importing", "source=github-public", "next_step=fugue app build demo"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppScaleByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotScaleBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/scale":
			if err := json.NewDecoder(r.Body).Decode(&gotScaleBody); err != nil {
				t.Fatalf("decode scale body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "scale", "demo",
		"--replicas", "3",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app scale: %v", err)
	}

	if gotScaleBody["replicas"] != float64(3) {
		t.Fatalf("expected replicas=3, got %#v", gotScaleBody["replicas"])
	}
	output := stdout.String()
	if !strings.Contains(output, "app=demo") {
		t.Fatalf("expected stdout to contain app name, got %q", output)
	}
	if !strings.Contains(output, "operation_id=op_123") {
		t.Fatalf("expected stdout to contain operation id, got %q", output)
	}
}

func TestRunAppSourceBindGitHubUsesRepairEndpoint(t *testing.T) {
	t.Parallel()

	var gotRequest struct {
		OriginSource *model.AppSource `json:"origin_source"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","source":{"type":"upload","upload_id":"upload_123","build_strategy":"dockerfile","dockerfile_path":"Dockerfile","build_context_dir":".","image_name_suffix":"gateway","compose_service":"gateway","compose_depends_on":["runtime"]},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","source":{"type":"upload","upload_id":"upload_123","build_strategy":"dockerfile","dockerfile_path":"Dockerfile","build_context_dir":".","image_name_suffix":"gateway","compose_service":"gateway","compose_depends_on":["runtime"]},"build_source":{"type":"upload","upload_id":"upload_123","build_strategy":"dockerfile","dockerfile_path":"Dockerfile","build_context_dir":".","image_name_suffix":"gateway","compose_service":"gateway","compose_depends_on":["runtime"]},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_123/source":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode source patch request: %v", err)
			}
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","source":{"type":"upload","upload_id":"upload_123","build_strategy":"dockerfile","dockerfile_path":"Dockerfile","build_context_dir":".","image_name_suffix":"gateway","compose_service":"gateway","compose_depends_on":["runtime"]},"origin_source":{"type":"github-public","repo_url":"https://github.com/example/demo","repo_branch":"main","build_strategy":"dockerfile","dockerfile_path":"Dockerfile","build_context_dir":".","image_name_suffix":"gateway","compose_service":"gateway","compose_depends_on":["runtime"]},"build_source":{"type":"upload","upload_id":"upload_123","build_strategy":"dockerfile","dockerfile_path":"Dockerfile","build_context_dir":".","image_name_suffix":"gateway","compose_service":"gateway","compose_depends_on":["runtime"]},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"already_current":false}`))
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
		"app", "source", "bind-github", "demo", "example/demo",
		"--branch", "main",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app source bind-github: %v", err)
	}

	if gotRequest.OriginSource == nil {
		t.Fatal("expected origin_source patch payload")
	}
	if got := gotRequest.OriginSource.Type; got != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected origin source type %q, got %q", model.AppSourceTypeGitHubPublic, got)
	}
	if got := gotRequest.OriginSource.RepoURL; got != "https://github.com/example/demo" {
		t.Fatalf("expected origin repo url to be normalized, got %q", got)
	}
	if got := gotRequest.OriginSource.RepoBranch; got != "main" {
		t.Fatalf("expected origin branch main, got %q", got)
	}
	if got := gotRequest.OriginSource.BuildStrategy; got != model.AppBuildStrategyDockerfile {
		t.Fatalf("expected build strategy to be preserved, got %q", got)
	}
	if got := gotRequest.OriginSource.DockerfilePath; got != "Dockerfile" {
		t.Fatalf("expected dockerfile path to be preserved, got %q", got)
	}
	if got := gotRequest.OriginSource.BuildContextDir; got != "." {
		t.Fatalf("expected build context to be preserved, got %q", got)
	}
	if got := gotRequest.OriginSource.ImageNameSuffix; got != "gateway" {
		t.Fatalf("expected image name suffix to be preserved, got %q", got)
	}
	if got := gotRequest.OriginSource.ComposeService; got != "gateway" {
		t.Fatalf("expected compose service to be preserved, got %q", got)
	}
	if len(gotRequest.OriginSource.ComposeDependsOn) != 1 || gotRequest.OriginSource.ComposeDependsOn[0] != "runtime" {
		t.Fatalf("expected compose dependencies to be preserved, got %+v", gotRequest.OriginSource.ComposeDependsOn)
	}
	if got := gotRequest.OriginSource.UploadID; got != "" {
		t.Fatalf("expected bind-github request to avoid stale upload ownership fields, got upload_id %q", got)
	}

	out := stdout.String()
	for _, want := range []string{"app=demo", "origin_source_type=github-public", "origin_source_ref=https://github.com/example/demo"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppContinuityAuditByNameUsesExplicitCommand(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":2},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":2},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":2},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":2},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","name":"shared","type":"managed-shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
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
		"app", "continuity", "audit", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app continuity audit: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"app_id=app_123",
		"classification=ready",
		"summary=eligible for live transfer",
		"runtime_type=managed-shared",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppContinuityEnableUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody patchAppContinuityRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_123/continuity":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode continuity body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app_failover":{"target_runtime_id":"runtime_b","auto":true},"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "continuity", "enable", "demo",
		"--app-to", "runtime-b",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app continuity enable: %v", err)
	}

	if gotBody.AppFailover == nil || gotBody.AppFailover.TargetRuntimeID != "runtime_b" || !gotBody.AppFailover.Enabled {
		t.Fatalf("unexpected app failover request %+v", gotBody.AppFailover)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "app_failover_enabled=true", "app_failover_target_runtime_id=runtime_b"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppFailoverConfigureUsesNewSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody patchAppContinuityRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_123/continuity":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode continuity body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app_failover":{"target_runtime_id":"runtime_b","auto":true},"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "failover", "configure", "demo",
		"--app-to", "runtime-b",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app failover configure: %v", err)
	}

	if gotBody.AppFailover == nil || gotBody.AppFailover.TargetRuntimeID != "runtime_b" || !gotBody.AppFailover.Enabled {
		t.Fatalf("unexpected app failover request %+v", gotBody.AppFailover)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "app_failover_enabled=true", "app_failover_target_runtime_id=runtime_b"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunDeployInspectLocalUsesUploadInspectEndpoint(t *testing.T) {
	t.Parallel()

	workDir := filepath.Join(t.TempDir(), "demo-stack")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("mkdir work dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	var gotRequest importUploadRequest
	var archiveBytes []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-upload":
			if err := r.ParseMultipartForm(8 << 20); err != nil {
				t.Fatalf("parse multipart form: %v", err)
			}
			if err := json.Unmarshal([]byte(r.FormValue("request")), &gotRequest); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			archive, header, err := r.FormFile("archive")
			if err != nil {
				t.Fatalf("read archive part: %v", err)
			}
			defer archive.Close()
			archiveBytes, err = io.ReadAll(archive)
			if err != nil {
				t.Fatalf("read archive body: %v", err)
			}
			if header.Filename != "demo-stack.tgz" {
				t.Fatalf("expected archive filename demo-stack.tgz, got %q", header.Filename)
			}
			_, _ = w.Write([]byte(`{
  "upload":{
    "archive_filename":"demo-stack.tgz",
    "archive_sha256":"abc123",
    "archive_size_bytes":123,
    "default_app_name":"demo-stack",
    "source_kind":"compose",
    "source_path":"docker-compose.yml"
  },
  "compose_stack":{
    "compose_path":"docker-compose.yml",
    "primary_service":"web",
    "warnings":["missing HEALTHCHECK for web"],
    "inference_report":[{"level":"warning","category":"persistent-storage","service":"web","message":"editable persistent file detected"}],
    "services":[
      {
        "service":"web",
        "kind":"app",
        "service_type":"web",
        "backing_service":false,
        "build_strategy":"dockerfile",
        "internal_port":3000,
        "compose_service":"web",
        "published":true,
        "source_dir":"web",
        "dockerfile_path":"web/Dockerfile",
        "build_context_dir":"web",
        "binding_targets":["db"],
        "persistent_storage_seed_files":[{"path":"/workspace/config.yaml","mode":420,"seed_content":""}]
      }
    ]
  }
}`))
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
		"deploy", "inspect", workDir,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy inspect: %v", err)
	}

	if gotRequest.Name != "demo-stack" {
		t.Fatalf("expected request name demo-stack, got %+v", gotRequest)
	}
	if len(archiveBytes) == 0 {
		t.Fatal("expected archive bytes to be uploaded")
	}
	out := stdout.String()
	for _, want := range []string{
		"mode=inspect",
		"source=upload",
		"topology=compose_stack",
		"primary_service=web",
		"[services]",
		"BINDINGS",
		"[persistent_storage_seed_files]",
		"/workspace/config.yaml",
		"[warnings]",
		"[inference_report]",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunDeployInspectGitHubOwnerRepoUsesParentFlags(t *testing.T) {
	t.Parallel()

	var gotRequest inspectGitHubTemplateRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-github":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode inspect github request: %v", err)
			}
			_, _ = w.Write([]byte(`{
  "repository":{
    "repo_url":"https://github.com/example/demo",
    "repo_visibility":"private",
    "repo_owner":"example",
    "repo_name":"demo",
    "branch":"main",
    "commit_sha":"abc123",
    "commit_committed_at":"2026-04-02T00:00:00Z",
    "default_app_name":"demo"
  },
  "fugue_manifest":{
    "manifest_path":"fugue.yaml",
    "primary_service":"web",
    "warnings":[],
    "inference_report":[],
    "services":[{"service":"web","kind":"app","service_type":"web","build_strategy":"dockerfile","internal_port":3000,"compose_service":"web","published":true,"source_dir":"web","dockerfile_path":"web/Dockerfile","build_context_dir":"web","binding_targets":[],"persistent_storage_seed_files":[]}]
  }
}`))
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
		"deploy", "inspect", "example/demo",
		"--branch", "main",
		"--private",
		"--repo-token", "secret",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy inspect github shorthand: %v", err)
	}

	if gotRequest.RepoURL != "https://github.com/example/demo" || gotRequest.Branch != "main" || gotRequest.RepoVisibility != "private" || gotRequest.RepoAuthToken != "secret" {
		t.Fatalf("unexpected inspect github request %+v", gotRequest)
	}
	out := stdout.String()
	for _, want := range []string{"source=github", "repo_url=https://github.com/example/demo", "topology=fugue_manifest", "topology_path=fugue.yaml"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunDeployGitHubIncludesIdempotencyAndSeedFiles(t *testing.T) {
	t.Parallel()

	seedPath := filepath.Join(t.TempDir(), "seed.sql")
	if err := os.WriteFile(seedPath, []byte("create table demo();\n"), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	gatewayEnvPath := filepath.Join(t.TempDir(), "gateway.env")
	if err := os.WriteFile(gatewayEnvPath, []byte("GATEWAY_ONLY=1\nSHARED_MODE=gateway\n"), 0o644); err != nil {
		t.Fatalf("write gateway env file: %v", err)
	}
	runtimeEnvPath := filepath.Join(t.TempDir(), "runtime.env")
	if err := os.WriteFile(runtimeEnvPath, []byte("RUNTIME_ONLY=1\n"), 0o644); err != nil {
		t.Fatalf("write runtime env file: %v", err)
	}

	var gotRequest importGitHubRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-github":
			_, _ = w.Write([]byte(`{"repository":{"repo_url":"https://github.com/example/demo","repo_visibility":"public","repo_owner":"example","repo_name":"demo","branch":"main","default_app_name":"demo"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-github":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode github import request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{
  "app":{"id":"app_123","name":"demo"},
  "operation":{"id":"op_123"},
  "idempotency":{"key":"import-123","status":"completed","replayed":true}
}`))
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
		"deploy", "github", "example/demo",
		"--idempotency-key", "import-123",
		"--seed-file", "web:/workspace/seed.sql=" + seedPath,
		"--service-env-file", "gateway=" + gatewayEnvPath,
		"--service-env-file", "runtime=" + runtimeEnvPath,
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy github: %v", err)
	}

	if gotRequest.IdempotencyKey != "import-123" {
		t.Fatalf("expected idempotency key import-123, got %+v", gotRequest)
	}
	if len(gotRequest.PersistentStorageSeedFiles) != 1 {
		t.Fatalf("expected one seed file override, got %+v", gotRequest.PersistentStorageSeedFiles)
	}
	if gotRequest.PersistentStorageSeedFiles[0].Service != "web" || gotRequest.PersistentStorageSeedFiles[0].Path != "/workspace/seed.sql" {
		t.Fatalf("unexpected seed file override %+v", gotRequest.PersistentStorageSeedFiles[0])
	}
	if gotRequest.PersistentStorageSeedFiles[0].SeedContent != "create table demo();\n" {
		t.Fatalf("unexpected seed content %+v", gotRequest.PersistentStorageSeedFiles[0])
	}
	if got := gotRequest.ServiceEnv["gateway"]["GATEWAY_ONLY"]; got != "1" {
		t.Fatalf("expected gateway service env override, got %+v", gotRequest.ServiceEnv)
	}
	if got := gotRequest.ServiceEnv["gateway"]["SHARED_MODE"]; got != "gateway" {
		t.Fatalf("expected gateway shared mode override, got %+v", gotRequest.ServiceEnv)
	}
	if got := gotRequest.ServiceEnv["runtime"]["RUNTIME_ONLY"]; got != "1" {
		t.Fatalf("expected runtime service env override, got %+v", gotRequest.ServiceEnv)
	}
	out := stdout.String()
	for _, want := range []string{
		"app_id=app_123",
		"operation_id=op_123",
		"idempotency_key=import-123",
		"idempotency_status=completed",
		"idempotency_replayed=true",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunServicePostgresCreateUsesTypedCommand(t *testing.T) {
	t.Parallel()

	var gotRequest createBackingServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_default","tenant_id":"tenant_123","name":"default","slug":"default","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backing-services":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode backing service request: %v", err)
			}
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_123","tenant_id":"tenant_123","project_id":"project_default","name":"app-db","type":"postgres","status":"active","spec":{"postgres":{"runtime_id":"runtime_managed_shared","database":"app","user":"app"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"service", "postgres", "create", "app-db",
		"--runtime", "shared",
		"--database", "app",
		"--user", "app",
		"--password", "secret",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run service postgres create: %v", err)
	}

	if gotRequest.Name != "app-db" || gotRequest.ProjectID != "project_default" {
		t.Fatalf("unexpected create request %+v", gotRequest)
	}
	if gotRequest.Spec.Postgres == nil || gotRequest.Spec.Postgres.RuntimeID != "runtime_managed_shared" {
		t.Fatalf("expected postgres runtime runtime_managed_shared, got %+v", gotRequest.Spec.Postgres)
	}
	out := stdout.String()
	for _, want := range []string{"service=app-db", "project=default", "type=postgres", "runtime=shared"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunProjectOverviewUsesConsoleEndpoints(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/gallery":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","name":"demo","app_count":1,"service_count":1,"lifecycle":{"label":"live","live":true,"sync_mode":"auto","tone":"positive"},"resource_usage_snapshot":{},"service_badges":[{"kind":"postgres","label":"Postgres","meta":"1"}]}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/projects/project_123":
			_, _ = w.Write([]byte(`{
  "project_id":"project_123",
  "project_name":"demo",
  "project":{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},
  "apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"web","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],
  "operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],
  "cluster_nodes":[{"name":"node-a","status":"ready","conditions":{},"created_at":"2026-04-02T00:00:00Z"}]
}`))
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
		"project", "overview", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run project overview: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"project=demo", "lifecycle=live", "[service_status]", "[apps]", "[operations]", "[cluster_nodes]"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunProjectDeleteWaitTracksFinalRemoval(t *testing.T) {
	t.Parallel()

	var projectPolls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/projects/project_123":
			_, _ = w.Write([]byte(`{"project":{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"deleted":false,"delete_requested":true,"queued_operations":1,"already_deleting_apps":0,"deleted_backing_services":1,"operations":[{"id":"op_delete","tenant_id":"tenant_123","app_id":"app_123","type":"delete","status":"pending","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/projects/project_123":
			projectPolls++
			if projectPolls >= 2 {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"not found"}`))
				return
			}
			_, _ = w.Write([]byte(`{
  "project_id":"project_123",
  "project_name":"demo",
  "project":{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},
  "apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"web","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],
  "operations":[{"id":"op_delete","tenant_id":"tenant_123","app_id":"app_123","type":"delete","status":"pending","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],
  "cluster_nodes":[]
}`))
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
		"project", "delete", "demo",
		"--wait",
		"--interval", "1ms",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run project delete --wait: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"delete_requested=true", "queued_operations=1", "remaining_apps=1", "[delete_operations]", "final_state=deleted"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunProjectVerifyChecksPublicRoutes(t *testing.T) {
	t.Parallel()

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/projects/project_123":
			_, _ = w.Write([]byte(fmt.Sprintf(`{
  "project_id":"project_123",
  "project_name":"demo",
  "project":{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},
  "apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"web","route":{"public_url":"%s","hostname":"demo.example.com"},"spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],
  "operations":[],
  "cluster_nodes":[]
}`, server.URL)))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/gallery":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","name":"demo","app_count":1,"service_count":1,"lifecycle":{"label":"live","live":true,"sync_mode":"auto","tone":"positive"},"resource_usage_snapshot":{},"service_badges":[]}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/healthz":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`ok`))
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
		"project", "verify", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run project verify: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"checks=1", "passed=1", "SERVICE", "web", "/healthz", "200"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunRuntimeAttachUsesSaferInstructions(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runtimes/enroll-tokens":
			_, _ = w.Write([]byte(`{"enrollment_token":{"id":"token_123","label":"edge-a","prefix":"fgt_123","expires_at":"2026-04-10T00:00:00Z"},"secret":"super-secret-token"}`))
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
		"runtime", "attach", "edge-a",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run runtime attach: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"secret=super-secret-token", "export_token=export FUGUE_ENROLL_TOKEN=<paste-secret-above>", "join_command=curl -fsSL " + server.URL + "/install/join-cluster.sh | sudo bash"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
	if strings.Contains(out, "FUGUE_ENROLL_TOKEN=super-secret-token") {
		t.Fatalf("expected attach instructions to avoid embedding the real secret, got %q", out)
	}
}

func TestRunRuntimeOfferSetPublishesOffer(t *testing.T) {
	t.Parallel()

	var gotRequest setRuntimePublicOfferRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","access_mode":"public","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_123":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","access_mode":"public","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runtimes/runtime_123/public-offer":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode runtime offer request: %v", err)
			}
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","access_mode":"public","status":"active","public_offer":{"reference_bundle":{"cpu_millicores":2000,"memory_mebibytes":4096,"storage_gibibytes":50},"reference_monthly_price_microcents":19990000,"free_storage":true,"price_book":{"currency":"USD","hours_per_month":730},"updated_at":"2026-04-02T00:00:00Z"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"runtime", "offer", "set", "edge-a",
		"--cpu", "2000",
		"--memory", "4096",
		"--storage", "50",
		"--monthly-usd", "19.99",
		"--free-storage",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run runtime offer set: %v", err)
	}

	if gotRequest.ReferenceBundle.CPUMilliCores != 2000 || gotRequest.ReferenceBundle.MemoryMebibytes != 4096 || gotRequest.ReferenceBundle.StorageGibibytes != 50 {
		t.Fatalf("unexpected reference bundle %+v", gotRequest.ReferenceBundle)
	}
	if gotRequest.ReferenceMonthlyPriceMicroCents != 19_990_000 || !gotRequest.FreeStorage {
		t.Fatalf("unexpected offer request %+v", gotRequest)
	}
	out := stdout.String()
	for _, want := range []string{"runtime=edge-a", "tenant=Acme", "published=true", "reference_monthly_price=USD 19.99", "free_storage=true"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunRuntimeDeleteUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_123":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/runtimes/runtime_123":
			_, _ = w.Write([]byte(`{"deleted":true,"runtime":{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","status":"deleted","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-03T00:00:00Z"}}`))
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
		"runtime", "delete", "edge-a",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run runtime delete: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"runtime=edge-a", "tenant=Acme", "deleted=true"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppOverviewAggregatesRelatedState(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/domains":
			_, _ = w.Write([]byte(`{"domains":[{"hostname":"www.example.com","status":"active","tls_status":"ready","route_target":"demo.apps.example.com","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/bindings":
			_, _ = w.Write([]byte(`{"bindings":[{"id":"binding_123","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_123","alias":"postgres","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"backing_services":[{"id":"svc_123","tenant_id":"tenant_123","project_id":"project_123","name":"app-db","type":"postgres","status":"active","spec":{"postgres":{"runtime_id":"runtime_managed_shared","database":"app","user":"app"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			_, _ = w.Write([]byte(`{"operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":1,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[{"image_ref":"registry.example.com/demo:abc123","status":"ready","current":true,"size_bytes":1048576}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app.kubernetes.io/name=demo","container":"demo","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-8c9f6d74f7","parent":{"kind":"Deployment","name":"demo"},"revision":"13","desired_replicas":1,"current_replicas":1,"ready_replicas":1,"available_replicas":1,"containers":[{"name":"demo","image":"registry.example.com/demo:abc123"}],"pods":[{"namespace":"tenant-123","name":"demo-8c9f6d74f7-abc12","phase":"Running","ready":true,"node_name":"gcp1","containers":[{"name":"demo","image":"registry.example.com/demo:abc123","ready":true,"restart_count":0,"state":"running"}]}],"warnings":[]}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
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
		"app", "overview", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app overview: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"app=demo", "project=demo", "runtime=shared", "domains", "www.example.com", "services", "postgres", "images", "versions=1", "pods", "demo-8c9f6d74f7", "operations", "op_123"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppBuildLogsShowsArtifactStages(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/build-logs":
			_, _ = w.Write([]byte(`{
				"operation_id":"op_import",
				"operation_status":"completed",
				"job_name":"build-demo",
				"available":false,
				"source":"job",
				"summary":"import build completed",
				"build_strategy":"dockerfile"
			}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_import":
			_, _ = w.Write([]byte(`{"operation":{
				"id":"op_import",
				"tenant_id":"tenant_123",
				"app_id":"app_123",
				"type":"import",
				"status":"completed",
				"result_message":"queued deploy operation op_deploy",
				"desired_source":{
					"type":"github-private",
					"build_strategy":"dockerfile",
					"compose_service":"runtime",
					"resolved_image_ref":"registry.example.com/demo-managed:sha256"
				},
				"created_at":"2026-04-02T00:00:00Z",
				"updated_at":"2026-04-02T00:01:00Z"
			}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if got := r.URL.Query().Get("app_id"); got != "app_123" {
				t.Fatalf("expected app_id filter app_123, got %q", got)
			}
			_, _ = w.Write([]byte(`{"operations":[
				{
					"id":"op_deploy",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"deploy",
					"status":"completed",
					"result_message":"deployed revision 13",
					"desired_spec":{"image":"registry.example.com/demo-runtime:sha256","runtime_id":"runtime_managed_shared","replicas":1},
					"created_at":"2026-04-02T00:02:00Z",
					"updated_at":"2026-04-02T00:03:00Z"
				},
				{
					"id":"op_import",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"import",
					"status":"completed",
					"result_message":"queued deploy operation op_deploy",
					"desired_source":{
						"type":"github-private",
						"build_strategy":"dockerfile",
						"compose_service":"runtime",
						"resolved_image_ref":"registry.example.com/demo-managed:sha256"
					},
					"created_at":"2026-04-02T00:00:00Z",
					"updated_at":"2026-04-02T00:01:00Z"
				}
			]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":1,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[{"image_ref":"registry.example.com/demo-managed:sha256","runtime_image_ref":"registry.example.com/demo-runtime:sha256","status":"ready","current":true,"size_bytes":1048576}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app.kubernetes.io/name=demo","container":"demo","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-8c9f6d74f7","parent":{"kind":"Deployment","name":"demo"},"revision":"13","desired_replicas":1,"current_replicas":1,"ready_replicas":1,"available_replicas":1,"containers":[{"name":"demo","image":"registry.example.com/demo-runtime:sha256"}],"pods":[{"namespace":"tenant-123","name":"demo-8c9f6d74f7-abc12","phase":"Running","ready":true,"node_name":"gcp1","containers":[{"name":"demo","image":"registry.example.com/demo-runtime:sha256","ready":true,"restart_count":0,"state":"running"}]}],"warnings":[]}],"warnings":[]}`))
		case r.Method == http.MethodGet && (r.URL.Path == "/v1/cluster/control-plane" || r.URL.Path == "/v1/cluster/pods" || r.URL.Path == "/v1/audit-events"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
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
		"app", "logs", "build", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app logs build: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"summary=import build completed",
		"service=runtime",
		"managed_image_ref=registry.example.com/demo-managed:sha256",
		"runtime_image_ref=registry.example.com/demo-runtime:sha256",
		"registry_image_status=available",
		"deploy_operation_id=op_deploy",
		"deploy_status=completed",
		"stages",
		"build",
		"push",
		"publish",
		"deploy",
		"runtime",
		"1/1 pods ready",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppBuildLogsFallsBackToArtifactContextWhenJobNameMissing(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/build-logs":
			_, _ = w.Write([]byte(`{
				"operation_id":"op_import",
				"operation_status":"completed",
				"job_name":"",
				"available":false,
				"source":"operation.result_message",
				"summary":"import build completed",
				"build_strategy":"dockerfile"
			}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_import":
			_, _ = w.Write([]byte(`{"operation":{
				"id":"op_import",
				"tenant_id":"tenant_123",
				"app_id":"app_123",
				"type":"import",
				"status":"completed",
				"result_message":"queued deploy operation op_deploy",
				"desired_source":{
					"type":"github-private",
					"build_strategy":"dockerfile",
					"compose_service":"runtime",
					"resolved_image_ref":"registry.example.com/demo-managed:sha256"
				},
				"created_at":"2026-04-02T00:00:00Z",
				"updated_at":"2026-04-02T00:01:00Z"
			}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if got := r.URL.Query().Get("app_id"); got != "app_123" {
				t.Fatalf("expected app_id filter app_123, got %q", got)
			}
			_, _ = w.Write([]byte(`{"operations":[
				{
					"id":"op_deploy",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"deploy",
					"status":"completed",
					"result_message":"deployed revision 14",
					"desired_spec":{"image":"registry.example.com/demo-runtime:sha256","runtime_id":"runtime_managed_shared","replicas":1},
					"created_at":"2026-04-02T00:02:00Z",
					"updated_at":"2026-04-02T00:03:00Z"
				},
				{
					"id":"op_import",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"import",
					"status":"completed",
					"result_message":"queued deploy operation op_deploy",
					"desired_source":{
						"type":"github-private",
						"build_strategy":"dockerfile",
						"compose_service":"runtime",
						"resolved_image_ref":"registry.example.com/demo-managed:sha256"
					},
					"created_at":"2026-04-02T00:00:00Z",
					"updated_at":"2026-04-02T00:01:00Z"
				}
			]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":1,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[{"image_ref":"registry.example.com/demo-managed:sha256","runtime_image_ref":"registry.example.com/demo-runtime:sha256","status":"missing","current":true}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app.kubernetes.io/name=demo","container":"demo","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-9f8d7c6b5","parent":{"kind":"Deployment","name":"demo"},"revision":"14","desired_replicas":1,"current_replicas":1,"ready_replicas":0,"available_replicas":0,"containers":[{"name":"demo","image":"registry.example.com/demo-runtime:sha256"}],"pods":[{"namespace":"tenant-123","name":"demo-9f8d7c6b5-abc12","phase":"Pending","ready":false,"node_name":"gcp1","containers":[{"name":"demo","image":"registry.example.com/demo-runtime:sha256","ready":false,"restart_count":0,"state":"waiting","reason":"ErrImagePull","message":"manifest unknown"}]}],"warnings":[]}],"warnings":[]}`))
		case r.Method == http.MethodGet && (r.URL.Path == "/v1/cluster/control-plane" || r.URL.Path == "/v1/cluster/pods" || r.URL.Path == "/v1/audit-events"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
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
		"app", "logs", "build", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app logs build with fallback context: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"job_name=ReplicaSet/demo-9f8d7c6b5",
		`summary=build op_import queued deploy op_deploy, but managed image "registry.example.com/demo-managed:sha256" is missing from registry inventory`,
		"build_message=import build completed",
		"latest_pod_group=ReplicaSet/demo-9f8d7c6b5",
		"pod_issue=pod demo-9f8d7c6b5-abc12 container demo ErrImagePull: manifest unknown",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppBuildLogsShowsBuilderAndRegistryLifecycleEvidence(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/build-logs":
			_, _ = w.Write([]byte(`{
				"operation_id":"op_import",
				"operation_status":"completed",
				"job_name":"build-demo-abc",
				"available":false,
				"source":"operation.result_message",
				"summary":"import build completed",
				"build_strategy":"dockerfile"
			}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_import":
			_, _ = w.Write([]byte(`{"operation":{
				"id":"op_import",
				"tenant_id":"tenant_123",
				"app_id":"app_123",
				"type":"import",
				"status":"completed",
				"result_message":"queued deploy operation op_deploy",
				"desired_source":{
					"type":"github-private",
					"build_strategy":"dockerfile",
					"compose_service":"runtime",
					"resolved_image_ref":"registry.example.com/fugue-apps/demo-managed:git-abc123"
				},
				"created_at":"2026-04-02T00:00:00Z",
				"updated_at":"2026-04-02T00:01:00Z"
			}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if got := r.URL.Query().Get("app_id"); got != "app_123" {
				t.Fatalf("expected app_id filter app_123, got %q", got)
			}
			_, _ = w.Write([]byte(`{"operations":[
				{
					"id":"op_deploy",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"deploy",
					"status":"completed",
					"result_message":"deployed revision 19",
					"desired_spec":{"image":"registry.example.com/demo-runtime:git-abc123","runtime_id":"runtime_managed_shared","replicas":1},
					"created_at":"2026-04-02T00:02:00Z",
					"updated_at":"2026-04-02T00:03:00Z"
				},
				{
					"id":"op_import",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"import",
					"status":"completed",
					"result_message":"queued deploy operation op_deploy",
					"desired_source":{
						"type":"github-private",
						"build_strategy":"dockerfile",
						"compose_service":"runtime",
						"resolved_image_ref":"registry.example.com/fugue-apps/demo-managed:git-abc123"
					},
					"created_at":"2026-04-02T00:00:00Z",
					"updated_at":"2026-04-02T00:01:00Z"
				}
			]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":1,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[{"image_ref":"registry.example.com/fugue-apps/demo-managed:git-abc123","runtime_image_ref":"registry.example.com/demo-runtime:git-abc123","status":"missing","current":true}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app.kubernetes.io/name=demo","container":"demo","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-9f8d7c6b5","parent":{"kind":"Deployment","name":"demo"},"revision":"19","desired_replicas":1,"current_replicas":1,"ready_replicas":0,"available_replicas":0,"containers":[{"name":"demo","image":"registry.example.com/demo-runtime:git-abc123"}],"pods":[{"namespace":"tenant-123","name":"demo-9f8d7c6b5-abc12","phase":"Pending","ready":false,"node_name":"gcp1","containers":[{"name":"demo","image":"registry.example.com/demo-runtime:git-abc123","ready":false,"restart_count":0,"state":"waiting","reason":"ErrImagePull","message":"manifest unknown"}]}],"warnings":[]}],"warnings":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/control-plane":
			_, _ = w.Write([]byte(`{"control_plane":{"namespace":"fugue-system","release_instance":"fugue","version":"deadbeef","status":"ready","observed_at":"2026-04-02T00:05:00Z","components":[]}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/pods":
			namespace := r.URL.Query().Get("namespace")
			selector := r.URL.Query().Get("label_selector")
			if namespace != "fugue-system" {
				t.Fatalf("expected cluster pod namespace fugue-system, got %q", namespace)
			}
			switch selector {
			case "job-name=build-demo-abc":
				_, _ = w.Write([]byte(`{"cluster_pods":[{"namespace":"fugue-system","name":"build-demo-abc-xyz12","phase":"Succeeded","ready":false,"node_name":"gcp2","start_time":"2026-04-02T00:00:05Z","containers":[{"name":"builder","image":"ghcr.io/acme/fugue-builder:deadbeef","ready":false,"restart_count":0,"state":"terminated"}]}]}`))
			case "app.kubernetes.io/component=controller":
				_, _ = w.Write([]byte(`{"cluster_pods":[{"namespace":"fugue-system","name":"fugue-fugue-controller-abc","phase":"Running","ready":true,"node_name":"gcp1","start_time":"2026-04-02T00:00:00Z","containers":[{"name":"controller","image":"ghcr.io/acme/fugue-controller:deadbeef","ready":true,"restart_count":0,"state":"running"}]}]}`))
			case "app.kubernetes.io/component=registry":
				_, _ = w.Write([]byte(`{"cluster_pods":[{"namespace":"fugue-system","name":"fugue-fugue-registry-abc","phase":"Running","ready":true,"node_name":"gcp1","start_time":"2026-04-02T00:00:00Z","containers":[{"name":"registry","image":"registry:2","ready":true,"restart_count":0,"state":"running"}]}]}`))
			default:
				t.Fatalf("unexpected cluster pod selector %q", selector)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/logs":
			switch r.URL.Query().Get("pod") {
			case "fugue-fugue-controller-abc":
				_, _ = w.Write([]byte(`{"namespace":"fugue-system","pod":"fugue-fugue-controller-abc","container":"controller","logs":"2026-04-02T00:00:10Z builder job start kind=dockerfile name=build-demo-abc namespace=fugue-system image=registry.example.com/fugue-apps/demo-managed:git-abc123 operation=op_import app=app_123 placement=node=gcp2\n2026-04-02T00:00:11Z builder job completed kind=dockerfile name=build-demo-abc namespace=fugue-system image=registry.example.com/fugue-apps/demo-managed:git-abc123\n2026-04-02T00:01:02Z operation op_import completed import build; managed_image=registry.example.com/fugue-apps/demo-managed:git-abc123 runtime_image=registry.example.com/demo-runtime:git-abc123 deploy=op_deploy"}`))
			case "fugue-fugue-registry-abc":
				_, _ = w.Write([]byte(`{"namespace":"fugue-system","pod":"fugue-fugue-registry-abc","container":"registry","logs":"time=\"2026-04-02T00:01:00Z\" level=info msg=\"PUT /v2/fugue-apps/demo-managed/manifests/git-abc123\" http.request.method=PUT http.request.uri=\"/v2/fugue-apps/demo-managed/manifests/git-abc123\"\ntime=\"2026-04-02T00:08:03Z\" level=info msg=\"DELETE /v2/fugue-apps/demo-managed/manifests/sha256:deadbeef\" http.request.uri=\"/v2/fugue-apps/demo-managed/manifests/sha256:deadbeef\""}`))
			default:
				t.Fatalf("unexpected cluster logs pod %q", r.URL.Query().Get("pod"))
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/audit-events":
			_, _ = w.Write([]byte(`{"audit_events":[{"id":"audit_123","tenant_id":"tenant_123","actor_type":"api-key","actor_id":"key_123","action":"app.image.delete","target_type":"app","target_id":"app_123","metadata":{"image_ref":"registry.example.com/fugue-apps/demo-managed:git-abc123"},"created_at":"2026-04-02T00:09:00Z"}]}`))
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
		"app", "logs", "build", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app logs build with lifecycle evidence: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"builder_namespace=fugue-system",
		"builder_pods=build-demo-abc-xyz12",
		"builder_nodes=gcp2",
		"builder_containers=builder",
		"builder_job_state=completed",
		"registry_lifecycle_state=deleted-after-publish",
		`summary=managed image "registry.example.com/fugue-apps/demo-managed:git-abc123" was published earlier and later deleted from registry inventory`,
		"controller_pod=fugue-fugue-controller-abc",
		"registry_pod=fugue-fugue-registry-abc",
		"builder_job_evidence=2026-04-02T00:00:10Z builder job start kind=dockerfile name=build-demo-abc namespace=fugue-system image=registry.example.com/fugue-apps/demo-managed:git-abc123 operation=op_import app=app_123 placement=node=gcp2",
		"controller_evidence=2026-04-02T00:01:02Z operation op_import completed import build; managed_image=registry.example.com/fugue-apps/demo-managed:git-abc123 runtime_image=registry.example.com/demo-runtime:git-abc123 deploy=op_deploy",
		`registry_publish_evidence=time="2026-04-02T00:01:00Z" level=info msg="PUT /v2/fugue-apps/demo-managed/manifests/git-abc123" http.request.method=PUT http.request.uri="/v2/fugue-apps/demo-managed/manifests/git-abc123"`,
		`registry_evidence=time="2026-04-02T00:08:03Z" level=info msg="DELETE /v2/fugue-apps/demo-managed/manifests/sha256:deadbeef" http.request.uri="/v2/fugue-apps/demo-managed/manifests/sha256:deadbeef"`,
		"registry_lifecycle_evidence=audit recorded app.image.delete at 2026-04-02T00:09:00Z",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppBuildLogsExplainsWhenPushWasNotObserved(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_replicas":0},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/build-logs":
			_, _ = w.Write([]byte(`{
				"operation_id":"op_import",
				"operation_status":"failed",
				"job_name":"",
				"available":false,
				"source":"operation.error_message",
				"summary":"build failed",
				"build_strategy":"dockerfile",
				"error_message":"build failed"
			}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_import":
			_, _ = w.Write([]byte(`{"operation":{
				"id":"op_import",
				"tenant_id":"tenant_123",
				"app_id":"app_123",
				"type":"import",
				"status":"failed",
				"error_message":"build failed",
				"desired_source":{
					"type":"github-private",
					"build_strategy":"dockerfile",
					"resolved_image_ref":"registry.example.com/fugue-apps/demo-managed:git-def456"
				},
				"created_at":"2026-04-02T00:00:00Z",
				"updated_at":"2026-04-02T00:01:00Z"
			}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if got := r.URL.Query().Get("app_id"); got != "app_123" {
				t.Fatalf("expected app_id filter app_123, got %q", got)
			}
			_, _ = w.Write([]byte(`{"operations":[
				{
					"id":"op_import",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"import",
					"status":"failed",
					"error_message":"build failed",
					"desired_source":{
						"type":"github-private",
						"build_strategy":"dockerfile",
						"resolved_image_ref":"registry.example.com/fugue-apps/demo-managed:git-def456"
					},
					"created_at":"2026-04-02T00:00:00Z",
					"updated_at":"2026-04-02T00:01:00Z"
				}
			]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":1,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[{"image_ref":"registry.example.com/fugue-apps/demo-managed:git-def456","runtime_image_ref":"registry.example.com/demo-runtime:git-def456","status":"missing","current":true}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app.kubernetes.io/name=demo","container":"demo","groups":[],"warnings":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/control-plane":
			_, _ = w.Write([]byte(`{"control_plane":{"namespace":"fugue-system","release_instance":"fugue","version":"deadbeef","status":"ready","observed_at":"2026-04-02T00:05:00Z","components":[]}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/pods":
			namespace := r.URL.Query().Get("namespace")
			selector := r.URL.Query().Get("label_selector")
			if namespace != "fugue-system" {
				t.Fatalf("expected cluster pod namespace fugue-system, got %q", namespace)
			}
			switch selector {
			case "app.kubernetes.io/component=controller":
				_, _ = w.Write([]byte(`{"cluster_pods":[{"namespace":"fugue-system","name":"fugue-fugue-controller-abc","phase":"Running","ready":true,"node_name":"gcp1","start_time":"2026-04-02T00:00:00Z","containers":[{"name":"controller","image":"ghcr.io/acme/fugue-controller:deadbeef","ready":true,"restart_count":0,"state":"running"}]}]}`))
			case "job-name=build-demo-def":
				_, _ = w.Write([]byte(`{"cluster_pods":[]}`))
			case "fugue.pro/operation-id=op_import":
				_, _ = w.Write([]byte(`{"cluster_pods":[]}`))
			case "app.kubernetes.io/component=registry":
				_, _ = w.Write([]byte(`{"cluster_pods":[{"namespace":"fugue-system","name":"fugue-fugue-registry-abc","phase":"Running","ready":true,"node_name":"gcp1","start_time":"2026-04-02T00:00:00Z","containers":[{"name":"registry","image":"registry:2","ready":true,"restart_count":0,"state":"running"}]}]}`))
			default:
				t.Fatalf("unexpected cluster pod selector %q", selector)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/logs":
			switch r.URL.Query().Get("pod") {
			case "fugue-fugue-controller-abc":
				_, _ = w.Write([]byte(`{"namespace":"fugue-system","pod":"fugue-fugue-controller-abc","container":"controller","logs":"2026-04-02T00:00:10Z builder job failed kind=dockerfile name=build-demo-def namespace=fugue-system image=registry.example.com/fugue-apps/demo-managed:git-def456 operation=op_import err=push denied to registry"}`))
			case "fugue-fugue-registry-abc":
				_, _ = w.Write([]byte(`{"namespace":"fugue-system","pod":"fugue-fugue-registry-abc","container":"registry","logs":"time=\"2026-04-02T00:08:03Z\" level=warn msg=\"manifest unknown\" http.request.uri=\"/v2/fugue-apps/demo-managed/manifests/git-def456\""}`))
			default:
				t.Fatalf("unexpected cluster logs pod %q", r.URL.Query().Get("pod"))
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/audit-events":
			_, _ = w.Write([]byte(`{"audit_events":[]}`))
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
		"app", "logs", "build", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app logs build without publish evidence: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"job_name=build-demo-def",
		"builder_job_state=failed",
		`summary=builder job failed before any registry manifest PUT was observed for managed image "registry.example.com/fugue-apps/demo-managed:git-def456"`,
		`builder_job_evidence=2026-04-02T00:00:10Z builder job failed kind=dockerfile name=build-demo-def namespace=fugue-system image=registry.example.com/fugue-apps/demo-managed:git-def456 operation=op_import err=push denied to registry`,
		"registry_lifecycle_state=push-not-observed",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppOverviewDiagnosisExplainsMissingRuntimeImage(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:10:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:10:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/domains":
			_, _ = w.Write([]byte(`{"domains":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/bindings":
			_, _ = w.Write([]byte(`{"bindings":[],"backing_services":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if got := r.URL.Query().Get("app_id"); got != "app_123" {
				t.Fatalf("expected app_id filter app_123, got %q", got)
			}
			_, _ = w.Write([]byte(`{"operations":[
				{
					"id":"op_deploy",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"deploy",
					"status":"completed",
					"result_message":"deployed revision 14",
					"desired_spec":{"image":"registry.example.com/demo-runtime:sha256","runtime_id":"runtime_managed_shared","replicas":1},
					"created_at":"2026-04-02T00:02:00Z",
					"updated_at":"2026-04-02T00:03:00Z"
				},
				{
					"id":"op_import",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"import",
					"status":"completed",
					"result_message":"queued deploy operation op_deploy",
					"desired_source":{
						"type":"github-private",
						"build_strategy":"dockerfile",
						"compose_service":"runtime",
						"resolved_image_ref":"registry.example.com/demo-managed:sha256"
					},
					"created_at":"2026-04-02T00:00:00Z",
					"updated_at":"2026-04-02T00:01:00Z"
				}
			]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":0,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[{"image_ref":"registry.example.com/demo-managed:sha256","runtime_image_ref":"registry.example.com/demo-runtime:sha256","status":"missing","current":false}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app.kubernetes.io/name=demo","container":"demo","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-9f8d7c6b5","parent":{"kind":"Deployment","name":"demo"},"revision":"14","desired_replicas":1,"current_replicas":1,"ready_replicas":0,"available_replicas":0,"containers":[{"name":"demo","image":"registry.example.com/demo-runtime:sha256"}],"pods":[{"namespace":"tenant-123","name":"demo-9f8d7c6b5-abc12","phase":"Pending","ready":false,"node_name":"gcp1","containers":[{"name":"demo","image":"registry.example.com/demo-runtime:sha256","ready":false,"restart_count":0,"state":"waiting","reason":"ErrImagePull","message":"manifest unknown"}]}],"warnings":[]}],"warnings":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
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
		"app", "overview", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app overview: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"diagnosis",
		"category=runtime-image-missing",
		`summary=build op_import queued deploy op_deploy, but managed image "registry.example.com/demo-managed:sha256" is missing from registry inventory`,
		"service=runtime",
		"build_operation_id=op_import",
		"deploy_operation_id=op_deploy",
		"registry_image_status=missing",
		"latest_pod_group=ReplicaSet/demo-9f8d7c6b5",
		"evidence=registry image status=missing",
		"evidence=pod demo-9f8d7c6b5-abc12 container demo ErrImagePull: manifest unknown",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppOverviewPrefersLatestImportFailureOverRuntimeNoise(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:10:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:10:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/domains":
			_, _ = w.Write([]byte(`{"domains":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/bindings":
			_, _ = w.Write([]byte(`{"bindings":[],"backing_services":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if got := r.URL.Query().Get("app_id"); got != "app_123" {
				t.Fatalf("expected app_id filter app_123, got %q", got)
			}
			_, _ = w.Write([]byte(`{"operations":[
				{
					"id":"op_import_failed",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"import",
					"status":"failed",
					"error_message":"push denied to registry",
					"desired_source":{
						"type":"github-private",
						"build_strategy":"dockerfile",
						"compose_service":"runtime",
						"resolved_image_ref":"registry.example.com/fugue-apps/demo-managed:git-def456"
					},
					"created_at":"2026-04-02T00:05:00Z",
					"updated_at":"2026-04-02T00:06:00Z"
				},
				{
					"id":"op_deploy_old",
					"tenant_id":"tenant_123",
					"app_id":"app_123",
					"type":"deploy",
					"status":"completed",
					"result_message":"deployed revision 17",
					"desired_spec":{"image":"registry.example.com/demo-runtime:old","runtime_id":"runtime_managed_shared","replicas":1},
					"created_at":"2026-04-02T00:02:00Z",
					"updated_at":"2026-04-02T00:03:00Z"
				}
			]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":1,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[{"image_ref":"registry.example.com/fugue-apps/demo-managed:git-def456","runtime_image_ref":"registry.example.com/demo-runtime:git-def456","status":"missing","current":true}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app.kubernetes.io/name=demo","container":"demo","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-7b6c5d4f3","parent":{"kind":"Deployment","name":"demo"},"revision":"17","desired_replicas":1,"current_replicas":1,"ready_replicas":0,"available_replicas":0,"containers":[{"name":"demo","image":"registry.example.com/demo-runtime:old"}],"pods":[{"namespace":"tenant-123","name":"demo-7b6c5d4f3-abc12","phase":"Pending","ready":false,"node_name":"gcp1","containers":[{"name":"demo","image":"registry.example.com/demo-runtime:old","ready":false,"restart_count":0,"state":"waiting","reason":"ImagePullBackOff","message":"manifest unknown"}]}],"warnings":[]}],"warnings":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{"category":"node-disk-pressure","summary":"old runtime pod was waiting on node disk pressure","hint":"old runtime noise"}}`))
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
		"app", "overview", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app overview with import failure priority: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"diagnosis",
		"category=import-failed",
		"summary=push denied to registry",
		"build_operation_id=op_import_failed",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
	if strings.Contains(out, "old runtime noise") {
		t.Fatalf("expected overview to suppress stale runtime diagnosis noise, got %q", out)
	}
}

func TestRunOperationListFiltersProjectTypeAndStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects":
			if got := r.URL.Query().Get("tenant_id"); got != "tenant_123" {
				t.Fatalf("expected tenant_id filter tenant_123, got %q", got)
			}
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if got := r.URL.Query().Get("app_id"); got != "" {
				t.Fatalf("expected operation ls project filter to stay client-side, got app_id=%q", got)
			}
			_, _ = w.Write([]byte(`{"operations":[
				{"id":"op_keep","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"pending","created_at":"2026-04-02T00:03:00Z","updated_at":"2026-04-02T00:03:00Z"},
				{"id":"op_other_status","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","created_at":"2026-04-02T00:02:00Z","updated_at":"2026-04-02T00:02:00Z"},
				{"id":"op_other_type","tenant_id":"tenant_123","app_id":"app_123","type":"import","status":"pending","created_at":"2026-04-02T00:01:00Z","updated_at":"2026-04-02T00:01:00Z"},
				{"id":"op_other_project","tenant_id":"tenant_123","app_id":"app_999","type":"deploy","status":"pending","created_at":"2026-04-02T00:04:00Z","updated_at":"2026-04-02T00:04:00Z"}
			]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[
				{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},
				{"id":"app_999","tenant_id":"tenant_123","project_id":"project_999","name":"other","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}
			]}`))
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
		"operation", "ls",
		"--project", "demo",
		"--type", "deploy",
		"--status", "pending",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run operation ls filtered: %v", err)
	}

	var payload struct {
		Operations []model.Operation `json:"operations"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode operation ls output: %v", err)
	}
	if len(payload.Operations) != 1 || payload.Operations[0].ID != "op_keep" {
		t.Fatalf("expected only op_keep after filtering, got %+v", payload.Operations)
	}
}

func TestRunOperationListTextDefaultsToTwentyRows(t *testing.T) {
	t.Parallel()

	operations := make([]model.Operation, 0, 25)
	for index := 1; index <= 25; index++ {
		stamp := time.Date(2026, time.April, 2, 0, index, 0, 0, time.UTC)
		operations = append(operations, model.Operation{
			ID:        fmt.Sprintf("op_%02d", index),
			TenantID:  "tenant_123",
			AppID:     "app_123",
			Type:      "deploy",
			Status:    "completed",
			CreatedAt: stamp,
			UpdatedAt: stamp,
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if err := json.NewEncoder(w).Encode(map[string]any{"operations": operations}); err != nil {
				t.Fatalf("encode operations: %v", err)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			if err := json.NewEncoder(w).Encode(map[string]any{
				"apps": []model.App{{
					ID:        "app_123",
					TenantID:  "tenant_123",
					ProjectID: "project_123",
					Name:      "demo",
					CreatedAt: time.Date(2026, time.April, 2, 0, 0, 0, 0, time.UTC),
					UpdatedAt: time.Date(2026, time.April, 2, 0, 0, 0, 0, time.UTC),
				}},
			}); err != nil {
				t.Fatalf("encode apps: %v", err)
			}
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
		"operation", "ls",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run operation ls: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"op_25", "op_06"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
	for _, unwanted := range []string{"op_05", "op_01"} {
		if strings.Contains(out, unwanted) {
			t.Fatalf("expected stdout to omit %q under default limit, got %q", unwanted, out)
		}
	}
	if !strings.Contains(stderr.String(), "showing 20 of 25 operations; use --limit, --all, --app, --project, --type, or --status to narrow") {
		t.Fatalf("expected narrowing hint on stderr, got %q", stderr.String())
	}
}

func TestRunAppFailoverRunByNameExecutesFailover(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/failover":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode failover body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123","type":"failover","status":"pending"}}`))
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
		"app", "failover", "run", "demo",
		"--to", "runtime_b",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app failover run: %v", err)
	}

	if gotBody["target_runtime_id"] != "runtime_b" {
		t.Fatalf("expected target_runtime_id runtime_b, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"app=demo", "operation_id=op_123"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppDeployShortcutUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/deploy":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "deploy", "demo",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app deploy shortcut: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"app=demo", "operation_id=op_123"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppRebuildCanClearFiles(t *testing.T) {
	t.Parallel()

	var gotBody rebuildPlanRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/rebuild":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode rebuild body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123"},"build":{"source_type":"github-public","build_strategy":"dockerfile","clear_files":true}}`))
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
		"app", "build", "demo",
		"--clear-files",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app build: %v", err)
	}

	if !gotBody.ClearFiles {
		t.Fatalf("expected clear_files request flag, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "source_type=github-public"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunEnvSetByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		Set    map[string]string `json:"set"`
		Delete []string          `json:"delete"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_123/env":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode env patch body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"env":{"DEBUG":"1","FOO":"bar"},"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"env", "set", "demo",
		"FOO=bar",
		"DEBUG=1",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run env set: %v", err)
	}

	if gotBody.Set["FOO"] != "bar" || gotBody.Set["DEBUG"] != "1" {
		t.Fatalf("unexpected env set body %+v", gotBody.Set)
	}
	if len(gotBody.Delete) != 0 {
		t.Fatalf("expected no delete keys, got %+v", gotBody.Delete)
	}
	out := stdout.String()
	for _, want := range []string{"app=demo", "app_id=app_123", "operation_id=op_123", "DEBUG", "FOO", "1", "bar"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunEnvListTextShowsSources(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/env":
			_, _ = w.Write([]byte(`{"env":{"DB_HOST":"override-db.internal","SERVICE_KEY":"svc-secret"},"entries":[{"key":"DB_HOST","value":"override-db.internal","source":"app","source_ref":"spec.env","overrides":["binding:postgres"]},{"key":"SERVICE_KEY","value":"svc-secret","source":"binding","source_ref":"postgres"}]}`))
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
		"env", "ls", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run env ls: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"app=demo", "env_count=2", "KEY", "VALUE", "SOURCE", "REF", "DB_HOST", "app", "spec.env", "binding", "postgres"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppDatabaseQueryByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		SQL       string `json:"sql"`
		MaxRows   int    `json:"max_rows"`
		TimeoutMS int    `json:"timeout_ms"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/query":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode db query body: %v", err)
			}
			_, _ = w.Write([]byte(`{"database":"demo","host":"db.internal","user":"demo","columns":[{"name":"status","database_type":"TEXT"}],"rows":[{"status":"ok"}],"row_count":1,"max_rows":100,"read_only":true,"duration_ms":12}`))
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
		"app", "db", "query", "demo",
		"--sql", "select status from gateway_request_logs limit 1",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db query: %v", err)
	}

	if gotBody.SQL != "select status from gateway_request_logs limit 1" {
		t.Fatalf("unexpected sql body %+v", gotBody)
	}
	if gotBody.MaxRows != 100 || gotBody.TimeoutMS <= 0 {
		t.Fatalf("expected default max_rows/timeout_ms, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"database=demo", "host=db.internal", "read_only=true", "status", "ok"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppLogsQueryBuildsStructuredSQL(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		SQL       string `json:"sql"`
		MaxRows   int    `json:"max_rows"`
		TimeoutMS int    `json:"timeout_ms"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/query":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode log query body: %v", err)
			}
			_, _ = w.Write([]byte(`{"database":"demo","host":"db.internal","user":"demo","columns":[{"name":"created_at","database_type":"TIMESTAMPTZ"},{"name":"status","database_type":"TEXT"}],"rows":[{"created_at":"2026-04-15T01:00:00Z","status":"500"}],"row_count":1,"max_rows":50,"read_only":true,"duration_ms":5}`))
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
		"app", "logs", "table", "demo",
		"--table", "gateway_request_logs",
		"--since", "2026-04-15T00:00:00Z",
		"--until", "2026-04-15T02:00:00Z",
		"--match", "status=500",
		"--contains", "path=/admin",
		"--column", "created_at",
		"--column", "status",
		"--limit", "50",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app logs table: %v", err)
	}

	for _, want := range []string{
		`from "gateway_request_logs"`,
		`"created_at" >= '2026-04-15T00:00:00Z'::timestamptz`,
		`"created_at" <= '2026-04-15T02:00:00Z'::timestamptz`,
		`"status" = '500'`,
		`"path"::text ILIKE '%/admin%'`,
		`order by "created_at" DESC limit 50`,
	} {
		if !strings.Contains(gotBody.SQL, want) {
			t.Fatalf("expected generated SQL to contain %q, got %q", want, gotBody.SQL)
		}
	}
	if gotBody.MaxRows != 50 || gotBody.TimeoutMS <= 0 {
		t.Fatalf("expected limit/timeout to be forwarded, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"database=demo", "row_count=1", "created_at", "500"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppLogsPodsShowsRolloutGroups(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app.kubernetes.io/name=demo","container":"demo","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-8c9f6d74f7","parent":{"kind":"Deployment","name":"demo"},"revision":"13","desired_replicas":1,"current_replicas":1,"ready_replicas":1,"available_replicas":1,"containers":[{"name":"demo","image":"ghcr.io/example/demo:v2"}],"pods":[{"namespace":"tenant-123","name":"demo-8c9f6d74f7-abc12","phase":"Running","ready":true,"node_name":"gcp1","owner":{"kind":"ReplicaSet","name":"demo-8c9f6d74f7"},"containers":[{"name":"demo","image":"ghcr.io/example/demo:v2","ready":true,"restart_count":0,"state":"running"}]}],"warnings":[]},{"owner_kind":"ReplicaSet","owner_name":"demo-7b7d9f8c5d","parent":{"kind":"Deployment","name":"demo"},"revision":"12","desired_replicas":0,"current_replicas":0,"ready_replicas":0,"available_replicas":0,"containers":[{"name":"demo","image":"ghcr.io/example/demo:v1"}],"pods":[],"warnings":[]}]}`))
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
		"app", "logs", "pods", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app logs pods: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"component=app", "group_count=2", "owner=ReplicaSet/demo-8c9f6d74f7", "revision=13", "demo-8c9f6d74f7-abc12", "owner=ReplicaSet/demo-7b7d9f8c5d", "revision=12"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppRequestUsesEnvHeaderAndQueryFlags(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		Method         string              `json:"method"`
		Path           string              `json:"path"`
		Query          map[string][]string `json:"query"`
		HeadersFromEnv map[string]string   `json:"headers_from_env"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","ports":[8080],"replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/request":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode app request body: %v", err)
			}
			_, _ = w.Write([]byte(`{"method":"GET","url":"http://demo-123/admin/requests?page=2","status":"200 OK","status_code":200,"headers":{"Content-Type":["application/json"]},"body":"{\"items\":[{\"id\":\"req_123\"}]}","body_encoding":"utf-8","body_size":27,"timing":{"total":"8ms"}}`))
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
		"-o", "json",
		"app", "request", "demo", "GET", "/admin/requests",
		"--query", "page=2",
		"--header-from-env", "X-Service-Key=SERVICE_KEY",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app request: %v", err)
	}

	if gotBody.Method != "GET" || gotBody.Path != "/admin/requests" {
		t.Fatalf("unexpected app request body %+v", gotBody)
	}
	if got := gotBody.Query["page"]; len(got) != 1 || got[0] != "2" {
		t.Fatalf("expected page=2 in query body, got %+v", gotBody.Query)
	}
	if gotBody.HeadersFromEnv["X-Service-Key"] != "SERVICE_KEY" {
		t.Fatalf("expected header-from-env to be forwarded, got %+v", gotBody.HeadersFromEnv)
	}
	var response struct {
		StatusCode int    `json:"status_code"`
		URL        string `json:"url"`
		Body       string `json:"body"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatalf("decode app request output: %v", err)
	}
	if response.StatusCode != 200 || response.URL != "http://demo-123/admin/requests?page=2" || !strings.Contains(response.Body, `"req_123"`) {
		t.Fatalf("unexpected app request output %+v", response)
	}
}

func TestRunDomainAddByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/domains":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode domain add body: %v", err)
			}
			_, _ = w.Write([]byte(`{"domain":{"hostname":"www.example.com","status":"pending","route_target":"target.example.net","updated_at":"2026-04-02T00:00:00Z","created_at":"2026-04-02T00:00:00Z"},"availability":{"hostname":"www.example.com","valid":true,"available":true,"current":false},"already_current":false}`))
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
		"domain", "add", "demo", "www.example.com",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run domain add: %v", err)
	}

	if gotBody["hostname"] != "www.example.com" {
		t.Fatalf("expected hostname www.example.com, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "hostname=www.example.com", "route_target=target.example.net", "available=true"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunFilesWriteByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		Files []struct {
			Path    string `json:"path"`
			Content string `json:"content"`
			Secret  bool   `json:"secret"`
			Mode    int32  `json:"mode"`
		} `json:"files"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPut && r.URL.Path == "/v1/apps/app_123/files":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode files put body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"files":[{"path":"/app/config.yaml","content":"port: 8080\n","secret":false,"mode":420}],"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"files", "write", "demo", "/app/config.yaml",
		"--content", "port: 8080\n",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run files write: %v", err)
	}

	if len(gotBody.Files) != 1 {
		t.Fatalf("expected one file in body, got %+v", gotBody.Files)
	}
	if gotBody.Files[0].Path != "/app/config.yaml" {
		t.Fatalf("expected file path /app/config.yaml, got %+v", gotBody.Files[0])
	}
	if gotBody.Files[0].Content != "port: 8080\n" {
		t.Fatalf("expected file content to be forwarded, got %+v", gotBody.Files[0])
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "/app/config.yaml", "644"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunFilesReadByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/files":
			_, _ = w.Write([]byte(`{"files":[{"path":"/app/config.yaml","content":"port: 8080\n","secret":false,"mode":420}]}`))
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
		"files", "read", "demo", "/app/config.yaml",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run files read: %v", err)
	}

	if got := stdout.String(); got != "port: 8080\n" {
		t.Fatalf("unexpected files read stdout %q", got)
	}
}

func TestRunWorkspaceReadUsesRelativePathWithinWorkspace(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"workspace":{"mount_path":"/workspace"}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"workspace":{"mount_path":"/workspace"}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/filesystem/file":
			if got := r.URL.Query().Get("path"); got != "/workspace/notes/hello.txt" {
				t.Fatalf("expected workspace path /workspace/notes/hello.txt, got %q", got)
			}
			_, _ = w.Write([]byte(`{"component":"app","pod":"demo-pod","path":"/workspace/notes/hello.txt","workspace_root":"/workspace","content":"hello from fugue\n","encoding":"utf-8","size":17,"mode":420,"modified_at":"2026-04-02T00:00:00Z","truncated":false}`))
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
		"workspace", "read", "demo", "notes/hello.txt",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run workspace read: %v", err)
	}

	if got := stdout.String(); got != "hello from fugue\n" {
		t.Fatalf("unexpected workspace read stdout %q", got)
	}
}

func TestRunAppFilesystemListFallsBackToLiveFilesystem(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/filesystem/tree":
			if got := r.URL.Query().Get("path"); got != "/" {
				t.Fatalf("expected live filesystem path /, got %q", got)
			}
			if got := r.URL.Query().Get("component"); got != "app" {
				t.Fatalf("expected component=app, got %q", got)
			}
			_, _ = w.Write([]byte(`{"component":"app","pod":"demo-pod","path":"/","depth":1,"workspace_root":"/","entries":[{"name":"tmp","path":"/tmp","kind":"dir","size":0,"mode":493,"modified_at":"2026-04-02T00:00:00Z","has_children":true}]}`))
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
		"app", "fs", "ls", "demo", "/",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app fs ls live: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{`"workspace_root": "/"`, `"/tmp"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected live fs output to contain %q, got %q", want, out)
		}
	}
}

func TestRunAPIRequestShowsRawResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/apps" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("expected bearer token auth, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Server-Timing", "app;dur=12.3")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte(`{"error":"short and stout"}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"api", "request", "GET", "/v1/apps",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run api request: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{`"status_code": 418`, `"server_timing": "app;dur=12.3"`, `short and stout`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected api request output to contain %q, got %q", want, out)
		}
	}
}

func TestRunDiagnoseTimingCapturesRequests(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			w.Header().Set("Server-Timing", "apps;dur=4.5")
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
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
		"-o", "json",
		"diagnose", "timing", "--", "app", "ls",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run diagnose timing: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{`"command": [`, `/v1/apps`, `"status_code": 200`, `"server_timing": "apps;dur=4.5"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected diagnose timing output to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminUsersListUsesWebSnapshot(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/fugue/admin/pages/users" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("expected bearer token auth, got %q", got)
		}
		_, _ = w.Write([]byte(`{
			"enrichmentState":"pending",
			"errors":[],
			"summary":{"adminCount":1,"blockedCount":0,"deletedCount":0,"userCount":1},
			"users":[{
				"billing":{"balanceLabel":"","limitLabel":"Loading billing…","loadError":"","loading":true,"monthlyEstimateLabel":"","statusLabel":"","statusReason":""},
				"canBlock":false,
				"canDelete":false,
				"canDemoteAdmin":false,
				"canPromoteToAdmin":false,
				"canUnblock":false,
				"email":"user@example.com",
				"isAdmin":true,
				"lastLoginExact":"2026-04-02T00:00:00Z",
				"lastLoginLabel":"today",
				"name":"User",
				"provider":"GitHub",
				"serviceCount":2,
				"status":"Active",
				"statusTone":"positive",
				"usage":{"cpuLabel":"200m cpu","diskLabel":"1 GiB","imageLabel":"500 MiB","loading":false,"memoryLabel":"512 MiB","serviceCount":2,"serviceCountLabel":"2 services"},
				"verified":true
			}]
		}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--web-base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"admin", "users", "ls",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin users ls: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{`"enrichmentState": "pending"`, `"email": "user@example.com"`, `"serviceCount": 2`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected admin users output to contain %q, got %q", want, out)
		}
	}
}

func TestRunWebDiagnoseUsesAliasTarget(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/fugue/admin/pages/users" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--web-base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"web", "diagnose", "admin-users",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run web diagnose: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{`/api/fugue/admin/pages/users`, `"status_code": 200`, `\"ok\":true`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected web diagnose output to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminRuntimeAccessShowsSharingGrants(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","access_mode":"shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_b":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","access_mode":"shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_b/sharing":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","access_mode":"shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"grants":[{"runtime_id":"runtime_b","tenant_id":"tenant_999","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-03T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Owner","slug":"owner"},{"id":"tenant_999","name":"Acme","slug":"acme"}]}`))
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
		"admin", "runtime", "access", "show", "runtime-b",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin runtime access: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"runtime=runtime-b", "access_mode=shared", "grants=1", "Acme", "tenant_999"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminNodeUpdaterTaskCreateTargetsRuntime(t *testing.T) {
	t.Parallel()

	var gotBody nodeUpdateTaskCreateRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_b":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/node-update-tasks":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode node update task body: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"task":{"id":"task_123","node_updater_id":"updater_123","runtime_id":"runtime_b","type":"diagnose-node","status":"pending","payload":{"reason":"manual"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"admin", "node-updater", "task", "create",
		"--runtime", "runtime-b",
		"--type", "diagnose-node",
		"--payload", "reason=manual",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run node-updater task create: %v", err)
	}

	if gotBody.RuntimeID != "runtime_b" || gotBody.Type != model.NodeUpdateTaskTypeDiagnoseNode || gotBody.Payload["reason"] != "manual" {
		t.Fatalf("unexpected node update task request %+v", gotBody)
	}
	for _, want := range []string{"task=task_123", "runtime=runtime_b", "type=diagnose-node", "status=pending"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, stdout.String())
		}
	}
}

func TestRunAdminDiscoveryBundleShow(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/discovery/bundle" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{
			"schema_version":"v1",
			"generation":"gen_123",
			"generated_at":"2026-04-02T00:00:00Z",
			"valid_until":"2026-04-02T01:00:00Z",
			"issuer":"fugue",
			"api_endpoints":[{"name":"api","url":"https://api.example.com"}],
			"kubernetes":[{"name":"main","server":"https://k8s.example.com"}],
			"registry":[{"name":"registry","push_base":"registry.example.com/fugue"}]
		}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"admin", "discovery", "bundle", "show",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run discovery bundle show: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"generation=gen_123", "[api_endpoints]", "https://api.example.com", "[kubernetes]", "https://k8s.example.com"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppOverviewRedactsSecretsByDefault(t *testing.T) {
	t.Parallel()

	server := newAppOverviewSecretFixtureServer(t)
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "overview", "demo",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app overview: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"repo_auth_token": "[redacted]"`,
		`"DB_PASSWORD": "[redacted]"`,
		`"content": "[redacted]"`,
		`"seed_content": "[redacted]"`,
		`"password": "[redacted]"`,
		`"DATABASE_URL": "[redacted]"`,
		`"OP_SECRET": "[redacted]"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected redacted overview output to contain %q, got %q", want, out)
		}
	}
	for _, secret := range []string{
		"repo-token-123",
		"db-secret-123",
		"TOKEN=runtime-secret",
		"seed-secret-123",
		"service-password-123",
		"postgres://demo:binding-secret-123@db",
		"operation-secret-123",
	} {
		if strings.Contains(out, secret) {
			t.Fatalf("expected overview output to redact %q, got %q", secret, out)
		}
	}
}

func TestRunAppOverviewShowSecretsOptIn(t *testing.T) {
	t.Parallel()

	server := newAppOverviewSecretFixtureServer(t)
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "overview", "demo",
		"--show-secrets",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app overview --show-secrets: %v", err)
	}

	out := stdout.String()
	for _, secret := range []string{
		`"repo_auth_token": "repo-token-123"`,
		`"DB_PASSWORD": "db-secret-123"`,
		`"content": "TOKEN=runtime-secret\n"`,
		`"seed_content": "seed-secret-123"`,
		`"password": "service-password-123"`,
		`"DATABASE_URL": "postgres://demo:binding-secret-123@db"`,
		`"OP_SECRET": "operation-secret-123"`,
	} {
		if !strings.Contains(out, secret) {
			t.Fatalf("expected overview output to contain %q, got %q", secret, out)
		}
	}
}

func TestRunOperationListRedactsSecretsByDefault(t *testing.T) {
	t.Parallel()

	server := newOperationSecretFixtureServer(t)
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"operation", "ls",
		"--app", "demo",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run operation ls: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"password": "[redacted]"`,
		`"OP_SECRET": "[redacted]"`,
		`"repo_auth_token": "[redacted]"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected redacted operation output to contain %q, got %q", want, out)
		}
	}
	for _, secret := range []string{
		"operation-db-password-123",
		"operation-secret-123",
		"operation-repo-token-123",
	} {
		if strings.Contains(out, secret) {
			t.Fatalf("expected operation output to redact %q, got %q", secret, out)
		}
	}
}

func TestRunOperationShowSecretsOptIn(t *testing.T) {
	t.Parallel()

	server := newOperationSecretFixtureServer(t)
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"operation", "show", "op_123",
		"--show-secrets",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run operation show --show-secrets: %v", err)
	}

	out := stdout.String()
	for _, secret := range []string{
		`"password": "operation-db-password-123"`,
		`"OP_SECRET": "operation-secret-123"`,
		`"repo_auth_token": "operation-repo-token-123"`,
	} {
		if !strings.Contains(out, secret) {
			t.Fatalf("expected operation output to contain %q, got %q", secret, out)
		}
	}
}

func TestRunOperationExplainRendersBuilderPlacementDetails(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123":
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"import","status":"failed","error_message":"select builder placement: no eligible builder nodes for profile heavy","created_at":"2026-04-22T10:00:00Z","updated_at":"2026-04-22T10:10:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{
				"category":"builder-no-eligible-nodes",
				"summary":"no builder nodes passed readiness, taint, disk-pressure, label, or stats checks for builder nodes profile \"heavy\"",
				"hint":"Check builder node policy.",
				"evidence":["active builder reservations: reservation-a@gcp1","active builder locks: gcp1 held by build-demo"],
				"builder_placement":{
					"profile":"heavy",
					"build_strategy":"dockerfile",
					"demand":{"cpu_milli":750,"memory_bytes":1073741824,"ephemeral_bytes":3221225472},
					"reservations":[{"name":"reservation-a","node_name":"gcp1","demand":{"cpu_milli":750,"memory_bytes":1073741824,"ephemeral_bytes":3221225472}}],
					"locks":[{"name":"lock-gcp1","node_name":"gcp1","holder_identity":"build-demo"}],
					"nodes":[
						{"node_name":"gcp1","hostname":"host-a","ready":true,"disk_pressure":true,"eligible":false,"reasons":["DiskPressure=True"]},
						{"node_name":"gcp2","hostname":"host-b","ready":true,"disk_pressure":false,"eligible":true,"rank":1,"available":{"cpu_milli":1240,"memory_bytes":2147483648,"ephemeral_bytes":8589934592},"remaining":{"cpu_milli":490,"memory_bytes":1073741824,"ephemeral_bytes":5368709120}}
					]
				}
			}}`))
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
		"operation", "explain", "op_123",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run operation explain: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"diagnosis_category=builder-no-eligible-nodes",
		"evidence=active builder reservations: reservation-a@gcp1",
		"[builder_reservations]",
		"reservation-a",
		"[builder_locks]",
		"build-demo",
		"[builder_nodes]",
		"gcp1",
		"pressure",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminClusterNodesShowsPolicyColumns(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/cluster/nodes" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"cluster_nodes":[{"name":"gcp1","status":"ready","runtime_id":"runtime_managed_shared","roles":["control-plane"],"region":"us-west1","cpu":{"usage_percent":42},"memory":{"usage_percent":67},"policy":{"allow_builds":true,"allow_shared_pool":false,"node_mode":"managed-owned","desired_control_plane_role":"candidate","effective_builds":false,"effective_shared_pool":true,"effective_control_plane_role":"member"},"created_at":"2026-04-02T00:00:00Z"}]}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"admin", "cluster", "nodes",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin cluster nodes: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"BUILD", "SHARED", "MODE", "CP", "on/off", "off/on", "managed-owned", "candidate/member"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminClusterNodePolicyCommands(t *testing.T) {
	t.Parallel()

	nodePolicyJSON := `{
		"node_name":"gcp1",
		"runtime_id":"runtime_us",
		"machine_id":"machine_gcp1",
		"ready":true,
		"disk_pressure":false,
		"node_schedulable":true,
		"reconciled":false,
		"reconcile_reasons":["node policy labels drift from desired policy"],
		"labels":{"fugue.io/role.edge":"true","fugue.io/schedulable":"true"},
		"taints":[{"key":"fugue.io/dedicated","value":"edge","effect":"NoSchedule"}],
		"policy":{
			"allow_app_runtime":false,
			"allow_builds":false,
			"allow_shared_pool":false,
			"allow_edge":true,
			"allow_dns":false,
			"allow_internal_maintenance":false,
			"node_mode":"managed-owned",
			"node_health":"ready",
			"desired_control_plane_role":"none",
			"effective_app_runtime":false,
			"effective_builds":false,
			"effective_shared_pool":false,
			"effective_edge":true,
			"effective_dns":false,
			"effective_internal_maintenance":false,
			"effective_schedulable":true,
			"effective_control_plane_role":"none"
		}
	}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/node-policies":
			_, _ = w.Write([]byte(`{"node_policies":[` + nodePolicyJSON + `]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/node-policies/gcp1":
			_, _ = w.Write([]byte(`{"node_policy":` + nodePolicyJSON + `}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/node-policies/status":
			_, _ = w.Write([]byte(`{"summary":{"total":1,"reconciled":0,"drifted":1,"ready":1,"disk_pressure":0,"blocked_by_health":0},"node_policies":[` + nodePolicyJSON + `]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	for _, tc := range []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "ls",
			args: []string{"admin", "cluster", "node-policy", "ls"},
			want: []string{"NODE", "EDGE", "RECONCILED", "gcp1", "on/on", "node policy labels drift"},
		},
		{
			name: "get",
			args: []string{"admin", "cluster", "node-policy", "get", "gcp1"},
			want: []string{"node=gcp1", "[policy]", "edge=on/on", "[labels]", "fugue.io/role.edge=true", "[taints]", "fugue.io/dedicated"},
		},
		{
			name: "status",
			args: []string{"admin", "cluster", "node-policy", "status"},
			want: []string{"total=1", "drifted=1", "[node_policies]", "gcp1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			args := append([]string{"--base-url", server.URL, "--token", "token"}, tc.args...)
			if err := runWithStreams(args, &stdout, &stderr); err != nil {
				t.Fatalf("run %v: %v", tc.args, err)
			}
			out := stdout.String()
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Fatalf("expected stdout to contain %q, got %q", want, out)
				}
			}
		})
	}
}

func TestRunRuntimeDoctorManagedSharedIncludesLocationNodes(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","status":"active","endpoint":"https://shared.example.com","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_managed_shared":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","status":"active","endpoint":"https://shared.example.com","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/nodes":
			_, _ = w.Write([]byte(`{"cluster_nodes":[{"name":"gcp3","status":"ready","runtime_id":"runtime_managed_shared_loc_gcp3","conditions":{"Ready":{"status":"True"}},"created_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
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
		"runtime", "doctor", "shared",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run runtime doctor: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"runtime=shared", "cluster_nodes=1", "gcp3"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminClusterStatusShowsDeployWorkflow(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/control-plane":
			_, _ = w.Write([]byte(`{
				"control_plane":{
					"namespace":"fugue-system",
					"release_instance":"fugue",
					"version":"deadbeef",
					"live_version":"deadbeef",
					"status":"ready",
					"observed_at":"2026-04-14T00:00:00Z",
					"deploy_workflow":{
						"repository":"acme/fugue",
						"workflow":"deploy-control-plane.yml",
						"status":"completed",
						"conclusion":"success",
						"run_number":42,
						"head_sha":"deadbeef",
						"head_branch":"main",
						"html_url":"https://github.com/acme/fugue/actions/runs/42",
						"observed_at":"2026-04-14T00:00:00Z"
					},
					"components":[
						{
							"component":"api",
							"deployment_name":"fugue-fugue-api",
							"image":"ghcr.io/acme/fugue-api:deadbeef",
							"image_repository":"ghcr.io/acme/fugue-api",
							"image_tag":"deadbeef",
							"observed_image_tags":["deadbeef"],
							"status":"ready",
							"desired_replicas":2,
							"ready_replicas":2,
							"updated_replicas":2,
							"available_replicas":2
						}
					]
				}
			}`))
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
		"admin", "cluster", "status",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin cluster status: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"deploy_workflow_repository=acme/fugue",
		"deploy_workflow=deploy-control-plane.yml",
		"deploy_workflow_status=completed",
		"deploy_workflow_run_number=42",
		"deploy_workflow_head_sha=deadbeef",
		"live_version=deadbeef",
		"LIVE_TAGS",
		"ghcr.io/acme/fugue-api:deadbeef",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunSourceUploadShowDisplaysMetadataAndReferences(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/source-uploads/upload_123":
			_, _ = w.Write([]byte(`{
				"source_upload":{
					"upload":{
						"id":"upload_123",
						"tenant_id":"tenant_123",
						"filename":"demo.tgz",
						"content_type":"application/gzip",
						"sha256":"abc123",
						"size_bytes":12345,
						"created_at":"2026-04-15T00:00:00Z",
						"updated_at":"2026-04-15T00:00:00Z"
					},
					"references":[
						{
							"operation_id":"op_import",
							"operation_type":"import",
							"operation_status":"completed",
							"app_id":"app_123",
							"app_name":"demo",
							"build_strategy":"dockerfile",
							"source_dir":"services/runtime",
							"resolved_image_ref":"registry.example.com/fugue-apps/demo:git-abc123",
							"created_at":"2026-04-15T00:01:00Z",
							"updated_at":"2026-04-15T00:02:00Z"
						}
					]
				}
			}`))
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
		"source-upload", "show", "upload_123",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run source-upload show: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"upload_id=upload_123",
		"archive_sha256=abc123",
		"OPERATION",
		"op_import",
		"demo",
		"services/runtime",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminClusterPodsListsSystemPods(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/pods":
			if got := r.URL.Query().Get("namespace"); got != "kube-system" {
				t.Fatalf("expected namespace filter kube-system, got %q", got)
			}
			_, _ = w.Write([]byte(`{"cluster_pods":[{"namespace":"kube-system","name":"coredns-abc","phase":"Running","ready":true,"node_name":"gcp1","owner":{"kind":"ReplicaSet","name":"coredns-85f7d9b4"},"containers":[{"name":"coredns","image":"coredns/coredns:v1.11.1","ready":true,"restart_count":1,"state":"running"}]}]}`))
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
		"admin", "cluster", "pods",
		"--namespace", "kube-system",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin cluster pods: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"kube-system", "coredns-abc", "gcp1", "ReplicaSet/coredns-85f7d9b4"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminClusterExecForwardsRetryFlags(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		Namespace    string   `json:"namespace"`
		Pod          string   `json:"pod"`
		Command      []string `json:"command"`
		Retries      int      `json:"retries"`
		RetryDelayMS int      `json:"retry_delay_ms"`
		TimeoutMS    int      `json:"timeout_ms"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/cluster/exec" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode cluster exec body: %v", err)
		}
		_, _ = w.Write([]byte(`{"namespace":"kube-system","pod":"coredns-abc","command":["cat","/etc/resolv.conf"],"output":"10.43.0.10\n","attempt_count":2}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"admin", "cluster", "exec",
		"--namespace", "kube-system",
		"--pod", "coredns-abc",
		"--retries", "4",
		"--retry-delay", "500ms",
		"--timeout", "2m",
		"--",
		"cat", "/etc/resolv.conf",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin cluster exec: %v", err)
	}

	if gotBody.Namespace != "kube-system" || gotBody.Pod != "coredns-abc" {
		t.Fatalf("unexpected exec target %+v", gotBody)
	}
	if gotBody.Retries != 4 || gotBody.RetryDelayMS != 500 || gotBody.TimeoutMS != 120000 {
		t.Fatalf("expected retry fields to be forwarded, got %+v", gotBody)
	}
	if got := strings.TrimSpace(stdout.String()); got != "10.43.0.10" {
		t.Fatalf("unexpected exec stdout %q", got)
	}
	if !strings.Contains(stderr.String(), "cluster_exec_attempts=2") {
		t.Fatalf("expected retry note on stderr, got %q", stderr.String())
	}
}

func TestRunAdminClusterNetWebSocketResolvesAppAndForwardsHeaders(t *testing.T) {
	t.Parallel()

	var gotBody clusterWebSocketProbeRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","route":{"public_url":"https://demo.apps.example.com"},"spec":{"runtime_id":"runtime_managed_shared","replicas":1,"ports":[3000]},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/cluster/net/websocket":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode websocket probe body: %v", err)
			}
			_, _ = w.Write([]byte(`{
  "app_id":"app_123",
  "app_name":"demo",
  "path":"/ws",
  "route_configured":true,
  "service":{"target":"service","url":"http://demo.svc.cluster.local:3000/ws","status":"101 Switching Protocols","status_code":101,"upgraded":true,"duration_ms":12},
  "public_route":{"target":"public_route","url":"https://demo.apps.example.com/ws","status":"502 Bad Gateway","status_code":502,"upgraded":false,"duration_ms":18,"body_preview":"upstream app is unavailable"},
  "conclusion_code":"public_route_502_service_ok",
  "conclusion":"WebSocket handshake succeeded directly against the app service, but the public route returned 502. The proxy layer is failing before the request reaches the app.",
  "observed_at":"2026-04-15T00:00:00Z"
}`))
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
		"admin", "cluster", "net", "websocket", "demo",
		"--path", "/ws",
		"--header", "Cookie=session=abc",
		"--timeout", "12s",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin cluster net websocket: %v", err)
	}

	if gotBody.AppID != "app_123" || gotBody.Path != "/ws" || gotBody.TimeoutMS != 12000 {
		t.Fatalf("unexpected websocket probe request %+v", gotBody)
	}
	if gotBody.Headers["Cookie"] != "session=abc" {
		t.Fatalf("expected header to be forwarded, got %+v", gotBody.Headers)
	}
	out := stdout.String()
	for _, want := range []string{
		"conclusion_code=public_route_502_service_ok",
		"service",
		"status_code=101",
		"public_route",
		"status_code=502",
		"body_preview=upstream app is unavailable",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func newAppOverviewSecretFixtureServer(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","source":{"type":"github-private","repo_url":"https://github.com/acme/demo","repo_auth_token":"repo-token-123"},"spec":{"image":"ghcr.io/acme/demo:latest","runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","source":{"type":"github-private","repo_url":"https://github.com/acme/demo","repo_auth_token":"repo-token-123"},"spec":{"image":"ghcr.io/acme/demo:latest","runtime_id":"runtime_managed_shared","replicas":1,"env":{"DB_PASSWORD":"db-secret-123"},"files":[{"path":"/app/.env","content":"TOKEN=runtime-secret\n","secret":true}],"persistent_storage":{"storage_size":"10Gi","mounts":[{"kind":"file","path":"/data/seed.txt","seed_content":"seed-secret-123","secret":true}]},"postgres":{"database":"demo","user":"demo","password":"service-password-123"}},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/domains":
			_, _ = w.Write([]byte(`{"domains":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/bindings":
			_, _ = w.Write([]byte(`{"bindings":[{"id":"binding_123","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_123","alias":"postgres","env":{"DATABASE_URL":"postgres://demo:binding-secret-123@db"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"backing_services":[{"id":"svc_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_managed_shared","database":"demo","user":"demo","password":"service-password-123"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			_, _ = w.Write([]byte(`{"operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","desired_source":{"type":"github-private","repo_url":"https://github.com/acme/demo","repo_auth_token":"operation-repo-token-123"},"desired_spec":{"image":"ghcr.io/acme/demo:next","runtime_id":"runtime_managed_shared","replicas":1,"env":{"OP_SECRET":"operation-secret-123"},"postgres":{"database":"demo","user":"demo","password":"operation-db-password-123"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":1,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app.kubernetes.io/name=demo","container":"demo","groups":[{"owner_kind":"ReplicaSet","owner_name":"demo-8c9f6d74f7","parent":{"kind":"Deployment","name":"demo"},"revision":"13","desired_replicas":1,"current_replicas":1,"ready_replicas":1,"available_replicas":1,"containers":[{"name":"demo","image":"ghcr.io/acme/demo:latest"}],"pods":[{"namespace":"tenant-123","name":"demo-8c9f6d74f7-abc12","phase":"Running","ready":true,"node_name":"gcp1","owner":{"kind":"ReplicaSet","name":"demo-8c9f6d74f7"},"containers":[{"name":"demo","image":"ghcr.io/acme/demo:latest","ready":true,"restart_count":0,"state":"running"}]}],"warnings":[]}],"warnings":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
}

func newOperationSecretFixtureServer(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"image":"ghcr.io/acme/demo:latest","runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if got := r.URL.Query().Get("app_id"); got != "app_123" {
				t.Fatalf("expected app_id filter app_123, got %q", got)
			}
			_, _ = w.Write([]byte(`{"operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","desired_source":{"type":"github-private","repo_url":"https://github.com/acme/demo","repo_auth_token":"operation-repo-token-123"},"desired_spec":{"image":"ghcr.io/acme/demo:next","runtime_id":"runtime_managed_shared","replicas":1,"env":{"OP_SECRET":"operation-secret-123"},"postgres":{"database":"demo","user":"demo","password":"operation-db-password-123"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123":
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","desired_source":{"type":"github-private","repo_url":"https://github.com/acme/demo","repo_auth_token":"operation-repo-token-123"},"desired_spec":{"image":"ghcr.io/acme/demo:next","runtime_id":"runtime_managed_shared","replicas":1,"env":{"OP_SECRET":"operation-secret-123"},"postgres":{"database":"demo","user":"demo","password":"operation-db-password-123"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
}
