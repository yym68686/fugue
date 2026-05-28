package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestRunFindRendersResultsAndFollowupCommands(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/search" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("q"); got != "uni-api-web" {
			t.Fatalf("expected query uni-api-web, got %q", got)
		}
		if got := r.URL.Query().Get("types"); got != "app,project" {
			t.Fatalf("expected app,project types, got %q", got)
		}
		if got := r.URL.Query().Get("limit"); got != "10" {
			t.Fatalf("expected limit 10, got %q", got)
		}
		_ = json.NewEncoder(w).Encode(model.SearchResponse{
			Query: "uni-api-web",
			Limit: 10,
			Results: []model.SearchResult{
				{
					Kind:          "app",
					ID:            "app_123",
					Name:          "uni-api-web-api",
					TenantID:      "tenant_123",
					TenantName:    "Ming Workspace",
					ProjectID:     "project_123",
					ProjectName:   "uni-api-web",
					AppID:         "app_123",
					AppName:       "uni-api-web-api",
					Status:        "ready",
					PublicURL:     "https://uni-api.example.com",
					MatchedFields: []string{"name"},
					Score:         100,
				},
			},
		})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"find", "uni-api-web",
		"--type", "app,project",
		"--limit", "10",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run find: %v", err)
	}
	out := stdout.String()
	for _, want := range []string{"uni-api-web-api", "https://uni-api.example.com", "next_commands", "fugue --tenant 'Ming Workspace' --project 'uni-api-web' app overview 'uni-api-web-api'"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppListSearchSendsServerSideFilters(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			if got := r.URL.Query().Get("q"); got != "uni" {
				t.Fatalf("expected q=uni, got %q", got)
			}
			if got := r.URL.Query().Get("domain"); got != "uni.example.com" {
				t.Fatalf("expected domain filter, got %q", got)
			}
			if got := r.URL.Query().Get("source_ref"); got != "github.com/example/uni" {
				t.Fatalf("expected source_ref filter, got %q", got)
			}
			if got := r.URL.Query().Get("include_live_status"); got != "false" {
				t.Fatalf("expected include_live_status=false, got %q", got)
			}
			if got := r.URL.Query().Get("include_resource_usage"); got != "false" {
				t.Fatalf("expected include_resource_usage=false, got %q", got)
			}
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"uni-api-web-api","spec":{"runtime_id":"runtime_123","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "ls",
		"--all-tenants",
		"--search", "uni",
		"--domain", "uni.example.com",
		"--source-ref", "github.com/example/uni",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app ls: %v", err)
	}
	if !strings.Contains(stdout.String(), "uni-api-web-api") {
		t.Fatalf("expected app list output, got %q", stdout.String())
	}
}

func TestRunProjectOverviewSearchesAllProjectsWhenTenantIsAmbiguous(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_a","name":"A","slug":"a"},{"id":"tenant_b","name":"B","slug":"b"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects":
			if got := r.URL.Query().Get("tenant_id"); got != "" {
				t.Fatalf("expected cross-tenant project lookup, got tenant_id=%q", got)
			}
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_b","name":"uni-api-web","slug":"uni-api-web","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/projects/project_123":
			_, _ = w.Write([]byte(`{"project_id":"project_123","project_name":"uni-api-web","project":{"id":"project_123","tenant_id":"tenant_b","name":"uni-api-web","slug":"uni-api-web","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"apps":[],"operations":[],"cluster_nodes":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/gallery":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","name":"uni-api-web","app_count":0,"service_count":0,"lifecycle":{"label":"idle","live":false,"sync_mode":"auto","tone":"neutral"},"resource_usage_snapshot":{},"service_badges":[]}]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"project", "overview", "uni-api-web",
		"--with-services=false",
		"--with-domains=false",
		"--with-db=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run project overview: %v", err)
	}
	if !strings.Contains(stdout.String(), "project=uni-api-web") {
		t.Fatalf("expected project overview output, got %q", stdout.String())
	}
}

func TestRunAppDatabaseTablesBuildsCatalogQuery(t *testing.T) {
	t.Parallel()

	var querySQL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_123","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/query":
			var request struct {
				SQL string `json:"sql"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode query request: %v", err)
			}
			querySQL = request.SQL
			_, _ = w.Write([]byte(`{"database":"app","host":"db","user":"app","columns":[{"name":"table_schema"},{"name":"table_name"},{"name":"table_type"}],"rows":[{"table_schema":"public","table_name":"users","table_type":"BASE TABLE"}],"row_count":1,"max_rows":250,"read_only":true,"duration_ms":1}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "db", "tables", "demo", "users",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db tables: %v", err)
	}
	if !strings.Contains(querySQL, "information_schema.tables") || !strings.Contains(querySQL, "users") {
		t.Fatalf("expected catalog query with users filter, got %q", querySQL)
	}
	if !strings.Contains(stdout.String(), "public") || !strings.Contains(stdout.String(), "users") {
		t.Fatalf("expected table output, got %q", stdout.String())
	}
}
