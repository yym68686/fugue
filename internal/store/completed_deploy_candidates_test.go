package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestCompletedDeployCandidatesPageThroughEqualCompletionTimes(t *testing.T) {
	for _, backend := range []string{"json", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			dsn := ""
			if backend == "postgres" {
				dsn = os.Getenv("FUGUE_TEST_DATABASE_URL")
				if dsn == "" {
					t.Skip("dedicated PostgreSQL fixture not configured")
				}
				if !strings.Contains(dsn, "fugue_test") {
					t.Fatal("dedicated fugue_test database required")
				}
			}
			st := New(t.TempDir()+"/state.json", dsn)
			if err := st.Init(); err != nil {
				t.Fatal(err)
			}
			suffix := fmt.Sprint(time.Now().UnixNano())
			tenant := "tenant-" + suffix
			appID := "app-" + suffix
			foreignTenant := tenant + "-foreign"
			if backend == "postgres" {
				owner, err := st.CreateTenant("Candidate page " + suffix)
				if err != nil {
					t.Fatal(err)
				}
				project, err := st.CreateProject(owner.ID, "candidate-pages", "")
				if err != nil {
					t.Fatal(err)
				}
				app, err := st.CreateApp(owner.ID, project.ID, "candidate-pages", "", model.AppSpec{Image: "registry.example.test/app:stable", Replicas: 1, RuntimeID: "runtime_managed_shared"})
				if err != nil {
					t.Fatal(err)
				}
				other, err := st.CreateTenant("Other candidate owner " + suffix)
				if err != nil {
					t.Fatal(err)
				}
				tenant, appID, foreignTenant = owner.ID, app.ID, other.ID
			}
			now := time.Now().UTC().Truncate(time.Microsecond)
			var fixture []model.Operation
			count := CompletedDeployCandidatePageSize*2 + 3
			for i := 0; i < count; i++ {
				fixture = append(fixture, model.Operation{ID: fmt.Sprintf("candidate-%s-%03d", suffix, i), TenantID: tenant, AppID: appID, Type: model.OperationTypeDeploy, Status: model.OperationStatusCompleted, CreatedAt: now.Add(-time.Minute), UpdatedAt: now, CompletedAt: &now, DesiredSpec: &model.AppSpec{Image: "registry.example.test/app:candidate", Replicas: 1}})
			}
			before := now.Add(-2 * time.Minute)
			for _, op := range []model.Operation{
				{ID: "old-" + suffix, TenantID: tenant, AppID: appID, Type: model.OperationTypeDeploy, Status: model.OperationStatusCompleted, CompletedAt: &before},
				{ID: "failed-" + suffix, TenantID: tenant, AppID: appID, Type: model.OperationTypeDeploy, Status: model.OperationStatusFailed, CompletedAt: &now},
				{ID: "import-" + suffix, TenantID: tenant, AppID: appID, Type: model.OperationTypeImport, Status: model.OperationStatusCompleted, CompletedAt: &now},
				{ID: "foreign-" + suffix, TenantID: foreignTenant, AppID: appID, Type: model.OperationTypeDeploy, Status: model.OperationStatusCompleted, CompletedAt: &now},
			} {
				fixture = append(fixture, op)
			}
			if backend == "json" {
				if err := st.withLockedState(true, func(s *model.State) error { s.Operations = fixture; return nil }); err != nil {
					t.Fatal(err)
				}
			} else {
				// Use real relational reads with neutral fixtures, without starting workers.
				for _, op := range fixture {
					_, err := st.db.Exec(`insert into fugue_operations(id,tenant_id,app_id,type,status,execution_mode,requested_by_type,requested_by_id,created_at,updated_at,completed_at,desired_spec_json,desired_source_json) values($1,$2,$3,$4,$5,'managed','system','fixture',$6,$6,$7,'{"image":"registry.example.test/app:candidate","replicas":1}'::jsonb,'{"desired_source":{"type":"github-public","repo_url":"https://github.com/example/source"},"config_base_spec":{"env":{"SHOULD_NOT_RETURN":"large"}}}'::jsonb)`, op.ID, op.TenantID, op.AppID, op.Type, op.Status, now, op.CompletedAt)
					if err != nil {
						t.Fatal(err)
					}
				}
				defer st.db.Exec(`delete from fugue_operations where app_id=$1`, appID)
			}
			seen := map[string]bool{}
			var cursor *CompletedDeployCursor
			for {
				page, err := st.ListCompletedDeployCandidates(context.Background(), tenant, appID, now.Add(-time.Minute), cursor)
				if err != nil {
					t.Fatal(err)
				}
				if len(page) > CompletedDeployCandidatePageSize {
					t.Fatal("unbounded page")
				}
				for _, op := range page {
					if !strings.HasPrefix(op.ID, "candidate-") || seen[op.ID] || op.ConfigBaseSpec != nil {
						t.Fatalf("unexpected/duplicate candidate %s", op.ID)
					}
					seen[op.ID] = true
				}
				if len(page) < CompletedDeployCandidatePageSize {
					break
				}
				last := page[len(page)-1]
				cursor = &CompletedDeployCursor{CompletedAt: *last.CompletedAt, ID: last.ID}
			}
			if len(seen) != count {
				t.Fatalf("read %d of %d candidates", len(seen), count)
			}
		})
	}
}
