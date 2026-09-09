package store

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestSourceSyncCheckOnlyChangesFactsAndFencesLateChecks(t *testing.T) {
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
			tenant, err := st.CreateTenant(fmt.Sprintf("Source check %d", time.Now().UnixNano()))
			if err != nil {
				t.Fatal(err)
			}
			project, err := st.CreateProject(tenant.ID, "source-check", "")
			if err != nil {
				t.Fatal(err)
			}
			app, err := st.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "source-check", "", model.AppSpec{Image: "registry.example.test/app:stable", Replicas: 1, RuntimeID: "runtime_managed_shared", Env: map[string]string{"SETTING": "keep"}}, model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://github.com/example/source", RepoBranch: "main", CommitSHA: "stable", BuildStrategy: model.AppBuildStrategyDockerfile})
			if err != nil {
				t.Fatal(err)
			}
			app, err = st.GetAppMetadata(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Microsecond)
			fact := &model.AppSourceSyncStatus{Provider: model.AppSourceSyncProviderGitHub, Phase: model.AppSourceSyncPhaseOK, LastCheckedAt: &now, LastSuccessAt: &now}
			if ok, err := st.RecordAppSourceSyncCheck(context.Background(), app, fact); err != nil || !ok {
				t.Fatalf("record=%v err=%v", ok, err)
			}
			got, err := st.GetAppMetadata(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status.SourceSync == nil || !got.Status.SourceSync.LastCheckedAt.Equal(now) {
				t.Fatal("check fact missing")
			}
			if !reflect.DeepEqual(got.Spec, app.Spec) || !reflect.DeepEqual(got.Source, app.Source) || !reflect.DeepEqual(got.OriginSource, app.OriginSource) || !got.UpdatedAt.Equal(app.UpdatedAt) || got.Status.Phase != app.Status.Phase {
				t.Fatal("fact update mutated serving configuration or revision")
			}
			if ok, err := st.RecordAppSourceSyncCheck(context.Background(), app, fact); err != nil || ok {
				t.Fatal("late observation overwrote newer fact")
			}
			if _, err := st.UpdateAppSourceSyncStatus(app.ID, nil); err != nil {
				t.Fatal(err)
			}
			if ok, err := st.RecordAppSourceSyncCheck(context.Background(), got, fact); err != nil || ok {
				t.Fatal("late observation undid resume")
			}
			fresh, err := st.GetAppMetadata(app.ID)
			if err != nil {
				t.Fatal(err)
			}
			rebound := *model.AppOriginSource(fresh)
			rebound.RepoBranch = "replacement"
			if _, err := st.UpdateAppOriginSource(app.ID, rebound); err != nil {
				t.Fatal(err)
			}
			if ok, err := st.RecordAppSourceSyncCheck(context.Background(), fresh, fact); err != nil || ok {
				t.Fatal("late observation crossed source rebind")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := st.RecordAppSourceSyncCheck(ctx, fresh, fact); err != context.Canceled {
				t.Fatalf("canceled check err=%v", err)
			}
		})
	}
}
