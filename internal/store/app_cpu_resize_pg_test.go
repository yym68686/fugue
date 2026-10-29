package store

import (
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGAppCPUResizeFencesObservedBaselineAndPreservesVerifiedTarget(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, active := range []bool{false, true} {
			name := "app"
			if postgres {
				name = "postgres"
			}
			if active {
				name += "/active"
			}
			t.Run(name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
				const appID = "app_cpu_observation"
				now := time.Now().UTC()
				template := int64(100)
				stored := model.AppSpec{Image: "registry.example/service:one", Replicas: 1, RuntimeID: "runtime_managed_shared",
					Resources: &model.ResourceSpec{CPUMilliCores: 75, MemoryMebibytes: 128}, CPURequestTemplateMilliCores: &template}
				encoded, err := json.Marshal(stored)
				if err != nil {
					t.Fatal(err)
				}
				mock.ExpectBegin()
				mock.ExpectQuery(`(?s)FROM fugue_apps.*WHERE id = \$1.*FOR UPDATE`).WithArgs(appID).
					WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "project_id", "name", "description", "source_json", "route_json", "spec_json", "status_json", "created_at", "updated_at"}).
						AddRow(appID, "tenant_cpu", "project_cpu", "service", "", []byte("null"), []byte("null"), encoded, []byte(`{"phase":"ready","current_replicas":1}`), now, now))
				expectPGLifecycleAppHydration(mock, appID)
				expectPGManagedPostgresExclusiveMutation(mock, appID, "", false)
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM fugue_operations WHERE app_id=$1 AND type=$2 AND status IN ('pending','running','waiting-agent'))`)).
					WithArgs(appID, model.OperationTypeAppCPUResize).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(active))
				if active {
					mock.ExpectRollback()
				} else {
					mock.ExpectExec(`(?s)UPDATE fugue_apps`).WithArgs(appID, "tenant_cpu", "project_cpu", "service", "", sqlmock.AnyArg(), sqlmock.AnyArg(), jsonArgument(func(raw []byte) bool {
						var spec model.AppSpec
						return json.Unmarshal(raw, &spec) == nil && spec.Resources.CPUMilliCores == 75 && spec.Resources.MemoryMebibytes == 192 && spec.CPURequestTemplateMilliCores != nil && *spec.CPURequestTemplateMilliCores == 100
					}), sqlmock.AnyArg(), now, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectCommit()
					expectPGLifecycleAppHydration(mock, appID)
				}
				observed := *cloneAppSpec(&stored)
				observed.CPURequestTemplateMilliCores = nil
				observed.Resources.CPUMilliCores = 100
				observed.Resources.MemoryMebibytes = 192
				var updated model.App
				if postgres {
					updated, err = s.SyncObservedManagedPostgresSpec(appID, observed)
				} else {
					updated, err = s.SyncObservedManagedAppBaseline(appID, observed, nil)
				}
				if active {
					if !errors.Is(err, ErrConflict) {
						t.Fatalf("active resize was not interlocked: %v", err)
					}
				} else if err != nil || updated.Spec.Resources.CPUMilliCores != 75 || updated.Spec.CPURequestTemplateMilliCores == nil {
					t.Fatalf("verified target not preserved: %v", err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
