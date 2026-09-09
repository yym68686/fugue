package store

import (
	"context"
	"errors"
	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"os"
	"sync"
	"testing"
)

func TestAppActionConcurrentRetryCreatesOneOperation(t *testing.T) {
	s := New(t.TempDir() + "/store.json")
	if os.Getenv("FUGUE_TEST_ACTION_DATABASE_URL") != "" {
		s = New("", os.Getenv("FUGUE_TEST_ACTION_DATABASE_URL"))
	}
	if err := s.Init(); err != nil {
		if os.Getenv("FUGUE_TEST_ACTION_DATABASE_URL") == "" {
			t.Fatal(err)
		}
		if err := schemamigrate.MigrateEdgeInstanceFencing(context.Background(), os.Getenv("FUGUE_TEST_ACTION_DATABASE_URL")); err != nil {
			t.Fatal(err)
		}
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
	}
	if s.usingDatabase() {
		defer s.db.Close()
	}
	tenant, _ := s.CreateTenant("Sample tenant")
	project, _ := s.CreateProject(tenant.ID, "apps", "")
	rt, _, _ := s.CreateRuntime(tenant.ID, "runtime", model.RuntimeTypeManagedOwned, "", nil)
	app, err := s.CreateApp(tenant.ID, project.ID, "sample", "", model.AppSpec{Image: "example/image:latest", Replicas: 1, RuntimeID: rt.ID})
	if err != nil {
		t.Fatal(err)
	}
	hash := model.AppSpecSHA256(app.Spec)
	var wg sync.WaitGroup
	ids := make(chan string, 16)
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			replicas := 2
			op, err := s.CreateAppActionOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeScale, DesiredReplicas: &replicas}, hash, "scope", "request-a", "request-hash")
			if err != nil {
				errs <- err
			} else {
				ids <- op.ID
			}
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("concurrent retry created duplicate")
		}
	}
	replica := 3
	_, err = s.CreateAppActionOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeScale, DesiredReplicas: &replica}, hash, "scope", "request-a", "different-hash")
	if !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("mismatch = %v", err)
	}
}
