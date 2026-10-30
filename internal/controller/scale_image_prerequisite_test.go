package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestScaleImagePrerequisiteWaitsWithoutFailingOrChangingStableState(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		current int
		desired int
		pending bool
		hardErr bool
	}{
		{"start after offline move", 0, 1, true, false},
		{"scale up", 1, 2, true, false},
		{"stop with unavailable cache", 1, 0, false, false},
		{"ordinary inspection failure", 0, 1, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			st := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := st.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := st.CreateTenant("Scale Image Tenant")
			if err != nil {
				t.Fatal(err)
			}
			project, err := st.CreateProject(tenant.ID, "applications", "")
			if err != nil {
				t.Fatal(err)
			}
			rt, _, err := st.CreateRuntime(tenant.ID, "destination", model.RuntimeTypeManagedOwned, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			app, err := st.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "sample", "", model.AppSpec{RuntimeID: rt.ID, Image: "registry.example/app:stable", Replicas: 1}, model.AppSource{Type: model.AppSourceTypeDockerImage, ResolvedImageRef: "registry.example/app:stable"})
			if err != nil {
				t.Fatal(err)
			}
			if test.current == 0 {
				zero := 0
				stopped, err := st.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeScale, DesiredReplicas: &zero})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := st.CompleteManagedOperation(stopped.ID, "", "fixture stopped after offline placement"); err != nil {
					t.Fatal(err)
				}
			}
			op, err := st.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeScale, DesiredReplicas: &test.desired})
			if err != nil {
				t.Fatal(err)
			}
			op, found, err := st.TryClaimPendingOperation(op.ID)
			if err != nil || !found {
				t.Fatal("claim scale", err)
			}
			available, calls := false, 0
			svc := &Service{Store: st, Renderer: runtime.Renderer{BaseDir: t.TempDir()}, Logger: log.New(io.Discard, "", 0),
				inspectManagedImage: func(context.Context, string) (bool, map[string]int64, error) {
					calls++
					if !test.pending {
						t.Fatal("stop must not depend on an image cache")
					}
					if test.hardErr {
						return false, nil, errors.New("invalid cache manifest")
					}
					if !available {
						return false, nil, fmt.Errorf("%w: destination copy in progress", errDeployImageReplicationPending)
					}
					return true, nil, nil
				},
			}
			if test.hardErr {
				if err := svc.handleClaimedOperation(context.Background(), op); err != nil {
					t.Fatal(err)
				}
				failed, err := st.GetOperation(op.ID)
				if err != nil || failed.Status != model.OperationStatusFailed {
					t.Fatal("ordinary image inspection failure was incorrectly retried", err)
				}
				stable, err := st.GetApp(app.ID)
				if err != nil || stable.Spec.Replicas != test.current {
					t.Fatal("hard image failure changed stable replicas", err)
				}
				return
			}
			err = svc.executeManagedOperation(context.Background(), op)
			if test.pending {
				if !errors.Is(err, errOperationNoLongerActive) || calls == 0 {
					t.Fatalf("expected asynchronous wait before apply, calls=%d err=%v", calls, err)
				}
				waiting, err := st.GetOperation(op.ID)
				if err != nil || !model.OperationWaitingForImageReplication(waiting) {
					t.Fatalf("scale must remain retryable, status=%s err=%v", waiting.Status, err)
				}
				stable, err := st.GetApp(app.ID)
				if err != nil || stable.Spec.Replicas != test.current || stable.Spec.RuntimeID != rt.ID || stable.Spec.Image != app.Spec.Image {
					t.Fatal("pending cache changed stable application configuration", err)
				}
				available = true
				op, found, err = st.TryClaimPendingOperation(op.ID)
				if err != nil || !found {
					t.Fatal("reclaim same scale operation", err)
				}
				if err := svc.executeManagedOperation(context.Background(), op); err != nil {
					t.Fatal("resume scale with verified image", err)
				}
			} else if err != nil {
				t.Fatal("stop with unavailable image cache", err)
			}
			completed, err := st.GetOperation(op.ID)
			if err != nil || completed.Status != model.OperationStatusCompleted {
				t.Fatalf("scale did not finish: status=%s err=%v", completed.Status, err)
			}
			stable, err := st.GetApp(app.ID)
			if err != nil || stable.Spec.Replicas != test.desired || stable.Spec.RuntimeID != rt.ID {
				t.Fatal("scale completion did not preserve placement", err)
			}
		})
	}
}
