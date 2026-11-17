package runtime

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestRecoverySeparatesWorkloadAndStorageWithoutRenamingManagedApp(t *testing.T) {
	app := model.App{ID: "app_fixture", TenantID: "tenant_fixture", ProjectID: "project_fixture", Name: "gateway", Spec: model.AppSpec{Image: "registry.example/app:v1", Replicas: 1, Ports: []int{8080}, Workspace: &model.AppWorkspaceSpec{}}}
	originalName, originalPVC, originalCRD := RuntimeAppResourceName(app), WorkspacePVCName(app), ManagedAppResourceName(app)
	app.Spec.WorkloadGeneration = "replacement-identity"
	app.Spec.EphemeralPaths = []string{"/tmp/request-spool"}
	if RuntimeAppResourceName(app) == originalName || WorkspacePVCName(app) == originalPVC || ManagedAppResourceName(app) != originalCRD {
		t.Fatal("recovery identity isolation failed")
	}
	objects := BuildManagedAppChildObjects(app, SchedulingConstraints{}, nil)
	for _, obj := range objects {
		if obj["kind"] != "Deployment" {
			continue
		}
		raw, _ := json.Marshal(obj)
		var d struct {
			Spec struct {
				Template struct {
					Spec struct {
						Volumes []struct {
							Name     string
							EmptyDir map[string]any `json:"emptyDir"`
						}
						Containers []struct {
							VolumeMounts []struct{ Name, MountPath string }
						}
					}
				}
			}
		}
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, v := range d.Spec.Template.Spec.Volumes {
			if v.Name == "ephemeral-0" && v.EmptyDir != nil {
				found = true
			}
		}
		if !found {
			t.Fatal("isolated spool is missing")
		}
	}
	copy := app
	copy.Spec.WorkloadGeneration = "another-identity"
	if reflect.DeepEqual(WorkspacePVCName(app), WorkspacePVCName(copy)) {
		t.Fatal("two recoveries share a PVC")
	}
	app.Spec.WorkloadGeneration = strings.Repeat("shared-prefix", 10) + "a"
	copy.Spec.WorkloadGeneration = strings.Repeat("shared-prefix", 10) + "b"
	if WorkspacePVCName(app) == WorkspacePVCName(copy) || len(WorkspacePVCName(app)) > PersistentStorageClaimNameMaxLength {
		t.Fatal("storage identity lost entropy or exceeded claim normalization length")
	}
}
