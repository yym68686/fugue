package api

import (
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type runtimeEvidenceExec map[string][]byte

func (f runtimeEvidenceExec) Run(ctx context.Context, namespace, pod, container string, stdin []byte, command ...string) ([]byte, error) {
	if len(command) != 4 || command[0] != "head" || command[1] != "-c" || command[2] != "1048577" {
		return nil, fmt.Errorf("unbounded or unexpected evidence command")
	}
	value, found := f[command[3]]
	if !found {
		return nil, fmt.Errorf("unavailable")
	}
	return value, nil
}
func TestRuntimeInspectionUsesProcessAndFileHashesWithoutLeakingValues(t *testing.T) {
	var pod kubePodInfo
	if err := json.Unmarshal([]byte(`{"metadata":{"name":"pod-test","uid":"uid-test"},"status":{"containerStatuses":[{"name":"app","ready":true,"imageID":"containerd://sha256:abc"}]}}`), &pod); err != nil {
		t.Fatal(err)
	}
	server := &Server{filesystemExecRunner: runtimeEvidenceExec{"/proc/1/environ": []byte("TOKEN=private-marker\x00PATH=/bin\x00"), "/proc/1/cmdline": []byte("worker\x00--serve\x00"), "/proc/1/mountinfo": []byte("1 2 0:1 / /data rw - ext4 /dev/test rw\n"), "/app/config": []byte("secret-config")}}
	spec := model.AppSpec{Image: "demo@sha256:abc", Command: []string{"worker"}, Args: []string{"--serve"}, Files: []model.AppFile{{Path: "/app/config", Content: "secret-config", Secret: true}}, PersistentStorage: &model.AppPersistentStorageSpec{Mounts: []model.AppPersistentStorageMount{{Path: "/data"}}}}
	checks := server.inspectRuntimePod(context.Background(), "tenant-test", "app", pod, spec, map[string]string{"TOKEN": "private-marker"}, "sha256:abc")
	for _, check := range checks {
		if check.State != "in_sync" {
			t.Fatalf("unexpected check %+v", check)
		}
	}
	raw, _ := json.Marshal(checks)
	for _, secret := range []string{"private-marker", "secret-config"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("runtime evidence leaked value")
		}
	}
}
func TestRuntimeInspectionMissingEvidenceNeverPasses(t *testing.T) {
	var pod kubePodInfo
	json.Unmarshal([]byte(`{"metadata":{"name":"pod-test"},"status":{"containerStatuses":[{"name":"app","ready":true}]}}`), &pod)
	server := &Server{filesystemExecRunner: runtimeEvidenceExec{}}
	checks := server.inspectRuntimePod(context.Background(), "tenant-test", "app", pod, model.AppSpec{Files: []model.AppFile{{Path: "/config"}}}, map[string]string{}, "")
	state := model.AppRuntimeState{Checks: checks}
	if summarizeRuntimeState(state) != "inconclusive" {
		t.Fatal(checks)
	}
}
func TestImagePinsAreTenantScoped(t *testing.T) {
	st := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	owner, _ := st.CreateTenant("Owner")
	other, _ := st.CreateTenant("Other")
	_, ownerKey, err := st.CreateAPIKey(owner.ID, "read", []string{"app.read"})
	if err != nil {
		t.Fatal(err)
	}
	_, otherKey, err := st.CreateAPIKey(other.ID, "read", []string{"app.read"})
	if err != nil {
		t.Fatal(err)
	}
	image, err := st.UpsertImage(model.Image{TenantID: owner.ID, AppID: "app_test", ImageRef: "registry.example.com/demo:v1", CanonicalDigest: "sha256:" + strings.Repeat("a", 64), LifecycleState: model.ImageLifecycleAvailable})
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.UpsertImagePin(model.ImagePin{ImageID: image.ID, TenantID: owner.ID, Reason: "user_pin", ExpiresAt: timePtrTest(time.Now().Add(time.Hour))})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(st, auth.New(st, ""), nil, ServerConfig{})
	for _, tc := range []struct {
		key  string
		want int
	}{{ownerKey, 200}, {otherKey, 404}} {
		response := performJSONRequest(t, server, http.MethodGet, "/v1/images/"+image.ID+"/pins", tc.key, nil)
		if response.Code != tc.want {
			t.Fatalf("got %d want %d body=%s", response.Code, tc.want, response.Body.String())
		}
	}
}
func timePtrTest(value time.Time) *time.Time { return &value }
