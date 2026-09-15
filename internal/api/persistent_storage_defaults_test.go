package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestPersistentStorageDefaultsPreserveVolumeIdentity(t *testing.T) {
	s := &Server{movableRWOStorageClass: "platform-rwo"}
	for _, tc := range []struct {
		name    string
		current model.AppSpec
		storage model.AppPersistentStorageSpec
		want    string
	}{
		{name: "new dedicated", storage: model.AppPersistentStorageSpec{Mode: "dedicated_pvc"}, want: "platform-rwo"},
		{name: "new movable", storage: model.AppPersistentStorageSpec{Mode: "movable_rwo"}, want: "platform-rwo"},
		{name: "explicit class", storage: model.AppPersistentStorageSpec{StorageClassName: "custom"}, want: "custom"},
		{name: "existing class", current: model.AppSpec{PersistentStorage: &model.AppPersistentStorageSpec{StorageClassName: "old-class"}}, want: "old-class"},
		{name: "legacy omitted class", current: model.AppSpec{PersistentStorage: &model.AppPersistentStorageSpec{}}, want: ""},
		{name: "workspace migration", current: model.AppSpec{Workspace: &model.AppWorkspaceSpec{StorageClassName: "workspace-class"}}, want: "workspace-class"},
		{name: "existing external claim", storage: model.AppPersistentStorageSpec{ClaimName: "external-claim"}, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.storage
			got := s.applyPersistentStorageDefaultsForDeploy(tc.current, model.AppSpec{PersistentStorage: &tc.storage})
			if got.PersistentStorage.StorageClassName != tc.want {
				t.Fatalf("class=%q, want %q", got.PersistentStorage.StorageClassName, tc.want)
			}
			if tc.storage.StorageClassName != before.StorageClassName {
				t.Fatal("mutated input intent")
			}
		})
	}
}

func TestDeployFirstPersistentVolumeUsesPlatformClass(t *testing.T) {
	_, server, key, app := setupAppConfigTestServer(t, model.AppSpec{Image: "example/service:latest", Replicas: 1, Ports: []int{8080}, RuntimeID: "runtime_managed_shared"})
	spec := app.Spec
	spec.PersistentStorage = &model.AppPersistentStorageSpec{Mode: "dedicated_pvc", StorageSize: "1Gi", Mounts: []model.AppPersistentStorageMount{{Path: "/data"}}}
	rr := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/deploy", key, map[string]any{"spec": spec})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status=%d: %s", rr.Code, rr.Body.String())
	}
	var result struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, rr, &result)
	if result.Operation.DesiredSpec.PersistentStorage.StorageClassName != defaultImportedMovableRWOStorageClassName {
		t.Fatalf("unexpected class: %+v", result.Operation.DesiredSpec.PersistentStorage)
	}
}
