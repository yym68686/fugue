package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type releaseSecretTestCluster struct {
	deployments map[string]map[string]any
	secrets     map[string]map[string]any
	pods        []map[string]any
	deleted     []string
	created     []map[string]any
	podError    bool
	podContinue bool
	conflict    bool
	beforeWrite func()
}

func (k *releaseSecretTestCluster) client(t *testing.T) *kubeClient {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		name := path.Base(r.URL.Path)
		if r.Method == http.MethodDelete {
			k.deleted = append(k.deleted, r.URL.Path)
			delete(k.secrets, name)
			delete(k.deployments, name)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		if r.Method == http.MethodPost && name == "secrets" {
			if k.conflict {
				w.WriteHeader(http.StatusConflict)
				return
			}
			var obj map[string]any
			if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
				t.Error(err)
			}
			k.created = append(k.created, obj)
			k.secrets[objectStringField(objectMapField(obj, "metadata"), "name")] = obj
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		if r.Method != http.MethodGet {
			t.Errorf("unexpected mutation: %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected mutation", 400)
			return
		}
		if strings.Contains(r.URL.Path, "/deployments/") {
			obj := k.deployments[name]
			if obj == nil {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(obj)
			return
		}
		if strings.Contains(r.URL.Path, "/secrets/") {
			if obj := k.secrets[name]; obj != nil {
				_ = json.NewEncoder(w).Encode(obj)
				return
			}
			if k.beforeWrite != nil {
				k.beforeWrite()
			}
			http.NotFound(w, r)
			return
		}
		items := []map[string]any{}
		switch name {
		case "deployments", "secrets":
			objects := k.deployments
			if name == "secrets" {
				objects = k.secrets
			}
			for _, obj := range objects {
				items = append(items, obj)
			}
		case "pods":
			if k.podError {
				http.Error(w, "unavailable", 503)
				return
			}
			items = append(items, k.pods...)
		}
		continuation := ""
		if name == "pods" && k.podContinue {
			continuation = "next-page"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "metadata": map[string]any{"continue": continuation}})
	}))
	t.Cleanup(server.Close)
	return &kubeClient{client: server.Client(), baseURL: server.URL}
}

func TestRetainedReleaseSecretsSurvivePruning(t *testing.T) {
	for _, status := range []string{model.AppReleaseStatusCreating, model.AppReleaseStatusReady, model.AppReleaseStatusServing, model.AppReleaseStatusDraining, model.AppReleaseStatusFailed} {
		t.Run(status, func(t *testing.T) {
			st, app, _, _ := newSafeRolloutTestState(t)
			release, err := st.CreateAppRelease(model.AppRelease{TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRoleStable, Status: status,
				DeploymentName: runtime.RuntimeAppResourceName(app), ServiceName: runtime.RuntimeAppServiceName(app)})
			if err != nil {
				t.Fatal(err)
			}
			name := runtime.RuntimeAppResourceNameWithOptions(app, runtime.RenderOptions{Revision: safeRolloutCandidateRevision(release.ID)})
			// Include each Kubernetes Secret reference form, including old file
			// chunks and SSH configuration which are absent from today's App.
			spec := map[string]any{
				"volumes": []map[string]any{{"name": "app-files", "secret": map[string]any{"secretName": "revision-files"}},
					{"name": "app-files-1", "secret": map[string]any{"secretName": "revision-files-1"}},
					{"name": "projected", "projected": map[string]any{"sources": []map[string]any{{"secret": map[string]any{"name": "revision-ssh"}}}}}},
				"imagePullSecrets":    []map[string]any{{"name": "pull-secret"}},
				"containers":          []map[string]any{{"envFrom": []map[string]any{{"secretRef": map[string]any{"name": "env-secret"}}}}},
				"initContainers":      []map[string]any{{"env": []map[string]any{{"valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "init-secret"}}}}}},
				"ephemeralContainers": []map[string]any{{"envFrom": []map[string]any{{"secretRef": map[string]any{"name": "debug-secret"}}}}},
			}
			cluster := &releaseSecretTestCluster{deployments: map[string]map[string]any{name: {"metadata": map[string]any{"name": name}, "spec": map[string]any{"template": map[string]any{"spec": spec}}}}, secrets: map[string]map[string]any{}}
			for _, secret := range []string{"revision-files", "revision-files-1", "revision-ssh", "pull-secret", "env-secret", "init-secret", "debug-secret"} {
				cluster.secrets[secret] = map[string]any{"metadata": map[string]any{"name": secret}}
			}
			client := cluster.client(t)
			svc := &Service{Store: st}
			prune := func() error {
				return svc.pruneManagedAppStaleObjects(context.Background(), client, runtime.NamespaceForTenant(app.TenantID), app, nil)
			}
			if err := prune(); err != nil || len(cluster.deleted) != 0 {
				t.Fatalf("retained revision lost its dependencies: %v %v", err, cluster.deleted)
			}
			// Explicit retirement releases the Deployment, but a terminating Pod
			// still owns its dependencies until Kubernetes finishes collecting it.
			release.Role, release.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
			if _, err := st.UpdateAppRelease(release); err != nil {
				t.Fatal(err)
			}
			cluster.pods = []map[string]any{{"metadata": map[string]any{"deletionTimestamp": "2026-01-01T00:00:00Z"}, "spec": spec}}
			if err := prune(); err != nil || len(cluster.deleted) != 1 {
				t.Fatalf("pruned terminating Pod dependencies: %v %v", err, cluster.deleted)
			}
			cluster.pods = nil
			if err := prune(); err != nil || len(cluster.secrets) != 0 {
				t.Fatalf("orphaned Secrets not collected after retirement: %v %v", err, cluster.secrets)
			}
		})
	}
}

func TestReleaseFileSecretRecoveryUsesImmutableBinding(t *testing.T) {
	for _, scenario := range []string{"missing", "ready", "chunked", "existing", "retired_existing", "create_conflict", "wrong_uid", "wrong_generation", "wrong_key", "wrong_template_key", "wrong_owner", "deleting", "stopped", "draining", "failed", "retired", "unbound", "wrong_layout", "wrong_reference", "retired_before_write", "replaced_before_write"} {
		t.Run(scenario, func(t *testing.T) {
			st, app, _, op := newSafeRolloutTestState(t)
			op, claimed, err := st.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed {
				t.Fatal("claim", err)
			}
			app.Spec.RuntimeID = "runtime"
			app.Spec.Files = []model.AppFile{{Path: "/etc/example.conf", Content: "immutable revision content"}}
			if scenario == "chunked" {
				app.Spec.Files = []model.AppFile{{Path: "/etc/first.conf", Content: strings.Repeat("a", 600000)}, {Path: "/etc/second.conf", Content: strings.Repeat("b", 600000)}}
			}
			release, err := st.CreateAppRelease(model.AppRelease{ID: "revision", TenantID: app.TenantID, AppID: app.ID, Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusCreating,
				RuntimeID: app.Spec.RuntimeID, ResolvedImageRef: app.Spec.Image, SpecSnapshot: cloneControllerAppSpec(&app.Spec)})
			if err != nil {
				t.Fatal(err)
			}
			revision := safeRolloutCandidateRevision(release.ID)
			options := runtime.RenderOptions{Revision: revision}
			release.DeploymentName = runtime.RuntimeAppResourceNameWithOptions(app, options)
			release.ServiceName = runtime.RuntimeAppServiceNameWithOptions(app, options)
			release, err = st.UpdateAppRelease(release)
			if err != nil {
				t.Fatal(err)
			}
			var dep map[string]any
			for _, obj := range (runtime.Renderer{}).BuildManagedAppRevisionChildObjects(app, runtime.SchedulingConstraints{}, nil, nil, revision) {
				if obj["kind"] == "Deployment" {
					dep = cloneKubeMap(obj)
				}
			}
			meta := objectMapField(dep, "metadata")
			meta["uid"], meta["generation"] = "revision-uid", float64(1)
			if scenario != "unbound" {
				release, err = st.BindAppReleaseWorkload(context.Background(), release, model.AppReleaseWorkload{OperationID: op.ID, Namespace: runtime.NamespaceForTenant(app.TenantID),
					DeploymentName: release.DeploymentName, DeploymentUID: "revision-uid", DeploymentGeneration: 1, ServiceName: release.ServiceName, ServiceUID: "service-uid",
					ReleaseKey: objectStringMapValue(meta["annotations"])[runtime.FugueAnnotationReleaseKey], RuntimeID: release.RuntimeID, ImageRef: release.ResolvedImageRef})
				if err != nil {
					t.Fatal(err)
				}
			}
			name := release.DeploymentName
			release.Role, release.Status = model.AppReleaseRoleStable, model.AppReleaseStatusServing
			release.DeploymentName, release.ServiceName = runtime.RuntimeAppResourceName(app), runtime.RuntimeAppServiceName(app)
			switch scenario {
			case "ready":
				release.Role, release.Status = model.AppReleaseRoleCandidate, model.AppReleaseStatusReady
			case "wrong_uid":
				meta["uid"] = "replacement-uid"
			case "wrong_generation":
				meta["generation"] = float64(2)
			case "wrong_key":
				objectMapField(meta, "annotations")[runtime.FugueAnnotationReleaseKey] = "different"
			case "wrong_template_key":
				objectMapField(objectMapField(objectMapField(objectMapField(dep, "spec"), "template"), "metadata"), "annotations")[runtime.FugueAnnotationReleaseKey] = "different"
			case "wrong_owner":
				objectMapField(meta, "labels")[runtime.FugueLabelTenantID] = "other-tenant"
			case "deleting":
				meta["deletionTimestamp"] = "2026-01-01T00:00:00Z"
			case "draining", "failed", "retired":
				release.Status = scenario
				if scenario == "retired" {
					release.Role = model.AppReleaseRoleRetired
				}
			case "retired_existing":
				release.Role, release.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
			case "wrong_layout":
				items := mapSlice(objectMapField(mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "volumes"))[0], "secret")["items"])
				items[0]["key"] = "unexpected-key"
			case "wrong_reference":
				objectMapField(mapSlice(nestedObjectValue(dep, "spec", "template", "spec", "volumes"))[0], "secret")["secretName"] = "unexpected-secret"
			}
			release, err = st.UpdateAppRelease(release)
			if err != nil {
				t.Fatal(err)
			}
			cluster := &releaseSecretTestCluster{deployments: map[string]map[string]any{name: dep}, secrets: map[string]map[string]any{}, conflict: scenario == "create_conflict"}
			expected := runtime.BuildAppFilesSecretObjects(app, name)
			if scenario == "existing" || scenario == "retired_existing" {
				for _, obj := range expected {
					obj["stringData"] = map[string]string{"file-0": "existing content must survive"}
					cluster.secrets[objectStringField(objectMapField(obj, "metadata"), "name")] = obj
				}
			}
			if scenario == "retired_before_write" || scenario == "replaced_before_write" {
				cluster.beforeWrite = func() {
					if scenario == "replaced_before_write" {
						meta["uid"] = "replacement-uid"
						return
					}
					release.Role, release.Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
					if _, err := st.UpdateAppRelease(release); err != nil {
						t.Error(err)
					}
				}
			}
			client := cluster.client(t)
			// Today's canonical config must never be substituted into an old
			// revision, even after restart and canonical target alignment.
			app.Spec.Files = []model.AppFile{{Path: "/etc/new.conf", Content: "new canonical content"}}
			if scenario == "stopped" {
				app.Spec.Replicas = 0
			}
			prune := func() error {
				return (&Service{Store: st}).pruneManagedAppStaleObjects(context.Background(), client, runtime.NamespaceForTenant(app.TenantID), app, nil)
			}
			err = prune()
			wantError := scenario == "wrong_layout" || scenario == "wrong_reference" || scenario == "retired_before_write" || scenario == "replaced_before_write" || scenario == "create_conflict"
			if (err != nil) != wantError {
				t.Fatalf("unexpected recovery error: %v", err)
			}
			wantCreates := 0
			if scenario == "missing" || scenario == "ready" || scenario == "chunked" {
				wantCreates = len(expected)
			}
			if len(cluster.created) != wantCreates || len(cluster.deleted) != 0 {
				t.Fatalf("unexpected mutations: creates=%d deletes=%v", len(cluster.created), cluster.deleted)
			}
			for index, obj := range cluster.created {
				data := objectStringMapValue(obj["stringData"])
				for key, want := range objectStringMapValue(expected[index]["stringData"]) {
					if data[key] != want {
						t.Error("recovered file differs from immutable snapshot")
					}
				}
				owners := mapSlice(objectMapField(obj, "metadata")["ownerReferences"])
				if len(owners) != 1 || owners[0]["uid"] != "revision-uid" {
					t.Error("recovery lacks the immutable Deployment owner")
				}
			}
			if wantCreates > 0 {
				if err := prune(); err != nil || len(cluster.created) != wantCreates || len(cluster.deleted) != 0 {
					t.Fatalf("second reconcile was not idempotent: %v", err)
				}
			}
		})
	}
}

func TestSecretPruningFailsClosedWithoutCompletePodInventory(t *testing.T) {
	for _, scenario := range []string{"error", "continued"} {
		t.Run(scenario, func(t *testing.T) {
			_, app, _, _ := newSafeRolloutTestState(t)
			cluster := &releaseSecretTestCluster{secrets: map[string]map[string]any{"old-secret": {"metadata": map[string]any{"name": "old-secret"}}}, podError: scenario == "error", podContinue: scenario == "continued"}
			client := cluster.client(t)
			if err := (&Service{}).pruneManagedAppStaleObjects(context.Background(), client, runtime.NamespaceForTenant(app.TenantID), app, nil); err == nil || len(cluster.deleted) != 0 {
				t.Fatalf("incomplete Pod observation authorized deletion: %v %v", err, cluster.deleted)
			}
		})
	}
}
