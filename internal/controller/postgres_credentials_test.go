package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

type credentialKubeFixture struct {
	mu           sync.Mutex
	objects      map[string]map[string]any
	writes       []string
	revision     int
	conflictOnce bool
}

func (f *credentialKubeFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	current, found := f.objects[r.URL.Path]
	if r.Method == http.MethodGet {
		if strings.HasSuffix(r.URL.Path, "/clusters") {
			items := []map[string]any{}
			for path, obj := range f.objects {
				if strings.HasPrefix(path, r.URL.Path+"/") {
					items = append(items, obj)
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"items": items})
			return
		}
		if !found {
			http.Error(w, `{"reason":"NotFound"}`, 404)
			return
		}
		json.NewEncoder(w).Encode(current)
		return
	}
	var body map[string]any
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&body)
	}
	path := r.URL.Path
	if r.Method == http.MethodPost {
		path += "/" + normalizeKubeMap(body["metadata"])["name"].(string)
		current, found = f.objects[path]
	}
	if f.conflictOnce && r.Method == http.MethodPut {
		f.conflictOnce = false
		f.revision++
		normalizeKubeMap(current["metadata"])["resourceVersion"] = fmt.Sprint(f.revision)
		current["metadata"].(map[string]any)["resourceVersion"] = fmt.Sprint(f.revision)
		http.Error(w, `{"reason":"Conflict"}`, 409)
		return
	}
	if r.Method == http.MethodPost && found {
		http.Error(w, `{"reason":"AlreadyExists"}`, 409)
		return
	}
	if r.Method == http.MethodPut || r.Method == http.MethodPatch {
		if !found {
			http.Error(w, `{"reason":"NotFound"}`, 404)
			return
		}
		cm, dm := normalizeKubeMap(current["metadata"]), normalizeKubeMap(body["metadata"])
		if dm["resourceVersion"] != cm["resourceVersion"] || dm["uid"] != cm["uid"] {
			http.Error(w, `{"reason":"Conflict"}`, 409)
			return
		}
	}
	if r.Method == http.MethodDelete {
		if found {
			delete(f.objects, path)
		}
		f.writes = append(f.writes, "DELETE "+path)
		json.NewEncoder(w).Encode(map[string]any{})
		return
	}
	if r.Method == http.MethodPatch {
		next := cloneKubeMap(current)
		metadata := normalizeKubeMap(next["metadata"])
		for k, v := range normalizeKubeMap(body["metadata"]) {
			metadata[k] = v
		}
		next["metadata"] = metadata
		spec := normalizeKubeMap(next["spec"])
		for k, v := range normalizeKubeMap(body["spec"]) {
			spec[k] = v
		}
		next["spec"] = spec
		body = next
	}
	f.revision++
	m := normalizeKubeMap(body["metadata"])
	m["resourceVersion"] = fmt.Sprint(f.revision)
	if !found {
		m["uid"] = "uid-" + fmt.Sprint(f.revision)
	}
	body["metadata"] = m
	f.objects[path] = body
	f.writes = append(f.writes, r.Method+" "+path)
	json.NewEncoder(w).Encode(body)
}

func newCredentialKube(t *testing.T) (*credentialKubeFixture, *kubeClient) {
	t.Helper()
	f := &credentialKubeFixture{objects: map[string]map[string]any{}, revision: 1}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	return f, &kubeClient{baseURL: srv.URL, client: srv.Client(), namespace: "fg-tenant-demo"}
}

func credentialTestObjects(serviceID, secretName, password string) (map[string]any, map[string]any) {
	labels := map[string]any{runtime.FugueLabelBackingServiceID: serviceID, runtime.FugueLabelBackingServiceType: "postgres", runtime.FugueLabelOwnerAppID: "app-" + serviceID, runtime.FugueLabelTenantID: "tenant-demo", runtime.FugueLabelProjectID: "project-" + serviceID}
	secret := map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": secretName, "namespace": "fg-tenant-demo", "labels": labels}, "type": "Opaque", "stringData": map[string]any{"username": "demo", "password": password}}
	cluster := map[string]any{"apiVersion": runtime.CloudNativePGAPIVersion, "kind": "Cluster", "metadata": map[string]any{"name": "cluster-" + serviceID, "namespace": "fg-tenant-demo", "labels": labels, "uid": "cluster-uid-" + serviceID, "resourceVersion": "1"}, "spec": map[string]any{"bootstrap": map[string]any{"initdb": map[string]any{"secret": map[string]any{"name": secretName}}}, "managed": map[string]any{"roles": []map[string]any{{"name": "demo", "passwordSecret": map[string]any{"name": secretName}}}}}}
	return secret, cluster
}

func TestCredentialSecretCreateNoopCASAndForeignOwner(t *testing.T) {
	f, c := newCredentialKube(t)
	ctx := context.Background()
	a, _ := credentialTestObjects("one", "credential", "first")
	if err := c.applyObject(ctx, a, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.applyObject(ctx, a, nil); err != nil {
		t.Fatal(err)
	}
	if len(f.writes) != 1 {
		t.Fatal("no-op rewrote secret", f.writes)
	}
	foreign, _ := credentialTestObjects("two", "credential", "second")
	if err := c.applyObject(ctx, foreign, nil); err == nil || !strings.Contains(err.Error(), "resource_ownership_conflict") {
		t.Fatal("foreign write accepted", err)
	}
	if len(f.writes) != 1 {
		t.Fatal("foreign ownership mutated")
	}
	f.conflictOnce = true
	a["stringData"].(map[string]any)["password"] = "rotated"
	if err := c.applyObject(ctx, a, nil); err != nil {
		t.Fatal(err)
	}
	if len(f.writes) != 2 {
		t.Fatal("CAS did not retry one update", f.writes)
	}
	if !reflect.DeepEqual(credentialSecretData(f.objects["/api/v1/namespaces/fg-tenant-demo/secrets/credential"]), credentialSecretData(a)) {
		t.Fatal("rotation lost")
	}
}

func TestCredentialSecretDeletionRetainsBootstrapAndForeignRoleReferences(t *testing.T) {
	f, c := newCredentialKube(t)
	ctx := context.Background()
	secret, own := credentialTestObjects("one", "legacy", "password")
	_, other := credentialTestObjects("two", "legacy", "password")
	if err := c.applyObject(ctx, secret, nil); err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []map[string]any{own, other} {
		name, _ := objectNameAndNamespace(c.namespace, cluster)
		f.objects[cloudNativePGClusterAPIPath(c.namespace, name)] = cluster
	}
	if err := c.deleteSecret(ctx, c.namespace, "legacy"); err != nil {
		t.Fatal(err)
	}
	if len(f.writes) != 1 {
		t.Fatal("referenced credential deleted")
	}
	for path, obj := range f.objects {
		if cloudNativePGObject(obj) {
			delete(f.objects, path)
		}
	}
	if err := c.deleteSecret(ctx, c.namespace, "legacy"); err != nil {
		t.Fatal(err)
	}
	if len(f.writes) != 3 || !strings.HasPrefix(f.writes[2], "DELETE") {
		t.Fatal("unreferenced credential retained")
	}
}

func TestCredentialMigrationFromSharedSecretPersistsAndKeepsDatabases(t *testing.T) {
	f, c := newCredentialKube(t)
	ctx := context.Background()
	s := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("credential migration")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{CPUMilliCores: 4000, MemoryMebibytes: 8192, StorageGibibytes: 20}); err != nil {
		t.Fatal(err)
	}
	svc := &Service{Store: s}
	apps := []model.App{}
	for _, name := range []string{"first", "second"} {
		project, err := s.CreateProject(tenant.ID, name, "")
		if err != nil {
			t.Fatal(err)
		}
		app, err := s.CreateApp(tenant.ID, project.ID, "same-name", "", model.AppSpec{Image: "example.invalid/missing:1", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Postgres: &model.AppPostgresSpec{Database: "demo", User: "demo", Password: name, ServiceName: "cluster-" + name, StorageSize: "1Gi"}})
		if err != nil {
			t.Fatal(err)
		}
		apps = append(apps, app)
		for _, obj := range runtime.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil) {
			if cloudNativePGObject(obj) {
				obj = cloneKubeMap(obj)
				m := normalizeKubeMap(obj["metadata"])
				m["uid"] = "uid-" + name
				m["resourceVersion"] = "1"
				obj["metadata"] = m
				spec := normalizeKubeMap(obj["spec"])
				normalizeKubeMap(spec["bootstrap"])["initdb"] = nil
				spec["bootstrap"] = map[string]any{"initdb": map[string]any{"database": "demo", "owner": "demo", "secret": map[string]any{"name": "shared-legacy"}}}
				roles := cloudNativePGManagedRolesFromObject(obj)
				roles[0]["passwordSecret"] = map[string]any{"name": "shared-legacy"}
				spec["managed"] = map[string]any{"roles": roles}
				obj["spec"] = spec
				f.objects[cloudNativePGClusterAPIPath(runtime.NamespaceForTenant(tenant.ID), "cluster-"+name)] = obj
			}
			if postgresCredentialSecret(obj) {
				obj = cloneKubeMap(obj)
				obj["metadata"].(map[string]any)["name"] = "shared-legacy"
				if err := c.applyObject(ctx, obj, nil); err != nil && name == "first" {
					t.Fatal(err)
				}
			}
		}
	}
	namespace := runtime.NamespaceForTenant(tenant.ID)
	before := map[string]any{}
	for path, obj := range f.objects {
		if cloudNativePGObject(obj) {
			before[path] = normalizeKubeMap(obj["spec"])["bootstrap"]
		}
	}
	for round := 0; round < 3; round++ {
		for i, app := range apps {
			next, err := svc.reconcileManagedPostgresCredentials(ctx, c, namespace, app)
			if err != nil {
				t.Fatal(err)
			}
			apps[i] = next
		}
	}
	a, b := apps[0].BackingServices[0].Spec.Postgres.CredentialSecretName, apps[1].BackingServices[0].Spec.Postgres.CredentialSecretName
	if a == "" || b == "" || a == b || a == "shared-legacy" || b == "shared-legacy" {
		t.Fatal("migration did not isolate identities", a, b)
	}
	for _, app := range apps {
		service := app.BackingServices[0]
		stored, err := s.GetBackingService(service.ID)
		if err != nil || stored.Spec.Postgres.CredentialSecretName != service.Spec.Postgres.CredentialSecretName {
			t.Fatal("identity not persisted", err)
		}
		path := cloudNativePGClusterAPIPath(namespace, service.Spec.Postgres.ServiceName)
		cluster := f.objects[path]
		expectedBootstrap := normalizeKubeMap(before[path])
		settings := normalizeKubeMap(expectedBootstrap["initdb"])
		settings["secret"] = map[string]any{"name": service.Spec.Postgres.CredentialSecretName}
		expectedBootstrap["initdb"] = settings
		if !reflect.DeepEqual(expectedBootstrap, normalizeKubeMap(cluster["spec"])["bootstrap"]) {
			t.Fatal("bootstrap fields other than credential reference changed")
		}
		if credentialReferenceName(cloudNativePGManagedRolesFromObject(cluster)[0]) != service.Spec.Postgres.CredentialSecretName {
			t.Fatal("role reference not migrated")
		}
	}
	for _, write := range f.writes {
		if strings.Contains(write, "/deployments") || strings.HasPrefix(write, "DELETE") {
			t.Fatal("migration mutated executable or destroyed resources", write)
		}
	}
	if len(f.writes) != 5 {
		t.Fatal("repeated reconcile was not idempotent", f.writes)
	}
}

func TestCredentialReadinessRequiresExactRoleReferenceAndAcknowledgement(t *testing.T) {
	deployment := runtime.ManagedBackingServiceDeployment{CredentialSecretName: "isolated", CredentialUser: "demo"}
	var cluster kubeCloudNativePGCluster
	if err := json.Unmarshal([]byte(`{"spec":{"managed":{"roles":[{"name":"demo","passwordSecret":{"name":"legacy"}}]}},"status":{"managedRolesStatus":{"passwordStatus":{"demo":{"resourceVersion":"10"}}},"secretsResourceVersion":{"managedRoleSecretVersion":{"isolated":"10"}}}}`), &cluster); err != nil {
		t.Fatal(err)
	}
	if managedPostgresCredentialAcknowledged(deployment, cluster) {
		t.Fatal("foreign role reference accepted")
	}
	cluster.Spec.Managed.Roles[0].PasswordSecret.Name = "isolated"
	cluster.Status.SecretsResourceVersion.ManagedRoleSecretVersion["isolated"] = "11"
	if managedPostgresCredentialAcknowledged(deployment, cluster) {
		t.Fatal("stale password ACK accepted")
	}
	cluster.Status.SecretsResourceVersion.ManagedRoleSecretVersion["isolated"] = "10"
	if !managedPostgresCredentialAcknowledged(deployment, cluster) {
		t.Fatal("matching ACK rejected")
	}
}
