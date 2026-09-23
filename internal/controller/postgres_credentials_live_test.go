package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Opt-in end-to-end rehearsal against a disposable local kind cluster with
// CNPG installed. Never accepts the user's default kube context or a remote
// control plane. The ordinary unit suite does not require Kubernetes.
func TestCredentialMigrationLiveCNPG(t *testing.T) {
	configPath := os.Getenv("FUGUE_CREDENTIAL_TEST_KUBECONFIG")
	if configPath == "" {
		t.Skip("requires disposable local kind credential-test cluster")
	}
	raw, err := clientcmd.LoadFromFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if raw.CurrentContext != "kind-fugue-credential-test" {
		t.Fatal("refusing non-test Kubernetes context")
	}
	cfg, err := clientcmd.NewDefaultClientConfig(*raw, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	endpoint, _ := url.Parse(cfg.Host)
	if endpoint.Hostname() != "127.0.0.1" && endpoint.Hostname() != "localhost" {
		t.Fatal("refusing non-loopback Kubernetes endpoint")
	}
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := &kubeClient{baseURL: cfg.Host, client: httpClient, bearerToken: cfg.BearerToken}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("credential rehearsal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{CPUMilliCores: 4000, MemoryMebibytes: 8192, StorageGibibytes: 20}); err != nil {
		t.Fatal(err)
	}
	ns := runtime.NamespaceForTenant(tenant.ID)
	c.namespace = ns
	_, err = c.doJSON(ctx, http.MethodPost, "/api/v1/namespaces", map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": ns}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("isolated namespace=%s", ns)
	apps := []model.App{}
	for _, name := range []string{"first", "second"} {
		project, err := s.CreateProject(tenant.ID, name, "")
		if err != nil {
			t.Fatal(err)
		}
		app, err := s.CreateApp(tenant.ID, project.ID, "same-name", "", model.AppSpec{Image: "example.invalid/not-built:1", RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Postgres: &model.AppPostgresSpec{Database: "demo", User: "demo", Password: "test-password-" + name, ServiceName: "credential-" + name, StorageSize: "1Gi"}})
		if err != nil {
			t.Fatal(err)
		}
		apps = append(apps, app)
		for _, obj := range runtime.BuildManagedAppChildObjects(app, runtime.SchedulingConstraints{}, nil) {
			if postgresCredentialSecret(obj) && name == "first" {
				obj["metadata"].(map[string]any)["name"] = "shared-legacy"
				if err := c.applyObject(ctx, obj, nil); err != nil {
					t.Fatal(err)
				}
			}
			if cloudNativePGObject(obj) {
				obj = cloneKubeMap(obj)
				spec := normalizeKubeMap(obj["spec"])
				spec["imageName"] = "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie"
				spec["bootstrap"] = map[string]any{"initdb": map[string]any{"database": "demo", "owner": "demo", "secret": map[string]any{"name": "shared-legacy"}}}
				roles := cloudNativePGManagedRolesFromObject(obj)
				roles[0]["passwordSecret"] = map[string]any{"name": "shared-legacy"}
				spec["managed"] = map[string]any{"roles": roles}
				obj["spec"] = spec
				if err := c.applyObject(ctx, obj, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	wait := func(description string, check func() bool) {
		t.Helper()
		for {
			if check() {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal(description, ctx.Err())
			case <-time.After(time.Second):
			}
		}
	}
	command := func(cluster, password, sql string) ([]byte, error) {
		args := []string{"--kubeconfig", configPath, "-n", ns, "exec", cluster + "-1", "-c", "postgres", "--", "env", "PGPASSWORD=" + password, "psql", "-X", "-h", "127.0.0.1", "-U", "demo", "-d", "demo", "-Atq", "-v", "ON_ERROR_STOP=1", "-c", sql}
		cmd := exec.CommandContext(ctx, "kubectl", args...)
		return cmd.CombinedOutput()
	}
	identities := map[string]string{}
	for _, app := range apps {
		name := app.BackingServices[0].Spec.Postgres.ServiceName
		wait("initial CNPG readiness", func() bool {
			out, err := command(name, "test-password-first", "SELECT 1")
			return err == nil && strings.TrimSpace(string(out)) == "1"
		})
		out, err := command(name, "test-password-first", "CREATE TABLE credential_sentinel (value text); INSERT INTO credential_sentinel VALUES ('retained');")
		if err != nil {
			t.Fatalf("seed sentinel: %s %v", out, err)
		}
		pod, _, err := c.getRawObject(ctx, "/api/v1/namespaces/"+ns+"/pods/"+name+"-1")
		if err != nil {
			t.Fatal(err)
		}
		identities[name] = normalizeKubeMap(pod["metadata"])["uid"].(string)
	}
	svc := &Service{Store: s}
	for i, app := range apps {
		wait("credential migration", func() bool {
			next, err := svc.reconcileManagedPostgresCredentials(ctx, c, ns, app)
			if err != nil {
				t.Logf("retry migration: %v", err)
				return false
			}
			apps[i] = next
			return true
		})
	}
	for _, app := range apps {
		pg := app.BackingServices[0].Spec.Postgres
		wait("new connection authentication", func() bool {
			out, err := command(pg.ServiceName, pg.Password, "SELECT value FROM credential_sentinel")
			return err == nil && strings.TrimSpace(string(out)) == "retained"
		})
		cluster, _, err := c.getRawObject(ctx, cloudNativePGClusterAPIPath(ns, pg.ServiceName))
		if err != nil {
			t.Fatal(err)
		}
		bootstrap := normalizeKubeMap(normalizeKubeMap(cluster["spec"])["bootstrap"])
		if normalizeKubeMap(normalizeKubeMap(bootstrap["initdb"])["secret"])["name"] != pg.CredentialSecretName {
			t.Fatal("bootstrap credential reference did not move with managed role")
		}
		pod, _, err := c.getRawObject(ctx, "/api/v1/namespaces/"+ns+"/pods/"+pg.ServiceName+"-1")
		if err != nil {
			t.Fatal(err)
		}
		if normalizeKubeMap(pod["metadata"])["uid"] != identities[pg.ServiceName] {
			t.Fatal("database Pod replaced")
		}
		t.Logf("service=%s credential=%s new_connection_ok=true pod_preserved=true sentinel_preserved=true", app.BackingServices[0].ID, pg.CredentialSecretName)
	}
	versions := map[string]any{}
	for _, app := range apps {
		name := app.BackingServices[0].Spec.Postgres.CredentialSecretName
		secret, _, err := c.getRawObject(ctx, "/api/v1/namespaces/"+ns+"/secrets/"+name)
		if err != nil {
			t.Fatal(err)
		}
		versions[name] = normalizeKubeMap(secret["metadata"])["resourceVersion"]
	}
	for round := 0; round < 3; round++ {
		for _, app := range apps {
			if _, err := svc.reconcileManagedPostgresCredentials(ctx, c, ns, app); err != nil {
				t.Fatal(err)
			}
		}
	}
	for name, version := range versions {
		secret, _, err := c.getRawObject(ctx, "/api/v1/namespaces/"+ns+"/secrets/"+name)
		if err != nil {
			t.Fatal(err)
		}
		if normalizeKubeMap(secret["metadata"])["resourceVersion"] != version {
			t.Fatal("no-op changed Secret")
		}
	}
	legacyPath := "/api/v1/namespaces/" + ns + "/secrets/shared-legacy"
	legacy, _, err := c.getRawObject(ctx, legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	data := credentialSecretData(legacy)
	data["password"] = base64.StdEncoding.EncodeToString([]byte("retired-wrong-password"))
	legacy["data"] = data
	if _, err := c.doJSON(ctx, http.MethodPut, legacyPath, legacy, nil); err != nil {
		t.Fatal(err)
	}
	for _, app := range apps {
		pg := app.BackingServices[0].Spec.Postgres
		wait("credential ACK after old Secret update", func() bool {
			cluster, found, err := c.getCloudNativePGCluster(ctx, ns, pg.ServiceName)
			return err == nil && found && managedPostgresCredentialAcknowledged(runtime.ManagedBackingServiceDeployment{CredentialSecretName: pg.CredentialSecretName, CredentialUser: pg.User}, cluster)
		})
		out, err := command(pg.ServiceName, pg.Password, "SELECT value FROM credential_sentinel")
		if err != nil || strings.TrimSpace(string(out)) != "retained" {
			t.Fatal("old Secret update reverted migrated password", err)
		}
	}
	wait("unreferenced legacy Secret retirement", func() bool {
		if err := c.deleteSecret(ctx, ns, "shared-legacy"); err != nil {
			t.Fatal(err)
		}
		_, found, err := c.getRawObject(ctx, legacyPath)
		return err == nil && !found
	})
	encoded, _ := json.Marshal(map[string]any{"namespace": ns, "databases": 2, "credential_versions": versions, "writes_to_production": 0})
	fmt.Println(string(encoded))
}
