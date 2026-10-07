package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestColdSeedEvidenceReadsCompleteLargeReceipt(t *testing.T) {
	want := coldFileEvidence{SystemID: "123", FileDigest: strings.Repeat("a", 64)}
	for i := 0; i < 2000; i++ {
		want.MetadataEntries = append(want.MetadataEntries, coldMetadataEntry{Path: fmt.Sprintf("base/42/%d", i), Mode: 0600, UID: 26, GID: 26, Size: 8192})
	}
	body, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) <= 5*16384 {
		t.Fatal("receipt must span more than five CRI records")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/namespaces/ns/pods/seed-verify/log" || r.URL.Query().Get("container") != "verify" {
			t.Errorf("unexpected evidence request: %s", r.URL)
		}
		if r.URL.Query().Has("tailLines") {
			t.Error("structured receipt requested as truncated log tail")
		}
		w.Write(body)
	}))
	defer server.Close()
	ev, err := readColdSeedEvidence(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "ns", "seed-verify")
	if err != nil || ev.FileDigest != want.FileDigest || len(ev.MetadataEntries) != len(want.MetadataEntries) || ev.MetadataEntries[1999] != want.MetadataEntries[1999] {
		t.Fatalf("incomplete evidence: entries=%d err=%v", len(ev.MetadataEntries), err)
	}
}

func TestColdCutoverOnlyChangesStableServiceSelector(t *testing.T) {
	for _, cluster := range []string{"source", "target", "unrelated"} {
		t.Run(cluster, func(t *testing.T) {
			writes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/namespaces/ns/services/stable" {
					t.Errorf("cutover touched non-endpoint resource: %s", r.URL.Path)
				}
				if r.Method == http.MethodGet {
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]string{"uid": "service-uid", "resourceVersion": "42"}, "spec": map[string]any{"selector": map[string]string{"cnpg.io/cluster": cluster, "cnpg.io/instanceRole": "primary"}}})
					return
				}
				writes++
				if r.Method != http.MethodPatch || r.Header.Get("Content-Type") != "application/json-patch+json" {
					t.Fatal("cutover must use compare-and-swap selector patch")
				}
				var patch []map[string]any
				if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
					t.Fatal(err)
				}
				if len(patch) != 4 || patch[0]["op"] != "test" || patch[0]["path"] != "/metadata/uid" || patch[0]["value"] != "service-uid" || patch[1]["op"] != "test" || patch[1]["path"] != "/metadata/resourceVersion" || patch[1]["value"] != "42" || patch[2]["op"] != "test" || patch[2]["path"] != "/spec/selector" || patch[3]["op"] != "replace" || patch[3]["path"] != "/spec/selector" {
					t.Fatal("cutover lost CAS or changed non-selector state", patch)
				}
				if normalizeKubeMap(patch[2]["value"])["cnpg.io/cluster"] != "source" || normalizeKubeMap(patch[3]["value"])["cnpg.io/cluster"] != "target" {
					t.Fatal("wrong cluster identity", patch)
				}
				w.Write([]byte(`{}`))
			}))
			defer server.Close()
			err := switchColdStableService(context.Background(), &kubeClient{baseURL: server.URL, client: server.Client()}, "ns", &coldPostgresState{SourceName: "source", TargetName: "target", Endpoint: "stable"})
			if cluster == "unrelated" {
				if err == nil || writes != 0 {
					t.Fatal("unrelated endpoint mutated", err, writes)
				}
				return
			}
			wantWrites := 0
			if cluster == "source" {
				wantWrites = 1
			}
			if err != nil || writes != wantWrites {
				t.Fatal("cutover not safely idempotent", err, writes)
			}
		})
	}
}

func TestColdTargetBootstrapNeverTouchesSourceOrServingEndpoint(t *testing.T) {
	var writes []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/clusters/restored") {
			t.Errorf("unexpected mutation: %s %s", r.Method, r.URL.Path)
		}
		var obj map[string]any
		if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
			t.Fatal(err)
		}
		writes = append(writes, obj)
		json.NewEncoder(w).Encode(obj)
	}))
	defer server.Close()
	c := &kubeClient{baseURL: server.URL, client: server.Client()}
	pg := model.AppPostgresSpec{ServiceName: "restored", EndpointServiceName: "original", CredentialSecretName: "stable-secret", Database: "demo", User: "owner", Password: "fixture", Image: "registry.example/postgres:18.3@sha256:" + strings.Repeat("a", 64), StorageSize: "4Gi", StorageClassName: "cloneable", Instances: 1}
	app := model.App{Bindings: []model.ServiceBinding{{ServiceID: "service_test", AppID: "app_test", Alias: "postgres"}}, ID: "app_test", Name: "consumer", TenantID: "tenant_test", Spec: model.AppSpec{Image: "example.invalid/app:v1", Replicas: 0}, BackingServices: []model.BackingService{{ID: "service_test", Name: "database", Type: model.BackingServiceTypePostgres, Provisioner: model.BackingServiceProvisionerManaged, Spec: model.BackingServiceSpec{Postgres: &pg}}}}
	st := &coldPostgresState{ServiceID: "service_test", TargetName: "restored", SeedName: "seed", SeedUID: "seed-uid", TargetClass: "cloneable", TargetSize: "4Gi", FileDigest: strings.Repeat("a", 64), SystemID: "1234"}
	svc := &Service{}
	if err := svc.ensureColdTarget(context.Background(), c, "namespace", st, app, pg, runtimepkg.SchedulingConstraints{}); err != nil {
		t.Fatal(err)
	}
	if len(writes) != 1 {
		t.Fatalf("writes=%d", len(writes))
	}
	spec := normalizeKubeMap(writes[0]["spec"])
	recovery := normalizeKubeMap(normalizeKubeMap(spec["bootstrap"])["recovery"])
	if recovery["database"] != "demo" || recovery["owner"] != "owner" || normalizeKubeMap(recovery["secret"])["name"] != "stable-secret" {
		t.Fatal("recovery lost application ownership/credentials", recovery)
	}
	seed := normalizeKubeMap(normalizeKubeMap(recovery["volumeSnapshots"])["storage"])
	if seed["kind"] != "PersistentVolumeClaim" || seed["name"] != "seed" {
		t.Fatal(seed)
	}
	if group, ok := seed["apiGroup"].(string); !ok || group != "" {
		t.Fatal("core PVC API group must survive CNPG defaulting as a string", seed)
	}
	if spec["imageName"] != pg.Image {
		t.Fatal("recovery changed PostgreSQL binary")
	}
	pvc := coldSeedPVC("namespace", st)
	labels := normalizeKubeMap(normalizeKubeMap(pvc["metadata"])["labels"])
	if labels["cnpg.io/cluster"] != nil || labels["cnpg.io/nodeSerial"] != nil {
		t.Fatal("seed may be adopted by source cluster")
	}
	writes = nil
	st.FileDigest = ""
	if err := svc.ensureColdTarget(context.Background(), c, "namespace", st, app, pg, runtimepkg.SchedulingConstraints{}); err == nil || len(writes) != 0 {
		t.Fatal("unverified seed bootstrapped")
	}
}

func TestColdSourceFenceSurvivesReconcileAndReplacement(t *testing.T) {
	mutations := 0
	live := map[string]any{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "metadata": map[string]any{"name": "original", "namespace": "ns", "annotations": map[string]string{coldMigrationAnnotation: "plan", "cnpg.io/fencedInstances": `["*"]`}}, "spec": map[string]any{"storage": map[string]any{"size": "2Gi"}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			mutations++
		}
		json.NewEncoder(w).Encode(live)
	}))
	defer server.Close()
	c := &kubeClient{baseURL: server.URL, client: server.Client()}
	desired := cloneKubeMap(live)
	desired["spec"] = map[string]any{"storage": map[string]any{"size": "10Gi"}}
	if err := c.applyObject(context.Background(), desired, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.replaceObjectSpec(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	if mutations != 0 {
		t.Fatal("reconcile mutated retained source")
	}
}

func TestColdTargetResumeRejectsReplacedCluster(t *testing.T) {
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writes++
		}
		json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "restored", "uid": "foreign", "annotations": map[string]string{coldMigrationAnnotation: coldStateName("service_test")}}, "spec": map[string]any{"storage": map[string]any{"storageClass": "cloneable"}}})
	}))
	defer server.Close()
	c := &kubeClient{baseURL: server.URL, client: server.Client()}
	st := &coldPostgresState{ServiceID: "service_test", TargetName: "restored", TargetUID: "expected", TargetClass: "cloneable", SeedUID: "seed-uid", SystemID: "123", FileDigest: strings.Repeat("a", 64)}
	if err := (&Service{}).ensureColdTarget(context.Background(), c, "ns", st, model.App{}, model.AppPostgresSpec{}, runtimepkg.SchedulingConstraints{}); err == nil || writes != 0 {
		t.Fatal("replaced target accepted/mutated")
	}
}

func TestColdStateRetryKeepsUIDsAndUsesResourceVersion(t *testing.T) {
	var saved map[string]any
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if saved == nil {
				http.NotFound(w, r)
				return
			}
			json.NewEncoder(w).Encode(saved)
			return
		}
		var obj map[string]any
		if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
			t.Fatal(err)
		}
		m := normalizeKubeMap(obj["metadata"])
		if saved != nil && (r.Method != http.MethodPut || m["resourceVersion"] != normalizeKubeMap(saved["metadata"])["resourceVersion"]) {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		writes++
		m["resourceVersion"] = strings.Repeat("1", writes)
		obj["metadata"] = m
		saved = obj
		json.NewEncoder(w).Encode(obj)
	}))
	defer server.Close()
	c := &kubeClient{baseURL: server.URL, client: server.Client()}
	st := &coldPostgresState{Version: 1, ServiceID: "service_fixture", SourceUID: "source-uid", SeedUID: "seed-uid", TargetUID: "target-uid", Phase: "restoring"}
	rv := ""
	if err := saveColdState(context.Background(), c, "ns", st, &rv); err != nil {
		t.Fatal(err)
	}
	restored, readRV, err := loadColdState(context.Background(), c, "ns", st.ServiceID)
	if err != nil || readRV != rv || restored.Phase != "restoring" || restored.TargetUID != "target-uid" || restored.SeedUID != "seed-uid" {
		t.Fatalf("lost durable state: %+v %v", restored, err)
	}
	stale := rv
	st.Phase = "validated"
	if err := saveColdState(context.Background(), c, "ns", st, &rv); err != nil {
		t.Fatal(err)
	}
	st.Phase = "copying"
	if err := saveColdState(context.Background(), c, "ns", st, &stale); err == nil {
		t.Fatal("stale worker overwrote validated phase")
	}
	if writes != 2 {
		t.Fatal("stale state write accepted")
	}
}

func TestColdTargetIdentitySurvivesOrdinaryReconcile(t *testing.T) {
	current := map[string]any{"metadata": map[string]any{"annotations": map[string]string{coldMigrationAnnotation: "persisted-plan"}}}
	desired := map[string]any{"metadata": map[string]any{"annotations": map[string]string{"cnpg.io/hibernation": "off"}}}
	preserveColdMigrationAnnotation(current, desired)
	a := normalizeKubeMap(normalizeKubeMap(desired["metadata"])["annotations"])
	if a[coldMigrationAnnotation] != "persisted-plan" || a["cnpg.io/hibernation"] != "off" {
		t.Fatal("normal apply would erase recovery identity", a)
	}
}
