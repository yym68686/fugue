package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestColdDirectPrimaryNeverRecopiesAnActivatedOrReplacedDestination(t *testing.T) {
	for _, scenario := range []string{"prepared", "unprepared", "cluster-replaced", "claim-replaced", "foreign-owner", "data-source"} {
		t.Run(scenario, func(t *testing.T) {
			st := &coldPostgresState{ServiceID: "service", TargetName: "destination", TargetUID: "target-uid", TargetClass: "local-storage", TargetSize: "3Gi", DirectBootstrap: true, PrimaryPrepared: scenario != "unprepared", PrimaryPVCUID: "primary-uid"}
			mutations := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					mutations++
					t.Errorf("activated target mutated: %s %s", r.Method, r.URL.Path)
				}
				if strings.Contains(r.URL.Path, "/clusters/") {
					uid := st.TargetUID
					if scenario == "cluster-replaced" {
						uid = "foreign"
					}
					json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": st.TargetName, "uid": uid, "annotations": map[string]string{coldMigrationAnnotation: coldStateName(st.ServiceID)}}})
					return
				}
				if !strings.HasSuffix(r.URL.Path, "/persistentvolumeclaims/destination-1") {
					t.Errorf("unexpected read: %s", r.URL.Path)
				}
				uid, owner := st.PrimaryPVCUID, st.TargetUID
				if scenario == "claim-replaced" {
					uid = "foreign"
				}
				if scenario == "foreign-owner" {
					owner = "foreign"
				}
				spec := map[string]any{"storageClassName": st.TargetClass, "resources": map[string]any{"requests": map[string]string{"storage": st.TargetSize}}}
				if scenario == "data-source" {
					spec["dataSource"] = map[string]string{"name": "foreign"}
				}
				json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": uid, "resourceVersion": "42", "labels": map[string]string{coldMigrationAnnotation: coldStateName(st.ServiceID), "cnpg.io/cluster": st.TargetName}, "ownerReferences": []any{map[string]any{"uid": owner, "name": st.TargetName, "kind": "Cluster", "apiVersion": runtimepkg.CloudNativePGAPIVersion, "controller": true}}}, "spec": spec})
			}))
			defer srv.Close()
			rv := "1"
			err := (&Service{}).prepareColdPrimaryVolume(context.Background(), &kubeClient{baseURL: srv.URL, client: srv.Client()}, "ns", st, &rv, runtimepkg.SchedulingConstraints{}, "operation")
			if (err == nil) != (scenario == "prepared") || mutations != 0 {
				t.Fatalf("target replay guard failed: %v writes=%d", err, mutations)
			}
		})
	}
}

func TestColdDirectPrimaryHandoffIsMetadataOnlyAndRequiresDurableReceipt(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		writes := 0
		st := &coldPostgresState{TargetName: "destination", PrimaryPVCUID: "primary-uid", PrimaryPrepared: prepared}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writes++
			var patch []map[string]any
			json.NewDecoder(r.Body).Decode(&patch)
			if r.Method != http.MethodPatch || r.Header.Get("Content-Type") != "application/json-patch+json" || len(patch) != 4 || patch[0]["path"] != "/metadata/uid" || patch[0]["value"] != st.PrimaryPVCUID || patch[1]["path"] != "/metadata/resourceVersion" || patch[1]["value"] != "42" || patch[2]["path"] != "/metadata/labels" || patch[3]["path"] != "/metadata/annotations" {
				t.Fatal("lost exact claim CAS or changed nonmetadata fields", patch)
			}
			labels, annotations := normalizeKubeMap(patch[2]["value"]), normalizeKubeMap(patch[3]["value"])
			if labels["audit"] != "retained" || labels["cnpg.io/cluster"] != st.TargetName || labels["cnpg.io/instanceRole"] != "primary" || annotations["cnpg.io/nodeSerial"] != "1" || annotations["cnpg.io/pvcStatus"] != "ready" {
				t.Fatal("missing verified CNPG retained-volume metadata", patch)
			}
			w.Write([]byte(`{}`))
		}))
		claim := map[string]any{"metadata": map[string]any{"uid": st.PrimaryPVCUID, "resourceVersion": "42", "labels": map[string]string{"audit": "retained"}}}
		err := handoffColdPrimaryClaim(context.Background(), &kubeClient{baseURL: srv.URL, client: srv.Client()}, "/claim", claim, st)
		srv.Close()
		if (err == nil) != prepared || (writes == 1) != prepared {
			t.Fatal("unverified claim handed off", err, writes)
		}
	}
}

func TestColdDirectBootstrapHasNoCSICloneDependencyAndBlocksUnpreparedPrimary(t *testing.T) {
	writes := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		writes++
		var obj map[string]any
		json.NewDecoder(r.Body).Decode(&obj)
		spec := normalizeKubeMap(obj["spec"])
		bootstrap := normalizeKubeMap(spec["bootstrap"])
		if bootstrap["recovery"] != nil || normalizeKubeMap(bootstrap["initdb"])["database"] != "records" || spec["instances"] != float64(1) {
			t.Fatal("direct bootstrap depends on clone or changes staged instance count", bootstrap)
		}
		json.NewEncoder(w).Encode(obj)
	}))
	defer srv.Close()
	c := &kubeClient{baseURL: srv.URL, client: srv.Client()}
	pg := model.AppPostgresSpec{ServiceName: "destination", Database: "records", User: "owner", Password: "fixture", Image: "example.test/postgres:18", StorageSize: "3Gi", StorageClassName: "local-storage", Instances: 2, SynchronousReplicas: 1}
	app := model.App{ID: "app", TenantID: "tenant", Bindings: []model.ServiceBinding{{ServiceID: "service", AppID: "app", Alias: "postgres"}}, BackingServices: []model.BackingService{{ID: "service", Type: model.BackingServiceTypePostgres, Provisioner: model.BackingServiceProvisionerManaged, Spec: model.BackingServiceSpec{Postgres: &pg}}}}
	st := &coldPostgresState{ServiceID: "service", TargetName: "destination", SeedName: "seed", SeedUID: "seed-uid", FileDigest: "verified", SystemID: "123", TargetClass: pg.StorageClassName, TargetSize: pg.StorageSize, DirectBootstrap: true}
	s := &Service{}
	if err := s.ensureColdTarget(context.Background(), c, "ns", st, app, pg, runtimepkg.SchedulingConstraints{}); err == nil || writes != 0 {
		t.Fatal("unprepared primary activated", err)
	}
	st.PrimaryPrepared, st.PrimaryPVCUID = true, "primary-uid"
	if err := s.ensureColdTarget(context.Background(), c, "ns", st, app, pg, runtimepkg.SchedulingConstraints{}); err != nil || writes != 1 {
		t.Fatal("verified primary not bootstrapped", err, writes)
	}
}
