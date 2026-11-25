package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"testing"

	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/runtime"
)

func TestTenantHoldReconcilesAttachedStandaloneAndRetainedPostgres(t *testing.T) {
	state, app := livePostgresFixture(t)
	attached := app.BackingServices[0]
	standalone, err := state.CreateBackingService(app.TenantID, app.ProjectID, "standalone", "", model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{ServiceName: "independent", RuntimeID: model.DefaultManagedRuntimeID, StorageSize: "1Gi"}})
	if err != nil {
		t.Fatal(err)
	}
	ns := runtime.NamespaceForTenant(app.TenantID)
	objects := map[string]map[string]any{}
	add := func(name, hibernation, owner string, db model.BackingService) {
		objects[name] = map[string]any{"metadata": map[string]any{"name": name, "uid": "uid-" + name, "resourceVersion": "1", "namespace": ns,
			"labels": map[string]string{runtime.FugueLabelTenantID: app.TenantID, runtime.FugueLabelProjectID: app.ProjectID,
				runtime.FugueLabelBackingServiceID: db.ID, runtime.FugueLabelOwnerAppID: owner},
			"annotations": map[string]any{runtime.CloudNativePGHibernationAnno: hibernation, "example.org/keep": "yes"}}}
	}
	add("current", "off", attached.OwnerAppID, attached)
	add("independent", "off", "", standalone)
	add("old-generation", "off", attached.OwnerAppID, attached)
	add("already-asleep", "on", "", standalone)
	add("foreign-owner", "off", "other", attached)
	f := newBootstrapJobFixture(t, ns, "independent", "uid-independent", 1, "initdb")
	objects[f.cluster]["status"] = map[string]any{"phase": "Setting up primary"}
	writes := 0
	c := &kubeClient{baseURL: "http://kube.test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/apis/postgresql.cnpg.io/v1/clusters":
			var items []map[string]any
			for _, obj := range objects {
				items = append(items, obj)
			}
			return bootstrapList(t, items...), nil
		case req.Method == http.MethodGet && req.URL.Path == postgresJobPath(ns, ""):
			if req.URL.Query().Get("labelSelector") == "cnpg.io/cluster="+f.cluster {
				return bootstrapList(t, f.job), nil
			}
			return bootstrapList(t), nil
		case req.Method == http.MethodGet && req.URL.Path == pgretire.Path(ns, "pods", ""):
			return bootstrapList(t, f.pod), nil
		case req.Method == http.MethodPatch && req.URL.Path == postgresJobPath(ns, f.name):
			next, err := patchBootstrapFixture(t, req, f.job)
			if err != nil {
				t.Fatal(err)
			}
			f.job = next
		case req.Method == http.MethodPatch && req.URL.Path == cloudNativePGClusterAPIPath(ns, path.Base(req.URL.Path)):
			meta := pgretire.Meta(objects[path.Base(req.URL.Path)])
			var patch map[string]any
			if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
				t.Fatal(err)
			}
			m := pgretire.Meta(patch)
			if len(patch) != 1 || len(m) != 3 || m["uid"] != meta["uid"] || m["resourceVersion"] != meta["resourceVersion"] {
				t.Fatalf("unexpected Cluster mutation: %v", patch)
			}
			for k, v := range pgretire.Object(m["annotations"]) {
				if k != postgresTenantHold && k != runtime.CloudNativePGHibernationAnno {
					t.Fatalf("unexpected annotation %s", k)
				}
				a := pgretire.Object(meta["annotations"])
				if v == nil {
					delete(a, k)
				} else {
					a[k] = v
				}
			}
			meta["resourceVersion"] = meta["resourceVersion"].(string) + "1"
		default:
			t.Fatalf("unexpected Kubernetes request %s %s", req.Method, req.URL)
		}
		writes++
		return okJSONResponse("{}"), nil
	})}}
	s := &Service{Store: state}
	ctx := context.Background()
	run := func(held bool) {
		t.Helper()
		if err := state.SetTenantAccessHold(ctx, app.TenantID, held); err != nil {
			t.Fatal(err)
		}
		if err := s.reconcileLivePostgres(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	check := func(name, hib, previous string) {
		t.Helper()
		a := pgretire.Object(pgretire.Meta(objects[name])["annotations"])
		if a[runtime.CloudNativePGHibernationAnno] != hib || pgretire.String(a, postgresTenantHold) != previous || a["example.org/keep"] != "yes" {
			t.Fatalf("%s annotations: %v", name, a)
		}
	}
	run(true)
	for _, name := range []string{"current", "independent", "old-generation"} {
		check(name, "on", "off")
	}
	check("already-asleep", "on", "on")
	check("foreign-owner", "off", "")
	if !postgresBootstrapJobHeld(f.job, "tenant-hold:"+f.clusterUID) {
		t.Fatal("pending bootstrap was not suspended")
	}
	f.settle(t)
	if writes != 5 {
		t.Fatalf("hold writes = %d", writes)
	}
	run(true)
	if writes != 5 {
		t.Fatalf("repeated hold wrote again: %d", writes)
	}
	run(false)
	check("current", "off", "")
	check("independent", "off", "")
	check("old-generation", "on", "")
	check("already-asleep", "on", "")
	check("foreign-owner", "off", "")
	if pgretire.Object(f.job["spec"])["suspend"] != false {
		t.Fatal("owned pending bootstrap did not resume")
	}
	run(false)
	if writes != 10 {
		t.Fatalf("total writes = %d", writes)
	}
}
