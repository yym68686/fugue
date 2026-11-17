// Package retiretest supplies synthetic Kubernetes objects for retirement tests.
package retiretest

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/pgretire"
)

func New(app model.App, svc model.BackingService) map[string]map[string]any {
	ns := "fg-" + strings.ReplaceAll(app.TenantID, "_", "-")
	target := svc.Spec.Postgres.ServiceName
	endpoint := model.PostgresEndpointName(*svc.Spec.Postgres)
	objects := map[string]map[string]any{}
	add := func(path, name, uid string, spec, status map[string]any) map[string]any {
		obj := map[string]any{"metadata": map[string]any{"name": name, "uid": uid, "resourceVersion": "1", "namespace": ns}, "spec": spec, "status": status}
		objects[path] = obj
		return obj
	}
	ready := func() []any { return []any{map[string]any{"type": "Ready", "status": "True"}} }
	owner := func(name, uid string) []any {
		return []any{map[string]any{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "name": name, "uid": uid, "controller": true}}
	}
	source := add(pgretire.Path(ns, "clusters", "previous"), "previous", "source-uid", map[string]any{"instances": 1}, map[string]any{})
	pgretire.Meta(source)["labels"] = map[string]any{"fugue.pro/backing-service-id": svc.ID, "fugue.pro/tenant-id": app.TenantID}
	pgretire.Meta(source)["annotations"] = map[string]any{"cnpg.io/fencedInstances": `["*"]`, "fugue.pro/cold-migration": pgretire.MigrationName(svc.ID)}
	dest := add(pgretire.Path(ns, "clusters", target), target, "target-uid", map[string]any{"instances": 1}, map[string]any{"currentPrimary": target + "-1", "systemID": "123456", "conditions": ready()})
	pgretire.Meta(dest)["labels"] = map[string]any{"fugue.pro/backing-service-id": svc.ID}
	for _, item := range []struct{ name, uid, node, claim, cluster, clusterUID string }{
		{"previous-1", "source-pod-uid", "node-a", "previous-1", "previous", "source-uid"},
		{target + "-1", "target-pod-uid", "node-b", target + "-1", target, "target-uid"},
	} {
		pod := add(pgretire.Path(ns, "pods", item.name), item.name, item.uid, map[string]any{"nodeName": item.node, "containers": []any{map[string]any{"name": "postgres"}}, "volumes": []any{map[string]any{"persistentVolumeClaim": map[string]any{"claimName": item.claim}}}}, map[string]any{"phase": "Running", "podIP": "10.0.0.2", "conditions": ready()})
		pgretire.Meta(pod)["ownerReferences"] = owner(item.cluster, item.clusterUID)
		pgretire.Meta(pod)["labels"] = map[string]any{pgretire.ClusterLabel: item.cluster}
	}
	pvc := add(pgretire.Path(ns, "persistentvolumeclaims", "previous-1"), "previous-1", "source-pvc-uid", map[string]any{"volumeName": "source-pv", "resources": map[string]any{"requests": map[string]any{"storage": "1Gi"}}}, map[string]any{"phase": "Bound"})
	pgretire.Meta(pvc)["ownerReferences"] = owner("previous", "source-uid")
	pgretire.Meta(pvc)["labels"] = map[string]any{pgretire.ClusterLabel: "previous"}
	add("/api/v1/persistentvolumes/source-pv", "source-pv", "source-pv-uid", map[string]any{"persistentVolumeReclaimPolicy": "Delete", "claimRef": map[string]any{"uid": "source-pvc-uid", "name": "previous-1", "namespace": ns}}, map[string]any{"phase": "Bound"})
	// A label does not make this ownerless, unused claim part of the source.
	protected := add(pgretire.Path(ns, "persistentvolumeclaims", "unowned-archive"), "unowned-archive", "protected-pvc-uid", map[string]any{"volumeName": "protected-pv"}, map[string]any{"phase": "Bound"})
	pgretire.Meta(protected)["labels"] = map[string]any{pgretire.ClusterLabel: "previous"}
	add("/api/v1/persistentvolumes/protected-pv", "protected-pv", "protected-pv-uid", map[string]any{"persistentVolumeReclaimPolicy": "Delete", "claimRef": map[string]any{"uid": "protected-pvc-uid", "name": "unowned-archive", "namespace": ns}}, map[string]any{"phase": "Bound"})
	add(pgretire.Path(ns, "services", endpoint), endpoint, "endpoint-uid", map[string]any{"selector": map[string]any{pgretire.ClusterLabel: target, "cnpg.io/instanceRole": "primary"}}, nil)
	ep := add(pgretire.Path(ns, "endpoints", endpoint), endpoint, "endpoints-uid", nil, nil)
	ep["subsets"] = []any{map[string]any{"addresses": []any{map[string]any{"ip": "10.0.0.2", "targetRef": map[string]any{"name": target + "-1", "uid": "target-pod-uid"}}}}}
	for _, node := range []string{"node-a", "node-b"} {
		add("/api/v1/nodes/"+node, node, node+"-uid", nil, map[string]any{"conditions": ready()})
	}
	cm := add(pgretire.Path(ns, "configmaps", pgretire.MigrationName(svc.ID)), pgretire.MigrationName(svc.ID), "migration-uid", nil, nil)
	state, _ := json.Marshal(map[string]any{"version": 1, "phase": "complete", "service_id": svc.ID, "source_name": "previous", "source_uid": "source-uid", "source_pvc": "previous-1", "source_pvc_uid": "source-pvc-uid", "source_pv": "source-pv", "target_name": target, "target_uid": "target-uid", "endpoint": endpoint, "system_id": "123456"})
	cm["data"] = map[string]any{"state.json": string(state)}
	return objects
}

func Get(objects map[string]map[string]any) pgretire.Get {
	return func(_ context.Context, path string) (map[string]any, bool, error) {
		if obj, ok := objects[path]; ok {
			return obj, true, nil
		}
		if strings.HasSuffix(path, "/pods") || strings.HasSuffix(path, "/jobs") || strings.HasSuffix(path, "/persistentvolumeclaims") {
			items := []any{}
			keys := []string{}
			for k := range objects {
				if strings.HasPrefix(k, path+"/") {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			for _, k := range keys {
				items = append(items, objects[k])
			}
			return map[string]any{"items": items}, true, nil
		}
		return nil, false, nil
	}
}

func ServeRead(objects map[string]map[string]any, w http.ResponseWriter, r *http.Request) {
	obj, found, _ := Get(objects)(r.Context(), r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if !found {
		w.WriteHeader(404)
		return
	}
	json.NewEncoder(w).Encode(obj)
}
