package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func filterAbsentRetiredReleaseResources(ctx context.Context, client *kubeClient, ns string, releases []model.AppRelease) []model.AppRelease {
	var deployments, services kubeObjectList
	if _, err := client.doMetadataList(ctx, "/apis/apps/v1/namespaces/"+url.PathEscape(ns)+"/deployments", &deployments); err != nil {
		return releases
	}
	if _, err := client.doMetadataList(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/services", &services); err != nil {
		return releases
	}
	if deployments.Metadata.Continue != "" || services.Metadata.Continue != "" {
		return releases
	}
	presentDeployments, presentServices := map[string]bool{}, map[string]bool{}
	for _, d := range deployments.Items {
		presentDeployments[objectStringField(objectMapField(d, "metadata"), "name")] = true
	}
	for _, s := range services.Items {
		presentServices[objectStringField(objectMapField(s, "metadata"), "name")] = true
	}
	filtered := make([]model.AppRelease, 0, len(releases))
	for _, r := range releases {
		b := r.RevisionWorkload
		if r.Status == model.AppReleaseStatusRetired && b != nil && !presentDeployments[b.DeploymentName] && !presentServices[b.ServiceName] {
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered
}

// A complete, unfiltered ownership index. Labels alone cannot reveal a child
// whose labels drifted while its owner still points at this Deployment.
type releaseDrainInventory struct {
	replicasByDeployment map[string][]map[string]any
	podsByReplica        map[string][]map[string]any
	podsByRelease        map[string][]map[string]any
	podsByWorkload       map[string][]map[string]any
	err                  error
}

type initialReleaseDrainInventory struct {
	mu          sync.Mutex
	byNamespace map[string]*releaseDrainInventory
}
type initialReleaseDrainInventoryKey struct{}
type releaseRetirementDiscoveryKey struct{}

func withInitialReleaseDrainInventory(ctx context.Context) context.Context {
	if _, ok := ctx.Value(initialReleaseDrainInventoryKey{}).(*initialReleaseDrainInventory); ok {
		return ctx
	}
	return context.WithValue(ctx, initialReleaseDrainInventoryKey{}, &initialReleaseDrainInventory{byNamespace: map[string]*releaseDrainInventory{}})
}

func readReleaseDrainInventory(ctx context.Context, client *kubeClient, ns string) (*releaseDrainInventory, error) {
	initial, _ := ctx.Value(initialReleaseDrainInventoryKey{}).(*initialReleaseDrainInventory)
	if initial != nil {
		initial.mu.Lock()
		defer initial.mu.Unlock()
		if inventory := initial.byNamespace[ns]; inventory != nil {
			return inventory, nil
		}
		if discovered, ok := ctx.Value(releaseRetirementDiscoveryKey{}).(*releaseDrainInventory); ok && discovered != nil {
			initial.byNamespace[ns] = discovered
			return discovered, nil
		}
	}
	var replicas, pods kubeObjectList
	if _, err := client.doMetadataList(ctx, "/apis/apps/v1/namespaces/"+url.PathEscape(ns)+"/replicasets", &replicas); err != nil {
		return nil, err
	}
	podPath := "/api/v1/namespaces/" + url.PathEscape(ns) + "/pods"
	if initial != nil {
		if _, err := client.doJSON(ctx, http.MethodGet, podPath, nil, &pods); err != nil {
			return nil, err
		}
	} else {
		// Fresh full ownership metadata is necessary, but repeatedly transferring
		// every unrelated Pod's executable spec is not. Only owned Pods are
		// hydrated below, with exact UID/resourceVersion checks.
		if _, err := client.doMetadataList(ctx, podPath, &pods); err != nil {
			return nil, err
		}
	}
	if replicas.Metadata.Continue != "" || pods.Metadata.Continue != "" {
		return nil, fmt.Errorf("release ownership inventory is incomplete (continued list)")
	}
	inv := indexReleaseDrainInventory(replicas.Items, pods.Items)
	if initial != nil {
		initial.byNamespace[ns] = inv
	}
	return inv, nil
}

func hydrateReleaseDrainPod(ctx context.Context, client *kubeClient, ns string, pod map[string]any) (map[string]any, error) {
	if pod["spec"] != nil {
		return pod, nil
	} // Servers may choose the full Accept fallback.
	meta := objectMapField(pod, "metadata")
	name, uid, version := objectStringField(meta, "name"), objectStringField(meta, "uid"), objectStringField(meta, "resourceVersion")
	if name == "" || uid == "" || version == "" {
		return nil, fmt.Errorf("release Pod metadata identity is incomplete")
	}
	full, found, err := client.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/pods/"+url.PathEscape(name))
	if err != nil {
		return nil, fmt.Errorf("owned release Pod is unavailable: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("owned release Pod disappeared after ownership snapshot")
	}
	m := objectMapField(full, "metadata")
	if objectStringField(m, "uid") != uid || objectStringField(m, "resourceVersion") != version {
		return nil, fmt.Errorf("release Pod changed after ownership snapshot")
	}
	return full, nil
}

func indexReleaseDrainInventory(replicas, pods []map[string]any) *releaseDrainInventory {
	inv := &releaseDrainInventory{replicasByDeployment: map[string][]map[string]any{}, podsByReplica: map[string][]map[string]any{}, podsByRelease: map[string][]map[string]any{}, podsByWorkload: map[string][]map[string]any{}}
	for _, rs := range replicas {
		for _, ref := range mapSlice(nestedObjectValue(rs, "metadata", "ownerReferences")) {
			if ref["kind"] == "Deployment" {
				uid := objectStringField(ref, "uid")
				inv.replicasByDeployment[uid] = append(inv.replicasByDeployment[uid], rs)
			}
		}
	}
	names := map[string]bool{}
	for _, pod := range pods {
		meta := objectMapField(pod, "metadata")
		name := objectStringField(meta, "name")
		if name == "" || names[name] {
			inv.err = fmt.Errorf("ambiguous Pod in release ownership inventory")
		}
		names[name] = true
		labels := objectStringMapValue(meta["labels"])
		inv.podsByRelease[labels[runtime.FugueLabelAppReleaseID]] = append(inv.podsByRelease[labels[runtime.FugueLabelAppReleaseID]], pod)
		inv.podsByWorkload[labels[runtime.FugueLabelAppWorkload]] = append(inv.podsByWorkload[labels[runtime.FugueLabelAppWorkload]], pod)
		for _, ref := range mapSlice(meta["ownerReferences"]) {
			// Match the previous complete-list reader: any reference to an
			// owned UID makes this Pod relevant. The capture then rejects a
			// wrong kind/controller/name rather than silently hiding that Pod.
			uid := objectStringField(ref, "uid")
			if uid != "" {
				inv.podsByReplica[uid] = append(inv.podsByReplica[uid], pod)
			}
		}
	}
	return inv
}

func (inv *releaseDrainInventory) podsForRelease(releaseID, workload string, canonical bool, owned map[string]string) []map[string]any {
	byName := map[string]map[string]any{}
	add := func(pods []map[string]any) {
		for _, pod := range pods {
			byName[objectStringField(objectMapField(pod, "metadata"), "name")] = pod
		}
	}
	if canonical {
		add(inv.podsByWorkload[workload])
	} else if releaseID != "" {
		add(inv.podsByRelease[releaseID])
	}
	for _, uid := range owned {
		add(inv.podsByReplica[uid])
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	pods := make([]map[string]any, 0, len(names))
	for _, name := range names {
		pods = append(pods, byName[name])
	}
	return pods
}

func captureReleaseApplicationContainers(pod map[string]any, requireReady bool, p *releaseDrainPod) error {
	statuses := map[string]map[string]any{}
	for _, c := range mapSlice(nestedObjectValue(pod, "status", "containerStatuses")) {
		name := objectStringField(c, "name")
		if name == "" || statuses[name] != nil {
			return fmt.Errorf("release container status identity is ambiguous")
		}
		statuses[name] = c
	}
	containers := mapSlice(nestedObjectValue(pod, "spec", "containers"))
	if len(statuses) != len(containers) {
		return fmt.Errorf("release container status set is incomplete")
	}
	for _, declared := range containers {
		name := objectStringField(declared, "name")
		if name == "fugue-drain-agent" {
			continue
		}
		c := statuses[name]
		id := objectStringField(c, "containerID")
		state := objectMapField(c, "state")
		if id == "" || len(state) != 1 || requireReady && (c["ready"] != true || state["running"] == nil) {
			return fmt.Errorf("release container has no stable executable identity")
		}
		kind := ""
		for _, candidate := range []string{"running", "waiting", "terminated"} {
			if state[candidate] != nil {
				kind = candidate
			}
		}
		if kind == "" {
			return fmt.Errorf("release container runtime state is unknown")
		}
		// Readiness alone is intentionally excluded. Restarts, state transitions,
		// runtime replacement and exit times invalidate the positive observation.
		p.Containers = append(p.Containers, fmt.Sprintf("%s:%s:%v:%s:%s:%s", name, id, c["restartCount"], kind,
			objectStringField(objectMapValue(state[kind]), "startedAt"), objectStringField(objectMapValue(state[kind]), "finishedAt")))
	}
	return nil
}
