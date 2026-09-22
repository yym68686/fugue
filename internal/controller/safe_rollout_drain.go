package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/drainprotocol"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

type kubeSafeRolloutDrainObserver struct{ service *Service }

type releaseDrainPod struct {
	Name       string   `json:"name"`
	UID        string   `json:"uid"`
	ReplicaUID string   `json:"replica_set_uid"`
	AgentImage string   `json:"agent_image"`
	Restarts   int      `json:"agent_restarts"`
	Port       int      `json:"agent_port"`
	QuietMS    int64    `json:"quiet_period_ms"`
	AppPorts   []int    `json:"app_ports"`
	Containers []string `json:"container_identities"`
}

type releaseDrainWorkload struct {
	DeploymentUID     string            `json:"deployment_uid"`
	DeploymentVersion string            `json:"deployment_version"`
	ServiceVersion    string            `json:"service_version"`
	Generation        int64             `json:"generation"`
	Pods              []releaseDrainPod `json:"pods"`
	ReplicaSets       []string          `json:"replica_set_uids"`
}

func (q kubeSafeRolloutDrainObserver) QuerySafeRolloutDrainMetrics(ctx context.Context, app model.App, previous model.AppRelease, since time.Time) (safeRolloutDrainMetrics, error) {
	out := safeRolloutDrainMetrics{Source: "kubernetes_pod_drain", Summary: map[string]any{"release_id": previous.ID, "previous_deployment": previous.DeploymentName}}
	s := q.service
	if s == nil || s.Store == nil || !s.Config.KubectlApply {
		return out, fmt.Errorf("release drain observer is not configured")
	}
	expected := previous
	binding := previous.RevisionWorkload
	if binding == nil || binding.Namespace != runtime.NamespaceForTenant(app.TenantID) || binding.DeploymentUID == "" || binding.ServiceUID == "" || binding.ReleaseKey == "" || binding.DeploymentGeneration < 1 ||
		binding.RuntimeID != previous.RuntimeID || !s.migrationImageRefsEquivalent(app, binding.ImageRef, previous.ResolvedImageRef) {
		return out, fmt.Errorf("drain requires an immutable revision workload binding")
	}
	previous.DeploymentName, previous.ServiceName, previous.UpstreamURL = binding.DeploymentName, binding.ServiceName, ""
	out.Summary["previous_deployment"] = previous.DeploymentName
	out.Summary["revision_workload"] = *binding
	// A canonical Deployment can now serve a different release. Draining it
	// would not prove anything about this release's original connections.
	if previous.ID == "" || previous.AppID != app.ID || previous.TenantID != app.TenantID ||
		previous.DeploymentName == "" || previous.DeploymentName == runtime.RuntimeAppResourceName(app) ||
		previous.RuntimeID == "" || previous.ResolvedImageRef == "" || !store.AppReleaseAwaitingDrain(previous) {
		return out, fmt.Errorf("drain requires an independent release workload identity")
	}
	policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil || policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 || policy.CandidateWeight != 0 || policy.CandidateReleaseID != "" || policy.StableReleaseID == previous.ID || policy.StableReleaseID == "" {
		return out, fmt.Errorf("release traffic has not been withdrawn")
	}
	stable, err := s.Store.GetAppRelease(app.TenantID, true, policy.StableReleaseID)
	if err != nil {
		return out, fmt.Errorf("read stable release before drain: %w", err)
	}
	if stable.DeploymentName == previous.DeploymentName || stable.ServiceName == previous.ServiceName {
		return out, fmt.Errorf("stable traffic still uses the previous workload")
	}
	proof, err := s.edgeBundleObserverForSafeRollout().WaitForSafeRolloutEdgeRouteBundle(ctx, app, stable, 0, since)
	if err != nil || !proof.Ready {
		return out, fmt.Errorf("current stable traffic is not confirmed before drain")
	}
	client, err := s.kubeClient()
	if err != nil {
		return out, err
	}
	before, err := s.captureReleaseDrainWorkload(ctx, client, app, previous)
	if err != nil {
		return out, err
	}
	out.Summary["workload"] = before
	// Observation is bounded independently of the agent's configured drain
	// timeout. A busy Pod stays retained and a later reconcile can retry.
	drainCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	quietPeriod := time.Duration(0)
	for _, pod := range before.Pods {
		if period := time.Duration(pod.QuietMS) * time.Millisecond; period > quietPeriod {
			quietPeriod = period
		}
	}
	var idleSince time.Time
	for {
		out.ActiveConnections = 0
		receipts := make([]map[string]any, 0, len(before.Pods))
		for _, pod := range before.Pods {
			started := time.Now().UTC()
			receipt, err := client.observeReleasePodDrain(drainCtx, runtime.NamespaceForTenant(app.TenantID), pod)
			if err != nil {
				out.ObserverErrors++
				return out, fmt.Errorf("observe Pod %s: %w", pod.Name, err)
			}
			out.SampleCount++
			out.ActiveConnections += *receipt.ActiveConnections
			receipts = append(receipts, map[string]any{"pod_uid": pod.UID, "started_at": started, "completed_at": time.Now().UTC(), "receipt": receipt})
		}
		out.Summary["receipts"] = receipts
		out.MaxActiveConnections = max(out.MaxActiveConnections, out.ActiveConnections)
		if out.ActiveConnections == 0 {
			if idleSince.IsZero() {
				idleSince = time.Now()
			}
			if time.Since(idleSince) >= quietPeriod {
				out.FinalCount = len(before.Pods)
				out.Summary["quiet_since"] = idleSince.UTC()
				break
			}
		} else {
			idleSince = time.Time{}
		}
		select {
		case <-drainCtx.Done():
			return out, fmt.Errorf("release did not become idle within observation budget")
		case <-time.After(200 * time.Millisecond):
		}
	}
	after, err := s.captureReleaseDrainWorkload(ctx, client, app, previous)
	if err != nil || !reflect.DeepEqual(before, after) {
		return out, fmt.Errorf("release workload changed during drain observation")
	}
	currentPolicy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil || !reflect.DeepEqual(policy, currentPolicy) {
		return out, fmt.Errorf("traffic policy changed during drain observation")
	}
	currentRelease, err := s.Store.GetAppRelease(app.TenantID, true, previous.ID)
	if err != nil || !sameReleaseDrainTarget(expected, currentRelease) || drainCtx.Err() != nil {
		return out, fmt.Errorf("release target changed during drain observation")
	}
	out.Ready = true
	out.Workload = &after
	out.ObservedAt = time.Now().UTC()
	out.Summary["ready"] = true
	out.Summary["active_connections"] = out.ActiveConnections
	out.Summary["observer_errors"] = out.ObserverErrors
	return out, nil
}

func sameReleaseDrainTarget(a, b model.AppRelease) bool {
	return a.ID == b.ID && a.TenantID == b.TenantID && a.AppID == b.AppID && a.RuntimeID == b.RuntimeID && a.ResolvedImageRef == b.ResolvedImageRef &&
		a.DeploymentName == b.DeploymentName && a.ServiceName == b.ServiceName && a.UpstreamURL == b.UpstreamURL && a.Role == b.Role && a.Status == b.Status && a.UpdatedAt.Equal(b.UpdatedAt) && reflect.DeepEqual(a.RevisionWorkload, b.RevisionWorkload)
}

func (s *Service) captureReleaseDrainWorkload(ctx context.Context, client *kubeClient, app model.App, release model.AppRelease) (releaseDrainWorkload, error) {
	var out releaseDrainWorkload
	ns := runtime.NamespaceForTenant(app.TenantID)
	deployment, found, err := client.getRawObject(ctx, deploymentAPIPath(ns, release.DeploymentName))
	if err != nil || !found {
		return out, fmt.Errorf("release Deployment is unavailable")
	}
	meta := objectMapField(deployment, "metadata")
	owner := func(metadata map[string]any) bool {
		labels := objectStringMapValue(metadata["labels"])
		return appWorkloadOwnerMatches(metadata, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) && labels[runtime.FugueLabelAppReleaseID] == release.ID
	}
	if !owner(meta) || objectStringField(meta, "deletionTimestamp") != "" {
		return out, fmt.Errorf("release Deployment ownership or lifecycle differs")
	}
	binding := release.RevisionWorkload
	if binding == nil || binding.DeploymentUID != objectStringField(meta, "uid") || binding.ReleaseKey != objectStringMapValue(nestedObjectValue(deployment, "spec", "template", "metadata", "annotations"))[runtime.FugueAnnotationReleaseKey] {
		return out, fmt.Errorf("drain Deployment differs from immutable binding")
	}
	service, found, err := client.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/services/"+url.PathEscape(binding.ServiceName))
	if err != nil || !found {
		return out, fmt.Errorf("drain Service unavailable")
	}
	sm := objectMapField(service, "metadata")
	selector := objectStringMapValue(nestedObjectValue(service, "spec", "selector"))
	if objectStringField(sm, "uid") != binding.ServiceUID || objectStringField(sm, "deletionTimestamp") != "" || !owner(sm) || selector[runtime.FugueLabelAppWorkload] != binding.DeploymentName || selector[runtime.FugueLabelAppReleaseID] != release.ID {
		return out, fmt.Errorf("drain Service differs from immutable binding")
	}
	var dep kubeDeployment
	data, _ := json.Marshal(deployment)
	if err := json.Unmarshal(data, &dep); err != nil || dep.Metadata.UID == "" || dep.Spec.Replicas == nil || *dep.Spec.Replicas < 1 || *dep.Spec.Replicas > 32 || !managedDeploymentStatusReady(dep, *dep.Spec.Replicas) || !s.migrationImageRefsEquivalent(app, deploymentPrimaryContainerImage(dep), release.ResolvedImageRef) {
		return out, fmt.Errorf("release Deployment is not a complete current workload")
	}
	if dep.Metadata.Generation < binding.DeploymentGeneration {
		return out, fmt.Errorf("drain Deployment generation regressed")
	}
	out.DeploymentUID, out.Generation = dep.Metadata.UID, dep.Metadata.Generation
	out.DeploymentVersion, out.ServiceVersion = objectStringField(meta, "resourceVersion"), objectStringField(sm, "resourceVersion")
	var replicas, pods kubeObjectList
	// Follow ownership even when a child's selector labels have changed. A
	// label-only list could hide an old Pod that still owns active connections.
	if _, err := client.doMetadataList(ctx, "/apis/apps/v1/namespaces/"+url.PathEscape(ns)+"/replicasets", &replicas); err != nil {
		return out, err
	}
	owned := map[string]string{}
	for _, rs := range replicas.Items {
		rm := objectMapField(rs, "metadata")
		if appWorkloadControlledBy(rm, "Deployment", release.DeploymentName, out.DeploymentUID) {
			if !owner(rm) || objectStringField(rm, "uid") == "" || objectStringField(rm, "deletionTimestamp") != "" {
				return out, fmt.Errorf("release ReplicaSet ownership or lifecycle differs")
			}
			uid := objectStringField(rm, "uid")
			owned[objectStringField(rm, "name")] = uid
			out.ReplicaSets = append(out.ReplicaSets, uid)
		}
	}
	sort.Strings(out.ReplicaSets)
	if _, err := client.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+url.PathEscape(ns)+"/pods", nil, &pods); err != nil {
		return out, err
	}
	for _, pod := range pods.Items {
		pm := objectMapField(pod, "metadata")
		belongs := objectStringMapValue(pm["labels"])[runtime.FugueLabelAppReleaseID] == release.ID
		for _, ref := range mapSlice(pm["ownerReferences"]) {
			if uid := owned[objectStringField(ref, "name")]; uid != "" && ref["uid"] == uid {
				belongs = true
			}
		}
		if !belongs {
			continue
		}
		if !owner(pm) || objectStringField(pm, "uid") == "" || objectStringField(pm, "deletionTimestamp") != "" {
			return out, fmt.Errorf("release Pod ownership or lifecycle differs")
		}
		if objectStringMapValue(pm["annotations"])[runtime.FugueAnnotationReleaseKey] != binding.ReleaseKey || objectStringMapValue(pm["labels"])[runtime.FugueLabelAppWorkload] != binding.DeploymentName {
			return out, fmt.Errorf("drain Pod belongs to another executable revision")
		}
		p := releaseDrainPod{Name: objectStringField(pm, "name"), UID: objectStringField(pm, "uid")}
		appContainers := mapSlice(nestedObjectValue(pod, "spec", "containers"))
		if len(appContainers) == 0 || !s.migrationImageRefsEquivalent(app, objectStringField(appContainers[0], "image"), release.ResolvedImageRef) {
			return out, fmt.Errorf("release Pod application image differs")
		}
		for _, port := range mapSlice(appContainers[0]["ports"]) {
			n, ok := port["containerPort"].(float64)
			if !ok || n != float64(int(n)) || n < 1 || n > 65535 || (port["protocol"] != nil && port["protocol"] != "TCP") {
				return out, fmt.Errorf("release application port cannot be observed")
			}
			p.AppPorts = append(p.AppPorts, int(n))
		}
		sort.Ints(p.AppPorts)
		for _, ref := range mapSlice(pm["ownerReferences"]) {
			if rsUID := owned[objectStringField(ref, "name")]; appWorkloadControlledBy(pm, "ReplicaSet", objectStringField(ref, "name"), rsUID) {
				p.ReplicaUID = rsUID
			}
		}
		if p.ReplicaUID == "" {
			return out, fmt.Errorf("release Pod has no verified Deployment owner chain")
		}
		for _, field := range []string{"initContainers", "containers"} {
			for _, c := range mapSlice(nestedObjectValue(pod, "spec", field)) {
				if c["name"] != "fugue-drain-agent" {
					continue
				}
				for _, port := range mapSlice(c["ports"]) {
					if port["name"] == "drain-agent" && (port["protocol"] == nil || port["protocol"] == "TCP") {
						n, ok := port["containerPort"].(float64)
						if ok && n == float64(int(n)) {
							p.Port = int(n)
						}
					}
				}
				failClosed := false
				for _, env := range mapSlice(c["env"]) {
					if env["name"] == "FUGUE_DRAIN_FAIL_CLOSED" && env["value"] == "true" {
						failClosed = true
					}
					if env["name"] == "FUGUE_DRAIN_QUIET_PERIOD_SECONDS" {
						seconds, err := strconv.ParseInt(objectStringField(env, "value"), 10, 64)
						if err == nil && seconds > 0 && seconds <= 600 {
							p.QuietMS = seconds * 1000
						}
					}
				}
				if !failClosed {
					return out, fmt.Errorf("drain agent does not require positive observation")
				}
			}
		}
		for _, field := range []string{"initContainerStatuses", "containerStatuses"} {
			for _, c := range mapSlice(nestedObjectValue(pod, "status", field)) {
				if nestedObjectValue(c, "state", "running") != nil {
					id := objectStringField(c, "containerID")
					if id == "" || c["ready"] != true {
						return out, fmt.Errorf("release container is not ready with a stable identity")
					}
					p.Containers = append(p.Containers, fmt.Sprintf("%s:%s:%v", objectStringField(c, "name"), id, c["restartCount"]))
				}
				if c["name"] == "fugue-drain-agent" && c["ready"] == true && nestedObjectValue(c, "state", "running") != nil {
					p.AgentImage = objectStringField(c, "imageID")
					count, _ := c["restartCount"].(float64)
					p.Restarts = int(count)
				}
			}
		}
		if p.Port < 1 || p.Port > 65535 || p.AgentImage == "" || p.QuietMS == 0 || len(p.AppPorts) == 0 || len(p.Containers) < len(appContainers)+1 {
			return out, fmt.Errorf("release Pod has no ready drain agent identity")
		}
		sort.Strings(p.Containers)
		out.Pods = append(out.Pods, p)
	}
	if len(out.Pods) != *dep.Spec.Replicas {
		return out, fmt.Errorf("release Pod set does not match complete Deployment")
	}
	sort.Slice(out.Pods, func(i, j int) bool { return out.Pods[i].UID < out.Pods[j].UID })
	return out, nil
}

func (c *kubeClient) observeReleasePodDrain(ctx context.Context, namespace string, pod releaseDrainPod) (drainprotocol.Snapshot, error) {
	var result drainprotocol.Snapshot
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return result, err
	}
	nonce := hex.EncodeToString(random[:])
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods/" + url.PathEscape(pod.Name+":"+strconv.Itoa(pod.Port)) + "/proxy/drain/observe?nonce=" + nonce
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return result, err
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	client := *c.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(req)
	if err != nil {
		return result, fmt.Errorf("Pod drain proxy request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, fmt.Errorf("Pod drain proxy returned %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(body) > 4096 {
		return result, fmt.Errorf("oversized or unreadable Pod observation")
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("invalid Pod drain receipt")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF || result.APIVersion != drainprotocol.Version || result.Nonce != nonce || result.Pod != pod.Name || result.Namespace != namespace ||
		result.ActiveConnections == nil || *result.ActiveConnections < 0 || !reflect.DeepEqual(result.AppPorts, pod.AppPorts) {
		return result, fmt.Errorf("incomplete Pod drain receipt")
	}
	return result, nil
}
