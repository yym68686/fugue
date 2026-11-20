package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgimage"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

var errPostgresImageMigrationUnsettled = errors.New("database image migration awaits verified settlement")

// The witness contains identities and hashes, never database credentials or raw
// Kubernetes manifests. It survives a lost PATCH response or controller restart.
type postgresImageWitness struct {
	Namespace      string            `json:"namespace"`
	Cluster        string            `json:"cluster"`
	ClusterUID     string            `json:"cluster_uid"`
	SpecHash       string            `json:"spec_hash"`
	SystemID       string            `json:"system_id"`
	Pod            string            `json:"pod"`
	PodUID         string            `json:"pod_uid"`
	Node           string            `json:"node"`
	SourceImage    string            `json:"source_image"`
	TargetImage    string            `json:"target_image"`
	ResourcesHash  string            `json:"resources_hash"`
	Volumes        map[string]string `json:"volumes"`
	VerifiedPodUID string            `json:"verified_pod_uid,omitempty"`
	ColdMigration  string            `json:"cold_migration,omitempty"`
	SharedNodeUID  string            `json:"shared_node_uid,omitempty"`
	// Optional for compatibility with image-only witnesses already in flight.
	RestartResources      map[string]any             `json:"restart_resources,omitempty"`
	RestartSpecHash       string                     `json:"restart_spec_hash,omitempty"`
	RestartResourceTarget *model.ResourceSpec        `json:"restart_resource_target,omitempty"`
	SourceResourcesHash   string                     `json:"source_resources_hash,omitempty"`
	SourceResizeID        string                     `json:"source_resize_id,omitempty"`
	Drain                 *postgresImageDrainWitness `json:"drain,omitempty"`
	DrainRestored         bool                       `json:"drain_restored,omitempty"`
	// Nil means the intent predates podPatch compatibility checks.
	BootstrapPodPatch *string `json:"bootstrap_pod_patch,omitempty"`
}

// Verify the control file itself as well as CNPG's reported system ID. The
// SQL transaction uses a temporary table and is always rolled back.
func probePostgresImageMigration(ctx context.Context, c *kubeClient, ns string, postgres model.AppPostgresSpec, snapshot managedPostgresResizeSnapshot, pod map[string]any) error {
	pgdata := ""
	for _, env := range imageObjects(imageContainer(pod)["env"]) {
		if imageString(env, "name") == "PGDATA" {
			pgdata = imageString(env, "value")
		}
	}
	if !strings.HasPrefix(pgdata, "/") {
		return fmt.Errorf("database PGDATA is not an explicit absolute path")
	}
	var output bytes.Buffer
	call, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := coldExec(call, c, ns, snapshot.Observation.PodName, managedPostgresMainContainerName, []string{"env", "LC_ALL=C", "pg_controldata", pgdata}, &output); err != nil {
		return err
	}
	match := regexp.MustCompile(`(?m)^Database system identifier:\s*([0-9]+)\s*$`).FindStringSubmatch(output.String())
	if len(match) != 2 || match[1] != snapshot.Cluster.Status.SystemID {
		return fmt.Errorf("control-file database system ID differs from the migration identity")
	}
	return probeManagedPostgresResizeDatabase(ctx, c, ns, postgres, snapshot)
}

func (c *kubeClient) patchImageMigration(ctx context.Context, w postgresImageWitness, cluster map[string]any) error {
	original, staged, err := originalImageDrainSpec(cluster, w.Drain)
	if err != nil {
		return err
	}
	if (w.Drain != nil && !staged) || imageString(imageObject(cluster["metadata"]), "uid") != w.ClusterUID || imagePreservedSpecHash(original) != w.SpecHash || imageString(imageObject(cluster["spec"]), "imageName") != w.SourceImage {
		return fmt.Errorf("image migration compare-and-swap baseline changed")
	}
	patch := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": w.ClusterUID}, {"op": "test", "path": "/metadata/resourceVersion", "value": imageString(imageObject(cluster["metadata"]), "resourceVersion")}, {"op": "test", "path": "/spec", "value": cluster["spec"]}, {"op": "replace", "path": "/spec/imageName", "value": w.TargetImage}}
	if w.RestartResourceTarget != nil {
		if err := validateImageRestartResources(w, original, false); err != nil {
			return err
		}
		patch = append(patch, map[string]any{"op": "replace", "path": "/spec/resources", "value": w.RestartResources})
	}
	patchValue, err := imageMigrationPodPatch(cluster)
	if err != nil {
		return err
	}
	if err := validateImageMigrationPodPatch(w, patchValue, false); err != nil {
		return err
	}
	patch = append(patch, removeImageMigrationPodPatch(patchValue)...)
	_, err = c.doRequest(ctx, http.MethodPatch, cloudNativePGClusterAPIPath(w.Namespace, w.Cluster), "application/json-patch+json", patch, nil)
	return err
}

func imageObject(value any) map[string]any { m, _ := value.(map[string]any); return m }
func imageObjects(value any) []map[string]any {
	var out []map[string]any
	items, _ := value.([]any)
	for _, item := range items {
		if m := imageObject(item); m != nil {
			out = append(out, m)
		}
	}
	return out
}
func imageString(m map[string]any, key string) string { s, _ := m[key].(string); return s }
func imageContainer(pod map[string]any) map[string]any {
	for _, c := range imageObjects(imageObject(pod["spec"])["containers"]) {
		if imageString(c, "name") == managedPostgresMainContainerName {
			return c
		}
	}
	return nil
}
func imagePreservedSpecHash(cluster map[string]any) string {
	spec := map[string]any{}
	for k, v := range imageObject(cluster["spec"]) {
		if k != "imageName" {
			spec[k] = v
		}
	}
	return pgimage.Hash(spec)
}

// Compare every non-resize field too: an in-place CPU/memory target cannot
// authorize dropping an ephemeral-storage request, claim or future resource.
func imageNonRuntimeResources(value any) map[string]any {
	out := map[string]any{}
	for key, value := range imageObject(value) {
		if key == "requests" || key == "limits" {
			other := map[string]any{}
			for name, quantity := range imageObject(value) {
				if name != "cpu" && name != "memory" {
					other[name] = quantity
				}
			}
			if len(other) > 0 {
				out[key] = other
			}
		} else {
			out[key] = value
		}
	}
	return out
}

func imageRestartSpecHash(cluster map[string]any, resources map[string]any) string {
	spec := map[string]any{}
	for key, value := range imageObject(cluster["spec"]) {
		spec[key] = value
	}
	spec["resources"] = resources
	return imagePreservedSpecHash(map[string]any{"spec": spec})
}

func imageResourcesWithRuntimeTarget(source any, target kubeResourceRequirements) map[string]any {
	resources := imageNonRuntimeResources(source)
	for scope, quantities := range map[string]map[string]string{"requests": target.Requests, "limits": target.Limits} {
		values := imageObject(resources[scope])
		if values == nil {
			values = map[string]any{}
		}
		for name, value := range quantities {
			// Kubernetes serializes an applied quantity in canonical form.
			q, _ := parseResizeQuantity(value)
			values[name] = q.String()
		}
		resources[scope] = values
	}
	return resources
}

func validateImageRestartResources(w postgresImageWitness, cluster map[string]any, target bool) error {
	if (w.SourceResourcesHash == "") != (w.SourceResizeID == "") ||
		(w.SourceResourcesHash != "" && (w.RestartResourceTarget == nil || w.SourceResourcesHash == w.ResourcesHash)) {
		return fmt.Errorf("incomplete source resize witness")
	}
	if w.RestartResourceTarget == nil {
		if w.RestartResources != nil || w.RestartSpecHash != "" {
			return fmt.Errorf("incomplete restart resource witness")
		}
		return nil
	}
	authorized, err := managedPostgresKubeResources(*w.RestartResourceTarget)
	var resources kubeResourceRequirements
	raw, marshalErr := json.Marshal(w.RestartResources)
	if err != nil || marshalErr != nil || json.Unmarshal(raw, &resources) != nil || w.RestartSpecHash == "" ||
		pgimage.Hash(w.RestartResources) != w.ResourcesHash || !managedPostgresResizeResourcesEqual(resources, authorized) {
		return fmt.Errorf("invalid restart resource witness")
	}
	if imageRestartSpecHash(cluster, w.RestartResources) != w.RestartSpecHash ||
		pgimage.Hash(imageNonRuntimeResources(imageObject(cluster["spec"])["resources"])) != pgimage.Hash(imageNonRuntimeResources(w.RestartResources)) ||
		(target && pgimage.Hash(imageObject(cluster["spec"])["resources"]) != w.ResourcesHash) {
		return fmt.Errorf("restart resource template differs from the accepted witness")
	}
	return nil
}

func (c *kubeClient) imageMigrationObject(ctx context.Context, path string) (map[string]any, error) {
	m, found, err := c.getRawObject(ctx, path)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("migration prerequisite missing: %s", path)
	}
	if imageString(imageObject(m["metadata"]), "deletionTimestamp") != "" {
		return nil, fmt.Errorf("migration prerequisite is terminating: %s", path)
	}
	return m, nil
}

func (c *kubeClient) imageMigrationVolumes(ctx context.Context, ns string, pod map[string]any, sharedNode *kubeNode) (map[string]string, error) {
	out := map[string]string{}
	for _, volume := range imageObjects(imageObject(pod["spec"])["volumes"]) {
		claim := imageString(imageObject(volume["persistentVolumeClaim"]), "claimName")
		if claim == "" {
			continue
		}
		identity, err := c.imageMigrationVolumeIdentity(ctx, ns, claim, sharedNode)
		if err != nil {
			return nil, err
		}
		out[imageString(volume, "name")] = identity
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("database has no bound persistent volume")
	}
	return out, nil
}

func (c *kubeClient) imageMigrationVolumeIdentity(ctx context.Context, ns, claim string, sharedNode *kubeNode) (string, error) {
	pvc, err := c.imageMigrationObject(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/persistentvolumeclaims/"+url.PathEscape(claim))
	if err != nil {
		return "", err
	}
	meta, spec := imageObject(pvc["metadata"]), imageObject(pvc["spec"])
	pvName := imageString(spec, "volumeName")
	if imageString(meta, "uid") == "" || pvName == "" || imageString(imageObject(pvc["status"]), "phase") != "Bound" {
		return "", fmt.Errorf("PVC %s is not persistently bound", claim)
	}
	pv, err := c.imageMigrationObject(ctx, "/api/v1/persistentvolumes/"+url.PathEscape(pvName))
	if err != nil {
		return "", err
	}
	pvSpec := imageObject(pv["spec"])
	ref := imageObject(pvSpec["claimRef"])
	if imageString(ref, "uid") != imageString(meta, "uid") || imageString(ref, "namespace") != ns || imageString(ref, "name") != claim || imageString(imageObject(pv["metadata"]), "uid") == "" {
		return "", fmt.Errorf("PV %s does not match its PVC identity", pvName)
	}
	if sharedNode != nil {
		if err := validateImageMigrationVolumeNode(pvSpec, pvName, *sharedNode); err != nil {
			return "", err
		}
	}
	return pgimage.Hash([]any{claim, imageString(meta, "uid"), spec, pvName, imageString(imageObject(pv["metadata"]), "uid"), pvSpec}), nil
}

// Require the exact rehearsed operator; an unrelated operator or a new mutable
// tag cannot silently inherit this migration's compatibility guarantees.
func (c *kubeClient) verifyImageMigrationOperator(ctx context.Context, catalog pgimage.Catalog) error {
	var list map[string]any
	if _, err := c.doJSON(ctx, http.MethodGet, "/apis/apps/v1/namespaces/"+url.PathEscape(c.namespace)+"/deployments", nil, &list); err != nil {
		return err
	}
	for _, d := range imageObjects(list["items"]) {
		status, meta := imageObject(d["status"]), imageObject(d["metadata"])
		ready, _ := status["readyReplicas"].(float64)
		replicas, _ := imageObject(d["spec"])["replicas"].(float64)
		if replicas < 1 || ready != replicas || status["updatedReplicas"] != replicas || status["observedGeneration"] != meta["generation"] || imageString(meta, "deletionTimestamp") != "" {
			continue
		}
		for _, container := range imageObjects(imageObject(imageObject(imageObject(d["spec"])["template"])["spec"])["containers"]) {
			if imageString(container, "image") == catalog.OperatorImage {
				return nil
			}
		}
	}
	return fmt.Errorf("the verified baked-manager operator is not Ready")
}

func (c *kubeClient) observeImageMigration(ctx context.Context, ns, serviceID string, postgres model.AppPostgresSpec) (map[string]any, map[string]any, managedPostgresResizeSnapshot, error) {
	name := postgres.ServiceName
	cluster, err := c.imageMigrationObject(ctx, cloudNativePGClusterAPIPath(ns, name))
	if err != nil {
		return nil, nil, managedPostgresResizeSnapshot{}, err
	}
	var typed kubeCloudNativePGCluster
	b, _ := json.Marshal(cluster)
	if err = json.Unmarshal(b, &typed); err != nil {
		return nil, nil, managedPostgresResizeSnapshot{}, err
	}
	annotations := typed.Metadata.Annotations
	if annotations[runtime.CloudNativePGHibernationAnno] == runtime.CloudNativePGHibernationOn || annotations[coldReconciliationAnnotation] == coldReconciliationDisabled || (annotations["cnpg.io/fencedInstances"] != "" && annotations["cnpg.io/fencedInstances"] != "[]") {
		return nil, nil, managedPostgresResizeSnapshot{}, fmt.Errorf("database is fenced, hibernating or has reconciliation held")
	}
	pod, err := c.imageMigrationObject(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/pods/"+url.PathEscape(typed.Status.CurrentPrimary))
	if err != nil {
		return nil, nil, managedPostgresResizeSnapshot{}, err
	}
	var p kubeResizePod
	b, _ = json.Marshal(pod)
	if err = json.Unmarshal(b, &p); err != nil {
		return nil, nil, managedPostgresResizeSnapshot{}, err
	}
	observation, err := observeManagedPostgresResize(p, managedPostgresMainContainerName)
	if err == nil {
		err = validateManagedPostgresResizeLiveIdentity(ns, name, typed, observation, false)
	}
	if err == nil && (typed.Spec.Instances != 1 || typed.Status.SystemID == "" || len(typed.Status.DanglingPVC) > 0) {
		err = fmt.Errorf("migration requires one healthy instance, a system ID and no dangling PVC")
	}
	if err == nil && annotations[coldMigrationAnnotation] != "" {
		err = c.verifyImageMigrationColdTarget(ctx, ns, serviceID, postgres, typed, b)
	}
	if err != nil {
		return nil, nil, managedPostgresResizeSnapshot{}, err
	}
	return cluster, pod, managedPostgresResizeSnapshot{Cluster: typed, Observation: observation}, nil
}

func (s *Service) captureImageWitness(ctx context.Context, c *kubeClient, app model.App, ns, serviceID, target string, postgres model.AppPostgresSpec, catalog pgimage.Catalog) (postgresImageWitness, error) {
	name := postgres.ServiceName
	cluster, pod, snapshot, err := c.observeImageMigration(ctx, ns, serviceID, postgres)
	if err != nil {
		return postgresImageWitness{}, err
	}
	spec, meta := imageObject(cluster["spec"]), imageObject(cluster["metadata"])
	patchValue, err := imageMigrationPodPatch(cluster)
	if err != nil {
		return postgresImageWitness{}, err
	}
	main := imageContainer(pod)
	source := imageString(spec, "imageName")
	if imageString(spec, "primaryUpdateStrategy") != "unsupervised" || imageString(spec, "primaryUpdateMethod") != "restart" {
		return postgresImageWitness{}, fmt.Errorf("migration requires existing primaryUpdateStrategy=unsupervised and primaryUpdateMethod=restart; policy is never changed automatically")
	}
	if source != catalog.PostgresBase && source != strings.Split(catalog.PostgresBase, "@")[0] {
		return postgresImageWitness{}, fmt.Errorf("source image is outside the verified same-version upgrade path")
	}
	verifiedDigest := strings.Split(catalog.PostgresBase, "@")[1]
	resolved := false
	for _, status := range imageObjects(imageObject(pod["status"])["containerStatuses"]) {
		if imageString(status, "name") == managedPostgresMainContainerName && strings.HasSuffix(imageString(status, "imageID"), verifiedDigest) {
			resolved = true
		}
	}
	if !resolved || imageString(main, "image") != source {
		return postgresImageWitness{}, fmt.Errorf("running database image digest does not match the verified source")
	}
	authorized, err := managedPostgresKubeResources(model.ManagedPostgresRuntimeResources(postgres))
	if err != nil {
		return postgresImageWitness{}, fmt.Errorf("invalid persisted resource intent: %w", err)
	}
	if snapshot.Observation.ActualResources == nil || !managedPostgresResizeResourcesEqual(snapshot.Observation.DesiredResources, *snapshot.Observation.ActualResources) {
		return postgresImageWitness{}, fmt.Errorf("database resource resize is not fully applied")
	}
	restartResources := imageObject(main["resources"])
	var sourceResizeID string
	if !managedPostgresResizeResourcesEqual(snapshot.Observation.DesiredResources, authorized) {
		bootstrap, _, err := postgresRuntimeRestoreBaseline(serviceID, postgres, snapshot)
		if err != nil {
			return postgresImageWitness{}, fmt.Errorf("live resources differ from the persisted resource intent: %w", err)
		}
		if !postgresRuntimeRestoreAtBootstrap(snapshot, bootstrap) {
			return postgresImageWitness{}, fmt.Errorf("live resources differ from both the persisted resource intent and the CNPG bootstrap template")
		}
		// The same-node image switch does not reserve any additional capacity.
		// Never require greater requests or impose a smaller memory limit at
		// this cutover based only on an earlier process's resource evidence.
		_, raised, reduced := compareResizeResources(bootstrap, authorized)
		if containsResizeResource(raised, "requests.cpu") || containsResizeResource(raised, "requests.memory") ||
			containsResizeResource(reduced, "limits.memory") {
			return postgresImageWitness{}, fmt.Errorf("restoring the persisted resources would increase requests or lower the memory limit; align resources before restarting")
		}
		prior, err := s.postgresRuntimeRestoreProof(app, serviceID, postgres, snapshot)
		if err != nil {
			return postgresImageWitness{}, fmt.Errorf("live resources reverted to bootstrap; no verified resize to restore: %w", err)
		}
		sourceResizeID = prior.ID
		restartResources = imageResourcesWithRuntimeTarget(main["resources"], authorized)
	}
	alignResources := pgimage.Hash(restartResources) != pgimage.Hash(spec["resources"])
	if alignResources && (postgres.RuntimeResources == nil || pgimage.Hash(imageNonRuntimeResources(main["resources"])) != pgimage.Hash(imageNonRuntimeResources(spec["resources"]))) {
		return postgresImageWitness{}, fmt.Errorf("live resources differ from the restart template without a matching persisted in-place resource target")
	}
	if len(imageObjects(imageObject(pod["spec"])["containers"])) != 1 {
		return postgresImageWitness{}, fmt.Errorf("migration does not support database sidecar containers")
	}
	for _, status := range imageObjects(imageObject(pod["status"])["initContainerStatuses"]) {
		term := imageObject(imageObject(status["state"])["terminated"])
		if term == nil || term["exitCode"] != float64(0) {
			return postgresImageWitness{}, fmt.Errorf("initialization is not successfully complete")
		}
	}
	if patchValue != "" {
		init := imageObjects(imageObject(pod["spec"])["initContainers"])
		if len(init) != 1 || imageString(init[0], "name") != "bootstrap-controller" {
			return postgresImageWitness{}, fmt.Errorf("source Pod does not match the legacy bootstrap CPU patch")
		}
	}
	placement, err := s.Store.GetRuntime(postgres.RuntimeID)
	if err != nil {
		return postgresImageWitness{}, fmt.Errorf("read configured database runtime for image migration: %w", err)
	}
	nodeName := snapshot.Observation.NodeName
	pods, err := c.listPodsBySelector(ctx, ns, cloudNativePGClusterLabel+"="+name+",cnpg.io/podRole=instance")
	if err != nil {
		return postgresImageWitness{}, err
	}
	if len(pods) != 1 || pods[0].Metadata.Name != snapshot.Observation.PodName || pods[0].ObservedUID != snapshot.Observation.PodUID {
		return postgresImageWitness{}, fmt.Errorf("migration requires exactly the observed primary Pod and no other database instance")
	}
	var sharedNode *kubeNode
	if placement.Type == model.RuntimeTypeManagedShared && !imageMigrationRuntimePinned(spec, postgres.RuntimeID) {
		node, err := c.imageMigrationSharedNode(ctx, placement, postgres, spec, nodeName, "")
		if err != nil {
			return postgresImageWitness{}, err
		}
		if err := validateImageMigrationNode(node, true); err != nil {
			return postgresImageWitness{}, err
		}
		sharedNode = &node
	} else {
		// Preserve the single-Node, hard runtime-id proof for existing placements
		// that use a physical runtime label, including legacy Clusters.
		nodes, err := c.listNodeRuntimeIDs(ctx)
		if err != nil {
			return postgresImageWitness{}, err
		}
		count := 0
		for node, id := range nodes {
			if id == postgres.RuntimeID {
				count++
				if node != nodeName {
					return postgresImageWitness{}, fmt.Errorf("runtime placement is ambiguous")
				}
			}
		}
		if count != 1 || !imageMigrationRuntimePinned(spec, postgres.RuntimeID) {
			return postgresImageWitness{}, fmt.Errorf("migration requires an unambiguous existing runtime placement")
		}
		node, found, err := c.getNode(ctx, nodeName)
		if err != nil {
			return postgresImageWitness{}, err
		}
		if err := validateImageMigrationNode(node, found); err != nil {
			return postgresImageWitness{}, err
		}
	}
	volumes, err := c.imageMigrationVolumes(ctx, ns, pod, sharedNode)
	if err != nil {
		return postgresImageWitness{}, err
	}
	witness := postgresImageWitness{Namespace: ns, Cluster: name, ClusterUID: imageString(meta, "uid"), SpecHash: imagePreservedSpecHash(cluster), SystemID: snapshot.Cluster.Status.SystemID, Pod: snapshot.Observation.PodName, PodUID: snapshot.Observation.PodUID, Node: snapshot.Observation.NodeName, SourceImage: source, TargetImage: target, ResourcesHash: pgimage.Hash(main["resources"]), Volumes: volumes}
	if sharedNode != nil {
		witness.SharedNodeUID = sharedNode.Metadata.UID
	}
	witness.BootstrapPodPatch = &patchValue
	witness.ColdMigration = snapshot.Cluster.Metadata.Annotations[coldMigrationAnnotation]
	if alignResources {
		witness.RestartResources = restartResources
		witness.RestartResourceTarget = model.CloneResourceSpec(postgres.RuntimeResources)
		witness.RestartSpecHash = imageRestartSpecHash(cluster, witness.RestartResources)
	}
	if sourceResizeID != "" {
		witness.SourceResizeID = sourceResizeID
		witness.SourceResourcesHash = witness.ResourcesHash
		witness.ResourcesHash = pgimage.Hash(restartResources)
	}
	return witness, nil
}

func imageMigrationRuntimePinned(spec map[string]any, id string) bool {
	affinity := imageObject(spec["affinity"])
	if imageString(imageObject(affinity["nodeSelector"]), runtime.RuntimeIDLabelKey) == id {
		return true
	}
	required := imageObject(imageObject(affinity["nodeAffinity"])["requiredDuringSchedulingIgnoredDuringExecution"])
	terms := imageObjects(required["nodeSelectorTerms"])
	if len(terms) == 0 {
		return false
	}
	for _, term := range terms {
		pinned := false
		for _, expr := range imageObjects(term["matchExpressions"]) {
			values, _ := expr["values"].([]any)
			if imageString(expr, "key") == runtime.RuntimeIDLabelKey && imageString(expr, "operator") == "In" && len(values) == 1 && values[0] == id {
				pinned = true
			}
		}
		if !pinned {
			return false
		}
	}
	return true
}

func validateImageWitness(w postgresImageWitness, cluster, pod map[string]any, snapshot managedPostgresResizeSnapshot) error {
	if snapshot.Cluster.Metadata.Annotations[coldMigrationAnnotation] != w.ColdMigration {
		return fmt.Errorf("database cold migration marker changed since preflight")
	}
	image := imageString(imageObject(cluster["spec"]), "imageName")
	expectedSpecHash := w.SpecHash
	if image == w.TargetImage && w.RestartResourceTarget != nil {
		expectedSpecHash = w.RestartSpecHash
	}
	original, _, err := originalImageDrainSpec(cluster, w.Drain)
	if err != nil {
		return err
	}
	if err := validateImageRestartResources(w, original, image == w.TargetImage); err != nil {
		return err
	}
	if imageString(imageObject(cluster["metadata"]), "uid") != w.ClusterUID || imagePreservedSpecHash(original) != expectedSpecHash || snapshot.Cluster.Status.SystemID != w.SystemID {
		return fmt.Errorf("database Cluster identity or preserved specification changed")
	}
	expectedResourcesHash := w.ResourcesHash
	if w.SourceResourcesHash != "" && snapshot.Observation.PodUID == w.PodUID {
		expectedResourcesHash = w.SourceResourcesHash
	}
	if snapshot.Observation.PodName != w.Pod || snapshot.Observation.NodeName != w.Node || pgimage.Hash(imageContainer(pod)["resources"]) != expectedResourcesHash {
		return fmt.Errorf("database primary, node or resources changed")
	}
	if snapshot.Observation.ActualResources == nil || !managedPostgresResizeResourcesEqual(snapshot.Observation.DesiredResources, *snapshot.Observation.ActualResources) {
		return fmt.Errorf("database actual resources have not converged")
	}
	if image != w.SourceImage && image != w.TargetImage {
		return fmt.Errorf("database image changed outside this operation")
	}
	patchValue, err := imageMigrationPodPatch(cluster)
	if err != nil {
		return err
	}
	return validateImageMigrationPodPatch(w, patchValue, image == w.TargetImage)
}

func (s *Service) imageMigrationEvidence(op model.Operation, kind string, w postgresImageWitness) error {
	b, err := json.Marshal(w)
	if err != nil {
		return err
	}
	var payload map[string]any
	if err = json.Unmarshal(b, &payload); err != nil {
		return err
	}
	_, err = s.Store.RecordOperationEvidence(model.OperationEvidence{TenantID: op.TenantID, AppID: op.AppID, OperationID: op.ID, Type: kind, SubjectKind: "Cluster", SubjectName: w.Cluster, SubjectNamespace: w.Namespace, SubjectUID: w.ClusterUID, Summary: kind, PayloadVersion: 1, Payload: payload})
	return err
}

func (s *Service) executePostgresImageMigration(ctx context.Context, op model.Operation, app model.App) (executionErr error) {
	// Unknown evidence state is unsafe to classify as an ordinary failed job.
	evidence, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{pgimage.IntentEvidence, pgimage.DrainAbortEvidence}, IncludeMigrationLedger: true, Limit: 3})
	if err != nil {
		return fmt.Errorf("%w: read durable intent: %v", errPostgresImageMigrationUnsettled, err)
	}
	var recordedIntent *model.OperationEvidence
	abortDrain := false
	for i := range evidence {
		switch evidence[i].Type {
		case pgimage.IntentEvidence:
			recordedIntent = &evidence[i]
		case pgimage.DrainAbortEvidence:
			abortDrain = true
		}
	}
	intent := recordedIntent != nil
	resuming := intent
	defer func() {
		if executionErr != nil && intent && !errors.Is(executionErr, context.Canceled) && !errors.Is(executionErr, errOperationNoLongerActive) {
			_, _ = s.Store.UpdateOperationProgress(op.ID, "image migration waiting for verification: "+executionErr.Error())
			executionErr = fmt.Errorf("%w: %v", errPostgresImageMigrationUnsettled, executionErr)
		}
	}()
	if !s.Config.KubectlApply {
		return fmt.Errorf("database image migration requires Kubernetes apply mode")
	}
	target, err := store.PostgresImageMigrationTargetForApp(app, op.ServiceID)
	if err != nil {
		return err
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return fmt.Errorf("migration target specification is missing")
	}
	image, err := pgimage.Target(op.DesiredSpec.Postgres.Image)
	if err != nil {
		return err
	}
	budget := op.DesiredSpec.Postgres.ImageMigrationDrainBudgetSeconds
	if !pgimage.ValidDrainBudget(budget) || store.PostgresImageMigrationPlanHash(app, *target.Service, image, budget) != op.DesiredSpec.Postgres.ImageMigrationConfigHash {
		return fmt.Errorf("configuration changed since migration admission")
	}
	catalog, err := pgimage.ApprovedCatalog()
	if err != nil {
		return err
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	if err = client.verifyImageMigrationOperator(ctx, catalog); err != nil {
		return err
	}
	probe := s.postgresImageMigrationProbe
	if probe == nil {
		probe = probePostgresImageMigration
	}
	ns, name := runtime.NamespaceForTenant(app.TenantID), target.Postgres.ServiceName
	var witness postgresImageWitness
	// Once armed, a timeout is measured from the first durable intent, not
	// from each retry or a new controller leader.
	armDeadline := time.Now().Add(30 * time.Second)
	if intent {
		b, _ := json.Marshal(recordedIntent.Payload)
		if err = json.Unmarshal(b, &witness); err != nil {
			return err
		}
		if witness.Namespace != ns || witness.Cluster != name || witness.TargetImage != image || witness.ClusterUID == "" || witness.SystemID == "" || witness.PodUID == "" || witness.SpecHash == "" || len(witness.Volumes) == 0 ||
			(budget == 0 && witness.Drain != nil) || (budget != 0 && (witness.Drain == nil || witness.Drain.BudgetSeconds != budget || witness.Drain.OriginalSeconds <= budget || witness.Drain.Marker != pgimage.Hash([]string{op.ID, witness.ClusterUID, witness.PodUID}))) || witness.DrainRestored {
			return fmt.Errorf("durable migration witness is incomplete or mismatched")
		}
		armDeadline = recordedIntent.CollectedAt.Add(30 * time.Second)
	} else {
		witness, err = s.captureImageWitness(ctx, client, app, ns, op.ServiceID, image, target.Postgres, catalog)
		if err != nil {
			return err
		}
		cluster, pod, snapshot, err := client.observeImageMigration(ctx, ns, op.ServiceID, target.Postgres)
		if err != nil {
			return err
		}
		if err = validateImageWitness(witness, cluster, pod, snapshot); err != nil {
			return err
		}
		if snapshot.Observation.PodUID != witness.PodUID {
			return fmt.Errorf("primary changed during migration preflight")
		}
		if err = probe(ctx, client, ns, target.Postgres, snapshot, pod); err != nil {
			return err
		}
		if budget != 0 {
			if err := configurePostgresImageDrain(&witness, cluster, op.ID, budget); err != nil {
				return err
			}
			sample := s.imageDrainSample(ctx, client, witness)
			s.recordImageDrainSample(op, sample)
			if sample.Status != "eligible" || sample.LoadedMarker != "" {
				reason := sample.Reason
				if reason == "" {
					reason = "an unexpected drain marker is loaded"
				}
				return fmt.Errorf("fast drain preflight blocked: %s", reason)
			}
		}
		if err = s.imageMigrationEvidence(op, pgimage.BaselineEvidence, witness); err != nil {
			return err
		}
		if err = s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
			return err
		}
		// From here on even an uncertain write must retain the mutation interlock.
		if err = s.imageMigrationEvidence(op, pgimage.IntentEvidence, witness); err != nil {
			return err
		}
		intent = true
		armDeadline = time.Now().Add(30 * time.Second)
	}
	if witness.RestartResourceTarget != nil && (target.Postgres.RuntimeResources == nil || *witness.RestartResourceTarget != *target.Postgres.RuntimeResources) {
		return fmt.Errorf("accepted restart resources differ from the persisted in-place resource target")
	}
	_, _ = s.Store.UpdateOperationProgress(op.ID, "verified database image migration: awaiting CNPG replacement and SQL verification")
	deadline := time.Now().Add(15 * time.Minute)
	waitingReason := "awaiting a new healthy primary Pod using the target image"
	reportedReason := ""
	var timing *postgresImageEndpointSampler
	if witness.Drain != nil {
		timing = s.observeImageEndpoint(ctx, op, client, witness, target.Postgres, resuming)
		defer timing.stopAndRecord()
	}
	for {
		if err = s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
			return err
		}
		if resuming && witness.BootstrapPodPatch == nil && witness.Drain == nil {
			repaired, err := client.recoverLegacyImageMigrationPodPatch(ctx, witness, target.Postgres.RuntimeID)
			if err != nil {
				return err
			}
			if repaired {
				if s.Logger != nil {
					s.Logger.Printf("database image migration %s removed a stale bootstrap-only CNPG podPatch during recovery", op.ID)
				}
				continue
			}
		}
		if witness.Drain != nil {
			current, readErr := client.imageMigrationObject(ctx, cloudNativePGClusterAPIPath(ns, name))
			if readErr != nil {
				return readErr
			}
			staged, verifyErr := verifyImageDrainCluster(witness, current)
			if verifyErr != nil {
				return verifyErr
			}
			oldImage := imageString(imageObject(current["spec"]), "imageName") == witness.SourceImage
			if oldImage && (abortDrain || time.Now().After(armDeadline)) {
				if err := s.abortImageDrain(ctx, client, op, witness, current, target.Postgres, abortDrain, "old instance did not reach the approved cutover"); err != nil {
					return err
				}
				intent = false
				return fmt.Errorf("fast drain aborted before image replacement; original shutdown settings restored")
			}
			if !oldImage && staged {
				// Restore only once the original writer has exited and CNPG has
				// created the exact replacement. No need to wait for SQL readiness.
				if replacement, err := client.imageMigrationObject(ctx, imageDrainPodPath(witness)); err == nil && imageDrainReplacement(replacement, witness) {
					if err := s.restoreImageDrain(ctx, client, op, witness, current); err != nil {
						return err
					}
					continue
				}
			}
		}
		cluster, pod, snapshot, observeErr := client.observeImageMigration(ctx, ns, op.ServiceID, target.Postgres)
		if observeErr != nil {
			waitingReason = observeErr.Error()
		}
		if observeErr == nil {
			waitingReason = "awaiting a new healthy primary Pod using the target image"
			if err = validateImageWitness(witness, cluster, pod, snapshot); err != nil {
				return err
			}
			spec := imageObject(cluster["spec"])
			currentImage := imageString(spec, "imageName")
			var sharedNode *kubeNode
			if witness.SharedNodeUID != "" {
				placement, err := s.Store.GetRuntime(target.Postgres.RuntimeID)
				if err != nil {
					return fmt.Errorf("recheck configured shared database runtime: %w", err)
				}
				node, err := client.imageMigrationSharedNode(ctx, placement, target.Postgres, spec, witness.Node, witness.SharedNodeUID)
				if err != nil {
					return err
				}
				if currentImage == witness.SourceImage {
					if err := validateImageMigrationNode(node, true); err != nil {
						return err
					}
				}
				sharedNode = &node
			}
			volumes, err := client.imageMigrationVolumes(ctx, ns, pod, sharedNode)
			if err != nil {
				return err
			}
			if pgimage.Hash(volumes) != pgimage.Hash(witness.Volumes) {
				return fmt.Errorf("database PVC/PV identity or specification changed")
			}
			if currentImage == witness.SourceImage {
				if snapshot.Observation.PodUID != witness.PodUID {
					return fmt.Errorf("source primary was replaced outside the migration")
				}
				if witness.Drain != nil {
					staged, err := verifyImageDrainCluster(witness, cluster)
					if err != nil {
						return err
					}
					sample := s.imageDrainSample(ctx, client, witness)
					if sample.Status != "eligible" {
						s.recordImageDrainSample(op, sample)
						if err := s.abortImageDrain(ctx, client, op, witness, cluster, target.Postgres, abortDrain, sample.Reason); err != nil {
							return err
						}
						intent = false
						return fmt.Errorf("fast drain aborted: %s", sample.Reason)
					}
					if !staged {
						if sample.LoadedMarker != "" {
							return fmt.Errorf("old instance reports a foreign drain marker")
						}
						if err := s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
							return err
						}
						if err := client.patchImageDrain(ctx, witness, cluster, true); err != nil {
							return err
						}
						continue
					}
					if sample.LoadedMarker != witness.Drain.Marker || !sample.MarkerFromFile {
						reason := "awaiting old instance manager acknowledgement of temporary shutdown settings"
						if reportedReason != reason {
							_, _ = s.Store.UpdateOperationProgress(op.ID, "database image migration waiting: "+reason)
							reportedReason = reason
						}
						if err := waitManagedPostgresResizePollInterval(ctx, 500*time.Millisecond); err != nil {
							return err
						}
						continue
					}
					// UID and resourceVersion pin the old Pod and Cluster
					// observations across the acknowledged reload.
					currentPod, err := client.imageMigrationObject(ctx, imageDrainPodPath(witness))
					if err != nil || imageString(imageObject(currentPod["metadata"]), "uid") != witness.PodUID {
						return fmt.Errorf("primary changed while verifying temporary shutdown settings")
					}
					s.recordImageDrainSample(op, sample)
				}
				if err = s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
					return err
				}
				if timing != nil {
					timing.markCutover()
				}
				if err = client.patchImageMigration(ctx, witness, cluster); err != nil {
					return fmt.Errorf("image compare-and-swap outcome requires verification: %w", err)
				}
			} else if snapshot.Observation.PodUID != witness.PodUID && imageString(imageContainer(pod), "image") == image {
				if witness.Drain != nil {
					staged, err := verifyImageDrainCluster(witness, cluster)
					if err != nil {
						return err
					}
					if staged {
						if err := s.restoreImageDrain(ctx, client, op, witness, cluster); err != nil {
							return err
						}
						continue
					}
					if sample := s.imageDrainSample(ctx, client, witness); sample.SampledAt == nil || sample.LoadedMarker != "" {
						return fmt.Errorf("waiting for replacement instance to acknowledge restored shutdown settings")
					}
				}
				resolved := false
				for _, status := range imageObjects(imageObject(pod["status"])["containerStatuses"]) {
					if imageString(status, "name") == managedPostgresMainContainerName && strings.HasSuffix(imageString(status, "imageID"), strings.Split(image, "@")[1]) {
						resolved = true
					}
				}
				if !resolved {
					return fmt.Errorf("replacement database image digest does not match the verified target")
				}
				if len(imageObjects(imageObject(pod["spec"])["initContainers"])) != 0 {
					return fmt.Errorf("baked-manager primary unexpectedly has init containers")
				}
				command, _ := imageContainer(pod)["command"].([]any)
				if len(command) == 0 || command[0] != "/usr/local/bin/fugue-cnpg-manager" {
					return fmt.Errorf("primary does not execute the baked manager")
				}
				if err = probe(ctx, client, ns, target.Postgres, snapshot, pod); err != nil {
					return err
				}
				verified := witness
				verified.VerifiedPodUID = snapshot.Observation.PodUID
				verified.DrainRestored = witness.Drain != nil
				if timing != nil {
					timing.stopAndRecord()
				}
				if err = s.imageMigrationEvidence(op, pgimage.VerifiedEvidence, verified); err != nil {
					return err
				}
				if err = s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
					return err
				}
				_, err = s.Store.CompleteManagedOperationGuarded(op, nil, "", "database image migrated; original storage, placement and resource envelope verified; bootstrap init removed", nil, nil, nil)
				return err
			}
		}
		if waitingReason != reportedReason {
			_, _ = s.Store.UpdateOperationProgress(op.ID, "database image migration waiting: "+waitingReason)
			reportedReason = waitingReason
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("CNPG image migration verification timed out: %s; no rollback, deletion or storage mutation performed", waitingReason)
		}
		if err = waitManagedPostgresResizePollInterval(ctx, 2*time.Second); err != nil {
			return err
		}
	}
}
