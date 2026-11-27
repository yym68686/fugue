package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgimage"
	"fugue/internal/runtime"
)

type postgresImageWitness = pgimage.Witness

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
