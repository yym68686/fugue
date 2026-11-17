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
	if imageString(imageObject(cluster["metadata"]), "uid") != w.ClusterUID || imagePreservedSpecHash(cluster) != w.SpecHash || imageString(imageObject(cluster["spec"]), "imageName") != w.SourceImage {
		return fmt.Errorf("image migration compare-and-swap baseline changed")
	}
	patch := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": w.ClusterUID}, {"op": "test", "path": "/metadata/resourceVersion", "value": imageString(imageObject(cluster["metadata"]), "resourceVersion")}, {"op": "test", "path": "/spec", "value": cluster["spec"]}, {"op": "replace", "path": "/spec/imageName", "value": w.TargetImage}}
	_, err := c.doRequest(ctx, http.MethodPatch, cloudNativePGClusterAPIPath(w.Namespace, w.Cluster), "application/json-patch+json", patch, nil)
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

func (c *kubeClient) imageMigrationVolumes(ctx context.Context, ns string, pod map[string]any) (map[string]string, error) {
	out := map[string]string{}
	for _, volume := range imageObjects(imageObject(pod["spec"])["volumes"]) {
		claim := imageString(imageObject(volume["persistentVolumeClaim"]), "claimName")
		if claim == "" {
			continue
		}
		pvc, err := c.imageMigrationObject(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/persistentvolumeclaims/"+url.PathEscape(claim))
		if err != nil {
			return nil, err
		}
		meta, spec := imageObject(pvc["metadata"]), imageObject(pvc["spec"])
		pvName := imageString(spec, "volumeName")
		if imageString(meta, "uid") == "" || pvName == "" || imageString(imageObject(pvc["status"]), "phase") != "Bound" {
			return nil, fmt.Errorf("PVC %s is not persistently bound", claim)
		}
		pv, err := c.imageMigrationObject(ctx, "/api/v1/persistentvolumes/"+url.PathEscape(pvName))
		if err != nil {
			return nil, err
		}
		pvSpec := imageObject(pv["spec"])
		ref := imageObject(pvSpec["claimRef"])
		if imageString(ref, "uid") != imageString(meta, "uid") || imageString(ref, "namespace") != ns || imageString(ref, "name") != claim || imageString(imageObject(pv["metadata"]), "uid") == "" {
			return nil, fmt.Errorf("PV %s does not match its PVC identity", pvName)
		}
		out[imageString(volume, "name")] = pgimage.Hash([]any{claim, imageString(meta, "uid"), spec, pvName, imageString(imageObject(pv["metadata"]), "uid"), pvSpec})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("database has no bound persistent volume")
	}
	return out, nil
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

func (c *kubeClient) observeImageMigration(ctx context.Context, ns, name string) (map[string]any, map[string]any, managedPostgresResizeSnapshot, error) {
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
	if annotations[runtime.CloudNativePGHibernationAnno] == runtime.CloudNativePGHibernationOn || annotations[coldReconciliationAnnotation] == coldReconciliationDisabled || annotations[coldMigrationAnnotation] != "" || (annotations["cnpg.io/fencedInstances"] != "" && annotations["cnpg.io/fencedInstances"] != "[]") {
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
	if err != nil {
		return nil, nil, managedPostgresResizeSnapshot{}, err
	}
	return cluster, pod, managedPostgresResizeSnapshot{Cluster: typed, Observation: observation}, nil
}

func (c *kubeClient) captureImageWitness(ctx context.Context, ns, name, target, runtimeID string, catalog pgimage.Catalog) (postgresImageWitness, error) {
	cluster, pod, snapshot, err := c.observeImageMigration(ctx, ns, name)
	if err != nil {
		return postgresImageWitness{}, err
	}
	spec, meta := imageObject(cluster["spec"]), imageObject(cluster["metadata"])
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
	if pgimage.Hash(main["resources"]) != pgimage.Hash(spec["resources"]) {
		return postgresImageWitness{}, fmt.Errorf("live resources differ from the restart template; reconcile resource intent before migration")
	}
	if !managedPostgresResizeResourcesEqual(snapshot.Observation.DesiredResources, *snapshot.Observation.ActualResources) {
		return postgresImageWitness{}, fmt.Errorf("database resource resize is not fully applied")
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
	// A single runtime node and the existing mandatory runtime affinity prevent
	// a restart from becoming an unrequested placement migration.
	nodes, err := c.listNodeRuntimeIDs(ctx)
	if err != nil {
		return postgresImageWitness{}, err
	}
	count := 0
	for node, id := range nodes {
		if id == runtimeID {
			count++
			if node != snapshot.Observation.NodeName {
				return postgresImageWitness{}, fmt.Errorf("runtime placement is ambiguous")
			}
		}
	}
	if count != 1 || !imageMigrationRuntimePinned(spec, runtimeID) {
		return postgresImageWitness{}, fmt.Errorf("migration requires an unambiguous existing runtime placement")
	}
	pods, err := c.listPodsBySelector(ctx, ns, cloudNativePGClusterLabel+"="+name+",cnpg.io/podRole=instance")
	if err != nil {
		return postgresImageWitness{}, err
	}
	if len(pods) != 1 || pods[0].Metadata.Name != snapshot.Observation.PodName || pods[0].ObservedUID != snapshot.Observation.PodUID {
		return postgresImageWitness{}, fmt.Errorf("migration requires exactly the observed primary Pod and no other database instance")
	}
	node, found, err := c.getNode(ctx, snapshot.Observation.NodeName)
	if err != nil {
		return postgresImageWitness{}, err
	}
	if err = validateManagedPostgresAutoMemoryNode(node, found, false); err != nil {
		return postgresImageWitness{}, err
	}
	if node.Spec.Unschedulable {
		return postgresImageWitness{}, fmt.Errorf("database node is cordoned")
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect == "NoSchedule" || taint.Effect == "NoExecute" {
			return postgresImageWitness{}, fmt.Errorf("database node has a scheduling/execution taint; migration requires a stable node")
		}
	}
	volumes, err := c.imageMigrationVolumes(ctx, ns, pod)
	if err != nil {
		return postgresImageWitness{}, err
	}
	return postgresImageWitness{Namespace: ns, Cluster: name, ClusterUID: imageString(meta, "uid"), SpecHash: imagePreservedSpecHash(cluster), SystemID: snapshot.Cluster.Status.SystemID, Pod: snapshot.Observation.PodName, PodUID: snapshot.Observation.PodUID, Node: snapshot.Observation.NodeName, SourceImage: source, TargetImage: target, ResourcesHash: pgimage.Hash(main["resources"]), Volumes: volumes}, nil
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
	if imageString(imageObject(cluster["metadata"]), "uid") != w.ClusterUID || imagePreservedSpecHash(cluster) != w.SpecHash || snapshot.Cluster.Status.SystemID != w.SystemID {
		return fmt.Errorf("database Cluster identity or preserved specification changed")
	}
	if snapshot.Observation.PodName != w.Pod || snapshot.Observation.NodeName != w.Node || pgimage.Hash(imageContainer(pod)["resources"]) != w.ResourcesHash {
		return fmt.Errorf("database primary, node or resources changed")
	}
	if snapshot.Observation.ActualResources == nil || !managedPostgresResizeResourcesEqual(snapshot.Observation.DesiredResources, *snapshot.Observation.ActualResources) {
		return fmt.Errorf("database actual resources have not converged")
	}
	image := imageString(imageObject(cluster["spec"]), "imageName")
	if image != w.SourceImage && image != w.TargetImage {
		return fmt.Errorf("database image changed outside this operation")
	}
	return nil
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
	evidence, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{pgimage.IntentEvidence}, IncludeMigrationLedger: true, Limit: 1})
	if err != nil {
		return fmt.Errorf("%w: read durable intent: %v", errPostgresImageMigrationUnsettled, err)
	}
	intent := len(evidence) > 0
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
	if store.PostgresImageMigrationConfigHash(app, *target.Service, image) != op.DesiredSpec.Postgres.ImageMigrationConfigHash {
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
	if intent {
		b, _ := json.Marshal(evidence[0].Payload)
		if err = json.Unmarshal(b, &witness); err != nil {
			return err
		}
		if witness.Namespace != ns || witness.Cluster != name || witness.TargetImage != image || witness.ClusterUID == "" || witness.SystemID == "" || witness.PodUID == "" || witness.SpecHash == "" || len(witness.Volumes) == 0 {
			return fmt.Errorf("durable migration witness is incomplete or mismatched")
		}
	} else {
		witness, err = client.captureImageWitness(ctx, ns, name, image, target.Postgres.RuntimeID, catalog)
		if err != nil {
			return err
		}
		cluster, pod, snapshot, err := client.observeImageMigration(ctx, ns, name)
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
	}
	_, _ = s.Store.UpdateOperationProgress(op.ID, "verified database image migration: awaiting CNPG replacement and SQL verification")
	deadline := time.Now().Add(15 * time.Minute)
	waitingReason := "awaiting a new healthy primary Pod using the target image"
	reportedReason := ""
	for {
		if err = s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
			return err
		}
		cluster, pod, snapshot, observeErr := client.observeImageMigration(ctx, ns, name)
		if observeErr != nil {
			waitingReason = observeErr.Error()
		}
		if observeErr == nil {
			waitingReason = "awaiting a new healthy primary Pod using the target image"
			if err = validateImageWitness(witness, cluster, pod, snapshot); err != nil {
				return err
			}
			volumes, err := client.imageMigrationVolumes(ctx, ns, pod)
			if err != nil {
				return err
			}
			if pgimage.Hash(volumes) != pgimage.Hash(witness.Volumes) {
				return fmt.Errorf("database PVC/PV identity or specification changed")
			}
			currentImage := imageString(imageObject(cluster["spec"]), "imageName")
			if currentImage == witness.SourceImage {
				if snapshot.Observation.PodUID != witness.PodUID {
					return fmt.Errorf("source primary was replaced outside the migration")
				}
				if err = s.ensureManagedPostgresLifecycleOperationOwned(op); err != nil {
					return err
				}
				if err = client.patchImageMigration(ctx, witness, cluster); err != nil {
					return fmt.Errorf("image compare-and-swap outcome requires verification: %w", err)
				}
			} else if snapshot.Observation.PodUID != witness.PodUID && imageString(imageContainer(pod), "image") == image {
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
