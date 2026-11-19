package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"fugue/internal/runtime"
)

const (
	imageMigrationPodPatchAnnotation = "cnpg.io/podPatch"
	imageMigrationPodPatchPath       = "/metadata/annotations/cnpg.io~1podPatch"
	legacyBootstrapInitName          = "bootstrap-controller"
)

var legacyBootstrapCPUQuantity = regexp.MustCompile(`^[0-9]+m?$`)

// Only the two-operation bootstrap CPU contract from the platform's legacy
// maintenance tool is removable. Do not reinterpret arbitrary Pod patches.
func isLegacyBootstrapCPUPodPatch(value string) bool {
	if len(value) > 512 {
		return false
	}
	var patch []map[string]string
	if err := json.Unmarshal([]byte(value), &patch); err != nil || len(patch) != 2 || len(patch[0]) != 3 || len(patch[1]) != 3 {
		return false
	}
	var compact bytes.Buffer
	canonical, _ := json.Marshal(patch)
	if json.Compact(&compact, []byte(value)) != nil || !bytes.Equal(compact.Bytes(), canonical) {
		return false // Only the legacy tool's canonical object shape, no duplicate keys.
	}
	if patch[0]["op"] != "test" || patch[0]["path"] != "/spec/initContainers/0/name" || patch[0]["value"] != legacyBootstrapInitName ||
		patch[1]["op"] != "replace" || patch[1]["path"] != "/spec/initContainers/0/resources/requests/cpu" ||
		!legacyBootstrapCPUQuantity.MatchString(patch[1]["value"]) {
		return false
	}
	cpu, err := strconv.ParseUint(strings.TrimSuffix(patch[1]["value"], "m"), 10, 64)
	return err == nil && cpu > 0
}

func imageMigrationPodPatch(cluster map[string]any) (string, error) {
	value, present := imageObject(imageObject(cluster["metadata"])["annotations"])[imageMigrationPodPatchAnnotation]
	if !present {
		return "", nil
	}
	patch, ok := value.(string)
	if !ok || !isLegacyBootstrapCPUPodPatch(patch) {
		return "", fmt.Errorf("migration cannot preserve an unsupported cnpg.io/podPatch on the baked-manager Pod")
	}
	return patch, nil
}

func validateImageMigrationPodPatch(w postgresImageWitness, value string, target bool) error {
	if target && value != "" {
		return fmt.Errorf("a bootstrap-only cnpg.io/podPatch remains on the baked-manager Cluster")
	}
	if !target && w.BootstrapPodPatch != nil && value != *w.BootstrapPodPatch {
		return fmt.Errorf("cnpg.io/podPatch changed since migration preflight")
	}
	return nil
}

func removeImageMigrationPodPatch(value string) []map[string]any {
	if value == "" {
		return nil
	}
	return []map[string]any{
		{"op": "test", "path": imageMigrationPodPatchPath, "value": value},
		{"op": "remove", "path": imageMigrationPodPatchPath},
	}
}

// Old intent records do not pin the annotation. Repair only a proven
// interrupted cutover of the original healthy one-volume primary: same
// Cluster, target spec, placement, and original still-bound PGDATA. CNPG,
// not Fugue, recreates the Pod and reopens the volume; completion still goes
// through the regular system-ID and SQL checks.
func (c *kubeClient) recoverLegacyImageMigrationPodPatch(ctx context.Context, w postgresImageWitness, runtimeID string) (bool, error) {
	path := cloudNativePGClusterAPIPath(w.Namespace, w.Cluster)
	cluster, found, err := c.getRawObject(ctx, path)
	if err != nil || !found {
		return false, err
	}
	meta, spec, status := imageObject(cluster["metadata"]), imageObject(cluster["spec"]), imageObject(cluster["status"])
	annotations := imageObject(meta["annotations"])
	if imageString(spec, "imageName") != w.TargetImage || annotations[imageMigrationPodPatchAnnotation] == nil {
		return false, nil
	}
	value, err := imageMigrationPodPatch(cluster)
	if err != nil {
		return false, err
	}
	dangling, _ := status["danglingPVC"].([]any)
	hash := w.SpecHash
	if w.RestartResourceTarget != nil {
		hash = w.RestartSpecHash
	}
	if imageString(meta, "uid") != w.ClusterUID || imageString(meta, "name") != w.Cluster ||
		imageString(meta, "namespace") != w.Namespace || imageString(meta, "resourceVersion") == "" ||
		imageString(meta, "deletionTimestamp") != "" || imagePreservedSpecHash(cluster) != hash ||
		annotations[runtime.CloudNativePGHibernationAnno] == runtime.CloudNativePGHibernationOn ||
		annotations[coldReconciliationAnnotation] == coldReconciliationDisabled ||
		(imageString(annotations, "cnpg.io/fencedInstances") != "" && imageString(annotations, "cnpg.io/fencedInstances") != "[]") ||
		w.ColdMigration != "" || imageString(annotations, coldMigrationAnnotation) != "" ||
		spec["instances"] != float64(1) || !imageMigrationRuntimePinned(spec, runtimeID) ||
		imageString(status, "currentPrimary") != w.Pod || imageString(status, "targetPrimary") != w.Pod ||
		imageString(status, "phase") != "Primary instance is being restarted without a switchover" ||
		(status["readyInstances"] != nil && status["readyInstances"] != float64(0)) ||
		(imageString(status, "systemID") != "" && imageString(status, "systemID") != w.SystemID) ||
		len(dangling) != 1 || dangling[0] != w.Pod || len(w.Volumes) != 1 || w.Volumes["pgdata"] == "" {
		return false, fmt.Errorf("stale bootstrap podPatch recovery lacks the original interrupted cutover identity")
	}
	if err := validateImageRestartResources(w, cluster, true); err != nil {
		return false, err
	}
	if _, podFound, err := c.getRawObject(ctx, imageDrainPodPath(w)); err != nil || podFound {
		return false, err
	}
	pods, err := c.listPodsBySelector(ctx, w.Namespace, cloudNativePGClusterLabel+"="+w.Cluster)
	if err != nil || len(pods) != 0 {
		return false, err
	}
	nodeIDs, err := c.listNodeRuntimeIDs(ctx)
	if err != nil {
		return false, err
	}
	if runtimeID == "" || nodeIDs[w.Node] != runtimeID {
		return false, fmt.Errorf("stale bootstrap podPatch recovery cannot verify the original runtime node")
	}
	for node, id := range nodeIDs {
		if id == runtimeID && node != w.Node {
			return false, fmt.Errorf("stale bootstrap podPatch recovery found an ambiguous runtime node")
		}
	}
	node, found, err := c.getNode(ctx, w.Node)
	if err != nil {
		return false, err
	}
	if err := validateManagedPostgresAutoMemoryNode(node, found, false); err != nil {
		return false, err
	}
	if node.Spec.Unschedulable {
		return false, fmt.Errorf("stale bootstrap podPatch recovery requires the original node to be schedulable")
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect == "NoSchedule" || taint.Effect == "NoExecute" {
			return false, fmt.Errorf("stale bootstrap podPatch recovery requires the original node to have no scheduling taints")
		}
	}
	identity, err := c.imageMigrationVolumeIdentity(ctx, w.Namespace, w.Pod)
	if err != nil {
		return false, err
	}
	if identity != w.Volumes["pgdata"] {
		return false, fmt.Errorf("stale bootstrap podPatch recovery found changed PGDATA storage")
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": w.ClusterUID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]},
		{"op": "test", "path": "/spec", "value": spec},
	}
	patch = append(patch, removeImageMigrationPodPatch(value)...)
	_, err = c.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil)
	return err == nil, err
}
