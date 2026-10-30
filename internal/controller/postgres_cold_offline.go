package controller

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	runtimepkg "fugue/internal/runtime"
)

const coldReconciliationAnnotation = "cnpg.io/reconciliationLoop"
const coldReconciliationDisabled = "disabled"

// An unscheduled Pod has never run a database process. Recover its identified
// primary claim without allocating a PostgreSQL instance on the source node.
func coldPrimaryUnstarted(pod kubePod) bool {
	return pod.ObservedUID != "" && pod.Spec.NodeName == "" && pod.Metadata.DeletionTimestamp == "" && pod.Status.Phase == "Pending" && len(pod.Status.ContainerStatuses) == 0 && len(pod.Status.InitContainerStatuses) == 0
}

func coldClusterOwnsPod(cluster kubeCloudNativePGCluster, pod kubePod) bool {
	for _, ref := range pod.ObservedOwnerReferences {
		if ref.Controller && ref.UID == cluster.Metadata.UID && ref.Name == cluster.Metadata.Name && ref.Kind == runtimepkg.CloudNativePGClusterKind && ref.APIVersion == runtimepkg.CloudNativePGAPIVersion {
			return cluster.Metadata.UID != ""
		}
	}
	return false
}

func coldPostgresImage(pod kubePod) string {
	for _, container := range pod.Spec.Containers {
		if container.Name == "postgres" {
			return container.Image
		}
	}
	return ""
}

func coldPeerImageID(ctx context.Context, c *kubeClient, ns string, cluster kubeCloudNativePGCluster, primary kubePod) (string, error) {
	if !coldClusterOwnsPod(cluster, primary) || coldPostgresImage(primary) == "" {
		return "", fmt.Errorf("unstarted primary is not owned by the identified source cluster")
	}
	pods, err := c.listPodsBySelector(ctx, ns, "cnpg.io/cluster="+cluster.Metadata.Name)
	if err != nil {
		return "", err
	}
	image := ""
	for _, pod := range pods {
		if !coldClusterOwnsPod(cluster, pod) || coldPostgresImage(pod) != coldPostgresImage(primary) || pod.Metadata.DeletionTimestamp != "" {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name != "postgres" || !cs.Ready {
				continue
			}
			candidate := strings.TrimPrefix(cs.ImageID, "docker-pullable://")
			_, digest, ok := strings.Cut(candidate, "@sha256:")
			decoded, err := hex.DecodeString(digest)
			if !ok || err != nil || len(decoded) != 32 {
				continue
			}
			if image != "" && image != candidate {
				return "", fmt.Errorf("source peer immutable PostgreSQL images disagree")
			}
			image = candidate
		}
	}
	if image == "" {
		return "", fmt.Errorf("no same-cluster peer proves the unstarted primary's PostgreSQL image digest")
	}
	return image, nil
}

func prepareColdOfflineReader(ctx context.Context, c *kubeClient, ns string, cluster kubeCloudNativePGCluster, primary kubePod, st *coldPostgresState) error {
	raw, found, err := c.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/pods/"+url.PathEscape(primary.Metadata.Name))
	if err != nil || !found || objectStringField(normalizeKubeMap(raw["metadata"]), "uid") != primary.ObservedUID {
		return fmt.Errorf("unstarted primary changed while recording read-only volume layout")
	}
	if err := coldRecordVolumeLayout(raw, st); err != nil {
		return err
	}
	pods, err := c.listPodsBySelector(ctx, ns, "cnpg.io/cluster="+cluster.Metadata.Name)
	if err != nil {
		return err
	}
	st.SourceNodes = []string{st.SourceNode}
	for _, pod := range pods {
		if managedPostgresPodFinished(pod) {
			continue
		}
		if !coldClusterOwnsPod(cluster, pod) {
			return fmt.Errorf("source cluster label includes an unowned Pod")
		}
		if pod.Spec.NodeName != "" && !slices.Contains(st.SourceNodes, pod.Spec.NodeName) {
			st.SourceNodes = append(st.SourceNodes, pod.Spec.NodeName)
		}
	}
	if err := coldSourceNodesReady(ctx, c, st); err != nil {
		return err
	}
	st.OfflineSource = true
	st.SourceReader = st.SeedName + "-source"
	st.SourceTolerations = primary.Spec.Tolerations
	return nil
}

func coldRecordVolumeLayout(raw map[string]any, st *coldPostgresState) error {
	spec := normalizeKubeMap(raw["spec"])
	var container map[string]any
	for _, item := range mapSlice(spec["containers"]) {
		if item["name"] == "postgres" {
			container = item
		}
	}
	dataPath := ""
	for _, env := range mapSlice(container["env"]) {
		if env["name"] == "PGDATA" {
			dataPath, _ = env["value"].(string)
		}
	}
	if !path.IsAbs(dataPath) || path.Clean(dataPath) != dataPath || dataPath == "/" {
		return fmt.Errorf("source Pod does not declare a canonical PGDATA path")
	}
	claimVolume := ""
	for _, volume := range mapSlice(spec["volumes"]) {
		if normalizeKubeMap(volume["persistentVolumeClaim"])["claimName"] == st.SourcePVC {
			claimVolume, _ = volume["name"].(string)
		}
	}
	mountPath := ""
	for _, mount := range mapSlice(container["volumeMounts"]) {
		if mount["name"] != claimVolume || claimVolume == "" {
			continue
		}
		mountPath, _ = mount["mountPath"].(string)
		if mount["subPath"] != nil || mount["subPathExpr"] != nil {
			return fmt.Errorf("offline PGDATA subPath volumes require another recovery strategy")
		}
	}
	if !path.IsAbs(mountPath) || path.Clean(mountPath) != mountPath || mountPath == "/" || !strings.HasPrefix(dataPath, mountPath+"/") {
		return fmt.Errorf("source PGDATA must be inside the identified primary data claim")
	}
	security := cloneKubeMap(normalizeKubeMap(spec["securityContext"]))
	if security == nil {
		security = map[string]any{}
	}
	for key, value := range normalizeKubeMap(container["securityContext"]) {
		security[key] = value
	}
	uid, uidOK := coldNumericID(security["runAsUser"])
	gid, gidOK := coldNumericID(security["runAsGroup"])
	if !uidOK || !gidOK || uid <= 0 || gid <= 0 {
		return fmt.Errorf("source PostgreSQL non-root numeric user/group unavailable")
	}
	st.SourceDataPath, st.SourceMountPath = dataPath, mountPath
	st.SourceRunAsUser, st.SourceRunAsGroup = uid, gid
	return nil
}

func coldNumericID(value any) (int64, bool) {
	number, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
	return number, err == nil
}

func coldJSONValue(value any) any {
	raw, _ := json.Marshal(value)
	var result any
	_ = json.Unmarshal(raw, &result)
	return result
}

func coldSourceNodesReady(ctx context.Context, c *kubeClient, st *coldPostgresState) error {
	if len(st.SourceNodes) == 0 {
		return fmt.Errorf("offline source node witness is missing")
	}
	for _, name := range st.SourceNodes {
		node, found, err := c.getNode(ctx, name)
		if err != nil || !found || !kubeNodeReady(node) {
			return fmt.Errorf("source node %s must be healthy to acknowledge stopped database I/O", name)
		}
	}
	return nil
}

func verifyColdOfflineQuiesced(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState) error {
	cluster, found, err := c.getCloudNativePGCluster(ctx, ns, st.SourceName)
	if err != nil || !found || cluster.Metadata.UID != st.SourceUID || cluster.Metadata.Annotations[coldMigrationAnnotation] != coldStateName(st.ServiceID) || cluster.Metadata.Annotations["cnpg.io/fencedInstances"] != `["*"]` || cluster.Metadata.Annotations[coldReconciliationAnnotation] != coldReconciliationDisabled || cluster.Metadata.Annotations[runtimepkg.CloudNativePGHibernationAnno] != runtimepkg.CloudNativePGHibernationOn {
		return fmt.Errorf("offline source fencing/hibernation identity changed")
	}
	pods, err := c.listPodsBySelector(ctx, ns, "cnpg.io/cluster="+st.SourceName)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if !managedPostgresPodFinished(pod) {
			return fmt.Errorf("source database Pods have not finished normal termination")
		}
	}
	return coldSourceNodesReady(ctx, c, st)
}

func coldOfflineReaderPod(ns string, st *coldPostgresState) map[string]any {
	return movableRWOPodObject(ns, st.SourceReader, map[string]string{coldMigrationAnnotation: coldStateName(st.ServiceID)}, map[string]any{
		"restartPolicy": "Never", "automountServiceAccountToken": false,
		"nodeSelector": map[string]string{kubeHostnameLabelKey: st.SourceNode}, "tolerations": st.SourceTolerations,
		"securityContext": map[string]any{"runAsUser": st.SourceRunAsUser, "runAsGroup": st.SourceRunAsGroup, "runAsNonRoot": true, "seccompProfile": map[string]string{"type": "RuntimeDefault"}},
		"containers": []map[string]any{{"name": "postgres", "image": st.Image, "command": []string{"python3", "-c", "import time; time.sleep(86400)"},
			"resources":       map[string]any{"requests": map[string]string{"cpu": "5m", "memory": "128Mi"}, "limits": map[string]string{"memory": "512Mi"}},
			"securityContext": map[string]any{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": map[string]any{"drop": []string{"ALL"}}},
			"volumeMounts":    []map[string]any{{"name": "source", "mountPath": st.SourceMountPath, "readOnly": true}}}},
		"volumes": []map[string]any{{"name": "source", "persistentVolumeClaim": map[string]any{"claimName": st.SourcePVC, "readOnly": true}}},
	})
}

func verifyColdOfflineReader(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState) error {
	raw, found, err := c.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/pods/"+url.PathEscape(st.SourceReader))
	if err != nil || !found {
		return fmt.Errorf("offline source reader unavailable")
	}
	m := normalizeKubeMap(raw["metadata"])
	if objectStringField(m, "uid") == "" || (st.SourceReaderUID != "" && m["uid"] != st.SourceReaderUID) || normalizeKubeMap(m["labels"])[coldMigrationAnnotation] != coldStateName(st.ServiceID) || m["deletionTimestamp"] != nil {
		return fmt.Errorf("offline source reader ownership/UID changed")
	}
	// Kube defaults are allowed; every field we explicitly requested must match.
	want := normalizeKubeMap(coldOfflineReaderPod(ns, st)["spec"])
	got := normalizeKubeMap(raw["spec"])
	for _, key := range []string{"nodeSelector", "securityContext", "automountServiceAccountToken", "volumes"} {
		if !reflect.DeepEqual(got[key], coldJSONValue(want[key])) {
			return fmt.Errorf("offline source reader %s changed", key)
		}
	}
	containers := mapSlice(got["containers"])
	expected := mapSlice(coldJSONValue(want["containers"]))
	if len(containers) != 1 || len(mapSlice(got["initContainers"])) != 0 || got["hostPID"] == true || got["hostNetwork"] == true || len(mapSlice(got["ephemeralContainers"])) != 0 {
		return fmt.Errorf("offline source reader has unexpected execution access")
	}
	for _, key := range []string{"name", "image", "command", "securityContext", "volumeMounts"} {
		if !reflect.DeepEqual(containers[0][key], expected[0][key]) {
			return fmt.Errorf("offline source reader container %s changed", key)
		}
	}
	if got["nodeName"] != st.SourceNode {
		return fmt.Errorf("offline source reader is not scheduled on the identified volume node")
	}
	return nil
}

func (s *Service) ensureColdOfflineReader(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState, rv *string, operationID string) error {
	cluster, found, err := c.getCloudNativePGCluster(ctx, ns, st.SourceName)
	if err != nil || !found || cluster.Metadata.UID != st.SourceUID || cluster.Metadata.ResourceVersion == "" {
		return fmt.Errorf("source identity unavailable before hibernation")
	}
	if err := coldSourceNodesReady(ctx, c, st); err != nil {
		return err
	}
	if cluster.Metadata.Annotations[coldReconciliationAnnotation] != coldReconciliationDisabled || cluster.Metadata.Annotations[runtimepkg.CloudNativePGHibernationAnno] != runtimepkg.CloudNativePGHibernationOn {
		// Declarative hibernation waits for a healthy cluster in newer CNPG.
		// An offline recovery must not start its Pending primary to become healthy.
		// Pause reconciliation, retain the durable hibernation/fencing intent, then
		// gracefully terminate only this cluster's Pods with UID preconditions.
		body := map[string]any{"metadata": map[string]any{"uid": st.SourceUID, "resourceVersion": cluster.Metadata.ResourceVersion, "annotations": map[string]string{runtimepkg.CloudNativePGHibernationAnno: runtimepkg.CloudNativePGHibernationOn, coldReconciliationAnnotation: coldReconciliationDisabled}}}
		if _, err := c.doRequest(ctx, http.MethodPatch, cloudNativePGClusterAPIPath(ns, st.SourceName), "application/merge-patch+json", body, nil); err != nil {
			return err
		}
	}
	s.updateManagedPostgresTransitionProgress(operationID, "source reconciliation paused; gracefully stopping its instances while retaining every data claim")
	stable := 0
	for {
		if err := s.ensureOperationStillActive(operationID); err != nil {
			return err
		}
		if err := stopColdOfflineInstances(ctx, c, ns, st); err != nil {
			return err
		}
		if err := verifyColdOfflineQuiesced(ctx, c, ns, st); err == nil {
			stable++
			if stable >= 3 {
				break
			}
		} else {
			stable = 0
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("source hibernation not verified: %w", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	podPath := "/api/v1/namespaces/" + url.PathEscape(ns) + "/pods/" + url.PathEscape(st.SourceReader)
	reader, exists, err := c.getRawObject(ctx, podPath)
	if err != nil {
		return err
	}
	if !exists {
		if st.SourceReaderUID != "" {
			return fmt.Errorf("recorded offline source reader disappeared")
		}
		_, err := c.doRequest(ctx, http.MethodPost, "/api/v1/namespaces/"+url.PathEscape(ns)+"/pods", "application/json", coldOfflineReaderPod(ns, st), &reader)
		if err != nil {
			return err
		}
	}
	if err := waitForMovableRWOPodReady(ctx, c, ns, st.SourceReader, 5*time.Minute); err != nil {
		return err
	}
	if err := verifyColdOfflineReader(ctx, c, ns, st); err != nil {
		return err
	}
	st.SourceReaderUID = objectStringField(normalizeKubeMap(reader["metadata"]), "uid")
	return saveColdState(ctx, c, ns, st, rv)
}

func stopColdOfflineInstances(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState) error {
	cluster, found, err := c.getCloudNativePGCluster(ctx, ns, st.SourceName)
	if err != nil || !found || cluster.Metadata.UID != st.SourceUID || cluster.Metadata.Annotations[coldReconciliationAnnotation] != coldReconciliationDisabled || cluster.Metadata.Annotations[coldMigrationAnnotation] != coldStateName(st.ServiceID) || cluster.Metadata.Annotations["cnpg.io/fencedInstances"] != `["*"]` {
		return fmt.Errorf("source pause/fencing identity changed before stopping instances")
	}
	if err := coldSourceNodesReady(ctx, c, st); err != nil {
		return err
	}
	pods, err := c.listPodsBySelector(ctx, ns, "cnpg.io/cluster="+st.SourceName)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if managedPostgresPodFinished(pod) {
			continue
		}
		if !coldClusterOwnsPod(cluster, pod) || pod.ObservedUID == "" || (pod.Spec.NodeName != "" && !slices.Contains(st.SourceNodes, pod.Spec.NodeName)) {
			return fmt.Errorf("refusing to stop an unowned or relocated source instance")
		}
		if pod.Metadata.DeletionTimestamp != "" {
			continue
		}
		// Normal deletion lets the instance manager finish PostgreSQL shutdown.
		// No force deletion or zero grace period is ever requested. Healthy-node
		// and repeated absence gates precede the read-only volume mount.
		if err := c.deleteObjectWithUID(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/pods/"+url.PathEscape(pod.Metadata.Name), pod.ObservedUID); err != nil {
			return err
		}
	}
	return nil
}
