package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"

	"fugue/internal/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var errPendingResourceObservation = errors.New("pending resource observation unavailable or changed")

type pendingResourceCluster struct {
	Metadata metav1.ObjectMeta `json:"metadata"`
	Status   struct {
		CurrentPrimary string `json:"currentPrimary"`
		TargetPrimary  string `json:"targetPrimary"`
		ReadyInstances int    `json:"readyInstances"`
		Instances      int    `json:"instances"`
		Phase          string `json:"phase"`
	} `json:"status"`
}

type pendingResourceLonghornVolume struct {
	Metadata metav1.ObjectMeta `json:"metadata"`
	Spec     struct {
		Size   string `json:"size"`
		NodeID string `json:"nodeID"`
	} `json:"spec"`
	Status struct {
		State         string `json:"state"`
		Robustness    string `json:"robustness"`
		CurrentNodeID string `json:"currentNodeID"`
	} `json:"status"`
}

// Projected identities are safe to return through the private Unix socket.
// Raw Pod specs (environment/credentials) and node summary inventory never leave
// this function. Resource versions bracket sequential, non-atomic source reads.
type pendingResourceStorage struct {
	Identity                                                                        store.PendingRootResourceStorageIdentity `json:"identity"`
	ClusterVersion, PodVersion, PVCVersion, PVVersion, LonghornVersion, NodeVersion string
	Primary, NodeName, NodeUID, PVCName, PVName, VolumeName                         string
	PodAddress, ContainerID, ImageID                                                string
	RestartCount                                                                    int32
	ContainerStarted                                                                time.Time
	CapacityBytes                                                                   int64
	DataDirectorySource                                                             string
}

type pendingResourceVolumeStats struct {
	Time           time.Time                         `json:"time"`
	CapacityBytes  *uint64                           `json:"capacityBytes"`
	AvailableBytes *uint64                           `json:"availableBytes"`
	UsedBytes      *uint64                           `json:"usedBytes"`
	PVCRef         *struct{ Name, Namespace string } `json:"pvcRef"`
}

type pendingResourceSummary struct {
	Pods []struct {
		PodRef  struct{ Name, Namespace, UID string } `json:"podRef"`
		Volumes []pendingResourceVolumeStats          `json:"volume"`
	} `json:"pods"`
}

// PendingResourceObservation is demand-driven and observation-only. It cannot
// issue a PendingRootResourceWitness or install/enable a production resource
// pool. Whole-operation bounds require independent evidence, not these samples.
func (s *Server) PendingResourceObservation(parent context.Context) (any, error) {
	started := time.Now().UTC()
	value, err := s.pendingResourceObservation(parent)
	s.pendingResourceStatus.record(started, err)
	return value, err
}

func (s *Server) pendingResourceObservation(parent context.Context) (any, error) {
	if parent == nil {
		return nil, pendingResourceRefuse("request", "invalid-context", nil)
	}
	if !s.pendingResourceObservationMu.TryLock() {
		return nil, pendingResourceRefuse("request", "busy", nil)
	}
	defer s.pendingResourceObservationMu.Unlock()
	ctx, cancel := context.WithTimeout(parent, 2500*time.Millisecond)
	defer cancel()
	client, err := newKubeLogsClient(s.controlPlaneNamespace)
	if err != nil {
		return nil, pendingResourceRefuse("kubernetes-client", "unavailable", err)
	}
	defer client.closeIdleConnections()
	lhNS := firstNonEmptyString(getenv(longhornBackupNamespaceEnv), "longhorn-system")
	return observePendingResources(ctx, client, s.controlPlaneNamespace, s.controlPlanePostgresClusterName, lhNS, s.store.ReadPendingRootDatabaseResources)
}

func pendingResourceGet(ctx context.Context, c *kubeLogsClient, stage, apiPath string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+apiPath, nil)
	if err != nil {
		return pendingResourceRefuse(stage, "request", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")
	response, err := c.client.Do(req)
	if err != nil {
		return pendingResourceRefuse(stage, "transport", err)
	}
	defer response.Body.Close()
	const limit = 2 << 20
	raw, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return pendingResourceRefuse(stage, "response-read", err)
	}
	if len(raw) > limit {
		return pendingResourceRefuse(stage, "response-limit", nil)
	}
	if response.StatusCode != http.StatusOK {
		reason := "http-unavailable"
		if response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusUnauthorized {
			reason = "http-denied"
		} else if response.StatusCode == http.StatusNotFound {
			reason = "http-not-found"
		}
		return pendingResourceRefuse(stage, reason, nil)
	}
	if json.Unmarshal(raw, out) != nil {
		return pendingResourceRefuse(stage, "response-format", nil)
	}
	return nil
}

func pendingResourceObjectPath(namespace, resource, name string) string {
	return "/api/v1/namespaces/" + url.PathEscape(namespace) + "/" + resource + "/" + url.PathEscape(name)
}

func readPendingResourceStorage(ctx context.Context, c *kubeLogsClient, namespace, clusterName, lhNS, directory string) (pendingResourceStorage, error) {
	var out pendingResourceStorage
	var cluster pendingResourceCluster
	if err := pendingResourceGet(ctx, c, "cluster", "/apis/postgresql.cnpg.io/v1/namespaces/"+url.PathEscape(namespace)+"/clusters/"+url.PathEscape(clusterName), &cluster); err != nil {
		return out, err
	}
	if cluster.Metadata.Name != clusterName || cluster.Metadata.Namespace != namespace || cluster.Metadata.ResourceVersion == "" || cluster.Metadata.UID == "" || cluster.Metadata.DeletionTimestamp != nil || cluster.Status.ReadyInstances != 1 || cluster.Status.Instances != 1 || cluster.Status.Phase != "Cluster in healthy state" || cluster.Status.CurrentPrimary == "" || (cluster.Status.TargetPrimary != "" && cluster.Status.TargetPrimary != cluster.Status.CurrentPrimary) {
		return out, pendingResourceRefuse("cluster", "identity-or-health", nil)
	}
	var pod corev1.Pod
	if err := pendingResourceGet(ctx, c, "pg-pod", pendingResourceObjectPath(namespace, "pods", cluster.Status.CurrentPrimary), &pod); err != nil {
		return out, err
	}
	owner := metav1.GetControllerOf(&pod)
	if pod.Name != cluster.Status.CurrentPrimary || pod.Namespace != namespace || pod.ResourceVersion == "" || pod.UID == "" || pod.DeletionTimestamp != nil || owner == nil || owner.Kind != "Cluster" || owner.UID != cluster.Metadata.UID || owner.Name != clusterName || pod.Status.Phase != corev1.PodRunning || pod.Spec.NodeName == "" || pod.Status.PodIP == "" {
		return out, pendingResourceRefuse("pg-pod", "identity-or-health", nil)
	}
	ready := false
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			ready = true
		}
	}
	if !ready {
		return out, pendingResourceRefuse("pg-pod", "not-ready", nil)
	}
	var container *corev1.Container
	var status *corev1.ContainerStatus
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "postgres" {
			container = &pod.Spec.Containers[i]
		}
	}
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == "postgres" {
			status = &pod.Status.ContainerStatuses[i]
		}
	}
	if container == nil || status == nil || !status.Ready || status.State.Running == nil || status.State.Running.StartedAt.IsZero() || status.ContainerID == "" || status.ImageID == "" {
		return out, pendingResourceRefuse("pg-container", "identity-or-health", nil)
	}
	// data_directory is deliberately not granted to the application role. When
	// PG hides it, use the CNPG postgres container's literal PGDATA and label its
	// provenance. This does not claim independent filesystem/path attestation.
	out.DataDirectorySource = "postgres-setting"
	if directory == "" {
		out.DataDirectorySource = "cnpg-pod-pgdata"
		for _, env := range container.Env {
			if env.Name == "PGDATA" && env.ValueFrom == nil {
				directory = env.Value
			}
		}
	}
	if !strings.HasPrefix(directory, "/") || path.Clean(directory) != directory {
		return out, pendingResourceRefuse("pg-mount", "invalid-directory", nil)
	}
	pvcName := ""
	for _, mount := range container.VolumeMounts {
		if directory != mount.MountPath && !strings.HasPrefix(directory, strings.TrimRight(mount.MountPath, "/")+"/") {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.Name == mount.Name && volume.PersistentVolumeClaim != nil {
				if pvcName != "" || mount.SubPath != "" || mount.SubPathExpr != "" || mount.ReadOnly {
					return out, pendingResourceRefuse("pg-mount", "ambiguous-or-unsupported", nil)
				}
				pvcName = volume.PersistentVolumeClaim.ClaimName
			}
		}
	}
	if pvcName == "" {
		return out, pendingResourceRefuse("pg-mount", "missing-pvc", nil)
	}
	var pvc corev1.PersistentVolumeClaim
	if err := pendingResourceGet(ctx, c, "pvc", pendingResourceObjectPath(namespace, "persistentvolumeclaims", pvcName), &pvc); err != nil {
		return out, err
	}
	capacity := pvc.Status.Capacity[corev1.ResourceStorage]
	requested := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if pvc.Name != pvcName || pvc.Namespace != namespace || pvc.ResourceVersion == "" || pvc.UID == "" || pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" || capacity.Sign() <= 0 || capacity.Cmp(requested) != 0 || (pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem) {
		return out, pendingResourceRefuse("pvc", "identity-or-capacity", nil)
	}
	for _, condition := range pvc.Status.Conditions {
		if condition.Status == corev1.ConditionTrue {
			return out, pendingResourceRefuse("pvc", "condition-active", nil)
		}
	}
	var pv corev1.PersistentVolume
	if err := pendingResourceGet(ctx, c, "pv", "/api/v1/persistentvolumes/"+url.PathEscape(pvc.Spec.VolumeName), &pv); err != nil {
		return out, err
	}
	if pv.Name != pvc.Spec.VolumeName || pv.ResourceVersion == "" || pv.UID == "" || pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID || pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.ClaimRef.Namespace != namespace || pv.Spec.CSI == nil || pv.Spec.CSI.ReadOnly || pv.Spec.CSI.Driver != "driver.longhorn.io" || pv.Spec.CSI.VolumeHandle == "" || pv.Spec.Capacity.Storage().Cmp(capacity) != 0 {
		return out, pendingResourceRefuse("pv", "identity-or-capacity", nil)
	}
	var lh pendingResourceLonghornVolume
	if err := pendingResourceGet(ctx, c, "longhorn", "/apis/longhorn.io/v1beta2/namespaces/"+url.PathEscape(lhNS)+"/volumes/"+url.PathEscape(pv.Spec.CSI.VolumeHandle), &lh); err != nil {
		return out, err
	}
	if lh.Metadata.Name != pv.Spec.CSI.VolumeHandle || lh.Metadata.Namespace != lhNS || lh.Metadata.ResourceVersion == "" || lh.Metadata.UID == "" || lh.Metadata.DeletionTimestamp != nil || lh.Status.State != "attached" || lh.Status.Robustness != "healthy" || lh.Status.CurrentNodeID != pod.Spec.NodeName || lh.Spec.NodeID != pod.Spec.NodeName {
		return out, pendingResourceRefuse("longhorn", "identity-or-health", nil)
	}
	lhBytes, err := strconv.ParseInt(lh.Spec.Size, 10, 64)
	if err != nil || lhBytes != capacity.Value() {
		return out, pendingResourceRefuse("longhorn", "capacity", nil)
	}
	var node corev1.Node
	if err := pendingResourceGet(ctx, c, "node", "/api/v1/nodes/"+url.PathEscape(pod.Spec.NodeName), &node); err != nil {
		return out, err
	}
	nodeReady := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			nodeReady = true
		}
	}
	if node.Name != pod.Spec.NodeName || node.ResourceVersion == "" || node.UID == "" || node.DeletionTimestamp != nil || !nodeReady {
		return out, pendingResourceRefuse("node", "identity-or-health", nil)
	}
	out.Identity = store.PendingRootResourceStorageIdentity{ClusterUID: string(cluster.Metadata.UID), PGPodUID: string(pod.UID), PVCUID: string(pvc.UID), PVUID: string(pv.UID), LonghornVolumeUID: string(lh.Metadata.UID)}
	out.ClusterVersion, out.PodVersion, out.PVCVersion, out.PVVersion, out.LonghornVersion, out.NodeVersion = cluster.Metadata.ResourceVersion, pod.ResourceVersion, pvc.ResourceVersion, pv.ResourceVersion, lh.Metadata.ResourceVersion, node.ResourceVersion
	out.Primary, out.NodeName, out.NodeUID, out.PVCName, out.PVName, out.VolumeName = pod.Name, node.Name, string(node.UID), pvc.Name, pv.Name, lh.Metadata.Name
	out.PodAddress, out.ContainerID, out.ImageID = pod.Status.PodIP, status.ContainerID, status.ImageID
	out.RestartCount, out.ContainerStarted, out.CapacityBytes = status.RestartCount, status.State.Running.StartedAt.Time, capacity.Value()
	return out, nil
}

func observePendingResources(ctx context.Context, c *kubeLogsClient, namespace, cluster, lhNS string, database func(context.Context) (store.PendingRootDatabaseResources, error)) (any, error) {
	if ctx == nil || c == nil || namespace == "" || cluster == "" || lhNS == "" || database == nil {
		return nil, pendingResourceRefuse("request", "invalid-configuration", nil)
	}
	started := time.Now().UTC()
	db, err := database(ctx)
	if err != nil {
		return nil, pendingResourceRefuse("database-before", "query", err)
	}
	if db.Database == "" || db.DatabaseOID == 0 || db.Schema == "" || db.PostmasterStarted.IsZero() {
		return nil, pendingResourceRefuse("database-before", "identity", nil)
	}
	before, err := readPendingResourceStorage(ctx, c, namespace, cluster, lhNS, db.DataDirectory)
	if err != nil {
		return nil, pendingResourcePhase("storage-before", err)
	}
	if db.ServerAddress != before.PodAddress || db.ServerPort <= 0 || db.InRecovery || !db.ReadOnly || !db.TempFilesForbidden {
		return nil, pendingResourceRefuse("database-before", "endpoint-or-guard", nil)
	}
	var summary pendingResourceSummary
	if err := pendingResourceGet(ctx, c, "volume-stats", "/api/v1/nodes/"+url.PathEscape(before.NodeName)+"/proxy/stats/summary", &summary); err != nil {
		return nil, err
	}
	var stats *pendingResourceVolumeStats
	for _, pod := range summary.Pods {
		if pod.PodRef.UID != before.Identity.PGPodUID || pod.PodRef.Name != before.Primary || pod.PodRef.Namespace != namespace {
			continue
		}
		for _, volume := range pod.Volumes {
			if volume.PVCRef != nil && volume.PVCRef.Name == before.PVCName && volume.PVCRef.Namespace == namespace {
				if stats != nil {
					return nil, pendingResourceRefuse("volume-stats", "duplicate", nil)
				}
				copy := volume
				stats = &copy
			}
		}
	}
	now := time.Now().UTC()
	if stats == nil || stats.CapacityBytes == nil || stats.AvailableBytes == nil || stats.UsedBytes == nil {
		return nil, pendingResourceRefuse("volume-stats", "missing", nil)
	}
	if *stats.CapacityBytes == 0 || *stats.CapacityBytes > uint64(before.CapacityBytes) || *stats.AvailableBytes > *stats.CapacityBytes || *stats.UsedBytes > *stats.CapacityBytes {
		return nil, pendingResourceRefuse("volume-stats", "capacity", nil)
	}
	if stats.Time.IsZero() || stats.Time.After(now.Add(5*time.Second)) || now.Sub(stats.Time) > 30*time.Second {
		return nil, pendingResourceRefuse("volume-stats", "stale-or-clock", nil)
	}
	after, err := readPendingResourceStorage(ctx, c, namespace, cluster, lhNS, db.DataDirectory)
	if err != nil {
		return nil, pendingResourcePhase("storage-after", err)
	}
	if !reflect.DeepEqual(before, after) {
		return nil, pendingResourceRefuse("storage-consistency", "resource-version-or-identity-changed", nil)
	}
	endDB, err := database(ctx)
	if err != nil {
		return nil, pendingResourceRefuse("database-after", "query", err)
	}
	if db.Database != endDB.Database || db.DatabaseOID != endDB.DatabaseOID || db.Schema != endDB.Schema || db.ServerAddress != endDB.ServerAddress || db.ServerPort != endDB.ServerPort || !db.PostmasterStarted.Equal(endDB.PostmasterStarted) || db.DataDirectory != endDB.DataDirectory || endDB.InRecovery || !endDB.ReadOnly || !endDB.TempFilesForbidden || !reflect.DeepEqual(db.LedgerRelations, endDB.LedgerRelations) {
		return nil, pendingResourceRefuse("database-consistency", "identity-or-guard-changed", nil)
	}
	before.Identity.Database, before.Identity.DatabaseOID, before.Identity.Schema = db.Database, db.DatabaseOID, db.Schema
	raw, _ := json.Marshal(before)
	hash := sha256.Sum256(raw)
	return map[string]any{"schema": "fugue.pending-root-resource.observation.v1", "started_at": started, "completed_at": time.Now().UTC(), "storage": before, "storage_identity_sha256": hex.EncodeToString(hash[:]), "database_before": db, "database_after": endDB, "volume_stats": stats, "observation_only": true, "atomic_cross_source_snapshot": false, "admission_allowed": false, "whole_operation_budget_proven": false, "whole_operation_bounds": nil, "counter_attribution": "WAL cluster-wide; temp database-wide; neither is a per-operation bound", "volume_sample_max_age_seconds": 30}, nil
}
