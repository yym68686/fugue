package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	persistentVolumeUsageCacheKey           = "shared"
	persistentVolumeUsageCacheTTL           = 5 * time.Minute
	persistentVolumeUsageCacheMaxStale      = 30 * time.Minute
	persistentVolumeUsageRefreshTimeout     = 2 * time.Minute
	persistentVolumeDirectoryCommandTimeout = 2 * time.Minute
	persistentVolumeDirectoryPathTimeoutSec = 20
	legacyLocalPathProvisioner              = "rancher.io/local-path"
	legacyLocalPathStorageClass             = "local-path"
	legacyLocalPathSelectedNodeAnnotation   = "local.path.provisioner/selected-node"
	persistentVolumeProvisionerAnnotation   = "pv.kubernetes.io/provisioned-by"
	persistentVolumeStorageAnnotation       = "volume.kubernetes.io/storage-provisioner"
	persistentVolumeBetaStorageAnnotation   = "volume.beta.kubernetes.io/storage-provisioner"
)

type persistentVolumeUsagePolicy struct {
	useKubelet bool
	usedBytes  *int64
	measuredAt time.Time
}

type persistentVolumeUsagePolicies struct {
	fingerprint string
	strict      bool
	byClaim     map[string]persistentVolumeUsagePolicy
}

type observedPersistentVolumeClaim struct {
	namespace string
	name      string
	nodes     map[string]struct{}
}

type persistentVolumeDirectoryTarget struct {
	claimKey string
	hostPath string
}

type kubePersistentVolumeList struct {
	Items []kubePersistentVolume `json:"items"`
}

type kubePersistentVolume struct {
	Metadata struct {
		Name        string            `json:"name"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		ClaimRef *struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"claimRef,omitempty"`
		StorageClassName string `json:"storageClassName,omitempty"`
		Local            *struct {
			Path string `json:"path,omitempty"`
		} `json:"local,omitempty"`
		HostPath *struct {
			Path string `json:"path,omitempty"`
		} `json:"hostPath,omitempty"`
		NFS *struct {
			Server string `json:"server,omitempty"`
			Path   string `json:"path,omitempty"`
		} `json:"nfs,omitempty"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase,omitempty"`
	} `json:"status"`
}

func (c *clusterNodeClient) listPersistentVolumes(ctx context.Context) ([]kubePersistentVolume, error) {
	var response kubePersistentVolumeList
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/persistentvolumes", &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

func (s *Server) loadPersistentVolumeUsagePolicies(
	ctx context.Context,
	snapshots []clusterNodeSnapshot,
) (persistentVolumeUsagePolicies, error) {
	observed := collectObservedPersistentVolumeClaims(snapshots)
	fingerprint := persistentVolumeClaimFingerprint(observed)
	if len(observed) == 0 {
		return persistentVolumeUsagePolicies{
			fingerprint: fingerprint,
			strict:      true,
			byClaim:     map[string]persistentVolumeUsagePolicy{},
		}, nil
	}

	now := time.Now()
	if entry, ok := s.persistentVolumeUsageCache.getEntry(persistentVolumeUsageCacheKey); ok &&
		entry.value.fingerprint == fingerprint {
		if now.Before(entry.expiresAt) {
			return entry.value, nil
		}
		if now.Before(entry.expiresAt.Add(persistentVolumeUsageCacheMaxStale)) {
			s.refreshPersistentVolumeUsagePoliciesAsync(observed, fingerprint)
			return entry.value, nil
		}
	}

	return s.refreshPersistentVolumeUsagePolicies(ctx, observed, fingerprint)
}

func (s *Server) refreshPersistentVolumeUsagePolicies(
	ctx context.Context,
	observed map[string]observedPersistentVolumeClaim,
	fingerprint string,
) (persistentVolumeUsagePolicies, error) {
	cacheGroupKey := persistentVolumeUsageCacheKey + "|" + fingerprint
	raw, err, _ := s.persistentVolumeUsageCache.group.Do(cacheGroupKey, func() (any, error) {
		if entry, ok := s.persistentVolumeUsageCache.getEntry(persistentVolumeUsageCacheKey); ok &&
			entry.value.fingerprint == fingerprint && time.Now().Before(entry.expiresAt) {
			return entry.value, nil
		}

		refreshCtx, cancel := context.WithTimeout(contextOrBackground(ctx), persistentVolumeUsageRefreshTimeout)
		defer cancel()
		value, fetchErr := s.fetchPersistentVolumeUsagePolicies(refreshCtx, observed, fingerprint)
		if fetchErr != nil {
			return persistentVolumeUsagePolicies{}, fetchErr
		}
		if previous, ok := s.persistentVolumeUsageCache.getEntry(persistentVolumeUsageCacheKey); ok {
			value = mergePersistentVolumeUsagePolicies(value, previous.value, time.Now())
		}
		s.persistentVolumeUsageCache.set(persistentVolumeUsageCacheKey, value)
		return value, nil
	})
	if err != nil {
		return persistentVolumeUsagePolicies{}, err
	}
	return raw.(persistentVolumeUsagePolicies), nil
}

func (s *Server) refreshPersistentVolumeUsagePoliciesAsync(
	observed map[string]observedPersistentVolumeClaim,
	fingerprint string,
) {
	if s == nil {
		return
	}
	go func() {
		if _, err := s.refreshPersistentVolumeUsagePolicies(context.Background(), observed, fingerprint); err != nil && s.log != nil {
			s.log.Printf("persistent volume usage background refresh error: %v", err)
		}
	}()
}

func (s *Server) fetchPersistentVolumeUsagePolicies(
	ctx context.Context,
	observed map[string]observedPersistentVolumeClaim,
	fingerprint string,
) (persistentVolumeUsagePolicies, error) {
	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return persistentVolumeUsagePolicies{}, err
	}
	defer client.closeIdleConnections()

	volumes, err := client.listPersistentVolumes(ctx)
	if err != nil {
		return persistentVolumeUsagePolicies{}, fmt.Errorf("list persistent volumes: %w", err)
	}

	policies, targetsByNode := planPersistentVolumeUsagePolicies(observed, volumes)
	if len(targetsByNode) > 0 {
		s.measurePersistentVolumeDirectories(ctx, client, targetsByNode, policies)
	}

	return persistentVolumeUsagePolicies{
		fingerprint: fingerprint,
		strict:      true,
		byClaim:     policies,
	}, nil
}

func collectObservedPersistentVolumeClaims(snapshots []clusterNodeSnapshot) map[string]observedPersistentVolumeClaim {
	observed := make(map[string]observedPersistentVolumeClaim)
	for _, snapshot := range snapshots {
		if snapshot.summary == nil || len(snapshot.summary.Pods) == 0 || len(snapshot.pods) == 0 {
			continue
		}
		managedPods := make(map[string]struct{}, len(snapshot.pods))
		for _, pod := range snapshot.pods {
			key := clusterNamespacedResourceKey(pod.Metadata.Namespace, pod.Metadata.Name)
			if key != "" {
				managedPods[key] = struct{}{}
			}
		}
		nodeName := strings.TrimSpace(snapshot.node.Name)
		for _, pod := range snapshot.summary.Pods {
			podKey := clusterNamespacedResourceKey(pod.PodRef.Namespace, pod.PodRef.Name)
			if _, ok := managedPods[podKey]; !ok {
				continue
			}
			for _, volume := range pod.Volumes {
				if volume.PVCRef == nil {
					continue
				}
				namespace := strings.TrimSpace(volume.PVCRef.Namespace)
				if namespace == "" {
					namespace = strings.TrimSpace(pod.PodRef.Namespace)
				}
				name := strings.TrimSpace(volume.PVCRef.Name)
				claimKey := clusterNamespacedResourceKey(namespace, name)
				if claimKey == "" {
					continue
				}
				claim := observed[claimKey]
				claim.namespace = namespace
				claim.name = name
				if claim.nodes == nil {
					claim.nodes = make(map[string]struct{})
				}
				if nodeName != "" {
					claim.nodes[nodeName] = struct{}{}
				}
				observed[claimKey] = claim
			}
		}
	}
	return observed
}

func persistentVolumeClaimFingerprint(observed map[string]observedPersistentVolumeClaim) string {
	parts := make([]string, 0, len(observed))
	for key, claim := range observed {
		nodes := make([]string, 0, len(claim.nodes))
		for node := range claim.nodes {
			nodes = append(nodes, node)
		}
		sort.Strings(nodes)
		parts = append(parts, key+"@"+strings.Join(nodes, ","))
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:8])
}

func planPersistentVolumeUsagePolicies(
	observed map[string]observedPersistentVolumeClaim,
	volumes []kubePersistentVolume,
) (map[string]persistentVolumeUsagePolicy, map[string][]persistentVolumeDirectoryTarget) {
	volumesByClaim := make(map[string]kubePersistentVolume, len(volumes))
	for _, volume := range volumes {
		if volume.Spec.ClaimRef == nil || !strings.EqualFold(strings.TrimSpace(volume.Status.Phase), "Bound") {
			continue
		}
		claimKey := clusterNamespacedResourceKey(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name)
		if claimKey == "" {
			continue
		}
		volumesByClaim[claimKey] = volume
	}

	policies := make(map[string]persistentVolumeUsagePolicy, len(observed))
	targetsByNode := make(map[string][]persistentVolumeDirectoryTarget)
	for claimKey, claim := range observed {
		volume, ok := volumesByClaim[claimKey]
		if !ok {
			// Unknown claims are intentionally absent. Strict consumers must not
			// mistake an unclassified filesystem-wide kubelet value for PVC usage.
			continue
		}
		if volume.Spec.NFS != nil {
			// A kubelet statfs call for an unquoted NFS subdirectory generally
			// describes the whole export. Until the backend exposes a per-directory
			// measurement, omit it instead of leaking a shared filesystem total.
			policies[claimKey] = persistentVolumeUsagePolicy{useKubelet: false}
			continue
		}
		if !isDirectoryBackedPersistentVolume(volume) {
			policies[claimKey] = persistentVolumeUsagePolicy{useKubelet: true}
			continue
		}

		// Directory-backed local volumes share a node filesystem and usually do
		// not enforce the PVC's declared capacity. Their kubelet volume stats
		// describe the backing filesystem, not this claim.
		policies[claimKey] = persistentVolumeUsagePolicy{useKubelet: false}
		hostPath, pathOK := persistentVolumeDirectoryPath(volume)
		nodeName := persistentVolumeDirectoryNode(volume, claim)
		if !pathOK || nodeName == "" {
			continue
		}
		targetsByNode[nodeName] = append(targetsByNode[nodeName], persistentVolumeDirectoryTarget{
			claimKey: claimKey,
			hostPath: hostPath,
		})
	}
	for nodeName := range targetsByNode {
		sort.Slice(targetsByNode[nodeName], func(i, j int) bool {
			return targetsByNode[nodeName][i].claimKey < targetsByNode[nodeName][j].claimKey
		})
	}
	return policies, targetsByNode
}

func isLegacyLocalPathPersistentVolume(volume kubePersistentVolume) bool {
	for _, annotation := range []string{
		persistentVolumeProvisionerAnnotation,
		persistentVolumeStorageAnnotation,
		persistentVolumeBetaStorageAnnotation,
	} {
		provisioner := strings.TrimSpace(volume.Metadata.Annotations[annotation])
		if strings.EqualFold(provisioner, legacyLocalPathProvisioner) {
			return true
		}
	}
	return strings.EqualFold(strings.TrimSpace(volume.Spec.StorageClassName), legacyLocalPathStorageClass) &&
		(volume.Spec.Local != nil || volume.Spec.HostPath != nil)
}

func isDirectoryBackedPersistentVolume(volume kubePersistentVolume) bool {
	return isLegacyLocalPathPersistentVolume(volume) || volume.Spec.Local != nil || volume.Spec.HostPath != nil
}

func persistentVolumeDirectoryPath(volume kubePersistentVolume) (string, bool) {
	raw := ""
	if volume.Spec.Local != nil {
		raw = volume.Spec.Local.Path
	} else if volume.Spec.HostPath != nil {
		raw = volume.Spec.HostPath.Path
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.ContainsAny(raw, "\x00\r\n") || !strings.HasPrefix(raw, "/") {
		return "", false
	}
	cleaned := path.Clean(raw)
	if cleaned == "/" || cleaned == "." {
		return "", false
	}
	return cleaned, true
}

func persistentVolumeDirectoryNode(volume kubePersistentVolume, claim observedPersistentVolumeClaim) string {
	selected := strings.TrimSpace(volume.Metadata.Annotations[legacyLocalPathSelectedNodeAnnotation])
	if selected != "" {
		if len(claim.nodes) == 0 {
			return selected
		}
		if _, ok := claim.nodes[selected]; ok {
			return selected
		}
	}
	nodes := make([]string, 0, len(claim.nodes))
	for nodeName := range claim.nodes {
		if strings.TrimSpace(nodeName) != "" {
			nodes = append(nodes, strings.TrimSpace(nodeName))
		}
	}
	sort.Strings(nodes)
	if len(nodes) == 0 {
		return selected
	}
	return nodes[0]
}

func (s *Server) measurePersistentVolumeDirectories(
	ctx context.Context,
	client *clusterNodeClient,
	targetsByNode map[string][]persistentVolumeDirectoryTarget,
	policies map[string]persistentVolumeUsagePolicy,
) {
	type nodeResult struct {
		usage map[string]int64
	}

	results := make(chan nodeResult, len(targetsByNode))
	sem := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for nodeName, targets := range targetsByNode {
		nodeName := nodeName
		targets := append([]persistentVolumeDirectoryTarget(nil), targets...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}

			usage, err := s.measurePersistentVolumeDirectoriesOnNode(ctx, client, nodeName, targets)
			if err != nil {
				if s.log != nil {
					s.log.Printf("persistent volume directory usage unavailable for node=%s: %v", nodeName, err)
				}
				return
			}
			results <- nodeResult{usage: usage}
		}()
	}
	wg.Wait()
	close(results)

	for result := range results {
		for claimKey, usedBytes := range result.usage {
			policy, ok := policies[claimKey]
			if !ok || policy.useKubelet {
				continue
			}
			value := usedBytes
			policy.usedBytes = &value
			policy.measuredAt = time.Now().UTC()
			policies[claimKey] = policy
		}
	}
}

func (s *Server) measurePersistentVolumeDirectoriesOnNode(
	ctx context.Context,
	client *clusterNodeClient,
	nodeName string,
	targets []persistentVolumeDirectoryTarget,
) (map[string]int64, error) {
	janitorNamespace, janitorPod, err := s.findNodeJanitorPod(ctx, client, nodeName)
	if err != nil {
		return nil, err
	}
	script := buildPersistentVolumeDirectoryUsageScript(targets)
	if script == "" {
		return map[string]int64{}, nil
	}

	runner := s.filesystemExecRunner
	if runner == nil {
		runner = kubeFilesystemExecRunner{}
	}
	commandCtx, cancel := context.WithTimeout(contextOrBackground(ctx), persistentVolumeDirectoryCommandTimeout)
	defer cancel()
	output, _, err := runClusterExecWithRetries(
		commandCtx,
		runner,
		janitorNamespace,
		janitorPod,
		clusterNodeJanitorContainer,
		[]string{"/bin/bash", "-lc", script},
		2,
		250*time.Millisecond,
		persistentVolumeDirectoryCommandTimeout,
	)
	if err != nil {
		return nil, err
	}
	return parsePersistentVolumeDirectoryUsage(output, targets), nil
}

func mergePersistentVolumeUsagePolicies(
	current persistentVolumeUsagePolicies,
	previous persistentVolumeUsagePolicies,
	now time.Time,
) persistentVolumeUsagePolicies {
	if current.fingerprint == "" || current.fingerprint != previous.fingerprint || len(previous.byClaim) == 0 {
		return current
	}
	if now.IsZero() {
		now = time.Now()
	}
	for claimKey, policy := range current.byClaim {
		if policy.useKubelet || policy.usedBytes != nil {
			continue
		}
		oldPolicy, ok := previous.byClaim[claimKey]
		if !ok || oldPolicy.useKubelet || oldPolicy.usedBytes == nil || oldPolicy.measuredAt.IsZero() {
			continue
		}
		age := now.Sub(oldPolicy.measuredAt)
		if age < 0 {
			age = 0
		}
		if age > persistentVolumeUsageCacheMaxStale {
			continue
		}
		policy.usedBytes = cloneInt64Pointer(oldPolicy.usedBytes)
		policy.measuredAt = oldPolicy.measuredAt
		current.byClaim[claimKey] = policy
	}
	return current
}

func buildPersistentVolumeDirectoryUsageScript(targets []persistentVolumeDirectoryTarget) string {
	if len(targets) == 0 {
		return ""
	}
	var inner strings.Builder
	inner.WriteString(`set -u
measure() {
  id="$1"
  target="$2"
  raw=""
  if command -v timeout >/dev/null 2>&1; then
    raw="$(timeout --kill-after=5s `)
	inner.WriteString(strconv.Itoa(persistentVolumeDirectoryPathTimeoutSec))
	inner.WriteString(`s du -x -s -B1 -- "$target" 2>/dev/null || true)"
  else
    raw="$(du -x -s -B1 -- "$target" 2>/dev/null || true)"
  fi
  bytes="$(printf '%s\n' "$raw" | awk 'NR == 1 {print $1}')"
  case "$bytes" in
    ''|*[!0-9]*) return 0 ;;
  esac
  printf '%s\t%s\n' "$id" "$bytes"
}
`)
	for index, target := range targets {
		inner.WriteString("measure ")
		inner.WriteString(strconv.Itoa(index))
		inner.WriteByte(' ')
		inner.WriteString(shellQuote(target.hostPath))
		inner.WriteByte('\n')
	}
	return "set -euo pipefail\nchroot /host /bin/sh -lc " + shellQuote(inner.String())
}

func parsePersistentVolumeDirectoryUsage(output []byte, targets []persistentVolumeDirectoryTarget) map[string]int64 {
	usage := make(map[string]int64)
	for _, rawLine := range strings.Split(string(output), "\n") {
		parts := strings.Split(strings.TrimSpace(rawLine), "\t")
		if len(parts) != 2 {
			continue
		}
		index, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil || index < 0 || index >= len(targets) {
			continue
		}
		bytes, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || bytes < 0 {
			continue
		}
		claimKey := targets[index].claimKey
		if current, ok := usage[claimKey]; !ok || bytes > current {
			usage[claimKey] = bytes
		}
	}
	return usage
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
