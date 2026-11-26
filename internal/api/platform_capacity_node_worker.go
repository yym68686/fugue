package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"

	"fugue/internal/edgegroupfront"
	"fugue/internal/model"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
)

// A node-local Worker transition deliberately does not move the group slot.
// Recover against the latest completed authority for this physical node,
// not against whichever group-level DaemonSet happens to share its slot.
func (s *Server) capacityNodeWorkerTarget(ctx context.Context, client *clusterNodeClient, base string, node corev1.Node, service corev1.Service, group string) (model.EdgeCodeRecoveryTarget, string, error) {
	var target model.EdgeCodeRecoveryTarget
	var marker corev1.ConfigMap
	err := readCapacityMetadata(ctx, client, base+"/configmaps/"+url.PathEscape(releaseguardian.NodeWorkerModeName(group)), &marker)
	if errors.Is(err, errCapacityMetadataNotFound) {
		return target, "", nil
	}
	if err != nil {
		return target, "", err
	}
	if !capacityGuardianRecord(marker, group, "node-worker-mode", "mode.json") {
		return target, "", errors.New("invalid Guardian node-mode marker")
	}
	var mode map[string]string
	if json.Unmarshal([]byte(marker.Data["mode.json"]), &mode) != nil || len(mode) != 2 || mode["schema"] != "edge-node-worker-mode/v1" || mode["group"] != group {
		return target, "", errors.New("Guardian node-mode identity differs")
	}
	var archives corev1.ConfigMapList
	q := url.Values{"labelSelector": {"fugue.pro/group=" + group + ",fugue.pro/authority-kind=node-worker-archive"}, "limit": {"128"}}
	if err := readCapacityMetadata(ctx, client, base+"/configmaps?"+q.Encode(), &archives); err != nil || archives.Continue != "" || len(archives.Items) > 128 {
		return target, "", errors.New("Guardian node Worker history unavailable or incomplete")
	}
	var digest string
	var journal edgegroupfront.NodeWorkerJournal
	seen := map[uint64]bool{}
	for _, a := range archives.Items {
		j, err := edgegroupfront.ReadNodeWorkerJournal([]byte(a.Data["journal.json"]))
		if err != nil || !capacityGuardianRecord(a, group, "node-worker-archive", "journal.json") ||
			j.Recipient.Group != group || a.Name != "fugue-node-worker-archive-"+j.Digest[7:39] {
			return target, "", errors.New("invalid Guardian node Worker archive")
		}
		if j.Recipient.Node != node.Name {
			continue
		}
		if j.NodeUID != string(node.UID) {
			continue // old physical incarnation cannot grant recovery authority
		}
		name := releaseguardian.NodeWorkerResultName(j.Digest)
		var result corev1.ConfigMap
		if err := readCapacityMetadata(ctx, client, base+"/configmaps/"+url.PathEscape(name), &result); err != nil {
			return target, "", err
		}
		var receipt releaseguardian.NodeWorkerResult
		if !capacityGuardianRecord(result, group, "node-worker-result", "result.json") || result.Name != name ||
			json.Unmarshal([]byte(result.Data["result.json"]), &receipt) != nil || !receipt.MatchesJournal(j) {
			return target, "", errors.New("invalid Guardian node Worker result")
		}
		state := receipt.Activation
		if receipt.Outcome == "expired_without_cutover" {
			continue
		}
		if seen[state.Generation] {
			return target, "", errors.New("ambiguous Guardian node Worker result")
		}
		seen[state.Generation] = true
		if state.Generation > target.FrontGeneration {
			digest, journal = j.Digest, j
			target = model.EdgeCodeRecoveryTarget{Slot: state.ActiveSlot, FrontGeneration: state.Generation, BundleGeneration: state.BundleGeneration, WorkerSourceSHA: state.WorkerSourceCommit, WorkerImageDigest: state.WorkerImageDigest}
		}
	}
	if digest != "" && journal.ServiceUID != string(service.UID) {
		return target, "", errors.New("completed Guardian handoff refers to a different public Front Service")
	}
	return target, digest, nil
}

func capacityGuardianRecord(a corev1.ConfigMap, group, kind, key string) bool {
	return a.UID != "" && a.ResourceVersion != "" && a.DeletionTimestamp == nil && a.Immutable != nil && *a.Immutable &&
		a.Labels["app.kubernetes.io/managed-by"] == "fugue-release-guardian" && a.Labels["fugue.pro/authority-store"] == "true" &&
		a.Labels["fugue.pro/group"] == group && a.Labels["fugue.pro/authority-kind"] == kind &&
		len(a.Data) == 1 && len(a.BinaryData) == 0 && len(a.Data[key]) > 0 && len(a.Data[key]) <= 16<<10
}
