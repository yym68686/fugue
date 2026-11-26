package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"

	"fugue/internal/model"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
)

type nodeWorkerRecoveryAuthority struct {
	target        model.EdgeCodeRecoveryTarget
	journalDigest string
	releaseDigest string
}

// A node-local Worker transition deliberately does not move the group slot.
// Recover against the latest completed authority for this physical node,
// not against whichever group-level DaemonSet happens to share its slot.
func (s *Server) capacityNodeWorkerTarget(ctx context.Context, client *clusterNodeClient, base string, node corev1.Node, service corev1.Service, group string) (nodeWorkerRecoveryAuthority, error) {
	var target nodeWorkerRecoveryAuthority
	var marker corev1.ConfigMap
	err := readCapacityMetadata(ctx, client, base+"/configmaps/"+url.PathEscape(releaseguardian.NodeWorkerModeName(group)), &marker)
	if errors.Is(err, errCapacityMetadataNotFound) {
		return target, nil
	}
	if err != nil {
		return target, err
	}
	if !capacityGuardianRecord(marker, group, "node-worker-mode", "mode.json") {
		return target, errors.New("invalid Guardian node-mode marker")
	}
	var mode map[string]string
	if json.Unmarshal([]byte(marker.Data["mode.json"]), &mode) != nil || len(mode) != 2 || mode["schema"] != "edge-node-worker-mode/v1" || mode["group"] != group {
		return target, errors.New("Guardian node-mode identity differs")
	}
	read := func(name string, into *corev1.ConfigMap) error {
		return readCapacityMetadata(ctx, client, base+"/configmaps/"+url.PathEscape(name), into)
	}
	var headObject corev1.ConfigMap
	indexName := releaseguardian.NodeWorkerIndexName(group)
	if err := read(indexName, &headObject); err != nil {
		return target, errors.New("Guardian node Worker index unavailable")
	}
	index, err := releaseguardian.ReadNodeWorkerIndex(&headObject, group)
	if err != nil {
		return target, err
	}
	head, local := index.Nodes[string(node.UID)]
	checkCurrent := func() error {
		var latest corev1.ConfigMap
		if err := read(indexName, &latest); err != nil || latest.UID != headObject.UID || latest.ResourceVersion != headObject.ResourceVersion {
			return errors.New("Guardian node Worker head changed during observation")
		}
		return nil
	}
	if !local {
		return target, checkCurrent()
	}
	var archive, result corev1.ConfigMap
	archiveName := releaseguardian.NodeWorkerArchiveName(head.JournalDigest)
	if err := read(archiveName, &archive); err != nil {
		return target, err
	}
	j, err := releaseguardian.ReadArchivedNodeWorkerJournal([]byte(archive.Data["journal.json"]))
	if err != nil || !capacityGuardianRecord(archive, group, "node-worker-archive", "journal.json") ||
		archive.Name != archiveName || j.Digest != head.JournalDigest || j.Recipient.Group != group ||
		j.Recipient.Node != node.Name || j.NodeUID != string(node.UID) || j.ServiceUID != string(service.UID) {
		return target, errors.New("Guardian node Worker archive does not bind this node and public Front Service")
	}
	resultName := releaseguardian.NodeWorkerResultName(j.Digest)
	if err := read(resultName, &result); err != nil {
		return target, err
	}
	var receipt releaseguardian.NodeWorkerResult
	if !capacityGuardianRecord(result, group, "node-worker-result", "result.json") || result.Name != resultName ||
		json.Unmarshal([]byte(result.Data["result.json"]), &receipt) != nil || !receipt.MatchesJournal(j) ||
		receipt.Outcome == "expired_without_cutover" || receipt.Activation.Generation != head.Generation {
		return target, errors.New("invalid Guardian node Worker result")
	}
	if err := checkCurrent(); err != nil {
		return target, err
	}
	state := receipt.Activation
	target.target = model.EdgeCodeRecoveryTarget{Slot: state.ActiveSlot, FrontGeneration: state.Generation, BundleGeneration: state.BundleGeneration,
		WorkerSourceSHA: state.WorkerSourceCommit, WorkerImageDigest: state.WorkerImageDigest}
	target.journalDigest = j.Digest
	// A reverted journal proves the predecessor is serving, not that the
	// candidate's immutable release is installed.
	if receipt.Outcome == "verified" {
		target.releaseDigest = j.ReleaseDigest
	}
	return target, nil
}

func capacityGuardianRecord(a corev1.ConfigMap, group, kind, key string) bool {
	return a.UID != "" && a.ResourceVersion != "" && a.DeletionTimestamp == nil && a.Immutable != nil && *a.Immutable &&
		a.Labels["app.kubernetes.io/managed-by"] == "fugue-release-guardian" && a.Labels["fugue.pro/authority-store"] == "true" &&
		a.Labels["fugue.pro/group"] == group && a.Labels["fugue.pro/authority-kind"] == kind &&
		len(a.Data) == 1 && len(a.BinaryData) == 0 && len(a.Data[key]) > 0 && len(a.Data[key]) <= 16<<10
}
