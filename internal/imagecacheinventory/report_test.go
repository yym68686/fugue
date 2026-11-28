package imagecacheinventory

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/imagecacheevidence"
	"fugue/internal/model"
)

func TestReportBindsIdentityAndDiscardUncountableGraphEvidence(t *testing.T) {
	const payload = `{
		"node":{"reported_by_node_updater_id":"untrusted"},
		"chunk_count":2,"chunk_index":0,"manifest_total_count":2,
		"upload_temp":{"state_mismatches":1},
		"pins":[{"repo":"/apps/sample/","target":" old "}],
		"manifests":[{"repo":"apps/sample","target":"old","graph_status":"incomplete",
			"graph_failure_reason":"missing_blob","size_bytes":500,"referenced_blob_bytes":1000,
			"referenced_manifests":["sha256:dead"]}]
	}`
	var report Report
	if err := json.Unmarshal([]byte(payload), &report); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("UTC+1", 3600))
	node, manifests, err := report.Prepare(model.NodeUpdater{ID: "auth", MachineID: "machine", ClusterNodeName: "worker", RuntimeID: "runtime"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if node.ReportedByNodeUpdaterID != "auth" || node.NodeID != "machine" || node.ClusterNodeName != "worker" ||
		node.RuntimeID != "runtime" || node.ManifestCount != 2 || node.SnapshotComplete || node.ObservedAt != now.UTC() {
		t.Fatalf("unexpected node snapshot: %+v", node)
	}
	if len(manifests) != 1 {
		t.Fatalf("expected one manifest, got %+v", manifests)
	}
	m := manifests[0]
	if m.NodeID != node.NodeID || m.ClusterNodeName != node.ClusterNodeName || m.RuntimeID != node.RuntimeID ||
		m.LastSeenAt != node.ObservedAt || !m.Present || !m.PinnedLocally ||
		m.GraphFailureReason != imagecacheevidence.ReasonMissingBlob || m.ManifestSizeBytes != 0 ||
		m.TotalBlobBytes != 0 || len(m.ReferencedManifests) != 0 {
		t.Fatalf("unexpected manifest: %+v", m)
	}
	report.ChunkIndex = 1
	if node, _, err := report.Prepare(model.NodeUpdater{MachineID: "machine"}, now); err != nil || !node.SnapshotComplete {
		t.Fatalf("final chunk did not declare completion: %+v, %v", node, err)
	}
}

func TestReportRejectsDifferentUpdaterIdentity(t *testing.T) {
	updater := model.NodeUpdater{MachineID: "machine", ClusterNodeName: "worker", RuntimeID: "runtime"}
	for name, report := range map[string]Report{
		"machine":     {Node: model.ImageCacheNodeInventory{NodeID: "other"}},
		"legacy node": {ClusterNode: "other"},
		"runtime":     {Node: model.ImageCacheNodeInventory{RuntimeID: "other"}},
	} {
		t.Run(name, func(t *testing.T) {
			node, manifests, err := report.Prepare(updater, time.Now())
			if err == nil || !strings.Contains(err.Error(), "does not match authenticated updater") ||
				node.NodeID != "" || len(manifests) != 0 {
				t.Fatalf("foreign report prepared a snapshot: %+v, %+v, %v", node, manifests, err)
			}
		})
	}
}
