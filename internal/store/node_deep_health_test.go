package store

import (
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestRecordNodeDeepHealthQuarantinesDNSHardFailAndRecovers(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	failed, err := s.RecordNodeDeepHealthResult(model.NodeDeepHealthResult{
		NodeUpdaterID:   "nodeupdater_dns",
		ClusterNodeName: "node-a",
		Checks: []model.NodeDeepHealthCheck{{
			Name:     model.NodeDeepHealthCheckPodDNSToKubeDNSService,
			Status:   model.NodeDeepHealthStatusFail,
			HardFail: true,
			Observed: "lookup timeout",
		}},
	})
	if err != nil {
		t.Fatalf("record failed health: %v", err)
	}
	if failed.QuarantineState != model.NodeQuarantineStateQuarantined || failed.QuarantineReason != model.NodeQuarantineReasonDNSHardFail || failed.QuarantineExpiresAt == nil {
		t.Fatalf("expected DNS hard fail quarantine, got %+v", failed)
	}
	if !failed.ObservedOnly {
		t.Fatalf("node deep health must be observe-only before repair automation is enabled")
	}
	if len(failed.RecoveryConditions) == 0 {
		t.Fatalf("expected recovery conditions")
	}

	recovered, err := s.RecordNodeDeepHealthResult(model.NodeDeepHealthResult{
		NodeUpdaterID:   "nodeupdater_dns",
		ClusterNodeName: "node-a",
		Checks: []model.NodeDeepHealthCheck{{
			Name:   model.NodeDeepHealthCheckPodDNSToKubeDNSService,
			Status: model.NodeDeepHealthStatusPass,
		}},
	})
	if err != nil {
		t.Fatalf("record recovered health: %v", err)
	}
	if recovered.QuarantineState != model.NodeQuarantineStateClear || recovered.QuarantineReason != "" || recovered.QuarantineExpiresAt != nil {
		t.Fatalf("expected clear quarantine after passing hard check, got %+v", recovered)
	}
}

func TestRecordNodeDeepHealthDetectsStaleManagedIptablesIncidentShape(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	result, err := s.RecordNodeDeepHealthResult(model.NodeDeepHealthResult{
		NodeUpdaterID:   "nodeupdater_iptables",
		ClusterNodeName: "node-a",
		Checks: []model.NodeDeepHealthCheck{{
			Name:     model.NodeDeepHealthCheckManagedIptablesStale,
			Status:   model.NodeDeepHealthStatusFail,
			HardFail: true,
			Observed: "-A PREROUTING -d 10.43.0.10/32 -p udp --dport 53 -j DNAT --to-destination 10.42.8.1:53",
			Evidence: map[string]string{
				"suspect_rules": "1",
			},
		}},
	})
	if err != nil {
		t.Fatalf("record stale iptables health: %v", err)
	}
	if result.QuarantineState != model.NodeQuarantineStateQuarantined || result.QuarantineReason != model.NodeQuarantineReasonIptablesHardFail {
		t.Fatalf("expected iptables hard fail quarantine, got %+v", result)
	}
}
