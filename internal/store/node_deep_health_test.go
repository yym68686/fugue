package store

import (
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestRecordNodeDeepHealthObserveOnlyHardFailDegradesWithoutQuarantine(t *testing.T) {
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
	if !failed.ObservedOnly {
		t.Fatalf("node deep health must be observe-only before repair automation is enabled")
	}
	if failed.QuarantineState != model.NodeQuarantineStateDegraded || failed.QuarantineReason != "warning_or_soft_fail" || failed.QuarantineExpiresAt != nil {
		t.Fatalf("expected observed-only DNS hard fail to degrade without quarantine, got %+v", failed)
	}
	if len(failed.RecoveryConditions) == 0 {
		t.Fatalf("expected recovery conditions for degraded result")
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

func TestNodeDeepHealthDecisionQuarantinesOnlyWhenEnforced(t *testing.T) {
	t.Parallel()

	checks := []model.NodeDeepHealthCheck{{
		Name:     model.NodeDeepHealthCheckPodDNSToKubeDNSService,
		Status:   model.NodeDeepHealthStatusFail,
		HardFail: true,
		Observed: "lookup timeout",
	}}
	_, observedState, observedReason := nodeDeepHealthDecision(checks, true)
	if observedState != model.NodeQuarantineStateDegraded || observedReason != "warning_or_soft_fail" {
		t.Fatalf("expected observed-only hard fail to degrade, got state=%s reason=%s", observedState, observedReason)
	}
	_, enforcedState, enforcedReason := nodeDeepHealthDecision(checks, false)
	if enforcedState != model.NodeQuarantineStateQuarantined || enforcedReason != model.NodeQuarantineReasonDNSHardFail {
		t.Fatalf("expected enforced hard fail to quarantine, got state=%s reason=%s", enforcedState, enforcedReason)
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
	if result.QuarantineState != model.NodeQuarantineStateDegraded || result.QuarantineReason != "warning_or_soft_fail" || result.QuarantineExpiresAt != nil {
		t.Fatalf("expected observed-only iptables hard fail to degrade without quarantine, got %+v", result)
	}
}
