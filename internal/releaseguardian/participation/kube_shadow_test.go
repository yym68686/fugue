package participation

import (
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
)

func TestKubernetesShadowWaitsForEvidenceItCannotCollect(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	adopt := true
	policy := productioncontrol.ParticipationPolicy{Namespace: "fixture", Members: []productioncontrol.ParticipationMember{{
		Node: "node-a", NodeUID: "uid-a", Group: "cell-a", Component: "release-a", Front: "front-a",
		Workers: productioncontrol.ParticipationWorkers{A: "worker-a", B: "worker-b"}, Reason: "network_unavailable",
		StableSeconds: 300, AdoptHold: &adopt,
	}}}
	for _, tc := range []struct {
		name       string
		ready      bool
		serving    bool
		baseline   int
		prior      ParticipationPhase
		wantPhase  ParticipationPhase
		wantAction ParticipationAction
		waiting    string
	}{
		{"ready and serving waits without proposing", true, true, 0, ParticipationOffline, "", "", "independent_dns_and_heartbeat_observation_required"},
		{"offline does not need transport DNS", false, true, 2, "", ParticipationOffline, ParticipationHold, ""},
		{"ready without selected service can preview recovery start", true, false, 2, ParticipationOffline, ParticipationRecovering, ParticipationHold, ""},
		{"elapsed stabilization is not readmission", true, false, 2, ParticipationRecovering, ParticipationRecovering, ParticipationReviewRecovery, "independent_front_worker_and_network_recovery_observation_required"},
		{"no reader baseline", false, true, 0, "", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := participationFixtureSnapshot(t, policy, now, tc.prior, time.Hour)
			s.ValidatedReaderBaseline = 99 // Raw captured objects cannot determine this value.
			if tc.ready {
				s.Node.Status.Conditions[0].Status = corev1.ConditionTrue
			}
			member := MemberObservation{Readiness: s.Node.Status.Conditions[0].Status}
			if tc.serving {
				member.Service = &ServingServiceObservation{Address: "192.0.2.4"}
			}
			var readers *ReaderObservation
			if tc.baseline > 0 {
				readers = &ReaderObservation{Baseline: tc.baseline, Namespace: policy.Namespace, Group: policy.Members[0].Group}
			}
			got, err := evaluateKubernetesShadow(policy, "node-a", memberCapture{report: member, snapshot: s}, readers, nil, now)
			if tc.waiting == "" && tc.wantAction == "" {
				if err == nil || got.Preview != nil {
					t.Fatalf("unverified reader baseline should fail closed: %+v, %v", got, err)
				}
				return
			}
			if err != nil || got.Waiting != tc.waiting || got.Member.DNSObserved {
				t.Fatalf("shadow: %+v, %v", got, err)
			}
			if tc.wantAction == "" {
				if got.Preview != nil {
					t.Fatalf("missing DNS evidence produced a preview: %+v", got.Preview)
				}
				return
			}
			if got.Preview == nil || got.Preview.Phase != tc.wantPhase || got.Preview.Action != tc.wantAction ||
				(got.Preview.HoldPlan != nil) != (tc.wantAction == ParticipationHold) {
				t.Fatalf("unexpected shadow decision: %+v", got.Preview)
			}
		})
	}
}
