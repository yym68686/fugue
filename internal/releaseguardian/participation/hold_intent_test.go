package participation

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
)

func TestDraftHoldBindsExactRollbackAndJournalHead(t *testing.T) {
	const cluster, node, uid, group = "fixture", "node-a", "uid-a", "cell-a"
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	adopt := false
	topology := productioncontrol.VerifiedParticipationTopology{
		Version:        productioncontrol.Version{Generation: 2, Digest: digest([]byte("topology"))},
		ApprovalDigest: digest([]byte("approval")), RecordedAt: now.Add(-time.Hour),
		Policy: productioncontrol.ParticipationPolicy{
			Schema: productioncontrol.BoundParticipationPolicySchema, Namespace: "fixture", Generation: 2,
			Kubernetes: &productioncontrol.ParticipationKubernetesBinding{
				Endpoint: "https://cluster.example.test", CADigest: digest([]byte("CA")), NamespaceUID: "fixture-ns",
			},
			Members: []productioncontrol.ParticipationMember{{
				Node: node, NodeUID: uid, Group: group, Component: "component-a", Front: "front-a",
				Workers: productioncontrol.ParticipationWorkers{A: "worker-a", B: "worker-b"},
				Reason:  "network_unavailable", AdoptHold: &adopt, StableSeconds: 300,
			}},
		},
	}
	for _, tc := range []struct {
		name   string
		change func(*productioncontrol.ResourceState, *KubernetesShadow)
	}{
		{"exact predecessor", nil},
		{"in-flight", func(h *productioncontrol.ResourceState, _ *KubernetesShadow) { h.Pending = digest([]byte("pending")) }},
		{"verdict still waiting", func(_ *productioncontrol.ResourceState, s *KubernetesShadow) { s.Waiting = "missing-evidence" }},
		{"replacement UID", func(_ *productioncontrol.ResourceState, s *KubernetesShadow) { s.Member.Node.UID = "some-other-uid" }},
		{"changed resource version", func(_ *productioncontrol.ResourceState, s *KubernetesShadow) { s.Member.Node.ResourceVersion = "12" }},
		{"different last verified state", func(h *productioncontrol.ResourceState, _ *KubernetesShadow) {
			h.LastVerified.Digest = digest([]byte("reset"))
		}},
		{"unrelated annotation change", func(_ *productioncontrol.ResourceState, s *KubernetesShadow) {
			s.Preview.HoldPlan.After["fixture"] = "unapproved"
		}},
		{"missing owned hold", func(_ *productioncontrol.ResourceState, s *KubernetesShadow) {
			delete(s.Preview.HoldPlan.After, edgecohort.ReleaseHoldAnnotation)
		}},
		{"readmission", func(_ *productioncontrol.ResourceState, s *KubernetesShadow) {
			s.Preview.Action = ParticipationReviewRecovery
		}},
		{"foreign held annotation", func(_ *productioncontrol.ResourceState, s *KubernetesShadow) {
			s.Preview.HoldPlan.After[edgecohort.ReleaseHoldAnnotation] = "{}"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := participationFixtureSnapshot(t, topology.Policy, now, "", 0)
			report, err := evaluateKubernetesShadow(topology.Policy, node, memberCapture{report: MemberObservation{
				Node: describeObject(&input.Node), Readiness: input.Node.Status.Conditions[0].Status,
			}, snapshot: input}, &ReaderObservation{Namespace: "fixture", Group: group, Baseline: 2}, nil, now)
			if err != nil || report.Preview == nil || report.Preview.HoldPlan == nil {
				t.Fatalf("expected a hold proposal, got %+v, %v", report, err)
			}
			previous := productioncontrol.Version{Generation: 8, Digest: HoldStateDigest(node, uid, group, input.Node.Annotations)}
			h := productioncontrol.ResourceState{Baseline: productioncontrol.Baseline{
				Scope: HoldResource(cluster, node), Verified: previous, EvidenceDigest: digest([]byte("witness")),
			}, LastVerified: previous}
			if tc.change != nil {
				tc.change(&h, &report)
				if s, err := DraftHold(cluster, "request-a", node, topology, h, report); err == nil || len(s.Payload) != 0 {
					t.Fatalf("accepted invalid hold draft: %+v, %v", s.Intent, err)
				}
				return
			}
			s, err := DraftHold(cluster, "request-a", node, topology, h, report)
			if err != nil {
				t.Fatal(err)
			}
			if shown, err := InspectHold(s); err != nil || string(shown) != string(s.Payload) {
				t.Fatalf("inspection did not show the bound hold verbatim: %v", err)
			}
			misbound := s
			misbound.Intent.RollbackDigest = misbound.Intent.TargetDigest
			if _, err := InspectHold(misbound); err == nil {
				t.Fatal("inspection accepted a different rollback digest")
			}
			var body holdIntentPayload
			if err := json.Unmarshal(s.Payload, &body); err != nil {
				t.Fatal(err)
			}
			if s.Intent.VerifyPayload(s.Payload) != nil || s.Intent.Operation != productioncontrol.NodeParticipation ||
				s.Intent.ResourceID != h.Scope.ResourceID || s.Intent.Generation != 8 ||
				s.Intent.CurrentDigest != previous.Digest || s.Intent.RollbackDigest != previous.Digest ||
				s.Intent.TargetDigest != HoldStateDigest(node, uid, group, report.Preview.HoldPlan.After) ||
				body.Topology != topology.Version || body.TopologyApproval != topology.ApprovalDigest ||
				body.ResourceVersion != input.Node.ResourceVersion || body.Phase != ParticipationOffline ||
				len(body.From.Annotations) != 2 || body.From.Annotations[edgecohort.ReleaseHoldAnnotation] != nil ||
				len(body.To.Annotations) != 2 || body.To.Annotations[edgecohort.ReleaseHoldAnnotation] == nil {
				t.Fatalf("incorrect rollback or scope: %+v, %+v", s.Intent, body)
			}
			freshAt := now.Add(time.Minute)
			for _, change := range []struct {
				name       string
				observedAt time.Time
				edit       func(*productioncontrol.VerifiedParticipationTopology, *productioncontrol.ResourceState, *KubernetesShadow)
			}{
				{"fresh, same decision", freshAt, nil},
				{"stale shadow", now, func(*productioncontrol.VerifiedParticipationTopology, *productioncontrol.ResourceState, *KubernetesShadow) {
				}},
				{"future shadow", freshAt.Add(time.Second), func(*productioncontrol.VerifiedParticipationTopology, *productioncontrol.ResourceState, *KubernetesShadow) {
				}},
				{"updated topology approval", freshAt, func(p *productioncontrol.VerifiedParticipationTopology, _ *productioncontrol.ResourceState, _ *KubernetesShadow) {
					p.ApprovalDigest = digest([]byte("replaced approval"))
				}},
				{"moved last-known-good", freshAt, func(_ *productioncontrol.VerifiedParticipationTopology, head *productioncontrol.ResourceState, _ *KubernetesShadow) {
					head.LastVerified.Generation++
				}},
				{"exact live version moved", freshAt, func(_ *productioncontrol.VerifiedParticipationTopology, _ *productioncontrol.ResourceState, result *KubernetesShadow) {
					result.Member.Node.ResourceVersion = "11"
					result.Preview.ResourceVersion = "11"
				}},
			} {
				t.Run(change.name, func(t *testing.T) {
					next, err := evaluateKubernetesShadow(topology.Policy, node, memberCapture{report: MemberObservation{
						Node: describeObject(&input.Node), Readiness: input.Node.Status.Conditions[0].Status,
					}, snapshot: input}, &ReaderObservation{Namespace: "fixture", Group: group, Baseline: 2}, nil, change.observedAt)
					if err != nil {
						t.Fatal(err)
					}
					p, head := topology, h
					if change.edit != nil {
						change.edit(&p, &head, &next)
					}
					if got, err := HoldTarget(s); err != nil || got != node {
						t.Fatalf("target=%q: %v", got, err)
					}
					if err := PreflightHold(s, p, head, next, freshAt); (err == nil) != (change.edit == nil) {
						t.Fatalf("unexpected preflight result: %v", err)
					}
				})
			}
			s.Payload[0] ^= 1
			if s.Intent.VerifyPayload(s.Payload) == nil {
				t.Fatal("mutated payload still verifies")
			}
		})
	}
}
