package participation

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"os/exec"
	"strings"
	"testing"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParticipationPreviewMatchesLegacyPhaseAndRecoveryBoundary(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is required for cross-runtime migration parity")
	}
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	member := productioncontrol.ParticipationMember{Node: "node-a", NodeUID: "uid-a", Group: "cell-a",
		Component: "release-a", Front: "front-a", Workers: productioncontrol.ParticipationWorkers{A: "worker-a", B: "worker-b"},
		Reason: "network_unavailable", StableSeconds: 300}
	adopt := true
	member.AdoptHold = &adopt
	policy := productioncontrol.ParticipationPolicy{Namespace: "fixture", Members: []productioncontrol.ParticipationMember{member}}
	for _, tc := range []struct {
		name      string
		ready     bool
		transport bool
		heartbeat bool
		excluded  bool
		state     ParticipationPhase
		stateAge  time.Duration
		phase     ParticipationPhase
		action    ParticipationAction
	}{
		{"offline first observation", false, false, false, false, "", 0, ParticipationOffline, ParticipationHold},
		{"offline no duplicate", false, false, false, false, ParticipationOffline, time.Hour, ParticipationOffline, ParticipationNone},
		{"unverified exclusion", true, true, false, false, "", 0, ParticipationActive, ParticipationNone},
		{"verified DNS exclusion", true, true, false, true, "", 0, ParticipationOffline, ParticipationHold},
		{"verified returning member", true, true, true, true, ParticipationOffline, time.Hour, ParticipationRecovering, ParticipationHold},
		{"before stabilization", true, true, true, true, ParticipationRecovering, time.Minute, ParticipationRecovering, ParticipationNone},
		{"after stabilization", true, true, true, true, ParticipationRecovering, time.Hour, ParticipationRecovering, ParticipationReviewRecovery},
		{"after stabilization without transport", true, false, false, false, ParticipationRecovering, time.Hour, ParticipationRecovering, ParticipationReviewRecovery},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := participationFixtureSnapshot(t, policy, now, tc.state, tc.stateAge)
			if tc.ready {
				s.Node.Status.Conditions[0].Status = corev1.ConditionTrue
			}
			if tc.transport {
				s.Transport = &ParticipationTransport{Node: member.Node, Group: member.Group, PublicIPv4: "192.0.2.4",
					ExternalIPs: []string{"192.0.2.4"}, DNSState: "suspended", DNSReason: member.Reason, HeartbeatFresh: tc.heartbeat}
				if tc.excluded {
					s.Transport.DNSServingEvidence = "excluded_from_verified_full"
				}
			}
			preview, err := PreviewParticipation(policy, member.Node, s, now)
			if err != nil || preview.Phase != tc.phase || preview.Action != tc.action {
				t.Fatalf("native preview %+v, %v; wanted %s/%s", preview, err, tc.phase, tc.action)
			}
			// Drive the actual Python dry-run against the same observations.
			// Run only the legacy dry-run, then render the isolated hold with
			// an intercepted patch. No provider client or recovery path runs.
			in, err := json.Marshal(struct {
				Policy   productioncontrol.ParticipationPolicy
				Snapshot ParticipationSnapshot
				Now      time.Time
			}{policy, s, now})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-B", "-c", legacyParticipationPreviewFixture)
			cmd.Dir, cmd.Stdin = "../../..", bytes.NewReader(in)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("legacy dry-run failed: %v: %s", err, out)
			}
			var actual struct {
				Phase  ParticipationPhase
				Review bool
				Patch  []struct {
					Op, Path string
					Value    json.RawMessage
				}
			}
			if err := json.Unmarshal(out, &actual); err != nil || actual.Phase != preview.Phase ||
				actual.Review != (preview.Action == ParticipationReviewRecovery) {
				t.Fatalf("Go and Python differ: legacy=%s native=%+v error=%v", out, preview, err)
			}
			if preview.HoldPlan == nil {
				if len(actual.Patch) != 0 {
					t.Fatal("legacy proposed an unexpected hold patch")
				}
				return
			}
			if len(actual.Patch) != 3 || actual.Patch[0].Op != "test" || actual.Patch[1].Op != "test" ||
				actual.Patch[0].Path != "/metadata/uid" || actual.Patch[1].Path != "/metadata/resourceVersion" ||
				actual.Patch[2].Op != "add" || actual.Patch[2].Path != "/metadata/annotations" {
				t.Fatalf("legacy does not propose an exact CAS: %s", out)
			}
			var uid, rv string
			var after map[string]string
			if json.Unmarshal(actual.Patch[0].Value, &uid) != nil || json.Unmarshal(actual.Patch[1].Value, &rv) != nil ||
				json.Unmarshal(actual.Patch[2].Value, &after) != nil || uid != preview.NodeUID || rv != preview.ResourceVersion ||
				!maps.Equal(after, preview.HoldPlan.After) || !maps.Equal(preview.HoldPlan.Before, s.Node.Annotations) {
				t.Fatalf("legacy patch and native proposal differ: %s; native=%+v", out, preview.HoldPlan)
			}
		})
	}
}

func TestReplayParticipationBindsApprovedTopologyAndStaysAdvisory(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	adopt := true
	policy := productioncontrol.ParticipationPolicy{Generation: 7, Namespace: "fixture", Members: []productioncontrol.ParticipationMember{{
		Node: "node-a", NodeUID: "uid-a", Group: "cell-a", Component: "release-a", Front: "front-a",
		Workers: productioncontrol.ParticipationWorkers{A: "worker-a", B: "worker-b"}, Reason: "network_unavailable",
		AdoptHold: &adopt, StableSeconds: 300,
	}}}
	snapshot := participationFixtureSnapshot(t, policy, now, "", 0)
	before := maps.Clone(snapshot.Node.Annotations)
	head := productioncontrol.VerifiedParticipationTopology{
		Version:      productioncontrol.Version{Generation: policy.Generation, Digest: digest([]byte("approved topology"))},
		IntentDigest: digest([]byte("intent")), ApprovalDigest: digest([]byte("approval")),
		RecordedAt: now.Add(-time.Minute), Policy: policy,
	}
	in := ReplayInput{Schema: ReplaySchema, Cluster: "cluster-a", Topology: ReplayVersion{Generation: head.Version.Generation, Digest: head.Version.Digest},
		ObservedAt: now.Add(-time.Second), Observations: &snapshot}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	report, err := ReplayParticipation("cluster-a", "node-a", head, raw, now)
	if err != nil || report.Kind != "untrusted_snapshot_replay" || report.ExecutionEvidence ||
		report.Preview.Action != ParticipationHold || report.Preview.HoldPlan == nil ||
		!maps.Equal(snapshot.Node.Annotations, before) || report.ObservationDigest != digest(raw) {
		t.Fatalf("advisory replay was not correctly bound: %+v, %v", report, err)
	}

	changedTopology := in
	changedTopology.Topology.Digest = digest([]byte("different topology"))
	changed, _ := json.Marshal(changedTopology)
	if _, err := ReplayParticipation("cluster-a", "node-a", head, changed, now); err == nil {
		t.Fatal("replay accepted a topology other than the authenticated head")
	}
	duplicateField := strings.Replace(string(raw), `"schema":"`+ReplaySchema+`"`,
		`"schema":"`+ReplaySchema+`","schema":"`+ReplaySchema+`"`, 1)
	if _, err := ReplayParticipation("cluster-a", "node-a", head, []byte(duplicateField), now); err == nil {
		t.Fatal("replay accepted a duplicate schema field")
	}
	if _, err := ReplayParticipation("other-cluster", "node-a", head, raw, now); err == nil {
		t.Fatal("replay accepted an observation from another journal cluster")
	}
}

func TestParticipationPreviewPreservesHoldsOnUnprovenOwnership(t *testing.T) {
	now := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	adopt := false
	policy := productioncontrol.ParticipationPolicy{Namespace: "fixture", Members: []productioncontrol.ParticipationMember{{
		Node: "node-a", NodeUID: "uid-a", Group: "cell-a", Component: "release-a", Front: "front-a",
		Workers: productioncontrol.ParticipationWorkers{A: "worker-a", B: "worker-b"}, Reason: "network_unavailable",
		StableSeconds: 300, AdoptHold: &adopt,
	}}}
	for name, modify := range map[string]func(*ParticipationSnapshot){
		"replacement Node":      func(s *ParticipationSnapshot) { s.Node.UID = "foreign" },
		"inventory drift":       func(s *ParticipationSnapshot) { s.Inventory.Items[0].ResourceVersion = "99" },
		"no alternative member": func(s *ParticipationSnapshot) { s.Inventory.Items[1].Labels["fugue.io/schedulable"] = "false" },
		"no reader baseline":    func(s *ParticipationSnapshot) { s.ValidatedReaderBaseline = 0 },
		"manual hold": func(s *ParticipationSnapshot) {
			s.Node.Annotations[edgecohort.ReleaseHoldAnnotation] = `{"schema":"fugue.edge-release-hold/v1","node_uid":"uid-a","edge_group_id":"cell-a","reason":"maintenance"}`
		},
		"retained workload drift": func(s *ParticipationSnapshot) {
			s.Retained[0].Spec.UpdateStrategy.Type = appsv1.RollingUpdateDaemonSetStrategyType
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := participationFixtureSnapshot(t, policy, now, "", 0)
			modify(&s)
			if p, err := PreviewParticipation(policy, "node-a", s, now); err == nil || p.Action == ParticipationHold {
				t.Fatalf("unproven ownership produced an action: %+v, %v", p, err)
			}
		})
	}
}

func participationFixtureSnapshot(t *testing.T, policy productioncontrol.ParticipationPolicy, now time.Time, state ParticipationPhase, age time.Duration) ParticipationSnapshot {
	t.Helper()
	m := policy.Members[0]
	n := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: m.Node, UID: "uid-a", ResourceVersion: "10",
		Labels: map[string]string{participationRole: "true", participationGroup: m.Group}, Annotations: map[string]string{"fixture": "preserve"}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown,
			LastTransitionTime: metav1.NewTime(now.Add(-2 * time.Hour))}}}}
	if state != "" {
		identity, _ := json.Marshal(map[string]string{"node": m.Node, "node_uid": m.NodeUID, "group": m.Group, "component": m.Component})
		raw, _ := json.Marshal(memberParticipationState{participationStateSchema, digest(identity), state, now.Add(-age)})
		n.Annotations[participationAnnotation] = string(raw)
		raw, _ = json.Marshal(edgecohort.ReleaseHold{Schema: edgecohort.ReleaseHoldSchema, NodeUID: m.NodeUID, EdgeGroupID: m.Group, Reason: m.Reason})
		n.Annotations[edgecohort.ReleaseHoldAnnotation] = string(raw)
	}
	ref := n.DeepCopy()
	ref.Name, ref.UID, ref.Annotations = "ref-node", "ref-uid", nil
	s := ParticipationSnapshot{Node: n, Inventory: &corev1.NodeList{Items: []corev1.Node{*n.DeepCopy(), *ref}}, ValidatedReaderBaseline: 2}
	for _, name := range []string{m.Front, m.Workers.A, m.Workers.B} {
		s.Retained = append(s.Retained, appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: policy.Namespace, UID: "workload-uid"},
			Spec: appsv1.DaemonSetSpec{UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{participationGroup: m.Group}}}}})
	}
	return s
}

const legacyParticipationPreviewFixture = `
import json, sys
from contextlib import ExitStack
from unittest.mock import patch
from scripts import reconcile_node_participation as legacy

d = json.load(sys.stdin)
p, s = d['Policy'], d['Snapshot']
member = p['members'][0]
t = s['transport']
cluster_read = None if t is None else (object(), {'spec': {'externalIPs': t['externalIPs']}})
edge_read = None if t is None else {
    'public_ipv4': t['publicIPv4'],
    'observation': {'heartbeat_fresh': t['heartbeatFresh'], 'dns_admission_intent': {
        'state': t['dnsState'], 'reason': t['dnsReason'], 'serving_evidence': t['dnsServingEvidence']}}}

class Fixture(legacy.Participant):
    patch = None
    def get(self, kind, name):
        if kind == 'node' and name == member['node']:
            return s['node']
        if kind == 'daemonset':
            return next(item for item in s['retained'] if item['metadata']['name'] == name)
        raise AssertionError((kind, name))
    def kube(self, *args, **kwargs):
        if args[:2] == ('get', 'nodes'):
            return json.dumps(s['inventory'])
        assert args[:3] == ('patch', 'node', member['node']) and self.patch is None, args
        self.patch = json.loads(args[args.index('-p') + 1])
        return json.dumps(s['node'])
    def readers(self):
        return s['validatedReaderBaseline']
    def public_service(self):
        return cluster_read
    def node_local_snapshot(self, public):
        return None
    def recovery_config(self):
        return {}

with ExitStack() as stack:
    stack.enter_context(patch.object(legacy, 'now', return_value=legacy.timestamp(d['Now'])))
    stack.enter_context(patch.object(legacy, 'read_edge_node', return_value=edge_read))
    recovery = stack.enter_context(patch.object(legacy, 'reconcile', return_value={'verified': False}))
    runner = Fixture(p, member)
    result = runner.run(False)
    assert result['changed'] is False
    node = s['node']
    raw = node['metadata']['annotations'].get(legacy.STATE)
    prior_phase = json.loads(raw)['phase'] if raw is not None else None
    if result['state'] in ('offline', 'recovering') and prior_phase != result['state']:
        runner.transition_node(node, result['state'])
    print(json.dumps(dict(phase=result['state'], review=recovery.called, patch=runner.patch)))
`
