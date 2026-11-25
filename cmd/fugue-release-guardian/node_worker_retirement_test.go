package main

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestWorkerRetirementCountsRetiredFrontSocketsAndIgnoresTimeWait(t *testing.T) {
	raw := ` sl  local_address rem_address st tx_queue rx_queue
 0: 00000000:46A0 00000000:0000 0A 00000000
 1: 00000000:480B 00000000:0000 0A 00000000
 2: 0010007F:480B 0210007F:DC43 06 00000000
 3: 0010007F:1234 0210007F:DC44 01 00000000
 sl  local_address remote_address st tx_queue rx_queue
 0: 00000000000000000000000000000000:480B 00000000000000000000000000000000:AAF0 01 00000000
 1: 00000000000000000000000000000000:46A0 00000000000000000000000000000000:AAF1 08 00000000
`
	n, err := workerSlotSockets([]byte(raw), 18080, 18443)
	if err != nil || n != 2 {
		t.Fatalf("count=%d, err=%v", n, err)
	}
	if _, err := workerSlotSockets([]byte(strings.Split(raw, " sl  local_address remote_address")[0]), 18080, 18443); err == nil {
		t.Fatal("accepted a partial IPv6 observation")
	}
	if _, err := workerSlotSockets([]byte(strings.Replace(raw, "480B 00000000:0000 0A", "480B 00000000:0000 00", 1)), 18080, 18443); err == nil {
		t.Fatal("accepted a missing listener / invalid kernel state")
	}
}

func TestWorkerRetirementKeepsEveryUnrelatedPlacementAndRequiresReceipt(t *testing.T) {
	original := corev1.PodSpec{NodeSelector: map[string]string{"country": "us"},
		Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"primary"}}}},
				{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"peer"}}}},
			}}}}}
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", UID: "daemon-a", ResourceVersion: "71", Generation: 9},
		Spec: appsv1.DaemonSetSpec{UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				"fugue.io/edge-slot": "a", "fugue.io/edge-group-id": "edge-group-test",
			}}, Spec: original}}}
	j := edgegroupfront.NodeWorkerJournal{Recipient: edgegroupfront.NativeRecipient{Group: "edge-group-test", Node: "target"},
		Expected: edgegroupfront.ActivationState{ActiveSlot: "a"}, PredecessorWorkerUID: "old-pod", Digest: "sha256:" + strings.Repeat("a", 64)}
	p := workerRetirementPlan{workerRetirementSchema, j.Digest, ds.Name, ds.UID, ds.Generation}
	deadline := time.Unix(900, 0).UTC()
	if d, err := p.checkDS(ds, j, deadline); err != nil || d != nil {
		t.Fatalf("fresh DS: %+v %v", d, err)
	}
	next := ds.DeepCopy()
	if err := excludeWorkerRetirementNode(&next.Spec.Template.Spec, j.Recipient.Node); err != nil {
		t.Fatal(err)
	}
	if !workerNodeExcluded(next.Spec.Template.Spec, j.Recipient.Node) || workerNodeExcluded(next.Spec.Template.Spec, "peer") {
		t.Fatal("not scoped to target")
	}
	copy := next.Spec.Template.Spec.DeepCopy()
	for i := range copy.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		term := &copy.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i]
		term.MatchFields = term.MatchFields[:len(term.MatchFields)-1]
		if len(term.MatchFields) == 0 {
			term.MatchFields = nil
		}
	}
	if !reflect.DeepEqual(copy, &original) {
		t.Fatal("changed other scheduling constraints")
	}
	d := workerRetirementDecision{p, j.Recipient.Node, j.PredecessorWorkerUID, deadline, deadline, "deadline", 2}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	next.Generation++
	if _, err := p.checkDS(next, j, deadline); err == nil {
		t.Fatal("resumed exclusion that lacked the durable retirement decision")
	}
	next.Annotations = map[string]string{workerRetirementKey(j.Recipient.Node): string(raw)}
	if got, err := p.checkDS(next, j, deadline); err != nil || got == nil || *got != d {
		t.Fatalf("lost-write recovery failed: %+v %v", got, err)
	}
	if _, err := p.checkDS(next, j, deadline.Add(time.Second)); err == nil {
		t.Fatal("resumed a different deadline")
	}
}

func TestWorkerRetirementConditions(t *testing.T) {
	end := time.Unix(600, 0)
	for _, tt := range []struct {
		seconds int64
		sockets int
		drained bool
		want    string
	}{
		{40, 0, false, ""}, {40, 1, true, ""}, {40, 0, true, "empty"}, {599, 1, false, ""}, {600, 1, false, "deadline"},
		{599, -1, false, ""}, {600, -1, false, "deadline"},
	} {
		if got := retirementOutcome(time.Unix(tt.seconds, 0), end, tt.sockets, tt.drained); got != tt.want {
			t.Errorf("retirementOutcome(%+v)=%q", tt, got)
		}
	}
}

func TestWorkerRetirementReceiptRequiresRealListenerClosureAndKeepsPeer(t *testing.T) {
	a, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	const ns = "system"
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "old-slot", UID: "old-controller", Generation: 8},
		Status: appsv1.DaemonSetStatus{ObservedGeneration: 8}}
	selected := true
	makePod := func(name, uid, node string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid), ResourceVersion: "5", OwnerReferences: []metav1.OwnerReference{
			{APIVersion: "apps/v1", Kind: "DaemonSet", Name: ds.Name, UID: ds.UID, Controller: &selected},
		}}, Spec: corev1.PodSpec{NodeName: node}}
	}
	client := fake.NewSimpleClientset(makePod("old", "old-uid", "target"), makePod("peer", "peer-uid", "other"))
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		cm := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		cm.UID, cm.ResourceVersion = "retirement-receipt", "11"
		return false, nil, nil
	})
	activator := &frontAuthorityActivator{client: client, config: frontAuthorityConfig{Namespace: ns}}
	j := edgegroupfront.NodeWorkerJournal{Recipient: edgegroupfront.NativeRecipient{Group: "edge-group-test", Node: "target"},
		Digest: "sha256:" + strings.Repeat("b", 64), PredecessorWorkerUID: "old-uid"}
	decision := workerRetirementDecision{Plan: workerRetirementPlan{workerRetirementSchema, j.Digest, ds.Name, ds.UID, 7},
		Node: "target", WorkerUID: "old-uid", Initiated: time.Unix(600, 0).UTC(), Deadline: time.Unix(600, 0).UTC(), Outcome: "deadline"}
	ctx := context.Background()
	ports := []uint16{uint16(a.Addr().(*net.TCPAddr).Port), uint16(b.Addr().(*net.TCPAddr).Port)}
	call := func(want bool) {
		t.Helper()
		if got, err := activator.confirmWorkerRetirement(ctx, j, ds, decision, "127.0.0.1", ports...); err != nil || got != want {
			t.Fatalf("confirmation=%t, error=%v, want %t", got, err, want)
		}
	}
	call(false) // exact old Pod removed
	if _, err := client.CoreV1().Pods(ns).Get(ctx, "old", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old Pod was not deleted: %v", err)
	}
	call(false) // API absence cannot substitute for a live listener check
	a.Close()
	call(false) // both slots' ports must be released
	b.Close()
	call(true)
	call(true) // lost result reply does not cause another deletion
	if _, err := client.CoreV1().Pods(ns).Get(ctx, "peer", metav1.GetOptions{}); err != nil {
		t.Fatalf("peer changed: %v", err)
	}
}
