package releaseguardian

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

func TestObserveSealedRecoveryPair(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	key := Key{Component: "api", Group: "api"}
	history, prior, priorReceipt, predecessor := guardianStableFixture(t, key, now)
	forward, current, receipt := sealedRecoverySuccessor(t, guardianCandidateFixture(t, key, predecessor, priorReceipt.TopDigest, []byte(history["forward.json"]), now))
	immutable := true
	state, _, err := declarativerelease.NewMonitorState(current, declarativerelease.MonitorState{}, true, "", now)
	if err != nil {
		t.Fatal(err)
	}
	newMap := func(name string, monitor declarativerelease.MonitorRecord, data map[string]string) *corev1.ConfigMap {
		return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "fugue-system",
			UID: types.UID(name + "-uid"), ResourceVersion: "10",
			Labels: map[string]string{"app.kubernetes.io/managed-by": "fugue-declarative-release", "fugue.pro/component": key.Component, "fugue.pro/config-sha": monitor.ConfigSHA}},
			Data: data}
	}
	stateJSON, _ := declarativerelease.CanonicalJSON(state)
	currentName := monitorRecordNameFromDigest(key.Component, current.RecordDigest)
	pointer := newMap("fugue-release-monitor-api", current, map[string]string{"recordName": currentName, "state.json": string(stateJSON)})
	forwardMap := newMap(currentName, current, forward)
	priorMap := newMap(monitorRecordNameFromDigest(key.Component, prior.RecordDigest), prior, history)
	forwardMap.Immutable, priorMap.Immutable = &immutable, &immutable

	changed := make(map[string][]byte, len(history))
	for k, v := range history {
		changed[k] = []byte(v)
	}
	var prepared declarativerelease.ExecutionPlan
	if err := decodeStrict(changed["execution-plan.json"], &prepared); err != nil {
		t.Fatal(err)
	}
	prepared.PreparedAt, prepared.PlanDigest = now.Add(time.Second).Format(time.RFC3339Nano), ""
	raw, _ := declarativerelease.CanonicalJSON(prepared)
	prepared.PlanDigest = digest(raw)
	changed["execution-plan.json"], _ = declarativerelease.CanonicalJSON(prepared)
	duplicateData, alternate, _ := sealedRecoverySuccessor(t, changed)
	duplicate := newMap(monitorRecordNameFromDigest(key.Component, alternate.RecordDigest), alternate, duplicateData)
	duplicate.Immutable = &immutable

	for _, tc := range []struct {
		name, want string
		edit       func(*fake.Clientset, *corev1.ConfigMap, *corev1.ConfigMap, *corev1.ConfigMap)
	}{
		{name: "exact immutable records"},
		{name: "missing predecessor", want: "no sealed record", edit: func(_ *fake.Clientset, _, _, p *corev1.ConfigMap) {
			delete(p.Labels, "fugue.pro/config-sha")
		}},
		{name: "mutable predecessor", want: "invalid sealed record", edit: func(_ *fake.Clientset, _, _, p *corev1.ConfigMap) {
			p.Immutable = nil
		}},
		{name: "tampered rollback manifest", want: "does not bind", edit: func(_ *fake.Clientset, _, f, _ *corev1.ConfigMap) {
			f.Data["lkg.json"] = f.Data["forward.json"]
		}},
		{name: "record content differs from pointer", want: "differs from its sealed record", edit: func(_ *fake.Clientset, p, _, h *corev1.ConfigMap) {
			p.Data["recordName"] = h.Name
		}},
		{name: "pointer reports rollback", want: "rolled-back", edit: func(_ *fake.Clientset, p, _, _ *corev1.ConfigMap) {
			rolled := state
			rolled.RollbackStatus = "lkg-restored"
			raw, _ := declarativerelease.CanonicalJSON(rolled)
			p.Data["state.json"] = string(raw)
		}},
		{name: "too much matching history", want: "observation bound", edit: func(c *fake.Clientset, _, _, _ *corev1.ConfigMap) {
			c.PrependReactor("list", "configmaps", func(kubetesting.Action) (bool, runtime.Object, error) {
				return true, &corev1.ConfigMapList{ListMeta: metav1.ListMeta{Continue: "too-many"}}, nil
			})
		}},
		{name: "pointer changes mid-read", want: "changed during observation", edit: func(c *fake.Clientset, p, _, _ *corev1.ConfigMap) {
			reads := 0
			c.PrependReactor("get", "configmaps", func(action kubetesting.Action) (bool, runtime.Object, error) {
				if action.(kubetesting.GetAction).GetName() == p.Name {
					reads++
					if reads == 2 {
						latest := p.DeepCopy()
						latest.ResourceVersion = "11"
						return true, latest, nil
					}
				}
				return false, nil, nil
			})
		}},
		{name: "predecessor recreated with same resource version", want: "changed during observation", edit: func(c *fake.Clientset, _, _, h *corev1.ConfigMap) {
			c.PrependReactor("get", "configmaps", func(action kubetesting.Action) (bool, runtime.Object, error) {
				if action.(kubetesting.GetAction).GetName() != h.Name {
					return false, nil, nil
				}
				latest := h.DeepCopy()
				latest.UID = "recreated-uid"
				return true, latest, nil
			})
		}},
		{name: "exact predecessor is ambiguous", want: "multiple sealed records", edit: func(c *fake.Clientset, _, _, h *corev1.ConfigMap) {
			c.PrependReactor("list", "configmaps", func(kubetesting.Action) (bool, runtime.Object, error) {
				return true, &corev1.ConfigMapList{Items: []corev1.ConfigMap{*h, *duplicate}}, nil
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, f, h := pointer.DeepCopy(), forwardMap.DeepCopy(), priorMap.DeepCopy()
			client := fake.NewSimpleClientset()
			if tc.edit != nil {
				tc.edit(client, p, f, h)
			}
			for _, object := range []*corev1.ConfigMap{p, f, h} {
				if err := client.Tracker().Add(object); err != nil {
					t.Fatal(err)
				}
			}
			pair, err := ObserveSealedRecoveryPair(context.Background(), client.CoreV1(), p.Namespace, key.Component)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) || pair != (SealedRecoveryPair{}) {
					t.Fatalf("pair=%+v err=%v, want %q with no evidence", pair, err, tc.want)
				}
			} else if err != nil || pair.Pointer != recoveryRef(p) || pair.Forward.Receipt != receipt || pair.Rollback.Receipt != priorReceipt ||
				pair.Forward.Monitor != current || pair.Rollback.Monitor != prior {
				t.Fatalf("pair=%+v err=%v", pair, err)
			}
			for _, action := range client.Actions() {
				if action.GetResource().Resource != "configmaps" || action.GetNamespace() != p.Namespace ||
					(action.GetVerb() != "get" && action.GetVerb() != "list") {
					t.Fatalf("unexpected recovery provider request: %v", action)
				}
				if list, ok := action.(kubetesting.ListAction); ok &&
					!strings.Contains(list.GetListRestrictions().Labels.String(), "fugue.pro/config-sha="+predecessor.ConfigSHA) {
					t.Fatalf("unscoped rollback history request: %v", list)
				}
			}
		})
	}
}

func sealedRecoverySuccessor(t *testing.T, files map[string][]byte) (map[string]string, declarativerelease.MonitorRecord, declarativerelease.ArtifactReceipt) {
	t.Helper()
	plan, err := declarativerelease.DecodePlan(bytes.NewReader(files["release-plan.json"]))
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(files["artifact-receipt.json"]))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := declarativerelease.DecodeRecordedExecutionPlan(bytes.NewReader(files["execution-plan.json"]), plan, files["forward.json"], files["lkg.json"])
	if err != nil {
		t.Fatal(err)
	}
	terminal := declarativerelease.ExecutionResult{APIVersion: declarativerelease.ExecutionPlanAPIVersion, Kind: declarativerelease.ExecutionResultKind,
		Component: prepared.Component, ConfigSHA: prepared.ConfigSHA, ExecutionPlanDigest: prepared.PlanDigest, Status: "verified",
		Reason: "forward-verified", ForwardApplyCount: 1, Final: guardianObservation(prepared.Forward)}
	raw, _ := declarativerelease.CanonicalJSON(terminal)
	terminal.ReceiptDigest = digest(raw)
	record, err := declarativerelease.NewMonitorRecord(plan, artifact, prepared, terminal, files["forward.json"], files["lkg.json"])
	if err != nil {
		t.Fatal(err)
	}
	data := map[string]string{}
	for k, v := range files {
		data[k] = string(v)
	}
	raw, _ = declarativerelease.CanonicalJSON(terminal)
	data["terminal-result.json"] = string(raw)
	raw, _ = declarativerelease.CanonicalJSON(record)
	data["record.json"] = string(raw)
	return data, record, artifact
}
