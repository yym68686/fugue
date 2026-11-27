package releaseguardian

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestObserveNativeServingNeverSettlesFromPointerAlone(t *testing.T) {
	for _, mode := range []string{"candidate", "rollback", "pointer only", "wrong image", "foreign owner", "manifest drift", "stale route", "pointer changed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f := healthyPublicationFixture(t)
			draft, err := f.store.DraftNativeRelease(ctx, f.key, "isolated-cluster", "request-a", f.candidate)
			if err != nil {
				t.Fatal(err)
			}
			intentDigest, err := draft.Intent.Digest()
			if err != nil {
				t.Fatal(err)
			}
			claim := productioncontrol.Claim{Scope: productioncontrol.Scope{ClusterID: draft.Intent.ClusterID, ResourceID: draft.Intent.ResourceID},
				Fence: 1, IntentDigest: intentDigest, ApprovalDigest: testDigest}
			journal := &publicationJournalFixture{draft: draft, claim: claim}
			p := &NativePublisher{store: f.store, journal: journal, clusterID: draft.Intent.ClusterID,
				writer:    f.client.CoreV1().ConfigMaps(draft.Preview.Namespace),
				admission: publicationAdmissionFunc(func(context.Context, NativePublicationBoundary) error { return nil })}
			publication, err := p.Publish(ctx, "fixture-executor", intentDigest, testDigest)
			if err != nil || !publication.Submitted {
				t.Fatalf("publish: %+v %v", publication, err)
			}
			bundle, err := DecodeExecutionBundle(f.candidate, f.key)
			if err != nil {
				t.Fatal(err)
			}
			core := f.client.CoreV1()
			maps := core.ConfigMaps(draft.Preview.Namespace)
			pointer, err := maps.Get(ctx, desiredName(f.key), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pointer.ResourceVersion = "20"
			if _, err := maps.Update(ctx, pointer, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			servingRecord := draft.Preview.Record
			if mode == "rollback" {
				snapshot, loadErr := f.store.Load(ctx, f.key)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				if err := f.store.SetDesiredToLKG(ctx, snapshot); err != nil {
					t.Fatal(err)
				}
				servingRecord = f.stableGuardian
			}

			monitorData := f.stableData
			monitor := f.stableMonitor
			if mode != "rollback" && mode != "pointer only" {
				terminal := declarativerelease.ExecutionResult{
					APIVersion: declarativerelease.ExecutionPlanAPIVersion, Kind: declarativerelease.ExecutionResultKind,
					Component: bundle.Prepared.Component, ConfigSHA: bundle.Prepared.ConfigSHA, ExecutionPlanDigest: bundle.Prepared.PlanDigest,
					Status: "verified", Reason: "forward-verified", ForwardApplyCount: 1, Final: guardianObservation(bundle.Prepared.Forward),
				}
				terminal = resealProcessResult(t, terminal)
				monitor, err = declarativerelease.NewMonitorRecord(bundle.Plan, bundle.Artifact, bundle.Prepared, terminal, bundle.Forward, bundle.LKG)
				if err != nil {
					t.Fatal(err)
				}
				monitorData = make(map[string]string, len(bundle.Files)+2)
				for name, raw := range bundle.Files {
					monitorData[name] = string(raw)
				}
				monitorData["record.json"] = servingObservationJSON(t, monitor)
				monitorData["terminal-result.json"] = servingObservationJSON(t, terminal)
				_, err = maps.Create(ctx, &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: monitorRecordNameFromDigest(f.key.Component, monitor.RecordDigest),
						Labels: map[string]string{"fugue.pro/component": f.key.Component}},
					Immutable: ptr.To(true), Data: monitorData,
				}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}
			statePointer, err := maps.Get(ctx, "fugue-release-monitor-"+f.key.Component, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			state, _, err := declarativerelease.NewMonitorState(monitor, declarativerelease.MonitorState{}, true, "", f.now)
			if err != nil {
				t.Fatal(err)
			}
			statePointer.Data = map[string]string{"recordName": monitorRecordNameFromDigest(f.key.Component, monitor.RecordDigest),
				"state.json": servingObservationJSON(t, state)}
			if _, err := maps.Update(ctx, statePointer, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			_, artifact, _, _, manifest, _, err := decodeStableRecord(monitorData)
			if err != nil {
				t.Fatal(err)
			}
			installServingObservationDeployment(t, f, manifest, artifact, mode)
			observed := f.now
			if mode == "stale route" {
				observed = f.now.Add(-3 * time.Minute)
			}
			canary, err := NewCanaryResult(servingRecord, HealthHealthy, testDigest, observed, f.now.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.PutCanaryResult(ctx, canary); err != nil {
				t.Fatal(err)
			}
			if mode == "pointer changed" {
				pointerReads := 0
				f.client.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
					if action.(clienttesting.GetAction).GetName() != pointer.Name {
						return false, nil, nil
					}
					pointerReads++
					if pointerReads < 3 {
						return false, nil, nil
					}
					replaced := pointer.DeepCopy()
					replaced.ResourceVersion = "unexpected"
					return true, replaced, nil
				})
			}

			f.client.ClearActions()
			witness, err := p.ObserveServingRelease(ctx, intentDigest)
			if mode == "candidate" || mode == "rollback" {
				generation := draft.Intent.Generation + 1
				if mode == "rollback" {
					generation++
				}
				if err != nil || witness.Claim != claim || witness.Serving != (productioncontrol.Version{Generation: generation, Digest: servingRecord.RecordDigest}) ||
					witness.MonitorDigest != monitor.RecordDigest || witness.DeploymentUID != "uid" || len(witness.VerifiedPods) != 1 ||
					!digestPattern.MatchString(witness.EvidenceDigest) || !allLayersHealthy(witness.Health) {
					t.Fatalf("serving witness=%+v err=%v", witness, err)
				}
			} else if err == nil || witness.EvidenceDigest != "" {
				t.Fatalf("invalid serving state was considered witnessed: %+v err=%v", witness, err)
			}
			if mode == "pointer changed" && !strings.Contains(err.Error(), "changed during native serving observation") {
				t.Fatalf("test did not reach the pointer consistency check: %v", err)
			}
			for _, action := range f.client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("serving observation made a write: %+v", action)
				}
			}
		})
	}
}

func installServingObservationDeployment(t *testing.T, f healthyPublication, manifest []byte, artifact declarativerelease.ArtifactReceipt, mode string) {
	t.Helper()
	ctx := context.Background()
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de"}
	item, err := declarativerelease.ResourceSetItem(manifest, identity)
	if err != nil {
		t.Fatal(err)
	}
	var deployment appsv1.Deployment
	if err := json.Unmarshal([]byte(servingObservationJSON(t, item)), &deployment); err != nil {
		t.Fatal(err)
	}
	deployment.UID, deployment.ResourceVersion, deployment.Generation = "uid", "21", 5
	deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": identity.Name}}
	deployment.Spec.Template.Labels = map[string]string{"app": identity.Name}
	if mode == "manifest drift" {
		deployment.Spec.Strategy.Type = appsv1.RollingUpdateDeploymentStrategyType
	}
	deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: deployment.Generation, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	apps := f.client.AppsV1()
	if _, err := apps.Deployments(identity.Namespace).Update(ctx, &deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: identity.Name + "-rs", UID: "rs-uid", ResourceVersion: "22",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&deployment, appsv1.SchemeGroupVersion.WithKind("Deployment"))}}}
	if mode == "foreign owner" {
		rs.OwnerReferences[0].UID = "foreign"
	}
	if _, err := apps.ReplicaSets(identity.Namespace).Create(ctx, rs, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pods := f.client.CoreV1().Pods(identity.Namespace)
	pod, err := pods.Get(ctx, identity.Name+"-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.UID, pod.ResourceVersion = types.UID("pod-uid"), "23"
	pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}
	pod.Spec = *deployment.Spec.Template.Spec.DeepCopy()
	pod.Status.ContainerStatuses[0].ImageID = artifact.ImmutableRef
	if mode == "wrong image" {
		pod.Status.ContainerStatuses[0].ImageID = "sha256:" + strings.Repeat("9", 64)
	}
	if _, err := pods.Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func servingObservationJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := declarativerelease.CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
