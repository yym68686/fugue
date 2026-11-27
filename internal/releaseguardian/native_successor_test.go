package releaseguardian

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNativeSuccessorKeepsVerifiedDesiredAliasAndByteExactRollback(t *testing.T) {
	ctx := context.Background()
	f := healthyPublicationFixture(t)
	bundle, err := DecodeExecutionBundle(f.candidate, f.key)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a normal Guardian rollout: the Desired pointer retains its
	// submitted record, while the monitor seals the now-serving image.
	published, firstDesired, err := f.store.PublishDesired(ctx, f.key, f.candidate)
	if err != nil {
		t.Fatal(err)
	}
	terminal := declarativerelease.ExecutionResult{
		APIVersion: declarativerelease.ExecutionPlanAPIVersion, Kind: declarativerelease.ExecutionResultKind,
		Component: f.key.Component, ConfigSHA: bundle.Prepared.ConfigSHA, ExecutionPlanDigest: bundle.Prepared.PlanDigest,
		Status: "verified", Reason: "forward-verified", ForwardApplyCount: 1, Final: guardianObservation(bundle.Prepared.Forward),
	}
	raw, err := declarativerelease.CanonicalJSON(terminal)
	if err != nil {
		t.Fatal(err)
	}
	terminal.ReceiptDigest = digest(raw)
	monitor, err := declarativerelease.NewMonitorRecord(bundle.Plan, bundle.Artifact, bundle.Prepared, terminal, bundle.Forward, bundle.LKG)
	if err != nil {
		t.Fatal(err)
	}
	data := make(map[string]string, len(f.candidate)+2)
	for name, value := range f.candidate {
		data[name] = string(value)
	}
	for name, value := range map[string]any{"record.json": monitor, "terminal-result.json": terminal} {
		raw, err := declarativerelease.CanonicalJSON(value)
		if err != nil {
			t.Fatal(err)
		}
		data[name] = string(raw)
	}
	canonical, _, err := canonicalStableReleaseRecord(f.key, data)
	if err != nil || published.RecordDigest == canonical.RecordDigest || !SameCommittedReleaseTarget(published, canonical) {
		t.Fatalf("test requires two distinct digests for the same serving release: published=%+v canonical=%+v err=%v", published, canonical, err)
	}
	namespace := bundle.Release.Workload.Namespace
	configmaps := f.client.CoreV1().ConfigMaps(namespace)
	name := monitorRecordNameFromDigest(f.key.Component, monitor.RecordDigest)
	immutable := true
	if _, err := configmaps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: data, Immutable: &immutable}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	state, err := configmaps.Get(ctx, "fugue-release-monitor-"+f.key.Component, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	state.Data["recordName"] = name
	if _, err := configmaps.Update(ctx, state, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deployments := f.client.AppsV1().Deployments(namespace)
	workload, err := deployments.Get(ctx, bundle.Release.Workload.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	workload.ResourceVersion = "11"
	workload.Annotations["fugue.pro/production-config-sha"] = bundle.Prepared.Forward.ConfigSHA
	workload.Spec.Template.Annotations["fugue.pro/oci-revision"] = bundle.Prepared.Forward.OCIRevision
	workload.Spec.Template.Spec.Containers[0].Image = bundle.Prepared.Forward.ImageRef
	if _, err := deployments.Update(ctx, workload, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pods := f.client.CoreV1().Pods(namespace)
	pod, err := pods.Get(ctx, f.key.Component+"-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses[0].ImageID = bundle.Prepared.Forward.ImageRef
	if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	canary, err := NewCanaryResult(published, HealthHealthy, testDigest, f.now, f.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.PutCanaryResult(ctx, canary); err != nil {
		t.Fatal(err)
	}

	successor := guardianCandidateWithResourceVersion(t, guardianCandidateFixtureForTarget(t, f.key, bundle.Prepared.Forward,
		bundle.Artifact.TopDigest, bundle.Forward, f.now, strings.Repeat("3", 40), "sha256:"+strings.Repeat("d", 64)), "11")
	f.client.ClearActions()
	draft, err := f.store.DraftNativeRelease(ctx, f.key, "isolated-cluster", "request-next", successor)
	if err != nil {
		t.Fatal(err)
	}
	if draft.Intent.Generation != firstDesired.Generation || draft.Intent.CurrentDigest != published.RecordDigest ||
		draft.Intent.RollbackDigest != published.RecordDigest || draft.Preview.Record.LKGRecordDigest != published.RecordDigest ||
		draft.Preview.RollbackManifestDigest != monitor.ForwardManifestDigest || draft.Preview.RollbackMonitorDigest != monitor.RecordDigest {
		t.Fatalf("successor lost the recorded release or its exact monitor evidence: %+v", draft)
	}
	for _, action := range f.client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("draft of successor wrote to provider: %+v", action)
		}
	}
	legacy, err := f.store.PreviewDesired(ctx, f.key, successor)
	if err != nil || legacy.Record.LKGRecordDigest != canonical.RecordDigest {
		t.Fatalf("native alias accidentally changed the legacy publication contract: preview=%+v error=%v", legacy, err)
	}
}
