package releaseguardian

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNativeRuntimeChecksLiveApprovalAndExactCandidate(t *testing.T) {
	ctx := context.Background()
	f := healthyPublicationFixture(t)
	p, err := f.store.planDesiredPublicationFor(ctx, f.key, f.candidate, true)
	if err != nil {
		t.Fatal(err)
	}
	p.desired.ResourceVersion = "20"
	p.desired.Annotations = map[string]string{nativeIntentAnnotation: testDigest, nativeFenceAnnotation: "2"}
	core := f.client.CoreV1()
	maps := core.ConfigMaps(p.preview.Namespace)
	for _, object := range []*corev1.ConfigMap{p.record, p.execution, p.desired} {
		if _, err := maps.Create(ctx, object, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	binding := NativeRuntimeBinding{Key: f.key, DesiredResourceVersion: "20",
		DesiredRecordDigest: p.preview.Record.RecordDigest, LKGMonitorRecordDigest: f.stableMonitor.RecordDigest, IntentDigest: testDigest, Fence: 2}
	if err := binding.VerifyCandidate(ctx, core, p.preview.Namespace, f.candidate); err != nil {
		t.Fatalf("valid approval was refused: %v", err)
	}
	altered := p.execution.DeepCopy()
	altered.Data["execution-plan.json"] = "{}"
	if _, err := maps.Update(ctx, altered, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := binding.VerifyCandidate(ctx, core, p.preview.Namespace, f.candidate); err == nil || !strings.Contains(err.Error(), "execution snapshot") {
		t.Fatalf("substituted snapshot did not fail closed: %v", err)
	}
	for _, mode := range []string{"replaced", "malformed fence", "different valid fence", "unmarked"} {
		t.Run(mode, func(t *testing.T) {
			pointer := p.desired.DeepCopy()
			switch mode {
			case "replaced":
				pointer.ResourceVersion = "21"
			case "malformed fence":
				pointer.Annotations[nativeFenceAnnotation] = "bogus"
			case "different valid fence":
				pointer.Annotations[nativeFenceAnnotation] = "3"
			case "unmarked":
				pointer.Annotations = nil
			}
			if _, err := maps.Update(ctx, pointer, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			f.client.ClearActions()
			if err := binding.Verify(ctx, core, p.preview.Namespace); err == nil {
				t.Fatal("obsolete or unauthorized pointer was accepted")
			}
			for _, action := range f.client.Actions() {
				if action.GetVerb() != "get" {
					t.Fatalf("runtime binding verification made a non-read request: %v", action)
				}
			}
		})
	}
}

func TestNativeProcessEnvironmentDoesNotPassTrustedLegacyArtifact(t *testing.T) {
	snapshot := processSnapshot(t)
	snapshot.NativePublication = true
	snapshot.NativeIntent, snapshot.NativeFence = testDigest, 2
	snapshot.Managed = true
	snapshot.DesiredResourceVersion = "30"
	snapshot.Desired.RecordDigest = snapshot.Record.RecordDigest
	snapshot.CurrentMonitorData = nil // Native child obtains exact records from the runtime store.
	raw, legacy, err := executorArtifactEnvironment(snapshot)
	binding, decodeErr := DecodeNativeRuntimeBinding(raw)
	if err != nil || decodeErr != nil || legacy != "" || binding.Key != snapshot.Key ||
		binding.DesiredRecordDigest != snapshot.Record.RecordDigest || binding.LKGMonitorRecordDigest != snapshot.LKGMonitorRecordDigest {
		t.Fatalf("binding=%+v hasLegacyReceipt=%v err=%v decode=%v", binding, legacy != "", err, decodeErr)
	}
	snapshot.DesiredRecordMissing = true
	if _, _, err := executorArtifactEnvironment(snapshot); err == nil {
		t.Fatal("orphaned native release proceeded to execution")
	}
}
