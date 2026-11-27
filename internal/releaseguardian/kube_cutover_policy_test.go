package releaseguardian

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	admission "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

type deploymentEvidenceFunc func(context.Context, KubeDeploymentCutover, NativePublicationBoundary) error

func (f deploymentEvidenceFunc) VerifyDeploymentCutoverEvidence(ctx context.Context, c KubeDeploymentCutover, b NativePublicationBoundary) error {
	return f(ctx, c, b)
}

func cutoverFixture() KubeDeploymentCutover {
	return KubeDeploymentCutover{
		Namespace: "isolated-cutover", Key: Key{Component: "demo", Group: "cell-a"}, Deployment: "demo-deployment",
		MonitorComponent: "demo", ComponentLease: "demo-lease", Phase: KubeCutoverFrozen,
		Publisher: "system:serviceaccount:isolated-identity:native-publisher", Guardian: "system:serviceaccount:isolated-identity:native-guardian",
		CanaryProber: "system:serviceaccount:isolated-identity:native-canary", DeploymentStatusWriter: "system:kube-controller-manager",
	}
}

func TestDeploymentCutoverRequiresExplicitDistinctIdentities(t *testing.T) {
	for _, mutate := range []func(*KubeDeploymentCutover){
		func(c *KubeDeploymentCutover) { c.Deployment += "\" || true" },
		func(c *KubeDeploymentCutover) { c.Publisher = c.Guardian },
		func(c *KubeDeploymentCutover) { c.CanaryProber = "system:serviceaccount:isolated-identity:default" },
		func(c *KubeDeploymentCutover) { c.Guardian = "system:kube-controller-manager" },
		func(c *KubeDeploymentCutover) { c.Phase = "unknown" },
		func(c *KubeDeploymentCutover) { c.DeploymentStatusWriter = "" },
	} {
		config := cutoverFixture()
		mutate(&config)
		if _, err := DeploymentCutoverPolicies(config); err == nil {
			t.Fatalf("unsafe ambiguous cutover config was rendered: %+v", config)
		}
	}
}

func TestDeploymentCutoverAdmissionAtProvider(t *testing.T) {
	cfg := startAdmissionTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	config := cutoverFixture()
	admin := admissionClient(t, cfg, "")
	legacy := admissionClient(t, cfg, "system:serviceaccount:isolated-identity:old-runner")
	publisher := admissionClient(t, cfg, config.Publisher)
	guardian := admissionClient(t, cfg, config.Guardian)
	prober := admissionClient(t, cfg, config.CanaryProber)
	controller := admissionClient(t, cfg, config.DeploymentStatusWriter)
	createAdmissionTestNamespace(t, ctx, admin, config.Namespace)
	const unrelated = "isolated-unrelated"
	createAdmissionTestNamespace(t, ctx, admin, unrelated)
	deployments := legacy.AppsV1().Deployments(config.Namespace)
	base := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: config.Deployment, Labels: map[string]string{"fence-fixture": "exists"}},
		Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "demo"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "demo", Image: "fixture.invalid/demo:recorded"}}},
			},
		},
	}
	workload, err := deployments.Create(ctx, base.DeepCopy(), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("before freeze the old identity should be a valid writer: %v", err)
	}
	if _, err := legacy.AppsV1().Deployments(unrelated).Create(ctx, base.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	const mapLabels = "fence-fixture=protected"
	pointer := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: desiredName(config.Key), Labels: map[string]string{"fence-fixture": "protected"}},
		Data: map[string]string{"desired.json": "verified predecessor"}}
	pointer, err = legacy.CoreV1().ConfigMaps(config.Namespace).Create(ctx, pointer, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	delayed := workload.DeepCopy()
	delayed.Spec.Template.Spec.Containers[0].Image = "fixture.invalid/demo:late"
	freshLease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: config.ComponentLease}}
	dryWrite := metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}}
	dryCreate := metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}
	waitForCutover(t, ctx, admin, config, func(ctx context.Context) (bool, error) {
		_, depErr := deployments.Update(ctx, delayed.DeepCopy(), dryWrite)
		_, leaseErr := legacy.CoordinationV1().Leases(config.Namespace).Create(ctx, freshLease.DeepCopy(), dryCreate)
		_, ptrErr := legacy.CoreV1().ConfigMaps(config.Namespace).Update(ctx, pointer.DeepCopy(), dryWrite)
		return isCutoverDenial(depErr) && isCutoverDenial(leaseErr) && isCutoverDenial(ptrErr), nil
	})
	if err := VerifyDeploymentCutoverPolicies(ctx, publisher, config); err != nil {
		t.Fatalf("frozen policy did not match readback: %v", err)
	}
	// This valid pre-freeze resourceVersion was captured before admission was
	// installed. The test sends it after activation, not after pre-admission.
	_, err = deployments.Update(ctx, delayed.DeepCopy(), metav1.UpdateOptions{})
	requireCutoverDenial(t, err)
	_, err = guardian.AppsV1().Deployments(config.Namespace).Update(ctx, delayed.DeepCopy(), metav1.UpdateOptions{})
	requireCutoverDenial(t, err)
	_, err = legacy.CoordinationV1().Leases(config.Namespace).Create(ctx, freshLease.DeepCopy(), metav1.CreateOptions{})
	requireCutoverDenial(t, err)
	_, err = legacy.CoreV1().ConfigMaps(config.Namespace).Update(ctx, pointer.DeepCopy(), metav1.UpdateOptions{})
	requireCutoverDenial(t, err)
	err = legacy.CoreV1().ConfigMaps(config.Namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: mapLabels})
	requireCutoverDenial(t, err)
	_, err = deployments.Patch(ctx, config.Deployment, types.MergePatchType, []byte(`{"spec":{"replicas":4}}`), metav1.PatchOptions{}, "scale")
	requireCutoverDenial(t, err)
	healthy := workload.DeepCopy()
	healthy.Status.Replicas = 1
	healthy.Status.ReadyReplicas = 1
	healthy.Status.ObservedGeneration = workload.Generation
	_, err = deployments.UpdateStatus(ctx, healthy, metav1.UpdateOptions{})
	requireCutoverDenial(t, err)
	workload, err = controller.AppsV1().Deployments(config.Namespace).UpdateStatus(ctx, healthy, metav1.UpdateOptions{})
	if err != nil || workload.Status.ReadyReplicas != 1 {
		t.Fatalf("freeze interfered with controller status: %v", err)
	}
	_, err = legacy.AppsV1().Deployments(unrelated).Patch(ctx, config.Deployment, types.MergePatchType, []byte(`{"spec":{"replicas":2}}`), metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("fence leaked to an unrelated namespace: %v", err)
	}
	otherMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "fugue-guardian-record-demo-cell-a-other-0123456789abcdef"}}
	if _, err := legacy.CoreV1().ConfigMaps(config.Namespace).Create(ctx, otherMap, metav1.CreateOptions{}); err != nil {
		t.Fatalf("fence leaked to a neighboring release key: %v", err)
	}

	config.Phase = KubeCutoverNative
	write := workload.DeepCopy()
	write.Spec.Template.Spec.Containers[0].Image = "fixture.invalid/demo:native"
	canary := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: canaryName(config.Key)}}
	waitForCutover(t, ctx, admin, config, func(ctx context.Context) (bool, error) {
		_, depErr := guardian.AppsV1().Deployments(config.Namespace).Update(ctx, write.DeepCopy(), dryWrite)
		_, leaseErr := guardian.CoordinationV1().Leases(config.Namespace).Create(ctx, freshLease.DeepCopy(), dryCreate)
		_, canaryErr := prober.CoreV1().ConfigMaps(config.Namespace).Create(ctx, canary.DeepCopy(), dryCreate)
		for _, err := range []error{depErr, leaseErr, canaryErr} {
			if err != nil && !isCutoverDenial(err) {
				return false, err
			}
		}
		return depErr == nil && leaseErr == nil && canaryErr == nil, nil
	})
	if err := VerifyDeploymentCutoverPolicies(ctx, publisher, config); err != nil {
		t.Fatalf("native policy did not match readback: %v", err)
	}
	_, err = deployments.Update(ctx, write.DeepCopy(), metav1.UpdateOptions{})
	requireCutoverDenial(t, err)
	_, err = publisher.AppsV1().Deployments(config.Namespace).Update(ctx, write.DeepCopy(), metav1.UpdateOptions{})
	requireCutoverDenial(t, err)
	guardianDeploys := guardian.AppsV1().Deployments(config.Namespace)
	_, err = guardianDeploys.UpdateScale(ctx, config.Deployment, &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: config.Deployment}, Spec: autoscalingv1.ScaleSpec{Replicas: 4},
	}, metav1.UpdateOptions{})
	requireCutoverDenial(t, err)
	if err := guardianDeploys.Delete(ctx, config.Deployment, metav1.DeleteOptions{}); !isCutoverDenial(err) {
		t.Fatalf("Guardian can delete the enrolled existing workload, err=%v", err)
	}
	updated, err := guardianDeploys.Update(ctx, write, metav1.UpdateOptions{})
	if err != nil || updated.Spec.Template.Spec.Containers[0].Image != "fixture.invalid/demo:native" ||
		*updated.Spec.Replicas != 1 || updated.UID != workload.UID {
		t.Fatalf("native Guardian could not update exact workload: %v", err)
	}
	_, err = legacy.CoordinationV1().Leases(config.Namespace).Create(ctx, freshLease.DeepCopy(), metav1.CreateOptions{})
	requireCutoverDenial(t, err)
	if _, err := guardian.CoordinationV1().Leases(config.Namespace).Create(ctx, freshLease, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	publisherMaps, guardianMaps := publisher.CoreV1().ConfigMaps(config.Namespace), guardian.CoreV1().ConfigMaps(config.Namespace)
	_, err = guardianMaps.Create(ctx, canary.DeepCopy(), metav1.CreateOptions{})
	requireCutoverDenial(t, err)
	if _, err := prober.CoreV1().ConfigMaps(config.Namespace).Create(ctx, canary, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	immutableRecord := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: releaseRecordName(config.Key, testDigest)}}
	_, err = publisherMaps.Create(ctx, immutableRecord.DeepCopy(), metav1.CreateOptions{})
	requireCutoverDenial(t, err)
	immutableRecord.Immutable = ptr.To(true)
	_, err = legacy.CoreV1().ConfigMaps(config.Namespace).Create(ctx, immutableRecord.DeepCopy(), metav1.CreateOptions{})
	requireCutoverDenial(t, err)
	if _, err := publisherMaps.Create(ctx, immutableRecord, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	_, err = publisherMaps.Update(ctx, pointer.DeepCopy(), metav1.UpdateOptions{})
	requireCutoverDenial(t, err)
	pointer.Annotations = map[string]string{nativeIntentAnnotation: testDigest, nativeFenceAnnotation: "2"}
	pointer.Data["desired.json"] = "exact fixture intent"
	_, err = legacy.CoreV1().ConfigMaps(config.Namespace).Update(ctx, pointer.DeepCopy(), metav1.UpdateOptions{})
	requireCutoverDenial(t, err)
	pointer, err = publisherMaps.Update(ctx, pointer, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, fence := range []int{1, 2} {
		stale := pointer.DeepCopy()
		stale.Annotations[nativeFenceAnnotation] = strconv.Itoa(fence)
		_, err := publisherMaps.Update(ctx, stale, metav1.UpdateOptions{})
		requireCutoverDenial(t, err)
	}
	forged := pointer.DeepCopy()
	forged.Annotations[nativeFenceAnnotation] = "3"
	_, err = guardianMaps.Update(ctx, forged, metav1.UpdateOptions{})
	requireCutoverDenial(t, err)
	pointer.Data["desired.json"] = "rollback retaining exact claim identity"
	if _, err := guardianMaps.Update(ctx, pointer, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Guardian could not update rollback pointer with same fence: %v", err)
	}

	id := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: config.Namespace, Name: config.Deployment}
	pending := DesiredRelease{APIVersion: APIVersion, Kind: DesiredReleaseKind, Component: config.Key.Component,
		Group: config.Key.Group, RecordDigest: otherDigest, Generation: 2}
	previous := pending
	previous.RecordDigest, previous.Generation = testDigest, 1
	boundary := NativePublicationBoundary{
		Scope:     productioncontrol.Scope{ClusterID: "fixture-cluster", ResourceID: nativeKubernetesResourceID(id)},
		Namespace: config.Namespace, Workload: id, Predecessor: previous, Candidate: pending, ComponentLease: config.ComponentLease,
		MonitorComponent: config.MonitorComponent, DesiredObject: desiredName(config.Key), ExecutionObject: executionSnapshotName(config.Key, 2),
		RecordObject: releaseRecordName(config.Key, pending.RecordDigest),
	}
	var checks int
	missingEvidence := errors.New("no verified evidence that in-flight legacy requests drained")
	gate := kubeDeploymentAdmission{publisher, config, boundary.Scope.ClusterID, deploymentEvidenceFunc(
		func(_ context.Context, c KubeDeploymentCutover, b NativePublicationBoundary) error {
			checks++
			if c != config || b != boundary {
				t.Fatal("independent verifier received the wrong enrollment scope")
			}
			return missingEvidence
		})}
	if err := gate.VerifyNativePublication(ctx, boundary); !errors.Is(err, missingEvidence) || checks != 1 {
		t.Fatalf("valid policy alone conferred authority: checks=%d err=%v", checks, err)
	}
	wrongPrincipal := gate
	wrongPrincipal.client = legacy
	if err := wrongPrincipal.VerifyNativePublication(ctx, boundary); err == nil || checks != 1 {
		t.Fatalf("legacy principal got through native provider gate: checks=%d err=%v", checks, err)
	}
	pairs, err := DeploymentCutoverPolicies(config)
	if err != nil {
		t.Fatal(err)
	}
	bindings := admin.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings()
	binding, err := bindings.Get(ctx, pairs[0].Binding.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	binding.Spec.ValidationActions = []admission.ValidationAction{admission.Audit}
	if _, err := bindings.Update(ctx, binding, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := gate.VerifyNativePublication(ctx, boundary); err == nil || errors.Is(err, missingEvidence) || checks != 1 {
		t.Fatalf("audit-only cutover policy was accepted: checks=%d err=%v", checks, err)
	}
}
