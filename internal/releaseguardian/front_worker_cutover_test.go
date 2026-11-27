package releaseguardian

import (
	"context"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func frontWorkerCutoverFixture() FrontWorkerCutover {
	return FrontWorkerCutover{
		Namespace: "front-test", Group: "edge-group-fixture", Phase: KubeCutoverFrozen,
		Guardian: "system:serviceaccount:front-test:native-guardian", DaemonSetStatusWriter: "system:kube-controller-manager",
		WorkerDaemonSets: []string{"worker-slot-a", "worker-slot-b"}, ComponentLeases: []string{"release-worker-a", "release-worker-b"},
	}
}

func TestFrontWorkerCutoverRequiresAnExplicitScope(t *testing.T) {
	for name, edit := range map[string]func(*FrontWorkerCutover){
		"no workers":          func(config *FrontWorkerCutover) { config.WorkerDaemonSets = nil },
		"no component leases": func(config *FrontWorkerCutover) { config.ComponentLeases = nil },
		"same identity":       func(config *FrontWorkerCutover) { config.DaemonSetStatusWriter = config.Guardian },
		"duplicate group lease": func(config *FrontWorkerCutover) {
			config.ComponentLeases = []string{FrontWorkerGroupLeaseName(config.Group)}
		},
		"default service account": func(config *FrontWorkerCutover) {
			config.Guardian = "system:serviceaccount:front-test:default"
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := frontWorkerCutoverFixture()
			edit(&config)
			if _, err := FrontWorkerCutoverPolicies(config); err == nil {
				t.Fatal("accepted an incomplete or ambiguous scope")
			}
		})
	}
}

func TestFrontWorkerCutoverObservationRejectsUnverifiedOrChangingPolicy(t *testing.T) {
	config := frontWorkerCutoverFixture()
	pairs, err := FrontWorkerCutoverPolicies(config)
	if err != nil {
		t.Fatal(err)
	}
	objects := make([]runtime.Object, 0, 2*len(pairs))
	for _, pair := range pairs {
		pair.Policy.UID = types.UID(pair.Policy.Name + "-policy")
		pair.Policy.Generation = 2
		pair.Policy.ResourceVersion = "10"
		pair.Policy.Status.ObservedGeneration = pair.Policy.Generation
		pair.Policy.Status.TypeChecking = &admissionv1.TypeChecking{}
		pair.Binding.UID = types.UID(pair.Binding.Name + "-binding")
		pair.Binding.ResourceVersion = "11"
		objects = append(objects, pair.Policy, pair.Binding)
	}
	client := fake.NewSimpleClientset(objects...)
	got, err := ObserveFrontWorkerCutover(t.Context(), client, config)
	if err != nil || len(got) != len(pairs) || got[0].PolicyRevision != "10" || got[0].BindingRevision != "11" {
		t.Fatalf("readback did not identify the exact phase and revisions: %+v %v", got, err)
	}
	groupLease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Name: FrontWorkerGroupLeaseName(config.Group), Namespace: config.Namespace, UID: "lease-uid", ResourceVersion: "21",
	}, Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr.To("")}}
	leaseClient := fake.NewSimpleClientset(append(objects, groupLease)...)
	frozen, err := ObserveFrozenFrontWorkerBoundary(t.Context(), leaseClient, config)
	if err != nil || len(frozen.Leases) != 3 || frozen.Leases[0].Revision != "21" || frozen.Leases[1].UID != "" {
		t.Fatalf("readback did not distinguish a released Lease from an absent Lease: %+v %v", frozen, err)
	}
	for _, name := range []string{FrontRepairFenceName(config.Group), transitionActivatedJournalName(config.Group)} {
		transaction := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: config.Namespace}}
		client := fake.NewSimpleClientset(append(objects, transaction)...)
		if _, err := ObserveFrozenFrontWorkerBoundary(t.Context(), client, config); err == nil {
			t.Fatalf("accepted frozen group with an outstanding transaction object %s", name)
		}
	}
	expiredLease := groupLease.DeepCopy()
	expiredLease.Spec.HolderIdentity = ptr.To("old-process")
	expiredLease.Spec.RenewTime = &metav1.MicroTime{Time: time.Unix(1, 0)}
	expiredLease.Spec.LeaseDurationSeconds = ptr.To(int32(120))
	if _, err := ObserveFrozenFrontWorkerBoundary(t.Context(), fake.NewSimpleClientset(append(objects, expiredLease)...), config); err == nil {
		t.Fatal("expired timestamp did not release an outstanding Lease holder")
	}
	checks := []struct {
		name string
		edit func(*admissionv1.ValidatingAdmissionPolicy)
	}{
		{"stale type check", func(p *admissionv1.ValidatingAdmissionPolicy) { p.Status.ObservedGeneration-- }},
		{"unverified expressions", func(p *admissionv1.ValidatingAdmissionPolicy) {
			p.Status.TypeChecking.ExpressionWarnings = []admissionv1.ExpressionWarning{{Warning: "test warning"}}
		}},
		{"modified expression", func(p *admissionv1.ValidatingAdmissionPolicy) { p.Spec.Validations[0].Expression = "true" }},
		{"policy revision changed", func(p *admissionv1.ValidatingAdmissionPolicy) { p.ResourceVersion = "12" }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(objects...)
			reads := 0
			client.PrependReactor("get", "validatingadmissionpolicies", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.(ktesting.GetAction).GetName() != pairs[0].Policy.Name {
					return false, nil, nil
				}
				reads++
				if reads == 1 {
					return false, nil, nil
				}
				policy := pairs[0].Policy.DeepCopy()
				check.edit(policy)
				return true, policy, nil
			})
			if observations, err := ObserveFrontWorkerCutover(t.Context(), client, config); err == nil || len(observations) != 0 {
				t.Fatalf("accepted %s: %+v %v", check.name, observations, err)
			}
		})
	}
}

func TestFrontWorkerCutoverDeniedAndNativeAdmissionsAtProvider(t *testing.T) {
	config := frontWorkerCutoverFixture()
	cfg := startAdmissionTestServer(t)
	admin := admissionClient(t, cfg, "")
	legacy := admissionClient(t, cfg, "system:serviceaccount:front-test:legacy-runner")
	native := admissionClient(t, cfg, config.Guardian)
	status := admissionClient(t, cfg, config.DaemonSetStatusWriter)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	createAdmissionTestNamespace(t, ctx, admin, config.Namespace)
	install := func() {
		t.Helper()
		pairs, err := FrontWorkerCutoverPolicies(config)
		if err != nil {
			t.Fatal(err)
		}
		policies := admin.AdmissionregistrationV1().ValidatingAdmissionPolicies()
		bindings := admin.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings()
		for _, pair := range pairs {
			if config.Phase == KubeCutoverFrozen {
				if _, err := policies.Create(ctx, pair.Policy, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				if _, err := bindings.Create(ctx, pair.Binding, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			} else {
				old, err := policies.Get(ctx, pair.Policy.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pair.Policy.ResourceVersion = old.ResourceVersion
				if _, err := policies.Update(ctx, pair.Policy, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	names := func(prefix string) string { return "fugue-node-worker-" + prefix + "-" + strings.Repeat("f", 32) }
	record := func(name, group, kind string) *corev1.ConfigMap {
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"fugue.pro/group": group, "fugue.pro/authority-kind": kind}},
			Immutable:  ptr.To(true), Data: map[string]string{"value": "fixture"},
		}
	}
	mapCreate := func(client kubernetes.Interface, object *corev1.ConfigMap) error {
		_, err := client.CoreV1().ConfigMaps(config.Namespace).Create(ctx, object, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		return err
	}
	fence := record(FrontRepairFenceName(config.Group), config.Group, nodeWorkerJournalKind)
	arm := record(names("armed"), config.Group, "node-worker-arm")
	retirement := record(names("retirement"), config.Group, "node-worker-retirement")
	witness := record(names("connected"), config.Group, "node-worker-witness")
	otherGroup := record(names("connected"), "another-group", "node-worker-witness")
	worker, err := admin.AppsV1().DaemonSets(config.Namespace).Create(ctx, &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: config.WorkerDaemonSets[0]},
		Spec: appsv1.DaemonSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "worker:fixture"}}}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	workerUpdate := func(client kubernetes.Interface, statusOnly bool) error {
		ds := worker.DeepCopy()
		options := metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}}
		if statusOnly {
			ds.Status.NumberReady = 0
			_, err := client.AppsV1().DaemonSets(config.Namespace).UpdateStatus(ctx, ds, options)
			return err
		}
		ds.Spec.Template.Spec.Containers[0].Image = "worker:fixture-next"
		_, err := client.AppsV1().DaemonSets(config.Namespace).Update(ctx, ds, options)
		return err
	}
	leaseCreate := func(client kubernetes.Interface, name string) error {
		_, err := client.CoordinationV1().Leases(config.Namespace).Create(ctx,
			&coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name}},
			metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		return err
	}
	waitForPhase := func(ready func() bool) {
		t.Helper()
		if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 12*time.Second, true, func(context.Context) (bool, error) {
			return ready(), nil
		}); err != nil {
			t.Fatalf("provider did not enforce %s: %v", config.Phase, err)
		}
	}
	requireAllowed := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	install()
	waitForPhase(func() bool {
		return isCutoverDenial(mapCreate(legacy, arm)) && isCutoverDenial(workerUpdate(native, false)) &&
			isCutoverDenial(leaseCreate(native, config.ComponentLeases[0]))
	})
	for _, object := range []*corev1.ConfigMap{fence, arm, witness, retirement} {
		requireCutoverDenial(t, mapCreate(legacy, object))
		requireCutoverDenial(t, mapCreate(native, object))
	}
	requireAllowed(mapCreate(legacy, otherGroup))
	requireAllowed(workerUpdate(status, true))
	requireCutoverDenial(t, workerUpdate(status, false))
	requireCutoverDenial(t, workerUpdate(legacy, true))
	requireCutoverDenial(t, leaseCreate(legacy, FrontWorkerGroupLeaseName(config.Group)))

	config.Phase = KubeCutoverNative
	install()
	waitForPhase(func() bool {
		return mapCreate(native, arm) == nil && workerUpdate(native, false) == nil &&
			leaseCreate(native, config.ComponentLeases[0]) == nil
	})
	for _, object := range []*corev1.ConfigMap{fence, arm, witness, retirement} {
		requireCutoverDenial(t, mapCreate(legacy, object))
		requireAllowed(mapCreate(native, object))
	}
	requireCutoverDenial(t, mapCreate(native, record(fence.Name, config.Group, "ordinary-transition")))
	requireCutoverDenial(t, mapCreate(native, record(transitionActivatedJournalName(config.Group), config.Group, "ordinary-transition")))
	nonImmutable := arm.DeepCopy()
	nonImmutable.Immutable = ptr.To(false)
	requireCutoverDenial(t, mapCreate(native, nonImmutable))
	configs := native.CoreV1().ConfigMaps(config.Namespace)
	projection, err := configs.Create(ctx, record(NodeWorkerProjectionName(config.Group, "node-fixture"), config.Group, nodeWorkerProjectionKind),
		metav1.CreateOptions{})
	requireAllowed(err)
	delete(projection.Labels, "fugue.pro/group")
	_, err = legacy.CoreV1().ConfigMaps(config.Namespace).Update(ctx, projection, metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}})
	requireCutoverDenial(t, err)
	requireCutoverDenial(t, legacy.CoreV1().ConfigMaps(config.Namespace).Delete(ctx, projection.Name, metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}}))
	requireCutoverDenial(t, workerUpdate(legacy, false))
	requireAllowed(workerUpdate(status, true))
	requireAllowed(mapCreate(legacy, otherGroup))
}
