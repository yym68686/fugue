package releaseguardian

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

func registryRehomeTestDeployment() *appsv1.Deployment {
	labels := map[string]string{"app": "rehome"}
	image := "ghcr.io/fixture/rehome@" + testDigest
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "rehome", Namespace: "isolated-rehome", UID: "reviewed-workload", ResourceVersion: "10", Generation: 3,
			Labels: labels, Annotations: map[string]string{podArtifactImageAnnotation: "top-level unchanged"}},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](2), Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{podArtifactImageAnnotation: image, "fixture.invalid/unrelated": "keep"}},
				Spec: corev1.PodSpec{
					Containers:       []corev1.Container{{Name: "main", Image: image, Args: []string{"serve"}}, {Name: "sidecar", Image: image, Args: []string{"watch"}}},
					InitContainers:   []corev1.Container{{Name: "init", Image: image, Args: []string{"check"}}},
					ImagePullSecrets: []corev1.LocalObjectReference{{Name: "already-enrolled"}},
				},
			},
		},
	}
}

func registryRehomeTestReview(t *testing.T, deployment *appsv1.Deployment) RegistryRehomeReview {
	t.Helper()
	shape, err := declarativerelease.CanonicalJSON(deployment.Spec)
	if err != nil {
		t.Fatal(err)
	}
	id := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: deployment.Namespace, Name: deployment.Name}
	repository := testNativeRegistry + "/fixture/rehome"
	source := "ghcr.io/fixture/rehome@" + testDigest
	destination := repository + "@" + testDigest
	return RegistryRehomeReview{
		Source: RegistryRehomeSource{Deployment: RegistryRehomeObservedObject{id, string(deployment.UID), deployment.Generation, digest(shape)}},
		Draft: RegistryRehomeDraft{Workload: id,
			Current: RegistryRehomeProjection{SourceImage: source, IndependentImage: destination, RecordDigest: testDigest, SourceManifestDigest: otherDigest,
				SourcePodSpecDigest: testDigest, TargetPodSpecDigest: otherDigest,
				Containers: []RegistryRehomeContainer{{"containers", "main"}, {"containers", "sidecar"}, {"initContainers", "init"}}},
			Mirror: RegistryRecoveryReport{Forward: RegistryRecoveryReadback{SourceImage: source, SourceReceiptDigest: testDigest,
				RecoveryPin: repository + ":fugue-recovery-sha256-" + strings.TrimPrefix(testDigest, "sha256:"),
				RecoveryImage: declarativerelease.RegistryVerification{Image: destination, IndexDigest: testDigest,
					Verification: "independent_registry_full_readback", Platform: "linux/amd64", OCIRevision: strings.Repeat("1", 40),
					ManifestDigest: otherDigest, ConfigDigest: testDigest}}},
		},
	}
}

func TestRegistryRehomeDeploymentPatchExactScope(t *testing.T) {
	live := registryRehomeTestDeployment()
	review := registryRehomeTestReview(t, live)
	cas, err := prepareRegistryRehomeDeploymentCAS(review, live)
	if err != nil {
		t.Fatal(err)
	}
	var ops []rehomePatchOperation
	if err := json.Unmarshal(cas.patch, &ops); err != nil {
		t.Fatal(err)
	}
	want := []rehomePatchOperation{{"test", "/metadata/uid", string(live.UID)}, {"test", "/metadata/resourceVersion", live.ResourceVersion}}
	image := review.Draft.Current
	paths := []string{"/spec/template/spec/containers/0", "/spec/template/spec/containers/1", "/spec/template/spec/initContainers/0"}
	for i, path := range paths {
		want = append(want, rehomePatchOperation{"test", path + "/name", image.Containers[i].Name}, rehomePatchOperation{"test", path + "/image", image.SourceImage})
	}
	const artifact = "/spec/template/metadata/annotations/fugue.pro~1artifact-image"
	want = append(want, rehomePatchOperation{"test", artifact, image.SourceImage})
	for _, path := range paths {
		want = append(want, rehomePatchOperation{"replace", path + "/image", image.IndependentImage})
	}
	want = append(want, rehomePatchOperation{"replace", artifact, image.IndependentImage})
	if !apiequality.Semantic.DeepEqual(ops, want) ||
		live.Spec.Template.Annotations[podArtifactImageAnnotation] != image.SourceImage ||
		live.Spec.Template.Spec.InitContainers[0].Image != image.SourceImage {
		t.Fatalf("registry rehome requested a mutation outside the exact image projection: %+v", ops)
	}
	for name, change := range map[string]func(*RegistryRehomeReview, *appsv1.Deployment){
		"new UID":        func(_ *RegistryRehomeReview, d *appsv1.Deployment) { d.UID = "different" },
		"new generation": func(_ *RegistryRehomeReview, d *appsv1.Deployment) { d.Generation++ },
		"spec drift":     func(_ *RegistryRehomeReview, d *appsv1.Deployment) { d.Spec.Replicas = ptr.To[int32](3) },
		"new digest": func(r *RegistryRehomeReview, _ *appsv1.Deployment) {
			r.Draft.Current.IndependentImage = testNativeRegistry + "/fixture/rehome@" + otherDigest
		},
		"unreviewed order": func(r *RegistryRehomeReview, _ *appsv1.Deployment) {
			r.Draft.Current.Containers[0], r.Draft.Current.Containers[1] = r.Draft.Current.Containers[1], r.Draft.Current.Containers[0]
		},
	} {
		t.Run(name, func(t *testing.T) {
			d := live.DeepCopy()
			r := registryRehomeTestReview(t, d)
			change(&r, d)
			got, err := prepareRegistryRehomeDeploymentCAS(r, d)
			if !errors.Is(err, declarativerelease.ErrWriteRejected) || len(got.patch) != 0 {
				t.Fatalf("prepared a request for an unreviewed object: %s %v", got.patch, err)
			}
		})
	}
}

func TestRegistryRehomeDeploymentWriteResponseIsNotReplayable(t *testing.T) {
	live := registryRehomeTestDeployment()
	cas, err := prepareRegistryRehomeDeploymentCAS(registryRehomeTestReview(t, live), live)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"provider busy", "admission altered spec", "admission added finalizer"} {
		t.Run(mode, func(t *testing.T) {
			requests := make(chan struct{}, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- struct{}{}
				if r.Method != http.MethodPatch || r.URL.Path != "/apis/apps/v1/namespaces/isolated-rehome/deployments/rehome" ||
					r.Header.Get("Content-Type") != "application/json-patch+json" {
					t.Errorf("wrong provider request: %s %s %s", r.Method, r.URL.Path, r.Header.Get("Content-Type"))
				}
				if mode == "provider busy" {
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				response := cas.expected.DeepCopy()
				response.APIVersion, response.Kind = "apps/v1", "Deployment"
				response.ResourceVersion, response.Generation = "11", live.Generation+1
				if mode == "admission altered spec" {
					response.Spec.Template.Spec.ImagePullSecrets = nil
				} else {
					response.Finalizers = []string{"fixture.invalid/finalizer"}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			updated, err := writeRegistryRehomeDeploymentOnce(t.Context(), client.AppsV1().RESTClient(), cas)
			if updated != nil || !errors.Is(err, declarativerelease.ErrWriteOutcomeUnknown) || len(requests) != 1 {
				t.Fatalf("uncertain write was replayed or accepted: requests=%d updated=%v err=%v", len(requests), updated, err)
			}
		})
	}
}

func TestRegistryRehomeDeploymentCASAtProvider(t *testing.T) {
	admin := admissionClient(t, startAdmissionTestServer(t), "")
	ctx := t.Context()
	initial := registryRehomeTestDeployment()
	initial.UID, initial.ResourceVersion, initial.Generation = "", "", 0
	createAdmissionTestNamespace(t, ctx, admin, initial.Namespace)
	deployments := admin.AppsV1().Deployments(initial.Namespace)
	service, err := admin.CoreV1().Services(initial.Namespace).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "companion"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 443}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	live, err := deployments.Create(ctx, initial, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	review := registryRehomeTestReview(t, live)
	stale, err := prepareRegistryRehomeDeploymentCAS(review, live)
	if err != nil {
		t.Fatal(err)
	}
	live.Status.ObservedGeneration = live.Generation
	live.Status.Replicas = 1
	live.Status.ReadyReplicas = 1
	live.Status.AvailableReplicas = 1
	fresh, err := deployments.UpdateStatus(ctx, live, metav1.UpdateOptions{})
	if err != nil || fresh.ResourceVersion == live.ResourceVersion || fresh.Generation != live.Generation {
		t.Fatalf("could not advance only the Kubernetes resourceVersion: %v", err)
	}
	cas, err := prepareRegistryRehomeDeploymentCAS(review, fresh)
	if err != nil {
		t.Fatal(err)
	}
	replaced := fresh.DeepCopy()
	replaced.UID = "a665d60b-a03b-419f-81d1-e7f647f4c0bc"
	foreign, err := prepareRegistryRehomeDeploymentCAS(registryRehomeTestReview(t, replaced), replaced)
	if err != nil {
		t.Fatal(err)
	}
	assertRejected := func(cas rehomeDeploymentCAS) {
		t.Helper()
		got, err := writeRegistryRehomeDeploymentOnce(ctx, admin.AppsV1().RESTClient(), cas)
		if got != nil || !errors.Is(err, declarativerelease.ErrWriteRejected) || (!apierrors.IsConflict(err) && !apierrors.IsInvalid(err)) {
			t.Fatalf("precondition mismatch was not rejected by kube-apiserver: %v", err)
		}
	}
	assertRejected(stale)
	assertRejected(foreign)
	updated, err := writeRegistryRehomeDeploymentOnce(ctx, admin.AppsV1().RESTClient(), cas)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Generation != fresh.Generation+1 || len(updated.Spec.Template.Spec.ImagePullSecrets) != 1 ||
		updated.Annotations[podArtifactImageAnnotation] != "top-level unchanged" ||
		updated.Spec.Template.Annotations["fixture.invalid/unrelated"] != "keep" ||
		updated.Spec.Template.Spec.InitContainers[0].Image != review.Draft.Current.IndependentImage ||
		updated.Status.ObservedGeneration != fresh.Status.ObservedGeneration {
		t.Fatalf("provider did not apply exactly the reviewed image changes: %+v", updated)
	}
	assertRejected(cas)
	observed, err := deployments.Get(ctx, initial.Name, metav1.GetOptions{})
	if err != nil || !apiequality.Semantic.DeepEqual(observed, updated) {
		t.Fatalf("replaying the one-shot patch changed the workload: %v", err)
	}
	companion, err := admin.CoreV1().Services(initial.Namespace).Get(ctx, service.Name, metav1.GetOptions{})
	if err != nil || !apiequality.Semantic.DeepEqual(companion, service) {
		t.Fatalf("registry move modified the companion Service: %v", err)
	}
}
