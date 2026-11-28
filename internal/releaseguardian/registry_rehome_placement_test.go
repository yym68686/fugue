package releaseguardian

import (
	"fmt"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestObserveRegistryRehomePlacementIsReadOnlyAndFailsClosed(t *testing.T) {
	for mode, wantError := range map[string]string{
		"healthy moved":                 "",
		"same artifact at old location": "does not use the independent image",
		"deployment spec drift":         "cannot reconstruct the signed source spec",
		"wrong generation":              "not the next reviewed",
		"unobserved rollout":            "has not converged",
		"wrong Pod image digest":        "actual image differs",
		"Pod using the original repo":   "actual image differs",
		"wrong Pod owner":               "actual image differs",
		"Service drift":                 "drifted from its reviewed identity or spec",
		"Service version changed":       "companion Services changed during readback",
		"Deployment version changed":    "Deployment changed during readback",
	} {
		t.Run(mode, func(t *testing.T) {
			source := registryRehomeTestDeployment()
			review := registryRehomeTestReview(t, source)
			cas, err := prepareRegistryRehomeDeploymentCAS(review, source)
			if err != nil {
				t.Fatal(err)
			}
			d := cas.expected.DeepCopy()
			d.Generation++
			d.ResourceVersion = "11"
			replicas := *d.Spec.Replicas
			d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: replicas,
				UpdatedReplicas: replicas, ReadyReplicas: replicas, AvailableReplicas: replicas}

			service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: d.Namespace, Name: "companion", UID: "service-uid", ResourceVersion: "12"},
				Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 443}}}}
			spec, err := declarativerelease.CanonicalJSON(service.Spec)
			if err != nil {
				t.Fatal(err)
			}
			id := declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name}
			review.Source.Services = []RegistryRehomeObservedService{
				{Live: RegistryRehomeObservedObject{id, string(service.UID), service.Generation, digest(spec)}},
			}
			rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: d.Namespace, Name: "serving-rs", UID: "rs-uid", ResourceVersion: "13",
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(d, appsv1.SchemeGroupVersion.WithKind("Deployment"))}}}
			fakePod := func(i int) *corev1.Pod {
				statuses := func(containers []corev1.Container) []corev1.ContainerStatus {
					out := make([]corev1.ContainerStatus, 0, len(containers))
					for _, c := range containers {
						out = append(out, corev1.ContainerStatus{Name: c.Name, ImageID: c.Image})
					}
					return out
				}
				return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: d.Namespace, Name: fmt.Sprintf("rehome-%d", i), UID: types.UID(fmt.Sprintf("pod-%d", i)),
					ResourceVersion: "14", Labels: d.Spec.Template.Labels, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}},
					Spec: *d.Spec.Template.Spec.DeepCopy(),
					Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
						ContainerStatuses: statuses(d.Spec.Template.Spec.Containers), InitContainerStatuses: statuses(d.Spec.Template.Spec.InitContainers)}}
			}
			first, second := fakePod(0), fakePod(1)
			switch mode {
			case "same artifact at old location":
				d.Spec.Template.Spec.InitContainers[0].Image = review.Draft.Current.SourceImage
			case "deployment spec drift":
				d.Spec.Replicas = ptr.To[int32](3)
			case "wrong generation":
				d.Generation++
			case "unobserved rollout":
				d.Status.ObservedGeneration--
			case "wrong Pod image digest":
				first.Status.ContainerStatuses[0].ImageID = "sha256:" + strings.Repeat("9", 64)
			case "Pod using the original repo":
				first.Spec.Containers[0].Image = review.Draft.Current.SourceImage
			case "wrong Pod owner":
				rs.OwnerReferences[0].UID = "other-deployment"
			case "Service drift":
				service.Spec.Ports[0].Port++
			}
			client := fake.NewSimpleClientset(d, rs, first, second, service)
			changed := func(kind string, object func() runtime.Object) {
				reads := 0
				client.PrependReactor("get", kind, func(clienttesting.Action) (bool, runtime.Object, error) {
					reads++
					if reads == 2 {
						return true, object(), nil
					}
					return false, nil, nil
				})
			}
			if mode == "Service version changed" {
				moved := service.DeepCopy()
				moved.ResourceVersion = "30"
				changed("services", func() runtime.Object { return moved })
			}
			if mode == "Deployment version changed" {
				moved := d.DeepCopy()
				moved.ResourceVersion = "30"
				changed("deployments", func() runtime.Object { return moved })
			}
			got, err := observeRegistryRehomePlacement(t.Context(), client, review)
			if wantError == "" {
				if err != nil || got.Deployment != recoveryRef(d) || got.Generation != d.Generation ||
					len(got.Pods) != 2 || len(got.Services) != 1 || got.Services[0] != recoveryRef(service) {
					t.Fatalf("incomplete readback: %+v %v", got, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), wantError) || got.Deployment.UID != "" {
				t.Fatalf("accepted unsafe readback: %+v %v (wanted %q)", got, err, wantError)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("placement readback attempted a provider write: %v", action)
				}
			}
		})
	}
}
