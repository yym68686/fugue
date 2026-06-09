package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLiveManagedImageReferencesScansAllWorkloadKinds(t *testing.T) {
	t.Parallel()

	const (
		pushBase = "registry.push.example"
		pullBase = "registry.pull.example"
	)
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	kubeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/apps/v1/deployments":
			_ = json.NewEncoder(w).Encode(appsv1.DeploymentList{Items: []appsv1.Deployment{{
				ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "web"},
				Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name:  "web",
						Image: pullBase + "/fugue-apps/demo:current",
					}}},
				}},
			}}})
		case "/apis/apps/v1/statefulsets":
			_ = json.NewEncoder(w).Encode(appsv1.StatefulSetList{Items: []appsv1.StatefulSet{{
				ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "worker"},
				Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{InitContainers: []corev1.Container{{
						Name:  "migrate",
						Image: pullBase + "/fugue-apps/demo@" + digest,
					}}},
				}},
			}}})
		case "/apis/apps/v1/daemonsets":
			_ = json.NewEncoder(w).Encode(appsv1.DaemonSetList{Items: []appsv1.DaemonSet{{
				ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "sidecar"},
				Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name:  "sidecar",
						Image: "example.invalid/sidecar:latest",
						Env: []corev1.EnvVar{{
							Name:  "APP_IMAGE",
							Value: pushBase + "/fugue-apps/demo:env",
						}},
					}}},
				}},
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(kubeAPI.Close)

	server := &Server{
		registryPushBase: pushBase,
		registryPullBase: pullBase,
		newClusterNodeClient: func() (*clusterNodeClient, error) {
			return &clusterNodeClient{
				client:      kubeAPI.Client(),
				baseURL:     kubeAPI.URL,
				bearerToken: "test",
			}, nil
		},
	}

	refs := server.liveManagedImageRefSet(context.Background(), nil)
	for _, expected := range []string{
		pushBase + "/fugue-apps/demo:current",
		pushBase + "/fugue-apps/demo@" + digest,
		pushBase + "/fugue-apps/demo:env",
	} {
		if _, ok := refs[expected]; !ok {
			t.Fatalf("expected live image reference %q, got %#v", expected, refs)
		}
	}
}

func TestManagedImageDigestInUseMatchesPinnedWorkloadDigest(t *testing.T) {
	t.Parallel()

	const candidate = "registry.push.example/fugue-apps/demo:old"
	digest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	server := &Server{
		appImageRegistry: &fakeAppImageRegistry{
			images: map[string]appImageRegistryInspectResult{
				candidate: {
					ImageRef: candidate,
					Digest:   digest,
					Exists:   true,
				},
			},
		},
	}

	inUse, err := server.managedImageDigestInUse(context.Background(), candidate, map[string]struct{}{
		"registry.push.example/fugue-apps/demo@" + digest: {},
	})
	if err != nil {
		t.Fatalf("check managed image digest: %v", err)
	}
	if !inUse {
		t.Fatal("expected digest-pinned workload reference to protect candidate tag")
	}
}
