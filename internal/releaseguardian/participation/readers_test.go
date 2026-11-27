package participation

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

func TestObserveParticipationReadersTLSAndExactLiveOwnership(t *testing.T) {
	for _, mode := range []string{"valid", "wrong image", "wrong env", "foreign owner", "incomplete pods", "drift", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			var guardianReads, redirects, requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests.Add(1)
				if req.Method != http.MethodGet || req.URL.Query().Has("watch") || req.Body != http.NoBody {
					t.Errorf("unexpected observation request: %s %s", req.Method, req.URL.Path)
					http.Error(w, "bad request", http.StatusBadRequest)
					return
				}
				if req.URL.Path == "/redirect" {
					redirects.Add(1)
					http.Error(w, "unexpected redirect target", http.StatusBadRequest)
					return
				}
				resource := strings.TrimPrefix(req.URL.Path, "/apis/apps/v1/namespaces/fixture/")
				role := "guardian"
				if strings.Contains(resource, "canary") || strings.Contains(req.URL.Query().Get("labelSelector"), "canary") {
					role = "canary-prober"
				}
				d, rs, p := readerObjects(role)
				if role == "guardian" {
					switch mode {
					case "wrong image":
						p.Status.ContainerStatuses[0].ImageID = "registry.example/guardian@sha256:" + strings.Repeat("c", 64)
					case "wrong env":
						p.Spec.Containers[0].Env[1].Value = "cell-a,x,y,77,a,b,c,d,e,f"
					case "foreign owner":
						rs.OwnerReferences[0].UID = "unrelated"
					}
				}
				var response any
				switch req.URL.Path {
				case "/apis/apps/v1/namespaces/fixture/deployments/guardian":
					reads := guardianReads.Add(1)
					if mode == "drift" && reads > 1 {
						d.ResourceVersion = "2"
					}
					if mode == "redirect" {
						http.Redirect(w, req, "/redirect", http.StatusFound)
						return
					}
					response = d
				case "/apis/apps/v1/namespaces/fixture/deployments/canary-prober":
					response = d
				case "/apis/apps/v1/namespaces/fixture/replicasets/" + role + "-rs":
					response = rs
				case "/api/v1/namespaces/fixture/pods":
					if req.URL.Query().Get("labelSelector") != "reader="+role || req.URL.Query().Get("limit") != "33" {
						t.Errorf("unbounded reader Pod list: %s", req.URL.RawQuery)
					}
					result := corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"},
						ListMeta: metav1.ListMeta{ResourceVersion: "40"}, Items: []corev1.Pod{p}}
					if mode == "incomplete pods" {
						result.Continue = "next"
					}
					response = result
				default:
					http.Error(w, "unexpected observation path", http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(response); err != nil {
					t.Errorf("writing fixture: %v", err)
				}
			}))
			defer server.Close()

			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			observer, err := NewKubernetesObserver(&rest.Config{Host: server.URL,
				TLSClientConfig: rest.TLSClientConfig{CAData: ca}, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			policy := productioncontrol.ParticipationPolicy{Namespace: "fixture", Readers: productioncontrol.ParticipationReaders{
				Guardian: "guardian", CanaryProber: "canary-prober",
			}, Members: []productioncontrol.ParticipationMember{{Node: "node-a", Group: "cell-a"}}}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			got, err := observer.ObserveReaderRollouts(ctx, policy, "node-a")
			if mode == "valid" {
				if err != nil || got.Baseline != 2 || len(got.Rollouts) != 2 || got.Rollouts[0].Pods[0].UID != "guardian-pod-uid" ||
					requests.Load() != 12 {
					t.Fatalf("observation=%+v requests=%d error=%v", got, requests.Load(), err)
				}
			} else if err == nil || got.Baseline != 0 || len(got.Rollouts) != 0 || redirects.Load() != 0 {
				t.Fatalf("unproven observation=%+v redirects=%d error=%v", got, redirects.Load(), err)
			}

			before := requests.Load()
			if _, err := observer.core.Nodes().Create(ctx, &corev1.Node{}, metav1.CreateOptions{}); err == nil || requests.Load() != before {
				t.Fatalf("transport allowed an outbound mutation: %v", err)
			}
		})
	}
}

func readerObjects(role string) (appsv1.Deployment, appsv1.ReplicaSet, corev1.Pod) {
	const ns = "fixture"
	controller := true
	revision := strings.Repeat("a", 40)
	image := "registry.example/reader@sha256:" + strings.Repeat("b", 64)
	meta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(name + "-uid"), ResourceVersion: "1",
			Labels: map[string]string{"reader": role}, Annotations: map[string]string{readerRevision: revision}}
	}
	c := corev1.Container{Name: "reader", Image: image, Env: []corev1.EnvVar{{Name: readerRoleEnv, Value: role}}}
	if role == "guardian" {
		c.Env = append(c.Env, corev1.EnvVar{Name: readerBaselineEnv, Value: "cell-a,x,y,2,a,b,c,d,e,f"})
	}
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{readerRevision: revision}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{c}}}
	d := appsv1.Deployment{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: meta(role),
		Spec:   appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"reader": role}}, Template: template},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}}
	d.Generation = 1
	d.Annotations[edgecohort.ReleaseHoldCapabilityAnnotation] = "v1"
	rs := appsv1.ReplicaSet{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "ReplicaSet"}, ObjectMeta: meta(role + "-rs"),
		Spec: appsv1.ReplicaSetSpec{Template: template}}
	rs.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: d.Name, UID: d.UID, Controller: &controller}}
	p := corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: meta(role + "-pod"), Spec: *template.Spec.DeepCopy(),
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: c.Name, Ready: true, ImageID: image,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: &controller}}
	return d, rs, p
}
