package sshcontrol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRetirementRequiresFreshExactConsumerAndNoOldStreams(test *testing.T) {
	now := time.Now().UTC()
	activated := now.Add(-time.Minute)
	state := managedssh.ControlState{AppID: "app", TenantID: "tenant", ActivatedAt: &activated, Active: &managedssh.ServingSlot{ID: "new", Namespace: "platform"}}
	binding := model.ManagedSSHPublicBinding{PublicPort: 24000}
	edge := EdgeConsumerInstance{EdgeID: "edge", EdgeGroupID: "group", InstanceUID: "pod"}
	ack := model.EdgeSSHAcknowledgement{EdgeSSHAcknowledgementRequest: model.EdgeSSHAcknowledgementRequest{EdgeID: edge.EdgeID, EdgeGroupID: edge.EdgeGroupID, InstanceUID: edge.InstanceUID, Persisted: true, LoadedAt: now, Connections: []model.EdgeSSHRouteConnections{}}, ObservedAt: now, ExpiresAt: now.Add(time.Minute), Routes: []model.EdgeSSHRoute{{AppID: state.AppID, TenantID: state.TenantID, PublicPort: binding.PublicPort, TargetHost: "new.platform.svc.cluster.local", TargetPort: 2222}}}
	ack.RouteDigest = model.SSHRoutesDigest(ack.Routes)
	if ready, err := retirementAcknowledged(state, binding, edge, ack, now); err != nil || !ready {
		test.Fatal("valid retirement rejected", err)
	}
	for name, mutate := range map[string]func(*model.EdgeSSHAcknowledgement){
		"missing persistence":      func(value *model.EdgeSSHAcknowledgement) { value.Persisted = false },
		"another edge":             func(value *model.EdgeSSHAcknowledgement) { value.EdgeID = "other" },
		"restarted edge":           func(value *model.EdgeSSHAcknowledgement) { value.InstanceUID = "replacement" },
		"another group":            func(value *model.EdgeSSHAcknowledgement) { value.EdgeGroupID = "other" },
		"expired ACK":              func(value *model.EdgeSSHAcknowledgement) { value.ExpiresAt = now.Add(-time.Second) },
		"previous activation":      func(value *model.EdgeSSHAcknowledgement) { value.LoadedAt = activated.Add(-time.Second) },
		"unknown stream inventory": func(value *model.EdgeSSHAcknowledgement) { value.Connections = nil },
		"old live stream": func(value *model.EdgeSSHAcknowledgement) {
			value.Connections = []model.EdgeSSHRouteConnections{{AppID: "app", TargetHost: "old.platform.svc.cluster.local", TargetPort: 2222, Count: 1}}
		},
		"stale route": func(value *model.EdgeSSHAcknowledgement) {
			value.Routes = nil
			value.RouteDigest = model.SSHRoutesDigest(nil)
		},
	} {
		test.Run(name, func(test *testing.T) {
			candidate := ack
			mutate(&candidate)
			if ready, _ := retirementAcknowledged(state, binding, edge, candidate, now); ready {
				test.Fatal("unsafe retirement admitted")
			}
		})
	}
}

func TestStopSlotRequiresServerDrainAndZeroConnections(test *testing.T) {
	now := time.Now().UTC()
	slot := managedssh.ServingSlot{ID: "ssh-old", Namespace: "platform", RetiringAt: &now, Release: managedssh.RuntimeRelease{Image: "registry.example/ssh@sha256:" + strings.Repeat("a", 64)}, Artifact: managedssh.Artifact{Digest: "digest", Configuration: managedssh.Configuration{Draining: true, Identity: model.AppSSHIdentity{ID: "identity", TenantID: "tenant", AppID: "app"}}}}
	status := managedssh.Status{Digest: "digest", IdentityID: "identity", Draining: true, ActiveConnections: 1, ValidUntil: now.Add(time.Hour)}
	health := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { _ = json.NewEncoder(writer).Encode(status) }))
	defer health.Close()
	base, _ := url.Parse(health.URL)
	transport := rewriteRoundTripper{base: base, delegate: http.DefaultTransport}
	replicas := int32(1)
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: slot.ID, Namespace: slot.Namespace, UID: "deployment", ResourceVersion: "1", Labels: slotLabels(slot)}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Image: slot.Release.Image}}}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-pod", Namespace: slot.Namespace, UID: "pod", Labels: slotLabels(slot)}, Status: corev1.PodStatus{PodIP: "192.0.2.10"}}
	client := fake.NewClientset(deployment, pod)
	runtime := &KubernetesRuntime{client: client, httpClient: &http.Client{Transport: transport}}
	for _, check := range []struct {
		draining    bool
		connections int
	}{{true, 1}, {false, 0}} {
		status.Draining, status.ActiveConnections = check.draining, check.connections
		if stopped, err := runtime.StopDrainedSlot(context.Background(), slot); err != nil || stopped {
			test.Fatal("nonempty or undrained slot retired", err)
		}
		current, _ := client.AppsV1().Deployments(slot.Namespace).Get(context.Background(), slot.ID, metav1.GetOptions{})
		if *current.Spec.Replicas != 1 {
			test.Fatal("unsafe slot was scaled down")
		}
	}
	status.Draining, status.ActiveConnections = true, 0
	if stopped, err := runtime.StopDrainedSlot(context.Background(), slot); err != nil || stopped {
		test.Fatal("scaling alone was treated as proof of termination", err)
	}
	current, _ := client.AppsV1().Deployments(slot.Namespace).Get(context.Background(), slot.ID, metav1.GetOptions{})
	if *current.Spec.Replicas != 0 {
		test.Fatal("safe slot was not scaled down")
	}
	if err := client.CoreV1().Pods(slot.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{}); err != nil {
		test.Fatal(err)
	}
	if stopped, err := runtime.StopDrainedSlot(context.Background(), slot); err != nil || !stopped {
		test.Fatal("terminated drained slot was not confirmed", err)
	}
}

type rewriteRoundTripper struct {
	base     *url.URL
	delegate http.RoundTripper
}

func (transport rewriteRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.URL.Scheme, clone.URL.Host = transport.base.Scheme, transport.base.Host
	return transport.delegate.RoundTrip(clone)
}
