package sshcontrol

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/sshidentity"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type observationTransport func(*http.Request) (*http.Response, error)

func (f observationTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestKubernetesObserveBindsKnownPreviousArtifactBeforePendingAcceptance(test *testing.T) {
	ctx := context.Background()
	r, _, tenantID, appID := projectionFixture(test)
	state, err := r.Store.GetManagedSSHControl(tenantID, appID)
	if err != nil {
		test.Fatal(err)
	}
	slot := *state.Active
	desired := slot.Artifact
	desired.Digest = "sha256:" + strings.Repeat("e", 64)
	desired.Configuration.Generation++
	slot.BeginApply(desired, time.Now().UTC())
	evidence := *slot.Evidence
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "server", Namespace: slot.Namespace, UID: "server-pod", Labels: slotLabels(slot)}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ssh", Image: slot.Release.Image}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.2", ContainerStatuses: []corev1.ContainerStatus{{Name: "ssh", ContainerID: "containerd://server", Ready: true}}}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: slot.ID, Namespace: slot.Namespace, UID: "service", Labels: slotLabels(slot)}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ClusterIP: "10.0.0.3", Selector: map[string]string{"fugue.pro/ssh-instance": slot.ID}}}
	ready := true
	endpoints := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "endpoints", Namespace: slot.Namespace, Labels: map[string]string{"kubernetes.io/service-name": slot.ID}, OwnerReferences: []metav1.OwnerReference{{Kind: "Service", UID: service.UID}}}, Endpoints: []discoveryv1.Endpoint{{Conditions: discoveryv1.EndpointConditions{Ready: &ready}, TargetRef: &corev1.ObjectReference{Kind: "Pod", UID: pod.UID, Namespace: slot.Namespace}}}}
	client := fake.NewClientset(pod, service, endpoints)
	probes := 0
	runtime := &KubernetesRuntime{client: client, httpClient: &http.Client{Transport: observationTransport(func(*http.Request) (*http.Response, error) {
		raw, err := json.Marshal(evidence)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(raw)))}, err
	})}, probeSSH: func(context.Context, string, ssh.PublicKey, time.Duration) (sshidentity.Observation, error) {
		probes++
		return sshidentity.Observation{KeyExchangeVerified: true, IdentityMatched: true}, nil
	}}
	observed, err := runtime.Observe(ctx, slot)
	if err != nil || observed.ArtifactDigest != slot.LoadedArtifact.Digest || probes != 2 {
		test.Fatal("known loaded authorization rejected or binding probes skipped", err, probes)
	}
	slot.Evidence = &observed
	if slot.Verified(time.Now()) || !slot.Serving(time.Now()) {
		test.Fatal("pending observation verified desired")
	}
	evidence.ArtifactDigest = "sha256:" + strings.Repeat("f", 64)
	if _, err := runtime.Observe(ctx, slot); err == nil {
		test.Fatal("unknown digest accepted by runtime")
	}
	evidence = *slot.LastValidEvidence
	evidence.ServerPodUID = "replacement-pod"
	if _, err := runtime.Observe(ctx, slot); err == nil {
		test.Fatal("receipt crossed server pod identity")
	}
	evidence = *slot.LastValidEvidence
	slot.Artifact.Configuration.Keys = nil
	if _, err := runtime.Observe(ctx, slot); err == nil {
		test.Fatal("revoked loaded grants accepted by runtime")
	}
}
