package sshcontrol

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	"fugue/internal/sshidentity"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
)

func publicationFixture(test *testing.T) (corev1.Service, EdgeConsumer, corev1.Pod, model.ManagedSSHPublicBinding, managedssh.ControlState, model.EdgeSSHAcknowledgement, time.Time) {
	test.Helper()
	now := time.Now().UTC()
	policy := &BindingPublicationPolicy{PortStart: 24000, PortEnd: 25000, IntentDigest: "sha256:" + strings.Repeat("d", 64)}
	edge := EdgeConsumer{EdgeID: "edge-a", EdgeGroupID: "group-a", Namespace: "platform", PodLabels: map[string]string{"slot": "front-a"}, PublicAddress: "192.0.2.10", PublicService: "public-ssh", ServiceUID: "service-a", Image: "registry.example/front@sha256:" + strings.Repeat("a", 64), Publication: policy}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "front-a", Namespace: edge.Namespace, UID: "pod-a"}, Spec: corev1.PodSpec{NodeName: edge.EdgeID, Containers: []corev1.Container{{Image: edge.Image}}}, Status: corev1.PodStatus{PodIP: "10.1.1.2", ContainerStatuses: []corev1.ContainerStatus{{ImageID: edge.Image, Ready: true}}}}
	local := corev1.ServiceInternalTrafficPolicyLocal
	service := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: edge.PublicService, Namespace: edge.Namespace, UID: "service-a", ResourceVersion: "1", Labels: map[string]string{"app.kubernetes.io/managed-by": "fugue-managed-ssh-transport"}, Annotations: map[string]string{"ssh.fugue.dev/transport-phase": "serving", "ssh.fugue.dev/transport-intent": policy.IntentDigest}}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ClusterIP: "10.2.2.2", Selector: edge.PodLabels, ExternalIPs: []string{edge.PublicAddress}, ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal, InternalTrafficPolicy: &local, Ports: []corev1.ServicePort{{Name: "ssh-24000", Protocol: corev1.ProtocolTCP, Port: 24000, TargetPort: intstr.FromInt32(24000)}}}}
	digest, err := selectionDigest(service)
	if err != nil {
		test.Fatal(err)
	}
	service.Annotations[publicSelectionAnnotation] = digest
	binding := model.ManagedSSHPublicBinding{ID: "binding-a", AppID: "workspace", TenantID: "tenant", IdentityID: "identity-a", PublicPort: 24001}
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		test.Fatal(err)
	}
	sshKey, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		test.Fatal(err)
	}
	configuration := managedssh.Configuration{Identity: model.AppSSHIdentity{ID: binding.IdentityID, TenantID: binding.TenantID, AppID: binding.AppID, PublicKey: string(ssh.MarshalAuthorizedKey(sshKey)), Fingerprint: ssh.FingerprintSHA256(sshKey)}, Target: managedssh.Target{PodUID: "target-pod", ContainerID: "target-container"}, ValidUntil: now.Add(5 * time.Minute), Keys: []managedssh.KeyGrant{{}}}
	slot := &managedssh.ServingSlot{ID: "backend-a", Namespace: "backends", Artifact: managedssh.Artifact{Digest: "artifact-a", Configuration: configuration}, Evidence: &managedssh.CandidateEvidence{ArtifactDigest: "artifact-a", IdentityID: binding.IdentityID, Fingerprint: configuration.Identity.Fingerprint, ServerPodUID: "server-pod", TargetPodUID: "target-pod", TargetContainerID: "target-container", KeyExchangeVerified: true, RuntimeVerified: true, ObservedAt: now, ExpiresAt: now.Add(time.Minute)}}
	state := managedssh.ControlState{AppID: binding.AppID, TenantID: binding.TenantID, IdentityID: binding.IdentityID, Active: slot}
	ack := model.EdgeSSHAcknowledgement{EdgeSSHAcknowledgementRequest: model.EdgeSSHAcknowledgementRequest{EdgeID: edge.EdgeID, EdgeGroupID: edge.EdgeGroupID, InstanceUID: string(pod.UID), Persisted: true, LoadedAt: now, Connections: []model.EdgeSSHRouteConnections{}}, ObservedAt: now, ExpiresAt: now.Add(time.Minute), Routes: []model.EdgeSSHRoute{{AppID: binding.AppID, TenantID: binding.TenantID, PublicPort: binding.PublicPort, TargetHost: slot.ID + "." + slot.Namespace + ".svc.cluster.local", TargetPort: 2222, Status: model.AppSSHEndpointStatusReady}}}
	ack.RouteDigest = model.SSHRoutesDigest(ack.Routes)
	return service, edge, pod, binding, state, ack, now
}

func TestPublicBindingRuntimeFencesConcurrentServiceChanges(test *testing.T) {
	for _, scenario := range []string{"publish", "concurrent-change", "wrong-host-identity", "outside-policy"} {
		test.Run(scenario, func(test *testing.T) {
			conflict := scenario == "concurrent-change"
			service, edge, pod, binding, state, ack, _ := publicationFixture(test)
			pod.Labels = edge.PodLabels
			ready, controller := true, true
			port, portName, protocol := int32(24000), "ssh-24000", corev1.ProtocolTCP
			slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "selected", Namespace: edge.Namespace, Labels: map[string]string{"kubernetes.io/service-name": service.Name}, OwnerReferences: []metav1.OwnerReference{{Kind: "Service", UID: service.UID, Controller: &controller}}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &portName, Protocol: &protocol, Port: &port}}, Endpoints: []discoveryv1.Endpoint{{NodeName: &edge.EdgeID, Addresses: []string{pod.Status.PodIP}, TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}}}}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: edge.EdgeID}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeExternalIP, Address: edge.PublicAddress}}}}
			client := fake.NewClientset(service.DeepCopy(), pod.DeepCopy(), slice, node)
			patches := 0
			client.PrependReactor("patch", "services", func(action kubetesting.Action) (bool, kuberuntime.Object, error) {
				patches++
				patch := action.(kubetesting.PatchAction)
				var operations []map[string]any
				if json.Unmarshal(patch.GetPatch(), &operations) != nil || len(operations) != 7 || operations[0]["path"] != "/metadata/uid" || operations[1]["path"] != "/metadata/resourceVersion" || operations[4]["path"] != "/spec" {
					test.Fatal("publication lost its exact selection preconditions")
				}
				if conflict {
					changed := service.DeepCopy()
					changed.ResourceVersion = "2"
					if err := client.Tracker().Update(schema.GroupVersionResource{Version: "v1", Resource: "services"}, changed, edge.Namespace); err != nil {
						test.Fatal(err)
					}
				}
				return false, nil, nil
			})
			configuration := KubernetesOptions{Schema: "fugue.managed-ssh-runtime/v1", Namespace: "backends", RuntimeIDs: []string{"runtime_fixture"}, StateStorageClass: "test-storage", IngressNamespace: edge.Namespace, ControllerNamespace: edge.Namespace, ControllerPodLabels: map[string]string{"component": "ssh-control"}, RequiredEdges: []EdgeConsumer{edge}, Releases: map[string]managedssh.RuntimeRelease{"amd64": {Architecture: "amd64", Image: edge.Image, HelperDigest: "sha256:" + strings.Repeat("b", 64)}}}
			raw, err := json.Marshal(configuration)
			if err != nil {
				test.Fatal(err)
			}
			var options map[string]any
			_ = json.Unmarshal(raw, &options)
			options["api_servers"] = []map[string]any{{"cidr": "192.0.2.10/32", "port": 6443}}
			raw, _ = json.Marshal(options)
			path := filepath.Join(test.TempDir(), "runtime.json")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				test.Fatal(err)
			}
			probes := 0
			runtime := &KubernetesRuntime{client: client, ConfigurationFile: path, probeSSH: func(ctx context.Context, target string, key ssh.PublicKey, timeout time.Duration) (sshidentity.Observation, error) {
				probes++
				if target != "10.1.1.2:24001" || ssh.FingerprintSHA256(key) != state.Active.Artifact.Configuration.Identity.Fingerprint || timeout != 5*time.Second || patches != 0 {
					test.Fatal("new port was not probed with its exact identity before publication")
				}
				return sshidentity.Observation{IdentityMatched: scenario != "wrong-host-identity", KeyExchangeVerified: true, Fingerprint: ssh.FingerprintSHA256(key)}, nil
			}}
			if scenario == "outside-policy" {
				binding.PublicPort = 23000
			}
			err = runtime.ReconcilePublicBinding(context.Background(), binding, state, func(edgeID, instanceUID string) (model.EdgeSSHAcknowledgement, error) {
				if edgeID != edge.EdgeID || instanceUID != string(pod.UID) {
					test.Fatal("requested acknowledgement of another instance")
				}
				return ack, nil
			})
			expectedPatches := 1
			expectedProbes := 1
			if scenario == "wrong-host-identity" || scenario == "outside-policy" {
				expectedPatches = 0
			}
			if scenario == "outside-policy" {
				expectedProbes = 0
				if len(client.Actions()) != 0 {
					test.Fatal("unenrolled binding touched a public Service")
				}
			}
			if (err != nil) != (scenario == "concurrent-change" || scenario == "wrong-host-identity") || patches != expectedPatches || probes != expectedProbes {
				test.Fatal("unexpected publication or conflict result", patches, err)
			}
			actual, err := client.CoreV1().Services(edge.Namespace).Get(context.Background(), service.Name, metav1.GetOptions{})
			if err != nil || len(actual.Spec.Ports) != map[bool]int{true: 2, false: 1}[scenario == "publish"] || !reflect.DeepEqual(actual.Spec.Ports[0], service.Spec.Ports[0]) {
				test.Fatal("publication lost previous selection", err)
			}
		})
	}
}

func TestPublicBindingAddsOnlyVerifiedManagedPortAndPreservesExistingSelection(test *testing.T) {
	service, edge, pod, binding, state, ack, now := publicationFixture(test)
	before := service.DeepCopy()
	desired, changed, err := projectPublicBinding(service, edge, pod, binding, state, ack, now)
	if err != nil || !changed || len(desired.Spec.Ports) != 2 || desired.Spec.Ports[1].Port != 24001 || !reflect.DeepEqual(desired.Spec.Ports[0], service.Spec.Ports[0]) || !reflect.DeepEqual(service, *before) {
		test.Fatal("new binding changed existing port or modified its source", err)
	}
	restored := desired.DeepCopy()
	restored.Spec.Ports = before.Spec.Ports
	restored.Annotations = before.Annotations
	if !reflect.DeepEqual(restored, before) {
		test.Fatal("port publication modified unrelated Service state")
	}
	if _, changed, err := projectPublicBinding(desired, edge, pod, binding, state, ack, now); err != nil || changed {
		test.Fatal("same binding was not idempotent", err)
	}
	binding.ID = "other-logical-owner"
	if _, _, err := projectPublicBinding(desired, edge, pod, binding, state, ack, now); err == nil {
		test.Fatal("retained public port reassigned to another logical binding")
	}
}

func TestPublicBindingRejectsUnverifiedOwnershipAndStaleSelection(test *testing.T) {
	for _, scenario := range []string{"disabled policy", "outside range", "foreign tenant", "identity", "native endpoint", "missing route", "old target", "expired ACK", "foreign instance", "unverified backend", "revoked grant", "stale intent", "replaced Service", "selector drift", "occupied port"} {
		test.Run(scenario, func(test *testing.T) {
			service, edge, pod, binding, state, ack, now := publicationFixture(test)
			switch scenario {
			case "disabled policy":
				edge.Publication = nil
			case "outside range":
				edge.Publication.PortEnd = 24000
			case "foreign tenant":
				binding.TenantID = "other"
			case "identity":
				binding.IdentityID = "other"
			case "native endpoint":
				ack.Routes[0].TargetPort = 22
			case "missing route":
				ack.Routes = nil
			case "old target":
				ack.Routes[0].TargetHost = "previous.backups.svc.cluster.local"
			case "expired ACK":
				ack.ExpiresAt = now
			case "foreign instance":
				ack.InstanceUID = "another-pod"
			case "unverified backend":
				state.Active.Evidence = nil
			case "revoked grant":
				state.Active.Artifact.Configuration.Keys = nil
			case "stale intent":
				service.Annotations["ssh.fugue.dev/transport-intent"] = "newer"
			case "replaced Service":
				service.UID = "new-service"
			case "selector drift":
				service.Spec.Selector = map[string]string{"slot": "another"}
			case "occupied port":
				service.Spec.Ports[0].Name = "ssh-24001"
			}
			ack.RouteDigest = model.SSHRoutesDigest(ack.Routes)
			before := service.DeepCopy()
			if _, changed, err := projectPublicBinding(service, edge, pod, binding, state, ack, now); err == nil || changed || !reflect.DeepEqual(service, *before) {
				test.Fatal("unsafe publication was admitted or changed prior Service", err)
			}
		})
	}
}
