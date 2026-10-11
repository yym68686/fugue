package sshcontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	"fugue/internal/sshidentity"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type acknowledgementReader func(string, string) (model.EdgeSSHAcknowledgement, error)

type BindingRuntime interface {
	ReconcilePublicBinding(context.Context, model.ManagedSSHPublicBinding, managedssh.ControlState, acknowledgementReader) error
}

const publicSelectionAnnotation = "ssh.fugue.dev/transport-selection"
const publicBindingsAnnotation = "ssh.fugue.dev/transport-bindings"

var transportDigestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func selectionDigest(service corev1.Service) (string, error) {
	ports := make([]map[string]any, 0, len(service.Spec.Ports))
	for _, port := range service.Spec.Ports {
		if port.TargetPort.Type != intstr.Int {
			return "", errors.New("SSH transport requires explicit integer target ports")
		}
		ports = append(ports, map[string]any{"name": port.Name, "protocol": string(port.Protocol), "port": port.Port, "targetPort": port.TargetPort.IntVal})
	}
	internalPolicy := ""
	if service.Spec.InternalTrafficPolicy != nil {
		internalPolicy = string(*service.Spec.InternalTrafficPolicy)
	}
	external := append([]string{}, service.Spec.ExternalIPs...)
	raw, err := json.Marshal(map[string]any{"type": string(service.Spec.Type), "selector": service.Spec.Selector, "ports": ports, "publishNotReadyAddresses": service.Spec.PublishNotReadyAddresses, "internalTrafficPolicy": internalPolicy, "externalIPs": external, "externalTrafficPolicy": string(service.Spec.ExternalTrafficPolicy)})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func projectPublicBinding(service corev1.Service, edge EdgeConsumer, pod corev1.Pod, binding model.ManagedSSHPublicBinding, state managedssh.ControlState, ack model.EdgeSSHAcknowledgement, now time.Time) (corev1.Service, bool, error) {
	policy := edge.Publication
	if policy == nil || policy.PortStart < 1024 || policy.PortEnd > 65535 || policy.PortStart > policy.PortEnd || !transportDigestPattern.MatchString(policy.IntentDigest) || binding.PublicPort < policy.PortStart || binding.PublicPort > policy.PortEnd {
		return service, false, managedssh.ErrUnauthorized
	}
	if state.Active == nil || !state.Active.Verified(now) || !state.Active.Artifact.Configuration.ValidUntil.After(now) || state.Active.Artifact.Configuration.Draining || len(state.Active.Artifact.Configuration.Keys) == 0 || binding.AppID != state.AppID || binding.TenantID != state.TenantID || binding.IdentityID != state.IdentityID || state.Active.Artifact.Configuration.Identity.ID != binding.IdentityID || state.Active.Artifact.Configuration.Identity.AppID != binding.AppID || state.Active.Artifact.Configuration.Identity.TenantID != binding.TenantID || binding.EdgeGroupID != "" && binding.EdgeGroupID != edge.EdgeGroupID {
		return service, false, managedssh.ErrCandidateUnverified
	}
	if !ack.Persisted || ack.InstanceUID != string(pod.UID) || ack.EdgeID != edge.EdgeID || ack.EdgeGroupID != edge.EdgeGroupID || !ack.ExpiresAt.After(now) || ack.ObservedAt.After(now.Add(5*time.Second)) || ack.LoadedAt.After(now.Add(5*time.Second)) || ack.ExpiresAt.After(ack.ObservedAt.Add(2*time.Minute)) || ack.RouteDigest != model.SSHRoutesDigest(ack.Routes) {
		return service, false, errors.New("SSH port publication requires fresh exact-instance route evidence")
	}
	matched := 0
	for _, route := range ack.Routes {
		if route.PublicPort == binding.PublicPort {
			if route.AppID != binding.AppID || route.TenantID != binding.TenantID || route.TargetPort != 2222 || route.TargetHost != state.Active.ID+"."+state.Active.Namespace+".svc.cluster.local" || (route.Status != model.AppSSHEndpointStatusReady && route.Status != model.AppSSHEndpointStatusPending) {
				return service, false, errors.New("acknowledged public port belongs to another binding or target")
			}
			matched++
		}
	}
	if matched != 1 {
		return service, false, errors.New("managed binding is absent or ambiguous in the serving front")
	}
	digest, err := selectionDigest(service)
	if err != nil || service.Annotations[publicSelectionAnnotation] != digest || service.Annotations["ssh.fugue.dev/transport-intent"] != policy.IntentDigest || service.Annotations["ssh.fugue.dev/transport-phase"] != "serving" || string(service.UID) != edge.ServiceUID || service.Name != edge.PublicService || service.Namespace != edge.Namespace || service.DeletionTimestamp != nil || service.ResourceVersion == "" || service.Labels["app.kubernetes.io/managed-by"] != "fugue-managed-ssh-transport" || service.Spec.Type != corev1.ServiceTypeClusterIP || !reflect.DeepEqual(service.Spec.Selector, edge.PodLabels) || !reflect.DeepEqual(service.Spec.ExternalIPs, []string{edge.PublicAddress}) || service.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal || service.Spec.InternalTrafficPolicy == nil || *service.Spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyLocal || service.Spec.PublishNotReadyAddresses {
		return service, false, errors.New("public transport ownership or selection drifted")
	}
	if len(service.Spec.Ports) == 0 || service.Spec.LoadBalancerIP != "" || len(service.Status.LoadBalancer.Ingress) != 0 {
		return service, false, errors.New("public transport has no existing verified ports or unexpected external authority")
	}
	bindings := map[string]model.ManagedSSHPublicBinding{}
	if raw := service.Annotations[publicBindingsAnnotation]; raw != "" {
		if len(raw) > 128*1024 || json.Unmarshal([]byte(raw), &bindings) != nil || bindings == nil {
			return service, false, errors.New("public transport binding inventory is invalid")
		}
	}
	portKey := strconv.Itoa(binding.PublicPort)
	if previous, exists := bindings[portKey]; exists && (previous.ID != binding.ID || previous.TenantID != binding.TenantID || previous.AppID != binding.AppID || previous.IdentityID != binding.IdentityID) {
		return service, false, errors.New("public port is retained for another logical binding")
	}
	portName := "ssh-" + portKey
	found := false
	for _, port := range service.Spec.Ports {
		if int(port.Port) == binding.PublicPort || port.Name == portName {
			if found || int(port.Port) != binding.PublicPort || port.Name != portName || port.Protocol != corev1.ProtocolTCP || port.TargetPort.Type != intstr.Int || int(port.TargetPort.IntVal) != binding.PublicPort {
				return service, false, errors.New("public port or name is already used by another transport")
			}
			found = true
		}
	}
	if found && reflect.DeepEqual(bindings[portKey], binding) {
		return service, false, nil
	}
	desired := service.DeepCopy()
	if !found {
		if len(desired.Spec.Ports) >= 100 {
			return service, false, errors.New("SSH transport port inventory reached its declared implementation bound")
		}
		desired.Spec.Ports = append(desired.Spec.Ports, corev1.ServicePort{Name: portName, Protocol: corev1.ProtocolTCP, Port: int32(binding.PublicPort), TargetPort: intstr.FromInt(binding.PublicPort)})
		sort.Slice(desired.Spec.Ports, func(first, second int) bool { return desired.Spec.Ports[first].Port < desired.Spec.Ports[second].Port })
	}
	bindings[portKey] = binding
	raw, err := json.Marshal(bindings)
	if err != nil || len(raw) > 128*1024 {
		return service, false, errors.New("SSH transport binding inventory exceeds bound")
	}
	desired.Annotations[publicBindingsAnnotation] = string(raw)
	desired.Annotations[publicSelectionAnnotation], err = selectionDigest(*desired)
	return *desired, true, err
}

func (runtime *KubernetesRuntime) ReconcilePublicBinding(ctx context.Context, binding model.ManagedSSHPublicBinding, state managedssh.ControlState, readAck acknowledgementReader) error {
	options, err := runtime.options()
	if err != nil {
		return err
	}
	for _, edge := range options.RequiredEdges {
		if edge.Publication == nil || binding.EdgeGroupID != "" && binding.EdgeGroupID != edge.EdgeGroupID {
			continue
		}
		if binding.PublicPort < edge.Publication.PortStart || binding.PublicPort > edge.Publication.PortEnd {
			continue
		}
		pods, err := runtime.client.CoreV1().Pods(edge.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(edge.PodLabels).String(), FieldSelector: "spec.nodeName=" + edge.EdgeID, Limit: 3})
		if err != nil {
			return err
		}
		if pods.Continue != "" || len(pods.Items) != 1 {
			return errors.New("public port candidate is missing or ambiguous")
		}
		pod := pods.Items[0]
		if pod.DeletionTimestamp != nil || pod.UID == "" || pod.Spec.HostNetwork || pod.Spec.NodeName != edge.EdgeID || len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Image != edge.Image || len(pod.Status.ContainerStatuses) != 1 || !pod.Status.ContainerStatuses[0].Ready || strings.TrimPrefix(pod.Status.ContainerStatuses[0].ImageID, "docker-pullable://") != edge.Image {
			return errors.New("public port candidate is not the selected immutable front")
		}
		node, err := runtime.client.CoreV1().Nodes().Get(ctx, edge.EdgeID, metav1.GetOptions{})
		if err != nil {
			return err
		}
		bound := false
		for _, address := range node.Status.Addresses {
			if address.Address == edge.PublicAddress && (address.Type == corev1.NodeInternalIP || address.Type == corev1.NodeExternalIP) {
				bound = true
			}
		}
		if !bound {
			return errors.New("public transport address is no longer assigned to its node")
		}
		ack, err := readAck(edge.EdgeID, string(pod.UID))
		if err != nil {
			return err
		}
		service, err := runtime.client.CoreV1().Services(edge.Namespace).Get(ctx, edge.PublicService, metav1.GetOptions{})
		if err != nil {
			return err
		}
		desired, changed, err := projectPublicBinding(*service, edge, pod, binding, state, ack, time.Now().UTC())
		if err != nil {
			return err
		}
		if !changed {
			continue
		}
		if err := runtime.verifyEdgeTransport(ctx, edge, pod, int(service.Spec.Ports[0].Port)); err != nil {
			return err
		}
		key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(state.Active.Artifact.Configuration.Identity.PublicKey))
		if err != nil {
			return managedssh.ErrIdentityChange
		}
		probe := runtime.probeSSH
		if probe == nil {
			probe = sshidentity.Probe
		}
		observed, err := probe(ctx, net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(binding.PublicPort)), key, 5*time.Second)
		if err != nil || !observed.IdentityMatched || !observed.KeyExchangeVerified || observed.Fingerprint != state.Active.Artifact.Configuration.Identity.Fingerprint {
			return managedssh.ErrCandidateUnverified
		}
		services, err := runtime.client.CoreV1().Services("").List(ctx, metav1.ListOptions{Limit: 10000})
		if err != nil {
			return err
		}
		if services.Continue != "" {
			return errors.New("public port ownership inventory is incomplete")
		}
		for _, other := range services.Items {
			if other.UID == service.UID {
				continue
			}
			addresses := append([]string{}, other.Spec.ExternalIPs...)
			addresses = append(addresses, other.Spec.LoadBalancerIP)
			for _, ingress := range other.Status.LoadBalancer.Ingress {
				addresses = append(addresses, ingress.IP)
			}
			for _, address := range addresses {
				if address == edge.PublicAddress {
					for _, port := range other.Spec.Ports {
						if port.Protocol == corev1.ProtocolTCP && int(port.Port) == binding.PublicPort {
							return errors.New("another Service owns the new SSH public port")
						}
					}
				}
			}
		}
		patch, err := json.Marshal([]map[string]any{{"op": "test", "path": "/metadata/uid", "value": string(service.UID)}, {"op": "test", "path": "/metadata/resourceVersion", "value": service.ResourceVersion}, {"op": "test", "path": "/metadata/annotations", "value": service.Annotations}, {"op": "test", "path": "/metadata/labels", "value": service.Labels}, {"op": "test", "path": "/spec", "value": service.Spec}, {"op": "replace", "path": "/spec/ports", "value": desired.Spec.Ports}, {"op": "replace", "path": "/metadata/annotations", "value": desired.Annotations}})
		if err != nil {
			return err
		}
		if _, err := runtime.client.CoreV1().Services(edge.Namespace).Patch(ctx, service.Name, types.JSONPatchType, patch, metav1.PatchOptions{FieldManager: "fugue-managed-ssh-binding"}); err != nil {
			return fmt.Errorf("publish managed SSH binding port: %w", err)
		}
	}
	return nil
}
