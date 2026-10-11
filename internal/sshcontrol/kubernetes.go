package sshcontrol

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/sshdeployment"
	"fugue/internal/sshidentity"
	"fugue/internal/sshkube"

	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubernetesOptions struct {
	Schema                   string                               `json:"schema"`
	Namespace                string                               `json:"namespace"`
	RuntimeIDs               []string                             `json:"runtime_ids"`
	Releases                 map[string]managedssh.RuntimeRelease `json:"releases"`
	StateStorageClass        string                               `json:"state_storage_class"`
	APIServers               []sshdeployment.APIServerPeer        `json:"api_servers"`
	IngressNamespace         string                               `json:"ingress_namespace"`
	IngressPodLabels         map[string]string                    `json:"ingress_pod_labels"`
	IngressCIDRs             []string                             `json:"ingress_cidrs,omitempty"`
	ControllerNamespace      string                               `json:"controller_namespace"`
	ControllerPodLabels      map[string]string                    `json:"controller_pod_labels"`
	RequiredEdges            []EdgeConsumer                       `json:"required_edges,omitempty"`
	RollbackRetentionSeconds int                                  `json:"rollback_retention_seconds,omitempty"`
}

type EdgeConsumer struct {
	EdgeID        string                    `json:"edge_id"`
	EdgeGroupID   string                    `json:"edge_group_id"`
	Namespace     string                    `json:"namespace"`
	PodLabels     map[string]string         `json:"pod_labels"`
	PublicAddress string                    `json:"public_address"`
	PublicService string                    `json:"public_service,omitempty"`
	ServiceUID    string                    `json:"service_uid,omitempty"`
	Image         string                    `json:"image,omitempty"`
	Publication   *BindingPublicationPolicy `json:"binding_publication,omitempty"`
}

type BindingPublicationPolicy struct {
	PortStart    int    `json:"port_start"`
	PortEnd      int    `json:"port_end"`
	IntentDigest string `json:"intent_digest"`
}

type KubernetesRuntime struct {
	ConfigurationFile string
	client            kubernetes.Interface
	dynamic           dynamic.Interface
	httpClient        *http.Client
	probeSSH          func(context.Context, string, ssh.PublicKey, time.Duration) (sshidentity.Observation, error)
}

func NewKubernetesRuntime(configuration *rest.Config, configurationFile string) (*KubernetesRuntime, error) {
	if configuration == nil || configuration.Insecure || !strings.HasPrefix(configuration.Host, "https://") || configurationFile == "" {
		return nil, managedssh.ErrConfiguration
	}
	client, err := kubernetes.NewForConfig(configuration)
	if err != nil {
		return nil, err
	}
	dynamicClient, err := dynamic.NewForConfig(configuration)
	if err != nil {
		return nil, err
	}
	return &KubernetesRuntime{ConfigurationFile: configurationFile, client: client, dynamic: dynamicClient, httpClient: &http.Client{Timeout: 12 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, CheckRedirect: func(request *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (runtime *KubernetesRuntime) options() (KubernetesOptions, error) {
	file, err := os.Open(runtime.ConfigurationFile)
	if err != nil {
		return KubernetesOptions{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 65536 {
		return KubernetesOptions{}, managedssh.ErrConfiguration
	}
	decoder := json.NewDecoder(io.LimitReader(file, 65537))
	decoder.DisallowUnknownFields()
	var options KubernetesOptions
	if err := decoder.Decode(&options); err != nil {
		return options, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return options, managedssh.ErrConfiguration
	}
	if options.Schema != "fugue.managed-ssh-runtime/v1" || len(options.RuntimeIDs) == 0 || len(options.APIServers) == 0 || len(options.APIServers) > 16 || len(options.Releases) == 0 || len(options.Releases) > 2 || len(options.IngressCIDRs) > 64 || len(options.ControllerPodLabels) == 0 {
		return options, managedssh.ErrConfiguration
	}
	for _, name := range []string{options.Namespace, options.StateStorageClass, options.IngressNamespace, options.ControllerNamespace} {
		if len(validation.IsDNS1123Subdomain(name)) != 0 {
			return options, managedssh.ErrConfiguration
		}
	}
	for architecture, release := range options.Releases {
		if release.Architecture != architecture || managedssh.ValidateRuntimeRelease(release) != nil {
			return options, managedssh.ErrConfiguration
		}
	}
	return options, nil
}

func (runtime *KubernetesRuntime) Resolve(ctx context.Context, app model.App, intent model.ManagedSSHIntent, previous managedssh.Target) (managedssh.Target, managedssh.RuntimeRelease, error) {
	options, err := runtime.options()
	if err != nil {
		return managedssh.Target{}, managedssh.RuntimeRelease{}, err
	}
	allowed := false
	for _, identifier := range options.RuntimeIDs {
		if identifier == app.Spec.RuntimeID {
			allowed = true
		}
	}
	if !allowed || app.ID != intent.AppID || app.TenantID != intent.TenantID {
		return managedssh.Target{}, managedssh.RuntimeRelease{}, managedssh.ErrUnauthorized
	}
	target, err := sshkube.ResolveTarget(ctx, runtime.client, app.TenantID, app.ID, runtimepkg.NamespaceForTenant(app.TenantID), runtimepkg.RuntimeAppServiceName(app), intent.Spec, previous)
	if err != nil {
		return target, managedssh.RuntimeRelease{}, err
	}
	pod, err := runtime.client.CoreV1().Pods(target.Namespace).Get(ctx, target.PodName, metav1.GetOptions{})
	if err != nil {
		return target, managedssh.RuntimeRelease{}, err
	}
	if string(pod.UID) != target.PodUID || pod.Spec.NodeName == "" {
		return target, managedssh.RuntimeRelease{}, managedssh.ErrUnavailable
	}
	node, err := runtime.client.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return target, managedssh.RuntimeRelease{}, err
	}
	release, exists := options.Releases[node.Status.NodeInfo.Architecture]
	if !exists || node.Status.NodeInfo.OperatingSystem != "linux" {
		return target, release, managedssh.ErrCapabilities
	}
	return target, release, nil
}

func (runtime *KubernetesRuntime) Apply(ctx context.Context, slot managedssh.ServingSlot, privateKey []byte, trust map[string]ed25519.PublicKey) error {
	options, err := runtime.options()
	if err != nil {
		return err
	}
	configuration, err := managedssh.VerifyArtifact(slot.Artifact, trust)
	if err != nil {
		return err
	}
	signer, err := ssh.ParsePrivateKey(privateKey)
	if err != nil || signer.PublicKey() == nil || ssh.FingerprintSHA256(signer.PublicKey()) != configuration.Identity.Fingerprint {
		return managedssh.ErrIdentityChange
	}
	if slot.Namespace != options.Namespace || slot.Namespace == configuration.Target.Namespace || slot.ID != managedssh.SlotID(configuration.Identity.ID, configuration.Target, slot.Release) {
		return managedssh.ErrConfiguration
	}
	namespace, err := runtime.client.CoreV1().Namespaces().Get(ctx, options.Namespace, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if namespace.Labels["fugue.pro/managed-ssh-control"] != "true" || namespace.DeletionTimestamp != nil {
		return managedssh.ErrUnauthorized
	}
	plan := sshdeployment.DeploymentPlan{Name: slot.ID, Namespace: slot.Namespace, Image: slot.Release.Image, HelperDigest: slot.Release.HelperDigest, Architecture: slot.Release.Architecture, ArtifactSecret: slot.ID + "-artifact", TrustSecret: slot.ID + "-trust", HostKeySecret: slot.ID + "-identity", StateClaim: slot.ID + "-state", APIServerCIDR: options.APIServers[0].CIDR, APIServerPort: options.APIServers[0].Port, AdditionalAPIServers: options.APIServers[1:], IngressNamespace: options.IngressNamespace, IngressPodLabels: options.IngressPodLabels, IngressCIDRs: options.IngressCIDRs, ControllerNamespace: options.ControllerNamespace, ControllerPodLabels: options.ControllerPodLabels, Target: configuration.Target}
	objects, err := sshdeployment.DeploymentObjects(plan)
	if err != nil {
		return err
	}
	artifact, err := json.Marshal(slot.Artifact)
	if err != nil {
		return err
	}
	publicKeys := map[string]string{}
	for identifier, key := range trust {
		publicKeys[identifier] = base64.StdEncoding.EncodeToString(key)
	}
	publicTrust, err := json.Marshal(map[string]any{"schema": "fugue.managed-ssh-trust/v1", "keys": publicKeys})
	if err != nil {
		return err
	}
	immutable := true
	for _, secret := range []corev1.Secret{
		{ObjectMeta: metav1.ObjectMeta{Name: plan.HostKeySecret, Namespace: slot.Namespace}, Immutable: &immutable, Data: map[string][]byte{"host-key": privateKey}},
		{ObjectMeta: metav1.ObjectMeta{Name: plan.TrustSecret, Namespace: slot.Namespace}, Data: map[string][]byte{"trust.json": publicTrust}},
		{ObjectMeta: metav1.ObjectMeta{Name: plan.ArtifactSecret, Namespace: slot.Namespace}, Data: map[string][]byte{"artifact.json": artifact}},
	} {
		secret.Labels = slotLabels(slot)
		if err := runtime.applySecret(ctx, slot, secret); err != nil {
			return err
		}
	}
	claim := corev1.PersistentVolumeClaim{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"}, ObjectMeta: metav1.ObjectMeta{Name: plan.StateClaim, Namespace: slot.Namespace, Labels: slotLabels(slot)}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: &options.StateStorageClass, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}}}
	if err := runtime.applyObject(ctx, slot, claim); err != nil {
		return err
	}
	for _, object := range objects {
		if err := runtime.applyObject(ctx, slot, object); err != nil {
			return err
		}
	}
	return nil
}

func (runtime *KubernetesRuntime) PublishAuthorization(ctx context.Context, slot managedssh.ServingSlot, trust map[string]ed25519.PublicKey) error {
	options, err := runtime.options()
	if err != nil {
		return err
	}
	configuration, err := managedssh.VerifyArtifact(slot.Artifact, trust)
	if err != nil {
		return err
	}
	if slot.Namespace != options.Namespace || slot.Namespace == configuration.Target.Namespace || slot.ID != managedssh.SlotID(configuration.Identity.ID, configuration.Target, slot.Release) {
		return managedssh.ErrConfiguration
	}
	namespace, err := runtime.client.CoreV1().Namespaces().Get(ctx, slot.Namespace, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if namespace.Labels["fugue.pro/managed-ssh-control"] != "true" || namespace.DeletionTimestamp != nil {
		return managedssh.ErrUnauthorized
	}
	artifact, err := json.Marshal(slot.Artifact)
	if err != nil {
		return err
	}
	publicKeys := map[string]string{}
	for identifier, key := range trust {
		publicKeys[identifier] = base64.StdEncoding.EncodeToString(key)
	}
	publicTrust, err := json.Marshal(map[string]any{"schema": "fugue.managed-ssh-trust/v1", "keys": publicKeys})
	if err != nil {
		return err
	}
	for _, secret := range []corev1.Secret{
		{ObjectMeta: metav1.ObjectMeta{Name: slot.ID + "-trust", Namespace: slot.Namespace, Labels: slotLabels(slot)}, Data: map[string][]byte{"trust.json": publicTrust}},
		{ObjectMeta: metav1.ObjectMeta{Name: slot.ID + "-artifact", Namespace: slot.Namespace, Labels: slotLabels(slot)}, Data: map[string][]byte{"artifact.json": artifact}},
	} {
		if err := runtime.updateSecret(ctx, slot, secret, false); err != nil {
			return err
		}
	}
	return nil
}

func slotLabels(slot managedssh.ServingSlot) map[string]string {
	identity := slot.Artifact.Configuration.Identity
	return map[string]string{"app.kubernetes.io/component": "managed-ssh", "fugue.pro/ssh-instance": slot.ID, "fugue.pro/ssh-app-id": identity.AppID, "fugue.pro/ssh-tenant-id": identity.TenantID}
}

func ownedSlotLabels(actual map[string]string, slot managedssh.ServingSlot) bool {
	for key, value := range slotLabels(slot) {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func (runtime *KubernetesRuntime) applySecret(ctx context.Context, slot managedssh.ServingSlot, desired corev1.Secret) error {
	return runtime.updateSecret(ctx, slot, desired, true)
}

func (runtime *KubernetesRuntime) updateSecret(ctx context.Context, slot managedssh.ServingSlot, desired corev1.Secret, create bool) error {
	client := runtime.client.CoreV1().Secrets(slot.Namespace)
	current, err := client.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if !create {
			return nil
		}
		_, err = client.Create(ctx, &desired, metav1.CreateOptions{FieldManager: "fugue-managed-ssh"})
		return safeResourceError("Secret", desired.Name, err)
	}
	if err != nil {
		return safeResourceError("Secret", desired.Name, err)
	}
	if !ownedSlotLabels(current.Labels, slot) || current.DeletionTimestamp != nil {
		return managedssh.ErrUnauthorized
	}
	if desired.Immutable != nil && *desired.Immutable && (current.Immutable == nil || !*current.Immutable) {
		return managedssh.ErrIdentityChange
	}
	if reflect.DeepEqual(current.Data, desired.Data) {
		return nil
	}
	if desired.Immutable != nil && *desired.Immutable {
		if desired.Name == slot.ID+"-identity" && len(current.Data) == 1 && len(desired.Data) == 1 && equivalentHostKeys(current.Data["host-key"], desired.Data["host-key"]) {
			return nil
		}
		return managedssh.ErrIdentityChange
	}
	if encoded, exists := current.Data["artifact.json"]; exists {
		var previous managedssh.Artifact
		if json.Unmarshal(encoded, &previous) != nil || previous.Configuration.Identity.ID != slot.Artifact.Configuration.Identity.ID || previous.Configuration.Generation >= slot.Artifact.Configuration.Generation {
			return managedssh.ErrGeneration
		}
	}
	updated := current.DeepCopy()
	updated.Data = desired.Data
	_, err = client.Update(ctx, updated, metav1.UpdateOptions{FieldManager: "fugue-managed-ssh"})
	return safeResourceError("Secret", desired.Name, err)
}

func equivalentHostKeys(current, desired []byte) bool {
	decode := func(encoded []byte) ed25519.PrivateKey {
		key, err := ssh.ParseRawPrivateKey(encoded)
		if err != nil {
			return nil
		}
		switch decoded := key.(type) {
		case ed25519.PrivateKey:
			return decoded
		case *ed25519.PrivateKey:
			return *decoded
		default:
			return nil
		}
	}
	currentKey, desiredKey := decode(current), decode(desired)
	defer clear(currentKey)
	defer clear(desiredKey)
	return len(currentKey) == ed25519.PrivateKeySize && len(desiredKey) == ed25519.PrivateKeySize && subtle.ConstantTimeCompare(currentKey, desiredKey) == 1
}

var managedSSHResources = map[string]schema.GroupVersionResource{
	"v1/ServiceAccount": {Version: "v1", Resource: "serviceaccounts"}, "v1/Service": {Version: "v1", Resource: "services"}, "v1/PersistentVolumeClaim": {Version: "v1", Resource: "persistentvolumeclaims"},
	"rbac.authorization.k8s.io/v1/Role": {Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}, "rbac.authorization.k8s.io/v1/RoleBinding": {Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"},
	"apps/v1/Deployment": {Group: "apps", Version: "v1", Resource: "deployments"}, "networking.k8s.io/v1/NetworkPolicy": {Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"},
}

func (runtime *KubernetesRuntime) applyObject(ctx context.Context, slot managedssh.ServingSlot, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var object unstructured.Unstructured
	if err := object.UnmarshalJSON(raw); err != nil {
		return err
	}
	resource, allowed := managedSSHResources[object.GetAPIVersion()+"/"+object.GetKind()]
	if !allowed {
		return managedssh.ErrConfiguration
	}
	if object.GetNamespace() != slot.Namespace && (object.GetNamespace() != slot.Artifact.Configuration.Target.Namespace || (object.GetKind() != "Role" && object.GetKind() != "RoleBinding")) {
		return managedssh.ErrUnauthorized
	}
	object.SetLabels(slotLabels(slot))
	client := runtime.dynamic.Resource(resource).Namespace(object.GetNamespace())
	current, err := client.Get(ctx, object.GetName(), metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return safeResourceError(object.GetKind(), object.GetName(), err)
	}
	if err == nil {
		if !ownedSlotLabels(current.GetLabels(), slot) || current.GetDeletionTimestamp() != nil {
			return managedssh.ErrUnauthorized
		}
		if slot.RetiringAt != nil && object.GetKind() == "Deployment" {
			if replicas, found, err := unstructured.NestedInt64(current.Object, "spec", "replicas"); err != nil {
				return err
			} else if found && replicas == 0 {
				if err := unstructured.SetNestedField(object.Object, int64(0), "spec", "replicas"); err != nil {
					return err
				}
			}
		}
		desired, err := object.MarshalJSON()
		if err != nil {
			return err
		}
		digest := fmt.Sprintf("sha256:%x", sha256.Sum256(desired))
		annotations := object.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations["ssh.fugue.dev/resource-intent-digest"] = digest
		object.SetAnnotations(annotations)
		if containsDesiredObject(current.Object, object.Object) {
			return nil
		}
		object.SetResourceVersion(current.GetResourceVersion())
	}
	raw, err = object.MarshalJSON()
	if err != nil {
		return err
	}
	_, err = client.Patch(ctx, object.GetName(), types.ApplyPatchType, raw, metav1.PatchOptions{FieldManager: "fugue-managed-ssh"})
	return safeResourceError(object.GetKind(), object.GetName(), err)
}

func containsDesiredObject(current, desired any) bool {
	if desired == nil {
		return true
	}
	switch expected := desired.(type) {
	case map[string]any:
		actual, ok := current.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range expected {
			if !containsDesiredObject(actual[key], value) {
				return false
			}
		}
		return true
	case []any:
		actual, ok := current.([]any)
		if !ok || len(actual) != len(expected) {
			return false
		}
		for index, value := range expected {
			if !containsDesiredObject(actual[index], value) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(current, desired)
	}
}

func safeResourceError(kind, name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("managed SSH %s %s operation failed: %s", kind, name, apierrors.ReasonForError(err))
}

func (runtime *KubernetesRuntime) Observe(ctx context.Context, slot managedssh.ServingSlot) (managedssh.CandidateEvidence, error) {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	pods, err := runtime.client.CoreV1().Pods(slot.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Set{"fugue.pro/ssh-instance": slot.ID}.String(), Limit: 2})
	if err != nil {
		return managedssh.CandidateEvidence{}, err
	}
	if len(pods.Items) != 1 || pods.Continue != "" {
		return managedssh.CandidateEvidence{}, managedssh.ErrUnavailable
	}
	pod := pods.Items[0]
	if !ownedSlotLabels(pod.Labels, slot) || pod.DeletionTimestamp != nil || pod.Spec.HostNetwork || len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Image != slot.Release.Image || pod.Status.Phase != corev1.PodRunning || net.ParseIP(pod.Status.PodIP) == nil || net.ParseIP(pod.Status.PodIP).IsLoopback() || net.ParseIP(pod.Status.PodIP).IsUnspecified() {
		return managedssh.CandidateEvidence{}, managedssh.ErrUnavailable
	}
	if len(pod.Status.ContainerStatuses) != 1 || pod.Status.ContainerStatuses[0].ContainerID == "" || !pod.Status.ContainerStatuses[0].Ready {
		return managedssh.CandidateEvidence{}, managedssh.ErrUnavailable
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(pod.Status.PodIP, "7837")+"/readyz", nil)
	if err != nil {
		return managedssh.CandidateEvidence{}, err
	}
	response, err := runtime.httpClient.Do(request)
	if err != nil {
		return managedssh.CandidateEvidence{}, managedssh.ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return managedssh.CandidateEvidence{}, managedssh.ErrUnavailable
	}
	var evidence managedssh.CandidateEvidence
	if err := json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&evidence); err != nil {
		return evidence, managedssh.ErrCandidateUnverified
	}
	if evidence.ServerPodUID != string(pod.UID) {
		return evidence, managedssh.ErrCandidateUnverified
	}
	publicKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(slot.Artifact.Configuration.Identity.PublicKey))
	if err != nil {
		return evidence, managedssh.ErrIdentityChange
	}
	probe, err := sshidentity.Probe(ctx, net.JoinHostPort(pod.Status.PodIP, "2222"), publicKey, 5*time.Second)
	if err != nil || !probe.KeyExchangeVerified || !probe.IdentityMatched {
		return evidence, errors.Join(managedssh.ErrCandidateUnverified, err)
	}
	service, err := runtime.client.CoreV1().Services(slot.Namespace).Get(ctx, slot.ID, metav1.GetOptions{})
	if err != nil || !ownedSlotLabels(service.Labels, slot) || service.DeletionTimestamp != nil || service.Spec.Type != corev1.ServiceTypeClusterIP || net.ParseIP(service.Spec.ClusterIP) == nil || service.Spec.Selector["fugue.pro/ssh-instance"] != slot.ID {
		return evidence, managedssh.ErrCandidateUnverified
	}
	endpoints, err := runtime.client.DiscoveryV1().EndpointSlices(slot.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Set{"kubernetes.io/service-name": service.Name}.String(), Limit: 4})
	if err != nil || endpoints.Continue != "" {
		return evidence, managedssh.ErrCandidateUnverified
	}
	servingEndpoint := false
	for _, slice := range endpoints.Items {
		owned := false
		for _, owner := range slice.OwnerReferences {
			if owner.Kind == "Service" && owner.UID == service.UID {
				owned = true
			}
		}
		if !owned {
			return evidence, managedssh.ErrCandidateUnverified
		}
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready {
				continue
			}
			if endpoint.TargetRef == nil || endpoint.TargetRef.Kind != "Pod" || endpoint.TargetRef.UID != pod.UID || endpoint.TargetRef.Namespace != slot.Namespace || (endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating) {
				return evidence, managedssh.ErrCandidateUnverified
			}
			servingEndpoint = true
		}
	}
	if !servingEndpoint {
		return evidence, managedssh.ErrCandidateUnverified
	}
	serviceProbe, err := sshidentity.Probe(ctx, net.JoinHostPort(service.Spec.ClusterIP, "2222"), publicKey, 5*time.Second)
	if err != nil || !serviceProbe.KeyExchangeVerified || !serviceProbe.IdentityMatched {
		return evidence, errors.Join(managedssh.ErrCandidateUnverified, err)
	}
	evidence.KeyExchangeVerified = true
	current, err := runtime.client.CoreV1().Pods(slot.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || current.UID != pod.UID || current.DeletionTimestamp != nil || current.Status.PodIP != pod.Status.PodIP || len(current.Status.ContainerStatuses) != 1 || current.Status.ContainerStatuses[0].ContainerID != pod.Status.ContainerStatuses[0].ContainerID {
		return evidence, managedssh.ErrCandidateUnverified
	}
	observed := slot
	observed.Evidence = &evidence
	if !observed.Verified(time.Now().UTC()) {
		return evidence, fmt.Errorf("runtime bound evidence: %s: %w", observed.VerificationFailure(time.Now().UTC()), managedssh.ErrCandidateUnverified)
	}
	return evidence, nil
}
