package releaseguardian

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

// NativeStaticService identifies a companion that the Deployment executor
// must read, but must never create, patch, or delete.
type NativeStaticService struct {
	Declaration RegistryRehomeService
	Prewrite    declarativerelease.ResourceObservation
}

// VerifyNativeStaticServices brackets a publish, mutation or serving check
// with the same enrolled Service identities; there is no Service write path.
func VerifyNativeStaticServices(ctx context.Context, client coreclient.CoreV1Interface, raw []byte, bound []NativeStaticService) error {
	if client == nil && len(bound) != 0 {
		return errors.New("native static Services require a Kubernetes reader")
	}
	for _, service := range bound {
		id := service.Declaration.Identity
		live, err := client.Services(id.Namespace).Get(ctx, id.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read native static Service %s: %w", id.Name, err)
		}
		if string(live.UID) != service.Prewrite.UID || live.ResourceVersion != service.Prewrite.ResourceVersion {
			return fmt.Errorf("native static Service %s differs from its approved UID or resourceVersion", id.Name)
		}
		if err := CompareNativeStaticService(raw, service.Declaration, live); err != nil {
			return err
		}
	}
	return nil
}

// BindNativeStaticServices requires the same declarations in both versions
// and an exact prewrite identity for every object. It never delegates writes
// to the companions, including rolling back to the predecessor.
func BindNativeStaticServices(workload declarativerelease.ResourceIdentity, prepared declarativerelease.ExecutionPlan, forward, rollback []byte) ([]NativeStaticService, error) {
	a, err := VerifyNativeReleaseResourceSet(forward, workload, prepared.Forward)
	if err != nil {
		return nil, err
	}
	b, err := VerifyNativeReleaseResourceSet(rollback, workload, prepared.LKG)
	if err != nil || !slices.Equal(a, b) {
		return nil, errors.Join(errors.New("native release changes a read-only Service declaration"), err)
	}
	prewrite := prepared.Prewrite
	if prewrite.Primary != workload || !prewrite.Present || prewrite.UID == "" || prewrite.ResourceVersion == "" || len(prewrite.Resources) != len(a)+1 {
		return nil, errors.New("native release lacks its complete prewrite resource inventory")
	}
	observed := make(map[declarativerelease.ResourceIdentity]declarativerelease.ResourceObservation, len(prewrite.Resources))
	for _, obj := range prewrite.Resources {
		if _, known := observed[obj.Identity]; known || !obj.Present || obj.UID == "" || obj.ResourceVersion == "" {
			return nil, errors.New("native release prewrite has a missing or ambiguous resource identity")
		}
		observed[obj.Identity] = obj
	}
	if got := observed[workload]; got.UID != prewrite.UID || got.ResourceVersion != prewrite.ResourceVersion {
		return nil, errors.New("native release lacks the exact prewrite Deployment CAS")
	}
	var bound []NativeStaticService
	for _, d := range a {
		obj, exists := observed[d.Identity]
		if !exists {
			return nil, errors.New("native release lacks a prewrite CAS for a read-only Service")
		}
		bound = append(bound, NativeStaticService{Declaration: d, Prewrite: obj})
	}
	return bound, nil
}

// VerifyNativeReleaseResourceSet checks the Deployment image contract and
// returns the exact declarations of any read-only Service companions.
func VerifyNativeReleaseResourceSet(raw []byte, workload declarativerelease.ResourceIdentity, target declarativerelease.TargetIdentity) ([]RegistryRehomeService, error) {
	if workload.Kind != "Deployment" || workload.APIVersion != "apps/v1" || !componentPattern.MatchString(workload.Namespace) || !componentPattern.MatchString(workload.Name) {
		return nil, errors.New("native release has no scoped Deployment")
	}
	ids, err := declarativerelease.ResourceSetIdentities(raw)
	if err != nil {
		return nil, err
	}
	deployment, err := declarativerelease.ResourceSetItem(raw, workload)
	if err != nil {
		return nil, err
	}
	if err := verifyNativeDeploymentImageObject(deployment, target); err != nil {
		return nil, err
	}
	var services []RegistryRehomeService
	for _, id := range ids {
		if id == workload {
			continue
		}
		if !registryRehomeServiceIdentity(id, workload.Namespace) || len(services) == 8 {
			return nil, errors.New("native release only supports a single writable Deployment and bounded read-only Services")
		}
		service, err := declarativerelease.ResourceSetItem(raw, id)
		if err != nil {
			return nil, err
		}
		var value corev1.Service
		encoded, err := declarativerelease.CanonicalJSON(service)
		if err != nil || json.Unmarshal(encoded, &value) != nil || !nativeStaticServiceSpec(value.Spec) {
			return nil, errors.New("native static Service must be an internal ClusterIP with a selector and declared ports")
		}
		shape, err := nativeStaticServiceShape(service, id)
		if err != nil {
			return nil, err
		}
		services = append(services, shape)
	}
	return services, nil
}

func nativeStaticServiceSpec(spec corev1.ServiceSpec) bool {
	return (spec.Type == "" || spec.Type == corev1.ServiceTypeClusterIP) && spec.ClusterIP != corev1.ClusterIPNone &&
		len(spec.Selector) != 0 && len(spec.Ports) != 0 && !spec.PublishNotReadyAddresses && len(spec.ExternalIPs) == 0
}

func nativeStaticServiceShape(item map[string]any, id declarativerelease.ResourceIdentity) (RegistryRehomeService, error) {
	metadata, _ := item["metadata"].(map[string]any)
	// CAS fields refer to this attempt, not the declared Service shape.
	delete(metadata, "uid")
	delete(metadata, "resourceVersion")
	return registryRehomeCompanionShape(item, id, id.Namespace)
}

// CompareNativeStaticService proves the live routing spec is unchanged.
// Only Kubernetes-assigned IPs/families and documented defaults are ignored
// when omitted from the declaration; any explicit value is still checked.
func CompareNativeStaticService(raw []byte, decl RegistryRehomeService, live *corev1.Service) error {
	id := decl.Identity
	if live == nil || live.UID == "" || live.ResourceVersion == "" || live.DeletionTimestamp != nil || live.Namespace != id.Namespace || live.Name != id.Name {
		return errors.New("native static Service has no live provider identity")
	}
	item, err := declarativerelease.ResourceSetItem(raw, id)
	if err != nil {
		return err
	}
	shape, err := nativeStaticServiceShape(item, id)
	if err != nil || shape != decl {
		return errors.New("native static Service declaration changed")
	}
	encoded, err := declarativerelease.CanonicalJSON(item)
	var desired corev1.Service
	if err != nil || json.Unmarshal(encoded, &desired) != nil || !nativeStaticServiceSpec(desired.Spec) || !nativeStaticServiceSpec(live.Spec) {
		return errors.New("native static Service has an unsupported routing spec")
	}
	want, got := desired.Spec.DeepCopy(), live.Spec.DeepCopy()
	for _, spec := range []*corev1.ServiceSpec{want, got} {
		if spec.Type == "" {
			spec.Type = corev1.ServiceTypeClusterIP
		}
		if spec.SessionAffinity == "" {
			spec.SessionAffinity = corev1.ServiceAffinityNone
		}
		if spec.InternalTrafficPolicy != nil && *spec.InternalTrafficPolicy == corev1.ServiceInternalTrafficPolicyCluster {
			spec.InternalTrafficPolicy = nil
		}
		for i := range spec.Ports {
			if spec.Ports[i].Protocol == "" {
				spec.Ports[i].Protocol = corev1.ProtocolTCP
			}
			if spec.Ports[i].TargetPort == (intstr.IntOrString{}) {
				spec.Ports[i].TargetPort = intstr.FromInt32(spec.Ports[i].Port)
			}
		}
	}
	if want.ClusterIP == "" {
		got.ClusterIP = ""
	}
	if len(want.ClusterIPs) == 0 {
		got.ClusterIPs = nil
	}
	if len(want.IPFamilies) == 0 {
		got.IPFamilies = nil
	}
	if want.IPFamilyPolicy == nil && got.IPFamilyPolicy != nil && *got.IPFamilyPolicy == corev1.IPFamilyPolicySingleStack {
		got.IPFamilyPolicy = nil
	}
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("native static Service %s has a different live routing spec", id.Name)
	}
	object, err := rehomeJSONMap(live)
	if err != nil {
		return err
	}
	object["apiVersion"], object["kind"] = id.APIVersion, id.Kind
	delete(item, "spec")
	if mismatch := declarativerelease.ResourceDesiredMismatch(item, object); mismatch != "" {
		return fmt.Errorf("native static Service %s metadata differs at %s", id.Name, mismatch)
	}
	return nil
}
