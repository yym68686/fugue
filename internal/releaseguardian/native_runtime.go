package releaseguardian

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"fugue/internal/declarativerelease"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	NativeRuntimeBindingEnv  = "FUGUE_RELEASE_NATIVE_RUNTIME_BINDING"
	NativeRuntimeRegistryEnv = "FUGUE_RELEASE_NATIVE_REGISTRY"
	NativeRuntimeAuthFileEnv = "FUGUE_RELEASE_NATIVE_REGISTRY_AUTH_FILE"
)

// NativeRuntimeBinding carries the observed DesiredRelease CAS and exact
// rollback anchor into the Guardian child. It is not a credential or a new
// approval: the child must read the actual native pointer from Kubernetes.
type NativeRuntimeBinding struct {
	Key                    Key    `json:"key"`
	DesiredResourceVersion string `json:"desiredResourceVersion"`
	DesiredRecordDigest    string `json:"desiredRecordDigest"`
	LKGMonitorRecordDigest string `json:"lkgMonitorRecordDigest"`
	IntentDigest           string `json:"intentDigest"`
	Fence                  int64  `json:"fence"`
}

func DecodeNativeRuntimeBinding(raw string) (NativeRuntimeBinding, error) {
	var binding NativeRuntimeBinding
	if len(raw) > 1024 || decodeStrict([]byte(raw), &binding) != nil || !binding.valid() {
		return NativeRuntimeBinding{}, errors.New("Guardian native runtime binding is invalid")
	}
	return binding, nil
}

func (b NativeRuntimeBinding) valid() bool {
	return b.Key.Validate() == nil && b.DesiredResourceVersion != "" && len(b.DesiredResourceVersion) <= 128 &&
		!strings.ContainsAny(b.DesiredResourceVersion, " \r\n\t\x00") &&
		digestPattern.MatchString(b.DesiredRecordDigest) && digestPattern.MatchString(b.LKGMonitorRecordDigest) &&
		digestPattern.MatchString(b.IntentDigest) && b.Fence > 0
}

func (b NativeRuntimeBinding) Verify(ctx context.Context, client coreclient.CoreV1Interface, namespace string) error {
	_, err := b.readPointer(ctx, client, namespace)
	return err
}

func (b NativeRuntimeBinding) readPointer(ctx context.Context, client coreclient.CoreV1Interface, namespace string) (DesiredRelease, error) {
	if !b.valid() || client == nil || namespace == "" {
		return DesiredRelease{}, errors.New("Guardian native runtime has no valid DesiredRelease binding")
	}
	current, err := client.ConfigMaps(namespace).Get(ctx, desiredName(b.Key), metav1.GetOptions{})
	if err != nil {
		return DesiredRelease{}, fmt.Errorf("read native DesiredRelease: %w", err)
	}
	var desired DesiredRelease
	if current.ResourceVersion != b.DesiredResourceVersion ||
		current.Annotations[nativeIntentAnnotation] != b.IntentDigest || current.Annotations[nativeFenceAnnotation] != strconv.FormatInt(b.Fence, 10) ||
		decodeStrict([]byte(current.Data["desired.json"]), &desired) != nil || desired.Validate() != nil ||
		desired.Key() != b.Key || desired.RecordDigest != b.DesiredRecordDigest {
		return DesiredRelease{}, errors.New("Guardian native DesiredRelease changed or lost its approval fence")
	}
	return desired, nil
}

// VerifyCandidate also binds caller-supplied files to the immutable release
// record and exact execution snapshot named by the approved pointer. This is
// done before downloads; the caller rechecks the pointer after acquiring Lease.
func (b NativeRuntimeBinding) VerifyCandidate(ctx context.Context, client coreclient.CoreV1Interface, namespace string, files map[string][]byte) error {
	desired, err := b.readPointer(ctx, client, namespace)
	if err != nil {
		return err
	}
	bundle, err := DecodeExecutionBundle(files, b.Key)
	if err != nil {
		return err
	}
	maps := client.ConfigMaps(namespace)
	record, err := maps.Get(ctx, releaseRecordName(b.Key, desired.RecordDigest), metav1.GetOptions{})
	if err != nil || record.Immutable == nil || !*record.Immutable || !stringMapEqual(record.Labels, guardianLabels(b.Key)) ||
		record.Data["lkg-monitor-record-digest"] != b.LKGMonitorRecordDigest {
		return errors.New("native immutable candidate or its rollback binding differs from approval")
	}
	var stored ReleaseRecord
	if decodeStrict([]byte(record.Data["guardian-record.json"]), &stored) != nil || stored.Validate() != nil {
		return errors.New("native immutable candidate envelope is invalid")
	}
	derived, err := bundle.ReleaseRecord(b.Key, stored.LKGRecordDigest)
	if err != nil || stored != derived || stored.RecordDigest != desired.RecordDigest || len(record.Data) != len(executionFileNames)+2 {
		return errors.New("native immutable candidate envelope differs from approval")
	}
	for name, value := range bundle.Files {
		if strings.TrimSpace(record.Data[name]) != string(value) {
			return fmt.Errorf("native immutable candidate file %q differs from approval", name)
		}
	}
	execution, err := maps.Get(ctx, executionSnapshotName(b.Key, desired.Generation), metav1.GetOptions{})
	if err != nil || execution.Immutable == nil || !*execution.Immutable || !stringMapEqual(execution.Labels, guardianLabels(b.Key)) ||
		len(execution.Data) != 2 || execution.Data["record-digest"] != desired.RecordDigest ||
		strings.TrimSpace(execution.Data["execution-plan.json"]) != string(bundle.Files["execution-plan.json"]) {
		// A successfully settled release may have an advanced pointer
		// generation and no candidate execution snapshot. It is eligible only
		// for verified-monitor repair, not candidate dispatch.
		return errors.New("native execution snapshot differs from approval")
	}
	return nil
}

// VerifyNativeDeploymentImages limits this single-workload adapter to one
// reviewed image. Sidecars may reuse it, but mounting or pulling a separate
// image needs its own receipt and a wider artifact approval schema.
func VerifyNativeDeploymentImages(raw []byte, scope declarativerelease.ResourceIdentity, target declarativerelease.TargetIdentity) error {
	ids, err := declarativerelease.ResourceSetIdentities(raw)
	if err != nil || len(ids) != 1 || ids[0] != scope {
		return errors.New("native manifest does not contain only the enrolled Deployment")
	}
	object, err := declarativerelease.ResourceSetItem(raw, scope)
	if err != nil {
		return err
	}
	return verifyNativeDeploymentImageObject(object, target)
}

func verifyNativeDeploymentImageObject(object map[string]any, target declarativerelease.TargetIdentity) error {
	spec, _ := object["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	podSpec, _ := template["spec"].(map[string]any)
	_, err := singleArtifactDeploymentContainers(podSpec, target.ImageRef)
	return err
}

type singleArtifactContainer struct {
	field, name string
	object      map[string]any
}

func singleArtifactDeploymentContainers(podSpec map[string]any, imageRef string) ([]singleArtifactContainer, error) {
	var containers []singleArtifactContainer
	names := map[string]bool{}
	for _, field := range []string{"containers", "initContainers"} {
		list, ok := podSpec[field].([]any)
		if !ok && (field == "containers" || podSpec[field] != nil) || field == "containers" && len(list) == 0 {
			return nil, errors.New("native Deployment container specification is invalid")
		}
		for _, item := range list {
			container, _ := item.(map[string]any)
			name, _ := container["name"].(string)
			if name == "" || names[name] {
				return nil, errors.New("native Deployment container identity is absent or ambiguous")
			}
			if image, _ := container["image"].(string); image != imageRef || image == "" {
				return nil, errors.New("native Deployment uses an image outside its artifact approval")
			}
			names[name] = true
			containers = append(containers, singleArtifactContainer{field: field, name: name, object: container})
		}
	}
	if podSpec["ephemeralContainers"] != nil {
		return nil, errors.New("native Deployment may not declare ephemeral containers")
	}
	volumes, ok := podSpec["volumes"].([]any)
	if !ok && podSpec["volumes"] != nil {
		return nil, errors.New("native Deployment volumes are invalid")
	}
	for _, rawVolume := range volumes {
		volume, ok := rawVolume.(map[string]any)
		if !ok {
			return nil, errors.New("native Deployment volume is invalid")
		}
		if _, found := volume["image"]; found {
			return nil, errors.New("native Deployment may not pull an additional image volume")
		}
	}
	return containers, nil
}
