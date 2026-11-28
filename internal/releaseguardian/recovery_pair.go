package releaseguardian

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

const recoveryHistoryReadLimit = 16

type RecoveryRecordRef struct {
	Name            string `json:"name"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
}

type SealedRecoveryVersion struct {
	Record  RecoveryRecordRef                  `json:"record"`
	Monitor declarativerelease.MonitorRecord   `json:"monitor"`
	Receipt declarativerelease.ArtifactReceipt `json:"receipt"`
}

type SealedRecoveryPair struct {
	Namespace string                `json:"namespace"`
	Component string                `json:"component"`
	Pointer   RecoveryRecordRef     `json:"pointer"`
	Forward   SealedRecoveryVersion `json:"forward"`
	Rollback  SealedRecoveryVersion `json:"rollback"`
}

// ObserveSealedRecoveryPair reads verified monitor records, not desired
// candidates. The current record seals its rollback manifest and execution
// identity, but not the predecessor's receipt. Require a unique independent
// immutable monitor record that proves that exact predecessor. This does not
// prove which version is serving now, retention, or any authority to recover.
func ObserveSealedRecoveryPair(ctx context.Context, client coreclient.CoreV1Interface, namespace, component string) (SealedRecoveryPair, error) {
	return observeSealedRecoveryPair(ctx, client, namespace, component, nil)
}

func observeSealedRecoveryPair(ctx context.Context, client coreclient.CoreV1Interface, namespace, component string, archive *sealedRecoveryArchive) (SealedRecoveryPair, error) {
	if client == nil || !componentPattern.MatchString(namespace) || !componentPattern.MatchString(component) {
		return SealedRecoveryPair{}, errors.New("sealed recovery observation requires a Kubernetes namespace and release component")
	}
	maps := client.ConfigMaps(namespace)
	pointerName := "fugue-release-monitor-" + component
	pointer, err := maps.Get(ctx, pointerName, metav1.GetOptions{})
	if err != nil {
		return SealedRecoveryPair{}, fmt.Errorf("read recovery monitor pointer: %w", err)
	}
	var state declarativerelease.MonitorState
	if !recoveryObjectIs(pointer, namespace, pointerName, component) ||
		decodeStrict([]byte(pointer.Data["state.json"]), &state) != nil || state.Validate() != nil ||
		state.Component != component || pointer.Labels["fugue.pro/config-sha"] != state.ConfigSHA ||
		state.RollbackStatus != "" {
		return SealedRecoveryPair{}, errors.New("recovery monitor pointer has invalid or rolled-back state")
	}
	currentName := monitorRecordNameFromDigest(component, state.RecordDigest)
	if pointer.Data["recordName"] != currentName {
		return SealedRecoveryPair{}, errors.New("recovery monitor pointer differs from its sealed record")
	}
	object, err := maps.Get(ctx, currentName, metav1.GetOptions{})
	if err != nil {
		return SealedRecoveryPair{}, fmt.Errorf("read current recovery record: %w", err)
	}
	current, err := readSealedRecoveryRecord(object, namespace, component)
	if err != nil || current.version.Monitor.RecordDigest != state.RecordDigest || current.prepared.ConfigSHA != state.ConfigSHA {
		return SealedRecoveryPair{}, errors.New("recovery monitor pointer does not bind the current verified record")
	}
	if !current.prepared.LKG.Present || current.workload.Namespace != namespace {
		return SealedRecoveryPair{}, errors.New("current recovery record has no verified rollback workload in its namespace")
	}

	filter := labels.SelectorFromSet(labels.Set{
		"app.kubernetes.io/managed-by": "fugue-declarative-release",
		"fugue.pro/component":          component,
		"fugue.pro/config-sha":         current.prepared.LKG.ConfigSHA,
	})
	history, err := maps.List(ctx, metav1.ListOptions{LabelSelector: filter.String(), Limit: recoveryHistoryReadLimit})
	if err != nil {
		return SealedRecoveryPair{}, fmt.Errorf("read exact rollback release history: %w", err)
	}
	if history.Continue != "" || len(history.Items) > recoveryHistoryReadLimit {
		return SealedRecoveryPair{}, errors.New("rollback release history exceeds the recovery observation bound")
	}
	var rollback sealedRecoveryRecord
	for i := range history.Items {
		item := &history.Items[i]
		if !strings.HasPrefix(item.Name, "fugue-release-record-"+component+"-") || item.Name == currentName {
			continue
		}
		candidate, err := readSealedRecoveryRecord(item, namespace, component)
		if err != nil || candidate.prepared.ConfigSHA != current.prepared.LKG.ConfigSHA {
			return SealedRecoveryPair{}, errors.New("invalid sealed record in rollback release history")
		}
		if !isSealedRollback(current, candidate) {
			continue
		}
		if rollback.version.Record.Name != "" {
			return SealedRecoveryPair{}, errors.New("multiple sealed records match the exact rollback release")
		}
		rollback = candidate
	}
	if rollback.version.Record.Name == "" {
		return SealedRecoveryPair{}, errors.New("no sealed record matches the exact rollback release")
	}

	// A pointer or immutable-object replacement during collection invalidates
	// the observation, including deletion/recreation under the same name.
	for _, ref := range []RecoveryRecordRef{current.version.Record, rollback.version.Record, recoveryRef(pointer)} {
		latest, err := maps.Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil || !recoveryObjectIs(latest, namespace, ref.Name, component) || recoveryRef(latest) != ref {
			return SealedRecoveryPair{}, errors.New("verified recovery records changed during observation")
		}
	}
	pair := SealedRecoveryPair{Namespace: namespace, Component: component, Pointer: recoveryRef(pointer),
		Forward: current.version, Rollback: rollback.version}
	if archive != nil {
		*archive = sealedRecoveryArchive{Schema: recoveryArchiveSchema,
			Namespace: namespace, Component: component, Pointer: pair.Pointer, State: state,
			Forward:  archivedRecoveryRecord{Ref: pair.Forward.Record, Files: current.files},
			Rollback: archivedRecoveryRecord{Ref: pair.Rollback.Record, Files: rollback.files}}
	}
	return pair, nil
}

type sealedRecoveryRecord struct {
	version  SealedRecoveryVersion
	prepared declarativerelease.ExecutionPlan
	workload declarativerelease.Workload
	forward  []byte
	lkg      []byte
	files    map[string]string
}

func readSealedRecoveryRecord(object *corev1.ConfigMap, namespace, component string) (sealedRecoveryRecord, error) {
	if object == nil || !recoveryObjectIs(object, namespace, object.Name, component) ||
		object.Immutable == nil || !*object.Immutable || len(object.BinaryData) != 0 || totalConfigMapBytes(object.Data) > maxRecordBytes {
		return sealedRecoveryRecord{}, errors.New("invalid immutable recovery record metadata")
	}
	record, err := decodeSealedRecoveryRecord(recoveryRef(object), object.Data, component)
	if err != nil || object.Labels["fugue.pro/config-sha"] != record.version.Monitor.ConfigSHA {
		return sealedRecoveryRecord{}, errors.New("invalid binding in immutable recovery record")
	}
	return record, nil
}

func decodeSealedRecoveryRecord(ref RecoveryRecordRef, data map[string]string, component string) (sealedRecoveryRecord, error) {
	if ref.UID == "" || ref.ResourceVersion == "" || totalConfigMapBytes(data) > maxRecordBytes {
		return sealedRecoveryRecord{}, errors.New("invalid immutable recovery record identity or size")
	}
	plan, artifact, prepared, monitor, forward, lkg, err := decodeStableRecord(data)
	release, ok := releaseForComponent(plan, component)
	if err != nil || !ok || monitor.Component != component || prepared.Component != component ||
		ref.Name != monitorRecordNameFromDigest(component, monitor.RecordDigest) ||
		!prepared.Forward.Present || artifact.Component != component || artifact.ConfigSHA != prepared.Forward.ConfigSHA ||
		artifact.ImmutableRef != prepared.Forward.ImageRef || artifact.OCIRevision != prepared.Forward.OCIRevision ||
		artifact.PlanDigest != plan.PlanDigest || artifact.IntentDigest != release.IntentDigest || prepared.IntentDigest != release.IntentDigest {
		return sealedRecoveryRecord{}, errors.New("invalid binding in immutable recovery record")
	}
	return sealedRecoveryRecord{
		version:  SealedRecoveryVersion{Record: ref, Monitor: monitor, Receipt: artifact},
		prepared: prepared, workload: release.Workload, forward: forward, lkg: lkg, files: data,
	}, nil
}

func isSealedRollback(current, rollback sealedRecoveryRecord) bool {
	return current.version.Record.Name != rollback.version.Record.Name && current.prepared.LKG.Present &&
		rollback.prepared.Forward == current.prepared.LKG && bytes.Equal(rollback.forward, current.lkg) &&
		recoveryWorkloadKey(rollback.workload) == recoveryWorkloadKey(current.workload)
}

func recoveryObjectIs(object *corev1.ConfigMap, namespace, name, component string) bool {
	return object != nil && object.Namespace == namespace && object.Name == name && object.UID != "" && object.ResourceVersion != "" && object.DeletionTimestamp == nil &&
		object.Labels["app.kubernetes.io/managed-by"] == "fugue-declarative-release" && object.Labels["fugue.pro/component"] == component
}

func recoveryRef(object *corev1.ConfigMap) RecoveryRecordRef {
	return RecoveryRecordRef{Name: object.Name, UID: string(object.UID), ResourceVersion: object.ResourceVersion}
}

type recoveryWorkload struct {
	resource  declarativerelease.ResourceIdentity
	container string
}

func recoveryWorkloadKey(w declarativerelease.Workload) recoveryWorkload {
	return recoveryWorkload{resource: declarativerelease.ResourceIdentity{
		APIVersion: w.APIVersion, Kind: w.Kind, Namespace: w.Namespace, Name: w.Name,
	}, container: w.Container}
}
