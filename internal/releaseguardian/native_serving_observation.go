package releaseguardian

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NativeServingObservation is a read-only witness, not a terminal result.
// It does not prove that an old request already past admission has drained,
// or that the Guardian will not send another request under this claim.
type NativeServingObservation struct {
	Claim             productioncontrol.Claim
	Serving           productioncontrol.Version
	MonitorDigest     string
	DeploymentUID     string
	DeploymentVersion string
	VerifiedPods      []NativeServingPod
	Health            HealthSnapshot
	EvidenceDigest    string
}

type NativeServingPod struct {
	UID             string
	ResourceVersion string
}

// ObserveServingRelease reports positive evidence for the exact candidate or
// rollback version; inconclusive reads leave the claim and last verified
// version unchanged. This method performs only provider and journal reads.
func (p *NativePublisher) ObserveServingRelease(ctx context.Context, intentDigest string) (NativeServingObservation, error) {
	if p == nil || p.store == nil || p.journal == nil {
		return NativeServingObservation{}, errors.New("native Guardian serving observer is not configured")
	}
	intent, payload, err := p.readReleaseIntent(ctx, intentDigest)
	if err != nil {
		return NativeServingObservation{}, err
	}
	scope := productioncontrol.Scope{ClusterID: intent.ClusterID, ResourceID: intent.ResourceID}
	claim, _, err := p.journal.Execution(ctx, scope, intentDigest)
	if err != nil {
		return NativeServingObservation{}, err
	}
	readPointer := func() (NativePublicationObservation, error) {
		return p.observeNativePointer(ctx, intent, payload, NativePublicationObservation{Claim: claim, Pointer: NativePointerUnconfirmed})
	}
	before, err := readPointer()
	if err != nil {
		return NativeServingObservation{}, err
	}
	if before.Desired == nil || before.ResourceVersion == "" || (before.Pointer != NativePointerCandidate && before.Pointer != NativePointerRollback) {
		return NativeServingObservation{}, errors.New("no exact fenced candidate or rollback pointer to observe")
	}
	key := payload.ProposedRecord.Key()
	target := p.store.targets[key]
	if key.Component != target.MonitorComponent {
		return NativeServingObservation{}, errors.New("native serving monitor differs from its workload component")
	}
	bundle, err := DecodeExecutionBundle(payload.Files, key)
	if err != nil {
		return NativeServingObservation{}, err
	}
	maps := p.store.client.CoreV1().ConfigMaps(target.Namespace)
	candidate, err := maps.Get(ctx, releaseRecordName(key, intent.TargetDigest), metav1.GetOptions{})
	var published ReleaseRecord
	if err != nil || candidate.Immutable == nil || !*candidate.Immutable || !stringMapEqual(candidate.Labels, guardianLabels(key)) ||
		decodeStrict([]byte(candidate.Data["guardian-record.json"]), &published) != nil || published != payload.ProposedRecord ||
		!digestPattern.MatchString(candidate.Data["lkg-monitor-record-digest"]) {
		return NativeServingObservation{}, errors.New("native serving observation has no immutable publication and LKG anchor")
	}
	if before.Pointer == NativePointerCandidate {
		binding := NativeRuntimeBinding{Key: key, DesiredResourceVersion: before.ResourceVersion,
			DesiredRecordDigest: intent.TargetDigest, LKGMonitorRecordDigest: candidate.Data["lkg-monitor-record-digest"]}
		if err := binding.VerifyCandidate(ctx, p.store.client.CoreV1(), target.Namespace, payload.Files); err != nil {
			return NativeServingObservation{}, err
		}
	}
	readMonitorPointer := func() (*corev1.ConfigMap, error) {
		pointer, err := maps.Get(ctx, "fugue-release-monitor-"+target.MonitorComponent, metav1.GetOptions{})
		if err != nil || pointer.UID == "" || pointer.ResourceVersion == "" {
			return nil, errors.New("native serving monitor pointer has no verified identity")
		}
		return pointer, nil
	}
	monitorPointer, err := readMonitorPointer()
	if err != nil {
		return NativeServingObservation{}, err
	}
	stableMap, err := p.store.loadStableMonitorMap(ctx, target)
	if err != nil {
		return NativeServingObservation{}, err
	}
	plan, receipt, prepared, monitor, forward, _, err := decodeStableRecord(stableMap.Data)
	if err != nil {
		return NativeServingObservation{}, err
	}
	var state declarativerelease.MonitorState
	if monitorPointer.Data["recordName"] != stableMap.Name || decodeStrict([]byte(monitorPointer.Data["state.json"]), &state) != nil ||
		state.Validate() != nil || state.RecordDigest != monitor.RecordDigest || state.ConfigSHA != monitor.ConfigSHA || state.Component != monitor.Component ||
		stableMap.Labels["fugue.pro/component"] != monitor.Component {
		return NativeServingObservation{}, errors.New("native serving monitor state does not bind the exact verified record")
	}
	release, ok := releaseForComponent(plan, target.MonitorComponent)
	if !ok || monitor.Component != target.MonitorComponent || release.Concurrency != bundle.Release.Concurrency ||
		release.Workload.APIVersion != bundle.Release.Workload.APIVersion ||
		release.Workload.Kind != bundle.Release.Workload.Kind || release.Workload.Namespace != bundle.Release.Workload.Namespace ||
		release.Workload.Name != bundle.Release.Workload.Name {
		return NativeServingObservation{}, errors.New("verified monitor does not cover the enrolled workload")
	}
	switch before.Pointer {
	case NativePointerCandidate:
		if monitor.ExecutionPlanDigest != bundle.Prepared.PlanDigest || receipt != bundle.Artifact || !bytes.Equal(forward, bundle.Forward) {
			return NativeServingObservation{}, errors.New("candidate is not the verified monitor version")
		}
	case NativePointerRollback:
		if monitor.RecordDigest != candidate.Data["lkg-monitor-record-digest"] || digest(forward) != payload.RollbackManifest ||
			!bytes.Equal(forward, bundle.LKG) || prepared.Forward != bundle.Prepared.LKG {
			return NativeServingObservation{}, errors.New("rollback is not the exact verified predecessor")
		}
	}
	workload := release.Workload
	identity := declarativerelease.ResourceIdentity{APIVersion: workload.APIVersion, Kind: workload.Kind, Namespace: workload.Namespace, Name: workload.Name}
	if identity.Kind != "Deployment" || identity.APIVersion != "apps/v1" || nativeKubernetesResourceID(identity) != intent.ResourceID {
		return NativeServingObservation{}, errors.New("native serving resource does not match the production claim")
	}
	if err := VerifyNativeDeploymentImages(forward, identity, prepared.Forward); err != nil {
		return NativeServingObservation{}, err
	}
	deployments := p.store.client.AppsV1().Deployments(identity.Namespace)
	live, err := deployments.Get(ctx, identity.Name, metav1.GetOptions{})
	if err != nil || live.UID == "" || string(live.UID) != bundle.Prepared.Prewrite.UID || live.ResourceVersion == "" {
		return NativeServingObservation{}, errors.New("exact enrolled Deployment is not available")
	}
	if err := nativeDeploymentConverged(live, identity, prepared.Forward, forward, release); err != nil {
		return NativeServingObservation{}, err
	}
	pods, err := p.store.nativeServingPods(ctx, live, receipt)
	if err != nil {
		return NativeServingObservation{}, err
	}
	now := p.store.now().UTC()
	health := HealthSnapshot{
		Local:      p.store.localHealth(ctx, release, prepared.Forward, forward, now),
		Dependency: p.store.dependencyHealth(ctx, target, now),
		Route:      p.store.routeHealth(ctx, target, before.Desired.RecordDigest, now),
	}
	if !allLayersHealthy(health) || health.Validate(now) != nil {
		return NativeServingObservation{}, errors.New("native serving version lacks fresh local, dependency, or route health")
	}
	routeObserved, observedErr := time.Parse(time.RFC3339Nano, health.Route.ObservedAt)
	if observedErr != nil || routeObserved.Before(now.Add(-2*time.Minute)) {
		return NativeServingObservation{}, errors.New("native serving route evidence is too old")
	}
	freshPods, err := p.store.nativeServingPods(ctx, live, receipt)
	if err != nil || !slices.Equal(pods, freshPods) {
		return NativeServingObservation{}, errors.New("serving Pods changed during native serving observation")
	}
	fresh, err := deployments.Get(ctx, identity.Name, metav1.GetOptions{})
	if err != nil || fresh.UID != live.UID || fresh.ResourceVersion != live.ResourceVersion {
		return NativeServingObservation{}, errors.New("Deployment changed during native serving observation")
	}
	latestMonitor, err := readMonitorPointer()
	if err != nil || latestMonitor.UID != monitorPointer.UID || latestMonitor.ResourceVersion != monitorPointer.ResourceVersion ||
		latestMonitor.Data["recordName"] != monitorPointer.Data["recordName"] {
		return NativeServingObservation{}, errors.New("stable monitor changed during native serving observation")
	}
	after, err := readPointer()
	if err != nil || after.Desired == nil || *after.Desired != *before.Desired || after.Pointer != before.Pointer || after.ResourceVersion != before.ResourceVersion {
		return NativeServingObservation{}, errors.New("approved pointer changed during native serving observation")
	}
	witness := NativeServingObservation{
		Claim: claim, Serving: productioncontrol.Version{Generation: before.Desired.Generation, Digest: before.Desired.RecordDigest},
		MonitorDigest: monitor.RecordDigest, DeploymentUID: string(live.UID), DeploymentVersion: live.ResourceVersion,
		VerifiedPods: pods, Health: health,
	}
	raw, err := declarativerelease.CanonicalJSON(witness)
	if err != nil {
		return NativeServingObservation{}, err
	}
	witness.EvidenceDigest = digest(raw)
	return witness, nil
}

func nativeDeploymentConverged(live *appsv1.Deployment, identity declarativerelease.ResourceIdentity, target declarativerelease.TargetIdentity, manifest []byte, release declarativerelease.PlanRelease) error {
	wanted, err := declarativerelease.ResourceSetItem(manifest, identity)
	if err != nil {
		return err
	}
	// TypeMeta is not populated by every client. Keep the actual spec fields as
	// JSON numbers so ResourceDesiredMismatch can distinguish their types.
	object := live.DeepCopy()
	object.APIVersion, object.Kind = identity.APIVersion, identity.Kind
	raw, err := json.Marshal(object)
	if err != nil {
		return err
	}
	var observed map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&observed); err != nil {
		return err
	}
	if mismatch := declarativerelease.ResourceDesiredMismatch(wanted, observed); mismatch != "" {
		return fmt.Errorf("native serving Deployment differs from approved manifest at %s", mismatch)
	}
	return workloadIdentityMatchesDeployment(live, release, target)
}

func (store *KubeStore) nativeServingPods(ctx context.Context, live *appsv1.Deployment, receipt declarativerelease.ArtifactReceipt) ([]NativeServingPod, error) {
	selector, err := podTemplateLabelSelector(live.Spec.Template.Labels)
	if err != nil {
		return nil, err
	}
	list, err := store.client.CoreV1().Pods(live.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, err
	}
	active, _, err := store.deploymentHealthPods(ctx, nonTerminalPods(list.Items), live)
	if err != nil || len(active) < 1 || len(active) != int(derefReplicas(live.Spec.Replicas)) {
		return nil, errors.New("native serving Pod inventory is incomplete")
	}
	sets := map[string]*appsv1.ReplicaSet{}
	pods := make([]NativeServingPod, 0, len(active))
	for _, pod := range active {
		controller := metav1.GetControllerOf(&pod)
		if pod.UID == "" || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || !podReady(pod) || controller == nil ||
			controller.UID == "" || controller.APIVersion != "apps/v1" || controller.Kind != "ReplicaSet" {
			return nil, errors.New("native serving Pod is not a ready Deployment member")
		}
		replicas := sets[controller.Name]
		if replicas == nil {
			if len(sets) >= 32 {
				return nil, errors.New("native serving inventory exceeds bounded ReplicaSet reads")
			}
			replicas, err = store.client.AppsV1().ReplicaSets(live.Namespace).Get(ctx, controller.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			sets[controller.Name] = replicas
		}
		if replicas.UID != controller.UID || !metav1.IsControlledBy(replicas, live) ||
			!nativeServingImages(live.Spec.Template.Spec.Containers, pod.Spec.Containers, pod.Status.ContainerStatuses, receipt) ||
			!nativeServingImages(live.Spec.Template.Spec.InitContainers, pod.Spec.InitContainers, pod.Status.InitContainerStatuses, receipt) ||
			len(pod.Spec.EphemeralContainers) != 0 || hasNativeImageVolumes(pod.Spec.Volumes) {
			return nil, errors.New("native serving Pod owner or actual image differs from approved Deployment")
		}
		pods = append(pods, NativeServingPod{string(pod.UID), pod.ResourceVersion})
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].UID < pods[j].UID })
	return pods, nil
}

func hasNativeImageVolumes(volumes []corev1.Volume) bool {
	for _, volume := range volumes {
		if volume.Image != nil {
			return true
		}
	}
	return false
}

func nativeServingImages(declared, active []corev1.Container, statuses []corev1.ContainerStatus, receipt declarativerelease.ArtifactReceipt) bool {
	if len(declared) != len(active) || len(active) != len(statuses) {
		return false
	}
	for _, container := range declared {
		if container.Image != receipt.ImmutableRef || containerImageMatches(active, container.Name, receipt.ImmutableRef) != nil {
			return false
		}
		found := false
		for _, status := range statuses {
			if status.Name != container.Name {
				continue
			}
			for _, accepted := range []string{receipt.TopDigest, receipt.PlatformManifestDigest} {
				found = found || (accepted != "" && (status.ImageID == accepted ||
					strings.HasSuffix(status.ImageID, "@"+accepted) || strings.HasSuffix(status.ImageID, "://"+accepted)))
			}
		}
		if !found {
			return false
		}
	}
	return true
}
