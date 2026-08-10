package releaseguardian

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const (
	maxRecordBytes   = 900 << 10
	defaultFreshness = 45 * time.Second
)

type TargetConfig struct {
	Key               Key
	Namespace         string
	MonitorComponent  string
	DependencyService string
}

func (config TargetConfig) Validate() error {
	if config.Key.Validate() != nil || !componentPattern.MatchString(config.Namespace) ||
		!componentPattern.MatchString(config.MonitorComponent) || !componentPattern.MatchString(config.DependencyService) {
		return errors.New("release guardian target configuration is invalid")
	}
	return nil
}

type KubeStore struct {
	client  kubernetes.Interface
	targets map[Key]TargetConfig
	now     func() time.Time
}

func NewKubeStore(client kubernetes.Interface, targets []TargetConfig) (*KubeStore, error) {
	if client == nil || len(targets) == 0 {
		return nil, errors.New("release guardian Kubernetes store configuration is invalid")
	}
	values := make(map[Key]TargetConfig, len(targets))
	for _, target := range targets {
		if target.Validate() != nil {
			return nil, errors.New("release guardian Kubernetes target is invalid")
		}
		if _, exists := values[target.Key]; exists {
			return nil, errors.New("release guardian Kubernetes target is duplicated")
		}
		values[target.Key] = target
	}
	return &KubeStore{client: client, targets: values, now: time.Now}, nil
}

func (store *KubeStore) Keys() []Key {
	keys := make([]Key, 0, len(store.targets))
	for key := range store.targets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool { return keys[left].String() < keys[right].String() })
	return keys
}

type storedRelease struct {
	record              ReleaseRecord
	desired             DesiredRelease
	currentRecordDigest string
	bundle              ExecutionBundle
	currentBundle       ExecutionBundle
	currentMonitorData  map[string]string
	desiredRV           string
	statusRV            string
	managed             bool
	lkgMonitorDigest    string
}

func (store *KubeStore) Load(ctx context.Context, key Key) (Snapshot, error) {
	target, exists := store.targets[key]
	if !exists {
		return Snapshot{}, errors.New("release guardian target is not configured")
	}
	stored, err := store.loadRelease(ctx, target)
	if err != nil {
		return Snapshot{}, err
	}
	now := store.now().UTC()
	local := store.localHealth(ctx, stored.currentBundle.Release, stored.currentBundle.Prepared.Forward, now)
	dependency := store.dependencyHealth(ctx, target, now)
	route := store.routeHealth(ctx, target, stored.record.RecordDigest, now)
	return Snapshot{
		Key: key, Record: stored.record, Desired: stored.desired,
		CurrentRecordDigest: stored.currentRecordDigest, LastSuccessfulLKG: stored.currentRecordDigest,
		Health:                HealthSnapshot{Local: local, Dependency: dependency, Route: route},
		StatusResourceVersion: stored.statusRV, DesiredResourceVersion: stored.desiredRV,
		Bundle: stored.bundle, CurrentMonitorData: stored.currentMonitorData,
		LKGMonitorRecordDigest: stored.lkgMonitorDigest, Managed: stored.managed,
	}, nil
}

func (store *KubeStore) LoadRecord(ctx context.Context, key Key) (ReleaseRecord, error) {
	target, exists := store.targets[key]
	if !exists {
		return ReleaseRecord{}, errors.New("release guardian target is not configured")
	}
	stored, err := store.loadRelease(ctx, target)
	if err != nil {
		return ReleaseRecord{}, err
	}
	return stored.record, nil
}

func (store *KubeStore) loadRelease(ctx context.Context, target TargetConfig) (storedRelease, error) {
	configMaps := store.client.CoreV1().ConfigMaps(target.Namespace)
	stateName := "fugue-release-monitor-" + target.MonitorComponent
	state, err := configMaps.Get(ctx, stateName, metav1.GetOptions{})
	if err != nil {
		return storedRelease{}, fmt.Errorf("read stable monitor pointer: %w", err)
	}
	recordName := strings.TrimSpace(state.Data["recordName"])
	if !strings.HasPrefix(recordName, "fugue-release-record-"+target.MonitorComponent+"-") {
		return storedRelease{}, errors.New("stable monitor pointer is invalid")
	}
	recordMap, err := configMaps.Get(ctx, recordName, metav1.GetOptions{})
	if err != nil {
		return storedRelease{}, fmt.Errorf("read immutable stable release record: %w", err)
	}
	if recordMap.Immutable == nil || !*recordMap.Immutable || totalConfigMapBytes(recordMap.Data) > maxRecordBytes {
		return storedRelease{}, errors.New("stable release record metadata is invalid")
	}
	plan, artifact, prepared, monitor, forward, lkg, err := decodeStableRecord(recordMap.Data)
	if err != nil {
		return storedRelease{}, err
	}
	if monitor.Component != target.MonitorComponent || prepared.Component != target.MonitorComponent || len(plan.Releases) != 1 {
		return storedRelease{}, errors.New("stable release record component binding is invalid")
	}
	release := plan.Releases[0]
	healthRaw, err := declarativerelease.CanonicalJSON(release.Health)
	if err != nil {
		return storedRelease{}, err
	}
	stableRecord, err := NewReleaseRecord(target.Key, prepared.ConfigSHA, artifact.TopDigest, monitor.ForwardManifestDigest, monitor.RecordDigest, digest(healthRaw))
	if err != nil {
		return storedRelease{}, err
	}
	stableFiles, err := executionFilesFromStrings(recordMap.Data)
	if err != nil {
		return storedRelease{}, err
	}
	stableBundle := ExecutionBundle{Plan: plan, Artifact: artifact, Prepared: prepared, Release: release, Forward: forward, LKG: lkg, Files: stableFiles}
	desired := DesiredRelease{APIVersion: APIVersion, Kind: DesiredReleaseKind, Component: target.Key.Component, Group: target.Key.Group, RecordDigest: stableRecord.RecordDigest, Generation: 1}
	selectedRecord, selectedBundle := stableRecord, stableBundle
	lkgMonitorDigest := ""
	desiredRV := ""
	managed := false
	desiredMap, desiredErr := configMaps.Get(ctx, desiredName(target.Key), metav1.GetOptions{})
	if desiredErr == nil {
		managed = true
		desiredRV = desiredMap.ResourceVersion
		if err := decodeStrict([]byte(desiredMap.Data["desired.json"]), &desired); err != nil || desired.Key() != target.Key || desired.Validate() != nil {
			return storedRelease{}, errors.New("desired release pointer is invalid")
		}
		if desired.RecordDigest != stableRecord.RecordDigest {
			candidateMap, err := configMaps.Get(ctx, releaseRecordName(target.Key, desired.RecordDigest), metav1.GetOptions{})
			if err != nil {
				return storedRelease{}, fmt.Errorf("read immutable Guardian release record: %w", err)
			}
			if candidateMap.Immutable == nil || !*candidateMap.Immutable || totalConfigMapBytes(candidateMap.Data) > maxRecordBytes {
				return storedRelease{}, errors.New("Guardian release record metadata is invalid")
			}
			if err := decodeStrict([]byte(candidateMap.Data["guardian-record.json"]), &selectedRecord); err != nil || selectedRecord.Validate() != nil || selectedRecord.Key() != target.Key || selectedRecord.RecordDigest != desired.RecordDigest {
				return storedRelease{}, errors.New("Guardian release record envelope is invalid")
			}
			lkgMonitorDigest = strings.TrimSpace(candidateMap.Data["lkg-monitor-record-digest"])
			if !digestPattern.MatchString(lkgMonitorDigest) {
				return storedRelease{}, errors.New("Guardian release record LKG monitor binding is invalid")
			}
			candidateFiles, err := executionFilesFromStrings(candidateMap.Data)
			if err != nil {
				return storedRelease{}, err
			}
			selectedBundle, err = DecodeExecutionBundle(candidateFiles, target.Key)
			if err != nil {
				return storedRelease{}, err
			}
			want, err := selectedBundle.ReleaseRecord(target.Key, selectedRecord.LKGRecordDigest)
			if err != nil || want != selectedRecord {
				return storedRelease{}, errors.New("Guardian release record does not match its canonical execution bundle")
			}
		}
	} else if !apierrors.IsNotFound(desiredErr) {
		return storedRelease{}, desiredErr
	}
	if managed && lkgMonitorDigest == "" {
		lkgMonitorDigest, err = store.findLKGMonitorDigest(ctx, target, prepared.LKG, monitor.LKGManifestDigest)
		if err != nil {
			return storedRelease{}, err
		}
	}
	currentRecordDigest, currentBundle := stableRecord.RecordDigest, stableBundle
	if stableMatchesRecord(monitor, artifact, selectedRecord) {
		currentRecordDigest, currentBundle = selectedRecord.RecordDigest, selectedBundle
	}
	statusRV := ""
	if status, statusErr := configMaps.Get(ctx, statusName(target.Key), metav1.GetOptions{}); statusErr == nil {
		statusRV = status.ResourceVersion
	} else if !apierrors.IsNotFound(statusErr) {
		return storedRelease{}, statusErr
	}
	monitorData := make(map[string]string, len(recordMap.Data))
	for name, value := range recordMap.Data {
		monitorData[name] = value
	}
	return storedRelease{
		record: selectedRecord, desired: desired, currentRecordDigest: currentRecordDigest,
		bundle: selectedBundle, currentBundle: currentBundle, currentMonitorData: monitorData,
		desiredRV: desiredRV, statusRV: statusRV, managed: managed, lkgMonitorDigest: lkgMonitorDigest,
	}, nil
}

func stableMatchesRecord(monitor declarativerelease.MonitorRecord, artifact declarativerelease.ArtifactReceipt, record ReleaseRecord) bool {
	return monitor.ConfigSHA == record.ConfigSHA && artifact.TopDigest == record.ImageDigest && monitor.ForwardManifestDigest == record.ManifestDigest
}

func (store *KubeStore) findLKGMonitorDigest(ctx context.Context, target TargetConfig, lkg declarativerelease.TargetIdentity, lkgManifestDigest string) (string, error) {
	if !lkg.Present || !digestPattern.MatchString(lkgManifestDigest) {
		return "", errors.New("current stable release has no valid LKG identity")
	}
	values, err := store.client.CoreV1().ConfigMaps(target.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "fugue.pro/component=" + target.MonitorComponent})
	if err != nil {
		return "", err
	}
	match := ""
	for _, value := range values.Items {
		if !strings.HasPrefix(value.Name, "fugue-release-record-"+target.MonitorComponent+"-") || value.Immutable == nil || !*value.Immutable {
			continue
		}
		_, artifact, prepared, monitor, _, _, decodeErr := decodeStableRecord(value.Data)
		if decodeErr != nil || monitor.ForwardManifestDigest != lkgManifestDigest || prepared.Forward.ConfigSHA != lkg.ConfigSHA ||
			prepared.Forward.OCIRevision != lkg.OCIRevision || prepared.Forward.ImageRef != lkg.ImageRef || artifact.ImmutableRef != lkg.ImageRef {
			continue
		}
		if match != "" && match != monitor.RecordDigest {
			return "", errors.New("multiple immutable monitor records match the current LKG")
		}
		match = monitor.RecordDigest
	}
	if match == "" {
		return "", errors.New("immutable monitor record for the current LKG is absent")
	}
	return match, nil
}

func decodeStableRecord(data map[string]string) (declarativerelease.Plan, declarativerelease.ArtifactReceipt, declarativerelease.ExecutionPlan, declarativerelease.MonitorRecord, []byte, []byte, error) {
	for _, name := range []string{"release-plan.json", "artifact-receipt.json", "execution-plan.json", "forward.json", "lkg.json", "record.json", "terminal-result.json"} {
		if strings.TrimSpace(data[name]) == "" {
			return declarativerelease.Plan{}, declarativerelease.ArtifactReceipt{}, declarativerelease.ExecutionPlan{}, declarativerelease.MonitorRecord{}, nil, nil, fmt.Errorf("stable release record lacks %s", name)
		}
	}
	forward, lkg := []byte(data["forward.json"]), []byte(data["lkg.json"])
	plan, err := declarativerelease.DecodePlan(strings.NewReader(data["release-plan.json"]))
	if err != nil {
		return declarativerelease.Plan{}, declarativerelease.ArtifactReceipt{}, declarativerelease.ExecutionPlan{}, declarativerelease.MonitorRecord{}, nil, nil, err
	}
	artifact, err := declarativerelease.DecodeArtifactReceipt(strings.NewReader(data["artifact-receipt.json"]))
	if err != nil {
		return declarativerelease.Plan{}, declarativerelease.ArtifactReceipt{}, declarativerelease.ExecutionPlan{}, declarativerelease.MonitorRecord{}, nil, nil, err
	}
	prepared, err := declarativerelease.DecodeRecordedExecutionPlan(strings.NewReader(data["execution-plan.json"]), plan, forward, lkg)
	if err != nil {
		return declarativerelease.Plan{}, declarativerelease.ArtifactReceipt{}, declarativerelease.ExecutionPlan{}, declarativerelease.MonitorRecord{}, nil, nil, err
	}
	var monitor declarativerelease.MonitorRecord
	if err := decodeStrict([]byte(data["record.json"]), &monitor); err != nil {
		return declarativerelease.Plan{}, declarativerelease.ArtifactReceipt{}, declarativerelease.ExecutionPlan{}, declarativerelease.MonitorRecord{}, nil, nil, err
	}
	terminal, err := declarativerelease.DecodeExecutionResult(strings.NewReader(data["terminal-result.json"]))
	if err != nil {
		return declarativerelease.Plan{}, declarativerelease.ArtifactReceipt{}, declarativerelease.ExecutionPlan{}, declarativerelease.MonitorRecord{}, nil, nil, err
	}
	if monitor.Validate(plan, artifact, prepared, terminal, forward, lkg) != nil || monitor.ConfigSHA != prepared.ConfigSHA || monitor.ArtifactDigest != artifact.ReceiptDigest || monitor.ExecutionPlanDigest != prepared.PlanDigest ||
		monitor.ForwardManifestDigest != digest(bytes.TrimSpace(forward)) || monitor.LKGManifestDigest != digest(bytes.TrimSpace(lkg)) {
		return declarativerelease.Plan{}, declarativerelease.ArtifactReceipt{}, declarativerelease.ExecutionPlan{}, declarativerelease.MonitorRecord{}, nil, nil, errors.New("stable release record digest binding is invalid")
	}
	return plan, artifact, prepared, monitor, bytes.TrimSpace(forward), bytes.TrimSpace(lkg), nil
}

func (store *KubeStore) localHealth(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, now time.Time) LayerHealth {
	healthy := func(evidence any) LayerHealth {
		raw, _ := declarativerelease.CanonicalJSON(evidence)
		return LayerHealth{State: HealthHealthy, EvidenceDigest: digest(raw), ObservedAt: now.Format(time.RFC3339Nano)}
	}
	degraded := func(reason string) LayerHealth {
		return LayerHealth{State: HealthDegraded, Reason: reason, EvidenceDigest: digest([]byte(reason)), ObservedAt: now.Format(time.RFC3339Nano)}
	}
	var selector labels.Selector
	var desired, updated, ready, available int32
	var generation, observed int64
	switch release.Workload.Kind {
	case "Deployment":
		workload, err := store.client.AppsV1().Deployments(release.Workload.Namespace).Get(ctx, release.Workload.Name, metav1.GetOptions{})
		if err != nil {
			return degraded("read Deployment: " + err.Error())
		}
		if err := workloadIdentityMatchesDeployment(workload, release, target); err != nil {
			return degraded(err.Error())
		}
		selector, err = metav1.LabelSelectorAsSelector(workload.Spec.Selector)
		if err != nil {
			return degraded("Deployment selector is invalid")
		}
		desired = derefReplicas(workload.Spec.Replicas)
		updated, ready, available = workload.Status.UpdatedReplicas, workload.Status.ReadyReplicas, workload.Status.AvailableReplicas
		generation, observed = workload.Generation, workload.Status.ObservedGeneration
	case "DaemonSet":
		workload, err := store.client.AppsV1().DaemonSets(release.Workload.Namespace).Get(ctx, release.Workload.Name, metav1.GetOptions{})
		if err != nil {
			return degraded("read DaemonSet: " + err.Error())
		}
		if err := workloadIdentityMatchesDaemonSet(workload, release, target); err != nil {
			return degraded(err.Error())
		}
		selector, err = metav1.LabelSelectorAsSelector(workload.Spec.Selector)
		if err != nil {
			return degraded("DaemonSet selector is invalid")
		}
		desired, updated, ready, available = workload.Status.DesiredNumberScheduled, workload.Status.UpdatedNumberScheduled, workload.Status.NumberReady, workload.Status.NumberAvailable
		generation, observed = workload.Generation, workload.Status.ObservedGeneration
	default:
		return degraded("unsupported workload kind")
	}
	if desired < 1 || updated != desired || ready != desired || available != desired || observed != generation {
		return degraded(fmt.Sprintf("rollout is incomplete desired=%d updated=%d ready=%d available=%d generation=%d observed=%d", desired, updated, ready, available, generation, observed))
	}
	pods, err := store.client.CoreV1().Pods(release.Workload.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil || len(pods.Items) != int(desired) {
		return degraded("workload pod inventory is incomplete")
	}
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil || !podReady(pod) {
			return degraded("workload Pod is not Ready")
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == release.Workload.Container && (status.RestartCount != 0 || status.ImageID == "") {
				return degraded("workload container restarted or lacks immutable image identity")
			}
		}
	}
	return healthy(map[string]any{"desired": desired, "updated": updated, "ready": ready, "available": available, "generation": generation, "image": target.ImageRef})
}

func workloadIdentityMatchesDeployment(workload *appsv1.Deployment, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity) error {
	if workload == nil || workload.Annotations["fugue.pro/production-config-sha"] != target.ConfigSHA || workload.Spec.Template.Annotations["fugue.pro/oci-revision"] != target.OCIRevision {
		return errors.New("Deployment release identity differs from the stable record")
	}
	return containerImageMatches(workload.Spec.Template.Spec.Containers, release.Workload.Container, target.ImageRef)
}

func workloadIdentityMatchesDaemonSet(workload *appsv1.DaemonSet, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity) error {
	if workload == nil || workload.Annotations["fugue.pro/production-config-sha"] != target.ConfigSHA || workload.Spec.Template.Annotations["fugue.pro/oci-revision"] != target.OCIRevision {
		return errors.New("DaemonSet release identity differs from the stable record")
	}
	return containerImageMatches(workload.Spec.Template.Spec.Containers, release.Workload.Container, target.ImageRef)
}

func containerImageMatches(containers []corev1.Container, name, image string) error {
	for _, container := range containers {
		if container.Name == name {
			if container.Image != image {
				return errors.New("workload image differs from the stable record")
			}
			return nil
		}
	}
	return errors.New("workload container is absent")
}

func (store *KubeStore) dependencyHealth(ctx context.Context, target TargetConfig, now time.Time) LayerHealth {
	slices, err := store.client.DiscoveryV1().EndpointSlices(target.Namespace).List(ctx, metav1.ListOptions{LabelSelector: discoveryv1.LabelServiceName + "=" + target.DependencyService})
	if err != nil {
		return LayerHealth{State: HealthUnknown, Reason: "read dependency EndpointSlices: " + err.Error(), ObservedAt: now.Format(time.RFC3339Nano)}
	}
	ready := 0
	for _, slice := range slices.Items {
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready {
				ready++
			}
		}
	}
	raw, _ := declarativerelease.CanonicalJSON(map[string]any{"service": target.DependencyService, "ready": ready})
	state, reason := HealthHealthy, ""
	if ready == 0 {
		state, reason = HealthDegraded, "dependency has no Ready endpoint"
	}
	return LayerHealth{State: state, Reason: reason, EvidenceDigest: digest(raw), ObservedAt: now.Format(time.RFC3339Nano)}
}

func (store *KubeStore) routeHealth(ctx context.Context, target TargetConfig, recordDigest string, now time.Time) LayerHealth {
	value, err := store.client.CoreV1().ConfigMaps(target.Namespace).Get(ctx, canaryName(target.Key), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return LayerHealth{State: HealthUnknown, Reason: "independent canary result is absent", ObservedAt: now.Format(time.RFC3339Nano)}
	}
	if err != nil {
		return LayerHealth{State: HealthUnknown, Reason: "read independent canary result: " + err.Error(), ObservedAt: now.Format(time.RFC3339Nano)}
	}
	var result CanaryResult
	if err := decodeStrict([]byte(value.Data["result.json"]), &result); err != nil || result.Key() != target.Key || result.RecordDigest != recordDigest || result.Validate(now) != nil {
		return LayerHealth{State: HealthUnknown, Reason: "independent canary result is stale or invalid", ObservedAt: now.Format(time.RFC3339Nano)}
	}
	return LayerHealth{State: result.State, EvidenceDigest: result.EvidenceDigest, ObservedAt: result.ObservedAt}
}

func (store *KubeStore) UpdateStatus(ctx context.Context, snapshot Snapshot, status ReleaseStatus) error {
	target := store.targets[snapshot.Key]
	configMaps := store.client.CoreV1().ConfigMaps(target.Namespace)
	raw, err := declarativerelease.CanonicalJSON(status)
	if err != nil {
		return err
	}
	desired := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: statusName(snapshot.Key), Namespace: target.Namespace, Labels: guardianLabels(snapshot.Key)}, Data: map[string]string{"status.json": string(raw)}}
	current, err := configMaps.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if snapshot.StatusResourceVersion != "" {
			return errors.New("release status disappeared before resourceVersion CAS")
		}
		_, err = configMaps.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if current.ResourceVersion != snapshot.StatusResourceVersion {
		return errors.New("release status changed before resourceVersion CAS")
	}
	updated := current.DeepCopy()
	updated.Labels, updated.Data = desired.Labels, desired.Data
	_, err = configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

func (store *KubeStore) PutCanaryResult(ctx context.Context, result CanaryResult) error {
	target, exists := store.targets[result.Key()]
	if !exists {
		return errors.New("canary target is not configured")
	}
	raw, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		return err
	}
	configMaps := store.client.CoreV1().ConfigMaps(target.Namespace)
	desired := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: canaryName(result.Key()), Namespace: target.Namespace, Labels: guardianLabels(result.Key())}, Data: map[string]string{"result.json": string(raw)}}
	current, err := configMaps.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = configMaps.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	updated := current.DeepCopy()
	updated.Labels, updated.Data = desired.Labels, desired.Data
	_, err = configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

func NewCanaryResult(record ReleaseRecord, state HealthState, evidenceDigest string, observedAt, expiresAt time.Time) (CanaryResult, error) {
	result := CanaryResult{APIVersion: APIVersion, Kind: CanaryResultKind, Component: record.Component, Group: record.Group, RecordDigest: record.RecordDigest, State: state, EvidenceDigest: evidenceDigest, ObservedAt: observedAt.UTC().Format(time.RFC3339Nano), ExpiresAt: expiresAt.UTC().Format(time.RFC3339Nano)}
	raw, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		return CanaryResult{}, err
	}
	result.ResultDigest = digest(raw)
	if err := result.Validate(observedAt); err != nil {
		return CanaryResult{}, err
	}
	return result, nil
}

func statusName(key Key) string  { return objectName("fugue-release-status", key) }
func canaryName(key Key) string  { return objectName("fugue-canary-result", key) }
func desiredName(key Key) string { return objectName("fugue-desired-release", key) }

func releaseRecordName(key Key, recordDigest string) string {
	suffix := strings.TrimPrefix(recordDigest, "sha256:")
	if len(suffix) > 16 {
		suffix = suffix[:16]
	}
	return objectName("fugue-guardian-record", key) + "-" + suffix
}

func objectName(prefix string, key Key) string {
	name := prefix + "-" + key.Component
	if key.Group != "" {
		name += "-" + key.Group
	}
	return name
}

func guardianLabels(key Key) map[string]string {
	result := map[string]string{"app.kubernetes.io/managed-by": "fugue-release-guardian", "fugue.pro/component": key.Component}
	if key.Group != "" {
		result["fugue.pro/group"] = key.Group
	}
	return result
}

func totalConfigMapBytes(data map[string]string) int {
	total := 0
	for key, value := range data {
		total += len(key) + len(value)
	}
	return total
}

func decodeStrict(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains trailing content")
	}
	return nil
}

func derefReplicas(value *int32) int32 {
	if value == nil {
		return 1
	}
	return *value
}

func podReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
