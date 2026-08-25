package releaseguardian

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PublishDesired persists one immutable CI-produced target and advances only
// its component/group DesiredRelease pointer with a Kubernetes
// ResourceVersion CAS. It never writes a workload.
func (store *KubeStore) PublishDesired(ctx context.Context, key Key, files map[string][]byte) (ReleaseRecord, DesiredRelease, error) {
	if store == nil || store.client == nil {
		return ReleaseRecord{}, DesiredRelease{}, errors.New("release Guardian store is unavailable")
	}
	target, exists := store.targets[key]
	if !exists {
		return ReleaseRecord{}, DesiredRelease{}, errors.New("release Guardian target is not configured")
	}
	snapshot, err := store.Load(ctx, key)
	if err != nil {
		return ReleaseRecord{}, DesiredRelease{}, err
	}
	if err := snapshot.Validate(store.now().UTC()); err != nil {
		return ReleaseRecord{}, DesiredRelease{}, err
	}
	bundle, err := DecodeExecutionBundle(files, key)
	if err != nil {
		return ReleaseRecord{}, DesiredRelease{}, err
	}
	stableRecord, stableMonitor, err := canonicalStableReleaseRecord(key, snapshot.CurrentMonitorData)
	if err != nil {
		return ReleaseRecord{}, DesiredRelease{}, err
	}
	if !publishDesiredEligible(snapshot, bundle, stableRecord) {
		return ReleaseRecord{}, DesiredRelease{}, errors.New("current component is not a healthy settled release")
	}
	lkgMatchesStable := digest(bundle.LKG) == stableMonitor.ForwardManifestDigest
	if len(bundle.Release.RuntimeResourcesFromForward) > 0 && strings.TrimSpace(snapshot.CurrentMonitorData["forward.json"]) != "" {
		var compareErr error
		lkgMatchesStable, compareErr = declarativerelease.ResourceSetsEquivalentExceptRuntimeResources(
			bundle.LKG, []byte(snapshot.CurrentMonitorData["forward.json"]), bundle.Release.RuntimeResourcesFromForward,
		)
		if compareErr != nil {
			return ReleaseRecord{}, DesiredRelease{}, fmt.Errorf("compare Guardian LKG runtime resource recovery: %w", compareErr)
		}
	}
	if !bundle.Prepared.LKG.Present || bundle.Release.ExpectedPreviousConfigSHA != stableMonitor.ConfigSHA ||
		bundle.Release.ExpectedPreviousManifestSHA != stableMonitor.ConfigSHA ||
		bundle.Release.ExpectedPreviousOCIRevision != stableMonitor.ConfigSHA ||
		bundle.Release.ExpectedPreviousImageDigest != stableRecord.ImageDigest ||
		!lkgMatchesStable {
		return ReleaseRecord{}, DesiredRelease{}, errors.New("Guardian candidate LKG is not the exact current stable release")
	}
	record, err := bundle.ReleaseRecord(key, stableRecord.RecordDigest)
	if err != nil {
		return ReleaseRecord{}, DesiredRelease{}, err
	}
	recordRaw, err := declarativerelease.CanonicalJSON(record)
	if err != nil {
		return ReleaseRecord{}, DesiredRelease{}, err
	}
	data := make(map[string]string, len(bundle.Files)+1)
	total := 0
	for name, value := range bundle.Files {
		data[name] = string(value)
		total += len(name) + len(value)
	}
	data["guardian-record.json"] = string(recordRaw)
	total += len("guardian-record.json") + len(recordRaw)
	data["lkg-monitor-record-digest"] = stableMonitor.RecordDigest
	total += len("lkg-monitor-record-digest") + len(stableMonitor.RecordDigest)
	if total > maxRecordBytes {
		return ReleaseRecord{}, DesiredRelease{}, errors.New("Guardian release record exceeds the bounded ConfigMap size")
	}
	immutable := true
	recordMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: releaseRecordName(key, record.RecordDigest), Namespace: target.Namespace, Labels: guardianLabels(key)},
		Immutable:  &immutable, Data: data,
	}
	configMaps := store.client.CoreV1().ConfigMaps(target.Namespace)
	created, createErr := configMaps.Create(ctx, recordMap, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(createErr) {
		created, createErr = configMaps.Get(ctx, recordMap.Name, metav1.GetOptions{})
		if createErr == nil && !immutableReleaseRecordMatches(created, recordMap, key, record) {
			createErr = errors.New("existing immutable Guardian release record differs from the candidate")
		}
	}
	if createErr != nil {
		return ReleaseRecord{}, DesiredRelease{}, fmt.Errorf("persist immutable Guardian release record: %w", createErr)
	}
	next := DesiredRelease{
		APIVersion: APIVersion, Kind: DesiredReleaseKind, Component: key.Component, Group: key.Group,
		RecordDigest: record.RecordDigest, Generation: snapshot.Desired.Generation + 1,
	}
	executionData := map[string]string{
		"execution-plan.json": string(bundle.Files["execution-plan.json"]),
		"record-digest":       record.RecordDigest,
	}
	if totalConfigMapBytes(executionData) > maxRecordBytes {
		return ReleaseRecord{}, DesiredRelease{}, errors.New("Guardian execution snapshot exceeds the bounded ConfigMap size")
	}
	executionMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: executionSnapshotName(key, next.Generation), Namespace: target.Namespace, Labels: guardianLabels(key)},
		Immutable:  &immutable,
		Data:       executionData,
	}
	executionCreated, executionErr := configMaps.Create(ctx, executionMap, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(executionErr) {
		executionCreated, executionErr = configMaps.Get(ctx, executionMap.Name, metav1.GetOptions{})
		if executionErr == nil && (executionCreated.Immutable == nil || !*executionCreated.Immutable ||
			!stringMapEqual(executionCreated.Labels, executionMap.Labels) || !stringMapEqual(executionCreated.Data, executionMap.Data)) {
			executionErr = errors.New("existing immutable Guardian execution snapshot differs from the candidate")
		}
	}
	if executionErr != nil {
		return ReleaseRecord{}, DesiredRelease{}, fmt.Errorf("persist immutable Guardian execution snapshot: %w", executionErr)
	}
	raw, err := declarativerelease.CanonicalJSON(next)
	if err != nil {
		return ReleaseRecord{}, DesiredRelease{}, err
	}
	desiredMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: desiredName(key), Namespace: target.Namespace, Labels: guardianLabels(key)},
		Data:       map[string]string{"desired.json": string(raw)},
	}
	current, getErr := configMaps.Get(ctx, desiredMap.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		if snapshot.DesiredResourceVersion != "" {
			return ReleaseRecord{}, DesiredRelease{}, errors.New("DesiredRelease disappeared before CAS")
		}
		_, err = configMaps.Create(ctx, desiredMap, metav1.CreateOptions{})
	} else if getErr == nil {
		if current.ResourceVersion != snapshot.DesiredResourceVersion {
			return ReleaseRecord{}, DesiredRelease{}, errors.New("DesiredRelease changed before resourceVersion CAS")
		}
		updated := current.DeepCopy()
		updated.Labels, updated.Data = desiredMap.Labels, desiredMap.Data
		_, err = configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	} else {
		err = getErr
	}
	if err != nil {
		return ReleaseRecord{}, DesiredRelease{}, fmt.Errorf("advance DesiredRelease with resourceVersion CAS: %w", err)
	}
	return record, next, nil
}

func canonicalStableReleaseRecord(key Key, data map[string]string) (ReleaseRecord, declarativerelease.MonitorRecord, error) {
	plan, artifact, prepared, monitor, _, _, err := decodeStableRecord(data)
	release, found := releaseForComponent(plan, key.Component)
	if err != nil || !found || prepared.Component != key.Component || monitor.Component != key.Component {
		return ReleaseRecord{}, declarativerelease.MonitorRecord{}, errors.New("current stable monitor record is invalid")
	}
	healthRaw, err := declarativerelease.CanonicalJSON(release.Health)
	if err != nil {
		return ReleaseRecord{}, declarativerelease.MonitorRecord{}, err
	}
	record, err := NewReleaseRecord(key, prepared.ConfigSHA, artifact.TopDigest, monitor.ForwardManifestDigest, monitor.RecordDigest, digest(healthRaw))
	if err != nil {
		return ReleaseRecord{}, declarativerelease.MonitorRecord{}, err
	}
	return record, monitor, nil
}

func publishDesiredEligible(snapshot Snapshot, bundle ExecutionBundle, stableRecord ReleaseRecord) bool {
	settled := snapshot.CurrentRecordDigest == snapshot.Desired.RecordDigest
	if settled && snapshot.Health.Local.State == HealthHealthy && snapshot.Health.Dependency.State == HealthHealthy && snapshot.Health.Route.State == HealthHealthy {
		return true
	}
	return degradedPredecessorPublishEligible(snapshot, bundle) ||
		degradedEdgeRouteRecoveryEligible(snapshot, bundle) ||
		orphanedDesiredEdgeRecoveryEligible(snapshot, bundle, stableRecord) ||
		fencedDesiredReplacementEligible(snapshot, bundle, stableRecord) ||
		failedDesiredReplacementEligible(snapshot, bundle, stableRecord)
}

// orphanedDesiredEdgeRecoveryEligible admits only a signed Edge A/B successor
// when DesiredRelease points at a pruned/missing candidate. The stable monitor
// remains the exact LKG anchor; the successor must carry an explicit failed
// config supersession and a degraded-predecessor execution plan before
// Guardian advances the pointer again.
func orphanedDesiredEdgeRecoveryEligible(snapshot Snapshot, bundle ExecutionBundle, stableRecord ReleaseRecord) bool {
	prepared, release := bundle.Prepared, bundle.Release
	return snapshot.Managed && snapshot.DesiredRecordMissing && knownDegradedHealth(snapshot.Health) &&
		stableRecord.Key() == snapshot.Key && snapshot.Record.Key() == snapshot.Key &&
		snapshot.CurrentRecordDigest == stableRecord.RecordDigest &&
		snapshot.LastSuccessfulLKG == stableRecord.RecordDigest &&
		prepared.DegradedPredecessor && prepared.DegradedRoute && prepared.Component == snapshot.Key.Component &&
		release.Transition != nil && release.Transition.Type == "edge-group-ab" && release.Transition.EdgeGroupAB != nil &&
		release.SupersedesFailedConfigSHA != "" && release.ExpectedPreviousPresent &&
		release.ExpectedPreviousConfigSHA == stableRecord.ConfigSHA &&
		release.ExpectedPreviousManifestSHA == stableRecord.ConfigSHA &&
		release.ExpectedPreviousOCIRevision == stableRecord.ConfigSHA &&
		release.ExpectedPreviousImageDigest == stableRecord.ImageDigest && prepared.LKG.Present &&
		prepared.LKG.ConfigSHA == stableRecord.ConfigSHA && prepared.LKG.ManifestSHA == stableRecord.ConfigSHA &&
		prepared.LKG.OCIRevision == stableRecord.ConfigSHA &&
		prepared.LKG.ImageRef == release.Artifact.Repository+"@"+stableRecord.ImageDigest
}

// A degraded predecessor retry is admitted only when the immutable candidate
// and every mutable Guardian pointer still bind the same current LKG. The
// recorded prewrite plan remains the workload CAS authority during rollout.
func degradedPredecessorPublishEligible(snapshot Snapshot, bundle ExecutionBundle) bool {
	if !knownDegradedHealth(snapshot.Health) {
		return false
	}
	prepared, release := bundle.Prepared, bundle.Release
	return snapshot.Managed && prepared.DegradedPredecessor && prepared.Component == snapshot.Key.Component &&
		prepared.ConfigSHA == prepared.Forward.ConfigSHA && snapshot.Record.Key() == snapshot.Key &&
		snapshot.CurrentRecordDigest == snapshot.Record.RecordDigest && snapshot.Desired.RecordDigest == snapshot.Record.RecordDigest &&
		snapshot.LastSuccessfulLKG == snapshot.Record.RecordDigest && release.ExpectedPreviousPresent &&
		release.ExpectedPreviousConfigSHA == snapshot.Record.ConfigSHA && release.ExpectedPreviousManifestSHA == snapshot.Record.ConfigSHA &&
		release.ExpectedPreviousOCIRevision == snapshot.Record.ConfigSHA && release.ExpectedPreviousImageDigest == snapshot.Record.ImageDigest &&
		prepared.LKG.Present && prepared.LKG.ConfigSHA == snapshot.Record.ConfigSHA &&
		prepared.LKG.ManifestSHA == snapshot.Record.ConfigSHA && prepared.LKG.OCIRevision == snapshot.Record.ConfigSHA &&
		prepared.LKG.ImageRef == release.Artifact.Repository+"@"+snapshot.Record.ImageDigest
}

func fencedDesiredReplacementEligible(snapshot Snapshot, bundle ExecutionBundle, stableRecord ReleaseRecord) bool {
	previous := snapshot.PreviousStatus
	prepared, release := bundle.Prepared, bundle.Release
	if !knownDegradedHealth(snapshot.Health) || previous == nil || previous.Key() != snapshot.Key ||
		previous.RolloutReceiptDigest != "" || previous.RollbackReceiptDigest != "" {
		return false
	}
	supersededDesired := release.SupersedesFailedConfigSHA != "" && release.SupersedesFailedConfigSHA == snapshot.Record.ConfigSHA &&
		prepared.Prewrite.MatchesSupersededFailedAtom(release) &&
		prepared.Prewrite.ImageRef == release.Artifact.Repository+"@"+snapshot.Record.ImageDigest
	dependencyFenced := previous.State == StateDegraded &&
		strings.HasPrefix(previous.Reason, "desired rollout is fenced because the current release dependencies are degraded") &&
		(release.RetrySameLKG || supersededDesired)
	componentFenced := previous.State == StateRecoveryRequired &&
		strings.HasPrefix(previous.Reason, "desired rollout is fenced because the current component is degraded") && supersededDesired
	if !dependencyFenced && !componentFenced {
		return false
	}
	return snapshot.Managed && stableRecord.Key() == snapshot.Key && prepared.DegradedPredecessor && prepared.Component == snapshot.Key.Component &&
		prepared.ConfigSHA == prepared.Forward.ConfigSHA && snapshot.Desired.RecordDigest == snapshot.Record.RecordDigest &&
		snapshot.CurrentRecordDigest != snapshot.Desired.RecordDigest && snapshot.CurrentRecordDigest == stableRecord.RecordDigest &&
		snapshot.LastSuccessfulLKG == stableRecord.RecordDigest && previous.CurrentRecordDigest == snapshot.CurrentRecordDigest &&
		previous.TargetRecordDigest == snapshot.Desired.RecordDigest && previous.LastSuccessfulLKG == snapshot.CurrentRecordDigest &&
		release.ExpectedPreviousPresent && release.ExpectedPreviousConfigSHA == stableRecord.ConfigSHA &&
		release.ExpectedPreviousManifestSHA == stableRecord.ConfigSHA && release.ExpectedPreviousOCIRevision == stableRecord.ConfigSHA &&
		release.ExpectedPreviousImageDigest == stableRecord.ImageDigest && prepared.LKG.Present &&
		prepared.LKG.ConfigSHA == stableRecord.ConfigSHA && prepared.LKG.ManifestSHA == stableRecord.ConfigSHA &&
		prepared.LKG.OCIRevision == stableRecord.ConfigSHA && prepared.LKG.ImageRef == release.Artifact.Repository+"@"+stableRecord.ImageDigest
}

// A failed DesiredRelease may remain fenced after its executor restored the
// exact LKG but could not prove route health. A successor is admitted only
// when the failed target, rollout receipt, current LKG, and immutable rollback
// target all agree and the intent explicitly supersedes that failed config.
func failedDesiredReplacementEligible(snapshot Snapshot, bundle ExecutionBundle, stableRecord ReleaseRecord) bool {
	previous := snapshot.PreviousStatus
	prepared, release := bundle.Prepared, bundle.Release
	if !knownDegradedHealth(snapshot.Health) || previous == nil || previous.Key() != snapshot.Key ||
		previous.State != StateRecoveryRequired || previous.RolloutReceiptDigest == "" || previous.RollbackReceiptDigest != "" ||
		!unprovenLKGReason(previous.Reason) {
		return false
	}
	supersededDesired := release.SupersedesFailedConfigSHA != "" &&
		release.SupersedesFailedConfigSHA == snapshot.Record.ConfigSHA &&
		snapshot.Record.LKGRecordDigest == stableRecord.RecordDigest
	if !supersededDesired {
		return false
	}
	return snapshot.Managed && stableRecord.Key() == snapshot.Key && snapshot.Record.Key() == snapshot.Key &&
		prepared.DegradedPredecessor && prepared.Component == snapshot.Key.Component &&
		prepared.ConfigSHA == prepared.Forward.ConfigSHA && snapshot.Desired.RecordDigest == snapshot.Record.RecordDigest &&
		snapshot.CurrentRecordDigest != snapshot.Desired.RecordDigest && snapshot.CurrentRecordDigest == stableRecord.RecordDigest &&
		snapshot.LastSuccessfulLKG == stableRecord.RecordDigest && previous.CurrentRecordDigest == snapshot.CurrentRecordDigest &&
		previous.TargetRecordDigest == snapshot.Desired.RecordDigest && previous.LastSuccessfulLKG == snapshot.CurrentRecordDigest &&
		release.ExpectedPreviousPresent && release.ExpectedPreviousConfigSHA == stableRecord.ConfigSHA &&
		release.ExpectedPreviousManifestSHA == stableRecord.ConfigSHA && release.ExpectedPreviousOCIRevision == stableRecord.ConfigSHA &&
		release.ExpectedPreviousImageDigest == stableRecord.ImageDigest && prepared.LKG.Present &&
		prepared.LKG.ConfigSHA == stableRecord.ConfigSHA && prepared.LKG.ManifestSHA == stableRecord.ConfigSHA &&
		prepared.LKG.OCIRevision == stableRecord.ConfigSHA && prepared.LKG.ImageRef == release.Artifact.Repository+"@"+stableRecord.ImageDigest
}

func unprovenLKGReason(reason string) bool {
	return reason == "lkg-unproven" || strings.HasPrefix(reason, "lkg-unproven: ") ||
		strings.HasPrefix(reason, "failed candidate is fenced while LKG health awaits complete evidence") ||
		strings.HasPrefix(reason, "rollout result is unknown:")
}

func knownDegradedHealth(health HealthSnapshot) bool {
	return health.Local.State != HealthUnknown && health.Dependency.State != HealthUnknown && health.Route.State != HealthUnknown &&
		(health.Local.State == HealthDegraded || health.Dependency.State == HealthDegraded || health.Route.State == HealthDegraded)
}

func immutableReleaseRecordMatches(observed, expected *corev1.ConfigMap, key Key, record ReleaseRecord) bool {
	if observed == nil || expected == nil || observed.Immutable == nil || !*observed.Immutable ||
		!stringMapEqual(observed.Labels, expected.Labels) {
		return false
	}
	observedStable := make(map[string]string, len(observed.Data))
	expectedStable := make(map[string]string, len(expected.Data))
	for name, value := range observed.Data {
		if name != "execution-plan.json" {
			observedStable[name] = value
		}
	}
	for name, value := range expected.Data {
		if name != "execution-plan.json" {
			expectedStable[name] = value
		}
	}
	if !stringMapEqual(observedStable, expectedStable) {
		return false
	}
	files, err := executionFilesFromStrings(observed.Data)
	if err != nil {
		return false
	}
	bundle, err := DecodeExecutionBundle(files, key)
	if err != nil {
		return false
	}
	bound, err := bundle.ReleaseRecord(key, record.LKGRecordDigest)
	return err == nil && bound == record
}

func (store *KubeStore) WaitForTerminal(ctx context.Context, key Key, expected DesiredRelease, interval time.Duration) (ReleaseStatus, error) {
	if expected.Validate() != nil || expected.Key() != key || interval < time.Second || interval > 30*time.Second {
		return ReleaseStatus{}, errors.New("Guardian terminal wait configuration is invalid")
	}
	target, exists := store.targets[key]
	if !exists {
		return ReleaseStatus{}, errors.New("release Guardian target is not configured")
	}
	started := store.now().UTC().Add(-interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		value, err := store.client.CoreV1().ConfigMaps(target.Namespace).Get(ctx, statusName(key), metav1.GetOptions{})
		if err == nil {
			var status ReleaseStatus
			if decodeStrict([]byte(value.Data["status.json"]), &status) == nil && status.Key() == key && status.Validate(store.now().UTC()) == nil {
				observedAt, _ := time.Parse(time.RFC3339Nano, status.ObservedAt)
				if !observedAt.Before(started) {
					switch status.State {
					case StateStable:
						if status.TargetRecordDigest == expected.RecordDigest && status.CurrentRecordDigest == expected.RecordDigest {
							return status, nil
						}
					case StateDegraded:
						if degradedRouteTargetTerminal(status, expected) {
							return status, nil
						}
					case StateRecoveryRequired:
						if status.TargetRecordDigest == expected.RecordDigest {
							return status, fmt.Errorf("Guardian release ended in %s: %s", status.State, status.Reason)
						}
					case StateLKGStable:
						if status.RolloutReceiptDigest != "" && status.TargetRecordDigest == status.CurrentRecordDigest && status.TargetRecordDigest != expected.RecordDigest {
							current, getErr := store.client.CoreV1().ConfigMaps(target.Namespace).Get(ctx, desiredName(key), metav1.GetOptions{})
							var desired DesiredRelease
							if getErr == nil && decodeStrict([]byte(current.Data["desired.json"]), &desired) == nil && desired.Validate() == nil &&
								desired.Generation == expected.Generation+1 && desired.RecordDigest == status.TargetRecordDigest {
								return status, fmt.Errorf("Guardian release restored its LKG: %s", status.Reason)
							}
						}
					}
				}
			}
		} else if !apierrors.IsNotFound(err) {
			return ReleaseStatus{}, err
		}
		select {
		case <-ctx.Done():
			return ReleaseStatus{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func degradedRouteTargetTerminal(status ReleaseStatus, expected DesiredRelease) bool {
	return status.State == StateDegraded && status.TargetRecordDigest == expected.RecordDigest &&
		status.CurrentRecordDigest == expected.RecordDigest && status.LastSuccessfulLKG == expected.RecordDigest &&
		status.Health.Local.State == HealthHealthy && status.Health.Dependency.State == HealthHealthy &&
		status.Health.Route.State == HealthDegraded
}

func (store *KubeStore) SetDesiredToLKG(ctx context.Context, snapshot Snapshot) error {
	if store == nil || snapshot.Record.Validate() != nil || snapshot.DesiredResourceVersion == "" ||
		snapshot.Desired.RecordDigest != snapshot.Record.RecordDigest || !digestPattern.MatchString(snapshot.Record.LKGRecordDigest) ||
		!digestPattern.MatchString(snapshot.LKGMonitorRecordDigest) {
		return errors.New("DesiredRelease LKG rollback request is invalid")
	}
	target, exists := store.targets[snapshot.Key]
	if !exists {
		return errors.New("release Guardian target is not configured")
	}
	configMaps := store.client.CoreV1().ConfigMaps(target.Namespace)
	monitorName := monitorRecordNameFromDigest(target.MonitorComponent, snapshot.LKGMonitorRecordDigest)
	monitorMap, err := configMaps.Get(ctx, monitorName, metav1.GetOptions{})
	if err != nil || monitorMap.Immutable == nil || !*monitorMap.Immutable {
		return errors.New("Guardian LKG monitor record is absent or mutable")
	}
	_, _, _, monitor, _, _, err := decodeStableRecord(monitorMap.Data)
	if err != nil || monitor.RecordDigest != snapshot.LKGMonitorRecordDigest {
		return errors.New("Guardian LKG monitor record binding is invalid")
	}
	current, err := configMaps.Get(ctx, desiredName(snapshot.Key), metav1.GetOptions{})
	if err != nil || current.ResourceVersion != snapshot.DesiredResourceVersion {
		return errors.New("DesiredRelease changed before LKG resourceVersion CAS")
	}
	var observed DesiredRelease
	if decodeStrict([]byte(current.Data["desired.json"]), &observed) != nil || observed != snapshot.Desired {
		return errors.New("DesiredRelease payload changed before LKG CAS")
	}
	next := DesiredRelease{
		APIVersion: APIVersion, Kind: DesiredReleaseKind, Component: snapshot.Key.Component, Group: snapshot.Key.Group,
		RecordDigest: snapshot.Record.LKGRecordDigest, Generation: snapshot.Desired.Generation + 1,
	}
	raw, err := declarativerelease.CanonicalJSON(next)
	if err != nil {
		return err
	}
	updated := current.DeepCopy()
	updated.Data = map[string]string{"desired.json": string(raw)}
	updated.Labels = guardianLabels(snapshot.Key)
	_, err = configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

func monitorRecordNameFromDigest(component, recordDigest string) string {
	suffix := strings.TrimPrefix(recordDigest, "sha256:")
	if len(suffix) > 16 {
		suffix = suffix[:16]
	}
	return "fugue-release-record-" + component + "-" + suffix
}

func stringMapEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}
