package releaseguardian

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const maxArtifactInventory = 10_000

type ArtifactRetentionPolicy struct {
	MinimumAge     time.Duration
	MinimumHistory int
	MaximumDeletes int
}

func (policy ArtifactRetentionPolicy) Validate() error {
	if policy.MinimumAge < time.Hour || policy.MinimumAge > 7*24*time.Hour ||
		policy.MinimumHistory < 8 || policy.MinimumHistory > 256 ||
		policy.MaximumDeletes < 1 || policy.MaximumDeletes > 1_024 {
		return errors.New("release artifact retention policy is invalid")
	}
	return nil
}

type ArtifactPruneResult struct {
	Candidates int
	Deleted    int
	Remaining  int
	Deferred   bool
	Inventory  int
	Protected  int
}

type ArtifactPruner struct {
	client    kubernetes.Interface
	namespace string
	policy    ArtifactRetentionPolicy
}

func NewArtifactPruner(client kubernetes.Interface, namespace string, policy ArtifactRetentionPolicy) (*ArtifactPruner, error) {
	if client == nil || !componentPattern.MatchString(namespace) || policy.Validate() != nil {
		return nil, errors.New("release artifact pruner configuration is invalid")
	}
	return &ArtifactPruner{client: client, namespace: namespace, policy: policy}, nil
}

type retainedArtifact struct {
	object   corev1.ConfigMap
	kind     string
	scope    string
	identity string
	priority int
}

type guardianArtifactReference struct {
	name                string
	key                 Key
	lkgRecordDigest     string
	lkgMonitorRecordRef string
}

func (pruner *ArtifactPruner) Prune(ctx context.Context, now time.Time) (ArtifactPruneResult, error) {
	if pruner == nil || pruner.client == nil || now.IsZero() || !now.Equal(now.UTC()) {
		return ArtifactPruneResult{}, errors.New("release artifact prune request is invalid")
	}
	objects, err := pruner.listManagedConfigMaps(ctx, "")
	if err != nil {
		return ArtifactPruneResult{}, err
	}
	var guardianObjects, monitorObjects []corev1.ConfigMap
	for _, object := range objects {
		if object.Labels["app.kubernetes.io/managed-by"] == "fugue-release-guardian" {
			guardianObjects = append(guardianObjects, object)
		} else {
			monitorObjects = append(monitorObjects, object)
		}
	}
	if len(guardianObjects)+len(monitorObjects) > maxArtifactInventory {
		return ArtifactPruneResult{}, errors.New("release artifact inventory exceeds its cleanup bound")
	}

	keep := map[string]bool{}
	artifacts := make([]retainedArtifact, 0, len(guardianObjects)+len(monitorObjects))
	references := map[string][]string{}
	busy := false

	for index := range guardianObjects {
		object := &guardianObjects[index]
		name := object.Name
		switch {
		case strings.HasPrefix(name, "fugue-guardian-record-"):
			record, ref, artifactErr := validateGuardianRecordArtifact(object)
			if artifactErr != nil {
				return ArtifactPruneResult{}, artifactErr
			}
			artifacts = append(artifacts, retainedArtifact{object: *object, kind: "guardian-record", scope: record.Key().String(), identity: record.RecordDigest, priority: 2})
			references[name] = []string{releaseRecordName(ref.key, ref.lkgRecordDigest), ref.lkgMonitorRecordRef}
		case strings.HasPrefix(name, "fugue-guardian-execution-"):
			key, generation, artifactErr := validateGuardianExecutionArtifact(object)
			if artifactErr != nil {
				return ArtifactPruneResult{}, artifactErr
			}
			artifacts = append(artifacts, retainedArtifact{object: *object, kind: "guardian-execution", scope: key.String(), identity: strconv.FormatInt(generation, 10), priority: 1})
			references[name] = []string{releaseRecordName(key, strings.TrimSpace(object.Data["record-digest"]))}
		case strings.HasPrefix(name, "fugue-route-bundle-record-"):
			record, artifactErr := validateRouteBundleArtifact(object)
			if artifactErr != nil {
				return ArtifactPruneResult{}, artifactErr
			}
			artifacts = append(artifacts, retainedArtifact{object: *object, kind: "route-bundle", scope: record.GroupID, identity: record.RecordDigest, priority: 0})
		case strings.HasPrefix(name, "fugue-desired-release-"):
			var desired DesiredRelease
			if decodeStrict([]byte(object.Data["desired.json"]), &desired) != nil || desired.Validate() != nil || name != desiredName(desired.Key()) {
				return ArtifactPruneResult{}, fmt.Errorf("release artifact cleanup encountered invalid DesiredRelease %s", name)
			}
			keep[releaseRecordName(desired.Key(), desired.RecordDigest)] = true
			keep[executionSnapshotName(desired.Key(), desired.Generation)] = true
		case strings.HasPrefix(name, "fugue-release-status-"):
			var status ReleaseStatus
			if decodeStrict([]byte(object.Data["status.json"]), &status) != nil || status.Validate(now) != nil || name != statusName(status.Key()) {
				return ArtifactPruneResult{}, fmt.Errorf("release artifact cleanup encountered invalid ReleaseStatus %s", name)
			}
			if status.State != StateStable && status.State != StateLKGStable {
				busy = true
			}
			for _, digest := range []string{status.CurrentRecordDigest, status.TargetRecordDigest, status.LastSuccessfulLKG} {
				if digest != "" {
					keep[releaseRecordName(status.Key(), digest)] = true
				}
			}
		case strings.HasPrefix(name, "fugue-current-authority-"):
			var authority CurrentAuthority
			if decodeStrict([]byte(object.Data["authority.json"]), &authority) != nil || authority.Validate() != nil || name != currentAuthorityName(authority.GroupID) {
				return ArtifactPruneResult{}, fmt.Errorf("release artifact cleanup encountered invalid CurrentAuthority %s", name)
			}
			protectRouteRecords(keep, authority)
		case strings.HasPrefix(name, "fugue-candidate-authority-"):
			var candidate CandidateAuthority
			if decodeStrict([]byte(object.Data["candidate.json"]), &candidate) != nil || candidate.Validate() != nil || name != candidateAuthorityName(candidate.GroupID) {
				return ArtifactPruneResult{}, fmt.Errorf("release artifact cleanup encountered invalid CandidateAuthority %s", name)
			}
			keep[routeBundleRecordName(candidate.GroupID, candidate.RecordDigest)] = true
		case strings.HasPrefix(name, "fugue-authority-transition-prepared-") || strings.HasPrefix(name, "fugue-authority-transition-activated-"):
			var journal AuthorityTransitionJournal
			if decodeStrict([]byte(object.Data["journal.json"]), &journal) != nil || journal.Validate() != nil ||
				(journal.Phase == AuthorityTransitionPrepared && name != transitionJournalName(journal.GroupID)) ||
				(journal.Phase == AuthorityTransitionActivated && name != transitionActivatedJournalName(journal.GroupID)) {
				return ArtifactPruneResult{}, fmt.Errorf("release artifact cleanup encountered invalid authority journal %s", name)
			}
			protectRouteRecords(keep, journal.Before)
			keep[routeBundleRecordName(journal.GroupID, journal.Candidate.RecordDigest)] = true
		}
	}

	for index := range monitorObjects {
		object := &monitorObjects[index]
		switch {
		case strings.HasPrefix(object.Name, "fugue-release-record-"):
			_, _, _, record, _, _, recordErr := decodeStableRecord(object.Data)
			if recordErr != nil || object.Immutable == nil || !*object.Immutable || object.Name != monitorRecordNameFromDigest(record.Component, record.RecordDigest) ||
				object.Labels["fugue.pro/component"] != record.Component || object.Labels["fugue.pro/config-sha"] != record.ConfigSHA {
				return ArtifactPruneResult{}, fmt.Errorf("release artifact cleanup encountered invalid monitor record %s", object.Name)
			}
			artifacts = append(artifacts, retainedArtifact{object: *object, kind: "monitor-record", scope: record.Component, identity: record.RecordDigest, priority: 3})
		case strings.HasPrefix(object.Name, "fugue-release-monitor-"):
			var state declarativerelease.MonitorState
			recordName := strings.TrimSpace(object.Data["recordName"])
			if decodeStrict([]byte(object.Data["state.json"]), &state) != nil || state.Validate() != nil ||
				object.Name != "fugue-release-monitor-"+state.Component || recordName != monitorRecordNameFromDigest(state.Component, state.RecordDigest) {
				return ArtifactPruneResult{}, fmt.Errorf("release artifact cleanup encountered invalid monitor pointer %s", object.Name)
			}
			keep[recordName] = true
		}
	}
	for _, artifact := range artifacts {
		if artifact.object.CreationTimestamp.IsZero() {
			return ArtifactPruneResult{}, fmt.Errorf("release artifact %s lacks a creation timestamp", artifact.object.Name)
		}
	}

	cutoff := now.Add(-pruner.policy.MinimumAge)
	keepRecentArtifactHistory(artifacts, keep, cutoff, pruner.policy.MinimumHistory)

	// Reach a fixed point: execution -> Guardian -> exact rollback/monitor.
	// A single pass depends on list order and can delete a transitive predecessor.
	protectArtifactClosure(keep, references)

	candidates := make([]retainedArtifact, 0)
	for _, artifact := range artifacts {
		if !keep[artifact.object.Name] {
			candidates = append(candidates, artifact)
		}
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].priority != candidates[right].priority {
			return candidates[left].priority < candidates[right].priority
		}
		leftTime, rightTime := candidates[left].object.CreationTimestamp.Time, candidates[right].object.CreationTimestamp.Time
		if !leftTime.Equal(rightTime) {
			return leftTime.Before(rightTime)
		}
		return candidates[left].object.Name < candidates[right].object.Name
	})

	result := ArtifactPruneResult{Candidates: len(candidates), Remaining: len(candidates), Inventory: len(artifacts), Protected: len(artifacts) - len(candidates)}
	if len(candidates) == 0 {
		return result, nil
	}
	// A component in transition can still be consuming its immutable execution
	// bundle. Never collect around an unfinished rollout or rollback.
	if busy {
		result.Deferred = true
		return result, nil
	}
	latest, err := pruner.listManagedConfigMaps(ctx, "")
	if err != nil {
		return result, err
	}
	if artifactRootFingerprint(objects) != artifactRootFingerprint(latest) {
		result.Deferred = true
		return result, nil
	}
	// New publications may reference only their freshly created artifacts or the
	// protected current/LKG predecessor. A changed inventory defers the pass;
	// per-object UID/RV preconditions still fence each immutable deletion.
	for index := 0; index < len(candidates) && index < pruner.policy.MaximumDeletes; index++ {
		object := &candidates[index].object
		uid, rv := object.UID, object.ResourceVersion
		if uid == "" || strings.TrimSpace(rv) == "" {
			return result, fmt.Errorf("release artifact %s lacks CAS identity", object.Name)
		}
		err := pruner.client.CoreV1().ConfigMaps(pruner.namespace).Delete(ctx, object.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv},
		})
		if err != nil && !apierrors.IsNotFound(err) {
			return result, fmt.Errorf("delete expired release artifact %s: %w", object.Name, err)
		}
		result.Deleted++
	}
	result.Remaining -= result.Deleted
	return result, nil
}

func (pruner *ArtifactPruner) listManagedConfigMaps(ctx context.Context, manager string) ([]corev1.ConfigMap, error) {
	selector := labels.Set{"app.kubernetes.io/managed-by": manager}.AsSelector().String()
	if manager == "" {
		selector = "app.kubernetes.io/managed-by in (fugue-release-guardian,fugue-declarative-release)"
	}
	configMaps := pruner.client.CoreV1().ConfigMaps(pruner.namespace)
	result := make([]corev1.ConfigMap, 0)
	continuation := ""
	seen := map[string]bool{}
	for {
		page, err := configMaps.List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 500, Continue: continuation})
		if err != nil {
			return nil, err
		}
		result = append(result, page.Items...)
		if len(result) > maxArtifactInventory {
			return nil, errors.New("managed ConfigMap inventory exceeds its cleanup bound")
		}
		continuation = page.Continue
		if continuation == "" {
			return result, nil
		}
		if seen[continuation] {
			return nil, errors.New("release artifact inventory repeated pagination cursor")
		}
		seen[continuation] = true
	}
}

func validateGuardianRecordArtifact(object *corev1.ConfigMap) (ReleaseRecord, guardianArtifactReference, error) {
	key, err := keyFromGuardianLabels(object)
	if err != nil || object.Immutable == nil || !*object.Immutable || totalConfigMapBytes(object.Data) > maxRecordBytes {
		return ReleaseRecord{}, guardianArtifactReference{}, fmt.Errorf("release artifact cleanup encountered invalid Guardian record %s", object.Name)
	}
	var record ReleaseRecord
	lkgMonitorDigest := strings.TrimSpace(object.Data["lkg-monitor-record-digest"])
	if decodeStrict([]byte(object.Data["guardian-record.json"]), &record) != nil || record.Validate() != nil || record.Key() != key ||
		object.Name != releaseRecordName(key, record.RecordDigest) || !digestPattern.MatchString(lkgMonitorDigest) {
		return ReleaseRecord{}, guardianArtifactReference{}, fmt.Errorf("release artifact cleanup encountered invalid Guardian record %s", object.Name)
	}
	return record, guardianArtifactReference{
		name: object.Name, key: key, lkgRecordDigest: record.LKGRecordDigest,
		lkgMonitorRecordRef: monitorRecordNameFromDigest(key.Component, lkgMonitorDigest),
	}, nil
}

func validateGuardianExecutionArtifact(object *corev1.ConfigMap) (Key, int64, error) {
	key, err := keyFromGuardianLabels(object)
	prefix := objectName("fugue-guardian-execution", key) + "-"
	generation, generationErr := strconv.ParseInt(strings.TrimPrefix(object.Name, prefix), 10, 64)
	if err != nil || object.Immutable == nil || !*object.Immutable || !strings.HasPrefix(object.Name, prefix) || generationErr != nil || generation < 1 ||
		len(object.Data) != 2 || !digestPattern.MatchString(strings.TrimSpace(object.Data["record-digest"])) || strings.TrimSpace(object.Data["execution-plan.json"]) == "" {
		return Key{}, 0, fmt.Errorf("release artifact cleanup encountered invalid Guardian execution %s", object.Name)
	}
	return key, generation, nil
}

func validateRouteBundleArtifact(object *corev1.ConfigMap) (RouteBundleRecord, error) {
	var record RouteBundleRecord
	if object.Immutable == nil || !*object.Immutable || len(object.Data) != 1 || object.Labels["fugue.pro/authority-kind"] != "route-bundle" ||
		decodeStrict([]byte(object.Data["record.json"]), &record) != nil || record.Validate() != nil ||
		object.Labels["fugue.pro/group"] != record.GroupID || object.Name != routeBundleRecordName(record.GroupID, record.RecordDigest) {
		return RouteBundleRecord{}, fmt.Errorf("release artifact cleanup encountered invalid route bundle %s", object.Name)
	}
	return record, nil
}

func keyFromGuardianLabels(object *corev1.ConfigMap) (Key, error) {
	key := Key{Component: object.Labels["fugue.pro/component"], Group: object.Labels["fugue.pro/group"]}
	if key.Validate() != nil || !stringMapEqual(object.Labels, guardianLabels(key)) {
		return Key{}, errors.New("Guardian artifact labels are invalid")
	}
	return key, nil
}

func protectRouteRecords(keep map[string]bool, authority CurrentAuthority) {
	keep[routeBundleRecordName(authority.GroupID, authority.CurrentRecordDigest)] = true
	if authority.PreviousRecordDigest != "" {
		keep[routeBundleRecordName(authority.GroupID, authority.PreviousRecordDigest)] = true
	}
}

func keepRecentArtifactHistory(artifacts []retainedArtifact, keep map[string]bool, cutoff time.Time, minimumHistory int) {
	byScope := map[string][]retainedArtifact{}
	for _, artifact := range artifacts {
		if artifact.object.CreationTimestamp.IsZero() {
			continue
		}
		key := artifact.kind + "\x00" + artifact.scope
		byScope[key] = append(byScope[key], artifact)
	}
	for _, scoped := range byScope {
		sort.Slice(scoped, func(left, right int) bool {
			leftTime, rightTime := scoped[left].object.CreationTimestamp.Time, scoped[right].object.CreationTimestamp.Time
			if !leftTime.Equal(rightTime) {
				return leftTime.After(rightTime)
			}
			return scoped[left].object.Name > scoped[right].object.Name
		})
		for index, artifact := range scoped {
			if index < minimumHistory || !artifact.object.CreationTimestamp.Time.Before(cutoff) {
				keep[artifact.object.Name] = true
			}
		}
	}
}

func protectArtifactClosure(keep map[string]bool, references map[string][]string) {
	queue := make([]string, 0, len(keep))
	for name, retained := range keep {
		if retained {
			queue = append(queue, name)
		}
	}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		for _, ref := range references[name] {
			if ref != "" && !keep[ref] {
				keep[ref] = true
				queue = append(queue, ref)
			}
		}
	}
}

// Include new/deleted immutable records and the semantic pointer bytes. Health
// observation timestamps alone change continuously and do not change reachability.
func artifactRootFingerprint(objects []corev1.ConfigMap) string {
	rows := make([]string, 0, len(objects))
	for _, object := range objects {
		value := object.Name + "/" + string(object.UID)
		if strings.HasPrefix(object.Name, "fugue-release-status-") {
			var status ReleaseStatus
			if decodeStrict([]byte(object.Data["status.json"]), &status) != nil {
				value += "/invalid/" + object.ResourceVersion
			} else {
				value += "/" + string(status.State) + "/" + status.CurrentRecordDigest + "/" + status.TargetRecordDigest + "/" + status.LastSuccessfulLKG
			}
		} else if strings.HasPrefix(object.Name, "fugue-canary-result-") {
			var result CanaryResult
			if decodeStrict([]byte(object.Data["result.json"]), &result) != nil {
				value += "/invalid/" + object.ResourceVersion
			} else {
				value += "/" + result.Key().String() + "/" + result.RecordDigest + "/" + string(result.State)
			}
		} else if strings.HasPrefix(object.Name, "fugue-release-monitor-") {
			var state declarativerelease.MonitorState
			if decodeStrict([]byte(object.Data["state.json"]), &state) != nil {
				value += "/invalid/" + object.ResourceVersion
			} else {
				value += "/" + object.Data["recordName"] + "/" + state.RecordDigest + "/" + state.ConfigSHA + "/" + state.RollbackStatus
			}
		} else if object.Immutable == nil || !*object.Immutable {
			raw, _ := declarativerelease.CanonicalJSON(object.Data)
			value += "/" + digest(raw)
		}
		rows = append(rows, value)
	}
	sort.Strings(rows)
	return strings.Join(rows, "\n")
}
