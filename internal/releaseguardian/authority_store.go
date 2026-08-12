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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

var ErrCandidateCanaryUnavailable = errors.New("candidate canary result is unavailable")

// AuthorityStore persists only group-local authority records. It is not wired
// into the rollout controller until the inactive candidate path is proven.
type AuthorityStore struct {
	client    kubernetes.Interface
	namespace string
}

func NewAuthorityStore(client kubernetes.Interface, namespace string) (*AuthorityStore, error) {
	if client == nil || !componentPattern.MatchString(namespace) {
		return nil, errors.New("authority store configuration is invalid")
	}
	return &AuthorityStore{client: client, namespace: namespace}, nil
}

func (store *AuthorityStore) CreateRouteBundleRecord(ctx context.Context, record RouteBundleRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	return store.createImmutable(ctx, routeBundleRecordName(record.GroupID, record.RecordDigest), record.GroupID, "route-bundle", "record.json", record)
}

func (store *AuthorityStore) CreateCandidateCanaryResult(ctx context.Context, result CandidateCanaryResult, now time.Time) error {
	if err := result.Validate(now); err != nil {
		return err
	}
	return store.createImmutable(ctx, candidateCanaryResultName(result.GroupID, result.ResultDigest), result.GroupID, "candidate-canary", "result.json", result)
}

func (store *AuthorityStore) LoadCandidate(ctx context.Context, groupID string) (CandidateAuthority, types.UID, string, error) {
	var candidate CandidateAuthority
	uid, rv, err := store.loadMutable(ctx, candidateAuthorityName(groupID), groupID, "candidate.json", &candidate)
	if err != nil {
		return CandidateAuthority{}, "", "", err
	}
	if err := candidate.Validate(); err != nil {
		return CandidateAuthority{}, "", "", err
	}
	return candidate, uid, rv, nil
}

func (store *AuthorityStore) LoadCurrent(ctx context.Context, groupID string) (CurrentAuthority, types.UID, string, error) {
	var authority CurrentAuthority
	uid, rv, err := store.loadMutable(ctx, currentAuthorityName(groupID), groupID, "authority.json", &authority)
	if err != nil {
		return CurrentAuthority{}, "", "", err
	}
	if err := authority.Validate(); err != nil {
		return CurrentAuthority{}, "", "", err
	}
	return authority, uid, rv, nil
}

func (store *AuthorityStore) loadMutable(ctx context.Context, name, groupID, key string, destination any) (types.UID, string, error) {
	if !groupPattern.MatchString(groupID) {
		return "", "", errors.New("authority group identity is invalid")
	}
	object, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}
	if object.Immutable != nil && *object.Immutable || object.UID == "" || strings.TrimSpace(object.ResourceVersion) == "" ||
		object.Labels["fugue.pro/group"] != groupID || object.Labels["fugue.pro/authority-store"] != "true" || len(object.Data) != 1 {
		return "", "", errors.New("mutable authority object metadata is invalid")
	}
	if err := decodeStrict([]byte(object.Data[key]), destination); err != nil {
		return "", "", errors.New("mutable authority payload is invalid")
	}
	return object.UID, object.ResourceVersion, nil
}

func (store *AuthorityStore) createImmutable(ctx context.Context, name, groupID, kind, key string, value any) error {
	raw, err := declarativerelease.CanonicalJSON(value)
	if err != nil {
		return err
	}
	configMaps := store.client.CoreV1().ConfigMaps(store.namespace)
	labels := authorityLabels(groupID)
	labels["fugue.pro/authority-kind"] = kind
	desired := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: store.namespace, Labels: labels}, Immutable: boolPointer(true), Data: map[string]string{key: string(raw)}}
	created, err := configMaps.Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		if created.Immutable == nil || !*created.Immutable {
			return errors.New("immutable authority record was created mutable")
		}
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	existing, getErr := configMaps.Get(ctx, name, metav1.GetOptions{})
	if getErr != nil || existing.Immutable == nil || !*existing.Immutable || existing.Data[key] != string(raw) || existing.Labels["fugue.pro/group"] != groupID || existing.Labels["fugue.pro/authority-kind"] != kind {
		return errors.New("immutable authority record conflicts with existing object")
	}
	return nil
}

func (store *AuthorityStore) LoadCandidateCanaryResult(ctx context.Context, candidate CandidateAuthority, resultDigest string, now time.Time) (CandidateCanaryResult, error) {
	if candidate.Validate() != nil || !digestPattern.MatchString(resultDigest) {
		return CandidateCanaryResult{}, errors.New("candidate canary lookup is invalid")
	}
	object, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, candidateCanaryResultName(candidate.GroupID, resultDigest), metav1.GetOptions{})
	if err != nil {
		return CandidateCanaryResult{}, err
	}
	if object.Immutable == nil || !*object.Immutable || object.Labels["fugue.pro/group"] != candidate.GroupID ||
		object.Labels["fugue.pro/authority-kind"] != "candidate-canary" || len(object.Data) != 1 {
		return CandidateCanaryResult{}, errors.New("candidate canary object metadata is invalid")
	}
	var result CandidateCanaryResult
	if err := decodeStrict([]byte(object.Data["result.json"]), &result); err != nil || result.Validate(now) != nil ||
		result.ResultDigest != resultDigest || result.GroupID != candidate.GroupID || result.CandidateRecordDigest != candidate.RecordDigest ||
		result.WorkerSlot != candidate.WorkerSlot || result.ReleaseRecordDigest != candidate.ReleaseRecordDigest {
		return CandidateCanaryResult{}, errors.New("candidate canary object binding is invalid")
	}
	return result, nil
}

func (store *AuthorityStore) LoadLatestCandidateCanaryResult(ctx context.Context, candidate CandidateAuthority, now time.Time) (CandidateCanaryResult, error) {
	if candidate.Validate() != nil {
		return CandidateCanaryResult{}, errors.New("candidate canary lookup is invalid")
	}
	selector := labels.Set{"fugue.pro/group": candidate.GroupID, "fugue.pro/authority-kind": "candidate-canary"}.AsSelector().String()
	objects, err := store.client.CoreV1().ConfigMaps(store.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 65})
	if err != nil {
		return CandidateCanaryResult{}, err
	}
	if objects.Continue != "" || len(objects.Items) > 64 {
		return CandidateCanaryResult{}, errors.New("candidate canary result set exceeds its bound")
	}
	var latest CandidateCanaryResult
	var latestAt time.Time
	for index := range objects.Items {
		object := &objects.Items[index]
		if object.Immutable == nil || !*object.Immutable || len(object.Data) != 1 ||
			object.Labels["fugue.pro/group"] != candidate.GroupID || object.Labels["fugue.pro/authority-kind"] != "candidate-canary" {
			return CandidateCanaryResult{}, errors.New("candidate canary object metadata is invalid")
		}
		var result CandidateCanaryResult
		if err := decodeStrict([]byte(object.Data["result.json"]), &result); err != nil || result.Validate(time.Time{}) != nil ||
			object.Name != candidateCanaryResultName(result.GroupID, result.ResultDigest) {
			return CandidateCanaryResult{}, errors.New("candidate canary object is invalid")
		}
		if result.GroupID != candidate.GroupID || result.CandidateRecordDigest != candidate.RecordDigest ||
			result.WorkerSlot != candidate.WorkerSlot || result.ReleaseRecordDigest != candidate.ReleaseRecordDigest {
			continue
		}
		expiresAt, _ := time.Parse(time.RFC3339Nano, result.ExpiresAt)
		if now.UTC().After(expiresAt) {
			continue
		}
		observedAt, _ := time.Parse(time.RFC3339Nano, result.ObservedAt)
		if latest.ResultDigest == "" || observedAt.After(latestAt) || observedAt.Equal(latestAt) && result.ResultDigest > latest.ResultDigest {
			latest, latestAt = result, observedAt
		}
	}
	if latest.ResultDigest == "" {
		return CandidateCanaryResult{}, ErrCandidateCanaryUnavailable
	}
	return latest, nil
}

func (store *AuthorityStore) PutCandidate(ctx context.Context, candidate CandidateAuthority, expectedUID types.UID, expectedResourceVersion string) (types.UID, string, error) {
	if err := candidate.Validate(); err != nil {
		return "", "", err
	}
	return store.putMutable(ctx, candidateAuthorityName(candidate.GroupID), candidate.GroupID, "candidate.json", candidate, expectedUID, expectedResourceVersion, func(raw string) error {
		var current CandidateAuthority
		if err := decodeStrict([]byte(raw), &current); err != nil || current.Validate() != nil {
			return errors.New("current candidate authority is invalid")
		}
		if current.GroupID != candidate.GroupID || current.RecordDigest != candidate.RecordDigest || current.WorkerSlot != candidate.WorkerSlot ||
			current.ReleaseRecordDigest != candidate.ReleaseRecordDigest || candidate.Generation != current.Generation+1 {
			return errors.New("candidate authority transition changes immutable identity")
		}
		if current.State != CandidateAuthorityLoaded || (candidate.State != CandidateAuthorityVerified && candidate.State != CandidateAuthorityRejected) {
			return errors.New("candidate authority state transition is invalid")
		}
		return nil
	})
}

// ReplaceLoadedCandidate advances only the pre-traffic candidate pointer. A
// terminal candidate is immutable: once canary verification has started, an
// importer cannot substitute another release beneath that decision.
func (store *AuthorityStore) ReplaceLoadedCandidate(ctx context.Context, candidate CandidateAuthority, expectedUID types.UID, expectedResourceVersion string) (types.UID, string, error) {
	if err := candidate.Validate(); err != nil || candidate.State != CandidateAuthorityLoaded {
		return "", "", errors.New("loaded candidate replacement is invalid")
	}
	return store.putMutable(ctx, candidateAuthorityName(candidate.GroupID), candidate.GroupID, "candidate.json", candidate, expectedUID, expectedResourceVersion, func(raw string) error {
		var current CandidateAuthority
		if err := decodeStrict([]byte(raw), &current); err != nil || current.Validate() != nil || current.State != CandidateAuthorityLoaded ||
			candidate.Generation != current.Generation+1 || candidate.GroupID != current.GroupID ||
			(candidate.RecordDigest == current.RecordDigest && candidate.WorkerSlot == current.WorkerSlot && candidate.ReleaseRecordDigest == current.ReleaseRecordDigest) {
			return errors.New("loaded candidate replacement changes an ineligible pointer")
		}
		return nil
	})
}

// RefreshImportedCurrent synchronizes only the bootstrap current fact exposed
// by Edge Control. It cannot alter an authority that already carries a
// previous/LKG pointer, so it cannot impersonate a verified traffic switch.
func (store *AuthorityStore) RefreshImportedCurrent(ctx context.Context, authority CurrentAuthority, expectedUID types.UID, expectedResourceVersion string) (types.UID, string, error) {
	if err := authority.Validate(); err != nil || authority.PreviousRecordDigest != "" || authority.PreviousWorkerSlot != "" {
		return "", "", errors.New("imported current refresh is invalid")
	}
	return store.putMutable(ctx, currentAuthorityName(authority.GroupID), authority.GroupID, "authority.json", authority, expectedUID, expectedResourceVersion, func(raw string) error {
		var current CurrentAuthority
		if err := decodeStrict([]byte(raw), &current); err != nil || current.Validate() != nil ||
			current.PreviousRecordDigest != "" || current.PreviousWorkerSlot != "" || current.GroupID != authority.GroupID ||
			authority.AuthorityEpoch <= current.AuthorityEpoch ||
			(authority.CurrentRecordDigest == current.CurrentRecordDigest && authority.CurrentWorkerSlot == current.CurrentWorkerSlot) {
			return errors.New("imported current refresh changes an ineligible pointer")
		}
		return nil
	})
}

func (store *AuthorityStore) SwitchCurrent(ctx context.Context, authority CurrentAuthority, expectedUID types.UID, expectedResourceVersion string) (types.UID, string, error) {
	if err := authority.Validate(); err != nil {
		return "", "", err
	}
	return store.putMutable(ctx, currentAuthorityName(authority.GroupID), authority.GroupID, "authority.json", authority, expectedUID, expectedResourceVersion, func(raw string) error {
		var current CurrentAuthority
		if err := decodeStrict([]byte(raw), &current); err != nil || current.Validate() != nil {
			return errors.New("current authority payload is invalid")
		}
		if current.GroupID != authority.GroupID || authority.AuthorityEpoch != current.AuthorityEpoch+1 ||
			authority.PreviousRecordDigest != current.CurrentRecordDigest || authority.PreviousWorkerSlot != current.CurrentWorkerSlot ||
			authority.CurrentRecordDigest == current.CurrentRecordDigest || authority.CurrentWorkerSlot == current.CurrentWorkerSlot {
			return errors.New("current authority transition is not an atomic slot switch")
		}
		return nil
	})
}

type mutableTransitionCheck func(currentPayload string) error

func (store *AuthorityStore) putMutable(ctx context.Context, name, groupID, key string, value any, expectedUID types.UID, expectedResourceVersion string, check mutableTransitionCheck) (types.UID, string, error) {
	raw, err := declarativerelease.CanonicalJSON(value)
	if err != nil {
		return "", "", err
	}
	configMaps := store.client.CoreV1().ConfigMaps(store.namespace)
	if expectedUID == "" && expectedResourceVersion == "" {
		created, createErr := configMaps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: store.namespace, Labels: authorityLabels(groupID)}, Data: map[string]string{key: string(raw)}}, metav1.CreateOptions{})
		if createErr != nil {
			return "", "", createErr
		}
		return created.UID, created.ResourceVersion, nil
	}
	if expectedUID == "" || strings.TrimSpace(expectedResourceVersion) == "" {
		return "", "", errors.New("authority CAS requires both UID and resourceVersion")
	}
	current, err := configMaps.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}
	if current.UID != expectedUID || current.ResourceVersion != expectedResourceVersion || current.Labels["fugue.pro/group"] != groupID {
		return "", "", errors.New("authority UID/resourceVersion CAS changed")
	}
	if err := check(current.Data[key]); err != nil {
		return "", "", err
	}
	updated := current.DeepCopy()
	updated.Data = map[string]string{key: string(raw)}
	updated.Labels = authorityLabels(groupID)
	result, err := configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return "", "", err
	}
	return result.UID, result.ResourceVersion, nil
}

func routeBundleRecordName(groupID, recordDigest string) string {
	return authorityImmutableName("fugue-route-bundle-record", groupID, recordDigest)
}

func candidateCanaryResultName(groupID, resultDigest string) string {
	return authorityImmutableName("fugue-candidate-canary", groupID, resultDigest)
}

func candidateAuthorityName(groupID string) string { return "fugue-candidate-authority-" + groupID }
func currentAuthorityName(groupID string) string   { return "fugue-current-authority-" + groupID }

func authorityImmutableName(prefix, groupID, valueDigest string) string {
	suffix := strings.TrimPrefix(valueDigest, "sha256:")
	if len(suffix) > 16 {
		suffix = suffix[:16]
	}
	return fmt.Sprintf("%s-%s-%s", prefix, groupID, suffix)
}

func authorityLabels(groupID string) map[string]string {
	return map[string]string{"app.kubernetes.io/managed-by": "fugue-release-guardian", "fugue.pro/group": groupID, "fugue.pro/authority-store": "true"}
}

func boolPointer(value bool) *bool { return &value }
