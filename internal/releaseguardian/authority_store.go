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

type legacyCandidateCanaryResultV1 struct {
	APIVersion            string        `json:"apiVersion"`
	Kind                  string        `json:"kind"`
	GroupID               string        `json:"groupId"`
	CandidateRecordDigest string        `json:"candidateRecordDigest"`
	WorkerSlot            AuthoritySlot `json:"workerSlot"`
	ReleaseRecordDigest   string        `json:"releaseRecordDigest"`
	RouteState            HealthState   `json:"routeState"`
	DependencyState       HealthState   `json:"dependencyState"`
	EvidenceDigest        string        `json:"evidenceDigest"`
	ObservedAt            string        `json:"observedAt"`
	ExpiresAt             string        `json:"expiresAt"`
	KeyID                 string        `json:"keyId"`
	Signature             string        `json:"signature"`
	ResultDigest          string        `json:"resultDigest"`
}

type legacyCandidateCanaryResultV2 struct {
	APIVersion                 string        `json:"apiVersion"`
	Kind                       string        `json:"kind"`
	GroupID                    string        `json:"groupId"`
	CandidateRecordDigest      string        `json:"candidateRecordDigest"`
	WorkerSlot                 AuthoritySlot `json:"workerSlot"`
	AuthoritySequence          uint64        `json:"authoritySequence"`
	CandidateSequence          uint64        `json:"candidateSequence"`
	CurrentPublicationSequence uint64        `json:"currentPublicationSequence"`
	CurrentRecoveryEpoch       uint64        `json:"currentRecoveryEpoch"`
	CurrentBundleDigest        string        `json:"currentBundleDigest"`
	CandidateEpoch             uint64        `json:"candidateEpoch"`
	BundleGeneration           string        `json:"bundleGeneration"`
	ServingGeneration          string        `json:"servingGeneration"`
	WorkerSourceSHA            string        `json:"workerSourceSha"`
	WorkerImageDigest          string        `json:"workerImageDigest"`
	WorkerCohortDigest         string        `json:"workerCohortDigest"`
	ReleaseRecordDigest        string        `json:"releaseRecordDigest"`
	RouteState                 HealthState   `json:"routeState"`
	DependencyState            HealthState   `json:"dependencyState"`
	EvidenceDigest             string        `json:"evidenceDigest"`
	ObservedAt                 string        `json:"observedAt"`
	ExpiresAt                  string        `json:"expiresAt"`
	KeyID                      string        `json:"keyId"`
	Signature                  string        `json:"signature"`
	ResultDigest               string        `json:"resultDigest"`
}

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

func (store *AuthorityStore) LoadRouteBundleRecord(ctx context.Context, groupID, recordDigest string) (RouteBundleRecord, error) {
	if !groupPattern.MatchString(groupID) || !digestPattern.MatchString(recordDigest) {
		return RouteBundleRecord{}, errors.New("route bundle record lookup is invalid")
	}
	object, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, routeBundleRecordName(groupID, recordDigest), metav1.GetOptions{})
	if err != nil {
		return RouteBundleRecord{}, err
	}
	var record RouteBundleRecord
	if object.Immutable == nil || !*object.Immutable || len(object.Data) != 1 || object.Labels["fugue.pro/group"] != groupID ||
		object.Labels["fugue.pro/authority-kind"] != "route-bundle" || decodeStrict([]byte(object.Data["record.json"]), &record) != nil ||
		record.Validate() != nil || record.GroupID != groupID || record.RecordDigest != recordDigest {
		return RouteBundleRecord{}, errors.New("route bundle record object is invalid")
	}
	return record, nil
}

func (store *AuthorityStore) CreateCandidateCanaryResult(ctx context.Context, result CandidateCanaryResult, now time.Time) error {
	if err := result.Validate(now); err != nil {
		return err
	}
	return store.createImmutableWithLabels(ctx, candidateCanaryResultName(result.GroupID, result.ResultDigest), result.GroupID, "candidate-canary", "result.json", result,
		map[string]string{"fugue.pro/candidate-record": candidateRecordLabel(result.CandidateRecordDigest)})
}

func (store *AuthorityStore) CreateTransitionJournal(ctx context.Context, journal AuthorityTransitionJournal) error {
	if journal.Validate() != nil || journal.Phase != AuthorityTransitionPrepared {
		return errors.New("authority transition journal create is invalid")
	}
	return store.createImmutable(ctx, transitionJournalName(journal.GroupID), journal.GroupID, "transition-journal", "journal.json", journal)
}

func (store *AuthorityStore) LoadTransitionJournal(ctx context.Context, groupID string) (AuthorityTransitionJournal, bool, error) {
	if !groupPattern.MatchString(groupID) {
		return AuthorityTransitionJournal{}, false, errors.New("authority transition group is invalid")
	}
	for _, name := range []string{transitionActivatedJournalName(groupID), transitionJournalName(groupID)} {
		object, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return AuthorityTransitionJournal{}, false, err
		}
		var journal AuthorityTransitionJournal
		if object.Immutable == nil || !*object.Immutable || object.Labels["fugue.pro/group"] != groupID ||
			object.Labels["fugue.pro/authority-kind"] != "transition-journal" || len(object.Data) != 1 ||
			decodeStrict([]byte(object.Data["journal.json"]), &journal) != nil || journal.Validate() != nil || journal.GroupID != groupID {
			return AuthorityTransitionJournal{}, false, errors.New("authority transition journal object is invalid")
		}
		return journal, true, nil
	}
	return AuthorityTransitionJournal{}, false, nil
}

func (store *AuthorityStore) UpdateTransitionJournal(ctx context.Context, before, after AuthorityTransitionJournal) error {
	if before.Validate() != nil || after.Validate() != nil || before.Phase != AuthorityTransitionPrepared || after.Phase != AuthorityTransitionActivated ||
		before.GroupID != after.GroupID || before.CurrentUID != after.CurrentUID || before.CurrentRV != after.CurrentRV ||
		before.Candidate.RecordDigest != after.Candidate.RecordDigest || before.CanaryResultDigest != after.CanaryResultDigest {
		return errors.New("authority transition journal update is invalid")
	}
	// Immutable journals are phase-addressed. Once the activated witness exists
	// it is the durable recovery authority. Failure to garbage-collect the
	// prepared witness must not be misreported as failure to persist activation
	// (and must never trigger compensation of an otherwise resumable change).
	if err := store.createImmutable(ctx, transitionActivatedJournalName(after.GroupID), after.GroupID, "transition-journal", "journal.json", after); err != nil {
		return err
	}
	_ = store.deleteImmutableJournal(ctx, transitionJournalName(before.GroupID), before)
	return nil
}

func (store *AuthorityStore) DeleteTransitionJournal(ctx context.Context, journal AuthorityTransitionJournal) error {
	if journal.Validate() != nil {
		return errors.New("authority transition journal delete is invalid")
	}
	if journal.Phase == AuthorityTransitionActivated {
		prepared := journal
		prepared.Phase, prepared.Activation, prepared.JournalDigest = AuthorityTransitionPrepared, nil, ""
		var err error
		prepared, err = prepared.Seal()
		if err != nil {
			return err
		}
		if err := store.deleteImmutableJournal(ctx, transitionJournalName(journal.GroupID), prepared); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return store.deleteImmutableJournal(ctx, transitionActivatedJournalName(journal.GroupID), journal)
	}
	return store.deleteImmutableJournal(ctx, transitionJournalName(journal.GroupID), journal)
}

func (store *AuthorityStore) deleteImmutableJournal(ctx context.Context, name string, journal AuthorityTransitionJournal) error {
	configMaps := store.client.CoreV1().ConfigMaps(store.namespace)
	object, err := configMaps.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	raw, err := declarativerelease.CanonicalJSON(journal)
	if err != nil || object.Immutable == nil || !*object.Immutable || object.Data["journal.json"] != string(raw) ||
		object.Labels["fugue.pro/group"] != journal.GroupID || object.Labels["fugue.pro/authority-kind"] != "transition-journal" {
		return errors.New("authority transition journal CAS changed")
	}
	uid, rv := object.UID, object.ResourceVersion
	return configMaps.Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
}

func (store *AuthorityStore) PruneExpiredCandidateCanaryResults(ctx context.Context, groupID string, now time.Time) error {
	if !groupPattern.MatchString(groupID) || now.IsZero() || !now.Equal(now.UTC()) {
		return errors.New("candidate canary prune request is invalid")
	}
	protectedDigest := ""
	if journal, exists, err := store.LoadTransitionJournal(ctx, groupID); err != nil {
		return err
	} else if exists {
		protectedDigest = journal.CanaryResultDigest
	}
	selector := labels.Set{"fugue.pro/group": groupID, "fugue.pro/authority-kind": "candidate-canary"}.AsSelector().String()
	objects, err := store.client.CoreV1().ConfigMaps(store.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 257})
	if err != nil {
		return err
	}
	if objects.Continue != "" || len(objects.Items) > 256 {
		return errors.New("candidate canary result set exceeds its cleanup bound")
	}
	for index := range objects.Items {
		object := &objects.Items[index]
		result, resultErr := decodeCandidateCanaryForCleanup(object.Data["result.json"])
		if object.Immutable == nil || !*object.Immutable || len(object.Data) != 1 ||
			object.Labels["fugue.pro/group"] != groupID || object.Labels["fugue.pro/authority-kind"] != "candidate-canary" ||
			resultErr != nil ||
			object.Name != candidateCanaryResultName(result.GroupID, result.ResultDigest) ||
			object.Labels["fugue.pro/candidate-record"] != candidateRecordLabel(result.CandidateRecordDigest) {
			return errors.New("candidate canary cleanup encountered an invalid object")
		}
		expiresAt, _ := time.Parse(time.RFC3339Nano, result.ExpiresAt)
		if now.Before(expiresAt) || result.ResultDigest == protectedDigest {
			continue
		}
		uid := object.UID
		rv := object.ResourceVersion
		if err := store.client.CoreV1().ConfigMaps(store.namespace).Delete(ctx, object.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv},
		}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// Legacy candidate results are recognized only so an expired immutable object
// cannot permanently block the prober after a schema upgrade. They are never
// returned by either canary lookup and therefore cannot authorize authority.
func decodeCandidateCanaryForCleanup(raw string) (CandidateCanaryResult, error) {
	var current CandidateCanaryResult
	if decodeStrict([]byte(raw), &current) == nil && current.Validate(time.Time{}) == nil {
		return current, nil
	}
	var previous legacyCandidateCanaryResultV2
	if decodeStrict([]byte(raw), &previous) == nil && previous.APIVersion == APIVersion && previous.Kind == CandidateCanaryResultKind &&
		groupPattern.MatchString(previous.GroupID) && digestPattern.MatchString(previous.CandidateRecordDigest) && previous.WorkerSlot.Validate() == nil &&
		previous.AuthoritySequence > 0 && previous.CandidateSequence > 0 && previous.CurrentPublicationSequence > 0 &&
		previous.CurrentPublicationSequence <= previous.AuthoritySequence && digestPattern.MatchString(previous.CurrentBundleDigest) &&
		previous.CandidateEpoch > previous.CurrentPublicationSequence && authorityGenerationPattern.MatchString(previous.BundleGeneration) &&
		authorityGenerationPattern.MatchString(previous.ServingGeneration) && shaPattern.MatchString(previous.WorkerSourceSHA) &&
		digestPattern.MatchString(previous.WorkerImageDigest) && digestPattern.MatchString(previous.WorkerCohortDigest) &&
		digestPattern.MatchString(previous.ReleaseRecordDigest) && (previous.RouteState == HealthHealthy || previous.RouteState == HealthDegraded) &&
		(previous.DependencyState == HealthHealthy || previous.DependencyState == HealthDegraded) && digestPattern.MatchString(previous.EvidenceDigest) &&
		componentPattern.MatchString(previous.KeyID) && candidateCanarySignaturePattern.MatchString(previous.Signature) && digestPattern.MatchString(previous.ResultDigest) {
		observedAt, observedErr := time.Parse(time.RFC3339Nano, previous.ObservedAt)
		expiresAt, expiresErr := time.Parse(time.RFC3339Nano, previous.ExpiresAt)
		copy := previous
		copy.ResultDigest = ""
		encoded, encodeErr := declarativerelease.CanonicalJSON(copy)
		if observedErr == nil && expiresErr == nil && observedAt.Equal(observedAt.UTC()) && expiresAt.Equal(expiresAt.UTC()) &&
			expiresAt.After(observedAt) && expiresAt.Sub(observedAt) <= time.Minute && encodeErr == nil && digest(encoded) == previous.ResultDigest {
			return CandidateCanaryResult{GroupID: previous.GroupID, CandidateRecordDigest: previous.CandidateRecordDigest,
				ResultDigest: previous.ResultDigest, ExpiresAt: previous.ExpiresAt}, nil
		}
	}
	var legacy legacyCandidateCanaryResultV1
	if decodeStrict([]byte(raw), &legacy) != nil || legacy.APIVersion != APIVersion || legacy.Kind != CandidateCanaryResultKind ||
		!groupPattern.MatchString(legacy.GroupID) || !digestPattern.MatchString(legacy.CandidateRecordDigest) || legacy.WorkerSlot.Validate() != nil ||
		!digestPattern.MatchString(legacy.ReleaseRecordDigest) || (legacy.RouteState != HealthHealthy && legacy.RouteState != HealthDegraded) ||
		(legacy.DependencyState != HealthHealthy && legacy.DependencyState != HealthDegraded) || !digestPattern.MatchString(legacy.EvidenceDigest) ||
		!componentPattern.MatchString(legacy.KeyID) || !candidateCanarySignaturePattern.MatchString(legacy.Signature) || !digestPattern.MatchString(legacy.ResultDigest) {
		return CandidateCanaryResult{}, errors.New("legacy candidate canary result is invalid")
	}
	observedAt, observedErr := time.Parse(time.RFC3339Nano, legacy.ObservedAt)
	expiresAt, expiresErr := time.Parse(time.RFC3339Nano, legacy.ExpiresAt)
	if observedErr != nil || expiresErr != nil || !observedAt.Equal(observedAt.UTC()) || !expiresAt.Equal(expiresAt.UTC()) ||
		!expiresAt.After(observedAt) || expiresAt.Sub(observedAt) > time.Minute {
		return CandidateCanaryResult{}, errors.New("legacy candidate canary freshness is invalid")
	}
	copy := legacy
	copy.ResultDigest = ""
	encoded, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(encoded) != legacy.ResultDigest {
		return CandidateCanaryResult{}, errors.New("legacy candidate canary digest is invalid")
	}
	return CandidateCanaryResult{GroupID: legacy.GroupID, CandidateRecordDigest: legacy.CandidateRecordDigest,
		ResultDigest: legacy.ResultDigest, ExpiresAt: legacy.ExpiresAt}, nil
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
	if !groupPattern.MatchString(groupID) {
		return CurrentAuthority{}, "", "", errors.New("authority group identity is invalid")
	}
	object, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, currentAuthorityName(groupID), metav1.GetOptions{})
	if err != nil {
		return CurrentAuthority{}, "", "", err
	}
	if object.Immutable != nil && *object.Immutable || object.UID == "" || strings.TrimSpace(object.ResourceVersion) == "" ||
		object.Labels["fugue.pro/group"] != groupID || object.Labels["fugue.pro/authority-store"] != "true" || len(object.Data) < 1 || len(object.Data) > 3 {
		return CurrentAuthority{}, "", "", errors.New("mutable authority object metadata is invalid")
	}
	var authority CurrentAuthority
	if decodeStrict([]byte(object.Data["authority.json"]), &authority) != nil || authority.Validate() != nil {
		return CurrentAuthority{}, "", "", errors.New("mutable authority payload is invalid")
	}
	if raw, exists := object.Data["baseline-receipt.json"]; exists {
		var receipt AuthorityBaselineReceipt
		if decodeStrict([]byte(raw), &receipt) != nil || receipt.Validate() != nil || receipt.GroupID != groupID ||
			authority.BaselineReceiptDigest != receipt.ReceiptDigest {
			return CurrentAuthority{}, "", "", errors.New("authority baseline receipt binding is invalid")
		}
	} else if authority.BaselineReceiptDigest != "" {
		return CurrentAuthority{}, "", "", errors.New("authority baseline receipt is missing")
	}
	if raw, exists := object.Data["normalization-receipt.json"]; exists {
		var receipt AuthorityNormalizationReceipt
		if decodeStrict([]byte(raw), &receipt) != nil || receipt.Validate() != nil || receipt.GroupID != groupID ||
			receipt.BaselineReceiptDigest != authority.BaselineReceiptDigest || authority.AuthorityEpoch < receipt.After.AuthorityEpoch {
			return CurrentAuthority{}, "", "", errors.New("authority normalization receipt binding is invalid")
		}
	}
	return authority, object.UID, object.ResourceVersion, nil
}

func (store *AuthorityStore) LoadBaselineReceipt(ctx context.Context, groupID string) (AuthorityBaselineReceipt, error) {
	if !groupPattern.MatchString(groupID) {
		return AuthorityBaselineReceipt{}, errors.New("authority group identity is invalid")
	}
	object, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, currentAuthorityName(groupID), metav1.GetOptions{})
	if err != nil {
		return AuthorityBaselineReceipt{}, err
	}
	var authority CurrentAuthority
	var receipt AuthorityBaselineReceipt
	if object.UID == "" || object.ResourceVersion == "" || object.Labels["fugue.pro/group"] != groupID ||
		decodeStrict([]byte(object.Data["authority.json"]), &authority) != nil || authority.Validate() != nil ||
		decodeStrict([]byte(object.Data["baseline-receipt.json"]), &receipt) != nil || receipt.Validate() != nil ||
		receipt.GroupID != groupID || authority.BaselineReceiptDigest != receipt.ReceiptDigest {
		return AuthorityBaselineReceipt{}, errors.New("authority baseline receipt is invalid")
	}
	return receipt, nil
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
	return store.createImmutableWithLabels(ctx, name, groupID, kind, key, value, nil)
}

func (store *AuthorityStore) createImmutableWithLabels(ctx context.Context, name, groupID, kind, key string, value any, additional map[string]string) error {
	raw, err := declarativerelease.CanonicalJSON(value)
	if err != nil {
		return err
	}
	configMaps := store.client.CoreV1().ConfigMaps(store.namespace)
	labels := authorityLabels(groupID)
	labels["fugue.pro/authority-kind"] = kind
	for label, value := range additional {
		labels[label] = value
	}
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
	for label, value := range additional {
		if existing.Labels[label] != value {
			return errors.New("immutable authority record labels conflict with existing object")
		}
	}
	return nil
}

func (store *AuthorityStore) LoadCandidateCanaryResult(ctx context.Context, candidate CandidateAuthority, resultDigest string, now time.Time) (CandidateCanaryResult, error) {
	if candidate.Validate() != nil || !candidate.HasPromotionWitness() || !digestPattern.MatchString(resultDigest) {
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
		result.AuthoritySequence != candidate.AuthoritySequence || result.CandidateSequence != candidate.CandidateSequence ||
		result.CurrentPublicationSequence != candidate.CurrentPublicationSequence || result.CurrentRecoveryEpoch != candidate.CurrentRecoveryEpoch ||
		result.CurrentBundleDigest != candidate.CurrentBundleDigest || result.CurrentServingGeneration != candidate.CurrentServingGeneration || result.CandidateEpoch != candidate.CandidateEpoch ||
		result.BundleGeneration != candidate.BundleGeneration || result.ServingGeneration != candidate.ServingGeneration ||
		result.WorkerSlot != candidate.WorkerSlot || result.ReleaseRecordDigest != candidate.ReleaseRecordDigest {
		return CandidateCanaryResult{}, errors.New("candidate canary object binding is invalid")
	}
	return result, nil
}

func (store *AuthorityStore) LoadLatestCandidateCanaryResult(ctx context.Context, candidate CandidateAuthority, now time.Time) (CandidateCanaryResult, error) {
	if candidate.Validate() != nil || !candidate.HasPromotionWitness() {
		return CandidateCanaryResult{}, errors.New("candidate canary lookup is invalid")
	}
	selector := labels.Set{
		"fugue.pro/group": candidate.GroupID, "fugue.pro/authority-kind": "candidate-canary",
		"fugue.pro/candidate-record": candidateRecordLabel(candidate.RecordDigest),
	}.AsSelector().String()
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
			object.Labels["fugue.pro/group"] != candidate.GroupID || object.Labels["fugue.pro/authority-kind"] != "candidate-canary" ||
			object.Labels["fugue.pro/candidate-record"] != candidateRecordLabel(candidate.RecordDigest) {
			return CandidateCanaryResult{}, errors.New("candidate canary object metadata is invalid")
		}
		var result CandidateCanaryResult
		if err := decodeStrict([]byte(object.Data["result.json"]), &result); err != nil || result.Validate(time.Time{}) != nil ||
			object.Name != candidateCanaryResultName(result.GroupID, result.ResultDigest) {
			return CandidateCanaryResult{}, errors.New("candidate canary object is invalid")
		}
		if result.GroupID != candidate.GroupID || result.CandidateRecordDigest != candidate.RecordDigest ||
			result.AuthoritySequence != candidate.AuthoritySequence || result.CandidateSequence != candidate.CandidateSequence ||
			result.CurrentPublicationSequence != candidate.CurrentPublicationSequence || result.CurrentRecoveryEpoch != candidate.CurrentRecoveryEpoch ||
			result.CurrentBundleDigest != candidate.CurrentBundleDigest || result.CurrentServingGeneration != candidate.CurrentServingGeneration || result.CandidateEpoch != candidate.CandidateEpoch ||
			result.BundleGeneration != candidate.BundleGeneration || result.ServingGeneration != candidate.ServingGeneration ||
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
		if current.GroupID != candidate.GroupID || current.RecordDigest != candidate.RecordDigest || current.BundleGeneration != candidate.BundleGeneration ||
			current.ServingGeneration != candidate.ServingGeneration || current.WorkerSlot != candidate.WorkerSlot ||
			current.AuthoritySequence != candidate.AuthoritySequence || current.CandidateSequence != candidate.CandidateSequence ||
			current.CurrentPublicationSequence != candidate.CurrentPublicationSequence || current.CurrentRecoveryEpoch != candidate.CurrentRecoveryEpoch ||
			current.CurrentBundleDigest != candidate.CurrentBundleDigest || current.CurrentServingGeneration != candidate.CurrentServingGeneration || current.CandidateEpoch != candidate.CandidateEpoch ||
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
			(candidate.RecordDigest == current.RecordDigest && candidate.BundleGeneration == current.BundleGeneration && candidate.ServingGeneration == current.ServingGeneration &&
				candidate.AuthoritySequence == current.AuthoritySequence && candidate.CandidateSequence == current.CandidateSequence &&
				candidate.CurrentPublicationSequence == current.CurrentPublicationSequence && candidate.CurrentRecoveryEpoch == current.CurrentRecoveryEpoch &&
				candidate.CurrentBundleDigest == current.CurrentBundleDigest && candidate.CurrentServingGeneration == current.CurrentServingGeneration && candidate.CandidateEpoch == current.CandidateEpoch &&
				candidate.WorkerSlot == current.WorkerSlot && candidate.ReleaseRecordDigest == current.ReleaseRecordDigest) {
			return errors.New("loaded candidate replacement changes an ineligible pointer")
		}
		return nil
	})
}

// ReplaceSettledCandidate advances a terminal pointer only after no immutable
// transition journal remains. Reconcile never starts a verified candidate
// without such a journal, so this CAS cannot race a production activation.
func (store *AuthorityStore) ReplaceSettledCandidate(ctx context.Context, candidate CandidateAuthority, expectedUID types.UID, expectedResourceVersion string) (types.UID, string, error) {
	if err := candidate.Validate(); err != nil || candidate.State != CandidateAuthorityLoaded {
		return "", "", errors.New("settled candidate replacement is invalid")
	}
	if _, exists, err := store.LoadTransitionJournal(ctx, candidate.GroupID); err != nil || exists {
		return "", "", errors.New("settled candidate still has an active transition")
	}
	return store.putMutable(ctx, candidateAuthorityName(candidate.GroupID), candidate.GroupID, "candidate.json", candidate, expectedUID, expectedResourceVersion, func(raw string) error {
		var current CandidateAuthority
		if err := decodeStrict([]byte(raw), &current); err != nil || current.Validate() != nil || current.State == CandidateAuthorityLoaded ||
			candidate.Generation != current.Generation+1 || candidate.GroupID != current.GroupID ||
			(candidate.RecordDigest == current.RecordDigest && candidate.BundleGeneration == current.BundleGeneration && candidate.ServingGeneration == current.ServingGeneration &&
				candidate.AuthoritySequence == current.AuthoritySequence && candidate.CandidateSequence == current.CandidateSequence &&
				candidate.CurrentPublicationSequence == current.CurrentPublicationSequence && candidate.CurrentRecoveryEpoch == current.CurrentRecoveryEpoch &&
				candidate.CurrentBundleDigest == current.CurrentBundleDigest && candidate.CurrentServingGeneration == current.CurrentServingGeneration && candidate.CandidateEpoch == current.CandidateEpoch &&
				candidate.WorkerSlot == current.WorkerSlot && candidate.ReleaseRecordDigest == current.ReleaseRecordDigest) {
			return errors.New("settled candidate replacement changes an ineligible pointer")
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
			authority.CurrentRecordDigest == current.CurrentRecordDigest || authority.CurrentWorkerSlot == current.CurrentWorkerSlot ||
			authority.BaselineReceiptDigest != current.BaselineReceiptDigest {
			return errors.New("current authority transition is not an atomic slot switch")
		}
		return nil
	})
}

func (store *AuthorityStore) AdoptCurrentBaseline(ctx context.Context, authority CurrentAuthority, receipt AuthorityBaselineReceipt, expectedUID types.UID, expectedResourceVersion string) (types.UID, string, error) {
	if authority.Validate() != nil || receipt.Validate() != nil || authority.GroupID != receipt.GroupID ||
		authority.CurrentRecordDigest != receipt.RecordDigest || authority.CurrentWorkerSlot != receipt.WorkerSlot ||
		authority.AuthorityEpoch != receipt.AuthorityEpoch || authority.BaselineReceiptDigest != receipt.ReceiptDigest ||
		authority.PreviousRecordDigest != "" || authority.PreviousWorkerSlot != "" || expectedUID == "" || strings.TrimSpace(expectedResourceVersion) == "" {
		return "", "", errors.New("authority baseline adoption is invalid")
	}
	for _, node := range receipt.Nodes {
		if node.ActivationGeneration != authority.CurrentFrontGeneration || node.BundleGeneration != authority.CurrentBundleGeneration ||
			node.WorkerSourceSHA != authority.CurrentWorkerSourceSHA || node.WorkerImageDigest != authority.CurrentWorkerImageDigest {
			return "", "", errors.New("authority baseline workload identity is mixed")
		}
	}
	configMaps := store.client.CoreV1().ConfigMaps(store.namespace)
	current, err := configMaps.Get(ctx, currentAuthorityName(authority.GroupID), metav1.GetOptions{})
	if err != nil || current.UID != expectedUID || current.ResourceVersion != expectedResourceVersion ||
		current.Labels["fugue.pro/group"] != authority.GroupID || len(current.Data) != 1 {
		return "", "", errors.New("authority baseline adoption CAS changed")
	}
	var before CurrentAuthority
	if decodeStrict([]byte(current.Data["authority.json"]), &before) != nil || before.Validate() != nil || before.BaselineReceiptDigest != "" ||
		before.CurrentRecordDigest != receipt.BeforeRecordDigest || before.CurrentWorkerSlot != receipt.BeforeWorkerSlot || before.AuthorityEpoch != receipt.BeforeAuthorityEpoch {
		return "", "", errors.New("authority baseline adoption predecessor changed")
	}
	authorityRaw, err := declarativerelease.CanonicalJSON(authority)
	if err != nil {
		return "", "", err
	}
	receiptRaw, err := declarativerelease.CanonicalJSON(receipt)
	if err != nil {
		return "", "", err
	}
	updated := current.DeepCopy()
	updated.Data = map[string]string{"authority.json": string(authorityRaw), "baseline-receipt.json": string(receiptRaw)}
	updated.Labels = authorityLabels(authority.GroupID)
	result, err := configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return "", "", err
	}
	return result.UID, result.ResourceVersion, nil
}

func (store *AuthorityStore) NormalizeCurrentBaseline(ctx context.Context, authority CurrentAuthority, receipt AuthorityNormalizationReceipt, expectedUID types.UID, expectedResourceVersion string) (types.UID, string, error) {
	if authority.Validate() != nil || receipt.Validate() != nil || authority != receipt.After || receipt.Before.GroupID != authority.GroupID ||
		expectedUID == "" || strings.TrimSpace(expectedResourceVersion) == "" {
		return "", "", errors.New("authority normalization is invalid")
	}
	configMaps := store.client.CoreV1().ConfigMaps(store.namespace)
	current, err := configMaps.Get(ctx, currentAuthorityName(authority.GroupID), metav1.GetOptions{})
	if err != nil || current.UID != expectedUID || current.ResourceVersion != expectedResourceVersion ||
		current.Labels["fugue.pro/group"] != authority.GroupID || len(current.Data) != 2 {
		return "", "", errors.New("authority normalization CAS changed")
	}
	var before CurrentAuthority
	var baseline AuthorityBaselineReceipt
	if decodeStrict([]byte(current.Data["authority.json"]), &before) != nil || before != receipt.Before || before.Validate() != nil ||
		decodeStrict([]byte(current.Data["baseline-receipt.json"]), &baseline) != nil || baseline.Validate() != nil ||
		baseline.ReceiptDigest != receipt.BaselineReceiptDigest || before.BaselineReceiptDigest != baseline.ReceiptDigest ||
		before.PreviousRecordDigest == "" || before.PreviousWorkerSlot == "" {
		return "", "", errors.New("authority normalization predecessor changed")
	}
	authorityRaw, err := declarativerelease.CanonicalJSON(authority)
	if err != nil {
		return "", "", err
	}
	receiptRaw, err := declarativerelease.CanonicalJSON(receipt)
	if err != nil {
		return "", "", err
	}
	updated := current.DeepCopy()
	updated.Data = map[string]string{
		"authority.json": string(authorityRaw), "baseline-receipt.json": current.Data["baseline-receipt.json"],
		"normalization-receipt.json": string(receiptRaw),
	}
	updated.Labels = authorityLabels(authority.GroupID)
	result, err := configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return "", "", err
	}
	return result.UID, result.ResourceVersion, nil
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
	if key == "authority.json" {
		if receipt, exists := current.Data["baseline-receipt.json"]; exists {
			updated.Data["baseline-receipt.json"] = receipt
		}
		if receipt, exists := current.Data["normalization-receipt.json"]; exists {
			updated.Data["normalization-receipt.json"] = receipt
		}
	}
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

func candidateRecordLabel(recordDigest string) string {
	value := strings.TrimPrefix(recordDigest, "sha256:")
	if len(value) > 32 {
		value = value[:32]
	}
	return value
}

func candidateAuthorityName(groupID string) string { return "fugue-candidate-authority-" + groupID }
func currentAuthorityName(groupID string) string   { return "fugue-current-authority-" + groupID }
func transitionJournalName(groupID string) string {
	return "fugue-authority-transition-prepared-" + groupID
}
func transitionActivatedJournalName(groupID string) string {
	return "fugue-authority-transition-activated-" + groupID
}

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
