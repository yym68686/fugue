// Package reconcile defines the pure, non-executable CAS and last-known-good
// policy for one cell-local backup observer input Secret. It imports no
// Kubernetes client or type and owns no filesystem, network, datastore, or
// process capability.
package reconcile

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"fugue/internal/backupcontrol"
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/materialization"
)

const (
	ContractAPIVersion = "backup-materializer-reconcile.fugue.dev/v1"
	ManifestKind       = "BackupObserverSecretManifest"
	SnapshotKind       = "BackupObserverSecretSnapshot"
	ObservationKind    = "BackupObserverSecretObservation"
	DecisionKind       = "BackupObserverSecretReconcileDecision"
	ContractPolicy     = "cell-local-secret-cas-lkg-shadow-v1"

	SecretTypeOpaque = "Opaque"

	StateAbsent    CurrentState = "absent"
	StateManaged   CurrentState = "managed"
	StateForeign   CurrentState = "foreign"
	StateMalformed CurrentState = "malformed"

	ActionCreateIfAbsent            Action = "create-if-absent"
	ActionNoop                      Action = "noop"
	ActionReplaceResourceVersionCAS Action = "replace-resource-version-cas"
	ActionRetainLastKnownGood       Action = "retain-last-known-good"
	ActionBlock                     Action = "block"

	ReasonDesiredGenerationReady     Reason = "desired-generation-ready"
	ReasonCurrentGenerationMatches   Reason = "current-generation-matches"
	ReasonDesiredGenerationChanged   Reason = "desired-generation-changed"
	ReasonSourceUnavailableRetainLKG Reason = "source-unavailable-retain-lkg"
	ReasonSourceUnavailableNoLKG     Reason = "source-unavailable-no-lkg"
	ReasonLastKnownGoodExpired       Reason = "last-known-good-expired"
	ReasonLastKnownGoodUnavailable   Reason = "last-known-good-unavailable"
	ReasonCurrentObjectForeign       Reason = "current-object-foreign"
	ReasonCurrentObjectMalformed     Reason = "current-object-malformed"

	LabelName      = "app.kubernetes.io/name"
	LabelComponent = "app.kubernetes.io/component"
	LabelManagedBy = "app.kubernetes.io/managed-by"
	LabelCellID    = "backup.fugue.dev/cell-id"

	AnnotationPlanAPIVersion      = "backup.fugue.dev/plan-api-version"
	AnnotationPlanPolicy          = "backup.fugue.dev/plan-policy"
	AnnotationPlanDigest          = "backup.fugue.dev/plan-digest"
	AnnotationCellKey             = "backup.fugue.dev/cell-key"
	AnnotationRunID               = "backup.fugue.dev/run-id"
	AnnotationSpecDigest          = "backup.fugue.dev/spec-digest"
	AnnotationBundleDigest        = "backup.fugue.dev/bundle-digest"
	AnnotationCredentialID        = "backup.fugue.dev/credential-id"
	AnnotationTokenID             = "backup.fugue.dev/token-id"
	AnnotationSpecDocumentDigest  = "backup.fugue.dev/spec-document-digest"
	AnnotationObserverTokenDigest = "backup.fugue.dev/observer-token-digest"
	AnnotationIssuedAt            = "backup.fugue.dev/issued-at"
	AnnotationRenewAfter          = "backup.fugue.dev/renew-after"
	AnnotationExpiresAt           = "backup.fugue.dev/expires-at"

	labelNameValue      = "fugue-backup-observer"
	labelComponentValue = "backup-observer-input"
	labelManagedByValue = "fugue-backup-materializer"
	maxOpaqueBytes      = 256
)

var ErrReconcile = errors.New("invalid backup observer Secret reconcile contract")

type CurrentState string
type Action string
type Reason string

// Manifest is the public, data-free shape of one desired Secret generation.
// Its maps contain only owned metadata and content digests. A later writer must
// obtain the two private values from the separately sealed Plan.
type Manifest struct {
	APIVersion             string            `json:"apiVersion"`
	Kind                   string            `json:"kind"`
	Policy                 string            `json:"policy"`
	Namespace              string            `json:"namespace"`
	SecretName             string            `json:"secretName"`
	CellKey                string            `json:"cellKey"`
	CellID                 string            `json:"cellId"`
	SecretType             string            `json:"secretType"`
	Labels                 map[string]string `json:"labels"`
	Annotations            map[string]string `json:"annotations"`
	DataDigests            map[string]string `json:"dataDigests"`
	PlanDigest             string            `json:"planDigest"`
	Immutable              bool              `json:"immutable"`
	OwnerReferencesAllowed bool              `json:"ownerReferencesAllowed"`
	Digest                 string            `json:"digest"`
}

// SecretEvidence is the Kubernetes-neutral evidence returned by a future
// fixed-purpose reader. Formatting always redacts all data values.
type SecretEvidence struct {
	Namespace           string
	SecretName          string
	UID                 string
	ResourceVersion     string
	SecretType          string
	Labels              map[string]string
	Annotations         map[string]string
	Data                map[string][]byte
	Immutable           bool
	DeletionPending     bool
	OwnerReferenceCount int
}

// Snapshot seals exact managed-object evidence to the source plan while
// retaining neither public metadata maps nor independent raw Secret values.
// The private plan permits later LKG lifetime validation without exposing its
// bearer through JSON or diagnostic formatting.
type Snapshot struct {
	APIVersion          string    `json:"apiVersion"`
	Kind                string    `json:"kind"`
	Policy              string    `json:"policy"`
	Namespace           string    `json:"namespace"`
	SecretName          string    `json:"secretName"`
	CellKey             string    `json:"cellKey"`
	CellID              string    `json:"cellId"`
	UID                 string    `json:"uid"`
	ResourceVersion     string    `json:"resourceVersion"`
	SecretType          string    `json:"secretType"`
	PlanDigest          string    `json:"planDigest"`
	ManifestDigest      string    `json:"manifestDigest"`
	BundleDigest        string    `json:"bundleDigest"`
	SpecDigest          string    `json:"specDigest"`
	SpecDataDigest      string    `json:"specDataDigest"`
	TokenDataDigest     string    `json:"tokenDataDigest"`
	Immutable           bool      `json:"immutable"`
	OwnerReferenceCount int       `json:"ownerReferenceCount"`
	IssuedAt            time.Time `json:"issuedAt"`
	RenewAfter          time.Time `json:"renewAfter"`
	ExpiresAt           time.Time `json:"expiresAt"`
	Digest              string    `json:"digest"`

	plan materialization.Plan
}

// Observation classifies the exact target name as absent, validly managed, or
// obstructed by a foreign/malformed object. Only the managed constructor can
// attach a sealed Snapshot.
type Observation struct {
	APIVersion      string       `json:"apiVersion"`
	Kind            string       `json:"kind"`
	Policy          string       `json:"policy"`
	State           CurrentState `json:"state"`
	Namespace       string       `json:"namespace"`
	SecretName      string       `json:"secretName"`
	CellKey         string       `json:"cellKey"`
	CellID          string       `json:"cellId"`
	UID             string       `json:"uid,omitempty"`
	ResourceVersion string       `json:"resourceVersion,omitempty"`
	SnapshotDigest  string       `json:"snapshotDigest,omitempty"`
	Digest          string       `json:"digest"`

	snapshot Snapshot
}

// Decision is deliberately a shadow decision. MutationCandidate describes
// what a future, separately gated writer could do, but both execution flags
// remain false and deletion is forbidden in every state.
type Decision struct {
	APIVersion                string    `json:"apiVersion"`
	Kind                      string    `json:"kind"`
	Policy                    string    `json:"policy"`
	Action                    Action    `json:"action"`
	Reason                    Reason    `json:"reason"`
	Namespace                 string    `json:"namespace"`
	SecretName                string    `json:"secretName"`
	CellKey                   string    `json:"cellKey"`
	CellID                    string    `json:"cellId"`
	DesiredPlanDigest         string    `json:"desiredPlanDigest,omitempty"`
	CurrentPlanDigest         string    `json:"currentPlanDigest,omitempty"`
	CurrentSnapshotDigest     string    `json:"currentSnapshotDigest,omitempty"`
	CurrentUID                string    `json:"currentUid,omitempty"`
	CurrentResourceVersion    string    `json:"currentResourceVersion,omitempty"`
	ExpectedUID               string    `json:"expectedUid,omitempty"`
	ExpectedResourceVersion   string    `json:"expectedResourceVersion,omitempty"`
	LastKnownGoodExpiresAt    time.Time `json:"lastKnownGoodExpiresAt,omitempty"`
	DecidedAt                 time.Time `json:"decidedAt"`
	MutationCandidate         bool      `json:"mutationCandidate"`
	Stable                    bool      `json:"stable"`
	Blocked                   bool      `json:"blocked"`
	RequireAbsent             bool      `json:"requireAbsent"`
	RequireUIDMatch           bool      `json:"requireUidMatch"`
	RequireResourceVersionCAS bool      `json:"requireResourceVersionCas"`
	RetainExisting            bool      `json:"retainExisting"`
	DeleteAllowed             bool      `json:"deleteAllowed"`
	ExecutionAllowed          bool      `json:"executionAllowed"`
	ProductionMutationAllowed bool      `json:"productionMutationAllowed"`
	Digest                    string    `json:"digest"`
}

// BuildManifest returns the exact public shape for a currently applyable
// plan. It contains no raw spec document or observer bearer.
func BuildManifest(plan materialization.Plan, now time.Time) (Manifest, error) {
	if err := materialization.Validate(plan, now); err != nil {
		return Manifest{}, ErrReconcile
	}
	return manifestForSealedPlan(plan), nil
}

// SealCurrent accepts only the expected Opaque Secret, exact two data keys,
// matching content digests, and all owned metadata bindings. Unknown labels
// and annotations are preserved outside this contract and do not invalidate a
// generation; unknown data keys always do.
func SealCurrent(plan materialization.Plan, evidence SecretEvidence) (Snapshot, error) {
	if err := materialization.ValidateSealed(plan); err != nil ||
		evidence.Namespace != plan.Namespace || evidence.SecretName != plan.SecretName ||
		evidence.SecretType != SecretTypeOpaque || !validOpaque(evidence.UID) ||
		!validOpaque(evidence.ResourceVersion) || evidence.Immutable || evidence.DeletionPending ||
		evidence.OwnerReferenceCount != 0 || len(evidence.Data) != 2 {
		return Snapshot{}, ErrReconcile
	}
	manifest := manifestForSealedPlan(plan)
	if !containsRequired(evidence.Labels, manifest.Labels) ||
		!containsRequired(evidence.Annotations, manifest.Annotations) ||
		len(evidence.Data[plan.SpecKey]) == 0 || len(evidence.Data[plan.TokenKey]) == 0 ||
		digestBytes(evidence.Data[plan.SpecKey]) != plan.SpecDocumentDigest ||
		digestBytes(evidence.Data[plan.TokenKey]) != plan.ObserverTokenDigest {
		return Snapshot{}, ErrReconcile
	}
	for key := range evidence.Data {
		if key != plan.SpecKey && key != plan.TokenKey {
			return Snapshot{}, ErrReconcile
		}
	}
	snapshot := Snapshot{
		APIVersion:          ContractAPIVersion,
		Kind:                SnapshotKind,
		Policy:              ContractPolicy,
		Namespace:           plan.Namespace,
		SecretName:          plan.SecretName,
		CellKey:             plan.CellKey,
		CellID:              plan.CellID,
		UID:                 evidence.UID,
		ResourceVersion:     evidence.ResourceVersion,
		SecretType:          SecretTypeOpaque,
		PlanDigest:          plan.Digest,
		ManifestDigest:      manifest.Digest,
		BundleDigest:        plan.BundleDigest,
		SpecDigest:          plan.SpecDigest,
		SpecDataDigest:      plan.SpecDocumentDigest,
		TokenDataDigest:     plan.ObserverTokenDigest,
		Immutable:           false,
		OwnerReferenceCount: 0,
		IssuedAt:            plan.IssuedAt,
		RenewAfter:          plan.RenewAfter,
		ExpiresAt:           plan.ExpiresAt,
		plan:                plan,
	}
	snapshot.Digest = DigestSnapshot(snapshot)
	if err := ValidateSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

// RecoverCurrent reconstructs the private sealed plan using only one existing
// Secret's owned annotations and exact data values. It deliberately validates
// no current apply or LKG lifetime; SealCurrent proves the object binding and a
// later decision applies the appropriate current-time gate.
func RecoverCurrent(evidence SecretEvidence) (Snapshot, error) {
	issuedAt, issuedOK := parseCanonicalTimestamp(evidence.Annotations[AnnotationIssuedAt])
	renewAfter, renewOK := parseCanonicalTimestamp(evidence.Annotations[AnnotationRenewAfter])
	expiresAt, expiresOK := parseCanonicalTimestamp(evidence.Annotations[AnnotationExpiresAt])
	specDocument, specOK := evidence.Data[materialization.SpecDataKey]
	observerToken, tokenOK := evidence.Data[materialization.TokenDataKey]
	if !issuedOK || !renewOK || !expiresOK || !specOK || !tokenOK {
		return Snapshot{}, ErrReconcile
	}
	desiredSpec, err := backupcontrol.DecodeBackupRunSpec(specDocument)
	if err != nil {
		return Snapshot{}, ErrReconcile
	}
	bundle := materializercontract.ObserverInputBundle{
		APIVersion:                materializercontract.ObserverInputBundleAPIVersion,
		Kind:                      materializercontract.ObserverInputBundleKind,
		Policy:                    materializercontract.ObserverInputBundlePolicy,
		CellKey:                   evidence.Annotations[AnnotationCellKey],
		RunID:                     evidence.Annotations[AnnotationRunID],
		SpecDigest:                evidence.Annotations[AnnotationSpecDigest],
		CredentialID:              evidence.Annotations[AnnotationCredentialID],
		TokenID:                   evidence.Annotations[AnnotationTokenID],
		DesiredSpec:               desiredSpec,
		ObserverToken:             string(observerToken),
		IssuedAt:                  issuedAt,
		RenewAfter:                renewAfter,
		ExpiresAt:                 expiresAt,
		ObservationOnly:           true,
		ProductionMutationAllowed: false,
		Digest:                    evidence.Annotations[AnnotationBundleDigest],
	}
	plan, err := materialization.RestoreSealed(bundle)
	if err != nil {
		return Snapshot{}, ErrReconcile
	}
	return SealCurrent(plan, evidence)
}

// ObserveExisting classifies one object returned for the exact cell target.
// An object that does not claim this materializer's managed-by label is
// foreign. A claimed object that cannot be fully recovered is malformed. Both
// states are valid blocking observations rather than reconcile errors.
func ObserveExisting(cellKey string, evidence SecretEvidence) (Observation, error) {
	identity, err := materialization.SecretIdentityForCell(cellKey)
	if err != nil {
		return Observation{}, ErrReconcile
	}
	if evidence.Labels[LabelManagedBy] == labelManagedByValue {
		if snapshot, recoverErr := RecoverCurrent(evidence); recoverErr == nil && snapshotMatches(snapshot, identity) {
			return ObserveManaged(snapshot)
		}
		return ObserveObstruction(cellKey, StateMalformed, safeOpaque(evidence.UID), safeOpaque(evidence.ResourceVersion))
	}
	return ObserveObstruction(cellKey, StateForeign, safeOpaque(evidence.UID), safeOpaque(evidence.ResourceVersion))
}

// ObserveAbsent records an authoritative not-found result for the exact cell
// target. It does not authorize creating that target.
func ObserveAbsent(cellKey string) (Observation, error) {
	identity, err := materialization.SecretIdentityForCell(cellKey)
	if err != nil {
		return Observation{}, ErrReconcile
	}
	observation := baseObservation(identity, StateAbsent)
	observation.Digest = DigestObservation(observation)
	return observation, ValidateObservation(observation)
}

// ObserveManaged records a fully sealed managed generation.
func ObserveManaged(snapshot Snapshot) (Observation, error) {
	if err := ValidateSnapshot(snapshot); err != nil {
		return Observation{}, ErrReconcile
	}
	identity := materialization.SecretIdentity{
		Namespace:  snapshot.Namespace,
		SecretName: snapshot.SecretName,
		CellKey:    snapshot.CellKey,
		CellID:     snapshot.CellID,
	}
	observation := baseObservation(identity, StateManaged)
	observation.UID = snapshot.UID
	observation.ResourceVersion = snapshot.ResourceVersion
	observation.SnapshotDigest = snapshot.Digest
	observation.snapshot = snapshot
	observation.Digest = DigestObservation(observation)
	return observation, ValidateObservation(observation)
}

// ObserveObstruction records an existing object that must never be adopted or
// overwritten. State must be foreign or malformed. UID/resourceVersion are
// optional because failure to decode either is itself valid blocking evidence.
func ObserveObstruction(cellKey string, state CurrentState, uid, resourceVersion string) (Observation, error) {
	if state != StateForeign && state != StateMalformed ||
		!validOptionalOpaque(uid) || !validOptionalOpaque(resourceVersion) {
		return Observation{}, ErrReconcile
	}
	identity, err := materialization.SecretIdentityForCell(cellKey)
	if err != nil {
		return Observation{}, ErrReconcile
	}
	observation := baseObservation(identity, state)
	observation.UID = uid
	observation.ResourceVersion = resourceVersion
	observation.Digest = DigestObservation(observation)
	return observation, ValidateObservation(observation)
}

// Decide evaluates one cell in isolation. A nil desired plan means the source
// is currently unavailable; it is never interpreted as an instruction to
// delete. Every returned decision remains non-executable.
func Decide(cellKey string, desired *materialization.Plan, current Observation, now time.Time) (Decision, error) {
	now = now.UTC().Truncate(time.Second)
	identity, err := materialization.SecretIdentityForCell(cellKey)
	if err != nil || now.IsZero() || ValidateObservation(current) != nil || !observationMatches(current, identity) {
		return Decision{}, ErrReconcile
	}
	if desired != nil {
		if materialization.Validate(*desired, now) != nil || !planMatches(*desired, identity) {
			return Decision{}, ErrReconcile
		}
	}
	decision := Decision{
		APIVersion:                ContractAPIVersion,
		Kind:                      DecisionKind,
		Policy:                    ContractPolicy,
		Namespace:                 identity.Namespace,
		SecretName:                identity.SecretName,
		CellKey:                   identity.CellKey,
		CellID:                    identity.CellID,
		CurrentUID:                current.UID,
		CurrentResourceVersion:    current.ResourceVersion,
		CurrentSnapshotDigest:     current.SnapshotDigest,
		DecidedAt:                 now,
		DeleteAllowed:             false,
		ExecutionAllowed:          false,
		ProductionMutationAllowed: false,
	}
	if desired != nil {
		decision.DesiredPlanDigest = desired.Digest
	}
	if current.State == StateManaged {
		decision.CurrentPlanDigest = current.snapshot.PlanDigest
		decision.LastKnownGoodExpiresAt = current.snapshot.ExpiresAt
	}

	switch current.State {
	case StateForeign:
		setBlocked(&decision, ReasonCurrentObjectForeign)
	case StateMalformed:
		setBlocked(&decision, ReasonCurrentObjectMalformed)
	case StateAbsent:
		if desired == nil {
			setBlocked(&decision, ReasonSourceUnavailableNoLKG)
		} else {
			decision.Action = ActionCreateIfAbsent
			decision.Reason = ReasonDesiredGenerationReady
			decision.MutationCandidate = true
			decision.RequireAbsent = true
		}
	case StateManaged:
		if desired != nil {
			if desired.Digest == current.snapshot.PlanDigest {
				decision.Action = ActionNoop
				decision.Reason = ReasonCurrentGenerationMatches
				decision.Stable = true
			} else {
				decision.Action = ActionReplaceResourceVersionCAS
				decision.Reason = ReasonDesiredGenerationChanged
				decision.MutationCandidate = true
				decision.ExpectedUID = current.UID
				decision.ExpectedResourceVersion = current.ResourceVersion
				decision.RequireUIDMatch = true
				decision.RequireResourceVersionCAS = true
				decision.RetainExisting = true
			}
		} else if materialization.ValidateLastKnownGood(current.snapshot.plan, now) == nil {
			decision.Action = ActionRetainLastKnownGood
			decision.Reason = ReasonSourceUnavailableRetainLKG
			decision.Stable = true
			decision.RetainExisting = true
		} else if !current.snapshot.ExpiresAt.After(now) {
			setBlocked(&decision, ReasonLastKnownGoodExpired)
			decision.RetainExisting = true
		} else {
			setBlocked(&decision, ReasonLastKnownGoodUnavailable)
			decision.RetainExisting = true
		}
	default:
		return Decision{}, ErrReconcile
	}
	decision.Digest = DigestDecision(decision)
	if err := ValidateDecision(decision); err != nil {
		return Decision{}, err
	}
	return decision, nil
}

func ValidateSnapshot(snapshot Snapshot) error {
	if materialization.ValidateSealed(snapshot.plan) != nil || !validOpaque(snapshot.UID) ||
		!validOpaque(snapshot.ResourceVersion) {
		return ErrReconcile
	}
	plan := snapshot.plan
	manifest := manifestForSealedPlan(plan)
	if snapshot.APIVersion != ContractAPIVersion || snapshot.Kind != SnapshotKind || snapshot.Policy != ContractPolicy ||
		snapshot.Namespace != plan.Namespace || snapshot.SecretName != plan.SecretName || snapshot.CellKey != plan.CellKey ||
		snapshot.CellID != plan.CellID || snapshot.SecretType != SecretTypeOpaque || snapshot.PlanDigest != plan.Digest ||
		snapshot.ManifestDigest != manifest.Digest || snapshot.BundleDigest != plan.BundleDigest ||
		snapshot.SpecDigest != plan.SpecDigest || snapshot.SpecDataDigest != plan.SpecDocumentDigest ||
		snapshot.TokenDataDigest != plan.ObserverTokenDigest || snapshot.IssuedAt != plan.IssuedAt ||
		snapshot.Immutable || snapshot.OwnerReferenceCount != 0 ||
		snapshot.RenewAfter != plan.RenewAfter || snapshot.ExpiresAt != plan.ExpiresAt ||
		snapshot.Digest != DigestSnapshot(snapshot) {
		return ErrReconcile
	}
	return nil
}

func ValidateObservation(observation Observation) error {
	identity, err := materialization.SecretIdentityForCell(observation.CellKey)
	if err != nil || observation.APIVersion != ContractAPIVersion || observation.Kind != ObservationKind ||
		observation.Policy != ContractPolicy || !observationMatches(observation, identity) ||
		observation.Digest != DigestObservation(observation) {
		return ErrReconcile
	}
	switch observation.State {
	case StateAbsent:
		if observation.UID != "" || observation.ResourceVersion != "" || observation.SnapshotDigest != "" ||
			observation.snapshot.Digest != "" {
			return ErrReconcile
		}
	case StateManaged:
		if ValidateSnapshot(observation.snapshot) != nil || observation.UID != observation.snapshot.UID ||
			observation.ResourceVersion != observation.snapshot.ResourceVersion ||
			observation.SnapshotDigest != observation.snapshot.Digest || !snapshotMatches(observation.snapshot, identity) {
			return ErrReconcile
		}
	case StateForeign, StateMalformed:
		if !validOptionalOpaque(observation.UID) || !validOptionalOpaque(observation.ResourceVersion) ||
			observation.SnapshotDigest != "" || observation.snapshot.Digest != "" {
			return ErrReconcile
		}
	default:
		return ErrReconcile
	}
	return nil
}

func ValidateDecision(decision Decision) error {
	identity, err := materialization.SecretIdentityForCell(decision.CellKey)
	if err != nil || decision.APIVersion != ContractAPIVersion || decision.Kind != DecisionKind ||
		decision.Policy != ContractPolicy || decision.Namespace != identity.Namespace ||
		decision.SecretName != identity.SecretName || decision.CellID != identity.CellID || decision.DecidedAt.IsZero() ||
		decision.DecidedAt.Location() != time.UTC || decision.DecidedAt.Nanosecond() != 0 ||
		decision.DeleteAllowed || decision.ExecutionAllowed || decision.ProductionMutationAllowed ||
		!validOptionalOpaque(decision.CurrentUID) || !validOptionalOpaque(decision.CurrentResourceVersion) ||
		!validOptionalOpaque(decision.ExpectedUID) || !validOptionalOpaque(decision.ExpectedResourceVersion) ||
		decision.Digest != DigestDecision(decision) {
		return ErrReconcile
	}
	if (decision.DesiredPlanDigest != "" && !validDigest(decision.DesiredPlanDigest)) ||
		(decision.CurrentPlanDigest != "" && !validDigest(decision.CurrentPlanDigest)) ||
		(decision.CurrentSnapshotDigest != "" && !validDigest(decision.CurrentSnapshotDigest)) ||
		(!decision.LastKnownGoodExpiresAt.IsZero() && !canonicalTime(decision.LastKnownGoodExpiresAt)) {
		return ErrReconcile
	}
	switch decision.Action {
	case ActionCreateIfAbsent:
		if decision.Reason != ReasonDesiredGenerationReady || decision.DesiredPlanDigest == "" ||
			decision.CurrentPlanDigest != "" || decision.CurrentSnapshotDigest != "" || decision.CurrentUID != "" ||
			decision.CurrentResourceVersion != "" || !decision.LastKnownGoodExpiresAt.IsZero() ||
			!decision.MutationCandidate || decision.Stable || decision.Blocked ||
			!decision.RequireAbsent || decision.RequireUIDMatch || decision.RequireResourceVersionCAS ||
			decision.RetainExisting || decision.ExpectedUID != "" || decision.ExpectedResourceVersion != "" {
			return ErrReconcile
		}
	case ActionNoop:
		if decision.Reason != ReasonCurrentGenerationMatches || decision.DesiredPlanDigest == "" ||
			decision.DesiredPlanDigest != decision.CurrentPlanDigest || decision.CurrentSnapshotDigest == "" ||
			decision.CurrentUID == "" || decision.CurrentResourceVersion == "" || decision.MutationCandidate ||
			!decision.Stable || decision.Blocked || decision.RequireAbsent || decision.RequireUIDMatch ||
			decision.RequireResourceVersionCAS || decision.RetainExisting || decision.ExpectedUID != "" ||
			decision.ExpectedResourceVersion != "" || !decision.LastKnownGoodExpiresAt.After(decision.DecidedAt) {
			return ErrReconcile
		}
	case ActionReplaceResourceVersionCAS:
		if decision.Reason != ReasonDesiredGenerationChanged || decision.DesiredPlanDigest == "" ||
			decision.CurrentPlanDigest == "" || decision.DesiredPlanDigest == decision.CurrentPlanDigest ||
			decision.CurrentSnapshotDigest == "" || decision.CurrentUID == "" || decision.CurrentResourceVersion == "" ||
			!decision.MutationCandidate || decision.Stable || decision.Blocked || decision.RequireAbsent ||
			!decision.RequireUIDMatch || !decision.RequireResourceVersionCAS || !decision.RetainExisting ||
			decision.ExpectedUID != decision.CurrentUID ||
			decision.ExpectedResourceVersion != decision.CurrentResourceVersion || decision.LastKnownGoodExpiresAt.IsZero() {
			return ErrReconcile
		}
	case ActionRetainLastKnownGood:
		if decision.Reason != ReasonSourceUnavailableRetainLKG || decision.DesiredPlanDigest != "" ||
			decision.CurrentPlanDigest == "" || decision.CurrentSnapshotDigest == "" || decision.CurrentUID == "" ||
			decision.CurrentResourceVersion == "" || decision.MutationCandidate || !decision.Stable || decision.Blocked ||
			decision.RequireAbsent || decision.RequireUIDMatch || decision.RequireResourceVersionCAS ||
			!decision.RetainExisting || decision.ExpectedUID != "" || decision.ExpectedResourceVersion != "" ||
			!decision.LastKnownGoodExpiresAt.After(decision.DecidedAt) {
			return ErrReconcile
		}
	case ActionBlock:
		if !validBlockReason(decision.Reason) || decision.MutationCandidate || decision.Stable || !decision.Blocked ||
			decision.RequireAbsent || decision.RequireUIDMatch || decision.RequireResourceVersionCAS ||
			decision.ExpectedUID != "" || decision.ExpectedResourceVersion != "" {
			return ErrReconcile
		}
		switch decision.Reason {
		case ReasonSourceUnavailableNoLKG:
			if decision.DesiredPlanDigest != "" || decision.CurrentPlanDigest != "" ||
				decision.CurrentSnapshotDigest != "" || decision.CurrentUID != "" ||
				decision.CurrentResourceVersion != "" || !decision.LastKnownGoodExpiresAt.IsZero() ||
				decision.RetainExisting {
				return ErrReconcile
			}
		case ReasonCurrentObjectForeign, ReasonCurrentObjectMalformed:
			if decision.CurrentPlanDigest != "" || decision.CurrentSnapshotDigest != "" ||
				!decision.LastKnownGoodExpiresAt.IsZero() || decision.RetainExisting {
				return ErrReconcile
			}
		case ReasonLastKnownGoodExpired:
			if decision.DesiredPlanDigest != "" || !decision.RetainExisting || decision.CurrentPlanDigest == "" ||
				decision.CurrentSnapshotDigest == "" || decision.CurrentUID == "" ||
				decision.CurrentResourceVersion == "" || decision.LastKnownGoodExpiresAt.IsZero() ||
				decision.LastKnownGoodExpiresAt.After(decision.DecidedAt) {
				return ErrReconcile
			}
		case ReasonLastKnownGoodUnavailable:
			if decision.DesiredPlanDigest != "" || !decision.RetainExisting || decision.CurrentPlanDigest == "" ||
				decision.CurrentSnapshotDigest == "" || decision.CurrentUID == "" ||
				decision.CurrentResourceVersion == "" || !decision.LastKnownGoodExpiresAt.After(decision.DecidedAt) {
				return ErrReconcile
			}
		}
	default:
		return ErrReconcile
	}
	return nil
}

func DigestManifest(manifest Manifest) string {
	manifest.Digest = ""
	return digestJSON(manifest)
}

func DigestSnapshot(snapshot Snapshot) string {
	snapshot.Digest = ""
	return digestJSON(snapshot)
}

func DigestObservation(observation Observation) string {
	observation.Digest = ""
	return digestJSON(observation)
}

func DigestDecision(decision Decision) string {
	decision.Digest = ""
	return digestJSON(decision)
}

func (manifest Manifest) String() string {
	return fmt.Sprintf("BackupObserverSecretManifest{namespace=%q secret=%q cell=%q plan=%q data=[DIGESTS_ONLY] executionAllowed=false digest=%q}", manifest.Namespace, manifest.SecretName, manifest.CellKey, manifest.PlanDigest, manifest.Digest)
}

func (manifest Manifest) GoString() string { return manifest.String() }

func (evidence SecretEvidence) String() string {
	return fmt.Sprintf("BackupObserverSecretEvidence{namespace=%q secret=%q uid=%q resourceVersion=%q immutable=%t deletionPending=%t ownerReferences=%d data=[REDACTED]}", evidence.Namespace, evidence.SecretName, evidence.UID, evidence.ResourceVersion, evidence.Immutable, evidence.DeletionPending, evidence.OwnerReferenceCount)
}

func (evidence SecretEvidence) GoString() string { return evidence.String() }

// MarshalJSON deliberately excludes all caller-supplied labels, annotations,
// data keys, and data values. SecretEvidence is an in-process validation input,
// not a wire representation for the private object.
func (evidence SecretEvidence) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Namespace           string `json:"namespace"`
		SecretName          string `json:"secretName"`
		UID                 string `json:"uid"`
		ResourceVersion     string `json:"resourceVersion"`
		SecretType          string `json:"secretType"`
		Immutable           bool   `json:"immutable"`
		DeletionPending     bool   `json:"deletionPending"`
		OwnerReferenceCount int    `json:"ownerReferenceCount"`
		Metadata            string `json:"metadata"`
		Data                string `json:"data"`
	}{
		Namespace:           evidence.Namespace,
		SecretName:          evidence.SecretName,
		UID:                 evidence.UID,
		ResourceVersion:     evidence.ResourceVersion,
		SecretType:          evidence.SecretType,
		Immutable:           evidence.Immutable,
		DeletionPending:     evidence.DeletionPending,
		OwnerReferenceCount: evidence.OwnerReferenceCount,
		Metadata:            "[OMITTED]",
		Data:                "[REDACTED]",
	})
}

func (snapshot Snapshot) String() string {
	return fmt.Sprintf("BackupObserverSecretSnapshot{namespace=%q secret=%q cell=%q uid=%q resourceVersion=%q plan=%q data=[DIGESTS_ONLY] digest=%q}", snapshot.Namespace, snapshot.SecretName, snapshot.CellKey, snapshot.UID, snapshot.ResourceVersion, snapshot.PlanDigest, snapshot.Digest)
}

func (snapshot Snapshot) GoString() string { return snapshot.String() }

func manifestForSealedPlan(plan materialization.Plan) Manifest {
	manifest := Manifest{
		APIVersion: ContractAPIVersion,
		Kind:       ManifestKind,
		Policy:     ContractPolicy,
		Namespace:  plan.Namespace,
		SecretName: plan.SecretName,
		CellKey:    plan.CellKey,
		CellID:     plan.CellID,
		SecretType: SecretTypeOpaque,
		Labels: map[string]string{
			LabelName:      labelNameValue,
			LabelComponent: labelComponentValue,
			LabelManagedBy: labelManagedByValue,
			LabelCellID:    plan.CellID,
		},
		Annotations: map[string]string{
			AnnotationPlanAPIVersion:      plan.APIVersion,
			AnnotationPlanPolicy:          plan.Policy,
			AnnotationPlanDigest:          plan.Digest,
			AnnotationCellKey:             plan.CellKey,
			AnnotationRunID:               plan.RunID,
			AnnotationSpecDigest:          plan.SpecDigest,
			AnnotationBundleDigest:        plan.BundleDigest,
			AnnotationCredentialID:        plan.CredentialID,
			AnnotationTokenID:             plan.TokenID,
			AnnotationSpecDocumentDigest:  plan.SpecDocumentDigest,
			AnnotationObserverTokenDigest: plan.ObserverTokenDigest,
			AnnotationIssuedAt:            plan.IssuedAt.Format(time.RFC3339),
			AnnotationRenewAfter:          plan.RenewAfter.Format(time.RFC3339),
			AnnotationExpiresAt:           plan.ExpiresAt.Format(time.RFC3339),
		},
		DataDigests: map[string]string{
			plan.SpecKey:  plan.SpecDocumentDigest,
			plan.TokenKey: plan.ObserverTokenDigest,
		},
		PlanDigest:             plan.Digest,
		Immutable:              false,
		OwnerReferencesAllowed: false,
	}
	manifest.Digest = DigestManifest(manifest)
	return manifest
}

func baseObservation(identity materialization.SecretIdentity, state CurrentState) Observation {
	return Observation{
		APIVersion: ContractAPIVersion,
		Kind:       ObservationKind,
		Policy:     ContractPolicy,
		State:      state,
		Namespace:  identity.Namespace,
		SecretName: identity.SecretName,
		CellKey:    identity.CellKey,
		CellID:     identity.CellID,
	}
}

func setBlocked(decision *Decision, reason Reason) {
	decision.Action = ActionBlock
	decision.Reason = reason
	decision.Blocked = true
}

func containsRequired(actual, required map[string]string) bool {
	if len(actual) < len(required) {
		return false
	}
	for key, value := range required {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func observationMatches(observation Observation, identity materialization.SecretIdentity) bool {
	return observation.Namespace == identity.Namespace && observation.SecretName == identity.SecretName &&
		observation.CellKey == identity.CellKey && observation.CellID == identity.CellID
}

func snapshotMatches(snapshot Snapshot, identity materialization.SecretIdentity) bool {
	return snapshot.Namespace == identity.Namespace && snapshot.SecretName == identity.SecretName &&
		snapshot.CellKey == identity.CellKey && snapshot.CellID == identity.CellID
}

func planMatches(plan materialization.Plan, identity materialization.SecretIdentity) bool {
	return plan.Namespace == identity.Namespace && plan.SecretName == identity.SecretName &&
		plan.CellKey == identity.CellKey && plan.CellID == identity.CellID
}

func validBlockReason(reason Reason) bool {
	switch reason {
	case ReasonSourceUnavailableNoLKG, ReasonLastKnownGoodExpired, ReasonLastKnownGoodUnavailable,
		ReasonCurrentObjectForeign, ReasonCurrentObjectMalformed:
		return true
	default:
		return false
	}
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && strings.ToLower(value) == value
}

func validOptionalOpaque(value string) bool {
	return value == "" || validOpaque(value)
}

func validOpaque(value string) bool {
	if value == "" || len(value) > maxOpaqueBytes || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func canonicalTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Nanosecond() == 0
}

func parseCanonicalTimestamp(value string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339, value)
	return parsed, err == nil && canonicalTime(parsed) && parsed.Format(time.RFC3339) == value
}

func safeOpaque(value string) string {
	if validOpaque(value) {
		return value
	}
	return ""
}

func digestJSON(value any) string {
	document, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return digestBytes(document)
}

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}
