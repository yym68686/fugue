package releaseguardian

import (
	"errors"
	"time"

	"fugue/internal/declarativerelease"
)

const (
	RouteBundleRecordKind        = "RouteBundleRecord"
	CandidateAuthorityKind       = "CandidateAuthority"
	CurrentAuthorityKind         = "CurrentAuthority"
	CandidateCanaryResultKind    = "CandidateCanaryResult"
	GroupQualificationRecordKind = "GroupQualificationRecord"
)

type AuthoritySlot string

const (
	AuthoritySlotA AuthoritySlot = "a"
	AuthoritySlotB AuthoritySlot = "b"
)

func (slot AuthoritySlot) Validate() error {
	if slot != AuthoritySlotA && slot != AuthoritySlotB {
		return errors.New("authority worker slot is invalid")
	}
	return nil
}

// RouteBundleRecord is an immutable, signed description of one candidate
// route fact. It deliberately does not grant traffic authority.
type RouteBundleRecord struct {
	APIVersion           string `json:"apiVersion"`
	Kind                 string `json:"kind"`
	GroupID              string `json:"groupId"`
	Epoch                int64  `json:"epoch"`
	BundleDigest         string `json:"bundleDigest"`
	SourceSHA            string `json:"sourceSha"`
	ControlImageDigest   string `json:"controlImageDigest"`
	InventoryDigest      string `json:"inventoryDigest"`
	ManifestDigest       string `json:"manifestDigest"`
	HealthContractDigest string `json:"healthContractDigest"`
	IssuedAt             string `json:"issuedAt"`
	KeyID                string `json:"keyId"`
	Signature            string `json:"signature"`
	RecordDigest         string `json:"recordDigest"`
}

func (record RouteBundleRecord) Seal() (RouteBundleRecord, error) {
	record.APIVersion = APIVersion
	record.Kind = RouteBundleRecordKind
	record.RecordDigest = ""
	if err := record.validateUnsigned(); err != nil {
		return RouteBundleRecord{}, err
	}
	raw, err := declarativerelease.CanonicalJSON(record)
	if err != nil {
		return RouteBundleRecord{}, err
	}
	record.RecordDigest = digest(raw)
	return record, nil
}

func (record RouteBundleRecord) Validate() error {
	if !digestPattern.MatchString(record.RecordDigest) || record.validateUnsigned() != nil {
		return errors.New("route bundle record is invalid")
	}
	copy := record
	copy.RecordDigest = ""
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != record.RecordDigest {
		return errors.New("route bundle record digest is invalid")
	}
	return nil
}

func (record RouteBundleRecord) validateUnsigned() error {
	if record.APIVersion != APIVersion || record.Kind != RouteBundleRecordKind ||
		!groupPattern.MatchString(record.GroupID) || record.Epoch < 1 ||
		!digestPattern.MatchString(record.BundleDigest) || !shaPattern.MatchString(record.SourceSHA) ||
		!digestPattern.MatchString(record.ControlImageDigest) || !digestPattern.MatchString(record.InventoryDigest) ||
		!digestPattern.MatchString(record.ManifestDigest) || !digestPattern.MatchString(record.HealthContractDigest) ||
		!componentPattern.MatchString(record.KeyID) || !signaturePattern.MatchString(record.Signature) {
		return errors.New("route bundle record identity is invalid")
	}
	issuedAt, err := time.Parse(time.RFC3339Nano, record.IssuedAt)
	if err != nil || !issuedAt.Equal(issuedAt.UTC()) {
		return errors.New("route bundle record issuance time is invalid")
	}
	return nil
}

type CandidateAuthorityState string

const (
	CandidateAuthorityLoaded   CandidateAuthorityState = "loaded"
	CandidateAuthorityVerified CandidateAuthorityState = "verified"
	CandidateAuthorityRejected CandidateAuthorityState = "rejected"
)

// CandidateAuthority is the group-local mutable candidate pointer. Kubernetes
// UID/ResourceVersion is its external CAS token; Generation orders state
// transitions without granting user traffic.
type CandidateAuthority struct {
	APIVersion          string                  `json:"apiVersion"`
	Kind                string                  `json:"kind"`
	GroupID             string                  `json:"groupId"`
	RecordDigest        string                  `json:"recordDigest"`
	WorkerSlot          AuthoritySlot           `json:"workerSlot"`
	ReleaseRecordDigest string                  `json:"releaseRecordDigest"`
	State               CandidateAuthorityState `json:"state"`
	Generation          int64                   `json:"generation"`
}

func (candidate CandidateAuthority) Validate() error {
	if candidate.APIVersion != APIVersion || candidate.Kind != CandidateAuthorityKind ||
		!groupPattern.MatchString(candidate.GroupID) || !digestPattern.MatchString(candidate.RecordDigest) ||
		candidate.WorkerSlot.Validate() != nil || !digestPattern.MatchString(candidate.ReleaseRecordDigest) ||
		(candidate.State != CandidateAuthorityLoaded && candidate.State != CandidateAuthorityVerified && candidate.State != CandidateAuthorityRejected) ||
		candidate.Generation < 1 {
		return errors.New("candidate authority is invalid")
	}
	return nil
}

// CurrentAuthority is the only pointer that grants ordinary user traffic for
// a group. The previous fields are the immediately reversible LKG authority.
type CurrentAuthority struct {
	APIVersion           string        `json:"apiVersion"`
	Kind                 string        `json:"kind"`
	GroupID              string        `json:"groupId"`
	CurrentRecordDigest  string        `json:"currentRecordDigest"`
	CurrentWorkerSlot    AuthoritySlot `json:"currentWorkerSlot"`
	PreviousRecordDigest string        `json:"previousRecordDigest,omitempty"`
	PreviousWorkerSlot   AuthoritySlot `json:"previousWorkerSlot,omitempty"`
	AuthorityEpoch       int64         `json:"authorityEpoch"`
}

func (authority CurrentAuthority) Validate() error {
	if authority.APIVersion != APIVersion || authority.Kind != CurrentAuthorityKind ||
		!groupPattern.MatchString(authority.GroupID) || !digestPattern.MatchString(authority.CurrentRecordDigest) ||
		authority.CurrentWorkerSlot.Validate() != nil || authority.AuthorityEpoch < 1 {
		return errors.New("current authority is invalid")
	}
	if authority.PreviousRecordDigest == "" && authority.PreviousWorkerSlot == "" {
		return nil
	}
	if !digestPattern.MatchString(authority.PreviousRecordDigest) || authority.PreviousWorkerSlot.Validate() != nil ||
		authority.PreviousRecordDigest == authority.CurrentRecordDigest || authority.PreviousWorkerSlot == authority.CurrentWorkerSlot {
		return errors.New("current authority LKG is invalid")
	}
	return nil
}

// CandidateCanaryResult is immutable and candidate-bound. A canary result for
// one record, slot, release, or group cannot authorize another candidate.
type CandidateCanaryResult struct {
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

func (result CandidateCanaryResult) Seal() (CandidateCanaryResult, error) {
	result.APIVersion = APIVersion
	result.Kind = CandidateCanaryResultKind
	result.ResultDigest = ""
	if err := result.validateUnsigned(time.Time{}); err != nil {
		return CandidateCanaryResult{}, err
	}
	raw, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		return CandidateCanaryResult{}, err
	}
	result.ResultDigest = digest(raw)
	return result, nil
}

func (result CandidateCanaryResult) Validate(now time.Time) error {
	if !digestPattern.MatchString(result.ResultDigest) || result.validateUnsigned(now) != nil {
		return errors.New("candidate canary result is invalid")
	}
	copy := result
	copy.ResultDigest = ""
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != result.ResultDigest {
		return errors.New("candidate canary result digest is invalid")
	}
	return nil
}

func (result CandidateCanaryResult) validateUnsigned(now time.Time) error {
	if result.APIVersion != APIVersion || result.Kind != CandidateCanaryResultKind ||
		!groupPattern.MatchString(result.GroupID) || !digestPattern.MatchString(result.CandidateRecordDigest) ||
		result.WorkerSlot.Validate() != nil || !digestPattern.MatchString(result.ReleaseRecordDigest) ||
		(result.RouteState != HealthHealthy && result.RouteState != HealthDegraded) ||
		(result.DependencyState != HealthHealthy && result.DependencyState != HealthDegraded) ||
		!digestPattern.MatchString(result.EvidenceDigest) || !componentPattern.MatchString(result.KeyID) ||
		!signaturePattern.MatchString(result.Signature) {
		return errors.New("candidate canary result identity is invalid")
	}
	observedAt, observedErr := time.Parse(time.RFC3339Nano, result.ObservedAt)
	expiresAt, expiresErr := time.Parse(time.RFC3339Nano, result.ExpiresAt)
	if observedErr != nil || expiresErr != nil || !observedAt.Equal(observedAt.UTC()) ||
		!expiresAt.Equal(expiresAt.UTC()) || !expiresAt.After(observedAt) {
		return errors.New("candidate canary result freshness is invalid")
	}
	if !now.IsZero() && now.UTC().After(expiresAt) {
		return errors.New("candidate canary result is expired")
	}
	return nil
}
