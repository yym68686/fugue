package releaseguardian

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
)

const (
	APIVersion = "release.fugue.dev/v1"

	ReleaseRecordKind  = "ReleaseRecord"
	DesiredReleaseKind = "DesiredRelease"
	ReleaseStatusKind  = "ReleaseStatus"
	CanaryResultKind   = "CanaryResult"
)

var (
	componentPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
	groupPattern     = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
	shaPattern       = regexp.MustCompile(`^[0-9a-f]{40}$`)
	digestPattern    = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type Key struct {
	Component string `json:"component"`
	Group     string `json:"group,omitempty"`
}

func (key Key) Validate() error {
	if !componentPattern.MatchString(key.Component) {
		return errors.New("release guardian component identity is invalid")
	}
	if key.Group != "" && !groupPattern.MatchString(key.Group) {
		return errors.New("release guardian group identity is invalid")
	}
	return nil
}

func (key Key) String() string {
	if key.Group == "" {
		return key.Component
	}
	return key.Component + "/" + key.Group
}

// ReleaseRecord is the immutable pre-rollout binding consumed by the
// Guardian. Unlike the legacy post-success monitor record, it exists before a
// production mutation and binds the exact target, LKG, and health contract.
type ReleaseRecord struct {
	APIVersion           string `json:"apiVersion"`
	Kind                 string `json:"kind"`
	Component            string `json:"component"`
	Group                string `json:"group,omitempty"`
	ConfigSHA            string `json:"configSha"`
	ImageDigest          string `json:"imageDigest"`
	ManifestDigest       string `json:"manifestDigest"`
	LKGRecordDigest      string `json:"lkgRecordDigest"`
	HealthContractDigest string `json:"healthContractDigest"`
	RecordDigest         string `json:"recordDigest"`
}

func NewReleaseRecord(key Key, configSHA, imageDigest, manifestDigest, lkgRecordDigest, healthContractDigest string) (ReleaseRecord, error) {
	record := ReleaseRecord{
		APIVersion: APIVersion, Kind: ReleaseRecordKind,
		Component: key.Component, Group: key.Group, ConfigSHA: configSHA,
		ImageDigest: imageDigest, ManifestDigest: manifestDigest,
		LKGRecordDigest: lkgRecordDigest, HealthContractDigest: healthContractDigest,
	}
	if err := record.validateUnsigned(); err != nil {
		return ReleaseRecord{}, err
	}
	raw, err := declarativerelease.CanonicalJSON(record)
	if err != nil {
		return ReleaseRecord{}, err
	}
	record.RecordDigest = digest(raw)
	return record, nil
}

func (record ReleaseRecord) Key() Key { return Key{Component: record.Component, Group: record.Group} }

func (record ReleaseRecord) Validate() error {
	if err := record.validateUnsigned(); err != nil || !digestPattern.MatchString(record.RecordDigest) {
		return errors.New("release record identity is invalid")
	}
	copy := record
	copy.RecordDigest = ""
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != record.RecordDigest {
		return errors.New("release record digest is invalid")
	}
	return nil
}

func (record ReleaseRecord) validateUnsigned() error {
	if record.APIVersion != APIVersion || record.Kind != ReleaseRecordKind || record.Key().Validate() != nil ||
		!shaPattern.MatchString(record.ConfigSHA) || !digestPattern.MatchString(record.ImageDigest) ||
		!digestPattern.MatchString(record.ManifestDigest) || !digestPattern.MatchString(record.LKGRecordDigest) ||
		!digestPattern.MatchString(record.HealthContractDigest) {
		return errors.New("release record identity is invalid")
	}
	return nil
}

// DesiredRelease is the only mutable release pointer. Its Kubernetes
// ConfigMap ResourceVersion is the CAS authority; this payload deliberately
// carries no independent mutable token.
type DesiredRelease struct {
	APIVersion   string `json:"apiVersion"`
	Kind         string `json:"kind"`
	Component    string `json:"component"`
	Group        string `json:"group,omitempty"`
	RecordDigest string `json:"recordDigest"`
	Generation   int64  `json:"generation"`
}

func (desired DesiredRelease) Key() Key {
	return Key{Component: desired.Component, Group: desired.Group}
}

func (desired DesiredRelease) Validate() error {
	if desired.APIVersion != APIVersion || desired.Kind != DesiredReleaseKind || desired.Key().Validate() != nil ||
		!digestPattern.MatchString(desired.RecordDigest) || desired.Generation < 1 {
		return errors.New("desired release is invalid")
	}
	return nil
}

type HealthState string

const (
	HealthHealthy  HealthState = "healthy"
	HealthDegraded HealthState = "degraded"
	HealthUnknown  HealthState = "unknown"
)

type LayerHealth struct {
	State          HealthState `json:"state"`
	Reason         string      `json:"reason,omitempty"`
	EvidenceDigest string      `json:"evidenceDigest,omitempty"`
	ObservedAt     string      `json:"observedAt"`
}

func (health LayerHealth) Validate(now time.Time) error {
	if health.State != HealthHealthy && health.State != HealthDegraded && health.State != HealthUnknown {
		return errors.New("health state is invalid")
	}
	observed, err := time.Parse(time.RFC3339Nano, health.ObservedAt)
	if err != nil || observed.After(now.UTC().Add(30*time.Second)) {
		return errors.New("health observation time is invalid")
	}
	if health.EvidenceDigest != "" && !digestPattern.MatchString(health.EvidenceDigest) {
		return errors.New("health evidence digest is invalid")
	}
	if len(health.Reason) > 512 || strings.ContainsAny(health.Reason, "\r\n\x00") {
		return errors.New("health reason is invalid")
	}
	return nil
}

type HealthSnapshot struct {
	Local      LayerHealth `json:"local"`
	Dependency LayerHealth `json:"dependency"`
	Route      LayerHealth `json:"route"`
}

func (snapshot HealthSnapshot) Validate(now time.Time) error {
	if err := snapshot.Local.Validate(now); err != nil {
		return fmt.Errorf("local health: %w", err)
	}
	if err := snapshot.Dependency.Validate(now); err != nil {
		return fmt.Errorf("dependency health: %w", err)
	}
	if err := snapshot.Route.Validate(now); err != nil {
		return fmt.Errorf("route health: %w", err)
	}
	return nil
}

type CanaryResult struct {
	APIVersion     string      `json:"apiVersion"`
	Kind           string      `json:"kind"`
	Component      string      `json:"component"`
	Group          string      `json:"group,omitempty"`
	RecordDigest   string      `json:"recordDigest"`
	State          HealthState `json:"state"`
	EvidenceDigest string      `json:"evidenceDigest"`
	ObservedAt     string      `json:"observedAt"`
	ExpiresAt      string      `json:"expiresAt"`
	ResultDigest   string      `json:"resultDigest"`
}

func (result CanaryResult) Key() Key { return Key{Component: result.Component, Group: result.Group} }

func (result CanaryResult) Validate(now time.Time) error {
	if result.APIVersion != APIVersion || result.Kind != CanaryResultKind || result.Key().Validate() != nil ||
		!digestPattern.MatchString(result.RecordDigest) || !digestPattern.MatchString(result.EvidenceDigest) ||
		!digestPattern.MatchString(result.ResultDigest) || (result.State != HealthHealthy && result.State != HealthDegraded) {
		return errors.New("canary result identity is invalid")
	}
	observed, observedErr := time.Parse(time.RFC3339Nano, result.ObservedAt)
	expires, expiresErr := time.Parse(time.RFC3339Nano, result.ExpiresAt)
	if observedErr != nil || expiresErr != nil || !expires.After(observed) || now.UTC().After(expires) {
		return errors.New("canary result freshness is invalid")
	}
	copy := result
	copy.ResultDigest = ""
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != result.ResultDigest {
		return errors.New("canary result digest is invalid")
	}
	return nil
}

type ReleaseState string

const (
	StateStable           ReleaseState = "stable"
	StateRolloutPending   ReleaseState = "rollout_pending"
	StateRolling          ReleaseState = "rolling"
	StateVerifying        ReleaseState = "verifying"
	StateDegraded         ReleaseState = "degraded"
	StateRollbackPending  ReleaseState = "rollback_pending"
	StateRollingBack      ReleaseState = "rolling_back"
	StateLKGStable        ReleaseState = "lkg_stable"
	StateRecoveryRequired ReleaseState = "recovery_required"
)

type ReleaseStatus struct {
	APIVersion            string         `json:"apiVersion"`
	Kind                  string         `json:"kind"`
	Component             string         `json:"component"`
	Group                 string         `json:"group,omitempty"`
	State                 ReleaseState   `json:"state"`
	CurrentRecordDigest   string         `json:"currentRecordDigest,omitempty"`
	TargetRecordDigest    string         `json:"targetRecordDigest"`
	LastSuccessfulLKG     string         `json:"lastSuccessfulLkg"`
	Health                HealthSnapshot `json:"health"`
	Reason                string         `json:"reason,omitempty"`
	RolloutReceiptDigest  string         `json:"rolloutReceiptDigest,omitempty"`
	RollbackReceiptDigest string         `json:"rollbackReceiptDigest,omitempty"`
	ObservedAt            string         `json:"observedAt"`
	StatusDigest          string         `json:"statusDigest"`
}

func (status ReleaseStatus) Key() Key { return Key{Component: status.Component, Group: status.Group} }

func (status ReleaseStatus) Seal() (ReleaseStatus, error) {
	status.APIVersion = APIVersion
	status.Kind = ReleaseStatusKind
	status.StatusDigest = ""
	if err := status.validateUnsigned(); err != nil {
		return ReleaseStatus{}, err
	}
	raw, err := declarativerelease.CanonicalJSON(status)
	if err != nil {
		return ReleaseStatus{}, err
	}
	status.StatusDigest = digest(raw)
	return status, nil
}

func (status ReleaseStatus) Validate(now time.Time) error {
	if status.APIVersion != APIVersion || status.Kind != ReleaseStatusKind || !digestPattern.MatchString(status.StatusDigest) {
		return errors.New("release status identity is invalid")
	}
	if err := status.validateUnsigned(); err != nil || status.Health.Validate(now) != nil {
		return errors.New("release status is invalid")
	}
	copy := status
	copy.StatusDigest = ""
	raw, err := declarativerelease.CanonicalJSON(copy)
	if err != nil || digest(raw) != status.StatusDigest {
		return errors.New("release status digest is invalid")
	}
	return nil
}

func (status ReleaseStatus) validateUnsigned() error {
	if status.Key().Validate() != nil || !validReleaseState(status.State) ||
		!digestPattern.MatchString(status.TargetRecordDigest) || !digestPattern.MatchString(status.LastSuccessfulLKG) ||
		(status.CurrentRecordDigest != "" && !digestPattern.MatchString(status.CurrentRecordDigest)) ||
		(status.RolloutReceiptDigest != "" && !digestPattern.MatchString(status.RolloutReceiptDigest)) ||
		(status.RollbackReceiptDigest != "" && !digestPattern.MatchString(status.RollbackReceiptDigest)) || status.ObservedAt == "" ||
		len(status.Reason) > 512 || strings.ContainsAny(status.Reason, "\r\n\x00") {
		return errors.New("release status is invalid")
	}
	if _, err := time.Parse(time.RFC3339Nano, status.ObservedAt); err != nil {
		return errors.New("release status time is invalid")
	}
	return nil
}

func validReleaseState(state ReleaseState) bool {
	switch state {
	case StateStable, StateRolloutPending, StateRolling, StateVerifying, StateDegraded,
		StateRollbackPending, StateRollingBack, StateLKGStable, StateRecoveryRequired:
		return true
	default:
		return false
	}
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum)
}
