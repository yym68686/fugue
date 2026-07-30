// Package materialization defines the pure, non-executable desired plan for
// projecting one validated observer input bundle into one cell-local Secret.
// It owns no filesystem, network, Kubernetes, datastore, signer, or process
// capability; a later writer must cross a separately reviewed execution gate.
package materialization

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"fugue/internal/backupcontrol"
	materializercontract "fugue/internal/backupmaterializer/contract"
)

const (
	PlanAPIVersion = "backup-materialization.fugue.dev/v1"
	PlanKind       = "BackupObserverSecretPlan"
	PlanPolicy     = "cell-local-single-secret-cas-shadow-v1"

	SecretNamespace = "fugue-system"
	SpecDataKey     = "spec.json"
	TokenDataKey    = "token"

	maxSpecDocumentBytes  = 32 << 10
	maxObserverTokenBytes = 4096
)

var (
	ErrPlan = errors.New("invalid backup observer Secret materialization plan")

	canonicalCellKey = regexp.MustCompile(`^backup/(control-plane-db|app-database|persistent-storage|data-workspace|registry|platform-component)/[0-9a-f]{16}$`)
	canonicalDigest  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	canonicalName    = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

// Plan binds the private data generation to its exact future Kubernetes
// identity while remaining permanently non-executable in v1. The raw spec and
// token are unexported; their public digests participate in the plan digest.
type Plan struct {
	APIVersion                string                      `json:"apiVersion"`
	Kind                      string                      `json:"kind"`
	Policy                    string                      `json:"policy"`
	Namespace                 string                      `json:"namespace"`
	SecretName                string                      `json:"secretName"`
	CellKey                   string                      `json:"cellKey"`
	CellID                    string                      `json:"cellId"`
	RunID                     string                      `json:"runId"`
	SpecDigest                string                      `json:"specDigest"`
	BundleDigest              string                      `json:"bundleDigest"`
	CredentialID              string                      `json:"credentialId"`
	TokenID                   string                      `json:"tokenId"`
	DesiredSpec               backupcontrol.BackupRunSpec `json:"desiredSpec"`
	SpecKey                   string                      `json:"specKey"`
	TokenKey                  string                      `json:"tokenKey"`
	SpecDocumentDigest        string                      `json:"specDocumentDigest"`
	ObserverTokenDigest       string                      `json:"observerTokenDigest"`
	IssuedAt                  time.Time                   `json:"issuedAt"`
	RenewAfter                time.Time                   `json:"renewAfter"`
	ExpiresAt                 time.Time                   `json:"expiresAt"`
	IdempotencyKey            string                      `json:"idempotencyKey"`
	RetainExistingOnFailure   bool                        `json:"retainExistingOnFailure"`
	RequireResourceVersionCAS bool                        `json:"requireResourceVersionCas"`
	LastKnownGoodRequired     bool                        `json:"lastKnownGoodRequired"`
	ObservationOnly           bool                        `json:"observationOnly"`
	ExecutionAllowed          bool                        `json:"executionAllowed"`
	ProductionMutationAllowed bool                        `json:"productionMutationAllowed"`
	Digest                    string                      `json:"digest"`

	specDocument  []byte
	observerToken string
}

// SecretData is the explicit private handoff to a future fixed-purpose writer.
// Formatting always redacts both values, and Data returns fresh copies.
type SecretData struct {
	SpecKey       string
	SpecDocument  []byte
	TokenKey      string
	ObserverToken []byte
}

// Build validates the authenticated bundle envelope and deterministically
// seals its two data items into a non-executable cell-local plan.
func Build(bundle materializercontract.ObserverInputBundle, now time.Time) (Plan, error) {
	now = now.UTC().Truncate(time.Second)
	if err := materializercontract.ValidateObserverInputBundleEnvelope(bundle, now); err != nil ||
		bundle.IssuedAt.Before(now.Add(-materializercontract.MaxObserverInputDeliveryAge)) ||
		!bundle.RenewAfter.After(now) {
		return Plan{}, ErrPlan
	}
	specDocument, err := json.Marshal(bundle.DesiredSpec)
	if err != nil || len(specDocument) == 0 || len(specDocument) > maxSpecDocumentBytes {
		return Plan{}, ErrPlan
	}
	decodedSpec, err := backupcontrol.DecodeBackupRunSpec(specDocument)
	if err != nil || decodedSpec != bundle.DesiredSpec {
		return Plan{}, ErrPlan
	}
	cellID := cellIDForKey(bundle.CellKey)
	secretName := secretNameForCell(bundle.CellKey)
	if cellID == "" || !canonicalName.MatchString(secretName) {
		return Plan{}, ErrPlan
	}
	plan := Plan{
		APIVersion:                PlanAPIVersion,
		Kind:                      PlanKind,
		Policy:                    PlanPolicy,
		Namespace:                 SecretNamespace,
		SecretName:                secretName,
		CellKey:                   bundle.CellKey,
		CellID:                    cellID,
		RunID:                     bundle.RunID,
		SpecDigest:                bundle.SpecDigest,
		BundleDigest:              bundle.Digest,
		CredentialID:              bundle.CredentialID,
		TokenID:                   bundle.TokenID,
		DesiredSpec:               bundle.DesiredSpec,
		SpecKey:                   SpecDataKey,
		TokenKey:                  TokenDataKey,
		SpecDocumentDigest:        digestBytes(specDocument),
		ObserverTokenDigest:       digestBytes([]byte(bundle.ObserverToken)),
		IssuedAt:                  bundle.IssuedAt,
		RenewAfter:                bundle.RenewAfter,
		ExpiresAt:                 bundle.ExpiresAt,
		IdempotencyKey:            "backup-observer-input/" + cellID + "/" + strings.TrimPrefix(bundle.Digest, "sha256:"),
		RetainExistingOnFailure:   true,
		RequireResourceVersionCAS: true,
		LastKnownGoodRequired:     true,
		ObservationOnly:           true,
		ExecutionAllowed:          false,
		ProductionMutationAllowed: false,
		specDocument:              append([]byte(nil), specDocument...),
		observerToken:             bundle.ObserverToken,
	}
	plan.Digest = DigestPlan(plan)
	if err := Validate(plan, now); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

// Validate reconstructs and validates the source bundle, raw data digests,
// deterministic object identity, non-executable policy, and plan digest.
func Validate(plan Plan, now time.Time) error {
	now = now.UTC().Truncate(time.Second)
	if now.IsZero() || plan.APIVersion != PlanAPIVersion || plan.Kind != PlanKind || plan.Policy != PlanPolicy ||
		plan.Namespace != SecretNamespace || !canonicalCellKey.MatchString(plan.CellKey) ||
		plan.CellID != cellIDForKey(plan.CellKey) || plan.SecretName != secretNameForCell(plan.CellKey) ||
		!canonicalName.MatchString(plan.SecretName) || backupcontrol.ValidateBackupRunSpec(plan.DesiredSpec) != nil ||
		plan.RunID != plan.DesiredSpec.RunID || plan.CellKey != plan.DesiredSpec.CellKey ||
		plan.SpecDigest != plan.DesiredSpec.Digest || !canonicalDigest.MatchString(plan.BundleDigest) ||
		plan.SpecKey != SpecDataKey || plan.TokenKey != TokenDataKey ||
		len(plan.specDocument) == 0 || len(plan.specDocument) > maxSpecDocumentBytes ||
		plan.observerToken == "" || len(plan.observerToken) > maxObserverTokenBytes ||
		plan.SpecDocumentDigest != digestBytes(plan.specDocument) ||
		plan.ObserverTokenDigest != digestBytes([]byte(plan.observerToken)) ||
		plan.IdempotencyKey != "backup-observer-input/"+plan.CellID+"/"+strings.TrimPrefix(plan.BundleDigest, "sha256:") ||
		!plan.RetainExistingOnFailure || !plan.RequireResourceVersionCAS || !plan.LastKnownGoodRequired ||
		!plan.ObservationOnly || plan.ExecutionAllowed || plan.ProductionMutationAllowed ||
		plan.Digest != DigestPlan(plan) {
		return ErrPlan
	}
	decodedSpec, err := backupcontrol.DecodeBackupRunSpec(plan.specDocument)
	if err != nil || decodedSpec != plan.DesiredSpec {
		return ErrPlan
	}
	bundle := materializercontract.ObserverInputBundle{
		APIVersion:                materializercontract.ObserverInputBundleAPIVersion,
		Kind:                      materializercontract.ObserverInputBundleKind,
		Policy:                    materializercontract.ObserverInputBundlePolicy,
		CellKey:                   plan.CellKey,
		RunID:                     plan.RunID,
		SpecDigest:                plan.SpecDigest,
		CredentialID:              plan.CredentialID,
		TokenID:                   plan.TokenID,
		DesiredSpec:               plan.DesiredSpec,
		ObserverToken:             plan.observerToken,
		IssuedAt:                  plan.IssuedAt,
		RenewAfter:                plan.RenewAfter,
		ExpiresAt:                 plan.ExpiresAt,
		ObservationOnly:           true,
		ProductionMutationAllowed: false,
		Digest:                    plan.BundleDigest,
	}
	if materializercontract.ValidateObserverInputBundleEnvelope(bundle, now) != nil ||
		plan.IssuedAt.Before(now.Add(-materializercontract.MaxObserverInputDeliveryAge)) || !plan.RenewAfter.After(now) {
		return ErrPlan
	}
	return nil
}

func DigestPlan(plan Plan) string {
	plan.Digest = ""
	document, err := json.Marshal(plan)
	if err != nil {
		return ""
	}
	return digestBytes(document)
}

// Data returns independent copies only for a currently valid plan. It does not
// authorize applying them to any external system.
func (plan Plan) Data(now time.Time) (SecretData, error) {
	if err := Validate(plan, now); err != nil {
		return SecretData{}, err
	}
	return SecretData{
		SpecKey:       plan.SpecKey,
		SpecDocument:  append([]byte(nil), plan.specDocument...),
		TokenKey:      plan.TokenKey,
		ObserverToken: append([]byte(nil), plan.observerToken...),
	}, nil
}

func (plan Plan) String() string {
	return fmt.Sprintf(
		"BackupObserverSecretPlan{namespace=%q secret=%q cell=%q run=%q bundle=%q tokenID=%q specDocument=[REDACTED] observerToken=[REDACTED] executionAllowed=false digest=%q}",
		plan.Namespace,
		plan.SecretName,
		plan.CellKey,
		plan.RunID,
		plan.BundleDigest,
		plan.TokenID,
		plan.Digest,
	)
}

func (plan Plan) GoString() string {
	return plan.String()
}

func (data SecretData) String() string {
	return fmt.Sprintf(
		"BackupObserverSecretData{specKey=%q specDocument=[REDACTED] tokenKey=%q observerToken=[REDACTED]}",
		data.SpecKey,
		data.TokenKey,
	)
}

func (data SecretData) GoString() string {
	return data.String()
}

func secretNameForCell(cellKey string) string {
	cellID := cellIDForKey(cellKey)
	if cellID == "" {
		return ""
	}
	return "fugue-backup-observer-" + cellID + "-input"
}

func cellIDForKey(cellKey string) string {
	if !canonicalCellKey.MatchString(cellKey) {
		return ""
	}
	return strings.ReplaceAll(strings.TrimPrefix(cellKey, "backup/"), "/", "-")
}

func digestBytes(document []byte) string {
	digest := sha256.Sum256(document)
	return "sha256:" + hex.EncodeToString(digest[:])
}
