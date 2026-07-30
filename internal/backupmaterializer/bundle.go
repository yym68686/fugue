// Package backupmaterializer defines the private, secret-bearing handoff used
// to materialize one exact backup observer input generation. It deliberately
// has no store, network, Kubernetes, filesystem, or process-execution
// capability: a caller supplies an already validated desired spec and the
// dedicated observer identity keyring.
package backupmaterializer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
)

const (
	ObserverInputBundleAPIVersion = "backup-materializer.fugue.dev/v1"
	ObserverInputBundleKind       = "BackupObserverInputBundle"
	ObserverInputBundlePolicy     = "single-secret-observation-input-v1"

	ObserverIdentityTTL         = 15 * time.Minute
	ObserverIdentityRenewAfter  = 5 * time.Minute
	MaxObserverInputBundleBytes = 64 << 10
)

var ErrObserverInputBundle = errors.New("invalid backup observer input bundle")

// ObserverInputBundle is the one secret-bearing generation that a future
// fixed-purpose materializer will project into one cell-local Secret. Keeping
// the spec and token in one digest-bound envelope prevents independent callers
// from inventing or silently mixing either half of the input pair.
//
// ObserverToken is intentionally JSON-visible because it is the private
// handoff payload. String and GoString redact it so ordinary structured or
// diagnostic formatting cannot disclose the bearer credential.
type ObserverInputBundle struct {
	APIVersion                string                      `json:"apiVersion"`
	Kind                      string                      `json:"kind"`
	Policy                    string                      `json:"policy"`
	CellKey                   string                      `json:"cellKey"`
	RunID                     string                      `json:"runId"`
	SpecDigest                string                      `json:"specDigest"`
	CredentialID              string                      `json:"credentialId"`
	TokenID                   string                      `json:"tokenId"`
	DesiredSpec               backupcontrol.BackupRunSpec `json:"desiredSpec"`
	ObserverToken             string                      `json:"observerToken"`
	IssuedAt                  time.Time                   `json:"issuedAt"`
	RenewAfter                time.Time                   `json:"renewAfter"`
	ExpiresAt                 time.Time                   `json:"expiresAt"`
	ObservationOnly           bool                        `json:"observationOnly"`
	ProductionMutationAllowed bool                        `json:"productionMutationAllowed"`
	Digest                    string                      `json:"digest"`
}

// IssueObserverInputBundle mints one exact, fixed-lifetime observer input
// generation. The token is verified against the same keyring before it is
// returned, so signer and verifier configuration drift fails closed inside the
// trusted issuer rather than crossing the future HTTP/materialization seam.
func IssueObserverInputBundle(
	keyring backupidentity.Keyring,
	spec backupcontrol.BackupRunSpec,
	tenantID string,
	now time.Time,
) (ObserverInputBundle, error) {
	if now.IsZero() || backupcontrol.ValidateBackupRunSpec(spec) != nil {
		return ObserverInputBundle{}, ErrObserverInputBundle
	}
	now = now.UTC().Truncate(time.Second)
	token, err := backupidentity.Issue(keyring, backupidentity.Claims{
		CredentialID: backupidentity.CredentialIDForCell(spec.CellKey),
		RunID:        spec.RunID,
		TenantID:     strings.TrimSpace(tenantID),
		CellKey:      spec.CellKey,
		SpecDigest:   spec.Digest,
	}, now, ObserverIdentityTTL)
	if err != nil {
		return ObserverInputBundle{}, fmt.Errorf("%w: issue observer identity", ErrObserverInputBundle)
	}
	claims, err := backupidentity.Parse(keyring, token, now)
	if err != nil {
		return ObserverInputBundle{}, fmt.Errorf("%w: self-check observer identity", ErrObserverInputBundle)
	}
	bundle := ObserverInputBundle{
		APIVersion:                ObserverInputBundleAPIVersion,
		Kind:                      ObserverInputBundleKind,
		Policy:                    ObserverInputBundlePolicy,
		CellKey:                   spec.CellKey,
		RunID:                     spec.RunID,
		SpecDigest:                spec.Digest,
		CredentialID:              claims.CredentialID,
		TokenID:                   claims.TokenID,
		DesiredSpec:               spec,
		ObserverToken:             token,
		IssuedAt:                  time.Unix(claims.IssuedAtUnix, 0).UTC(),
		RenewAfter:                time.Unix(claims.IssuedAtUnix, 0).UTC().Add(ObserverIdentityRenewAfter),
		ExpiresAt:                 time.Unix(claims.ExpiresAtUnix, 0).UTC(),
		ObservationOnly:           true,
		ProductionMutationAllowed: false,
	}
	bundle.Digest = DigestObserverInputBundle(bundle)
	if err := ValidateObserverInputBundle(bundle, keyring, now); err != nil {
		return ObserverInputBundle{}, err
	}
	return bundle, nil
}

// ValidateObserverInputBundle verifies the strict redundant bindings, digest,
// fixed lifetime, and the bearer token signature/claims at the supplied time.
func ValidateObserverInputBundle(bundle ObserverInputBundle, keyring backupidentity.Keyring, now time.Time) error {
	if now.IsZero() ||
		bundle.APIVersion != ObserverInputBundleAPIVersion ||
		bundle.Kind != ObserverInputBundleKind ||
		bundle.Policy != ObserverInputBundlePolicy ||
		!bundle.ObservationOnly || bundle.ProductionMutationAllowed ||
		backupcontrol.ValidateBackupRunSpec(bundle.DesiredSpec) != nil ||
		bundle.CellKey != bundle.DesiredSpec.CellKey ||
		bundle.RunID != bundle.DesiredSpec.RunID ||
		bundle.SpecDigest != bundle.DesiredSpec.Digest ||
		bundle.CredentialID != backupidentity.CredentialIDForCell(bundle.CellKey) ||
		strings.TrimSpace(bundle.ObserverToken) != bundle.ObserverToken ||
		bundle.ObserverToken == "" || bundle.TokenID == "" ||
		!canonicalBundleTime(bundle.IssuedAt) ||
		!canonicalBundleTime(bundle.RenewAfter) ||
		!canonicalBundleTime(bundle.ExpiresAt) ||
		bundle.RenewAfter != bundle.IssuedAt.Add(ObserverIdentityRenewAfter) ||
		bundle.ExpiresAt != bundle.IssuedAt.Add(ObserverIdentityTTL) ||
		bundle.Digest != DigestObserverInputBundle(bundle) {
		return ErrObserverInputBundle
	}
	claims, err := backupidentity.Parse(keyring, bundle.ObserverToken, now.UTC().Truncate(time.Second))
	if err != nil ||
		claims.CredentialID != bundle.CredentialID ||
		claims.TokenID != bundle.TokenID ||
		claims.RunID != bundle.RunID ||
		claims.CellKey != bundle.CellKey ||
		claims.SpecDigest != bundle.SpecDigest ||
		claims.IssuedAtUnix != bundle.IssuedAt.Unix() ||
		claims.ExpiresAtUnix != bundle.ExpiresAt.Unix() {
		return ErrObserverInputBundle
	}
	return nil
}

// DecodeObserverInputBundle enforces a bounded, additional-properties-denied
// wire boundary before performing semantic and cryptographic validation.
func DecodeObserverInputBundle(
	document []byte,
	keyring backupidentity.Keyring,
	now time.Time,
) (ObserverInputBundle, error) {
	if len(document) == 0 || len(document) > MaxObserverInputBundleBytes {
		return ObserverInputBundle{}, ErrObserverInputBundle
	}
	var bundle ObserverInputBundle
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return ObserverInputBundle{}, ErrObserverInputBundle
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ObserverInputBundle{}, ErrObserverInputBundle
	}
	if err := ValidateObserverInputBundle(bundle, keyring, now); err != nil {
		return ObserverInputBundle{}, err
	}
	return bundle, nil
}

func DigestObserverInputBundle(bundle ObserverInputBundle) string {
	bundle.Digest = ""
	document, err := json.Marshal(bundle)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(document)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func (bundle ObserverInputBundle) String() string {
	return fmt.Sprintf(
		"BackupObserverInputBundle{cell=%q run=%q spec=%q credential=%q tokenID=%q observerToken=[REDACTED] issuedAt=%q renewAfter=%q expiresAt=%q digest=%q}",
		bundle.CellKey,
		bundle.RunID,
		bundle.SpecDigest,
		bundle.CredentialID,
		bundle.TokenID,
		bundle.IssuedAt.Format(time.RFC3339),
		bundle.RenewAfter.Format(time.RFC3339),
		bundle.ExpiresAt.Format(time.RFC3339),
		bundle.Digest,
	)
}

func (bundle ObserverInputBundle) GoString() string {
	return bundle.String()
}

func canonicalBundleTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Nanosecond() == 0
}
