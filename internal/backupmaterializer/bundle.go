// Package backupmaterializer owns issuance and full cryptographic validation
// for the private observer input bundle. The pure wire/envelope contract lives
// in the capability-separated contract subpackage so consumers do not import
// the observer signing implementation.
package backupmaterializer

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	materializercontract "fugue/internal/backupmaterializer/contract"
)

const (
	ObserverInputBundleAPIVersion = materializercontract.ObserverInputBundleAPIVersion
	ObserverInputBundleKind       = materializercontract.ObserverInputBundleKind
	ObserverInputBundlePolicy     = materializercontract.ObserverInputBundlePolicy

	ObserverIdentityTTL         = materializercontract.ObserverIdentityTTL
	ObserverIdentityRenewAfter  = materializercontract.ObserverIdentityRenewAfter
	MaxObserverInputBundleBytes = materializercontract.MaxObserverInputBundleBytes
)

var ErrObserverInputBundle = materializercontract.ErrObserverInputBundle

// ObserverInputBundle remains a source-compatible alias while its pure wire
// definition is owned by the contract package.
type ObserverInputBundle = materializercontract.ObserverInputBundle

// IssueObserverInputBundle mints one exact, fixed-lifetime observer input
// generation. The token is verified against the same keyring before it is
// returned, so signer and verifier configuration drift fails closed inside the
// trusted issuer rather than crossing the HTTP/materialization seam.
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

// ValidateObserverInputBundle verifies the public envelope and authenticates
// the bearer token signature and claims with the supplied keyring.
func ValidateObserverInputBundle(bundle ObserverInputBundle, keyring backupidentity.Keyring, now time.Time) error {
	if err := ValidateObserverInputBundleEnvelope(bundle, now); err != nil {
		return err
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

// ValidateObserverInputBundleEnvelope delegates to the pure, keyless wire
// contract. It does not authenticate the HMAC; use ValidateObserverInputBundle
// wherever a verification keyring is already owned.
func ValidateObserverInputBundleEnvelope(bundle ObserverInputBundle, now time.Time) error {
	return materializercontract.ValidateObserverInputBundleEnvelope(bundle, now)
}

// DecodeObserverInputBundle applies the strict envelope decoder and then the
// full cryptographic validator.
func DecodeObserverInputBundle(
	document []byte,
	keyring backupidentity.Keyring,
	now time.Time,
) (ObserverInputBundle, error) {
	bundle, err := materializercontract.DecodeObserverInputBundleEnvelope(document, now)
	if err != nil {
		return ObserverInputBundle{}, err
	}
	if err := ValidateObserverInputBundle(bundle, keyring, now); err != nil {
		return ObserverInputBundle{}, err
	}
	return bundle, nil
}

// DecodeObserverInputBundleEnvelope exposes the source-compatible keyless
// decoder while keeping its implementation in the pure contract package.
func DecodeObserverInputBundleEnvelope(document []byte, now time.Time) (ObserverInputBundle, error) {
	return materializercontract.DecodeObserverInputBundleEnvelope(document, now)
}

func DigestObserverInputBundle(bundle ObserverInputBundle) string {
	return materializercontract.DigestObserverInputBundle(bundle)
}
