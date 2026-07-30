// Package contract defines the private, secret-bearing observer input wire
// envelope without importing the observer signing implementation. It owns no
// store, network, filesystem, Kubernetes, process, or mutation capability.
package contract

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"fugue/internal/backupcontrol"
)

const (
	ObserverInputBundleAPIVersion = "backup-materializer.fugue.dev/v1"
	ObserverInputBundleKind       = "BackupObserverInputBundle"
	ObserverInputBundlePolicy     = "single-secret-observation-input-v1"

	ObserverIdentityTTL         = 15 * time.Minute
	ObserverIdentityRenewAfter  = 5 * time.Minute
	ObserverIdentityFutureSkew  = 30 * time.Second
	ObserverIdentityPermission  = "backup.run-observation.read.v1"
	MaxObserverInputBundleBytes = 64 << 10

	observerIdentityVersion = "v1"
	observerTokenPrefix     = "fugue_bo_v1."
	maxObserverTokenBytes   = 4096
)

var (
	ErrObserverInputBundle = errors.New("invalid backup observer input bundle")

	canonicalEnvelopeID      = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	canonicalEnvelopeTokenID = regexp.MustCompile(`^[A-Za-z0-9_-]{22}$`)
	canonicalEnvelopeDigest  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	canonicalEnvelopeCell    = regexp.MustCompile(`^backup/(control-plane-db|app-database|persistent-storage|data-workspace|registry|platform-component)/[0-9a-f]{16}$`)
)

// ObserverInputBundle is one digest-bound spec and observer credential
// generation. ObserverToken is intentionally JSON-visible in the private wire
// payload, while String and GoString always redact it.
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

// ValidateObserverInputBundleEnvelope validates the complete public envelope,
// redundant token claims, digest, fixed lifetime, and current validity without
// receiving the observer HMAC key. It is intended only for a caller that
// obtained the document over an authenticated materializer transport and does
// not authenticate the signature itself.
func ValidateObserverInputBundleEnvelope(bundle ObserverInputBundle, now time.Time) error {
	now = now.UTC().Truncate(time.Second)
	if now.IsZero() ||
		bundle.APIVersion != ObserverInputBundleAPIVersion ||
		bundle.Kind != ObserverInputBundleKind ||
		bundle.Policy != ObserverInputBundlePolicy ||
		!bundle.ObservationOnly || bundle.ProductionMutationAllowed ||
		backupcontrol.ValidateBackupRunSpec(bundle.DesiredSpec) != nil ||
		bundle.CellKey != bundle.DesiredSpec.CellKey ||
		bundle.RunID != bundle.DesiredSpec.RunID ||
		bundle.SpecDigest != bundle.DesiredSpec.Digest ||
		bundle.CredentialID != CredentialIDForCell(bundle.CellKey) ||
		!canonicalEnvelopeTokenID.MatchString(bundle.TokenID) ||
		!canonicalBundleTime(bundle.IssuedAt) ||
		!canonicalBundleTime(bundle.RenewAfter) ||
		!canonicalBundleTime(bundle.ExpiresAt) ||
		bundle.RenewAfter != bundle.IssuedAt.Add(ObserverIdentityRenewAfter) ||
		bundle.ExpiresAt != bundle.IssuedAt.Add(ObserverIdentityTTL) ||
		!canonicalEnvelopeDigest.MatchString(bundle.Digest) ||
		bundle.Digest != DigestObserverInputBundle(bundle) ||
		bundle.IssuedAt.After(now.Add(ObserverIdentityFutureSkew)) ||
		!bundle.ExpiresAt.After(now) ||
		!validObserverTokenEnvelope(bundle) {
		return ErrObserverInputBundle
	}
	return nil
}

// DecodeObserverInputBundleEnvelope applies a strict bounded JSON boundary and
// then validates the public envelope. It deliberately has no signing-key input.
func DecodeObserverInputBundleEnvelope(document []byte, now time.Time) (ObserverInputBundle, error) {
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
	if err := ValidateObserverInputBundleEnvelope(bundle, now); err != nil {
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

func CredentialIDForCell(cellKey string) string {
	if !canonicalEnvelopeCell.MatchString(cellKey) {
		return ""
	}
	segments := strings.Split(cellKey, "/")
	return "backup-observer:" + segments[1] + ":" + segments[2]
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

type observerIdentityClaims struct {
	Version       string `json:"v"`
	CredentialID  string `json:"credential_id"`
	TokenID       string `json:"token_id"`
	RunID         string `json:"run_id"`
	TenantID      string `json:"tenant_id,omitempty"`
	CellKey       string `json:"cell_key"`
	SpecDigest    string `json:"spec_digest"`
	Permission    string `json:"permission"`
	IssuedAtUnix  int64  `json:"issued_at"`
	ExpiresAtUnix int64  `json:"expires_at"`
}

func validObserverTokenEnvelope(bundle ObserverInputBundle) bool {
	token := bundle.ObserverToken
	if token == "" || len(token) > maxObserverTokenBytes || strings.TrimSpace(token) != token ||
		!strings.HasPrefix(token, observerTokenPrefix) || strings.ContainsAny(token, "\r\n") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(token, observerTokenPrefix), ".")
	if len(parts) != 3 {
		return false
	}
	keyIDDocument, ok := decodeCanonicalRawURLPart(parts[0], 256)
	if !ok || !canonicalEnvelopeID.Match(keyIDDocument) {
		return false
	}
	payload, ok := decodeCanonicalRawURLPart(parts[1], maxObserverTokenBytes)
	if !ok || len(payload) == 0 {
		return false
	}
	signature, ok := decodeCanonicalRawURLPart(parts[2], sha256.Size)
	if !ok || len(signature) != sha256.Size {
		return false
	}
	var claims observerIdentityClaims
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claims); err != nil {
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return false
	}
	return claims.Version == observerIdentityVersion &&
		claims.CredentialID == bundle.CredentialID &&
		claims.TokenID == bundle.TokenID &&
		claims.RunID == bundle.RunID &&
		(claims.TenantID == "" || canonicalEnvelopeID.MatchString(claims.TenantID)) &&
		claims.CellKey == bundle.CellKey &&
		claims.SpecDigest == bundle.SpecDigest &&
		claims.Permission == ObserverIdentityPermission &&
		claims.IssuedAtUnix == bundle.IssuedAt.Unix() &&
		claims.ExpiresAtUnix == bundle.ExpiresAt.Unix()
}

func decodeCanonicalRawURLPart(value string, maxDecodedBytes int) ([]byte, bool) {
	if value == "" || maxDecodedBytes <= 0 {
		return nil, false
	}
	document, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(document) > maxDecodedBytes || base64.RawURLEncoding.EncodeToString(document) != value {
		return nil, false
	}
	return document, true
}

func canonicalBundleTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Nanosecond() == 0
}
