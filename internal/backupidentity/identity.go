// Package backupidentity defines the short-lived, fixed-purpose identity used
// by one backup observer to read one exact run/spec observation. It is
// deliberately separate from tenant, workload, node, and platform-component
// credentials so none of those authority domains can be confused or reused.
package backupidentity

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

const (
	identityTokenPrefix = "fugue_bo_v1."
	identityVersion     = "v1"
	identityKeyContext  = "fugue/backup-observer-identity/v1"

	PermissionReadRunObservation = "backup.run-observation.read.v1"
	MaxTTL                       = 15 * time.Minute
	FutureSkew                   = 30 * time.Second
	maxTokenBytes                = 4096
	minSigningKeyBytes           = 32
)

var (
	ErrInvalidIdentity = errors.New("invalid backup observer identity")
	ErrExpiredIdentity = errors.New("expired backup observer identity")

	canonicalIDPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	canonicalTokenID       = regexp.MustCompile(`^[A-Za-z0-9_-]{22}$`)
	canonicalCellKey       = regexp.MustCompile(`^backup/(control-plane-db|app-database|persistent-storage|data-workspace|registry|platform-component)/[0-9a-f]{16}$`)
	canonicalDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type Keyring struct {
	ActiveKeyID   string
	Keys          map[string]string
	RevokedKeyIDs map[string]struct{}
}

// Claims bind a credential to one exact desired spec. A token cannot be used
// to enumerate another run in the same cell or to follow a spec generation
// after the externally owned ConfigMap/Secret pair has rotated.
type Claims struct {
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

// DeriveKeyring builds a rotation-aware keyring from a dedicated source key.
// Weak, missing, malformed, or revoked keys are omitted, which makes issuance
// and verification fail closed instead of silently falling back to another
// Fugue credential domain.
func DeriveKeyring(activeKey, activeKeyID, previousKey, previousKeyID string, revokedKeyIDs []string) Keyring {
	keyring := Keyring{
		Keys:          map[string]string{},
		RevokedKeyIDs: map[string]struct{}{},
	}
	for _, rawKeyID := range revokedKeyIDs {
		keyID := strings.TrimSpace(rawKeyID)
		if canonicalIDPattern.MatchString(keyID) {
			keyring.RevokedKeyIDs[keyID] = struct{}{}
		}
	}
	addKey := func(rawKey, rawKeyID string, active bool) {
		key := strings.TrimSpace(rawKey)
		keyID := strings.TrimSpace(rawKeyID)
		if len(key) < minSigningKeyBytes || !canonicalIDPattern.MatchString(keyID) || keyring.isRevoked(keyID) {
			return
		}
		keyring.Keys[keyID] = deriveSigningKey(key)
		if active {
			keyring.ActiveKeyID = keyID
		}
	}
	addKey(previousKey, previousKeyID, false)
	addKey(activeKey, activeKeyID, true)
	return keyring
}

func Issue(keyring Keyring, claims Claims, now time.Time, ttl time.Duration) (string, error) {
	now = canonicalTime(now)
	if ttl <= 0 || ttl > MaxTTL || ttl%time.Second != 0 {
		return "", ErrInvalidIdentity
	}
	keyID := strings.TrimSpace(keyring.ActiveKeyID)
	secret := strings.TrimSpace(keyring.Keys[keyID])
	if !canonicalIDPattern.MatchString(keyID) || len(secret) < minSigningKeyBytes || keyring.isRevoked(keyID) {
		return "", ErrInvalidIdentity
	}
	claims.Version = identityVersion
	claims.Permission = PermissionReadRunObservation
	claims.IssuedAtUnix = now.Unix()
	claims.ExpiresAtUnix = now.Add(ttl).Unix()
	tokenID, err := randomTokenID()
	if err != nil {
		return "", err
	}
	claims.TokenID = tokenID
	claims, err = normalizeClaims(claims)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal backup observer identity: %w", err)
	}
	keyIDEncoded := base64.RawURLEncoding.EncodeToString([]byte(keyID))
	payloadEncoded := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := identityVersion + "." + keyIDEncoded + "." + payloadEncoded
	signature := sign(secret, signingInput)
	return identityTokenPrefix + keyIDEncoded + "." + payloadEncoded + "." + signature, nil
}

func Parse(keyring Keyring, token string, now time.Time) (Claims, error) {
	token = strings.TrimSpace(token)
	if len(token) == 0 || len(token) > maxTokenBytes || !strings.HasPrefix(token, identityTokenPrefix) {
		return Claims{}, ErrInvalidIdentity
	}
	parts := strings.Split(strings.TrimPrefix(token, identityTokenPrefix), ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return Claims{}, ErrInvalidIdentity
	}
	keyIDBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Claims{}, ErrInvalidIdentity
	}
	keyID := strings.TrimSpace(string(keyIDBytes))
	secret := strings.TrimSpace(keyring.Keys[keyID])
	if !canonicalIDPattern.MatchString(keyID) || len(secret) < minSigningKeyBytes || keyring.isRevoked(keyID) {
		return Claims{}, ErrInvalidIdentity
	}
	signingInput := identityVersion + "." + parts[0] + "." + parts[1]
	if !hmac.Equal([]byte(sign(secret, signingInput)), []byte(parts[2])) {
		return Claims{}, ErrInvalidIdentity
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(payload) == 0 || len(payload) > maxTokenBytes {
		return Claims{}, ErrInvalidIdentity
	}
	var claims Claims
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claims); err != nil {
		return Claims{}, ErrInvalidIdentity
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Claims{}, ErrInvalidIdentity
	}
	claims, err = normalizeClaims(claims)
	if err != nil {
		return Claims{}, err
	}
	now = canonicalTime(now)
	issuedAt := time.Unix(claims.IssuedAtUnix, 0).UTC()
	expiresAt := time.Unix(claims.ExpiresAtUnix, 0).UTC()
	if !expiresAt.After(issuedAt) || expiresAt.After(issuedAt.Add(MaxTTL)) || issuedAt.After(now.Add(FutureSkew)) {
		return Claims{}, ErrInvalidIdentity
	}
	if !expiresAt.After(now) {
		return Claims{}, ErrExpiredIdentity
	}
	return claims, nil
}

func CredentialIDForCell(cellKey string) string {
	if !canonicalCellKey.MatchString(cellKey) {
		return ""
	}
	segments := strings.Split(cellKey, "/")
	return "backup-observer:" + segments[1] + ":" + segments[2]
}

func normalizeClaims(claims Claims) (Claims, error) {
	claims.Version = strings.TrimSpace(strings.ToLower(claims.Version))
	claims.CredentialID = strings.TrimSpace(claims.CredentialID)
	claims.TokenID = strings.TrimSpace(claims.TokenID)
	claims.RunID = strings.TrimSpace(claims.RunID)
	claims.TenantID = strings.TrimSpace(claims.TenantID)
	claims.CellKey = strings.TrimSpace(claims.CellKey)
	claims.SpecDigest = strings.TrimSpace(claims.SpecDigest)
	claims.Permission = strings.TrimSpace(claims.Permission)
	if claims.Version != identityVersion ||
		claims.CredentialID != CredentialIDForCell(claims.CellKey) ||
		!canonicalTokenID.MatchString(claims.TokenID) ||
		!canonicalIDPattern.MatchString(claims.RunID) ||
		(claims.TenantID != "" && !canonicalIDPattern.MatchString(claims.TenantID)) ||
		!canonicalDigestPattern.MatchString(claims.SpecDigest) ||
		claims.Permission != PermissionReadRunObservation ||
		claims.IssuedAtUnix <= 0 || claims.ExpiresAtUnix <= 0 {
		return Claims{}, ErrInvalidIdentity
	}
	return claims, nil
}

func canonicalTime(value time.Time) time.Time {
	if value.IsZero() {
		value = time.Now()
	}
	return value.UTC().Truncate(time.Second)
}

func randomTokenID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate backup observer token id: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func sign(secret, signingInput string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signingInput))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func deriveSigningKey(source string) string {
	mac := hmac.New(sha256.New, []byte(strings.TrimSpace(source)))
	_, _ = mac.Write([]byte(identityKeyContext))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (keyring Keyring) isRevoked(keyID string) bool {
	_, revoked := keyring.RevokedKeyIDs[strings.TrimSpace(keyID)]
	return revoked
}
