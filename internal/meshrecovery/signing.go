package meshrecovery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrExpiredManifest          = errors.New("mesh recovery manifest expired")
	ErrInvalidSignature         = errors.New("mesh recovery signature invalid")
	ErrKeyIDMismatch            = errors.New("mesh recovery key id mismatch")
	ErrMissingSignature         = errors.New("mesh recovery signature missing")
	ErrMissingSigningKey        = errors.New("mesh recovery signing key missing")
	ErrUnsupportedSchemaVersion = errors.New("unsupported mesh recovery schema version")
)

func SignPeerDirectory(directory PeerDirectory, key, keyID string, validFor time.Duration, now time.Time) (PeerDirectory, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return directory, ErrMissingSigningKey
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	out := directory
	out.SchemaVersion = SchemaVersionV1
	if strings.TrimSpace(out.Issuer) == "" {
		out.Issuer = DefaultIssuer
	}
	if out.GeneratedAt.IsZero() {
		out.GeneratedAt = now
	}
	if validFor > 0 {
		out.ValidUntil = out.GeneratedAt.Add(validFor)
	}
	out.KeyID = strings.TrimSpace(keyID)
	out.Signature = ""
	out.Signature = signCanonical(out, key)
	return out, nil
}

func VerifyPeerDirectory(directory PeerDirectory, key, expectedKeyID string, now time.Time) error {
	if err := validateSchemaVersion(directory.SchemaVersion); err != nil {
		return err
	}
	if !directory.ValidUntil.IsZero() && !now.IsZero() && now.After(directory.ValidUntil) {
		return ErrExpiredManifest
	}
	return verifyCanonical(directory, key, expectedKeyID, directory.KeyID, directory.Signature)
}

func SignGenerationManifest(manifest GenerationManifest, key, keyID string, validFor time.Duration, now time.Time) (GenerationManifest, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return manifest, ErrMissingSigningKey
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	out := manifest
	out.SchemaVersion = SchemaVersionV1
	if strings.TrimSpace(out.Issuer) == "" {
		out.Issuer = DefaultIssuer
	}
	if out.IssuedAt.IsZero() {
		out.IssuedAt = now
	}
	if strings.TrimSpace(out.Mode) == "" {
		out.Mode = GenerationModeNormal
	}
	if validFor > 0 {
		out.ValidUntil = out.IssuedAt.Add(validFor)
	}
	out.KeyID = strings.TrimSpace(keyID)
	out.Signature = ""
	out.Signature = signCanonical(out, key)
	return out, nil
}

func VerifyGenerationManifest(manifest GenerationManifest, key, expectedKeyID string, now time.Time) error {
	if err := validateSchemaVersion(manifest.SchemaVersion); err != nil {
		return err
	}
	if !manifest.ValidUntil.IsZero() && !now.IsZero() && now.After(manifest.ValidUntil) {
		return ErrExpiredManifest
	}
	return verifyCanonical(manifest, key, expectedKeyID, manifest.KeyID, manifest.Signature)
}

func verifyCanonical(bundle any, key, expectedKeyID, actualKeyID, signature string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return ErrMissingSigningKey
	}
	signature = strings.TrimSpace(signature)
	if signature == "" {
		return ErrMissingSignature
	}
	actualKeyID = strings.TrimSpace(actualKeyID)
	expectedKeyID = strings.TrimSpace(expectedKeyID)
	if expectedKeyID != "" && !strings.EqualFold(expectedKeyID, actualKeyID) {
		return fmt.Errorf("%w: got %s", ErrKeyIDMismatch, actualKeyID)
	}
	expected := signCanonical(clearSignature(bundle), key)
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return ErrInvalidSignature
	}
	return nil
}

func signCanonical(value any, key string) string {
	raw, _ := json.Marshal(value)
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write(raw)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func clearSignature(value any) any {
	switch typed := value.(type) {
	case PeerDirectory:
		typed.Signature = ""
		return typed
	case GenerationManifest:
		typed.Signature = ""
		return typed
	default:
		return value
	}
}

func validateSchemaVersion(schemaVersion string) error {
	schemaVersion = strings.TrimSpace(schemaVersion)
	if schemaVersion == "" {
		return nil
	}
	major, _, _ := strings.Cut(schemaVersion, ".")
	expectedMajor, _, _ := strings.Cut(SchemaVersionV1, ".")
	if major != expectedMajor {
		return fmt.Errorf("%w: %s", ErrUnsupportedSchemaVersion, schemaVersion)
	}
	return nil
}
