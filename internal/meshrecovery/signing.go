package meshrecovery

import (
	"crypto/ed25519"
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
	ErrUnsupportedSigningKey    = errors.New("unsupported mesh recovery signing key")
	ErrUnsupportedSchemaVersion = errors.New("unsupported mesh recovery schema version")
)

const (
	ed25519PrivatePrefix = "ed25519-private:"
	ed25519PublicPrefix  = "ed25519-public:"
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
	signature, err := signCanonical(out, key)
	if err != nil {
		return directory, err
	}
	out.Signature = signature
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
	signature, err := signCanonical(out, key)
	if err != nil {
		return manifest, err
	}
	out.Signature = signature
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
	ok, err := verifyCanonicalSignature(clearSignature(bundle), key, signature)
	if err != nil {
		return err
	}
	if !ok {
		return ErrInvalidSignature
	}
	return nil
}

func signCanonical(value any, key string) (string, error) {
	raw, _ := json.Marshal(value)
	if strings.HasPrefix(strings.TrimSpace(key), ed25519PrivatePrefix) {
		privateKey, err := parseEd25519PrivateKey(key)
		if err != nil {
			return "", err
		}
		return base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, raw)), nil
	}
	if strings.HasPrefix(strings.TrimSpace(key), ed25519PublicPrefix) {
		return "", ErrUnsupportedSigningKey
	}
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write(raw)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func verifyCanonicalSignature(value any, key, signature string) (bool, error) {
	raw, _ := json.Marshal(value)
	key = strings.TrimSpace(key)
	if strings.HasPrefix(key, ed25519PublicPrefix) {
		publicKey, err := parseEd25519PublicKey(key)
		if err != nil {
			return false, err
		}
		sig, err := decodeKeyMaterial(signature)
		if err != nil {
			return false, ErrInvalidSignature
		}
		return ed25519.Verify(publicKey, raw, sig), nil
	}
	if strings.HasPrefix(key, ed25519PrivatePrefix) {
		privateKey, err := parseEd25519PrivateKey(key)
		if err != nil {
			return false, err
		}
		sig, err := decodeKeyMaterial(signature)
		if err != nil {
			return false, ErrInvalidSignature
		}
		publicKey, ok := privateKey.Public().(ed25519.PublicKey)
		if !ok {
			return false, ErrUnsupportedSigningKey
		}
		return ed25519.Verify(publicKey, raw, sig), nil
	}
	expected, err := signCanonical(value, key)
	if err != nil {
		return false, err
	}
	return hmac.Equal([]byte(signature), []byte(expected)), nil
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

func parseEd25519PrivateKey(key string) (ed25519.PrivateKey, error) {
	raw, err := decodeKeyMaterial(strings.TrimPrefix(strings.TrimSpace(key), ed25519PrivatePrefix))
	if err != nil {
		return nil, ErrUnsupportedSigningKey
	}
	switch len(raw) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(raw), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(raw), nil
	default:
		return nil, ErrUnsupportedSigningKey
	}
}

func parseEd25519PublicKey(key string) (ed25519.PublicKey, error) {
	raw, err := decodeKeyMaterial(strings.TrimPrefix(strings.TrimSpace(key), ed25519PublicPrefix))
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, ErrUnsupportedSigningKey
	}
	return ed25519.PublicKey(raw), nil
}

func decodeKeyMaterial(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, ErrUnsupportedSigningKey
	}
	if raw, err := base64.RawURLEncoding.DecodeString(value); err == nil {
		return raw, nil
	}
	if raw, err := base64.StdEncoding.DecodeString(value); err == nil {
		return raw, nil
	}
	return nil, ErrUnsupportedSigningKey
}
