package trafficoverride

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"fugue/internal/model"
)

var (
	ErrMissingSigningKey = errors.New("traffic override signing key is missing")
	ErrInvalidSignature  = errors.New("traffic override signature is invalid")
)

func Sign(override model.TrafficOverride, privateKey, keyID string) (model.TrafficOverride, error) {
	privateKey = strings.TrimSpace(privateKey)
	keyID = strings.TrimSpace(keyID)
	privateKeyBytes, err := base64.RawStdEncoding.DecodeString(privateKey)
	if err != nil || len(privateKeyBytes) != ed25519.PrivateKeySize || keyID == "" {
		return model.TrafficOverride{}, ErrMissingSigningKey
	}
	override.Schema = model.TrafficOverrideSchemaV1
	override.KeyID = keyID
	override.Signature = ""
	override.ArtifactDigest = ""
	payload, err := signaturePayload(override)
	if err != nil {
		return model.TrafficOverride{}, err
	}
	digest := sha256.Sum256(payload)
	override.ArtifactDigest = "sha256:" + hex.EncodeToString(digest[:])
	override.Signature = "ed25519:" + base64.RawStdEncoding.EncodeToString(ed25519.Sign(ed25519.PrivateKey(privateKeyBytes), payload))
	return override, nil
}

func Verify(override model.TrafficOverride, publicKey, keyID string) error {
	publicKey = strings.TrimSpace(publicKey)
	keyID = strings.TrimSpace(keyID)
	publicKeyBytes, err := base64.RawStdEncoding.DecodeString(publicKey)
	if err != nil || len(publicKeyBytes) != ed25519.PublicKeySize || keyID == "" || override.KeyID != keyID {
		return ErrInvalidSignature
	}
	providedSignature := override.Signature
	providedDigest := override.ArtifactDigest
	unsigned := override
	unsigned.Signature = ""
	unsigned.ArtifactDigest = ""
	payload, err := signaturePayload(unsigned)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	wantDigest := "sha256:" + hex.EncodeToString(digest[:])
	signatureBytes, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(providedSignature, "ed25519:"))
	if err != nil || !strings.HasPrefix(providedSignature, "ed25519:") || providedDigest != wantDigest || !ed25519.Verify(ed25519.PublicKey(publicKeyBytes), payload, signatureBytes) {
		return ErrInvalidSignature
	}
	return nil
}

func VerifyWithKeyring(override model.TrafficOverride, keyring model.TrafficOverrideSigningKeyring) error {
	if override.KeyID == keyring.CurrentKeyID {
		return Verify(override, keyring.CurrentPublicKey, keyring.CurrentKeyID)
	}
	if override.KeyID == keyring.PreviousKeyID && keyring.PreviousKeyID != "" {
		return Verify(override, keyring.PreviousPublicKey, keyring.PreviousKeyID)
	}
	return ErrInvalidSignature
}

func signaturePayload(override model.TrafficOverride) ([]byte, error) {
	return json.Marshal(override)
}
