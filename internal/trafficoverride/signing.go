package trafficoverride

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

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
	preparedDigest, err := ComputePreparedDigest(override)
	if err != nil {
		return model.TrafficOverride{}, err
	}
	if override.PreparedDigest != "" && override.PreparedDigest != preparedDigest {
		return model.TrafficOverride{}, ErrInvalidSignature
	}
	override.PreparedDigest = preparedDigest
	payload, err := signaturePayload(override)
	if err != nil {
		return model.TrafficOverride{}, err
	}
	digest := sha256.Sum256(payload)
	override.ArtifactDigest = "sha256:" + hex.EncodeToString(digest[:])
	override.Signature = "ed25519:" + base64.RawStdEncoding.EncodeToString(ed25519.Sign(ed25519.PrivateKey(privateKeyBytes), payload))
	return override, nil
}

// ComputePreparedDigest identifies the exact route payload a DNS node prepares
// before activation. Signature, artifact digest, and audit timestamps are
// intentionally excluded so every authority computes the same digest.
func ComputePreparedDigest(override model.TrafficOverride) (string, error) {
	type preparedPayload struct {
		Schema             string    `json:"schema"`
		Hostname           string    `json:"hostname"`
		Generation         uint64    `json:"generation"`
		State              string    `json:"state"`
		Answers            []string  `json:"answers"`
		RequiredHostRoutes []string  `json:"required_host_routes"`
		RouteGeneration    string    `json:"route_generation"`
		RouteDigest        string    `json:"route_digest"`
		ActivateAt         time.Time `json:"activate_at"`
		ExpiresAt          time.Time `json:"expires_at"`
	}
	payload, err := json.Marshal(preparedPayload{
		Schema:             override.Schema,
		Hostname:           override.Hostname,
		Generation:         override.Generation,
		State:              override.State,
		Answers:            override.Answers,
		RequiredHostRoutes: override.RequiredHostRoutes,
		RouteGeneration:    override.RouteGeneration,
		RouteDigest:        override.RouteDigest,
		ActivateAt:         override.ActivateAt.UTC(),
		ExpiresAt:          override.ExpiresAt.UTC(),
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
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
	preparedDigest, err := ComputePreparedDigest(override)
	if err != nil || override.PreparedDigest != preparedDigest {
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
