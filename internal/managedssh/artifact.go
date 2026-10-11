package managedssh

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
)

type Artifact struct {
	Schema        string        `json:"schema"`
	Digest        string        `json:"digest"`
	KeyID         string        `json:"key_id"`
	Configuration Configuration `json:"configuration"`
	Signature     string        `json:"signature"`
}

var ErrArtifact = errors.New("managed SSH artifact signature or digest is invalid")

func SignArtifact(configuration Configuration, keyID string, key ed25519.PrivateKey) (Artifact, error) {
	if len(key) != ed25519.PrivateKeySize || !safeToken(keyID) {
		return Artifact{}, ErrArtifact
	}
	payload, err := json.Marshal(configuration)
	if err != nil {
		return Artifact{}, err
	}
	digest := sha256.Sum256(payload)
	artifact := Artifact{Schema: "fugue.managed-ssh-artifact/v1", Digest: "sha256:" + hex.EncodeToString(digest[:]), KeyID: keyID, Configuration: configuration}
	unsigned, err := json.Marshal(artifact)
	if err != nil {
		return Artifact{}, err
	}
	artifact.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, unsigned))
	return artifact, nil
}

func VerifyArtifact(artifact Artifact, keys map[string]ed25519.PublicKey) (Configuration, error) {
	if artifact.Schema != "fugue.managed-ssh-artifact/v1" || len(keys[artifact.KeyID]) != ed25519.PublicKeySize {
		return Configuration{}, ErrArtifact
	}
	signature, err := base64.StdEncoding.DecodeString(artifact.Signature)
	if err != nil {
		return Configuration{}, ErrArtifact
	}
	unsigned := artifact
	unsigned.Signature = ""
	raw, err := json.Marshal(unsigned)
	if err != nil || !ed25519.Verify(keys[artifact.KeyID], raw, signature) {
		return Configuration{}, ErrArtifact
	}
	payload, err := json.Marshal(artifact.Configuration)
	if err != nil {
		return Configuration{}, ErrArtifact
	}
	digest := sha256.Sum256(payload)
	if artifact.Digest != "sha256:"+hex.EncodeToString(digest[:]) {
		return Configuration{}, ErrArtifact
	}
	var configuration Configuration
	if err := json.Unmarshal(payload, &configuration); err != nil {
		return Configuration{}, err
	}
	return configuration, nil
}
