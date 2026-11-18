package platformsafety

import (
	"crypto/hmac"
	"encoding/json"
	"strings"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

// PreparedArtifactSignature is an owned snapshot of a single signing payload.
// It is not a content check or a trust verdict. Verify always resolves the
// current key and checks the signature, even when the payload was seen before.
type PreparedArtifactSignature struct {
	payload   []byte
	keyID     string
	signature string
}

func PrepareArtifactSignature(a model.PlatformArtifact) (PreparedArtifactSignature, error) {
	p := a.Provenance
	if !validArtifactSignatureProvenance(p) {
		return PreparedArtifactSignature{}, ErrPlatformSignatureInvalid
	}
	raw, err := json.Marshal(platformArtifactSigningPayloadFor(a))
	if err != nil {
		return PreparedArtifactSignature{}, err
	}
	return PreparedArtifactSignature{payload: raw, keyID: strings.Clone(strings.TrimSpace(p.KeyID)), signature: strings.Clone(strings.TrimSpace(p.Signature))}, nil
}

func (p PreparedArtifactSignature) Size() int {
	return len(p.payload) + len(p.keyID) + len(p.signature)
}

func (p PreparedArtifactSignature) Verify(keys bundleauth.Keyring) error {
	if len(p.payload) == 0 {
		return ErrPlatformSignatureInvalid
	}
	key, err := platformVerificationKey(keys, p.keyID)
	if err != nil {
		return err
	}
	if !hmac.Equal([]byte(p.signature), []byte(signPlatformPayload(p.payload, key))) {
		return ErrPlatformSignatureInvalid
	}
	return nil
}
