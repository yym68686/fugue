package entryfailover

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"

	c "fugue/internal/staticedgecontract"
)

const signatureDomain = "fugue.entry-failover.policy/v1\n"

type SignedPolicy struct {
	Policy    Policy `json:"policy"`
	KeyID     string `json:"key_id"`
	Digest    string `json:"digest"`
	Signature string `json:"signature"`
}

func Sign(policy Policy, keyID string, key ed25519.PrivateKey) (SignedPolicy, error) {
	if err := policy.Validate(); err != nil {
		return SignedPolicy{}, err
	}
	if !identifier.MatchString(keyID) || len(key) != ed25519.PrivateKeySize {
		return SignedPolicy{}, errors.New("valid signing key and ID required")
	}
	digest := policy.Digest()
	return SignedPolicy{Policy: policy, KeyID: keyID, Digest: digest,
		Signature: hex.EncodeToString(ed25519.Sign(key, []byte(signatureDomain+digest)))}, nil
}

func (s SignedPolicy) Verify(keys map[string]ed25519.PublicKey) error {
	if err := s.Policy.Validate(); err != nil {
		return err
	}
	if !identifier.MatchString(s.KeyID) || s.Digest != s.Policy.Digest() || s.Signature == "" {
		return errors.New("signed policy identity or digest mismatch")
	}
	key := keys[s.KeyID]
	sig, err := hex.DecodeString(s.Signature)
	if err != nil || len(key) != ed25519.PublicKeySize || !ed25519.Verify(key, []byte(signatureDomain+s.Digest), sig) {
		return errors.New("policy signature verification failed")
	}
	return nil
}

func ParseSignedPolicy(raw []byte, keys map[string]ed25519.PublicKey) (SignedPolicy, error) {
	var s SignedPolicy
	if len(raw) > 64<<10 {
		return s, errors.New("policy exceeds 64 KiB")
	}
	if err := c.StrictJSON(raw, &s); err != nil {
		return s, fmt.Errorf("decode signed policy: %w", err)
	}
	if err := s.Verify(keys); err != nil {
		return s, err
	}
	return s, nil
}
