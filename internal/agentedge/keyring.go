package agentedge

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"fugue/internal/staticedgecontract"
)

const PrivateKeyringSchema = "fugue.agent-edge-signing/v1"
const TrustKeyringSchema = "fugue.agent-edge-trust/v1"

type PublicKeyConfig struct {
	KeyID     string    `json:"key_id"`
	PublicKey string    `json:"public_key"`
	NotBefore time.Time `json:"not_before"`
	NotAfter  time.Time `json:"not_after"`
	Revoked   bool      `json:"revoked"`
}

type PrivateKeyConfig struct {
	PublicKeyConfig
	PrivateKey string `json:"private_key"`
}

type PrivateKeyring struct {
	Schema     string             `json:"schema"`
	Generation uint64             `json:"generation"`
	Keys       []PrivateKeyConfig `json:"keys"`
}

type TrustKeyring struct {
	Schema     string            `json:"schema"`
	Generation uint64            `json:"generation"`
	Keys       []PublicKeyConfig `json:"keys"`
}

func (k TrustKeyring) PublicKeys() (map[string]TrustKey, error) {
	if k.Schema != TrustKeyringSchema || k.Generation == 0 || len(k.Keys) == 0 || len(k.Keys) > 16 {
		return nil, errors.New("Agent Edge trust configuration is invalid")
	}
	result := make(map[string]TrustKey, len(k.Keys))
	for i, v := range k.Keys {
		key, err := base64.RawURLEncoding.DecodeString(v.PublicKey)
		if !identifier.MatchString(v.KeyID) || i > 0 && k.Keys[i-1].KeyID >= v.KeyID || err != nil || len(key) != ed25519.PublicKeySize ||
			v.NotBefore.IsZero() || !v.NotAfter.After(v.NotBefore) {
			return nil, errors.New("Agent Edge trust key or lifetime is invalid")
		}
		result[v.KeyID] = TrustKey{PublicKey: ed25519.PublicKey(key), NotBefore: v.NotBefore, NotAfter: v.NotAfter, Revoked: v.Revoked}
	}
	return result, nil
}

func (k PrivateKeyring) Public() TrustKeyring {
	result := TrustKeyring{Schema: TrustKeyringSchema, Generation: k.Generation}
	for _, v := range k.Keys {
		result.Keys = append(result.Keys, v.PublicKeyConfig)
	}
	return result
}

func (k PrivateKeyring) Validate() error {
	if k.Schema != PrivateKeyringSchema {
		return errors.New("Agent Edge private key configuration schema is invalid")
	}
	keys, err := k.Public().PublicKeys()
	if err != nil {
		return err
	}
	for _, v := range k.Keys {
		if v.Revoked && v.PrivateKey == "" {
			continue
		}
		key, err := base64.RawURLEncoding.DecodeString(v.PrivateKey)
		if err != nil || len(key) != ed25519.PrivateKeySize {
			return errors.New("Agent Edge private signing material is invalid")
		}
		derived := ed25519.NewKeyFromSeed(key[:ed25519.SeedSize])
		if !bytes.Equal(key, derived) || !bytes.Equal(derived.Public().(ed25519.PublicKey), keys[v.KeyID].PublicKey) {
			return errors.New("Agent Edge private and public keys do not match")
		}
	}
	return nil
}

func (k PrivateKeyring) SigningKey(id string, now time.Time) (ed25519.PrivateKey, PublicKeyConfig, error) {
	if err := k.Validate(); err != nil {
		return nil, PublicKeyConfig{}, err
	}
	for _, v := range k.Keys {
		if v.KeyID == id && !v.Revoked && !now.Before(v.NotBefore) && now.Before(v.NotAfter) {
			key, _ := base64.RawURLEncoding.DecodeString(v.PrivateKey)
			return ed25519.PrivateKey(key), v.PublicKeyConfig, nil
		}
	}
	return nil, PublicKeyConfig{}, errors.New("Agent Edge policy has no live configured signing key")
}

func readKeyFile(path string, private bool) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("Agent Edge key configuration requires an absolute path")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("Agent Edge key configuration unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<10 || info.Mode().Perm()&0022 != 0 || private && info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("Agent Edge key configuration permissions or size are invalid")
	}
	raw, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil || len(raw) > 64<<10 {
		return nil, errors.New("Agent Edge key configuration read failed")
	}
	return raw, nil
}

func LoadPrivateKeyring(path string) (PrivateKeyring, error) {
	var k PrivateKeyring
	raw, err := readKeyFile(path, true)
	if err != nil {
		return k, err
	}
	if staticedgecontract.StrictJSON(raw, &k) != nil || k.Validate() != nil {
		return k, errors.New("Agent Edge private key configuration is invalid")
	}
	return k, nil
}

func LoadTrustKeyring(path string) (TrustKeyring, error) {
	var k TrustKeyring
	raw, err := readKeyFile(path, false)
	if err != nil {
		return k, err
	}
	if staticedgecontract.StrictJSON(raw, &k) != nil {
		return k, errors.New("Agent Edge public trust configuration is invalid")
	}
	if _, err = k.PublicKeys(); err != nil {
		return k, err
	}
	return k, nil
}
