package managedssh

import (
	"crypto/ed25519"
	"encoding/base64"
)

type ConfigurationSigner struct {
	KeyID      string
	PrivateKey ed25519.PrivateKey
	PublicKeys map[string]ed25519.PublicKey
}

func LoadConfigurationSigner(path string) (ConfigurationSigner, error) {
	raw, err := privateFile(path, 65536)
	if err != nil {
		return ConfigurationSigner{}, err
	}
	defer clear(raw)
	var file struct {
		Schema      string            `json:"schema"`
		ActiveKeyID string            `json:"active_key_id"`
		Keys        map[string]string `json:"keys"`
	}
	if decodeStrict(raw, &file) != nil || file.Schema != "fugue.managed-ssh-signing/v1" || !safeToken(file.ActiveKeyID) || len(file.Keys) == 0 || len(file.Keys) > 16 {
		return ConfigurationSigner{}, ErrConfiguration
	}
	signer := ConfigurationSigner{KeyID: file.ActiveKeyID, PublicKeys: map[string]ed25519.PublicKey{}}
	for keyID, encoded := range file.Keys {
		seed, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(seed) != ed25519.SeedSize || !safeToken(keyID) {
			clear(seed)
			return ConfigurationSigner{}, ErrConfiguration
		}
		privateKey := ed25519.NewKeyFromSeed(seed)
		clear(seed)
		signer.PublicKeys[keyID] = append(ed25519.PublicKey(nil), privateKey.Public().(ed25519.PublicKey)...)
		if keyID == signer.KeyID {
			signer.PrivateKey = privateKey
		} else {
			clear(privateKey)
		}
	}
	if len(signer.PrivateKey) != ed25519.PrivateKeySize {
		return ConfigurationSigner{}, ErrConfiguration
	}
	return signer, nil
}
