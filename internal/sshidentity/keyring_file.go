package sshidentity

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
)

func LoadKeyring(path string) (Keyring, error) {
	file, err := os.Open(path)
	if err != nil {
		return Keyring{}, ErrKeyUnavailable
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 65536 || info.Mode().Perm()&0o077 != 0 {
		return Keyring{}, ErrKeyUnavailable
	}
	var configuration struct {
		Schema      string            `json:"schema"`
		ActiveKeyID string            `json:"active_key_id"`
		Keys        map[string]string `json:"keys"`
	}
	decoder := json.NewDecoder(io.LimitReader(file, 65537))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&configuration); err != nil || configuration.Schema != "fugue.ssh-identity-keyring/v1" {
		return Keyring{}, ErrKeyUnavailable
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Keyring{}, ErrKeyUnavailable
	}
	keys := make(map[string][]byte, len(configuration.Keys))
	for keyID, encoded := range configuration.Keys {
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return Keyring{}, ErrKeyUnavailable
		}
		keys[keyID] = decoded
	}
	keyring, err := NewKeyring(configuration.ActiveKeyID, keys)
	if err != nil {
		return Keyring{}, ErrKeyUnavailable
	}
	return keyring, nil
}
