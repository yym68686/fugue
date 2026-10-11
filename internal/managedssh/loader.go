package managedssh

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/crypto/ssh"
)

type Loader struct {
	Server         *Server
	ArtifactPath   string
	TrustPath      string
	PrivateKeyPath string
	LKGPath        string
}

func (loader Loader) Load(ctx context.Context, restore bool) error {
	if loader.Server == nil || loader.LKGPath == "" || loader.ArtifactPath == loader.LKGPath {
		return ErrConfiguration
	}
	keys, err := readTrust(loader.TrustPath)
	if err != nil {
		return err
	}
	candidatePath := loader.ArtifactPath
	if restore {
		candidatePath = loader.LKGPath
	}
	artifact, err := readArtifact(candidatePath)
	if err != nil {
		return err
	}
	configuration, err := VerifyArtifact(artifact, keys)
	if err != nil {
		return err
	}
	private, err := privateFile(loader.PrivateKeyPath, 16384)
	if err != nil {
		return err
	}
	signer, err := ssh.ParsePrivateKey(private)
	if err != nil {
		return ErrConfiguration
	}
	if loader.Server.ConfigurationDigest() == "" && !restore {
		previous, previousErr := readArtifact(loader.LKGPath)
		if previousErr == nil {
			verified, verifyErr := VerifyArtifact(previous, keys)
			if verifyErr != nil {
				return verifyErr
			}
			if configuration.Identity.ID != verified.Identity.ID || configuration.Identity.Fingerprint != verified.Identity.Fingerprint {
				return ErrIdentityChange
			}
			if configuration.Generation < verified.Generation || (configuration.Generation == verified.Generation && artifact.Digest != previous.Digest) {
				return ErrGeneration
			}
		} else if !errors.Is(previousErr, os.ErrNotExist) {
			return previousErr
		}
	}
	persist := func() error {
		if restore {
			return nil
		}
		raw, err := json.Marshal(artifact)
		if err != nil {
			return err
		}
		return writePrivateAtomic(loader.LKGPath, raw)
	}
	return loader.Server.activate(ctx, configuration, signer, persist)
}

func readArtifact(path string) (Artifact, error) {
	var artifact Artifact
	raw, err := privateFile(path, 1024*1024)
	if err != nil {
		return artifact, err
	}
	if err := decodeStrict(raw, &artifact); err != nil {
		return artifact, ErrArtifact
	}
	return artifact, nil
}

func readTrust(path string) (map[string]ed25519.PublicKey, error) {
	raw, err := privateFile(path, 65536)
	if err != nil {
		return nil, err
	}
	var trust struct {
		Schema string            `json:"schema"`
		Keys   map[string]string `json:"keys"`
	}
	if decodeStrict(raw, &trust) != nil || trust.Schema != "fugue.managed-ssh-trust/v1" || len(trust.Keys) == 0 {
		return nil, ErrArtifact
	}
	keys := make(map[string]ed25519.PublicKey, len(trust.Keys))
	for keyID, value := range trust.Keys {
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil || len(decoded) != ed25519.PublicKeySize || !safeToken(keyID) {
			return nil, ErrArtifact
		}
		keys[keyID] = ed25519.PublicKey(decoded)
	}
	return keys, nil
}

func decodeStrict(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrConfiguration
	}
	return nil
}

func privateFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > limit {
		return nil, ErrConfiguration
	}
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, ErrConfiguration
	}
	return raw, nil
}

func writePrivateAtomic(path string, raw []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".managed-ssh-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.Write(raw); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
