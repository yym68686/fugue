package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"fugue/internal/releaseguardian"
)

// materializeAuthorityPublicKey derives the verifier from the existing
// read-only candidate-import credential. It writes only the public Ed25519 key
// into the Guardian's memory-backed /tmp and never persists or logs the secret.
func materializeAuthorityPublicKeys(spec string) error {
	seen := map[string]bool{}
	for _, entry := range strings.Split(strings.TrimSpace(spec), ";") {
		fields := strings.Split(strings.TrimSpace(entry), ",")
		if len(fields) != 2 {
			return errors.New("authority public key source must be input-file,output-file")
		}
		output := strings.TrimSpace(fields[1])
		if seen[output] {
			return errors.New("authority public key output is duplicated")
		}
		seen[output] = true
		if err := materializeAuthorityPublicKey(entry); err != nil {
			return err
		}
	}
	if len(seen) == 0 {
		return errors.New("authority public key source is empty")
	}
	return nil
}

func materializeAuthorityPublicKey(spec string) error {
	fields := strings.Split(strings.TrimSpace(spec), ",")
	if len(fields) != 2 {
		return errors.New("authority public key source must be input-file,output-file")
	}
	input, output := filepath.Clean(strings.TrimSpace(fields[0])), filepath.Clean(strings.TrimSpace(fields[1]))
	if !filepath.IsAbs(input) || !filepath.IsAbs(output) || input != strings.TrimSpace(fields[0]) || output != strings.TrimSpace(fields[1]) ||
		filepath.Dir(output) != "/tmp" || filepath.Base(output) == "." {
		return errors.New("authority public key source path is invalid")
	}
	material, err := os.ReadFile(input)
	if err != nil || len(material) < 32 || len(material) > 4096 {
		return errors.New("authority public key source is unavailable")
	}
	defer zeroSecret(material)
	publicKey, err := releaseguardian.CandidateCanaryPublicKey(material)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp("/tmp", ".authority-public-key-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(publicKey); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, output)
}
