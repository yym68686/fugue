package sshmaintenance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"fugue/internal/sshidentity"
)

func Decode(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return ErrIntegrity
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return ErrIntegrity
	}
	return nil
}

func ReadPrivate(path string, maximum int64) ([]byte, error) {
	metadata, err := os.Lstat(path)
	if err != nil || !metadata.Mode().IsRegular() || metadata.Mode().Perm()&0o077 != 0 || metadata.Size() > maximum {
		return nil, errors.New("maintenance file must be bounded, private, and regular")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("maintenance file is unavailable")
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(metadata, opened) || opened.Mode().Perm()&0o077 != 0 {
		return nil, ErrPrecondition
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(raw)) > maximum {
		return nil, ErrIntegrity
	}
	return raw, nil
}

func LoadMaterial(path string) (sshidentity.Keyring, string, error) {
	raw, err := ReadPrivate(path, 65536)
	if err != nil {
		return sshidentity.Keyring{}, "", err
	}
	defer clear(raw)
	keyring, err := sshidentity.LoadKeyring(path)
	if err != nil {
		return keyring, "", err
	}
	after, err := ReadPrivate(path, 65536)
	defer clear(after)
	if err != nil || !bytes.Equal(raw, after) {
		return sshidentity.Keyring{}, "", ErrPrecondition
	}
	sum := sha256.Sum256(raw)
	return keyring, "sha256:" + hex.EncodeToString(sum[:]), nil
}

func WritePrivate(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return ErrIntegrity
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.New("maintenance output must be a new private file")
	}
	if _, err := file.Write(append(raw, '\n')); err != nil {
		file.Close()
		return errors.New("maintenance output write failed")
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return errors.New("maintenance output persistence failed")
	}
	if err := file.Close(); err != nil {
		return errors.New("maintenance output close failed")
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("maintenance output directory unavailable")
	}
	defer directory.Close()
	return directory.Sync()
}
