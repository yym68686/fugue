package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"

	"fugue/internal/model"
	"fugue/internal/sshidentity"
)

func (stateStore *Store) withSSHIdentityFile(write bool, callback func(*[]model.AppSSHIdentityRecord) error) error {
	path := stateStore.path + ".ssh-identities.json"
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	var state struct {
		Schema  string                       `json:"schema"`
		Records []model.AppSSHIdentityRecord `json:"records"`
	}
	state.Schema = "fugue.ssh-identities/v1"
	raw, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		if json.Unmarshal(raw, &state) != nil || state.Schema != "fugue.ssh-identities/v1" || state.Records == nil {
			return sshidentity.ErrIntegrity
		}
		seen := make(map[string]struct{}, len(state.Records))
		for _, record := range state.Records {
			if _, exists := seen[record.Identity.AppID]; exists {
				return sshidentity.ErrIntegrity
			}
			seen[record.Identity.AppID] = struct{}{}
			if err := validateStoredSSHIdentity(record, record.Identity.TenantID, record.Identity.AppID); err != nil {
				return err
			}
		}
	}
	anchors := map[string]model.AppSSHIdentity{}
	anchorRaw, anchorErr := os.ReadFile(path + ".anchors")
	if anchorErr != nil && !errors.Is(anchorErr, os.ErrNotExist) {
		return anchorErr
	}
	if anchorErr == nil && (json.Unmarshal(anchorRaw, &anchors) != nil || anchors == nil) {
		return sshidentity.ErrIntegrity
	}
	if len(anchors) != len(state.Records) {
		return sshidentity.ErrIntegrity
	}
	for _, record := range state.Records {
		if !reflect.DeepEqual(anchors[record.Identity.AppID], record.Identity) {
			return sshidentity.ErrIntegrity
		}
	}
	if err := callback(&state.Records); err != nil {
		return err
	}
	if !write {
		return nil
	}
	for _, record := range state.Records {
		if previous, exists := anchors[record.Identity.AppID]; exists && !reflect.DeepEqual(previous, record.Identity) {
			return sshidentity.ErrIntegrity
		}
		anchors[record.Identity.AppID] = record.Identity
	}
	encodedAnchors, err := json.Marshal(anchors)
	if err != nil {
		return err
	}
	if !bytes.Equal(encodedAnchors, anchorRaw) {
		if err := writeSSHIdentityFile(path+".anchors", encodedAnchors); err != nil {
			return err
		}
	}
	encoded, err := json.Marshal(state)
	if err != nil || bytes.Equal(encoded, raw) {
		return err
	}
	return writeSSHIdentityFile(path, encoded)
}

func writeSSHIdentityFile(path string, encoded []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".ssh-identities-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	defer temporary.Close()
	if _, err := temporary.Write(encoded); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary.Name(), path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
