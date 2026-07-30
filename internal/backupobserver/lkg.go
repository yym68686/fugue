package backupobserver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/backupcontrol"
)

const (
	LKGStateDisabled      = "disabled"
	LKGStateMemoryOnly    = "memory-only"
	LKGStateAbsent        = "absent"
	LKGStateCurrent       = "current"
	LKGStatePrevious      = "previous"
	LKGStateInvalid       = "invalid"
	LKGStatePersistFailed = "persist-failed"

	lkgAPIVersion = "backup-observer.fugue.dev/v1"
	lkgKind       = "BackupObserverLastKnownGood"
	maxLKGBytes   = int64(128 << 10)
)

var ErrLKG = errors.New("backup observer LKG is invalid")

// persistedLKG binds the locally recovered observation to the exact desired
// spec that validated it. It deliberately contains no bearer token, backend
// configuration, object location, or remote error body.
type persistedLKG struct {
	APIVersion  string                        `json:"apiVersion"`
	Kind        string                        `json:"kind"`
	CellKey     string                        `json:"cellKey"`
	Spec        backupcontrol.BackupRunSpec   `json:"spec"`
	Status      backupcontrol.BackupRunStatus `json:"status"`
	PersistedAt time.Time                     `json:"persistedAt"`
	Digest      string                        `json:"digest"`
}

type lkgRestore struct {
	Spec   backupcontrol.BackupRunSpec
	Status backupcontrol.BackupRunStatus
	State  string
	Err    error
}

func restoreBackupObserverLKG(path, expectedCellKey string) lkgRestore {
	if path == "" {
		return lkgRestore{State: LKGStateMemoryOnly}
	}
	current, currentErr := readPersistedLKG(path, expectedCellKey)
	if currentErr == nil {
		return lkgRestore{Spec: current.Spec, Status: current.Status, State: LKGStateCurrent}
	}
	previous, previousErr := readPersistedLKG(previousLKGPath(path), expectedCellKey)
	if previousErr == nil {
		return lkgRestore{
			Spec: previous.Spec, Status: previous.Status, State: LKGStatePrevious,
			Err: fmt.Errorf("%w: current generation unavailable", ErrLKG),
		}
	}
	if errors.Is(currentErr, os.ErrNotExist) && errors.Is(previousErr, os.ErrNotExist) {
		return lkgRestore{State: LKGStateAbsent}
	}
	return lkgRestore{State: LKGStateInvalid, Err: errors.Join(currentErr, previousErr)}
}

func persistBackupObserverLKG(
	path string,
	expectedCellKey string,
	spec backupcontrol.BackupRunSpec,
	status backupcontrol.BackupRunStatus,
	persistedAt time.Time,
) error {
	if path == "" {
		return nil
	}
	envelope, err := newPersistedLKG(expectedCellKey, spec, status, persistedAt)
	if err != nil {
		return err
	}
	document, err := encodePersistedLKG(envelope)
	if err != nil {
		return err
	}

	// Preserve only a fully validated current generation. Invalid content is
	// never promoted to previous, while unsafe filesystem topology blocks the
	// update instead of being hidden by a rename.
	currentDocument, readErr := readPrivateLKGFile(path)
	if readErr == nil {
		if _, decodeErr := decodePersistedLKG(currentDocument, expectedCellKey); decodeErr == nil {
			if err := writePrivateLKGFile(previousLKGPath(path), currentDocument); err != nil {
				return fmt.Errorf("%w: preserve previous generation: %v", ErrLKG, err)
			}
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return fmt.Errorf("%w: inspect current generation: %v", ErrLKG, readErr)
	}
	if err := writePrivateLKGFile(path, document); err != nil {
		return fmt.Errorf("%w: publish current generation: %v", ErrLKG, err)
	}
	return nil
}

func newPersistedLKG(
	expectedCellKey string,
	spec backupcontrol.BackupRunSpec,
	status backupcontrol.BackupRunStatus,
	persistedAt time.Time,
) (persistedLKG, error) {
	persistedAt = persistedAt.UTC().Truncate(time.Second)
	envelope := persistedLKG{
		APIVersion:  lkgAPIVersion,
		Kind:        lkgKind,
		CellKey:     strings.TrimSpace(expectedCellKey),
		Spec:        spec,
		Status:      status,
		PersistedAt: persistedAt,
	}
	envelope.Digest = digestPersistedLKG(envelope)
	if err := validatePersistedLKG(envelope, expectedCellKey); err != nil {
		return persistedLKG{}, err
	}
	return envelope, nil
}

func readPersistedLKG(path, expectedCellKey string) (persistedLKG, error) {
	document, err := readPrivateLKGFile(path)
	if err != nil {
		return persistedLKG{}, err
	}
	return decodePersistedLKG(document, expectedCellKey)
}

func decodePersistedLKG(document []byte, expectedCellKey string) (persistedLKG, error) {
	if len(document) == 0 || int64(len(document)) > maxLKGBytes {
		return persistedLKG{}, fmt.Errorf("%w: document size", ErrLKG)
	}
	var envelope persistedLKG
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return persistedLKG{}, fmt.Errorf("%w: decode document", ErrLKG)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return persistedLKG{}, fmt.Errorf("%w: trailing document", ErrLKG)
	}
	if err := validatePersistedLKG(envelope, expectedCellKey); err != nil {
		return persistedLKG{}, err
	}
	return envelope, nil
}

func validatePersistedLKG(envelope persistedLKG, expectedCellKey string) error {
	if envelope.APIVersion != lkgAPIVersion || envelope.Kind != lkgKind ||
		envelope.CellKey != strings.TrimSpace(expectedCellKey) || envelope.Spec.CellKey != envelope.CellKey ||
		envelope.Status.CellKey != envelope.CellKey || envelope.Digest != digestPersistedLKG(envelope) ||
		envelope.PersistedAt.IsZero() || envelope.PersistedAt.Location() != time.UTC || envelope.PersistedAt.Nanosecond() != 0 ||
		envelope.Status.ObservedAt.After(envelope.PersistedAt.Add(maxObservationFutureSkew)) ||
		!envelope.PersistedAt.Before(envelope.Status.ValidUntil) ||
		backupcontrol.ValidateBackupRunSpec(envelope.Spec) != nil ||
		backupcontrol.ValidateBackupRunStatus(envelope.Spec, envelope.Status) != nil {
		return ErrLKG
	}
	return nil
}

func encodePersistedLKG(envelope persistedLKG) ([]byte, error) {
	document, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("%w: encode document", ErrLKG)
	}
	if int64(len(document)+1) > maxLKGBytes {
		return nil, fmt.Errorf("%w: encoded document size", ErrLKG)
	}
	return append(document, '\n'), nil
}

func digestPersistedLKG(envelope persistedLKG) string {
	envelope.Digest = ""
	document, err := json.Marshal(envelope)
	if err != nil {
		panic(fmt.Sprintf("marshal backup observer LKG: %v", err))
	}
	digest := sha256.Sum256(document)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func previousLKGPath(path string) string {
	if path == "" {
		return ""
	}
	return path + ".previous"
}

func readPrivateLKGFile(path string) ([]byte, error) {
	parentInfo, err := validateLKGPath(path)
	if err != nil {
		return nil, err
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !privateRegularLKG(before) {
		return nil, fmt.Errorf("%w: state file is not private and regular", ErrLKG)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) || !privateRegularLKG(opened) {
		return nil, fmt.Errorf("%w: state file identity changed while opening", ErrLKG)
	}
	document, err := io.ReadAll(io.LimitReader(file, maxLKGBytes+1))
	if err != nil || int64(len(document)) > maxLKGBytes {
		return nil, fmt.Errorf("%w: read bounded state file", ErrLKG)
	}
	afterParent, err := os.Lstat(filepath.Dir(path))
	if err != nil || !os.SameFile(parentInfo, afterParent) || !privateLKGParent(afterParent) {
		return nil, fmt.Errorf("%w: state parent identity changed while reading", ErrLKG)
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(opened, after) || !privateRegularLKG(after) {
		return nil, fmt.Errorf("%w: state file identity changed while reading", ErrLKG)
	}
	return document, nil
}

func writePrivateLKGFile(path string, document []byte) error {
	if len(document) == 0 || int64(len(document)) > maxLKGBytes {
		return fmt.Errorf("%w: write document size", ErrLKG)
	}
	parentInfo, err := validateLKGPath(path)
	if err != nil {
		return err
	}
	before, exists, err := inspectPrivateLKGDestination(path)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	failed := func(cause error) error {
		_ = temporary.Close()
		return cause
	}
	if err := temporary.Chmod(0o600); err != nil {
		return failed(err)
	}
	if _, err := temporary.Write(document); err != nil {
		return failed(err)
	}
	if err := temporary.Sync(); err != nil {
		return failed(err)
	}
	temporaryInfo, err := temporary.Stat()
	if err != nil || !privateRegularLKG(temporaryInfo) {
		return failed(fmt.Errorf("%w: temporary state file is unsafe", ErrLKG))
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	afterParent, err := os.Lstat(filepath.Dir(path))
	if err != nil || !os.SameFile(parentInfo, afterParent) || !privateLKGParent(afterParent) {
		return fmt.Errorf("%w: state parent identity changed before publication", ErrLKG)
	}
	current, currentExists, err := inspectPrivateLKGDestination(path)
	if err != nil || currentExists != exists || (exists && !os.SameFile(before, current)) {
		return fmt.Errorf("%w: state destination identity changed before publication", ErrLKG)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	published, err := os.Lstat(path)
	if err != nil || !privateRegularLKG(published) {
		return fmt.Errorf("%w: published state file is unsafe", ErrLKG)
	}
	return nil
}

func validateLKGPath(path string) (os.FileInfo, error) {
	if path == "" || strings.TrimSpace(path) != path || !filepath.IsAbs(path) || filepath.Clean(path) != path ||
		filepath.Base(path) == "." || filepath.Base(path) == ".." {
		return nil, fmt.Errorf("%w: path is not canonical", ErrLKG)
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if !privateLKGParent(parent) {
		return nil, fmt.Errorf("%w: state parent is not a private non-symlink directory", ErrLKG)
	}
	return parent, nil
}

func inspectPrivateLKGDestination(path string) (os.FileInfo, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !privateRegularLKG(info) {
		return nil, false, fmt.Errorf("%w: state destination is not private and regular", ErrLKG)
	}
	return info, true, nil
}

func privateLKGParent(info os.FileInfo) bool {
	return info != nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm()&0o002 == 0
}

func privateRegularLKG(info os.FileInfo) bool {
	return info != nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm() == 0o600
}
