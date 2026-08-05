package edgegroupfront

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
	"regexp"
	"strings"
	"syscall"
	"time"
)

const (
	ActivationStateSchemaV1     = "edge-front-group-activation/v1"
	ActivationReceiptSchemaV1   = "edge-front-group-activation-receipt/v1"
	ActivationAuthority         = "edge-control"
	ActivationOperationInit     = "initialize"
	ActivationOperationPromote  = "promote"
	ActivationOperationRollback = "rollback"
)

var (
	activationGroupPattern  = regexp.MustCompile(`^edge-group-[a-z0-9]+(?:-[a-z0-9]+)*$`)
	activationCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
	activationDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	activationReasonPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 ._:/-]{7,255}$`)

	ErrActivationCASConflict = errors.New("edge front activation CAS conflict")
)

type ActivationState struct {
	Schema               string    `json:"schema"`
	GroupID              string    `json:"edge_group_id"`
	Generation           uint64    `json:"generation"`
	ActiveSlot           string    `json:"active_slot"`
	PreviousSlot         string    `json:"previous_slot,omitempty"`
	BundleGeneration     string    `json:"bundle_generation"`
	WorkerSourceCommit   string    `json:"worker_source_commit"`
	WorkerImageDigest    string    `json:"worker_image_digest"`
	Authority            string    `json:"authority"`
	Operation            string    `json:"operation"`
	RollbackOfGeneration uint64    `json:"rollback_of_generation,omitempty"`
	Reason               string    `json:"reason"`
	UpdatedAt            time.Time `json:"updated_at"`
}

type ActivationCASRequest struct {
	GroupID              string
	ExpectedGeneration   uint64
	ExpectedSlot         string
	TargetSlot           string
	BundleGeneration     string
	WorkerSourceCommit   string
	WorkerImageDigest    string
	Operation            string
	RollbackOfGeneration uint64
	Reason               string
}

type ActivationReceipt struct {
	Schema         string           `json:"schema"`
	GroupID        string           `json:"edge_group_id"`
	PreviousExists bool             `json:"previous_exists"`
	Previous       *ActivationState `json:"previous,omitempty"`
	Current        ActivationState  `json:"current"`
	StateDigest    string           `json:"state_digest"`
}

func ApplyActivationCAS(path string, request ActivationCASRequest, now time.Time) (ActivationReceipt, error) {
	if err := validateActivationPath(path); err != nil {
		return ActivationReceipt{}, err
	}
	if err := validateActivationRequest(request); err != nil {
		return ActivationReceipt{}, err
	}
	if now.IsZero() {
		return ActivationReceipt{}, errors.New("edge front activation timestamp is required")
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return ActivationReceipt{}, fmt.Errorf("open edge front activation lock: %w", err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return ActivationReceipt{}, fmt.Errorf("lock edge front activation: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck

	previous, exists, err := ReadActivationState(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return ActivationReceipt{}, err
	}
	if !exists {
		if request.Operation != ActivationOperationInit || request.ExpectedGeneration != 0 || normalizeSlot(request.ExpectedSlot) != normalizeSlot(request.TargetSlot) {
			return ActivationReceipt{}, ErrActivationCASConflict
		}
	} else {
		if previous.GroupID != request.GroupID || previous.Generation != request.ExpectedGeneration || previous.ActiveSlot != normalizeSlot(request.ExpectedSlot) || request.Operation == ActivationOperationInit {
			return ActivationReceipt{}, ErrActivationCASConflict
		}
		if normalizeSlot(request.TargetSlot) == previous.ActiveSlot {
			return ActivationReceipt{}, errors.New("edge front activation target slot is already active")
		}
		if request.Operation == ActivationOperationRollback && (request.RollbackOfGeneration == 0 || request.RollbackOfGeneration != previous.Generation) {
			return ActivationReceipt{}, errors.New("edge front rollback must bind the exact current generation")
		}
	}

	nextGeneration := uint64(1)
	previousSlot := ""
	if exists {
		nextGeneration = previous.Generation + 1
		previousSlot = previous.ActiveSlot
	}
	next := ActivationState{
		Schema: ActivationStateSchemaV1, GroupID: request.GroupID, Generation: nextGeneration,
		ActiveSlot: normalizeSlot(request.TargetSlot), PreviousSlot: previousSlot,
		BundleGeneration: strings.TrimSpace(request.BundleGeneration), WorkerSourceCommit: request.WorkerSourceCommit,
		WorkerImageDigest: request.WorkerImageDigest, Authority: ActivationAuthority, Operation: request.Operation,
		RollbackOfGeneration: request.RollbackOfGeneration, Reason: request.Reason, UpdatedAt: now.UTC(),
	}
	if err := validateActivationState(next); err != nil {
		return ActivationReceipt{}, err
	}
	encoded, err := json.Marshal(next)
	if err != nil {
		return ActivationReceipt{}, err
	}
	encoded = append(encoded, '\n')
	if err := replaceActivationState(path, encoded); err != nil {
		return ActivationReceipt{}, err
	}
	digest := sha256.Sum256(encoded)
	receipt := ActivationReceipt{
		Schema: ActivationReceiptSchemaV1, GroupID: request.GroupID, PreviousExists: exists,
		Current: next, StateDigest: "sha256:" + hex.EncodeToString(digest[:]),
	}
	if exists {
		copy := previous
		receipt.Previous = &copy
	}
	return receipt, nil
}

func ReadActivationState(path string) (ActivationState, bool, error) {
	if err := validateActivationPath(path); err != nil {
		return ActivationState{}, false, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return ActivationState{}, false, err
	}
	if len(raw) == 0 || len(raw) > 64<<10 {
		return ActivationState{}, false, errors.New("edge front activation state size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var state ActivationState
	if err := decoder.Decode(&state); err != nil {
		return ActivationState{}, false, errors.New("edge front activation state is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ActivationState{}, false, errors.New("edge front activation state has trailing data")
	}
	if err := validateActivationState(state); err != nil {
		return ActivationState{}, false, err
	}
	return state, true, nil
}

func validateActivationRequest(request ActivationCASRequest) error {
	request.ExpectedSlot = normalizeSlot(request.ExpectedSlot)
	request.TargetSlot = normalizeSlot(request.TargetSlot)
	if !activationGroupPattern.MatchString(request.GroupID) || request.ExpectedSlot == "" || request.TargetSlot == "" ||
		strings.TrimSpace(request.BundleGeneration) == "" || request.BundleGeneration != strings.TrimSpace(request.BundleGeneration) ||
		!activationCommitPattern.MatchString(request.WorkerSourceCommit) || !activationDigestPattern.MatchString(request.WorkerImageDigest) ||
		!activationReasonPattern.MatchString(request.Reason) {
		return errors.New("edge front activation request is invalid")
	}
	switch request.Operation {
	case ActivationOperationInit, ActivationOperationPromote:
		if request.RollbackOfGeneration != 0 {
			return errors.New("non-rollback edge front activation cannot bind rollback generation")
		}
	case ActivationOperationRollback:
	default:
		return errors.New("edge front activation operation is invalid")
	}
	return nil
}

func validateActivationState(state ActivationState) error {
	if state.Schema != ActivationStateSchemaV1 || !activationGroupPattern.MatchString(state.GroupID) || state.Generation == 0 || normalizeSlot(state.ActiveSlot) == "" ||
		state.ActiveSlot != normalizeSlot(state.ActiveSlot) || (state.PreviousSlot != "" && state.PreviousSlot != normalizeSlot(state.PreviousSlot)) ||
		strings.TrimSpace(state.BundleGeneration) == "" || state.BundleGeneration != strings.TrimSpace(state.BundleGeneration) ||
		!activationCommitPattern.MatchString(state.WorkerSourceCommit) || !activationDigestPattern.MatchString(state.WorkerImageDigest) ||
		state.Authority != ActivationAuthority || !activationReasonPattern.MatchString(state.Reason) || state.UpdatedAt.IsZero() {
		return errors.New("edge front activation state is invalid")
	}
	switch state.Operation {
	case ActivationOperationInit, ActivationOperationPromote:
		if state.RollbackOfGeneration != 0 {
			return errors.New("edge front activation state has invalid rollback binding")
		}
	case ActivationOperationRollback:
		if state.RollbackOfGeneration == 0 {
			return errors.New("edge front rollback state is not bound")
		}
	default:
		return errors.New("edge front activation state operation is invalid")
	}
	return nil
}

func validateActivationPath(path string) error {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Base(path) == "." {
		return errors.New("edge front activation state path must be absolute and normalized")
	}
	info, err := os.Stat(filepath.Dir(path))
	if err != nil || !info.IsDir() {
		return errors.New("edge front activation state directory is unavailable")
	}
	return nil
}

func replaceActivationState(path string, raw []byte) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".edge-front-activation-*")
	if err != nil {
		return fmt.Errorf("create edge front activation temp file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(raw); err != nil {
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
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace edge front activation state: %w", err)
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
