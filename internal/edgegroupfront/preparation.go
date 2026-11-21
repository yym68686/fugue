package edgegroupfront

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"syscall"
	"time"
)

const PublicationPreparationSchema = "edge-worker-publication-preparation/v1"
const PublicationPreparationLifetime = 90 * time.Second

// PublicationPreparation permits a specific inactive code instance to fetch
// the signed current publication before Front activation. It grants no traffic,
// publishes no routes, and never extends the publication's own lease.
type PublicationPreparation struct {
	Schema             string          `json:"schema"`
	Previous           ActivationState `json:"previous"`
	TargetSlot         string          `json:"target_slot"`
	BundleGeneration   string          `json:"bundle_generation"`
	WorkerSourceCommit string          `json:"worker_source_commit"`
	WorkerImageDigest  string          `json:"worker_image_digest"`
	CreatedAt          time.Time       `json:"created_at"`
	ExpiresAt          time.Time       `json:"expires_at"`
}

func (p PublicationPreparation) Matches(current ActivationState, slot, source string, now time.Time) bool {
	return p.Schema == PublicationPreparationSchema && validateActivationState(p.Previous) == nil &&
		p.Previous == current && p.TargetSlot != "" && p.TargetSlot == normalizeSlot(slot) && p.TargetSlot != current.ActiveSlot &&
		p.WorkerSourceCommit == source && activationCommitPattern.MatchString(source) &&
		activationDigestPattern.MatchString(p.WorkerImageDigest) && p.BundleGeneration != "" &&
		!p.CreatedAt.IsZero() && !p.CreatedAt.Before(current.UpdatedAt) && !p.CreatedAt.After(now) && p.ExpiresAt.After(now) &&
		p.ExpiresAt.Sub(p.CreatedAt) == PublicationPreparationLifetime
}

// PreparePublication uses the same lock and predecessor as activation CAS but
// writes a separate expiring record. Replaying the same request keeps its
// original deadline. A different unexpired preparation fails closed.
func PreparePublication(path string, request ActivationCASRequest, now time.Time) (PublicationPreparation, error) {
	if err := validateActivationPath(path); err != nil {
		return PublicationPreparation{}, err
	}
	if request.Operation != ActivationOperationPromote || now.IsZero() {
		return PublicationPreparation{}, errors.New("publication preparation requires promotion")
	}
	if err := validateActivationRequest(request); err != nil {
		return PublicationPreparation{}, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return PublicationPreparation{}, err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return PublicationPreparation{}, err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck
	current, exists, err := ReadActivationState(path)
	if err != nil || !exists || current.GroupID != request.GroupID || current.Generation != request.ExpectedGeneration || current.Generation == ^uint64(0) || now.Before(current.UpdatedAt) || current.ActiveSlot != normalizeSlot(request.ExpectedSlot) || current.ActiveSlot == normalizeSlot(request.TargetSlot) {
		return PublicationPreparation{}, ErrActivationCASConflict
	}
	p := PublicationPreparation{Schema: PublicationPreparationSchema, Previous: current,
		TargetSlot: normalizeSlot(request.TargetSlot), BundleGeneration: request.BundleGeneration,
		WorkerSourceCommit: request.WorkerSourceCommit, WorkerImageDigest: request.WorkerImageDigest,
		CreatedAt: now.UTC(), ExpiresAt: now.UTC().Add(PublicationPreparationLifetime)}
	if prior, found, readErr := ReadPublicationPreparation(path); readErr != nil {
		return PublicationPreparation{}, readErr
	} else if found && prior.Matches(current, prior.TargetSlot, prior.WorkerSourceCommit, now) {
		if prior.TargetSlot != p.TargetSlot || prior.BundleGeneration != p.BundleGeneration || prior.WorkerSourceCommit != p.WorkerSourceCommit || prior.WorkerImageDigest != p.WorkerImageDigest {
			return PublicationPreparation{}, ErrActivationCASConflict
		}
		return prior, nil
	}
	raw, err := json.Marshal(p)
	if err == nil {
		err = replaceActivationState(path+".prepared", append(raw, '\n'))
	}
	return p, err
}

func ReadPublicationPreparation(path string) (PublicationPreparation, bool, error) {
	if err := validateActivationPath(path); err != nil {
		return PublicationPreparation{}, false, err
	}
	info, err := os.Lstat(path + ".prepared")
	if errors.Is(err, os.ErrNotExist) {
		return PublicationPreparation{}, false, nil
	}
	if err != nil {
		return PublicationPreparation{}, false, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() <= 0 || info.Size() > 16<<10 {
		return PublicationPreparation{}, false, errors.New("publication preparation file is invalid")
	}
	raw, err := os.ReadFile(path + ".prepared")
	if err != nil || len(raw) > 16<<10 {
		return PublicationPreparation{}, false, errors.New("publication preparation is unavailable")
	}
	var p PublicationPreparation
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil || d.Decode(&struct{}{}) != io.EOF || p.Schema != PublicationPreparationSchema {
		return PublicationPreparation{}, false, errors.New("publication preparation payload is invalid")
	}
	return p, true, nil
}
