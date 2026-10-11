package model

import (
	"errors"
	"strings"
	"time"
)

const MachineEdgeEnrollmentLabel = "fugue.io/edge-enrollment-intent"

// EdgeNodeEnrollment is configuration intent, never a runtime health claim.
// Reading it cannot renew its deadline or initialize public serving state.
type EdgeNodeEnrollment struct {
	ID                    string    `json:"id"`
	NodeID                string    `json:"node_id"`
	EdgeGroupID           string    `json:"edge_group_id"`
	AuthorityRecordDigest string    `json:"authority_record_digest"`
	Slot                  string    `json:"slot"`
	FenceSequence         uint64    `json:"fence_sequence"`
	BundleVersion         string    `json:"bundle_version"`
	WorkerSourceSHA       string    `json:"worker_source_sha"`
	WorkerImageDigest     string    `json:"worker_image_digest"`
	IssuedAt              time.Time `json:"issued_at"`
	ExpiresAt             time.Time `json:"expires_at"`
}

func (e EdgeNodeEnrollment) Validate(node, group string, now time.Time) error {
	hex := func(s string, size int) bool { return len(s) == size && strings.Trim(s, "0123456789abcdef") == "" }
	digest := func(s string) bool {
		return strings.HasPrefix(s, "sha256:") && hex(strings.TrimPrefix(s, "sha256:"), 64)
	}
	if e.ID == "" || len(e.ID) > 128 || e.NodeID == "" || e.NodeID != node || e.EdgeGroupID != group || (group == "" || len(group) > 128 || strings.TrimSpace(group) != group) || (e.Slot != EdgeSlotA && e.Slot != EdgeSlotB) || e.FenceSequence == 0 || e.FenceSequence == ^uint64(0) || !digest(e.AuthorityRecordDigest) || !digest(e.WorkerImageDigest) || !hex(e.WorkerSourceSHA, 40) || e.BundleVersion == "" || len(e.BundleVersion) > 512 || strings.TrimSpace(e.BundleVersion) != e.BundleVersion {
		return errors.New("Edge enrollment identity or authority binding is invalid")
	}
	if e.IssuedAt.IsZero() || e.IssuedAt.After(now.Add(30*time.Second)) || !e.ExpiresAt.After(now) || !e.ExpiresAt.After(e.IssuedAt) || e.ExpiresAt.Sub(e.IssuedAt) > 15*time.Minute {
		return errors.New("Edge enrollment lease is expired or invalid; rerun the enrollment command")
	}
	return nil
}
