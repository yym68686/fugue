package inputarchive

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
)

// PendingRootAuthority is supplied by trusted transport authentication, never
// decoded from the pending-root body. A native authorization lease must match
// every field; Kubernetes preflight alone does not grant a database mutation.
type PendingRootAuthority struct {
	Owner, Namespace, ServiceAccount, ServiceAccountUID    string
	Deployment, DeploymentUID, PodUID, SourceSHA, ImageRef string
	PolicyRevision                                         string
}

var pendingAuthoritySource = regexp.MustCompile(`^[0-9a-f]{40}$`)
var pendingAuthorityImage = regexp.MustCompile(`^[a-z0-9./:_-]+@sha256:[0-9a-f]{64}$`)

func (a PendingRootAuthority) Valid() bool {
	for _, value := range []string{a.Owner, a.Namespace, a.ServiceAccount, a.ServiceAccountUID, a.Deployment, a.DeploymentUID, a.PodUID, a.PolicyRevision} {
		if !pendingRootText(value, 256) {
			return false
		}
	}
	return pendingAuthoritySource.MatchString(a.SourceSHA) && len(a.ImageRef) <= 512 && pendingAuthorityImage.MatchString(a.ImageRef)
}

// The independent source verifier may expose transport authority only after
// verifying its principal. The Store still checks the native policy itself.
type PendingRootAuthorizedSourceWitness interface {
	PendingRootSourceWitness
	PendingRootAuthority() PendingRootAuthority
}

// This is the complete bounded authorization registry, not a claim of complete
// external retention custody or H. Removing/expiring a lease removes permission
// only; it never removes an already registered retention root.
type PendingRootAuthorityLease struct {
	Authority   PendingRootAuthority `json:"authority"`
	LeaseID     string               `json:"lease_id"`
	ExpiresUnix int64                `json:"expires_unix"`
}

const PendingRootAuthorityPolicySchema = "fugue.compiler-input.pending-root-authority-policy.v1"
const PendingRootAuthorityPolicyBytes = 64 << 10

type PendingRootAuthorityPolicy struct {
	Schema string                      `json:"schema"`
	Leases []PendingRootAuthorityLease `json:"leases"`
}

func (p PendingRootAuthorityPolicy) Canonical() ([]byte, error) {
	if p.Schema != PendingRootAuthorityPolicySchema || len(p.Leases) > 64 || p.Leases == nil {
		return nil, ErrPendingRoot
	}
	previous := ""
	for _, lease := range p.Leases {
		key := lease.Authority.Owner + "\x00" + lease.Authority.PodUID
		if !lease.Authority.Valid() || !pendingRootHash.MatchString(lease.LeaseID) || lease.ExpiresUnix <= 0 || key <= previous || strings.ContainsRune(lease.Authority.Owner, '\x00') {
			return nil, ErrPendingRoot
		}
		previous = key
	}
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > PendingRootAuthorityPolicyBytes {
		return nil, ErrPendingRoot
	}
	return raw, nil
}

func DecodePendingRootAuthorityPolicy(raw []byte) (PendingRootAuthorityPolicy, error) {
	var p PendingRootAuthorityPolicy
	if len(raw) > PendingRootAuthorityPolicyBytes || json.Unmarshal(raw, &p) != nil {
		return p, ErrPendingRoot
	}
	canonical, err := p.Canonical()
	if err != nil || !bytes.Equal(raw, canonical) {
		return p, ErrPendingRoot
	}
	return p, nil
}
