// Package productioncontrol stores and verifies Fugue-native control intents.
// Recording an intent or an approval does not grant access to a provider.
package productioncontrol

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	MaxPayloadBytes = 2 << 20
	MaxApprovalTTL  = 24 * time.Hour

	intentDomain   = "fugue.production.intent.v1"
	approvalDomain = "fugue.production.approval.v1"
)

var (
	tokenPattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,95}$`)
	resourcePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]{0,159}$`)
	digestPattern   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type Operation string

const (
	ComponentRelease      Operation = "component_release"
	NodeParticipation     Operation = "node_participation"
	DNSMembership         Operation = "dns_membership"
	FrontWorker           Operation = "front_worker_handoff"
	Maintenance           Operation = "maintenance"
	ParticipationTopology Operation = "participation_topology"
)

type Intent struct {
	ClusterID      string    `json:"clusterId"`
	ResourceID     string    `json:"resourceId"`
	RequestID      string    `json:"requestId"`
	Operation      Operation `json:"operation"`
	Generation     int64     `json:"expectedGeneration"`
	CurrentDigest  string    `json:"expectedCurrentDigest"`
	TargetDigest   string    `json:"targetDigest"`
	RollbackDigest string    `json:"rollbackDigest"`
	PayloadDigest  string    `json:"payloadDigest"`
}

// NewIntent binds exact payload bytes. The resource identity must refer to
// the actual shared write boundary, not a Git lane or a display name.
func NewIntent(cluster, resource, request string, operation Operation, generation int64, current, target, rollback string, payload []byte) (Intent, error) {
	intent := Intent{cluster, resource, request, operation, generation, current, target, rollback, digest(payload)}
	return intent, intent.VerifyPayload(payload)
}

func (intent Intent) Validate() error {
	if !tokenPattern.MatchString(intent.ClusterID) || !resourcePattern.MatchString(intent.ResourceID) ||
		!tokenPattern.MatchString(intent.RequestID) || intent.Generation < 1 ||
		!digestPattern.MatchString(intent.CurrentDigest) || !digestPattern.MatchString(intent.TargetDigest) ||
		!digestPattern.MatchString(intent.RollbackDigest) ||
		!digestPattern.MatchString(intent.PayloadDigest) {
		return errors.New("invalid production intent binding")
	}
	switch intent.Operation {
	case ComponentRelease, NodeParticipation, DNSMembership, FrontWorker, Maintenance, ParticipationTopology:
		return nil
	}
	return errors.New("unsupported production operation")
}

func (intent Intent) VerifyPayload(payload []byte) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > MaxPayloadBytes || digest(payload) != intent.PayloadDigest {
		return errors.New("production payload does not match the intent")
	}
	return nil
}

func (intent Intent) Digest() (string, error) {
	if err := intent.Validate(); err != nil {
		return "", err
	}
	encoded, err := signedBytes(intentDomain, intent)
	if err != nil {
		return "", err
	}
	return digest(encoded), nil
}

// Approval signs the intent, not a source revision, registry tag, or build
// receipt. Trusted keys must be loaded by the operator out of band; they are
// never read from the submitted intent or artifact.
type Approval struct {
	IntentDigest string `json:"intentDigest"`
	KeyID        string `json:"keyId"`
	IssuedAt     int64  `json:"issuedAtUnix"`
	ExpiresAt    int64  `json:"expiresAtUnix"`
	Signature    []byte `json:"signature"`
}

type TrustedApprover struct {
	ClusterID string
	PublicKey ed25519.PublicKey
}

type Approvers map[string]TrustedApprover

func SignApproval(intent Intent, keyID string, privateKey ed25519.PrivateKey, now time.Time, ttl time.Duration) (Approval, error) {
	if len(privateKey) != ed25519.PrivateKeySize || ttl < time.Second || ttl > MaxApprovalTTL {
		return Approval{}, errors.New("invalid production signing key or approval lifetime")
	}
	intentDigest, err := intent.Digest()
	if err != nil {
		return Approval{}, err
	}
	approval := Approval{IntentDigest: intentDigest, KeyID: keyID, IssuedAt: now.Unix(), ExpiresAt: now.Add(ttl).Unix()}
	encoded, err := approval.signingBytes()
	if err != nil {
		return Approval{}, err
	}
	approval.Signature = ed25519.Sign(privateKey, encoded)
	return approval, nil
}

func (approval Approval) Verify(intent Intent, trusted Approvers, now time.Time) error {
	intentDigest, err := intent.Digest()
	if err != nil || intentDigest != approval.IntentDigest {
		return errors.New("production approval does not match the intent")
	}
	raw, err := approval.signingBytes()
	if err != nil {
		return err
	}
	if at := now.Unix(); at < approval.IssuedAt || at >= approval.ExpiresAt {
		return errors.New("production approval is outside its validity window")
	}
	owner, found := trusted[approval.KeyID]
	if !found || owner.ClusterID != intent.ClusterID || len(owner.PublicKey) != ed25519.PublicKeySize ||
		len(approval.Signature) != ed25519.SignatureSize || !ed25519.Verify(owner.PublicKey, raw, approval.Signature) {
		return errors.New("production approval lacks a trusted cluster-owner signature")
	}
	return nil
}

func (approval Approval) signingBytes() ([]byte, error) {
	if !digestPattern.MatchString(approval.IntentDigest) || !tokenPattern.MatchString(approval.KeyID) ||
		approval.IssuedAt <= 0 || approval.ExpiresAt <= approval.IssuedAt ||
		approval.ExpiresAt-approval.IssuedAt > int64(MaxApprovalTTL/time.Second) {
		return nil, errors.New("invalid production approval binding")
	}
	approval.Signature = nil
	return signedBytes(approvalDomain, approval)
}

func signedBytes(domain string, value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append([]byte(domain+"\x00"), raw...), nil
}

func digest(value []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(value))
}
