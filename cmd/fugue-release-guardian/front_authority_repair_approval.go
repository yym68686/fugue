package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
)

const frontRepairApprovalEnvelopeSchema = "edge-front-repair-signature/v1"
const frontRepairApprovalSignatureDomain = "fugue-edge-front-authority-repair/v1\x00"

type frontRepairArtifactIdentity struct {
	RepositoryID  uint64 `json:"repositoryId"`
	WorkflowRunID uint64 `json:"workflowRunId"`
	ProducerJobID uint64 `json:"producerJobId"`
	RunAttempt    uint32 `json:"runAttempt"`
	ArtifactID    uint64 `json:"artifactId"`
	ArchiveDigest string `json:"archiveDigest"`
}

func (a frontRepairArtifactIdentity) valid() bool {
	return a.RepositoryID != 0 && a.WorkflowRunID != 0 && a.RunAttempt != 0 &&
		a.ProducerJobID != 0 && a.ArtifactID != 0 && exactSHA256Digest(a.ArchiveDigest)
}

type frontRepairApprovalEnvelope struct {
	Schema    string                      `json:"schema"`
	Scope     string                      `json:"scope"`
	Statement frontRepairApproval         `json:"statement"`
	Artifact  frontRepairArtifactIdentity `json:"artifact"`
	Signature string                      `json:"signature,omitempty"`
}

// Constructed only by the protected launcher: verify its checkout and the
// authenticated CI artifact metadata before using them as evidence. The public
// key and scope are pinned release configuration, never supplied by a preview,
// signature envelope, artifact, or fall-back local command-line option.
type frontRepairApprovalTrust struct {
	Scope          string
	PublicKey      ed25519.PublicKey
	ExecutorCommit string
	Artifact       frontRepairArtifactIdentity
}

func (envelope frontRepairApprovalEnvelope) signingBytes() ([]byte, error) {
	envelope.Signature = ""
	envelope.Statement.ApprovedAt = envelope.Statement.ApprovedAt.UTC()
	envelope.Statement.ExpiresAt = envelope.Statement.ExpiresAt.UTC()
	raw, err := declarativerelease.CanonicalJSON(envelope)
	if err != nil {
		return nil, err
	}
	return append([]byte(frontRepairApprovalSignatureDomain), raw...), nil
}

func (envelope frontRepairApprovalEnvelope) verify(preview frontRepairPreview, trust frontRepairApprovalTrust,
	tools frontRepairToolSet, now time.Time) (frontRepairReviewedIntent, error) {
	if envelope.Schema != frontRepairApprovalEnvelopeSchema || envelope.Scope != trust.Scope ||
		(releaseguardian.Key{Component: trust.Scope}).Validate() != nil ||
		len(trust.PublicKey) != ed25519.PublicKeySize || !trust.Artifact.valid() || envelope.Artifact != trust.Artifact ||
		tools.source != trust.ExecutorCommit || len(tools.tools) == 0 ||
		tools.digest != envelope.Statement.ToolManifestDigest {
		return frontRepairReviewedIntent{}, errors.New("Front repair approval lacks exact protected scope or verified build")
	}
	encoding := base64.RawURLEncoding.Strict()
	if len(envelope.Signature) != encoding.EncodedLen(ed25519.SignatureSize) {
		return frontRepairReviewedIntent{}, errors.New("Front repair approval signature is invalid")
	}
	signature, err := encoding.DecodeString(envelope.Signature)
	message, messageErr := envelope.signingBytes()
	if err != nil || messageErr != nil || encoding.EncodeToString(signature) != envelope.Signature ||
		!ed25519.Verify(trust.PublicKey, message, signature) {
		return frontRepairReviewedIntent{}, errors.New("Front repair approval signature is invalid")
	}
	intent, err := envelope.Statement.bindPreview(preview, trust.ExecutorCommit, now)
	if err != nil {
		return frontRepairReviewedIntent{}, err
	}
	// A newly signed approval window can resume exactly this journal. A new
	// preview, scope, revision, tool set or Actions artifact cannot resume it.
	envelope.Statement.ApprovedAt, envelope.Statement.ExpiresAt = time.Time{}, time.Time{}
	binding, err := envelope.signingBytes()
	if err != nil {
		return frontRepairReviewedIntent{}, err
	}
	intent.AuthorizationDigest = shaDigest(binding)
	return intent, nil
}
