package edgecontrol

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

var ErrGroupOriginalPublication = errors.New("edge-control original publication binding unavailable")

// GroupOriginalPublicationBinding is a bounded, independently reproducible
// component of an external-root proof. It authenticates the original signed
// bundle using the currently projected keyring, and binds that exact issuance
// to a replayed original compiler input. It does NOT authenticate file custody,
// complete the live registry, admit reactivation, or authorize retirement.
// In particular these values must never become an empty verified HotNode.
type GroupOriginalPublicationBinding struct {
	GroupID                string                       `json:"edge_group_id"`
	Generation             string                       `json:"generation"`
	PublicationSequence    uint64                       `json:"publication_sequence"`
	CandidateSequence      uint64                       `json:"candidate_sequence"`
	RecoveryEpoch          uint64                       `json:"recovery_epoch"`
	SigningKeyID           string                       `json:"signing_key_id"`
	SigningKeyringRevision uint64                       `json:"signing_keyring_revision"`
	OriginalInputDigest    string                       `json:"original_input_digest"`
	SignedBundleDigest     string                       `json:"signed_bundle_digest"`
	TrafficRelease         *model.TrafficReleaseBinding `json:"traffic_release,omitempty"`
	References             []inputarchive.HotReference  `json:"references"`
	DependencyBindingKnown bool                         `json:"dependency_binding_known"`
	Commitment             string                       `json:"commitment"`
}

// Verification deliberately takes original typed values, not the export's
// serialized SignatureVerified/PublicationSequences flags. The caller must
// have loaded keys independently from the private projected policy. The
// complete source/registry transaction remains a separate admission gate.
func bindGroupOriginalPublication(group string, input GroupCompileInput, pub GroupAuthorityLedgerEntry, body model.EdgeRouteBundle, recovery uint64, keys groupBundleSigningKey) (GroupOriginalPublicationBinding, error) {
	var empty GroupOriginalPublicationBinding
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > groupOriginalRecordBytes || validateGroupCompileInput(input, group) != nil {
		return empty, ErrGroupOriginalPublication
	}
	e := input.Entry
	if pub.Schema != GroupAuthorityLedgerSchemaV1 || pub.GroupID != group || pub.Status != GroupAuthorityStatusPublished ||
		pub.Authority != "edge-control" || !pub.PublicationEnabled || pub.FailureCode != "" ||
		pub.Sequence == 0 || pub.CandidateLedgerSequence != e.Sequence || pub.BundleGeneration != e.BundleGeneration ||
		pub.LastPublishedBundleGeneration != e.BundleGeneration || pub.RouteIntentGeneration != e.RouteIntentGeneration ||
		pub.InventoryGeneration != e.InventoryGeneration || pub.RecordedAt.Before(e.RecordedAt) ||
		!pub.RecordedAt.Equal(body.GeneratedAt) || pub.PublishedBundleDigest != signedGroupBundleDigest(body) ||
		pub.SigningKeyID != body.KeyID || !sameGroupBundleGeneration(*e.Bundle, body) ||
		pub.RecoveryEpoch != 0 && pub.RecoveryEpoch != recovery {
		return empty, ErrGroupOriginalPublication
	}
	published := GroupPublishedBundle{Schema: GroupPublishedBundleSchemaV1, GroupID: group,
		PublicationSequence: pub.Sequence, CandidateLedgerSequence: e.Sequence, Digest: pub.PublishedBundleDigest,
		PublishedAt: pub.RecordedAt, RecoveryEpoch: recovery, Bundle: body}
	if validateGroupPublishedBundle(group, published) != nil || !verifyGroupOriginalIssuance(group, body, keys) {
		return empty, ErrGroupOriginalPublication
	}
	proof := GroupOriginalPublicationBinding{GroupID: group, Generation: e.BundleGeneration,
		PublicationSequence: pub.Sequence, CandidateSequence: e.Sequence, RecoveryEpoch: recovery,
		SigningKeyID: pub.SigningKeyID, SigningKeyringRevision: keys.generation,
		OriginalInputDigest: digestJSON(input), SignedBundleDigest: pub.PublishedBundleDigest,
		References: []inputarchive.HotReference{}}
	// A legacy unbound source is not a dependency-free leaf. A signed traffic
	// binding supplies concrete DB references, which must ALL be resolved and
	// rechecked under the final native RR snapshot and current trust policy.
	if b := input.RouteIntent.TrafficRelease; b != nil {
		copy := *b
		copy.EdgeGroupIDs = append([]string(nil), b.EdgeGroupIDs...)
		proof.TrafficRelease = &copy
		proof.DependencyBindingKnown = true
		proof.References = []inputarchive.HotReference{
			{Kind: "artifact", ID: b.ReleaseSetID, ExpectedHash: strings.TrimPrefix(b.ReleaseSetDigest, "sha256:")},
			{Kind: "artifact", ID: b.RouteArtifactID, ExpectedHash: strings.TrimPrefix(b.RouteArtifactDigest, "sha256:")},
			{Kind: "release", ID: b.ReleaseID},
			{Kind: "input", ID: strings.TrimPrefix(b.InputSnapshotDigest, "sha256:"), ExpectedHash: strings.TrimPrefix(b.InputSnapshotDigest, "sha256:")},
		}
	}
	proof.Commitment = digestJSON(proof)
	return proof, nil
}

// The original primary signature must verify under CURRENT trust. A refreshed
// secondary signature cannot rescue a revoked/missing original issuance key.
// Expiry is checked at original issuance for historical authentication only;
// this does not make an expired bundle valid for serving or reactivation now.
func verifyGroupOriginalIssuance(group string, body model.EdgeRouteBundle, keys groupBundleSigningKey) bool {
	if keys.GroupID != group || keys.generation == 0 || body.EdgeGroupID != group || body.GeneratedAt.IsZero() ||
		body.Issuer != groupAuthorityIssuer || body.KeyID == "" || body.Signature == "" || !body.ValidUntil.After(body.GeneratedAt) {
		return false
	}
	primary, previous, ok := groupOriginalVerificationKeys(keys)
	if !ok {
		return false
	}
	defer zeroBytes(primary)
	defer zeroBytes(previous)
	body.Signatures = nil
	return bundleauth.VerifyEdgeRouteBundleWithKeyring(body, bundleauth.NewKeyring(string(primary), keys.PrimaryKeyID,
		string(previous), keys.PreviousKeyID, keys.RevokedKeyIDs), body.GeneratedAt) == nil
}

func groupOriginalVerificationKeys(keys groupBundleSigningKey) ([]byte, []byte, bool) {
	primary, err := base64.RawURLEncoding.DecodeString(keys.PrimaryKey)
	if err != nil || len(primary) < 32 || len(primary) > 64 || !groupAuthorityKeyIDPattern.MatchString(keys.PrimaryKeyID) {
		zeroBytes(primary)
		return nil, nil, false
	}
	var previous []byte
	if keys.PreviousKey != "" || keys.PreviousKeyID != "" {
		previous, err = base64.RawURLEncoding.DecodeString(keys.PreviousKey)
		if err != nil || len(previous) < 32 || len(previous) > 64 || !groupAuthorityKeyIDPattern.MatchString(keys.PreviousKeyID) || keys.PreviousKeyID == keys.PrimaryKeyID {
			zeroBytes(primary)
			zeroBytes(previous)
			return nil, nil, false
		}
	}
	return primary, previous, true
}
