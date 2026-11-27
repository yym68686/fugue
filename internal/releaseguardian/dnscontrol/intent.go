// Package dnscontrol adapts the existing platform producer publication
// transaction to the production journal. A producer policy is not a DNS
// answer or serving receipt: actual traffic remains under the existing
// producer, consumer convergence and LKG gates.
package dnscontrol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/productioncontrol"
	kubejson "sigs.k8s.io/json"
)

const (
	Schema   = "fugue.native-global-dns-policy/v1"
	Resource = productioncontrol.GlobalProducerPolicyResource
)

// Scope is shared by changes to the same global producer policy, even when
// they mention different edges/hosts. Never use per-edge execution locks.
func Scope(cluster string) productioncontrol.Scope {
	return productioncontrol.Scope{ClusterID: cluster, ResourceID: Resource}
}

type Publication struct {
	Artifact model.PlatformArtifact        `json:"artifact"`
	Release  model.PlatformArtifactRelease `json:"release"`
}

type Sources struct {
	Producer     Publication               `json:"producer"`
	Full         Publication               `json:"full"`
	LKG          model.PlatformLKGSnapshot `json:"lkg"`
	Static       model.PlatformArtifact    `json:"static"`
	PreviousDNS  model.PlatformArtifact    `json:"previousDNS"`
	NextDNS      model.PlatformArtifact    `json:"nextDNS"`
	NextProducer model.PlatformArtifact    `json:"nextProducer"`
}

type ArtifactRef struct {
	ID         string `json:"id"`
	Digest     string `json:"digest"`
	Generation string `json:"generation"`
	Sequence   int64  `json:"sequence"`
}

type Payload struct {
	Schema           string                                       `json:"schema"`
	Origin           string                                       `json:"origin"`
	PreviousProducer ArtifactRef                                  `json:"previousProducer"`
	PreviousDNS      ArtifactRef                                  `json:"previousDNS"`
	Static           ArtifactRef                                  `json:"static"`
	NextDNS          ArtifactRef                                  `json:"nextDNS"`
	NextProducer     ArtifactRef                                  `json:"nextProducer"`
	Precondition     model.PlatformProducerReconfiguration        `json:"precondition"`
	LKGID            string                                       `json:"lkgId"`
	ChangedEdge      string                                       `json:"changedEdge"`
	BeforeHold       *platformproducer.OfflineEdge                `json:"beforeHold"`
	AfterHold        *platformproducer.OfflineEdge                `json:"afterHold"`
	ShadowDigest     string                                       `json:"shadowDigest"`
	Reverses         *productioncontrol.GlobalProducerRollbackRef `json:"reverses,omitempty"`
}

func validOrigin(origin string) bool {
	u, err := url.Parse(origin)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil &&
		u.Opaque == "" && u.Path == "" && u.RawQuery == "" && u.Fragment == "" && !u.ForceQuery && !strings.Contains(origin, "#")
}

func validID(id string) bool {
	return id != "" && len(id) <= 256 && strings.TrimSpace(id) == id && !strings.ContainsAny(id, "/?#\\\x00\r\n")
}

func artifactRef(a model.PlatformArtifact) ArtifactRef {
	return ArtifactRef{a.ID, a.ContentHash, a.Generation, a.GenerationSequence}
}

func (a ArtifactRef) valid() bool {
	return validID(a.ID) && validID(a.Generation) && platformproducer.ValidDigest(a.Digest) && a.Sequence > 0
}

func validateArtifact(a model.PlatformArtifact, kind, scope string) error {
	d, err := platformconfig.Digest(a.Content)
	if err != nil || a.Content == nil || a.ContentHash != d || !artifactRef(a).valid() ||
		a.ArtifactKind != kind || a.ScopeKey != scope || a.Scope.Key != scope ||
		a.Status != model.PlatformArtifactStatusValidated || a.SchemaVersion != model.PlatformArtifactSchemaVersionV1 ||
		a.Provenance.Issuer != model.PlatformArtifactIssuerFugue || a.Provenance.Signature == "" ||
		a.Provenance.SignedAt.IsZero() {
		return errors.New("DNS producer observation is not an exact validated artifact")
	}
	// Do not give the reader the artifact HMAC signing key. These are
	// authenticated API observations; the existing SQL release transaction
	// independently validates stored signatures with its own keyring.
	return nil
}

func publicationRef(p Publication, kind, scope, channel string) (model.PlatformPublicationPrecondition, error) {
	a, r := p.Artifact, p.Release
	if err := validateArtifact(a, kind, scope); err != nil {
		return model.PlatformPublicationPrecondition{}, err
	}
	if !validID(r.ID) || r.Status != model.PlatformArtifactReleaseStatusActive ||
		r.ArtifactID != a.ID || r.Generation != a.Generation || r.ScopeKey != scope ||
		r.ArtifactKind != kind || r.ReleaseChannel != channel || r.FencingToken <= 0 ||
		r.LaneKey != platformsafety.ReleaseLaneKey(kind, scope, channel) {
		return model.PlatformPublicationPrecondition{}, errors.New("DNS producer observation is not the exact active publication")
	}
	return model.PlatformPublicationPrecondition{ArtifactID: a.ID, ContentHash: a.ContentHash, ReleaseID: r.ID, FencingToken: r.FencingToken}, nil
}

// ValidateSources uses the same typed delta validator the production store
// already calls under both publication locks. It never resolves a newer LKG
// into an already approved operation.
func ValidateSources(s Sources, now time.Time) (Payload, platformproducer.ProjectionPolicyInput, error) {
	var out Payload
	var projection platformproducer.ProjectionPolicyInput
	previous, err := publicationRef(s.Producer, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil {
		return out, projection, err
	}
	p, err := platformproducer.Decode(s.Producer.Artifact)
	if err != nil {
		return out, projection, err
	}
	n, err := platformproducer.Decode(s.NextProducer)
	if err != nil {
		return out, projection, err
	}
	for _, a := range []struct {
		artifact    model.PlatformArtifact
		kind, scope string
	}{
		{s.Static, model.PlatformArtifactKindPlatformIntent, "global"},
		{s.PreviousDNS, model.PlatformArtifactKindPolicySnapshot, "global"},
		{s.NextDNS, model.PlatformArtifactKindPolicySnapshot, "global"},
		{s.NextProducer, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope},
	} {
		if err := validateArtifact(a.artifact, a.kind, a.scope); err != nil {
			return out, projection, err
		}
	}
	if s.NextProducer.GenerationSequence <= s.Producer.Artifact.GenerationSequence ||
		s.NextProducer.ID == s.Producer.Artifact.ID || s.NextDNS.ID == s.PreviousDNS.ID ||
		platformproducer.ValidateOfflineEdgeUpdate(p, n, s.Static, s.PreviousDNS, s.NextDNS) != nil {
		return out, projection, errors.New("DNS request is not the existing producer's exact single-hold transition")
	}
	full, err := verifiedServingFull(s.Producer, p, s.Full, s.LKG, now)
	if err != nil {
		return out, projection, err
	}
	l := s.LKG
	static, err := platformproducer.DecodeStaticIntent(s.Static)
	if err != nil {
		return out, projection, err
	}
	before, err := platformproducer.DecodeProjectionPolicy(s.PreviousDNS, static.Consumers, p.HostedZoneTemplates)
	if err != nil {
		return out, projection, err
	}
	projection, err = platformproducer.DecodeProjectionPolicy(s.NextDNS, static.Consumers, n.HostedZoneTemplates)
	if err != nil {
		return out, projection, err
	}
	if err := platformproducer.ValidatePinnedSources(p, static, &before); err != nil {
		return out, projection, err
	}
	if err := platformproducer.ValidatePinnedSources(n, static, &projection); err != nil {
		return out, projection, err
	}
	out = Payload{Schema: Schema, PreviousProducer: artifactRef(s.Producer.Artifact),
		PreviousDNS: artifactRef(s.PreviousDNS), Static: artifactRef(s.Static), NextDNS: artifactRef(s.NextDNS),
		NextProducer: artifactRef(s.NextProducer), LKGID: l.ID,
		Precondition: model.PlatformProducerReconfiguration{Operation: "edge_offline", PreviousPolicy: previous, ServingFull: full,
			VerificationEvidenceHash: l.VerificationEvidenceHash}}
	old := make(map[string]platformproducer.OfflineEdge, len(before.OfflineEdges))
	for _, h := range before.OfflineEdges {
		old[h.EdgeID] = h
	}
	for _, h := range projection.OfflineEdges {
		if previous, ok := old[h.EdgeID]; !ok || !reflect.DeepEqual(previous, h) {
			out.ChangedEdge, out.AfterHold = h.EdgeID, &h
			if ok {
				out.BeforeHold = &previous
			}
		}
		delete(old, h.EdgeID)
	}
	for _, h := range old {
		out.ChangedEdge, out.BeforeHold = h.EdgeID, &h
	}
	if out.ChangedEdge == "" {
		return Payload{}, projection, errors.New("DNS offline change has no exact edge delta")
	}
	return out, projection, nil
}

// verifiedServingFull binds a current Full and its verified, unexpired LKG
// to the exact selected producer policy and pinned source artifacts. It is
// not itself external DNS answer evidence.
func verifiedServingFull(producer Publication, p platformproducer.Policy, serving Publication, l model.PlatformLKGSnapshot, now time.Time) (model.PlatformPublicationPrecondition, error) {
	full, err := publicationRef(serving, model.PlatformArtifactKindReleaseSet, "global", "full")
	if err != nil {
		return full, err
	}
	a, r := serving.Artifact, serving.Release
	if r.VerificationState != model.PlatformArtifactVerificationStateVerified || r.VerifiedLKGGeneration != a.Generation ||
		r.ReleasedByType != model.ActorTypeBootstrap || r.ReleasedByID != platformproducer.Actor ||
		a.Metadata[platformproducer.PolicyReleaseMetadata] != producer.Release.ID ||
		a.Metadata[platformproducer.StaticIntentIDMetadata] != p.StaticIntentArtifactID ||
		a.Metadata[platformproducer.StaticIntentDigestMetadata] != p.StaticIntentDigest ||
		a.Metadata[platformproducer.DNSPolicyIDMetadata] != p.DNSPolicyArtifactID ||
		a.Metadata[platformproducer.DNSPolicyDigestMetadata] != p.DNSPolicyDigest ||
		(a.Content["publication_role"] != nil && a.Content["publication_role"] != "") {
		return full, errors.New("full publication does not verify the exact selected producer and pinned sources")
	}
	if _, err := platformconfig.ValidateReleaseComposition(a); err != nil {
		return full, err
	}
	if !validID(l.ID) || l.ArtifactID != full.ArtifactID || l.ContentHash != full.ContentHash ||
		l.ArtifactKind != a.ArtifactKind || l.Scope != a.Scope || l.ScopeKey != "global" ||
		l.SchemaVersion != a.SchemaVersion || l.Generation != a.Generation || l.GenerationSequence != a.GenerationSequence ||
		l.ArtifactProvenance != a.Provenance || l.SnapshotProvenance.Signature == "" ||
		l.VerifiedByReleaseID != full.ReleaseID || !platformproducer.ValidDigest(l.VerificationEvidenceHash) ||
		now.IsZero() || !l.ExpiresAt.After(now) {
		return full, errors.New("DNS control requires the exact unexpired verified full LKG")
	}
	return full, nil
}

func (p Payload) validate() error {
	if p.Schema != Schema || !validOrigin(p.Origin) || !p.PreviousProducer.valid() || !p.NextProducer.valid() ||
		!p.PreviousDNS.valid() || !p.NextDNS.valid() || !p.Static.valid() || !validID(p.LKGID) ||
		p.PreviousProducer.ID == p.NextProducer.ID || p.PreviousProducer.Digest == p.NextProducer.Digest ||
		p.NextProducer.Sequence <= p.PreviousProducer.Sequence || p.NextDNS.Sequence <= p.PreviousDNS.Sequence ||
		p.PreviousDNS.ID == p.NextDNS.ID || p.PreviousDNS.Digest == p.NextDNS.Digest || !platformproducer.ValidDigest(p.ShadowDigest) ||
		p.Precondition.Operation != "edge_offline" || p.Precondition.PreviousPolicy.ArtifactID != p.PreviousProducer.ID ||
		p.Precondition.PreviousPolicy.ContentHash != p.PreviousProducer.Digest ||
		!platformproducer.ValidDigest(p.Precondition.VerificationEvidenceHash) ||
		!validID(p.ChangedEdge) || p.BeforeHold == nil && p.AfterHold == nil ||
		reflect.DeepEqual(p.BeforeHold, p.AfterHold) {
		return errors.New("invalid exact DNS producer intent")
	}
	for _, r := range []model.PlatformPublicationPrecondition{p.Precondition.PreviousPolicy, p.Precondition.ServingFull} {
		if !validID(r.ArtifactID) || !validID(r.ReleaseID) || !platformproducer.ValidDigest(r.ContentHash) ||
			r.FencingToken < 1 || r.FencingToken == math.MaxInt64 {
			return errors.New("DNS producer intent has an invalid publication precondition")
		}
	}
	for _, h := range []*platformproducer.OfflineEdge{p.BeforeHold, p.AfterHold} {
		if h != nil && (h.EdgeID != p.ChangedEdge || platformproducer.ValidateOfflineEdges([]platformproducer.OfflineEdge{*h}) != nil) {
			return errors.New("invalid exact before or after DNS hold")
		}
	}
	if p.Reverses != nil && !p.Reverses.Valid() {
		return errors.New("invalid DNS rollback provenance")
	}
	return nil
}

// Draft requires the settled predecessor in the journal as well as a
// successful read-only shadow of validated immutable artifacts.
func Draft(cluster, request, origin string, head productioncontrol.ResourceState, s Sources, before, after Preview, now time.Time) (productioncontrol.Submission, error) {
	return draft(cluster, request, origin, head, s, before, after, now, nil)
}

func draft(cluster, request, origin string, head productioncontrol.ResourceState, s Sources, before, after Preview, now time.Time, reverse *rollbackSource) (productioncontrol.Submission, error) {
	p, projection, err := ValidateSources(s, now)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	shadow, err := ValidatePreview(before, after, s, projection, now)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	p.Origin, p.ShadowDigest = origin, shadow.Digest
	if reverse != nil {
		p.Reverses = &reverse.binding
		if err := reverse.verifyInverse(p); err != nil {
			return productioncontrol.Submission{}, err
		}
	}
	if err := p.validate(); err != nil {
		return productioncontrol.Submission{}, err
	}
	// Journal version is the producer policy lane's fence, not the full
	// traffic release fence. Every accepted reconfiguration advances it.
	base := productioncontrol.Version{Generation: p.Precondition.PreviousPolicy.FencingToken, Digest: p.PreviousProducer.Digest}
	if head.Scope != Scope(cluster) || head.Pending != "" || head.LastVerified != base || head.Verified.Generation < 1 ||
		!platformproducer.ValidDigest(head.EvidenceDigest) {
		return productioncontrol.Submission{}, errors.New("DNS request lacks the settled exact producer predecessor")
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	i, err := productioncontrol.NewIntent(cluster, Resource, request, productioncontrol.DNSMembership,
		base.Generation, base.Digest, p.NextProducer.Digest, base.Digest, raw)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	su := productioncontrol.Submission{Intent: i, Payload: raw}
	_, err = Inspect(su)
	return su, err
}

func Inspect(s productioncontrol.Submission) (Payload, error) {
	var p Payload
	strict, err := kubejson.UnmarshalStrict(s.Payload, &p)
	if s.Intent.Operation != productioncontrol.DNSMembership || s.Intent.ResourceID != Resource ||
		s.Intent.VerifyPayload(s.Payload) != nil || err != nil || len(strict) != 0 || p.validate() != nil ||
		s.Intent.Generation != p.Precondition.PreviousPolicy.FencingToken || s.Intent.CurrentDigest != p.PreviousProducer.Digest ||
		s.Intent.TargetDigest != p.NextProducer.Digest || s.Intent.RollbackDigest != s.Intent.CurrentDigest {
		return Payload{}, errors.New("DNS submission does not match its exact producer publication")
	}
	canonical, err := json.Marshal(p)
	if err != nil || !bytes.Equal(canonical, s.Payload) {
		return Payload{}, fmt.Errorf("DNS submission payload is not canonical")
	}
	return p, nil
}
