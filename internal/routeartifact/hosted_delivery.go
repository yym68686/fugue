package routeartifact

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

// A private payload for one authenticated recipient of an offered draft.
// The active full is the membership authority, NOT a serving fence for the
// new contents. Do not install this object or report it through the global
// heartbeat. The eventual host decision must name this exact payload digest.
type HostedDeliveryPart struct {
	Recipient HostedRecipient      `json:"recipient"`
	Content   HostedRecipientInput `json:"content"`
	// A content pin is not a phase decision. A DNS process serving more
	// than one zone must supply evidence for each explicitly named part.
	ContentDigest string `json:"content_digest"`
}

type HostedDelivery struct {
	Schema, Host, DraftDigest, ImageDigest, Phase string
	Member, MembershipSet, MembershipRelease      string
	MembershipRevision, MembershipFence           int64
	Parts                                         []HostedDeliveryPart
}

// The slot is for one registered physical member/kind/zone of this host.
// A digest of payload bytes alone cannot distinguish two DNS zones with
// identical answers, a replacement enrollment, or re-use after withdrawal.
// The host decision/fence is bound independently to each observation.
func (d HostedDelivery) PartKey(part HostedDeliveryPart) (string, error) {
	if d.Schema != "fugue.hosted-recipient-input/v1" || d.Host == "" || d.Member == "" || d.MembershipSet == "" || d.MembershipRelease == "" || d.MembershipRevision < 1 || d.MembershipFence < 1 {
		return "", errors.New("missing hosted recipient binding")
	}
	return pc.Digest([]any{"fugue.hosted-recipient-slot/v1", d.Host, d.Member, d.MembershipSet, d.MembershipRevision, d.MembershipRelease, d.MembershipFence, part.Recipient})
}

// AddressHostedPreparation is called with the verified draft, stored image
// pin and a *current, independently authenticated serving assignment* for
// this caller. It never accepts a requested zone, group, origin or host
// from the consumer. One DNS process can own more than one declared zone.
// For this first handoff only the proposal's own verified base is supported.
// Selecting a different global parent requires a fresh host predecessor CAS.
func AddressHostedPreparation(p pc.HostedScopeProposal, draft, digest string, wire []byte, phase string, who platformcontrol.PlatformComponentIdentityClaims, serving model.PlatformConsumerAssignment, set model.PlatformExpectedConsumerSet) (HostedDelivery, string, error) {
	unit, err := OpenHostedPhase(p, draft, digest, wire, phase)
	if err != nil {
		return HostedDelivery{}, "", err
	}
	return unit.ForMember(who, serving, set)
}

type hostedKindNode struct{ component, kind, node, group string }
type HostedPhaseResolver struct {
	proposal             pc.HostedScopeProposal
	draft, digest, phase string
	parts                map[hostedKindNode][]HostedDeliveryPart
}

// Decode one bounded selected image once, independently of the number of
// enrolled nodes. The rows are physical receiver inputs, never live facts.
func OpenHostedPhase(p pc.HostedScopeProposal, draft, digest string, wire []byte, phase string) (*HostedPhaseResolver, error) {
	bad := errors.New("invalid compiled hosted phase")
	if len(wire) == 0 || len(wire) > maxHostedProgramBytes || p.Base.Validate() != nil || p.Schema != pc.HostedProposalSchema {
		return nil, bad
	}
	sum := sha256.Sum256(wire)
	var image hostedProgramImage
	if digest != "sha256:"+hex.EncodeToString(sum[:]) || json.Unmarshal(wire, &image) != nil || image.Schema != "fugue.hosted-program/v1" ||
		p.Host == "" || image.Host != p.Host || image.DraftDigest != draft || image.Base != p.Base || !reflect.DeepEqual(image.Records, p.Records) {
		return nil, bad
	}
	stage := image.Prepared
	switch phase {
	case "prepare", "withdraw":
	case "forward":
		stage = image.Forward
	default:
		return nil, bad
	}
	parts, err := HostedRecipientInputs(p, stage)
	if err != nil {
		return nil, err
	}
	unit := &HostedPhaseResolver{p, draft, digest, phase, map[hostedKindNode][]HostedDeliveryPart{}}
	for r, v := range parts {
		pin, e := pc.Digest([]any{"fugue.hosted-recipient-part/v1", p.Host, r, v})
		if e != nil {
			return nil, e
		}
		k := hostedKindNode{r.Component, r.Kind, r.Node, r.Group}
		unit.parts[k] = append(unit.parts[k], HostedDeliveryPart{Recipient: r, Content: v, ContentDigest: pin})
	}
	for k := range unit.parts {
		slices.SortFunc(unit.parts[k], func(a, b HostedDeliveryPart) int { return strings.Compare(a.Recipient.Zone, b.Recipient.Zone) })
	}
	return unit, nil
}

func (u *HostedPhaseResolver) Required() []HostedRecipient {
	var out []HostedRecipient
	for _, parts := range u.parts {
		for _, part := range parts {
			out = append(out, part.Recipient)
		}
	}
	return out
}

func (u *HostedPhaseResolver) ForMember(who platformcontrol.PlatformComponentIdentityClaims, serving model.PlatformConsumerAssignment, set model.PlatformExpectedConsumerSet) (HostedDelivery, string, error) {
	var empty HostedDelivery
	bad := errors.New("no exact physical recipient for this hosted preparation")
	p := u.proposal
	if serving.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || serving.ReleaseSetID != p.Base.ReleaseSetID || serving.ArtifactReleaseID != p.Base.ReleaseID ||
		serving.FencingToken != p.Base.FencingToken || serving.ScopeKey != pc.GlobalScopeKey || set.ScopeKey != serving.ScopeKey ||
		set.ID == "" || serving.ExpectedConsumerSetID != set.ID || set.ReleaseSetID != serving.ReleaseSetID || set.ArtifactReleaseID != serving.ArtifactReleaseID ||
		set.Revision < 1 || serving.Revision != set.Revision || serving.ArtifactKind != set.ArtifactKind || set.ExpectedGeneration == "" || serving.ExpectedGeneration != set.ExpectedGeneration ||
		who.ScopeKey != serving.ScopeKey || who.ConsumerID() == "" || !slices.Contains(who.ArtifactKinds, serving.ArtifactKind) {
		return empty, "", bad
	}
	var child, hash string
	switch serving.ArtifactKind {
	case model.PlatformArtifactKindEdgeRouteBundle:
		child, hash = p.Base.RouteArtifactID, p.Base.RouteArtifactDigest
	case model.PlatformArtifactKindDNSAnswerBundle:
		child, hash = p.Base.DNSArtifactID, p.Base.DNSArtifactDigest
	case model.PlatformArtifactKindCaddyRouteConfig:
		child, hash = p.Base.TLSArtifactID, p.Base.TLSArtifactDigest
	default:
		return empty, "", bad
	}
	if serving.ArtifactID != child || serving.ContentHash != hash {
		return empty, "", bad
	}
	var member *model.PlatformExpectedConsumer
	owned := platformcontrol.ProjectExpectedConsumerOwners(set)
	for i := range owned.Consumers {
		v := &owned.Consumers[i]
		if v.Component != who.Component || v.NodeID != who.NodeID {
			continue
		}
		if member != nil || !v.Required || v.ArtifactKind != set.ArtifactKind || v.ScopeKey != set.ScopeKey || v.ExpectedGeneration != set.ExpectedGeneration ||
			!platformcontrol.ExpectedConsumerIdentityMatches(*v, who) {
			return empty, "", bad
		}
		member = v
	}
	if member == nil {
		return empty, "", bad
	}
	out := HostedDelivery{Schema: "fugue.hosted-recipient-input/v1", Host: p.Host, DraftDigest: u.draft, ImageDigest: u.digest, Phase: u.phase,
		Member: member.ConsumerID, MembershipSet: set.ID, MembershipRelease: serving.ArtifactReleaseID, MembershipRevision: set.Revision, MembershipFence: serving.FencingToken,
		Parts: u.parts[hostedKindNode{member.Component, member.ArtifactKind, member.NodeID, member.Cohort}]}
	if len(out.Parts) == 0 {
		return empty, "", bad
	}
	pin, err := pc.Digest(out)
	if err != nil {
		return empty, "", err
	}
	return out, pin, nil
}
