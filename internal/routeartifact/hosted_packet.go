package routeartifact

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// A point-in-time command, NOT a generic PlatformArtifact release, LKG,
// readiness certificate or proof of application. Ordinary global consumers
// never select this kind. Original selection time/expiry survive retries.
const HostedPacketKind = "hosted_recipient_selection_v1"

// DNS execution can compose peer heads over one shared platform target.
// A v1-only DNS process deliberately rejects such packets.
const HostedSharedTargetCapability = "hosted_shared_dns_target_v1"
const HostedSharedHoldCapability = "hosted_shared_dns_hold_v1"

type HostedSelectedPacket struct {
	Scope              string            `json:"host_scope"`
	Project            string            `json:"project_id"`
	Draft              string            `json:"draft_id"`
	Offer              int64             `json:"offer_sequence"`
	Fence              int64             `json:"host_fence"`
	Decision           string            `json:"host_decision"`
	Previous           string            `json:"previous_host_decision"`
	PriorParts         map[string]string `json:"previous_member_parts"`
	SkippedForwardFrom string            `json:"skipped_forward_from,omitempty"`
	Authority          string            `json:"authority_digest"`
	Business           string            `json:"business_digest"`
	Parent             string            `json:"membership_parent"`
	Child              string            `json:"membership_child"`
	ChildDigest        string            `json:"membership_child_digest"`
	SelectedAt         time.Time         `json:"selected_at"`
	ValidUntil         time.Time         `json:"valid_until"`
	Delivery           HostedDelivery    `json:"delivery"`
}

func (p HostedSelectedPacket) validate(now time.Time) error {
	d := p.Delivery
	bad := errors.New("host decision has invalid recipient, content, fence or lifetime")
	if p.Scope == "" || p.Project == "" || p.Draft == "" || p.Offer < 1 || p.Fence < 1 || p.Parent == "" || p.Child == "" || p.SelectedAt.IsZero() || p.SelectedAt.After(now) || !p.ValidUntil.After(now) ||
		d.Host == "" || len(d.Parts) == 0 || len(d.Parts) > 4096 || d.Phase != "prepare" && d.Phase != "forward" && d.Phase != "withdraw" {
		return bad
	}
	for _, v := range []string{p.Decision, p.Authority, p.Business, p.ChildDigest, d.DraftDigest, d.ImageDigest} {
		if !platformproducer.ValidDigest(v) {
			return bad
		}
	}
	one := d.Parts[0].Recipient
	if p.Fence == 1 && (p.Previous != "" || len(p.PriorParts) != 0 || d.Phase != "prepare") ||
		p.Fence > 1 && (!platformproducer.ValidDigest(p.Previous) || len(p.PriorParts) != len(d.Parts)) ||
		d.Phase == "withdraw" && (p.Fence < 3 || !platformproducer.ValidDigest(p.SkippedForwardFrom)) ||
		d.Phase != "withdraw" && p.SkippedForwardFrom != "" {
		return bad
	}
	if one.Node == "" || one.Group == "" {
		return bad
	}
	prevZone := ""
	for i, x := range d.Parts {
		r, v := x.Recipient, x.Content
		if r.Kind != one.Kind || r.Node != one.Node || r.Group != one.Group || r.Component != one.Component || i > 0 && r.Zone <= prevZone {
			return bad
		}
		prevZone = r.Zone
		switch r.Kind {
		case model.PlatformArtifactKindEdgeRouteBundle:
			if r.Component != model.PlatformConsumerComponentEdgeWorker || r.Zone != "" || len(d.Parts) != 1 || len(v.Routes) == 0 || v.DNS != nil || len(v.TLS) != 0 || len(v.Records) != 0 {
				return bad
			}
			for _, row := range v.Routes {
				if row.Hostname != d.Host || row.EdgeGroupID != r.Group {
					return bad
				}
			}
		case model.PlatformArtifactKindCaddyRouteConfig:
			if r.Component != model.PlatformConsumerComponentEdgeWorker || r.Zone != "" || len(d.Parts) != 1 || len(v.TLS) != 1 || v.TLS[0].Hostname != d.Host || len(v.Routes) != 0 || len(v.Cache) != 0 || v.DNS != nil || len(v.Records) != 0 {
				return bad
			}
		case model.PlatformArtifactKindDNSAnswerBundle:
			if r.Component != model.PlatformConsumerComponentDNSServer || r.Zone == "" || len(v.Routes) != 0 || len(v.Cache) != 0 || len(v.TLS) != 0 || len(v.Records) == 0 || v.DNS == nil || len(v.DNS.Views) != 1 {
				return bad
			}
			view := v.DNS.Views[0]
			if view.Zone != r.Zone || view.EdgeGroupID != r.Group || view.NodeID != r.Node {
				return bad
			}
		default:
			return bad
		}
		pin, e := pc.Digest([]any{"fugue.hosted-recipient-part/v1", d.Host, r, v})
		if e != nil || pin != x.ContentDigest {
			return bad
		}
		if k, e := d.PartKey(x); e != nil || (p.Fence > 1 && !platformproducer.ValidDigest(p.PriorParts[k])) {
			return bad
		}
	}
	return nil
}

func SignHostedPacket(p HostedSelectedPacket, keys bundleauth.Keyring, now time.Time) (model.PlatformArtifact, error) {
	var a model.PlatformArtifact
	if e := p.validate(now); e != nil {
		return a, e
	}
	b, e := json.Marshal(p)
	if e != nil || len(b) > maxHostedProgramBytes {
		return a, errors.New("host selection packet exceeds bound")
	}
	var body map[string]any
	// Keep integers exact in the signed generic artifact encoding.
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if e = dec.Decode(&body); e != nil {
		return a, e
	}
	digest, e := pc.Digest(body)
	if e != nil {
		return a, e
	}
	return platformsafety.SignPlatformArtifact(model.PlatformArtifact{ID: "host-selection:" + digest, ArtifactKind: HostedPacketKind, SchemaVersion: model.PlatformArtifactSchemaVersionV1, Scope: model.PlatformArtifactScope{ScopeType: "hosted-recipient", Key: p.Scope, ProjectID: p.Project, Hostname: p.Delivery.Host}, ScopeKey: p.Scope, Generation: p.Decision, GenerationSequence: p.Fence, Status: model.PlatformArtifactStatusValidated, Content: body, ContentHash: digest, CreatedAt: p.SelectedAt, UpdatedAt: p.SelectedAt}, keys)
}

// Verify with the receiver's current private assignment and enrollment even
// when a cached packet has a valid signature. Application still compares the
// local host predecessor and persists the high-water fence before serving.
func VerifyHostedPacket(a model.PlatformArtifact, host string, who platformcontrol.PlatformComponentIdentityClaims, serving model.PlatformConsumerAssignment, knownFence int64, knownDecision string, keys bundleauth.Keyring, now time.Time) (HostedSelectedPacket, error) {
	var p HostedSelectedPacket
	bad := errors.New("host selection is stale or addressed to another receiver")
	if a.ArtifactKind != HostedPacketKind || a.ID != "host-selection:"+a.ContentHash || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
		return p, bad
	}
	raw, e := json.Marshal(a.Content)
	if e != nil || len(raw) > maxHostedProgramBytes || json.Unmarshal(raw, &p) != nil || p.validate(now) != nil {
		return HostedSelectedPacket{}, bad
	}
	d := p.Delivery
	r := d.Parts[0].Recipient
	if a.Scope != (model.PlatformArtifactScope{ScopeType: "hosted-recipient", Key: p.Scope, ProjectID: p.Project, Hostname: host}) || a.ScopeKey != p.Scope || d.Host != host || d.Member != who.ConsumerID() ||
		r.Component != who.Component || r.Node != who.NodeID || r.Kind != serving.ArtifactKind || !slices.Contains(who.ArtifactKinds, r.Kind) || who.ScopeKey != serving.ScopeKey || serving.ScopeKey != pc.GlobalScopeKey ||
		serving.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || p.Parent != serving.ReleaseSetID || p.Child != serving.ArtifactID || p.ChildDigest != serving.ContentHash ||
		d.MembershipRelease != serving.ArtifactReleaseID || d.MembershipSet != serving.ExpectedConsumerSetID || d.MembershipRevision != serving.Revision || d.MembershipFence != serving.FencingToken ||
		p.Fence != a.GenerationSequence || p.Decision != a.Generation || knownFence < 0 || (knownFence == 0) != (knownDecision == "") || p.Fence < knownFence || p.Fence == knownFence && knownDecision != p.Decision {
		return HostedSelectedPacket{}, bad
	}
	return p, nil
}
