package routeartifact

import (
	"errors"
	"slices"
	"strings"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// The per-process input is a projection of the independently verified full.
// Its digest is not a digest of what the producer wants to install.
type hostedBaselinePin struct {
	Recipient HostedRecipient `json:"recipient"`
	Digest    string          `json:"digest"`
}

func hostedMemberBaselinePins(source pc.HostedAddressSource, p pc.HostedScopeProposal, phase HostedPhase) ([]hostedBaselinePin, error) {
	parts, err := HostedRecipientInputs(p, phase)
	if err != nil {
		return nil, err
	}
	groups := map[HostedRecipient][]HostedDeliveryPart{}
	for r := range parts {
		k := r
		k.Zone = ""
		groups[k] = append(groups[k], HostedDeliveryPart{Recipient: r})
	}
	snapshot, err := Project(*source.PriorRoute)
	if err != nil {
		return nil, err
	}
	type pins struct{ route, tls string }
	resolved := map[string]pins{}
	out := make([]hostedBaselinePin, 0, len(groups))
	for r, slots := range groups {
		var pin string
		switch r.Kind {
		case model.PlatformArtifactKindDNSAnswerBundle:
			slices.SortFunc(slots, func(a, b HostedDeliveryPart) int { return strings.Compare(a.Recipient.Zone, b.Recipient.Zone) })
			base, e := HostedDNSMemberBase(p.Host, r.Node, r.Group, source.Prior)
			if e != nil {
				return nil, e
			}
			pin, err = HostedDNSBaselinePin(HostedDelivery{Host: p.Host, Parts: slots}, base)
		case model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindEdgeRouteBundle:
			v, ok := resolved[r.Group]
			if !ok {
				b, e := MaterializeSnapshotForGroup(snapshot, r.Group)
				if e != nil {
					return nil, e
				}
				b, e = hostedGroupSource(b, p)
				if e != nil {
					return nil, e
				}
				if len(b.TLSAllowlist) != 1 || b.TLSAllowlist[0] != p.Before.Owner {
					return nil, errors.New("member baseline certificate owner differs")
				}
				v.route, e = pc.Digest([]any{HostedRecipientInput{Routes: b.Routes, Cache: b.CachePolicies}, b.TLSAllowlist[0]})
				if e != nil {
					return nil, e
				}
				v.tls, e = pc.Digest(b.TLSAllowlist[0])
				if e != nil {
					return nil, e
				}
				resolved[r.Group] = v
			}
			pin = v.route
			if r.Kind == model.PlatformArtifactKindCaddyRouteConfig {
				pin = v.tls
			}
		default:
			return nil, errors.New("unknown member baseline kind")
		}
		if err != nil {
			return nil, err
		}
		out = append(out, hostedBaselinePin{r, pin})
	}
	slices.SortFunc(out, func(a, b hostedBaselinePin) int {
		for _, p := range [][2]string{{a.Recipient.Kind, b.Recipient.Kind}, {a.Recipient.Group, b.Recipient.Group}, {a.Recipient.Node, b.Recipient.Node}, {a.Recipient.Component, b.Recipient.Component}} {
			if n := strings.Compare(p[0], p[1]); n != 0 {
				return n
			}
		}
		return 0
	})
	return out, nil
}

// Nil is the old immutable image format. New images must cover every
// physical member exactly once; never infer a baseline from a receipt.
func (u *HostedPhaseResolver) bindBaselines(xs []hostedBaselinePin) error {
	if xs == nil {
		return nil
	}
	if len(xs) != len(u.parts) || len(xs) == 0 {
		return errors.New("incomplete member baselines")
	}
	u.baselines = map[hostedKindNode]string{}
	for _, x := range xs {
		r := x.Recipient
		k := hostedKindNode{r.Component, r.Kind, r.Node, r.Group}
		if r.Zone != "" || len(u.parts[k]) == 0 || u.baselines[k] != "" || !platformproducer.ValidDigest(x.Digest) {
			return errors.New("foreign or duplicated member baseline")
		}
		u.baselines[k] = x.Digest
	}
	return nil
}
func (u *HostedPhaseResolver) MemberBaseline(d HostedDelivery) string {
	if len(d.Parts) == 0 || d.Host != u.proposal.Host {
		return ""
	}
	r := d.Parts[0].Recipient
	return u.baselines[hostedKindNode{r.Component, r.Kind, r.Node, r.Group}]
}
