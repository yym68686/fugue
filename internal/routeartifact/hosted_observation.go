package routeartifact

import (
	"encoding/hex"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/routeproof"
	"github.com/miekg/dns"
)

// Evidence is sent by the authenticated physical executor. Reconstruct the
// required observations from the selected program before accepting any part.
type HostedObservation struct {
	Host      string         `json:"host"`
	Member    string         `json:"member"`
	Decision  string         `json:"decision"`
	Fence     int64          `json:"fence"`
	StartedAt time.Time      `json:"started_at"`
	Failure   string         `json:"failure,omitempty"`
	Samples   []HostedSample `json:"samples,omitempty"`
}
type HostedSample struct {
	Host                 string    `json:"host"`
	Member               string    `json:"member"`
	Decision             string    `json:"decision"`
	Fence                int64     `json:"fence"`
	Part                 string    `json:"part"`
	Content              string    `json:"content"`
	Nonce                string    `json:"nonce"`
	Name                 string    `json:"name"`
	Transport            string    `json:"transport"`
	ObservedAt           time.Time `json:"observed_at"`
	ValidUntil           time.Time `json:"valid_until"`
	Zone                 string    `json:"zone,omitempty"`
	Family               string    `json:"family,omitempty"`
	RRSet                []string  `json:"rrset,omitempty"`
	Path                 string    `json:"path,omitempty"`
	RouteDigest          string    `json:"route_digest,omitempty"`
	State                string    `json:"state,omitempty"`
	EdgeID               string    `json:"edge_id,omitempty"`
	GroupID              string    `json:"group_id,omitempty"`
	OwnerDigest          string    `json:"owner_digest,omitempty"`
	CertificateDigest    string    `json:"certificate_digest,omitempty"`
	CertificateNotBefore time.Time `json:"certificate_not_before,omitzero"`
	ChainValidUntil      time.Time `json:"chain_valid_until,omitzero"`
}
type HostedPartObservation struct {
	Part, Content, Evidence string
	ObservedAt, ValidUntil  time.Time
	Passing                 bool
}
type observationKey struct{ part, transport, name, family, path string }
type observationRequirement struct {
	rr                                *model.EdgeDNSRecord
	digest, state, edge, group, owner string
}

var errHostedObservation = errors.New("host receipt differs from current selection or physical observation")

func hostedExpectedSamples(p HostedSelectedPacket) (map[observationKey]observationRequirement, error) {
	want := map[observationKey]observationRequirement{}
	add := func(key observationKey, r observationRequirement) error {
		if _, ok := want[key]; ok || len(want) >= 8192 {
			return errHostedObservation
		}
		want[key] = r
		return nil
	}
	for _, part := range p.Delivery.Parts {
		pin, e := p.Delivery.PartKey(part)
		if e != nil {
			return nil, e
		}
		r := part.Recipient
		switch r.Kind {
		case model.PlatformArtifactKindDNSAnswerBundle:
			if part.Content.DNS == nil || len(part.Content.DNS.Views) != 1 {
				return nil, errHostedObservation
			}
			for i := range part.Content.DNS.Views[0].Records {
				rr := part.Content.DNS.Views[0].Records[i]
				// The packet keeps its physical address declaration. A live
				// answer containing one of this decision's withheld addresses
				// is a failure, even when it is in that signed declaration.
				if len(part.Content.DNS.Withheld) > 0 {
					veto := map[string]bool{}
					for _, h := range part.Content.DNS.Withheld {
						veto[h.Address] = true
					}
					rr.Values = slices.DeleteFunc(slices.Clone(rr.Values), func(ip string) bool { return veto[ip] })
					if len(rr.Values) == 0 {
						return nil, errHostedObservation
					}
				}
				for _, network := range []string{"udp", "tcp"} {
					if e := add(observationKey{pin, network, rr.Name, rr.Type, ""}, observationRequirement{rr: &rr}); e != nil {
						return nil, e
					}
				}
			}
		case model.PlatformArtifactKindEdgeRouteBundle:
			for _, row := range part.Content.Routes {
				d, e := routeproof.Digest(row)
				if e != nil {
					return nil, e
				}
				state := row.Status
				if state == model.EdgeRouteStatusActive {
					state = ""
				}
				if slices.Contains(row.ExcludedEdgeIDs, r.Node) || slices.Contains(row.ExcludedEdgeGroupIDs, r.Group) {
					state = routeproof.StateExcluded
				}
				if e := add(observationKey{pin, "https", p.Delivery.Host, "", model.NormalizeAppRoutePathPrefix(row.PathPrefix)}, observationRequirement{digest: d, state: state, edge: r.Node, group: r.Group}); e != nil {
					return nil, e
				}
			}
		case model.PlatformArtifactKindCaddyRouteConfig:
			if len(part.Content.TLS) != 1 {
				return nil, errHostedObservation
			}
			d, e := pc.Digest(part.Content.TLS[0])
			if e != nil {
				return nil, e
			}
			if e := add(observationKey{pin, "tls", p.Delivery.Host, "", ""}, observationRequirement{owner: d, edge: r.Node, group: r.Group}); e != nil {
				return nil, e
			}
		default:
			return nil, errHostedObservation
		}
	}
	return want, nil
}

func (r observationRequirement) matches(s HostedSample, zone string) bool {
	if s.RouteDigest != r.digest || s.State != r.state || s.EdgeID != r.edge || s.GroupID != r.group || s.OwnerDigest != r.owner || s.Zone != zone {
		return false
	}
	if r.owner != "" {
		if !platformproducer.ValidDigest(s.CertificateDigest) || s.CertificateNotBefore.IsZero() || s.CertificateNotBefore.After(s.ObservedAt) || s.ValidUntil.After(s.ChainValidUntil) {
			return false
		}
	} else if s.CertificateDigest != "" || !s.CertificateNotBefore.IsZero() || !s.ChainValidUntil.IsZero() {
		return false
	}
	if r.rr == nil {
		return len(s.RRSet) == 0
	}
	if len(s.RRSet) == 0 || len(s.RRSet) > 256 {
		return false
	}
	addresses := map[string]bool{}
	for _, line := range s.RRSet {
		if len(line) > 1024 {
			return false
		}
		rr, e := dns.NewRR(line)
		if e != nil || rr == nil {
			return false
		}
		h := rr.Header()
		if h.Name != dns.Fqdn(s.Name) || h.Class != dns.ClassINET || h.Rrtype != dns.StringToType[s.Family] || h.Ttl == 0 || int64(h.Ttl) > int64(r.rr.TTL) || s.ValidUntil.After(s.ObservedAt.Add(time.Second*time.Duration(h.Ttl))) {
			return false
		}
		var ip string
		switch v := rr.(type) {
		case *dns.A:
			ip = v.A.String()
		case *dns.AAAA:
			ip = v.AAAA.String()
		default:
			return false
		}
		a, e := netip.ParseAddr(ip)
		if e != nil || a.Is4In6() || addresses[ip] || !slices.Contains(r.rr.Values, ip) {
			return false
		}
		addresses[ip] = true
	}
	return true
}

// Compare samples with the canonical packet obtained independently by the
// receiver. The batch start orders conflicting attempts, including delayed
// sends. All parts expire at the earliest actual proof or command deadline.
func (o HostedObservation) Evaluate(p HostedSelectedPacket, now time.Time) ([]HostedPartObservation, error) {
	if len(p.Delivery.Parts) == 0 || o.Host != p.Delivery.Host || o.Member != p.Delivery.Member || o.Decision != p.Decision || o.Fence != p.Fence || o.StartedAt.IsZero() || o.StartedAt.Before(p.SelectedAt) || o.StartedAt.After(now) || !p.ValidUntil.After(now) {
		return nil, errHostedObservation
	}
	parts := map[string]HostedDeliveryPart{}
	for _, part := range p.Delivery.Parts {
		pin, e := p.Delivery.PartKey(part)
		if e != nil {
			return nil, e
		}
		parts[pin] = part
	}
	until := p.ValidUntil
	if o.Failure != "" {
		if o.Failure != "local_apply" && o.Failure != "wire_observation" || o.Samples != nil {
			return nil, errHostedObservation
		}
	} else {
		if len(o.Samples) == 0 || len(o.Samples) > 8192 {
			return nil, errHostedObservation
		}
		want, e := hostedExpectedSamples(p)
		if e != nil || len(want) != len(o.Samples) {
			return nil, errHostedObservation
		}
		nonces := map[string]bool{}
		for _, s := range o.Samples {
			key := observationKey{s.Part, s.Transport, s.Name, s.Family, s.Path}
			r, ok := want[key]
			part := parts[s.Part]
			if !ok || s.Host != o.Host || s.Decision != o.Decision || s.Fence != o.Fence || s.Member != o.Member || s.Content != part.ContentDigest || s.ObservedAt.Before(o.StartedAt) || s.ObservedAt.After(now) || !s.ValidUntil.After(now) || s.ValidUntil.After(p.ValidUntil) || len(s.Nonce) != 32 || s.Nonce != strings.ToLower(s.Nonce) || nonces[s.Nonce] || !r.matches(s, part.Recipient.Zone) {
				return nil, errHostedObservation
			}
			if _, e := hex.DecodeString(s.Nonce); e != nil {
				return nil, errHostedObservation
			}
			delete(want, key)
			nonces[s.Nonce] = true
			if s.ValidUntil.Before(until) {
				until = s.ValidUntil
			}
		}
	}
	d, e := pc.Digest(o)
	if e != nil {
		return nil, e
	}
	out := make([]HostedPartObservation, 0, len(parts))
	for _, part := range p.Delivery.Parts {
		pin, _ := p.Delivery.PartKey(part)
		out = append(out, HostedPartObservation{pin, part.ContentDigest, d, o.StartedAt, until, o.Failure == ""})
	}
	return out, nil
}
