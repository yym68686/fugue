package dnsserver

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
	"github.com/miekg/dns"
)

type hostedRRKey struct {
	name   string
	family uint16
}
type hostedZoneKey struct {
	zone string
	hostedRRKey
}
type hostedDNSWire struct {
	publication DNSDecisionPublication
	packet      *routeartifact.HostedSelectedPacket
	rows        []dnsServingRecord
	shared      map[string]sharedDNSMember
}
type hostedDNSRoutes struct {
	keys   map[hostedZoneKey]*hostedDNSWire
	owners map[hostedRRKey]string
}

func (idx *hostedDNSRoutes) forQuery(parent *dnsServingState, q *dns.Msg, now time.Time) *dnsServingState {
	if idx == nil || parent == nil || len(q.Question) != 1 {
		return parent
	}
	question := q.Question[0]
	if question.Qclass != dns.ClassINET || question.Qtype != dns.TypeA && question.Qtype != dns.TypeAAAA {
		return parent
	}
	k := hostedRRKey{normalizeName(question.Name), question.Qtype}
	host, owned := idx.owners[k]
	if !owned {
		return parent
	}
	var w *hostedDNSWire
	var key hostedZoneKey
	for _, zone := range parent.zoneOrder {
		if nameWithinZone(k.name, zone) {
			key = hostedZoneKey{zone, k}
			w = idx.keys[key]
			break
		}
	}
	c := *parent
	c.transition = nil
	c.hostedBlocked = true
	if w == nil {
		c.hostedWire = &hostedDNSWire{publication: DNSDecisionPublication{HostedHostname: host}}
		return &c
	}
	c.hostedWire = w
	if w.packet != nil && w.packet.ValidUntil.After(now) && len(w.rows) > 0 && parent.record.Positive {
		a := parent.record.Candidate.Assignment
		p := w.packet
		c.hostedBlocked = !(a.ReleaseChannel == "full" && a.ScopeKey == "global" && p.Parent == a.ReleaseSetID && w.publication.ParentDigest == parent.record.Parent.ContentHash && p.Child == a.ArtifactID && p.ChildDigest == a.ContentHash && p.Delivery.MembershipRelease == a.ArtifactReleaseID && p.Delivery.MembershipRevision == a.Revision && p.Delivery.MembershipSet == a.ExpectedConsumerSetID && p.Delivery.MembershipFence == a.FencingToken)
		for _, m := range w.shared {
			if !m.packet.ValidUntil.After(now) {
				c.hostedBlocked = true
			}
		}
		if !c.hostedBlocked {
			c.hostedRows = w.effectiveRows(parent, key)
		}
	}
	return &c
}

func (s *Service) dnsHostCheckpoint(host string) (platformconsumer.HostedCheckpoint, error) {
	var cp platformconsumer.HostedCheckpoint
	if s.Config.CachePath == "" {
		return cp, errors.New("DNS cache not configured")
	}
	root, e := filepath.Abs(s.Config.CachePath + ".hosted-dns")
	kind := model.PlatformArtifactKindDNSAnswerBundle
	cp = platformconsumer.HostedCheckpoint{Root: root, Host: host, Keys: s.platformDNSKeys(), Who: platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentDNSServer, NodeID: s.Config.DNSNodeID, AuthorityID: platformcontrol.ConsumerAuthorityID(s.Config.EdgeGroupID), ScopeKey: "global", ArtifactKinds: []string{kind}}, Serving: model.PlatformConsumerAssignment{ArtifactKind: kind}}
	return cp, e
}

// Construct a new immutable index, retaining all other hosts. A name and
// family may exist in two nested zones, but can't be claimed by two hosts.
func (idx *hostedDNSRoutes) withPacket(p routeartifact.HostedSelectedPacket, parent *dnsServingState, facts map[routeartifact.HostedRecipient][]dnsReadinessFact) (*hostedDNSRoutes, error) {
	next := &hostedDNSRoutes{keys: map[hostedZoneKey]*hostedDNSWire{}, owners: map[hostedRRKey]string{}}
	if idx != nil {
		maps.Copy(next.keys, idx.keys)
		maps.Copy(next.owners, idx.owners)
	}
	wanted := map[hostedZoneKey]bool{}
	pub := DNSDecisionPublication{}
	if parent != nil {
		pub = dnsPublication(parent.record)
	}
	pub.HostedHostname, pub.HostedDecision, pub.HostedFence = p.Delivery.Host, p.Decision, p.Fence
	h := packetDNSDecision(&p)
	for _, part := range p.Delivery.Parts {
		r, u := part.Recipient, part.Content.DNS
		if u == nil || len(u.Views) != 1 || len(u.Views[0].Records) == 0 {
			return nil, errors.New("DNS selection has no record view")
		}
		shared, err := readSharedDNSPart(p.Delivery.Host, part.Content)
		if err != nil {
			return nil, err
		}
		for _, record := range u.Views[0].Records {
			family := dns.StringToType[record.Type]
			k := hostedZoneKey{r.Zone, hostedRRKey{record.Name, family}}
			prior := next.keys[k]
			if (family != dns.TypeA && family != dns.TypeAAAA) || !nameWithinZone(record.Name, r.Zone) || wanted[k] ||
				next.owners[k.hostedRRKey] != "" && next.owners[k.hostedRRKey] != p.Delivery.Host && (shared == nil || prior == nil || prior.shared == nil) ||
				prior != nil && (prior.shared == nil) != (shared == nil) {
				return nil, errors.New("conflicting DNS selection key")
			}
			wanted[k] = true
			w := &hostedDNSWire{publication: pub, packet: &p}
			rows, seen := facts[r]
			if shared != nil {
				if record.Name != shared.shape.Hostname {
					return nil, errors.New("shared selection contains foreign wire record")
				}
				if err := w.mergeShared(prior, sharedDNSMember{&p, shared, rows, seen}, record, parent); err != nil {
					return nil, err
				}
			} else if seen && parent != nil {
				w.rows = []dnsServingRecord{{hosted: &h, record: record, plan: u.Plan, facts: rows}}
			}
			next.keys[k], next.owners[k.hostedRRKey] = w, p.Delivery.Host
		}
	}
	for k, v := range next.keys {
		if v.forHost(p.Delivery.Host) != nil && !wanted[k] {
			return nil, errors.New("DNS selection cannot silently drop an existing owned key")
		}
	}
	return next, nil
}

func (s *Service) reserveHostedDNS() error {
	if s.Config.CachePath == "" {
		return nil
	}
	cp, err := s.dnsHostCheckpoint("")
	if err != nil {
		return err
	}
	names, err := platformconsumer.HostedCheckpointHosts(cp.Root)
	if err != nil {
		return err
	}
	var idx *hostedDNSRoutes
	for _, host := range names {
		cp.Host = host
		p, e := cp.Reserved()
		if errors.Is(e, os.ErrNotExist) {
			continue
		} // first attempt never renamed a selection
		if e != nil {
			return e
		}
		if p.Delivery.Parts[0].Recipient.Group != s.Config.EdgeGroupID {
			return errors.New("reserved DNS group differs")
		}
		idx, e = idx.withPacket(p, nil, nil)
		if e != nil {
			return e
		}
	}
	if idx != nil {
		s.platformServingBound.Store(true)
		s.hostedDNS.Store(idx)
	}
	return nil
}

func hostedDNSParentMatches(parent *dnsServingState, base map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput, p routeartifact.HostedSelectedPacket) bool {
	if len(base) != len(p.Delivery.Parts) || !parent.record.Positive {
		return false
	}
	for _, part := range p.Delivery.Parts {
		src, ok := base[part.Recipient]
		z, ownsZone := parent.zones[part.Recipient.Zone]
		u := part.Content.DNS
		if !ok || !ownsZone || u == nil || len(u.Views) != 1 || src.DNS == nil || len(src.DNS.Views) != 1 || !reflect.DeepEqual(src.Records, part.Content.Records) || len(src.DNS.Views[0].Records) != len(u.Views[0].Records) || !sameSharedPredecessor(p.Delivery.Host, src, part.Content) {
			return false
		}
		for i, old := range src.DNS.Views[0].Records {
			q := u.Views[0].Records[i]
			// The wire may be using another signed predecessor for just
			// this RR. Do not claim it was serving the plain global rows.
			if parent.transition != nil && parent.transition.hosts[old.Name] {
				return false
			}
			if old.Name != q.Name || old.Type != q.Type || old.TenantID != q.TenantID || old.AppID != q.AppID || old.TTL != q.TTL || len(q.Values) == 0 {
				return false
			}
			for _, v := range q.Values {
				if !slices.Contains(old.Values, v) {
					return false
				}
			}
			found := false
			for _, entry := range z.records[old.Name] {
				if entry.record.Type == old.Type {
					if found || !reflect.DeepEqual(entry.record, old) {
						return false
					}
					found = true
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}
