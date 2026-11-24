package dnsserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"slices"
	"strings"
	"time"

	"fugue/internal/routeartifact"
	"github.com/miekg/dns"
)

// Local-use EDNS option: only match a privately generated outstanding
// nonce on loopback. No public challenge endpoint, extra answer, receipt
// allocation or privilege is conferred by a client sending this option.
const hostedDNSChallengeOption = 65001

type hostedDNSWitness struct {
	key     hostedZoneKey
	wire    *hostedDNSWire
	network string
	id      uint16
	wrote   chan hostedDNSWrite
}
type hostedDNSWrite struct {
	rcode int
	rrs   []string
	at    time.Time
}
type hostedDNSWireEvidence = routeartifact.HostedSample

// Collect a complete observation for one DNS enrollment. A successful
// exchange for a parent zone, address family, unrelated name or second
// transport cannot cover a missing slot. Do not persist these as grants.
func (s *Service) witnessHostedDNSMember(ctx context.Context, host string, fence int64, decision string) ([]hostedDNSWireEvidence, error) {
	var out []hostedDNSWireEvidence
	seen := map[hostedZoneKey]bool{}
	selected := s.hostedDNS.Load()
	if selected == nil {
		return nil, errors.New("DNS host is not loaded")
	}
	// The caller can specify only the head, never which queries to omit.
	var canonical *routeartifact.HostedSelectedPacket
	for _, w := range selected.keys {
		if p := w.forHost(host); p != nil && p.Decision == decision && p.Fence == fence {
			canonical = p
			break
		}
	}
	if canonical == nil || len(canonical.Delivery.Parts) == 0 || len(canonical.Delivery.Parts) > 4096 {
		return nil, errors.New("DNS witness has no current local manifest")
	}
	p := *canonical
	for _, part := range p.Delivery.Parts {
		u := part.Content.DNS
		if u == nil || len(u.Views) != 1 || len(u.Views[0].Records) == 0 {
			return nil, errors.New("missing DNS witness zone")
		}
		for _, q := range u.Views[0].Records {
			k := hostedZoneKey{part.Recipient.Zone, hostedRRKey{q.Name, dns.StringToType[q.Type]}}
			if seen[k] || len(seen) >= 4096 {
				return nil, errors.New("ambiguous DNS wire witness manifest")
			}
			seen[k] = true
			for _, net := range []string{"udp", "tcp"} {
				row, e := s.witnessHostedDNSQuery(ctx, p, part, k, net)
				if e != nil {
					return nil, e
				}
				out = append(out, row)
			}
		}
	}
	now := time.Now().UTC()
	idx := s.hostedDNS.Load()
	parent := s.currentDNSServingState()
	if idx == nil || parent == nil {
		return nil, errors.New("DNS witness authority disappeared")
	}
	for _, ev := range out {
		k := hostedZoneKey{ev.Zone, hostedRRKey{ev.Name, dns.StringToType[ev.Family]}}
		q := new(dns.Msg).SetQuestion(dns.Fqdn(k.name), k.family)
		st := idx.forQuery(parent, q, now)
		if !ev.ValidUntil.After(now) || st.hostedBlocked || st.hostedWire == nil || st.hostedWire != selected.keys[k] {
			return nil, errors.New("DNS host changed during listener scan")
		}
	}
	return out, nil
}

func (s *Service) captureHostedDNSWrite(q, answer *dns.Msg, remote net.Addr, st *dnsServingState, now time.Time) {
	if len(q.Question) != 1 || st.hostedBlocked || st.hostedWire == nil || st.hostedWire.packet == nil {
		return
	}
	opt := q.IsEdns0()
	if opt == nil {
		return
	}
	var token []byte
	for _, v := range opt.Option {
		if o, ok := v.(*dns.EDNS0_LOCAL); ok && o.Code == hostedDNSChallengeOption {
			if token != nil || len(o.Data) != 16 {
				return
			}
			token = o.Data
		}
	}
	if token == nil {
		return
	}
	value, ok := s.hostedDNSWitnesses.Load(hex.EncodeToString(token))
	if !ok {
		return
	}
	w := value.(*hostedDNSWitness)
	host, _, err := net.SplitHostPort(remote.String())
	if err != nil || !net.ParseIP(host).IsLoopback() || !strings.HasPrefix(remote.Network(), w.network) || w.wire != st.hostedWire || w.id != q.Id || normalizeName(q.Question[0].Name) != w.key.name || q.Question[0].Qtype != w.key.family || q.Question[0].Qclass != dns.ClassINET {
		return
	}
	select {
	case w.wrote <- hostedDNSWrite{answer.Rcode, dnsRRStrings(answer.Answer), now}:
	default:
	}
}

func localHostedDNSAddress(addr string) (string, error) {
	host, port, e := net.SplitHostPort(addr)
	if e != nil {
		return "", e
	}
	ip := net.ParseIP(host)
	if host == "" || ip != nil && ip.IsUnspecified() {
		host = "127.0.0.1"
		if ip != nil && ip.To4() == nil {
			host = "::1"
		}
	} else if !ip.IsLoopback() {
		return "", errors.New("scoped wire witness requires a loopback listener")
	}
	return net.JoinHostPort(host, port), nil
}

// One physical listener observation: require the local write AND the actual
// reply on the requested transport. Matching IP/TTL on another process or
// another decision is insufficient. B may advance during A's check.
func (s *Service) witnessHostedDNSQuery(ctx context.Context, p routeartifact.HostedSelectedPacket, part routeartifact.HostedDeliveryPart, k hostedZoneKey, transport string) (hostedDNSWireEvidence, error) {
	var zero hostedDNSWireEvidence
	if transport != "udp" && transport != "tcp" {
		return zero, errors.New("invalid witness transport")
	}
	addr := s.Config.UDPAddr
	if transport == "tcp" {
		addr = s.Config.TCPAddr
	}
	addr, e := localHostedDNSAddress(addr)
	if e != nil {
		return zero, e
	}
	idx := s.hostedDNS.Load()
	if idx == nil || part.Recipient.Zone != k.zone || part.Recipient.Node != s.Config.DNSNodeID {
		return zero, errors.New("no local DNS selection")
	}
	w := idx.keys[k]
	member := w.forHost(p.Delivery.Host)
	if member == nil || len(w.rows) != 1 || member.Delivery.Member != p.Delivery.Member || member.Fence != p.Fence || member.Decision != p.Decision {
		return zero, errors.New("DNS selection not loaded for witness")
	}
	bound := false
	for _, local := range member.Delivery.Parts {
		if local.Recipient == part.Recipient && local.ContentDigest == part.ContentDigest {
			bound = true
			break
		}
	}
	pin, e := member.Delivery.PartKey(part)
	if !bound || e != nil {
		return zero, errors.New("DNS witness does not own part")
	}
	var nonce [16]byte
	if _, e = rand.Read(nonce[:]); e != nil {
		return zero, e
	}
	token := hex.EncodeToString(nonce[:])
	q := new(dns.Msg).SetQuestion(dns.Fqdn(k.name), k.family)
	q.SetEdns0(1232, false)
	q.IsEdns0().Option = append(q.IsEdns0().Option, &dns.EDNS0_LOCAL{Code: hostedDNSChallengeOption, Data: nonce[:]})
	at := time.Now().UTC()
	current := func(now time.Time) bool {
		base := s.currentDNSServingState()
		routes := s.hostedDNS.Load()
		if base == nil || routes == nil {
			return false
		}
		st := routes.forQuery(base, q, now)
		return !st.hostedBlocked && st.hostedWire == w
	}
	if !current(at) {
		return zero, errors.New("DNS witness is not on the selected zone or release")
	}
	pending := &hostedDNSWitness{k, w, transport, q.Id, make(chan hostedDNSWrite, 1)}
	if _, exists := s.hostedDNSWitnesses.LoadOrStore(token, pending); exists {
		return zero, errors.New("duplicate DNS challenge")
	}
	defer s.hostedDNSWitnesses.Delete(token)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	response, _, e := (&dns.Client{Net: transport}).ExchangeContext(ctx, q, addr)
	if e != nil {
		return zero, e
	}
	if response == nil || response.Truncated || !response.Authoritative || len(response.Question) != 1 || response.Question[0] != q.Question[0] || response.Id != q.Id || response.Rcode != dns.RcodeSuccess || len(response.Answer) == 0 {
		return zero, errors.New("DNS listener did not return a complete authorized RRset")
	}
	var wrote hostedDNSWrite
	select {
	case wrote = <-pending.wrote:
	case <-ctx.Done():
		return zero, ctx.Err()
	}
	now := time.Now().UTC()
	if wrote.rcode != response.Rcode || wrote.at.Before(at) || wrote.at.After(now) || !slices.Equal(wrote.rrs, dnsRRStrings(response.Answer)) || !current(now) {
		return zero, errors.New("listener write, wire answer or selected host changed")
	}
	until := member.ValidUntil
	for _, rr := range response.Answer {
		h := rr.Header()
		if normalizeName(h.Name) != k.name || h.Rrtype != k.family || h.Class != dns.ClassINET || h.Ttl == 0 || uint64(h.Ttl) > uint64(w.rows[0].record.TTL) {
			return zero, errors.New("wire witness served another key or TTL")
		}
		end := wrote.at.Add(time.Duration(h.Ttl) * time.Second)
		if end.Before(until) {
			until = end
		}
	}
	if !until.After(now) {
		return zero, errors.New("wire witness expired")
	}
	return hostedDNSWireEvidence{Host: p.Delivery.Host, Decision: p.Decision, Member: p.Delivery.Member, Part: pin, Content: part.ContentDigest, Zone: k.zone, Name: k.name, Family: dns.TypeToString[k.family], Transport: transport, Nonce: token, Fence: p.Fence, RRSet: wrote.rrs, ObservedAt: wrote.at, ValidUntil: until}, nil
}
