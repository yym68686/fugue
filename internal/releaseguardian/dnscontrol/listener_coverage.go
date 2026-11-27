package dnscontrol

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/productioncontrol"
	"github.com/miekg/dns"
)

const listenerCoverageSchema = "fugue.dns-listener-coverage/v1"
const maxListenerChecks = 8192

type listenerCheck struct {
	Node    string   `json:"node"`
	Address string   `json:"address"`
	Network string   `json:"network"`
	Name    string   `json:"name"`
	Type    uint16   `json:"type"`
	Serial  uint32   `json:"serial,omitempty"`
	Primary string   `json:"primary,omitempty"`
	IPs     []string `json:"ips,omitempty"`
}

// ListenerCoverage records a successful UDP/TCP check of every listener in
// the DNS member. ChecksDigest hashes the plan whose SOA and probe answers
// were matched exactly on the wire.
type ListenerCoverage struct {
	Schema           string `json:"schema"`
	CheckpointDigest string `json:"checkpointDigest"`
	ChecksDigest     string `json:"checksDigest"`
	CheckCount       int    `json:"checkCount"`
}

func (r ListenerCoverage) Digest() (string, error) {
	if r.Schema != listenerCoverageSchema || r.CheckCount < 1 || r.CheckCount > maxListenerChecks ||
		!platformproducer.ValidDigest(r.CheckpointDigest) || !platformproducer.ValidDigest(r.ChecksDigest) {
		return "", errors.New("invalid authoritative DNS listener coverage")
	}
	return platformconfig.Digest(r)
}

func buildListenerChecks(p platformconfig.DNSArtifactPayload, sequence int64) ([]listenerCheck, error) {
	authorityKey := func(node, zone string) string { return node + "\x00" + zone }
	if sequence < 1 || len(p.Policy.DNSAuthorities) == 0 || len(p.ConsumerViews) != len(p.Policy.DNSAuthorities) ||
		platformconfig.ValidateDNSAuthorities(p.Policy.DNSAuthorities) != nil ||
		platformconfig.ValidateDNSConsumerViews(p.ConsumerViews, p.Records) != nil {
		return nil, errors.New("verified DNS member lacks complete authority and listener inventory")
	}
	authorities := make(map[string]string, len(p.Policy.DNSAuthorities))
	for _, a := range p.Policy.DNSAuthorities {
		authorities[authorityKey(a.NodeID, a.Zone)] = dns.Fqdn(a.Nameservers[0])
	}
	var checks []listenerCheck
	for _, view := range p.ConsumerViews {
		key := authorityKey(view.NodeID, view.Zone)
		primary, ok := authorities[key]
		if !ok {
			return nil, errors.New("DNS listener view does not match its zone authority")
		}
		delete(authorities, key)
		for _, endpoint := range view.Records {
			for _, address := range endpoint.Values {
				for _, transport := range []string{"tcp", "udp"} {
					checks = append(checks, listenerCheck{Node: view.NodeID, Address: address, Network: transport,
						Name: dns.Fqdn(view.Zone), Type: dns.TypeSOA, Serial: uint32(sequence), Primary: primary})
					for _, record := range view.Records {
						ips := slices.Clone(record.Values)
						slices.Sort(ips)
						checks = append(checks, listenerCheck{Node: view.NodeID, Address: address, Network: transport,
							Name: dns.Fqdn(record.Hostname), Type: dns.StringToType[record.Type], IPs: ips})
					}
					if len(checks) > maxListenerChecks {
						return nil, errors.New("DNS listener coverage exceeds the maximum supported check count")
					}
				}
			}
		}
	}
	slices.SortFunc(checks, func(a, b listenerCheck) int {
		if c := strings.Compare(a.Node+"\x00"+a.Address+"\x00"+a.Network+"\x00"+a.Name, b.Node+"\x00"+b.Address+"\x00"+b.Network+"\x00"+b.Name); c != 0 {
			return c
		}
		return int(a.Type) - int(b.Type)
	})
	return checks, nil
}

type listenerExchange func(context.Context, string, string, *dns.Msg) (*dns.Msg, error)

func exchangeListener(ctx context.Context, transport, addr string, req *dns.Msg) (*dns.Msg, error) {
	c := dns.Client{Net: transport, Timeout: 2 * time.Second}
	response, _, err := c.ExchangeContext(ctx, req, net.JoinHostPort(addr, "53"))
	return response, err
}

func (c listenerCheck) observe(ctx context.Context, exchange listenerExchange) error {
	req := new(dns.Msg)
	req.SetQuestion(c.Name, c.Type)
	req.RecursionDesired = false
	if c.Network == "udp" {
		req.SetEdns0(1232, false)
	}
	r, err := exchange(ctx, c.Network, c.Address, req)
	if err != nil || r == nil || !r.Response || r.Id != req.Id || r.Opcode != req.Opcode ||
		!r.Authoritative || r.Truncated || r.Rcode != dns.RcodeSuccess ||
		len(r.Question) != 1 || r.Question[0] != req.Question[0] || len(r.Answer) == 0 {
		return fmt.Errorf("DNS listener %s at %s did not return an authoritative %s answer for %s over %s", c.Node, c.Address, dns.TypeToString[c.Type], c.Name, c.Network)
	}
	var ips []string
	for _, rr := range r.Answer {
		h := rr.Header()
		if !strings.EqualFold(h.Name, c.Name) || h.Class != dns.ClassINET || h.Rrtype != c.Type {
			return errors.New("authoritative DNS listener returned a different owner, class, or record type")
		}
		var ip net.IP
		switch a := rr.(type) {
		case *dns.SOA:
			if a.Serial != c.Serial || !strings.EqualFold(a.Ns, c.Primary) || len(r.Answer) != 1 {
				return errors.New("authoritative DNS listener serves a different SOA generation or primary")
			}
			return nil
		case *dns.A:
			ip = a.A
		case *dns.AAAA:
			ip = a.AAAA
		default:
			return errors.New("unexpected authoritative DNS probe answer")
		}
		addr, ok := netip.AddrFromSlice(ip)
		if !ok {
			return errors.New("invalid DNS listener probe address")
		}
		ips = append(ips, addr.Unmap().String())
	}
	slices.Sort(ips)
	if !slices.Equal(ips, c.IPs) {
		return errors.New("authoritative DNS probe answers differ from the verified listener inventory")
	}
	return nil
}

func runListenerChecks(ctx context.Context, checks []listenerCheck, exchange listenerExchange) error {
	if len(checks) == 0 {
		return errors.New("empty DNS listener coverage plan")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan listenerCheck)
	failure := make(chan error, 1)
	var workers sync.WaitGroup
	for range min(16, len(checks)) {
		workers.Go(func() {
			for check := range jobs {
				if err := check.observe(ctx, exchange); err != nil {
					select {
					case failure <- err:
					default:
					}
					cancel()
					return
				}
			}
		})
	}
send:
	for _, check := range checks {
		select {
		case jobs <- check:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	workers.Wait()
	select {
	case err := <-failure:
		return err
	default:
		return ctx.Err()
	}
}

// ObserveListenerCoverage repeats the exact checkpoint after checking every
// verified endpoint. The returned report establishes listener identity and
// artifact generation. Application answer/withdrawal evidence is separate.
func (r *Reader) ObserveListenerCoverage(ctx context.Context, j *productioncontrol.Journal, cluster, intent string, now func() time.Time) (ServingCheckpoint, ListenerCoverage, error) {
	var payload platformconfig.DNSArtifactPayload
	checkpoint, err := r.observeServingCheckpoint(ctx, j, cluster, intent, now, &payload)
	if err != nil {
		return ServingCheckpoint{}, ListenerCoverage{}, err
	}
	checks, err := buildListenerChecks(payload, checkpoint.DNS.Sequence)
	if err != nil {
		return ServingCheckpoint{}, ListenerCoverage{}, err
	}
	if err := runListenerChecks(ctx, checks, exchangeListener); err != nil {
		return ServingCheckpoint{}, ListenerCoverage{}, err
	}
	checkpointDigest, err := checkpoint.Digest()
	if err != nil {
		return ServingCheckpoint{}, ListenerCoverage{}, err
	}
	current, err := r.ObserveServingCheckpoint(ctx, j, cluster, intent, now)
	if err != nil {
		return ServingCheckpoint{}, ListenerCoverage{}, err
	}
	currentDigest, err := current.Digest()
	if err != nil || currentDigest != checkpointDigest {
		return ServingCheckpoint{}, ListenerCoverage{}, errors.New("verified DNS checkpoint changed during listener coverage")
	}
	checksDigest, err := platformconfig.Digest(checks)
	if err != nil {
		return ServingCheckpoint{}, ListenerCoverage{}, err
	}
	return checkpoint, ListenerCoverage{
		Schema: listenerCoverageSchema, CheckpointDigest: checkpointDigest, ChecksDigest: checksDigest, CheckCount: len(checks),
	}, nil
}
