package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"
)

type staticEdgeDNSWaiter func(context.Context, staticEdgeDNSBackend, staticEdgeCutoverOptions, string) (bool, error)

// Hosted DNS writes change intent first. Only authoritative UDP and TCP answers
// establish that the serving artifacts have converged; an API echo is not an ACK.
func waitStaticEdgeAuthoritative(ctx context.Context, backend staticEdgeDNSBackend, o staticEdgeCutoverOptions, target string) (bool, error) {
	provider, ok := backend.(interface{ staticEdgeAuthoritativeNS() []string })
	if !ok {
		return false, nil
	} // Preserve the existing Cloudflare API-only contract.
	nameservers := provider.staticEdgeAuthoritativeNS()
	if len(nameservers) == 0 || len(nameservers) > 16 {
		return false, errors.New("hosted DNS requires 1-16 expected authoritative nameservers")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	timeout := o.Timeout
	if timeout <= 0 || timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	var addresses []string
	for _, ns := range nameservers {
		ns = strings.TrimSuffix(strings.TrimSpace(ns), ".")
		if err := validateStaticEdgeZone(ns); err != nil {
			return false, fmt.Errorf("invalid authoritative nameserver %q: %w", ns, err)
		}
		lookupCtx, stop := context.WithTimeout(ctx, timeout)
		ips, err := net.DefaultResolver.LookupIPAddr(lookupCtx, ns)
		stop()
		if err != nil {
			return false, fmt.Errorf("resolve nameserver %s: %w", ns, err)
		}
		if len(ips) == 0 || len(ips) > 8 {
			return false, fmt.Errorf("nameserver %s has no addresses or exceeds the address bound", ns)
		}
		for _, ip := range ips {
			addresses = append(addresses, net.JoinHostPort(ip.IP.String(), "53"))
		}
	}
	err := waitStaticEdgeDNSAnswers(ctx, addresses, o.Hostnames, target, timeout, 5*time.Second, exchangeStaticEdgeDNS)
	return err == nil, err
}

type staticEdgeDNSExchange func(context.Context, string, string, *dns.Msg, time.Duration) (*dns.Msg, error)

func exchangeStaticEdgeDNS(ctx context.Context, address, network string, query *dns.Msg, timeout time.Duration) (*dns.Msg, error) {
	answer, _, err := (&dns.Client{Net: network, Timeout: timeout}).ExchangeContext(ctx, query, address)
	return answer, err
}
func waitStaticEdgeDNSAnswers(ctx context.Context, addresses, hostnames []string, target string, timeout, interval time.Duration, exchange staticEdgeDNSExchange) error {
	if len(addresses) == 0 || len(hostnames) == 0 {
		return errors.New("authoritative DNS verification requires servers and hostnames")
	}
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("authoritative DNS propagation not verified (last observation: %v): %w", last, err)
		}
		last = checkStaticEdgeDNSAnswers(ctx, addresses, hostnames, target, timeout, exchange)
		if last == nil {
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("authoritative DNS propagation not verified (last observation: %v): %w", last, ctx.Err())
		case <-timer.C:
		}
	}
}
func checkStaticEdgeDNSAnswers(ctx context.Context, addresses, hostnames []string, target string, timeout time.Duration, exchange staticEdgeDNSExchange) error {
	for _, address := range addresses {
		for _, host := range hostnames {
			for _, network := range []string{"udp", "tcp"} {
				query := new(dns.Msg)
				query.SetQuestion(dns.Fqdn(host), dns.TypeA)
				query.RecursionDesired = false
				answer, err := exchange(ctx, address, network, query, timeout)
				if err != nil {
					return fmt.Errorf("%s %s via %s: %w", host, network, address, err)
				}
				if err = verifyStaticEdgeDNSAnswer(query, answer, target); err != nil {
					return fmt.Errorf("%s %s via %s: %w", host, network, address, err)
				}
			}
		}
	}
	return nil
}
func verifyStaticEdgeDNSAnswer(query, answer *dns.Msg, target string) error {
	if query == nil || len(query.Question) != 1 || answer == nil || !answer.Response || answer.Id != query.Id || !answer.Authoritative || answer.Truncated || answer.Rcode != dns.RcodeSuccess || len(answer.Question) != 1 || answer.Question[0] != query.Question[0] {
		return errors.New("missing, mismatched, non-authoritative or unsuccessful DNS response")
	}
	if len(answer.Answer) != 1 {
		return errors.New("expected exactly one authoritative A answer")
	}
	a, ok := answer.Answer[0].(*dns.A)
	if !ok || a.Hdr.Class != dns.ClassINET || !strings.EqualFold(a.Hdr.Name, query.Question[0].Name) || a.A.String() != target {
		return errors.New("authoritative answer has not reached the intended IPv4 target")
	}
	return nil
}
