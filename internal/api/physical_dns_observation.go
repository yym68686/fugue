package api

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

func (s *Server) observePhysicalDNSAnswer(ctx context.Context, consumers []platformconfig.DNSConsumerObservation, nodeID, hostname string) (dnsserver.DNSDecisionReceipt, error) {
	address, err := physicalDNSObservationAddress(consumers, nodeID, time.Now().UTC())
	if err != nil {
		return dnsserver.DNSDecisionReceipt{}, err
	}
	client := &dns.Client{Net: "tcp", Timeout: 2 * time.Second}
	read := func(ctx context.Context) (platformDNSDecisionResponse, error) {
		return s.readPlatformDNSDecisions(ctx, nodeID, hostname, "", 20)
	}
	return observePhysicalDNSQuery(ctx, nodeID, hostname, address, client.ExchangeContext, read)
}

func physicalDNSObservationAddress(consumers []platformconfig.DNSConsumerObservation, nodeID string, now time.Time) (string, error) {
	address := ""
	for _, consumer := range consumers {
		if consumer.NodeID != nodeID {
			continue
		}
		if address != "" || len(consumer.A) != 1 || consumer.ObservedAt.After(now) || now.Sub(consumer.ObservedAt) > 2*time.Minute {
			return "", errors.New("physical DNS endpoint observation is ambiguous or stale")
		}
		ip, err := netip.ParseAddr(consumer.A[0])
		if err != nil || !ip.Is4() || !platformconfig.PublicDNSFlattenIP(ip) {
			return "", errors.New("physical DNS endpoint is not a public IPv4 address")
		}
		address = net.JoinHostPort(ip.String(), "53")
	}
	if address == "" {
		return "", errors.New("physical DNS endpoint observation is missing")
	}
	return address, nil
}

func observePhysicalDNSQuery(ctx context.Context, nodeID, hostname, address string, exchange func(context.Context, *dns.Msg, string) (*dns.Msg, time.Duration, error), read func(context.Context) (platformDNSDecisionResponse, error)) (dnsserver.DNSDecisionReceipt, error) {
	query := new(dns.Msg)
	query.SetQuestion(dns.Fqdn(hostname), dns.TypeA)
	query.RecursionDesired = false
	started := time.Now().UTC()
	response, _, err := exchange(ctx, query, address)
	if err != nil || response == nil || !response.Response || !response.Authoritative || response.Truncated || response.Id != query.Id || response.Rcode != dns.RcodeSuccess || !slices.Equal(response.Question, query.Question) || len(response.Answer) == 0 {
		return dnsserver.DNSDecisionReceipt{}, errors.New("physical DNS public TCP answer unavailable or invalid")
	}
	finished := time.Now().UTC()
	answers := make([]string, 0, len(response.Answer))
	for _, record := range response.Answer {
		answers = append(answers, record.String())
	}
	for attempt := 0; attempt < 3; attempt++ {
		observed, err := read(ctx)
		if err != nil {
			return dnsserver.DNSDecisionReceipt{}, err
		}
		for _, receipt := range observed.Snapshot.Receipts {
			if physicalDNSObservationMatches(receipt, nodeID, hostname, query.Id, response.Rcode, answers, started, finished) {
				if _, err := dnsserver.ReplayDNSDecision(receipt); err != nil {
					return dnsserver.DNSDecisionReceipt{}, errors.New("physical DNS actual answer does not replay")
				}
				return receipt, nil
			}
		}
		if attempt < 2 {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return dnsserver.DNSDecisionReceipt{}, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return dnsserver.DNSDecisionReceipt{}, errors.New("physical DNS query lacks its matching retained real answer")
}

func physicalDNSObservationMatches(receipt dnsserver.DNSDecisionReceipt, nodeID, hostname string, queryID uint16, rcode int, answers []string, started, finished time.Time) bool {
	return receipt.NodeID == nodeID && receipt.Hostname == hostname && receipt.QueryID == queryID && receipt.QType == dns.TypeA && receipt.Transport == "tcp" && receipt.WriteSucceeded &&
		!receipt.ObservedAt.Before(started.Add(-time.Second)) && !receipt.ObservedAt.After(finished.Add(time.Second)) && receipt.RCode == rcode && slices.Equal(receipt.RRSet, answers)
}
