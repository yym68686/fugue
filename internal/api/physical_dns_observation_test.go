package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

func TestPhysicalDNSProbeRequiresFreshUniquePublicConsumer(t *testing.T) {
	now := time.Now().UTC()
	consumer := platformconfig.DNSConsumerObservation{NodeID: "dns-a", ObservedAt: now, A: []string{"8.8.4.4"}}
	if address, err := physicalDNSObservationAddress([]platformconfig.DNSConsumerObservation{consumer}, consumer.NodeID, now); err != nil || address != "8.8.4.4:53" {
		t.Fatal(address, err)
	}
	for _, scenario := range []string{"missing", "duplicate", "multiple", "private", "future", "stale"} {
		t.Run(scenario, func(t *testing.T) {
			values := []platformconfig.DNSConsumerObservation{consumer}
			switch scenario {
			case "missing":
				values = nil
			case "duplicate":
				values = append(values, consumer)
			case "multiple":
				values[0].A = []string{"8.8.4.4", "1.1.1.1"}
			case "private":
				values[0].A = []string{"127.0.0.1"}
			case "future":
				values[0].ObservedAt = now.Add(time.Second)
			case "stale":
				values[0].ObservedAt = now.Add(-3 * time.Minute)
			}
			if _, err := physicalDNSObservationAddress(values, consumer.NodeID, now); err == nil {
				t.Fatal("untrusted observation became a probe destination")
			}
		})
	}
}

func TestPhysicalDNSObservationBindsExactWireAnswerAndTime(t *testing.T) {
	now := time.Now().UTC()
	base := dnsserver.DNSDecisionReceipt{NodeID: "dns-a", Hostname: "app.example.test", QueryID: 17, QType: dns.TypeA, Transport: "tcp", ObservedAt: now, WriteSucceeded: true,
		RRSet: []string{"app.example.test.\t60\tIN\tA\t192.0.2.1"}}
	match := func(receipt dnsserver.DNSDecisionReceipt) bool {
		return physicalDNSObservationMatches(receipt, base.NodeID, base.Hostname, base.QueryID, dns.RcodeSuccess, base.RRSet, now, now.Add(time.Second))
	}
	if !match(base) {
		t.Fatal("exact wire response rejected")
	}
	for _, edit := range []func(*dnsserver.DNSDecisionReceipt){
		func(value *dnsserver.DNSDecisionReceipt) { value.NodeID = "dns-b" },
		func(value *dnsserver.DNSDecisionReceipt) { value.Hostname = "other.example.test" },
		func(value *dnsserver.DNSDecisionReceipt) { value.QueryID++ },
		func(value *dnsserver.DNSDecisionReceipt) { value.QType = dns.TypeAAAA },
		func(value *dnsserver.DNSDecisionReceipt) { value.Transport = "udp" },
		func(value *dnsserver.DNSDecisionReceipt) { value.ObservedAt = now.Add(-time.Minute) },
		func(value *dnsserver.DNSDecisionReceipt) { value.ObservedAt = now.Add(time.Minute) },
		func(value *dnsserver.DNSDecisionReceipt) { value.WriteSucceeded = false },
		func(value *dnsserver.DNSDecisionReceipt) { value.RCode = dns.RcodeServerFailure },
		func(value *dnsserver.DNSDecisionReceipt) {
			value.RRSet = []string{"app.example.test.\t59\tIN\tA\t192.0.2.1"}
		},
	} {
		changed := base
		edit(&changed)
		if match(changed) {
			t.Fatal("unrelated or changed answer accepted", changed)
		}
	}
}

func TestPhysicalDNSProbeRejectsUnreplayableAndUnavailableAnswers(t *testing.T) {
	for _, scenario := range []string{"network", "non_authoritative", "truncated", "wrong_query", "servfail", "journal_error", "no_receipt", "unreplayable", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			var receipt dnsserver.DNSDecisionReceipt
			calls, reads := 0, 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			exchange := func(ctx context.Context, query *dns.Msg, address string) (*dns.Msg, time.Duration, error) {
				calls++
				if query.RecursionDesired || len(query.Extra) != 0 || query.Question[0].Qtype != dns.TypeA || address != "8.8.4.4:53" {
					t.Fatal("query changed scope or destination", query, address)
				}
				response := new(dns.Msg)
				response.SetReply(query)
				response.Authoritative = true
				record, _ := dns.NewRR("app.example.test. 60 IN A 192.0.2.1")
				response.Answer = []dns.RR{record}
				receipt = dnsserver.DNSDecisionReceipt{NodeID: "dns-a", Hostname: "app.example.test", QueryID: query.Id, QType: dns.TypeA, Transport: "tcp", ObservedAt: time.Now().UTC(), WriteSucceeded: true, RRSet: []string{record.String()}}
				switch scenario {
				case "network":
					return nil, 0, errors.New("offline")
				case "non_authoritative":
					response.Authoritative = false
				case "truncated":
					response.Truncated = true
				case "wrong_query":
					response.Id++
				case "servfail":
					response.Rcode = dns.RcodeServerFailure
				}
				return response, 0, nil
			}
			read := func(context.Context) (platformDNSDecisionResponse, error) {
				reads++
				if scenario == "journal_error" {
					return platformDNSDecisionResponse{}, errors.New("unavailable")
				}
				if scenario == "cancelled" {
					cancel()
				}
				out := platformDNSDecisionResponse{}
				if scenario == "unreplayable" {
					out.Snapshot.Receipts = []dnsserver.DNSDecisionReceipt{receipt}
				}
				return out, nil
			}
			if _, err := observePhysicalDNSQuery(ctx, "dns-a", "app.example.test", "8.8.4.4:53", exchange, read); err == nil || calls != 1 || reads > 3 {
				t.Fatal("unsafe answer accepted or unbounded probing", err, calls, reads)
			}
		})
	}
}
