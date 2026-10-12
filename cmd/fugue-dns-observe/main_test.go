package main

import (
	"testing"

	"github.com/miekg/dns"
)

func TestDNSObservationNeverAcceptsUnexpectedIdentityOrAddress(t *testing.T) {
	query := queryIntent{Hostname: "app.example.test", Type: "A", Allowed: []string{"192.0.2.2"}, Minimum: 1}
	request := new(dns.Msg)
	request.SetQuestion(dns.Fqdn(query.Hostname), dns.TypeA)
	request.RecursionDesired = false
	for _, scenario := range []string{"valid", "wrong_id", "wrong_question", "old_address", "servfail", "not_authoritative", "empty", "truncated"} {
		answer := new(dns.Msg)
		answer.SetReply(request)
		answer.Authoritative = true
		record, err := dns.NewRR("app.example.test. 60 IN A 192.0.2.2")
		if err != nil {
			t.Fatal(err)
		}
		answer.Answer = []dns.RR{record}
		switch scenario {
		case "wrong_id":
			answer.Id++
		case "wrong_question":
			answer.Question[0].Name = "other.example.test."
		case "old_address":
			answer.Answer[0], _ = dns.NewRR("app.example.test. 60 IN A 192.0.2.1")
		case "servfail":
			answer.Rcode = dns.RcodeServerFailure
		case "not_authoritative":
			answer.Authoritative = false
		case "empty":
			answer.Answer = nil
		case "truncated":
			answer.Truncated = true
		}
		result := observation{}
		evaluate(answer, request, query, &result)
		if result.Passed != (scenario == "valid") {
			t.Fatal(scenario, result)
		}
	}
}

func TestRecursiveSamplesDoNotRequireAuthoritativeBitOrAuthorizeTraffic(t *testing.T) {
	request := new(dns.Msg)
	request.SetQuestion("app.example.test.", dns.TypeA)
	request.RecursionDesired = true
	answer := new(dns.Msg)
	answer.SetReply(request)
	answer.RecursionAvailable = true
	record, err := dns.NewRR("app.example.test. 17 IN A 192.0.2.2")
	if err != nil {
		t.Fatal(err)
	}
	answer.Answer = []dns.RR{record}
	intent := queryIntent{Hostname: "app.example.test", Type: "A", Allowed: []string{"192.0.2.2"}, Minimum: 1}
	result := observation{}
	evaluate(answer, request, intent, &result)
	if !result.Passed || result.Authoritative || len(result.TTLs) != 1 || result.TTLs[0] != 17 {
		t.Fatal("recursive sample lost cache TTL or authority boundary", result)
	}
	answer.RecursionAvailable = false
	result = observation{}
	evaluate(answer, request, intent, &result)
	if result.Passed {
		t.Fatal("non-recursive endpoint accepted")
	}
}
