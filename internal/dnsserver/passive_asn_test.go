package dnsserver

import (
	"net"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/passiveasn"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

func passiveASNTestRuntime(t *testing.T, mode string, wins bool) (*dnsServingState, *dnsPassiveRuntime, time.Time) {
	t.Helper()
	st, now := decisionTestState(t)
	st.payload.Policy.DNSQueryPolicy = &platformconfig.DNSQueryPolicy{
		DynamicQuality:    &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic"},
		OrderedProjection: &platformconfig.DNSOrderedProjection{},
	}
	st.record.Candidate.Assignment.ReleaseSetID = "parent-a"
	st.record.Candidate.Assignment.ExpectedConsumerSetID = "set-a"
	st.record.Candidate.Assignment.FencingToken = 1
	artifact := st.record.Candidate.Artifact
	prefix := netip.MustParsePrefix("203.0.113.0/24")
	p := passiveasn.Publication{Mode: mode, DNSArtifactID: artifact.ID, DNSDigest: artifact.ContentHash, ReleaseSetID: "parent-a", NodeID: "dns-a", ConsumerSetID: "set-a", FencingToken: 1,
		CohortExploration: true,
		ObservedAt:        now.Add(-time.Second), ExpiresAt: now.Add(18 * time.Second), ResolverSourceAt: now.Add(-time.Hour),
		Bindings: []passiveasn.Binding{{Prefix: prefix, ASN: 64496}}, Resolvers: []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}}
	if wins {
		p.Pairs = []edgequality.PassiveASNComparison{{ASN: 64496, Family: 4,
			Left:      edgequality.PassiveEndpoint{EdgeID: "edge-a", GroupID: "edge-group-a", Address: "8.8.8.8"},
			Right:     edgequality.PassiveEndpoint{EdgeID: "edge-b", GroupID: "edge-group-b", Address: "9.9.9.9"},
			Preferred: "edge-b", SharedWindows: 3, RightWinWindows: 3, ObservedAt: now.Add(-time.Hour), Prefixes: []netip.Prefix{prefix}}}
	}
	keys := bundleauth.NewKeyring("test-key", "test-id", "", "", nil)
	signed, err := passiveasn.Sign(p, keys)
	if err != nil {
		t.Fatal(err)
	}
	view, err := passiveasn.Verify(signed, model.PlatformConsumerAssignment{ArtifactID: artifact.ID, ContentHash: artifact.ContentHash, ReleaseSetID: "parent-a", ExpectedConsumerSetID: "set-a", FencingToken: 1}, "dns-a", keys, now)
	if err != nil {
		t.Fatal(err)
	}
	return st, &dnsPassiveRuntime{view: view, node: "dns-a"}, now
}

func passiveASNTestQuery(subnet string, bits uint8) *dns.Msg {
	req := new(dns.Msg)
	req.SetQuestion("target.example.test.", dns.TypeA)
	if subnet != "" {
		req.SetEdns0(1232, false)
		req.IsEdns0().Option = append(req.IsEdns0().Option, &dns.EDNS0_SUBNET{Code: dns.EDNS0SUBNET, Family: 1, Address: net.ParseIP(subnet), SourceNetmask: bits})
	}
	return req
}

func TestPassiveASNWireSelectionAndReceipt(t *testing.T) {
	for _, test := range []struct {
		mode, subnet, remote, answer, reason string
		bits, scope                          uint8
		fixed                                bool
	}{
		{"active", "203.0.113.0", "198.51.100.2:53", "9.9.9.9", "passive_asn_preferred", 24, 24, false},
		{"active", "203.0.112.0", "198.51.100.2:53", "8.8.8.8", "ecs_prefix_too_broad", 23, 23, false},
		{"active", "203.0.113.0", "192.0.2.10:53", "8.8.8.8", "ecs_resolver_untrusted", 24, 24, false},
		{"active", "", "198.51.100.2:53", "8.8.8.8", "ecs_absent_or_invalid", 0, 0, false},
		{"active", "203.0.113.0", "198.51.100.2:53", "8.8.8.8", "record_not_eligible", 24, 0, true},
		{"shadow", "203.0.113.0", "198.51.100.2:53", "8.8.8.8", "passive_asn_preferred", 24, 24, false},
	} {
		t.Run(test.mode+"/"+test.reason, func(t *testing.T) {
			st, runtime, now := passiveASNTestRuntime(t, test.mode, true)
			if test.fixed {
				st.passiveFixed = map[string]bool{"target.example.test\x00A": true}
			}
			req := passiveASNTestQuery(test.subnet, test.bits)
			capture := &dnsDecisionCapture{input: dnsDecisionReplay{Version: DNSDecisionSchema}}
			ans := st.answerObserved(req, test.remote, now, capture, runtime)
			if ans.Rcode != dns.RcodeSuccess || len(ans.Answer) != 1 || ans.Answer[0].(*dns.A).A.String() != test.answer {
				t.Fatal("unexpected answer", ans)
			}
			if opt := ans.IsEdns0(); opt != nil && opt.Option[0].(*dns.EDNS0_SUBNET).SourceScope != test.scope {
				t.Fatal("incorrect cache scope", opt)
			}
			journal := decisionTestJournal(t)
			journal.enqueue(req, ans, capture, decisionPublicationState(st, nil), "udp", now, true)
			select {
			case pending := <-journal.queue:
				journal.append(pending)
			default:
				t.Fatal("receipt dropped")
			}
			receipts := *journal.records.Load()
			if len(receipts) != 1 || len(receipts[0].Records) != 1 || receipts[0].Records[0].PassiveASN == nil || receipts[0].Records[0].PassiveASN.Reason != test.reason {
				t.Fatal("missing passive reason", receipts)
			}
			if test.reason == "passive_asn_preferred" {
				got := receipts[0].Records[0].PassiveASN
				if got.EvidenceKind != "historical" || got.SharedWindows != 3 || got.ComparedChoice != "edge-b" {
					t.Fatal("missing historical provenance", got)
				}
			}
			if strings.Contains(string(receipts[0].ReplayInput), "198.51.100.") || strings.Contains(string(receipts[0].ReplayInput), "203.0.113.") {
				t.Fatal("receipt contains client or resolver prefix")
			}
			if replay, err := ReplayDNSDecision(receipts[0]); err != nil || !replay.Matched {
				t.Fatal("wire decision not replayable", err)
			}
		})
	}
}

func TestPassiveASNRejoinsOnlyAfterRouteProofRecovers(t *testing.T) {
	st, runtime, now := passiveASNTestRuntime(t, "active", true)
	zone := st.zones["example.test"]
	entries := zone.records["target.example.test"]
	entry := &entries[0]
	entry.plan.Records[0].MinimumHealthyEdges = 1
	ready := slices.Clone(entry.facts)
	entry.facts = slices.DeleteFunc(slices.Clone(ready), func(f dnsReadinessFact) bool { return f.Proof.EdgeID == "edge-b" })
	req := passiveASNTestQuery("203.0.113.0", 24)
	wantAnswer := func(at time.Time, answer string) {
		t.Helper()
		res := st.answerObserved(req, "198.51.100.2:53", at, nil, runtime)
		wire, err := res.Pack()
		var decoded dns.Msg
		if err != nil || decoded.Unpack(wire) != nil || len(decoded.Answer) != 1 || decoded.Answer[0].(*dns.A).A.String() != answer {
			t.Fatalf("want %s, got %v", answer, &decoded)
		}
	}
	wantAnswer(now, "8.8.8.8")
	entry.facts = ready
	wantAnswer(now, "9.9.9.9")
	runtime.view.ExpiresAt = now.Add(-time.Second)
	wantAnswer(now, "8.8.8.8") // lease expired; quality history did not
}

func TestPassiveASNBoundedExploration(t *testing.T) {
	st, runtime, now := passiveASNTestRuntime(t, "active", false)
	req := passiveASNTestQuery(exploredPrefix(t, now), 24)
	count := 0
	for range 20 {
		res := st.answerObserved(req, "198.51.100.2:53", now, nil, runtime)
		if len(res.Answer) != 1 {
			t.Fatal(res)
		}
		if res.Answer[0].(*dns.A).A.String() == "9.9.9.9" {
			count++
		}
	}
	if count != 20 {
		t.Fatalf("stable exploration split one cohort: %d answers", count)
	}
	count = 0
	for i := 0; i < 4096; i++ {
		ip := netip.AddrFrom4([4]byte{11, byte(i >> 8), byte(i), 0})
		if passiveExploration(netip.PrefixFrom(ip, 24), now) > 0 {
			count++
		}
	}
	if count < 140 || count > 280 {
		t.Fatalf("exploration exceeds expected cohort budget: %d", count)
	}
}

func exploredPrefix(t *testing.T, now time.Time) string {
	t.Helper()
	for i := 0; i < 4096; i++ {
		ip := netip.AddrFrom4([4]byte{11, byte(i >> 8), byte(i), 0})
		if passiveExploration(netip.PrefixFrom(ip, 24), now) > 0 {
			return ip.String()
		}
	}
	t.Fatal("no explored cohort")
	return ""
}

func TestPassiveASNAggregateFallbackOnWire(t *testing.T) {
	st, rt, now := passiveASNTestRuntime(t, "active", true)
	p := rt.view.Publication
	pair := p.Pairs[0]
	for i, address := range []string{"192.0.2.0/24", "9.9.9.0/24", "1.1.1.0/24"} {
		next := pair
		next.ASN += uint32(i + 1)
		next.Prefixes = []netip.Prefix{netip.MustParsePrefix(address)}
		if i == 2 {
			// This ASN disagrees. It must retain its own choice rather than
			// have the average (3 out of 4) overwrite it.
			next.LeftWinWindows, next.RightWinWindows, next.Preferred = 3, 0, "edge-a"
		}
		p.Pairs = append(p.Pairs, next)
		p.Bindings = append(p.Bindings, passiveasn.Binding{Prefix: next.Prefixes[0], ASN: next.ASN})
	}
	keys := bundleauth.NewKeyring("test-key", "test-id", "", "", nil)
	a, err := passiveasn.Sign(p, keys)
	if err != nil {
		t.Fatal(err)
	}
	assignment := st.record.Candidate.Assignment
	assignment.ArtifactID, assignment.ContentHash = st.record.Candidate.Artifact.ID, st.record.Candidate.Artifact.ContentHash
	rt.view, err = passiveasn.Verify(a, assignment, "dns-a", keys, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ subnet, remote, answer, why string }{
		{"8.8.4.0", "198.51.100.2:53", "9.9.9.9", "passive_aggregate_preferred"},
		{"1.1.1.0", "198.51.100.2:53", "8.8.8.8", "retained_no_challenger_advantage"},
		{"8.8.4.0", "192.0.2.10:53", "8.8.8.8", "ecs_resolver_untrusted"},
		{"", "198.51.100.2:53", "9.9.9.9", "passive_aggregate_preferred"},
	} {
		t.Run(test.why, func(t *testing.T) {
			req := passiveASNTestQuery(test.subnet, 24)
			capture := &dnsDecisionCapture{input: dnsDecisionReplay{Version: DNSDecisionSchema}}
			res := st.answerObserved(req, test.remote, now, capture, rt)
			wire, err := res.Pack()
			var decoded dns.Msg
			if err != nil || decoded.Unpack(wire) != nil || len(decoded.Answer) != 1 || decoded.Answer[0].(*dns.A).A.String() != test.answer {
				t.Fatalf("unexpected wire response %v, %v", res, err)
			}
			journal := decisionTestJournal(t)
			journal.enqueue(req, res, capture, decisionPublicationState(st, nil), "tcp", now, true)
			select {
			case pending := <-journal.queue:
				journal.append(pending)
			default:
				t.Fatal("missing receipt")
			}
			receipt := (*journal.records.Load())[0]
			decision := receipt.Records[0].PassiveASN
			if decision == nil || decision.Reason != test.why {
				t.Fatal("wrong decision", decision)
			}
			if test.why == "passive_aggregate_preferred" {
				opt := decoded.IsEdns0()
				if decision.AggregateASNs != 4 || decision.AggregateSupportingASNs != 3 || decision.ASN != 0 ||
					decision.EvidenceKind != "historical" || test.subnet == "" && opt != nil ||
					test.subnet != "" && (opt == nil || opt.Option[0].(*dns.EDNS0_SUBNET).SourceScope != 24) {
					t.Fatal("lost aggregate evidence or unsafe scope", decision, opt)
				}
			}
			if replay, err := ReplayDNSDecision(receipt); err != nil || !replay.Matched {
				t.Fatal("receipt did not replay", err)
			}
		})
	}
}

func TestPassiveASNExploresTrustedColdPrefixesWithoutOverridingFixedRecords(t *testing.T) {
	st, rt, now := passiveASNTestRuntime(t, "active", false)
	req := passiveASNTestQuery(exploredPrefix(t, now), 24)
	for _, fixed := range []bool{false, true} {
		st.passiveFixed = map[string]bool{"target.example.test\x00A": fixed}
		got := st.answerObserved(req, "198.51.100.2:53", now, nil, rt)
		expect := "9.9.9.9"
		if fixed {
			expect = "8.8.8.8"
		}
		if len(got.Answer) != 1 || got.Answer[0].(*dns.A).A.String() != expect {
			t.Fatal("cold exploration violated the policy", got)
		}
	}
}

func TestPassiveASNDoesNotExploreAFrontWithAnActiveHold(t *testing.T) {
	st, rt, now := passiveASNTestRuntime(t, "active", false)
	p := rt.view.Publication
	p.ExplorationHolds = []edgequality.PassiveEndpoint{{EdgeID: "edge-b", GroupID: "edge-group-b", Address: "9.9.9.9"}}
	keys := bundleauth.NewKeyring("test-key", "test-id", "", "", nil)
	signed, err := passiveasn.Sign(p, keys)
	if err != nil {
		t.Fatal(err)
	}
	assignment := st.record.Candidate.Assignment
	assignment.ArtifactID, assignment.ContentHash = st.record.Candidate.Artifact.ID, st.record.Candidate.Artifact.ContentHash
	rt.view, err = passiveasn.Verify(signed, assignment, "dns-a", keys, now)
	if err != nil {
		t.Fatal(err)
	}
	query := passiveASNTestQuery(exploredPrefix(t, now), 24)
	capture := &dnsDecisionCapture{input: dnsDecisionReplay{Version: DNSDecisionSchema}}
	resp := st.answerObserved(query, "198.51.100.2:53", now, capture, rt)
	if len(resp.Answer) != 1 || resp.Answer[0].(*dns.A).A.String() != "8.8.8.8" {
		t.Fatal("held Front received exploration", resp)
	}
}
