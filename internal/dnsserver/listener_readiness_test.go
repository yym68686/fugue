package dnsserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

func TestListenerReadinessDoesNotSpreadOneRecordFailure(t *testing.T) {
	view, plan, policy, facts, now := queryExecutionFixture()
	missing := "sha256:" + strings.Repeat("f", 64)
	probe := plan.Probes[0]
	probe.ID, probe.Hostname = missing, "unready.example.test"
	plan.Probes = append(plan.Probes, probe)
	target := plan.Records[0].Targets[0]
	target.ProbeIDs = []string{missing}
	plan.Records = append(plan.Records, platformconfig.DNSReadinessRecord{Hostname: probe.Hostname, MinimumHealthyEdges: 1, Targets: []platformconfig.DNSReadinessTarget{target}})
	view.Records = append(view.Records, model.EdgeDNSRecord{Name: probe.Hostname, Type: target.Family, Values: []string{target.Address}, TTL: 60, Candidates: []model.EdgeDNSAnswerCandidate{{IP: target.Address, EdgeID: target.EdgeID, EdgeGroupID: target.EdgeGroupID}}})
	view.Records[len(view.Records)-1] = physicalRecordForTest(view.Records[len(view.Records)-1])
	payload := dnsServingPayload{Plan: &plan, Queries: []platformconfig.DNSQueryView{view}, Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &policy,
		DNSAuthorities:    []platformconfig.DNSAuthorityPolicy{{NodeID: view.NodeID, Zone: view.Zone, Nameservers: []string{"ns.example.test"}, TTLSeconds: 60}},
		DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: view.NodeID}},
	}}
	st, err := buildDNSServingState(dnsServingCheckpoint{Positive: true, AppliedAt: now}, payload, "route", view.NodeID, view.EdgeGroupID, facts, now)
	if err != nil {
		t.Fatal(err)
	}
	s := NewService(config.DNSConfig{DNSNodeID: view.NodeID, EdgeGroupID: view.EdgeGroupID, Zone: view.Zone}, nil)
	s.platformServingBound.Store(true)
	s.platformServing.Store(st)
	s.udpListening.Store(true)
	s.tcpListening.Store(true)
	call := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		return w
	}
	if response := call("/readyz"); response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("one record removed the listener", response.Code, response.Body.String())
	}
	if response := call("/healthz"); response.Code != http.StatusServiceUnavailable {
		t.Fatal("record failure was hidden from complete release health")
	}
	for _, tc := range []struct {
		host string
		code int
	}{{"target.example.test", dns.RcodeSuccess}, {probe.Hostname, dns.RcodeServerFailure}} {
		request := new(dns.Msg)
		request.SetQuestion(dns.Fqdn(tc.host), dns.TypeA)
		answer := st.answer(request, "", now)
		if answer.Rcode != tc.code || (tc.code == dns.RcodeSuccess && len(answer.Answer) == 0) {
			t.Fatalf("%s: %s", tc.host, answer)
		}
	}
	for _, scenario := range []string{"missing", "negative", "future", "expired", "listener", "listener failure"} {
		t.Run(scenario, func(t *testing.T) {
			copyState := *st
			s.platformServing.Store(&copyState)
			s.udpListening.Store(true)
			s.tcpListening.Store(true)
			s.listenerFailed.Store(false)
			switch scenario {
			case "missing":
				s.platformServing.Store(nil)
			case "negative":
				copyState.record.Positive = false
			case "future":
				copyState.record.AppliedAt = time.Now().Add(time.Hour)
			case "expired":
				copyState.record.AppliedAt = time.Now().Add(-2 * time.Hour)
			case "listener":
				s.udpListening.Store(false)
			case "listener failure":
				s.listenerFailed.Store(true)
			}
			if response := call("/readyz"); response.Code != http.StatusServiceUnavailable {
				t.Fatal("unavailable or unauthorized listener became ready")
			}
		})
	}
}

func TestDNSListenerFactsFollowActualStartAndShutdown(t *testing.T) {
	s := NewService(config.DNSConfig{UDPAddr: "127.0.0.1:0", TCPAddr: "127.0.0.1:0"}, nil)
	stop, err := s.startDNSServers()
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	deadline := time.Now().Add(time.Second)
	for !s.udpListening.Load() || !s.tcpListening.Load() {
		if time.Now().After(deadline) {
			t.Fatal("live DNS listeners never became ready")
		}
		time.Sleep(time.Millisecond)
	}
	stop()
	if s.udpListening.Load() || s.tcpListening.Load() {
		t.Fatal("closed DNS listener still reported ready")
	}
}
