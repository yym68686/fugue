package dnsserver

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/routeartifact"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

// Actual listeners & checkpoints, synthetic physical observations. DNS must
// not decide a route/certificate passed just because an IP is unchanged.
func TestHostedDNSWireIndependentForwardWithdrawalAndRestart(t *testing.T) {
	view, _, policy, _, at := queryExecutionFixture()
	// Linux selection clocks can carry sub-microsecond precision. The
	// signed artifact envelope has a PostgreSQL-compatible microsecond
	// timestamp, while the signed selection body preserves the instant.
	at = at.Truncate(time.Microsecond).Add(317 * time.Nanosecond)
	digest := func(x any) string {
		d, e := pc.Digest(x)
		if e != nil {
			t.Fatal(e)
		}
		return d
	}
	const A, B = "alpha.example.test", "beta.example.test"
	meta := config.DNSConfig{DNSNodeID: view.NodeID, EdgeGroupID: view.EdgeGroupID, CachePath: filepath.Join(t.TempDir(), "dns"), BundleSigningKey: "fixture-wire-secret", BundleSigningKeyID: "key"}
	s := NewService(meta, nil)
	a := model.PlatformConsumerAssignment{ReleaseSetID: "full", ExpectedConsumerSetID: "members", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: "global", ArtifactReleaseID: "release", Revision: 1, ArtifactID: "child", ContentHash: digest("child"), ReleaseChannel: "full", FencingToken: 4}
	parent := &dnsServingState{record: dnsServingCheckpoint{Positive: true, AppliedAt: at, Parent: model.PlatformArtifact{ID: a.ReleaseSetID, ContentHash: digest("parent")}, Candidate: dnsPlatformCandidate{Assignment: a, Artifact: model.PlatformArtifact{ID: a.ArtifactID, ContentHash: a.ContentHash}, Release: model.PlatformArtifactRelease{ReleaseChannel: "full", FencingToken: a.FencingToken}}}, payload: dnsServingPayload{Policy: pc.PolicySnapshot{DNSReadiness: &policy, MaxStaleSeconds: 600}}, zones: map[string]dnsServingZone{}, zoneOrder: []string{"delegated.example.test", view.Zone}}
	for _, zone := range parent.zoneOrder {
		parent.zones[zone] = dnsServingZone{authority: pc.DNSAuthorityPolicy{Zone: zone, Nameservers: []string{"ns.example.test"}, TTLSeconds: 60}, records: map[string][]dnsServingRecord{}}
	}
	type fixture struct {
		cp    platformconsumer.HostedCheckpoint
		base  map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput
		head  routeartifact.HostedSelectedPacket
		first string
	}
	makeHost := func(host string) *fixture {
		cp, e := s.dnsHostCheckpoint(host)
		if e != nil {
			t.Fatal(e)
		}
		cp.Serving = a
		f := &fixture{cp: cp, base: map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput{}}
		for _, zone := range parent.zoneOrder {
			q := view.Records[0]
			q.Name = host
			if zone != view.Zone {
				q.Name = host[:len(host)-len(view.Zone)] + zone
			}
			q.AppID = "shared-app"
			q.TenantID = "tenant"
			q.TTL = 20
			z := parent.zones[zone]
			z.records[q.Name] = []dnsServingRecord{{record: q}}
			v := view
			v.Zone = zone
			v.Records = []model.EdgeDNSRecord{q}
			_, plan, _, _, _ := queryExecutionFixture()
			plan.Records[0].Hostname = q.Name
			plan.Probes = nil
			for i, target := range plan.Records[0].Targets {
				plan.Records[0].Targets[i].ProbeIDs = nil
				for _, path := range []string{"/", "/health", "/v1", "/v2"} {
					pr := pc.DNSReadinessProbe{EdgeID: target.EdgeID, EdgeGroupID: target.EdgeGroupID, Address: target.Address, Hostname: host, Path: path, RouteDigest: digest([]string{host, path, "baseline"})}
					pr.ID, _ = pc.DNSReadinessProbeID(pr)
					plan.Probes = append(plan.Probes, pr)
					plan.Records[0].Targets[i].ProbeIDs = append(plan.Records[0].Targets[i].ProbeIDs, pr.ID)
				}
			}
			r := routeartifact.HostedRecipient{Component: cp.Who.Component, Kind: a.ArtifactKind, Node: view.NodeID, Group: view.EdgeGroupID, Zone: zone}
			f.base[r] = routeartifact.HostedRecipientInput{Records: []pc.DNSIntent{{Hostname: q.Name, Type: q.Type, TenantID: q.TenantID, AppID: q.AppID, Values: q.Values, TTL: q.TTL}}, DNS: &pc.HostedDNSContent{Plan: plan, Views: []pc.DNSQueryView{v}}}
		}
		return f
	}
	fa, fb := makeHost(A), makeHost(B)
	command := func(f *fixture, phase string, changed bool) (model.PlatformArtifact, routeartifact.HostedSelectedPacket, map[routeartifact.HostedRecipient][]dnsReadinessFact) {
		n := f.head.Fence + 1
		dec := digest([]any{f.cp.Host, n, phase})
		p := routeartifact.HostedSelectedPacket{Scope: "host-" + f.cp.Host, Project: "project", Draft: "draft", Offer: 1, Fence: n, Decision: dec, Authority: digest("authority"), Business: digest("business"), Parent: a.ReleaseSetID, Child: a.ArtifactID, ChildDigest: a.ContentHash, SelectedAt: at.Add(-time.Second), ValidUntil: at.Add(time.Minute), Delivery: routeartifact.HostedDelivery{Schema: "fugue.hosted-recipient-input/v1", Host: f.cp.Host, DraftDigest: digest("draft"), ImageDigest: digest("image"), Phase: phase, Member: f.cp.Who.ConsumerID(), MembershipSet: a.ExpectedConsumerSetID, MembershipRelease: a.ArtifactReleaseID, MembershipRevision: a.Revision, MembershipFence: a.FencingToken}}
		if n > 1 {
			p.Previous = f.head.Decision
			p.PriorParts = map[string]string{}
			for _, part := range f.head.Delivery.Parts {
				k, _ := f.head.Delivery.PartKey(part)
				p.PriorParts[k] = part.ContentDigest
			}
		}
		if phase == "withdraw" {
			p.SkippedForwardFrom = f.first
		}
		facts := map[routeartifact.HostedRecipient][]dnsReadinessFact{}
		for _, zone := range parent.zoneOrder {
			r := routeartifact.HostedRecipient{Component: f.cp.Who.Component, Kind: a.ArtifactKind, Node: view.NodeID, Group: view.EdgeGroupID, Zone: zone}
			content := f.base[r]
			u := *content.DNS
			content.DNS = &u
			plan := u.Plan
			plan.Probes = append([]pc.DNSReadinessProbe(nil), plan.Probes...)
			plan.Records = append([]pc.DNSReadinessRecord(nil), plan.Records...)
			plan.Records[0].Targets = append([]pc.DNSReadinessTarget(nil), plan.Records[0].Targets...)
			ids := map[string]string{}
			for i, pr := range plan.Probes {
				old := pr.ID
				if changed {
					pr.RouteDigest = digest([]string{f.cp.Host, pr.Path, "new-backend"})
					pr.ID, _ = pc.DNSReadinessProbeID(pr)
					plan.Probes[i] = pr
				}
				ids[old] = pr.ID
				facts[r] = append(facts[r], dnsReadinessFact{ProbeID: pr.ID, Ready: true, Proof: routeprobe.Proof{Kind: "hosted-route", Version: dec, Digest: pr.RouteDigest, EdgeID: pr.EdgeID, GroupID: pr.EdgeGroupID, CheckedAt: at, ValidUntil: at.Add(55 * time.Second)}})
			}
			for i, v := range plan.Records[0].Targets {
				plan.Records[0].Targets[i].ProbeIDs = nil
				for _, old := range v.ProbeIDs {
					plan.Records[0].Targets[i].ProbeIDs = append(plan.Records[0].Targets[i].ProbeIDs, ids[old])
				}
			}
			u.Plan = plan
			p.Delivery.Parts = append(p.Delivery.Parts, routeartifact.HostedDeliveryPart{Recipient: r, Content: content, ContentDigest: digest([]any{"fugue.hosted-recipient-part/v1", f.cp.Host, r, content})})
		}
		sig, e := routeartifact.SignHostedPacket(p, s.platformDNSKeys(), at)
		if e != nil {
			t.Fatal(e)
		}
		return sig, p, facts
	}
	start := func(s *Service) func(string, int) {
		s.platformServing.Store(parent)
		s.platformServingBound.Store(true)
		udp, e := net.ListenPacket("udp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		tcp, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		s.Config.UDPAddr, s.Config.TCPAddr = udp.LocalAddr().String(), tcp.Addr().String()
		for _, srv := range []*dns.Server{{PacketConn: udp, Handler: s}, {Listener: tcp, Handler: s}} {
			ready := make(chan struct{})
			srv.NotifyStartedFunc = func() { close(ready) }
			go srv.ActivateAndServe()
			<-ready
			t.Cleanup(func() { srv.Shutdown() })
		}
		return func(name string, rcode int) {
			t.Helper()
			for _, v := range []struct{ n, a string }{{"udp", udp.LocalAddr().String()}, {"tcp", tcp.Addr().String()}} {
				resp, _, e := (&dns.Client{Net: v.n, Timeout: time.Second}).Exchange(new(dns.Msg).SetQuestion(dns.Fqdn(name), dns.TypeA), v.a)
				if e != nil || resp.Rcode != rcode {
					t.Fatalf("%s %s response=%v err=%v", v.n, name, resp, e)
				}
				if rcode == dns.RcodeSuccess && (len(resp.Answer) != 1 || resp.Answer[0].Header().Ttl != 20 || resp.Answer[0].(*dns.A).A.String() != "8.8.8.8") {
					t.Fatal("address or TTL changed", resp)
				}
				if rcode != dns.RcodeSuccess && len(resp.Answer) > 0 {
					t.Fatal("failed query leaked global answer", resp)
				}
			}
		}
	}
	read := start(s)
	apply := func(f *fixture, phase string, changed bool) (model.PlatformArtifact, map[routeartifact.HostedRecipient][]dnsReadinessFact) {
		signed, p, facts := command(f, phase, changed)
		if e := s.installHostedDNS(f.cp, signed, parent, f.base, facts); e != nil {
			t.Fatal(f.cp.Host, phase, e)
		}
		f.head = p
		if f.first == "" {
			f.first = p.Decision
		}
		return signed, facts
	}
	apply(fa, "prepare", false)
	apply(fb, "prepare", false)
	af, afacts := apply(fa, "forward", true)
	bf, bfacts := apply(fb, "forward", true)
	// Prove the exact per-zone writes, not just interchangeable IPs.
	witness := func(s *Service, p routeartifact.HostedSelectedPacket, network string) (hostedDNSWireEvidence, error) {
		part := p.Delivery.Parts[0]
		q := part.Content.DNS.Views[0].Records[0]
		return s.witnessHostedDNSQuery(t.Context(), p, part, hostedZoneKey{part.Recipient.Zone, hostedRRKey{q.Name, dns.TypeA}}, network)
	}
	udpProof, e := witness(s, fa.head, "udp")
	if e != nil {
		t.Fatal(e)
	}
	started := time.Now().UTC()
	all, e := s.witnessHostedDNSMember(t.Context(), A, fa.head.Fence, fa.head.Decision)
	if e != nil || len(all) != 4 {
		t.Fatal("incomplete two-zone dual-transport evidence", e)
	}
	o := routeartifact.HostedObservation{Host: A, Member: fa.head.Delivery.Member, Fence: fa.head.Fence, Decision: fa.head.Decision, StartedAt: started, Samples: all}
	rows, e := o.Evaluate(fa.head, time.Now().UTC())
	if e != nil || len(rows) != 2 || !rows[0].Passing || rows[0].Evidence != rows[1].Evidence {
		t.Fatal("listener receipts did not satisfy the whole signed member", e, rows)
	}
	for _, edit := range []func(*routeartifact.HostedObservation){
		func(o *routeartifact.HostedObservation) { o.Samples = o.Samples[1:] }, func(o *routeartifact.HostedObservation) { o.Samples[0].Nonce = o.Samples[1].Nonce },
		func(o *routeartifact.HostedObservation) {
			o.Samples[0].RRSet = []string{"another.example.test. 20 IN A 8.8.8.8"}
		},
		func(o *routeartifact.HostedObservation) {
			o.Samples[0].RRSet = []string{o.Samples[0].Name + ". 900 IN A 8.8.8.8"}
		},
		func(o *routeartifact.HostedObservation) {
			o.Samples[0].RRSet = []string{o.Samples[0].Name + ". 20 IN A 1.1.1.1"}
		},
		func(o *routeartifact.HostedObservation) { o.Fence++ }, func(o *routeartifact.HostedObservation) { o.Samples[0].Decision = fb.head.Decision },
		func(o *routeartifact.HostedObservation) {
			o.Samples[0].Transport = "tcp"
			o.Samples[1].Transport = "tcp"
		},
		func(o *routeartifact.HostedObservation) { o.Failure = "local_apply" },
	} {
		other := o
		other.Samples = append([]routeartifact.HostedSample(nil), all...)
		edit(&other)
		if _, e := other.Evaluate(fa.head, time.Now().UTC()); e == nil {
			t.Fatal("invalid DNS observation accepted")
		}
	}
	tcpProof, e := witness(s, fa.head, "tcp")
	if e != nil || tcpProof.Part != udpProof.Part || tcpProof.Nonce == udpProof.Nonce || tcpProof.Decision != fa.head.Decision || len(tcpProof.RRSet) != 1 {
		t.Fatal("wire did not identify both transports & host", e)
	}
	// This other process serves the exact same signed bytes and answers,
	// but its listener never saw the first process's private challenge.
	impostor := NewService(meta, nil)
	impostor.hostedDNS.Store(s.hostedDNS.Load())
	start(impostor)
	addr := s.Config.UDPAddr
	s.Config.UDPAddr = impostor.Config.UDPAddr
	if _, e := witness(s, fa.head, "udp"); e == nil {
		t.Fatal("borrowed another DNS process's identical answer")
	}
	s.Config.UDPAddr = addr
	for _, n := range []string{A, B, "alpha.delegated.example.test", "beta.delegated.example.test"} {
		read(n, dns.RcodeSuccess)
	}
	idx := s.hostedDNS.Load()
	// Prove the answer journal attributes the wire response to the scoped
	// decision, not to the global parent that only supplies membership.
	checkReplay := func(idx *hostedDNSRoutes, base *dnsServingState, host, want string, when time.Time, rcode int) {
		q := new(dns.Msg).SetQuestion(dns.Fqdn(host), dns.TypeA)
		st := idx.forQuery(base, q, when)
		captured := captureDecision(t, st, q, "", when)
		if _, err := QualityEvidenceFromDNSDecision(captured, when, time.Minute); err == nil {
			t.Fatal("scoped answer credited to the parent quality publication")
		}
		replayed, err := ReplayDNSDecision(captured)
		if err != nil || !replayed.Matched || captured.RCode != rcode || captured.AnswerPublication == nil || captured.AnswerPublication.HostedDecision != want || captured.AnswerPublication.HostedHostname != host {
			t.Fatal("scoped DNS replay or attribution mismatch", host, err, replayed, captured.AnswerPublication)
		}
	}
	checkReplay(idx, parent, A, fa.head.Decision, at, dns.RcodeSuccess)
	checkReplay(idx, parent, A, fa.head.Decision, fa.head.ValidUntil, dns.RcodeServerFailure)
	replaced := *parent
	replaced.record.Candidate.Assignment.FencingToken++
	checkReplay(idx, &replaced, B, fb.head.Decision, at, dns.RcodeServerFailure)
	qOther := new(dns.Msg).SetQuestion(dns.Fqdn(A), dns.TypeTXT)
	if idx.forQuery(parent, qOther, at) != parent {
		t.Fatal("address decision claimed unrelated TXT")
	}
	if idx.forQuery(parent, new(dns.Msg).SetQuestion(dns.Fqdn(B), dns.TypeA), at).hostedWire.publication.HostedDecision != fb.head.Decision {
		t.Fatal("B attributed to wrong head")
	}
	s = NewService(meta, nil)
	if e := s.reserveHostedDNS(); e != nil {
		t.Fatal(e)
	}
	read = start(s)
	read(A, dns.RcodeServerFailure)
	read("alpha.delegated.example.test", dns.RcodeServerFailure)
	if e := s.installHostedDNS(fb.cp, bf, parent, fb.base, bfacts); e != nil {
		t.Fatal(e)
	}
	read(B, dns.RcodeSuccess)
	read(A, dns.RcodeServerFailure)
	bWire := s.hostedDNS.Load().keys[hostedZoneKey{view.Zone, hostedRRKey{B, dns.TypeA}}]
	back, backFacts := apply(fa, "withdraw", false)
	oldHead := fa.head
	oldHead.Decision = af.Generation
	if _, e := witness(s, oldHead, "udp"); e == nil {
		t.Fatal("borrowed withdrawal's address as an old forward observation")
	}
	if e := s.installHostedDNS(fa.cp, af, parent, fa.base, afacts); e == nil {
		t.Fatal("old forward came back after withdrawal")
	}
	read(A, dns.RcodeSuccess)
	read(B, dns.RcodeSuccess)
	for r, fs := range backFacts {
		fs[0].Ready = false
		backFacts[r] = fs
	}
	if e := s.installHostedDNS(fa.cp, back, parent, fa.base, backFacts); e != nil {
		t.Fatal(e)
	}
	read(A, dns.RcodeServerFailure)
	read(B, dns.RcodeSuccess)
	read("alpha.delegated.example.test", dns.RcodeServerFailure)
	if s.hostedDNS.Load().keys[hostedZoneKey{view.Zone, hostedRRKey{B, dns.TypeA}}] != bWire {
		t.Fatal("A changed B's wire or proof")
	}
}
