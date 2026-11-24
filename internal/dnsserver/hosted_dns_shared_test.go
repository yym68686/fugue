package dnsserver

import (
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

// Both domains depend on the SAME RR, not merely on similar records.
// The producer of each packet sees just the original global parent.
func TestSharedTargetWireKeepsPeerDecisionThroughWithdrawAndRestart(t *testing.T) {
	for _, held := range []bool{false, true} {
		name := "ordinary"
		if held {
			name = "physical-hold"
		}
		t.Run(name, func(t *testing.T) { testSharedWireDecisions(t, held, false) })
		t.Run(name+"/parent-handoff", func(t *testing.T) { testSharedWireDecisions(t, held, true) })
	}
}
func testSharedWireDecisions(t *testing.T, hold, carry bool) {
	const a, b, name = "first.example.org", "second.example.org", "target.example.test"
	view, template, policy, _, at := queryExecutionFixture()
	if hold {
		template.Records[0].MinimumHealthyEdges = 1
	}
	digest := func(v any) string {
		d, err := pc.Digest(v)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	config := config.DNSConfig{DNSNodeID: view.NodeID, EdgeGroupID: view.EdgeGroupID, CachePath: filepath.Join(t.TempDir(), "dns"), BundleSigningKey: "fixture-strong-key", BundleSigningKeyID: "key"}
	s := NewService(config, nil)
	rec := view.Records[0]
	rec.Name, rec.AppID, rec.TenantID, rec.TTL = name, "owner", "tenant", 20
	view.Records = []model.EdgeDNSRecord{rec}
	policy.MaxProbes = 48
	kind := model.PlatformArtifactKindDNSAnswerBundle
	assign := model.PlatformConsumerAssignment{ReleaseSetID: "full", ExpectedConsumerSetID: "members", ArtifactKind: kind, ScopeKey: "global", ArtifactReleaseID: "release", Revision: 1, ArtifactID: "child", ContentHash: digest("child"), ReleaseChannel: "full", FencingToken: 9}
	check := dnsServingCheckpoint{Positive: true, AppliedAt: at, Parent: model.PlatformArtifact{ID: assign.ReleaseSetID, ContentHash: digest("parent")}, Candidate: dnsPlatformCandidate{Assignment: assign, Artifact: model.PlatformArtifact{ID: assign.ArtifactID, ContentHash: assign.ContentHash}, Release: model.PlatformArtifactRelease{ID: assign.ArtifactReleaseID, ReleaseChannel: "full", FencingToken: assign.FencingToken}}}
	parent := &dnsServingState{record: check, routeID: "routes", payload: dnsServingPayload{Policy: pc.PolicySnapshot{Scope: "global", DNSReadiness: &policy, MaxStaleSeconds: 600}}, zones: map[string]dnsServingZone{}, zoneOrder: []string{view.Zone}}
	r := routeartifact.HostedRecipient{Component: model.PlatformConsumerComponentDNSServer, Kind: kind, Node: view.NodeID, Group: view.EdgeGroupID, Zone: view.Zone}
	unit := func(ah, bh []string) routeartifact.HostedRecipientInput {
		rr := template.Records[0]
		rr.Targets = slices.Clone(rr.Targets)
		rr.Hostname = name
		plan := pc.DNSReadinessPlan{}
		for i, tgt := range rr.Targets {
			tgt.ProbeIDs = nil
			for _, pair := range []struct {
				h     string
				paths []string
			}{{a, ah}, {b, bh}} {
				for _, path := range pair.paths {
					pr := pc.DNSReadinessProbe{EdgeID: tgt.EdgeID, EdgeGroupID: tgt.EdgeGroupID, Address: tgt.Address, Hostname: pair.h, Path: path, RouteDigest: digest([]string{pair.h, path, strings.Join(pair.paths, ",")})}
					pr.ID, _ = pc.DNSReadinessProbeID(pr)
					plan.Probes = append(plan.Probes, pr)
					tgt.ProbeIDs = append(tgt.ProbeIDs, pr.ID)
				}
			}
			slices.Sort(tgt.ProbeIDs)
			rr.Targets[i] = tgt
		}
		slices.SortFunc(plan.Probes, func(x, y pc.DNSReadinessProbe) int { return strings.Compare(x.ID, y.ID) })
		plan.Records = []pc.DNSReadinessRecord{rr}
		return routeartifact.HostedRecipientInput{Records: []pc.DNSIntent{{Hostname: name, Type: "A", RecordKind: model.EdgeDNSRecordKindCustomDomainTarget, Values: rec.Values, TTL: rec.TTL, TenantID: rec.TenantID, AppID: rec.AppID}}, DNS: &pc.HostedDNSContent{Plan: plan, Views: []pc.DNSQueryView{view}}}
	}
	root, four, two := []string{"/"}, []string{"/", "/health", "/v1", "/v2"}, []string{"/", "/new"}
	for _, peerPaths := range [][]string{nil, root} {
		projection := unit(root, peerPaths)
		for i := range projection.DNS.Plan.Records[0].Targets {
			projection.DNS.Plan.Records[0].Targets[i].RequireSinglePublication = true
		}
		part, err := readSharedDNSProjection(a, projection, true)
		if (err == nil && part != nil) != (len(peerPaths) == 0) {
			t.Fatal("single-publication requirement must permit exactly one host")
		}
	}
	base := unit(root, root)
	// B's decision may see an administrative veto which A's earlier
	// immutable packet could not contain. Drop the preferred endpoint
	// from requirements only. Both address declarations still list it.
	withHold := func(v routeartifact.HostedRecipientInput) routeartifact.HostedRecipientInput {
		if !hold {
			return v
		}
		u := *v.DNS
		v.DNS = &u
		u.Plan.Records = slices.Clone(u.Plan.Records)
		n := u.Plan.Records[0].Targets[0]
		u.Withheld = []pc.HostedAddressHold{{EdgeID: n.EdgeID, EdgeGroupID: n.EdgeGroupID, Address: n.Address}}
		u.Plan.Records[0].Targets = slices.Clone(u.Plan.Records[0].Targets[1:])
		u.Plan.Probes = slices.DeleteFunc(slices.Clone(u.Plan.Probes), func(p pc.DNSReadinessProbe) bool { return p.Address == n.Address })
		return v
	}
	answer := "8.8.8.8"
	signedBase := map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput{r: base}
	fact := func(pr pc.DNSReadinessProbe) dnsReadinessFact {
		return dnsReadinessFact{ProbeID: pr.ID, Ready: true, Proof: routeprobe.Proof{Digest: pr.RouteDigest, EdgeID: pr.EdgeID, GroupID: pr.EdgeGroupID, Version: "global", CheckedAt: at, ValidUntil: at.Add(50 * time.Second)}}
	}
	var global []dnsReadinessFact
	for _, pr := range base.DNS.Plan.Probes {
		f := fact(pr)
		f.Proof.TrafficRelease = &model.TrafficReleaseBinding{ReleaseSetID: check.Parent.ID, ReleaseSetDigest: check.Parent.ContentHash, RouteArtifactID: parent.routeID, ReleaseID: assign.ArtifactReleaseID, ReleaseChannel: "full", FencingToken: assign.FencingToken, ScopeKey: "global"}
		global = append(global, f)
	}
	setGlobal := func(fs []dnsReadinessFact) {
		parent.zones[view.Zone] = dnsServingZone{authority: pc.DNSAuthorityPolicy{Zone: view.Zone, Nameservers: []string{"ns.example.test"}, TTLSeconds: 60}, records: map[string][]dnsServingRecord{name: {{record: rec, plan: base.DNS.Plan, facts: fs}}}}
	}
	setGlobal(global)
	start := func() {
		s.platformServing.Store(parent)
		s.platformServingBound.Store(true)
		udp, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		tcp, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		s.Config.UDPAddr, s.Config.TCPAddr = udp.LocalAddr().String(), tcp.Addr().String()
		for _, srv := range []*dns.Server{{PacketConn: udp, Handler: s}, {Listener: tcp, Handler: s}} {
			ready := make(chan struct{})
			srv.NotifyStartedFunc = func() { close(ready) }
			go srv.ActivateAndServe()
			<-ready
			t.Cleanup(func() { srv.Shutdown() })
		}
	}
	start()
	query := func(rcode int) {
		t.Helper()
		for _, pair := range [][2]string{{"udp", s.Config.UDPAddr}, {"tcp", s.Config.TCPAddr}} {
			v, _, err := (&dns.Client{Net: pair[0], Timeout: time.Second}).Exchange(new(dns.Msg).SetQuestion(dns.Fqdn(name), dns.TypeA), pair[1])
			if err != nil || v.Rcode != rcode || rcode == dns.RcodeSuccess && (len(v.Answer) != 1 || v.Answer[0].Header().Ttl != 20 || v.Answer[0].(*dns.A).A.String() != answer) || rcode != dns.RcodeSuccess && len(v.Answer) != 0 {
				t.Fatal(pair[0], v, err)
			}
		}
	}
	type head struct {
		p   routeartifact.HostedSelectedPacket
		cmd model.PlatformArtifact
		fs  map[routeartifact.HostedRecipient][]dnsReadinessFact
	}
	heads := map[string]head{}
	first := map[string]string{}
	awaitPeer := false
	install := func(host string, h head) error {
		cp, err := s.dnsHostCheckpoint(host)
		if err != nil {
			return err
		}
		cp.Serving = assign
		return s.installHostedDNS(cp, h.cmd, parent, signedBase, h.fs)
	}
	apply := func(host, phase string, v routeartifact.HostedRecipientInput) {
		t.Helper()
		cp, _ := s.dnsHostCheckpoint(host)
		old := heads[host].p
		n := old.Fence + 1
		dec := digest([]any{host, n, phase})
		p := routeartifact.HostedSelectedPacket{Scope: "host-" + host, Project: "project", Draft: "draft", Offer: 1, Fence: n, Decision: dec, Authority: digest("auth"), Business: digest("business"), Parent: assign.ReleaseSetID, Child: assign.ArtifactID, ChildDigest: assign.ContentHash, SelectedAt: at.Add(-time.Second), ValidUntil: at.Add(time.Minute), Delivery: routeartifact.HostedDelivery{Schema: "fugue.hosted-recipient-input/v1", Host: host, DraftDigest: digest("draft"), ImageDigest: digest("image"), Phase: phase, Member: cp.Who.ConsumerID(), MembershipSet: assign.ExpectedConsumerSetID, MembershipRelease: assign.ArtifactReleaseID, MembershipRevision: assign.Revision, MembershipFence: assign.FencingToken}}
		part := routeartifact.HostedDeliveryPart{Recipient: r, Content: v, ContentDigest: digest([]any{"fugue.hosted-recipient-part/v1", host, r, v})}
		p.Delivery.Parts = []routeartifact.HostedDeliveryPart{part}
		if n > 1 {
			p.Previous = old.Decision
			k, _ := old.Delivery.PartKey(old.Delivery.Parts[0])
			p.PriorParts = map[string]string{k: old.Delivery.Parts[0].ContentDigest}
		} else {
			first[host] = dec
		}
		if phase == "withdraw" {
			p.SkippedForwardFrom = first[host]
		}
		if carry {
			var err error
			p.MemberBaseline, err = routeartifact.HostedDNSBaselinePin(p.Delivery, signedBase)
			if err != nil {
				t.Fatal(err)
			}
			if assign.ReleaseSetID != "full" {
				p.Draft, p.Offer, p.Delivery.DraftDigest, p.Delivery.ImageDigest, p.Authority = "next-"+host, 2, digest([]string{"next-draft", host}), digest([]string{"next-image", host}), digest("next-authority")
				if phase == "withdraw" {
					p.ExactWithdrawal, p.SkippedForwardFrom = true, ""
				}
				if old.Parent != p.Parent {
					key, e := p.Delivery.PartKey(part)
					if e != nil {
						t.Fatal(e)
					}
					p.PriorParts = map[string]string{key: old.Delivery.Parts[0].ContentDigest}
					p.MembershipRebind, err = routeartifact.BindHostedContinuation(old, p, old.MemberBaseline, p.MemberBaseline)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		sig, err := routeartifact.SignHostedPacket(p, s.platformDNSKeys(), at)
		if err != nil {
			t.Fatal(err)
		}
		h := head{p: p, cmd: sig, fs: map[routeartifact.HostedRecipient][]dnsReadinessFact{}}
		for _, pr := range v.DNS.Plan.Probes {
			if pr.Hostname == host {
				f := fact(pr)
				f.Proof.Kind, f.Proof.Version = "hosted-route", dec
				h.fs[r] = append(h.fs[r], f)
			}
		}
		err = install(host, h)
		if awaitPeer {
			if !errors.Is(err, errHostedDNSPeerPending) {
				t.Fatal("first peer did not reserve the new membership", err)
			}
			heads[host] = h
			query(dns.RcodeServerFailure)
			return
		}
		if err != nil {
			t.Fatalf("install %s/%s: %v", host, phase, err)
		}
		heads[host] = h
		query(dns.RcodeSuccess)
	}
	apply(a, "prepare", base)
	apply(a, "forward", unit(four, root))
	setGlobal(nil)
	query(dns.RcodeServerFailure)
	setGlobal(global)
	query(dns.RcodeSuccess)
	if hold {
		answer = "9.9.9.9"
	}
	apply(b, "prepare", withHold(base))
	apply(b, "forward", withHold(unit(root, two)))
	setGlobal(nil)
	query(dns.RcodeSuccess)
	if carry {
		// A and B already share the same RR and have their own forward
		// changes. The new full release does not authorize the old facts.
		assign.ReleaseSetID, assign.ArtifactReleaseID, assign.ArtifactID, assign.ContentHash, assign.ExpectedConsumerSetID = "full-next", "release-next", "child-next", digest("child-next"), "members-next"
		assign.FencingToken++
		assign.Revision++
		n := *parent
		n.record.Parent = model.PlatformArtifact{ID: assign.ReleaseSetID, ContentHash: digest("parent-next")}
		n.record.Candidate.Assignment = assign
		n.record.Candidate.Artifact = model.PlatformArtifact{ID: assign.ArtifactID, ContentHash: assign.ContentHash}
		n.record.Candidate.Release.ID, n.record.Candidate.Release.FencingToken = assign.ArtifactReleaseID, assign.FencingToken
		parent = &n
		s.platformServing.Store(parent)
		query(dns.RcodeServerFailure)
		awaitPeer = true
		apply(a, "forward", unit(four, root))
		if got := s.hostedDNS.Load().keys[hostedZoneKey{view.Zone, hostedRRKey{name, dns.TypeA}}].forHost(b); got.Decision != heads[b].p.Decision {
			t.Fatal("staging A replaced B")
		}
		// Crash after A's durable rename and before B's. Reserve both;
		// never reconstruct an answer out of two parents or old facts.
		s = NewService(config, nil)
		if err := s.reserveHostedDNS(); err != nil {
			t.Fatal("mixed checkpoint restart failed", err)
		}
		start()
		query(dns.RcodeServerFailure)
		if err := install(a, heads[a]); !errors.Is(err, errHostedDNSPeerPending) {
			t.Fatal("mixed snapshot accepted a receipt", err)
		}
		awaitPeer = false
		apply(b, "forward", withHold(unit(root, two)))
	}
	oldA := heads[a]
	apply(a, "withdraw", base)
	if err := install(a, oldA); err == nil {
		t.Fatal("accepted old A after withdrawal")
	}
	k := hostedZoneKey{view.Zone, hostedRRKey{name, dns.TypeA}}
	w := s.hostedDNS.Load().keys[k]
	if !reflect.DeepEqual(w.rows[0].plan, withHold(unit(root, two)).DNS.Plan) || w.forHost(b).Decision != heads[b].p.Decision {
		t.Fatal("withdrawal replaced peer's current requirements")
	}
	for _, host := range []string{a, b} {
		p := heads[host].p
		obs, e := s.witnessHostedDNSMember(t.Context(), host, p.Fence, p.Decision)
		if e != nil || len(obs) != 2 {
			t.Fatal("missing independent listener receipts", host, e)
		}
	}
	bad := unit(root, two)
	if sameSharedPredecessor(a, base, bad) {
		t.Fatal("borrowed peer paths accepted under A")
	}
	if hold {
		v := withHold(unit(root, two))
		if !sameSharedPredecessor(b, base, v) {
			t.Fatal("exact signed policy contraction rejected")
		}
		v.DNS.Withheld = nil
		if sameSharedPredecessor(b, base, v) {
			t.Fatal("unexplained target contraction accepted")
		}
		v = withHold(unit(root, two))
		v.DNS.Withheld[0].EdgeGroupID = "edge-group-unrelated"
		if sameSharedPredecessor(b, base, v) {
			t.Fatal("hold for wrong physical group accepted")
		}
		v = withHold(unit(four, two))
		if sameSharedPredecessor(b, base, v) {
			t.Fatal("claimed hold copied an unrelated path edit")
		}
	}
	s = NewService(config, nil)
	if e := s.reserveHostedDNS(); e != nil {
		t.Fatal(e)
	}
	start()
	query(dns.RcodeServerFailure)
	if e := install(b, heads[b]); e != nil {
		t.Fatal(e)
	}
	query(dns.RcodeServerFailure)
	if e := install(a, heads[a]); e != nil {
		t.Fatal(e)
	}
	query(dns.RcodeSuccess)
	if !reflect.DeepEqual(s.hostedDNS.Load().keys[k].rows[0].plan, withHold(unit(root, two)).DNS.Plan) {
		t.Fatal("restart changed merged path sets")
	}
}
