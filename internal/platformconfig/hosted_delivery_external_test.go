package platformconfig_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
)

func TestHostedDeliveryUsesExactCurrentMemberWithoutTransferringGlobalProof(t *testing.T) {
	pc.HostedFourPathFixtureForTest(t, func(source pc.HostedAddressSource, p pc.HostedScopeProposal) {
		draft, err := pc.Digest(p)
		if err != nil {
			t.Fatal(err)
		}
		img, err := routeartifact.FreezeHostedProgram(source, p, draft)
		if err != nil {
			t.Fatal(err)
		}
		_, stage, err := routeartifact.BuildHostedProgramUnits(source, p)
		if err != nil {
			t.Fatal(err)
		}
		members, err := routeartifact.HostedRecipientInputs(p, stage)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[routeartifact.HostedRecipient]bool{}
		partPins := map[string]bool{}
		coverage := map[string]int{}
		for r := range members {
			if seen[r] {
				continue
			}
			coverage[r.Kind]++
			claims := platformcontrol.PlatformComponentIdentityClaims{Component: r.Component, NodeID: r.Node, ScopeKey: "global", ArtifactKinds: []string{r.Kind}}
			if r.Component == model.PlatformConsumerComponentEdgeWorker {
				claims.WorkerEnrollmentID = strings.Repeat("a", 32)
			}
			m := model.PlatformExpectedConsumer{Component: r.Component, NodeID: r.Node, ConsumerID: claims.ConsumerID(), ScopeKey: "global", Cohort: r.Group, ArtifactKind: r.Kind, ExpectedGeneration: "current", Required: true}
			if r.Kind == model.PlatformArtifactKindCaddyRouteConfig {
				m.Component = model.PlatformConsumerComponentCaddyEdgeFront
				m.ConsumerID, err = platformcontrol.EnrolledPlatformConsumerID(m.Component, m.NodeID, "", "global", claims.WorkerEnrollmentID)
				if err != nil {
					t.Fatal(err)
				}
			}
			set := model.PlatformExpectedConsumerSet{ID: "set-" + r.Kind, Revision: 13, ScopeKey: "global", ArtifactKind: r.Kind, ReleaseSetID: p.Base.ReleaseSetID, ArtifactReleaseID: p.Base.ReleaseID, ExpectedGeneration: "current", Consumers: []model.PlatformExpectedConsumer{m}}
			a := model.PlatformConsumerAssignment{ExpectedConsumerSetID: set.ID, ReleaseSetID: set.ReleaseSetID, ArtifactReleaseID: set.ArtifactReleaseID, ArtifactKind: r.Kind, ScopeKey: "global", ExpectedGeneration: "current", Revision: 13, FencingToken: p.Base.FencingToken, ReleaseChannel: "full"}
			switch r.Kind {
			case model.PlatformArtifactKindEdgeRouteBundle:
				a.ArtifactID, a.ContentHash = p.Base.RouteArtifactID, p.Base.RouteArtifactDigest
			case model.PlatformArtifactKindCaddyRouteConfig:
				a.ArtifactID, a.ContentHash = p.Base.TLSArtifactID, p.Base.TLSArtifactDigest
			case model.PlatformArtifactKindDNSAnswerBundle:
				a.ArtifactID, a.ContentHash = p.Base.DNSArtifactID, p.Base.DNSArtifactDigest
				base, e := routeartifact.HostedDNSMemberBase(p.Host, r.Node, r.Group, source.Prior)
				if e != nil {
					t.Fatal("could not independently resolve the full's DNS membership", e)
				}
				rr, ok := base[r]
				if !ok || !reflect.DeepEqual(rr.Records, members[r].Records) {
					t.Fatal("full disagrees with compiler about wire RR, owner, or TTL")
				}
				if _, e := routeartifact.HostedDNSMemberBase(p.Host, r.Node, "another-group", source.Prior); e == nil {
					t.Fatal("base taken from another group")
				}
				foreign := source.Prior
				plan := *foreign.ReadinessPlan
				plan.Records = slices.Clone(plan.Records)
				borrowed := plan.Records[0]
				borrowed.Hostname = "foreign.example.test"
				plan.Records = append(plan.Records, borrowed)
				foreign.ReadinessPlan = &plan
				if _, e := routeartifact.HostedDNSMemberBase(p.Host, r.Node, r.Group, foreign); e == nil {
					t.Fatal("reverse dependency of another host dropped")
				}
			}
			send := func(c platformcontrol.PlatformComponentIdentityClaims, x model.PlatformConsumerAssignment, s model.PlatformExpectedConsumerSet, phase string) (routeartifact.HostedDelivery, string, error) {
				return routeartifact.AddressHostedPreparation(p, draft, img.Digest, img.Wire, phase, c, x, s)
			}
			out, pin, err := send(claims, a, set, "forward")
			if err != nil || pin == "" || len(out.Parts) == 0 || out.Member != claims.ConsumerID() {
				t.Fatal("legitimate recipient lost", r, err)
			}
			now := time.Now().UTC().Truncate(time.Microsecond)
			packet := routeartifact.HostedSelectedPacket{Scope: "hosted-" + p.Host, Project: "project", Draft: "original-draft", Offer: 1, Fence: 3, Decision: pin, Authority: draft, Business: img.Digest, Parent: a.ReleaseSetID, Child: a.ArtifactID, ChildDigest: a.ContentHash, SelectedAt: now.Add(-time.Second), ValidUntil: now.Add(time.Minute), Delivery: out}
			keys := bundleauth.NewKeyring("local-test-only-host-selection-key", "host-test", "", "", nil)
			prepared, _, err := send(claims, a, set, "prepare")
			if err != nil {
				t.Fatal(err)
			}
			packet.Previous, packet.PriorParts = draft, map[string]string{}
			for _, part := range prepared.Parts {
				k, err := prepared.PartKey(part)
				if err != nil {
					t.Fatal(err)
				}
				packet.PriorParts[k] = part.ContentDigest
			}
			signed, e := routeartifact.SignHostedPacket(packet, keys, now)
			if e != nil {
				t.Fatal("cannot sign physical command", e)
			}
			// Three real, signed commands for the same member. At withdraw
			// the node may still be at prepare or may have applied forward.
			// A split local zone snapshot must not produce a partial result.
			baseline := map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput{}
			current := map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput{}
			forwardPins := map[string]string{}
			for _, part := range prepared.Parts {
				baseline[part.Recipient] = part.Content
			}
			for _, part := range out.Parts {
				current[part.Recipient] = part.Content
				k, err := out.PartKey(part)
				if err != nil {
					t.Fatal(err)
				}
				forwardPins[k] = part.ContentDigest
			}
			back, _, err := send(claims, a, set, "withdraw")
			if err != nil {
				t.Fatal(err)
			}
			p0, pf, pw := packet, packet, packet
			p0.Fence, p0.Decision, p0.Previous, p0.PriorParts, p0.Delivery = 1, draft, "", nil, prepared
			pf.Fence = 2
			pw.Fence, pw.Decision, pw.Previous, pw.SkippedForwardFrom, pw.PriorParts, pw.Delivery = 3, img.Digest, pin, draft, forwardPins, back
			run := func(command routeartifact.HostedSelectedPacket, cur routeartifact.HostedMemberCursor, live map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput) (routeartifact.HostedMemberChange, error) {
				signed, e := routeartifact.SignHostedPacket(command, keys, now)
				if e != nil {
					return routeartifact.HostedMemberChange{}, e
				}
				return routeartifact.PlanHostedMemberChange(signed, p.Host, claims, a, cur, live, baseline, keys, now)
			}
			z := routeartifact.HostedMemberCursor{}
			preparedCursor, e := run(p0, z, baseline)
			if e != nil || preparedCursor.Next.Fence != 1 {
				t.Fatal("initial choice", e)
			}
			forwardCursor, e := run(pf, preparedCursor.Next, baseline)
			if e != nil {
				t.Fatal("forward choice", e)
			}
			applied, e := run(pw, forwardCursor.Next, current)
			if e != nil {
				t.Fatal("ordinary withdrawal", e)
			}
			skipped, e := run(pw, preparedCursor.Next, baseline)
			if e != nil || len(skipped.Needed) != 0 || applied.Next != skipped.Next || applied.Next.Fence != 3 {
				t.Fatal("partial forward withdrawal", e)
			}
			if _, e = run(pf, skipped.Next, baseline); e == nil {
				t.Fatal("replayed forward after withdraw")
			}
			// Restart with no in-memory cursor. Only the signed on-disk
			// per-host decision can be reopened; it does not prove health.
			cp := platformconsumer.HostedCheckpoint{Root: t.TempDir(), Who: claims, Serving: a, Host: p.Host, Keys: keys}
			save := func(v routeartifact.HostedSelectedPacket, actual map[routeartifact.HostedRecipient]routeartifact.HostedRecipientInput) error {
				cmd, e := routeartifact.SignHostedPacket(v, keys, now)
				if e != nil {
					return e
				}
				_, e = cp.Commit(cmd, actual, baseline, now)
				return e
			}
			if e = save(p0, baseline); e != nil {
				t.Fatal("initial durable admission", e)
			}
			if v, e := cp.Load(now); e != nil || v.Fence != 1 {
				t.Fatal("lost selected host on reopen", e)
			}
			if e = save(pw, baseline); e != nil {
				t.Fatal("missed forward on restart", e)
			}
			if e = save(pf, baseline); e == nil {
				t.Fatal("downgraded durable high water")
			}
			if v, e := cp.Load(now); e != nil || v.Fence != 3 || v.Delivery.Phase != "withdraw" {
				t.Fatal("lost withdrawal after replay", e)
			}
			if _, e = cp.Load(pw.ValidUntil); e == nil {
				t.Fatal("expired durable selection served")
			}
			cpOld := cp
			cpOld.Serving.FencingToken++
			if _, e = cpOld.Load(now); e == nil {
				t.Fatal("foreign full reopened checkpoint")
			}
			fork := preparedCursor.Next
			fork.Decision = img.Digest
			if _, e = run(pw, fork, baseline); e == nil {
				t.Fatal("other predecessor qualified")
			}
			partial := maps.Clone(current)
			partial[out.Parts[len(out.Parts)-1].Recipient] = routeartifact.HostedRecipientInput{}
			if v, e := run(pw, forwardCursor.Next, partial); e == nil || v.Next != z || len(v.Needed) > 0 {
				t.Fatal("incomplete receiver state yielded a write")
			}
			extra := pf
			extra.PriorParts = maps.Clone(pf.PriorParts)
			extra.PriorParts[draft] = pin
			if _, e = routeartifact.SignHostedPacket(extra, keys, now); e == nil {
				t.Fatal("foreign member predecessor leaked")
			}
			raw, e := json.Marshal(signed)
			if e != nil {
				t.Fatal(e)
			}
			var wire model.PlatformArtifact
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.UseNumber()
			if e = dec.Decode(&wire); e != nil {
				t.Fatal(e)
			}
			verify := func(v model.PlatformArtifact, h string, c platformcontrol.PlatformComponentIdentityClaims, sel model.PlatformConsumerAssignment, fence int64, known string, at time.Time) error {
				_, e := routeartifact.VerifyHostedPacket(v, h, c, sel, fence, known, keys, at)
				return e
			}
			if e := verify(wire, p.Host, claims, a, 3, pin, now); e != nil {
				t.Fatal("signed transport changed content", e)
			}
			// Cross a real HTTP transport with the receiver's own client.
			// GET must not count as a receipt. Exercise the receiver's own
			// POST client and the independent server-side evidence verifier.
			selectedURL := "/v1/platform-state/consumers/artifacts/" + a.ArtifactID + "/host-selections/" + p.Host
			var missing, changed bool
			writes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Header.Get("Authorization") != "Bearer local-only" || req.Method != "GET" && (req.Method != "POST" || req.URL.Path != selectedURL) {
					w.WriteHeader(403)
					return
				}
				switch req.URL.Path {
				case strings.TrimSuffix(selectedURL, "/"+p.Host):
					h := routeartifact.HostedHint{Host: p.Host, Fence: 3, Decision: pin}
					if req.URL.Query().Get("expected_consumer_set_id") != a.ExpectedConsumerSetID {
						w.WriteHeader(404)
						return
					}
					_ = json.NewEncoder(w).Encode(routeartifact.HostedHintPage{Selections: []routeartifact.HostedHint{h}})
				case selectedURL:
					if missing || req.URL.Query().Get("expected_consumer_set_id") != set.ID {
						w.WriteHeader(404)
						return
					}
					if req.Method == "POST" {
						var o routeartifact.HostedObservation
						if json.NewDecoder(req.Body).Decode(&o) != nil {
							w.WriteHeader(400)
							return
						}
						rows, e := o.Evaluate(packet, time.Now().UTC())
						if e != nil || len(rows) != len(packet.Delivery.Parts) {
							w.WriteHeader(409)
							return
						}
						writes++
						w.WriteHeader(204)
						return
					}
					_, _ = w.Write(raw)
				case "/v1/platform-state/consumers/assignment":
					sel := a
					if changed {
						sel.FencingToken++
					}
					if req.URL.Query().Get("serving_only") != "true" {
						w.WriteHeader(400)
						return
					}
					_ = json.NewEncoder(w).Encode(model.PlatformConsumerAssignmentResponse{Assignments: []model.PlatformConsumerAssignment{sel}})
				default:
					w.WriteHeader(404)
				}
			}))
			client := platformconsumer.Client{BaseURL: server.URL, HTTPClient: server.Client()}
			id := platformconsumer.Identity{Token: "local-only", ExpiresAt: now.Add(time.Hour), ConsumerID: claims.ConsumerID(), Component: r.Component, NodeID: r.Node, ScopeKey: claims.ScopeKey, ArtifactKinds: claims.ArtifactKinds}
			pull := func() (routeartifact.HostedSelectedPacket, error) {
				return client.HostedSelection(t.Context(), id, a, p.Host, 2, draft, keys)
			}
			page, err := client.HostedSelections(t.Context(), id, a, "")
			if err != nil || len(page.Selections) != 1 || page.Selections[0].Host != p.Host || page.Next != "" {
				server.Close()
				t.Fatal("bounded membership discovery failed", err)
			}
			if _, e := client.HostedSelections(t.Context(), id, a, p.Host); e == nil {
				server.Close()
				t.Fatal("nonadvancing directory accepted")
			}
			actual, err := pull()
			if err != nil || actual.Fence != 3 || actual.Decision != pin || !reflect.DeepEqual(actual.Delivery, out) {
				server.Close()
				t.Fatal("real client did not retrieve exact decision", r, err)
			}
			o := routeartifact.HostedObservation{Host: p.Host, Member: claims.ConsumerID(), Fence: actual.Fence, Decision: actual.Decision, StartedAt: time.Now().UTC(), Failure: "wire_observation"}
			report := func() error { return client.ReportHostedObservation(t.Context(), id, a, o) }
			if writes != 0 || report() != nil || writes != 1 {
				server.Close()
				t.Fatal("GET credited or physical failure not delivered")
			}
			o.Fence--
			if report() == nil || writes != 1 {
				server.Close()
				t.Fatal("obsolete observation credited")
			}
			o.Fence++
			changed = true
			if e := report(); !errors.Is(e, platformconsumer.ErrAssignmentChanged) || writes != 1 {
				server.Close()
				t.Fatal("obsolete assignment allowed a receipt", e)
			}
			if _, err = pull(); !errors.Is(err, platformconsumer.ErrAssignmentChanged) {
				server.Close()
				t.Fatal("changed parent authority passed", err)
			}
			missing = true
			if _, err = pull(); !errors.Is(err, platformconsumer.ErrNoHostedSelection) {
				server.Close()
				t.Fatal("absent decision became an assignment", err)
			}
			server.Close()
			peer := claims
			peer.NodeID += "-new"
			retired := a
			retired.FencingToken++
			if verify(wire, p.Host, claims, a, 4, pin, now) == nil || verify(wire, p.Host, claims, a, 3, img.Digest, now) == nil ||
				verify(wire, p.Host, peer, a, 0, "", now) == nil || verify(wire, "another.example.test", claims, a, 0, "", now) == nil ||
				verify(wire, p.Host, claims, retired, 0, "", now) == nil || verify(wire, p.Host, claims, a, 0, "", packet.ValidUntil) == nil {
				t.Fatal("stale or foreign command accepted")
			}
			wire.Content["host_decision"] = draft
			if verify(wire, p.Host, claims, a, 0, "", now) == nil {
				t.Fatal("modified signed command accepted")
			}
			for _, part := range out.Parts {
				rec := part.Recipient
				if part.ContentDigest == "" || partPins[part.ContentDigest] || seen[rec] || rec.Kind != r.Kind || rec.Node != r.Node || rec.Group != r.Group || !reflect.DeepEqual(part.Content, members[rec]) {
					t.Fatal("foreign or incomplete contents", rec)
				}
				partPins[part.ContentDigest] = true
				seen[rec] = true
			}
			_, retry, err := send(claims, a, set, "forward")
			if err != nil || retry != pin {
				t.Fatal("unstable recipient content", err)
			}
			pre, preparePin, err := send(claims, a, set, "prepare")
			if err != nil {
				t.Fatal(err)
			}
			back, withdrawPin, err := send(claims, a, set, "withdraw")
			if err != nil || preparePin == withdrawPin || preparePin == pin || withdrawPin == pin || !reflect.DeepEqual(pre.Parts, back.Parts) {
				t.Fatal("withdraw reuses old phase decision or changes prepared content", err)
			}
			for _, change := range []func(*platformcontrol.PlatformComponentIdentityClaims, *model.PlatformConsumerAssignment, *model.PlatformExpectedConsumerSet){
				func(c *platformcontrol.PlatformComponentIdentityClaims, _ *model.PlatformConsumerAssignment, _ *model.PlatformExpectedConsumerSet) {
					c.NodeID += "-new"
				},
				func(c *platformcontrol.PlatformComponentIdentityClaims, _ *model.PlatformConsumerAssignment, _ *model.PlatformExpectedConsumerSet) {
					c.ScopeKey = "authority-cell:cell-other"
				},
				func(c *platformcontrol.PlatformComponentIdentityClaims, _ *model.PlatformConsumerAssignment, _ *model.PlatformExpectedConsumerSet) {
					c.WorkerEnrollmentID = strings.Repeat("b", 32)
				},
				func(c *platformcontrol.PlatformComponentIdentityClaims, _ *model.PlatformConsumerAssignment, _ *model.PlatformExpectedConsumerSet) {
					c.ArtifactKinds = nil
				},
				func(_ *platformcontrol.PlatformComponentIdentityClaims, a *model.PlatformConsumerAssignment, _ *model.PlatformExpectedConsumerSet) {
					a.FencingToken++
				},
				func(_ *platformcontrol.PlatformComponentIdentityClaims, a *model.PlatformConsumerAssignment, _ *model.PlatformExpectedConsumerSet) {
					a.ReleaseChannel = "shadow"
				},
				func(_ *platformcontrol.PlatformComponentIdentityClaims, a *model.PlatformConsumerAssignment, _ *model.PlatformExpectedConsumerSet) {
					a.ContentHash = img.Digest
				},
				func(_ *platformcontrol.PlatformComponentIdentityClaims, _ *model.PlatformConsumerAssignment, s *model.PlatformExpectedConsumerSet) {
					s.Revision++
				},
				func(_ *platformcontrol.PlatformComponentIdentityClaims, _ *model.PlatformConsumerAssignment, s *model.PlatformExpectedConsumerSet) {
					s.Consumers[0].Cohort += "-other"
				},
				func(_ *platformcontrol.PlatformComponentIdentityClaims, _ *model.PlatformConsumerAssignment, s *model.PlatformExpectedConsumerSet) {
					s.Consumers = append(s.Consumers, s.Consumers[0])
				},
			} {
				c, x, ss := claims, a, set
				ss.Consumers = append([]model.PlatformExpectedConsumer{}, set.Consumers...)
				change(&c, &x, &ss)
				if _, _, e := send(c, x, ss, "forward"); e == nil {
					t.Fatal("foreign or stale membership read contents", r)
				}
			}
		}
		if len(seen) != len(members) || len(coverage) != 3 {
			t.Fatal("missing physical or DNS participant")
		}
	})
}
