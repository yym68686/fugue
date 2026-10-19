package dnsserver

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

func TestControlPlaneOutageReobservesRetainedDNSWithoutRenewingAuthority(t *testing.T) {
	for _, scenario := range []string{"offline", "checkpoint expired", "not positive", "candidate rejected", "edge unreachable", "different release"} {
		t.Run(scenario, func(t *testing.T) {
			s, old, now := runtimeFactsFixture(t)
			originalFacts := append([]dnsReadinessFact(nil), old.facts...)
			old.facts = nil // No transient proof may be carried across the outage.
			old.record.AppliedAt = now.Add(-time.Minute)
			reason := "control_plane_unavailable"
			switch scenario {
			case "checkpoint expired":
				old.record.AppliedAt = now.Add(-time.Duration(old.payload.Policy.MaxStaleSeconds+1) * time.Second)
			case "not positive":
				old.record.Positive = false
			case "candidate rejected":
				reason = "candidate_rejected"
			}
			before := old.record
			probe := func(ctx context.Context, host, path, address, state string, _ time.Duration) (routeprobe.Proof, error) {
				allowed := scenario != "checkpoint expired" && scenario != "not positive" && scenario != "candidate rejected"
				if routeprobe.RetainedLKGAllowed(ctx) != allowed {
					t.Error("retained mode escaped its accepted checkpoint boundary")
				}
				if !allowed || scenario == "edge unreachable" {
					return routeprobe.Proof{}, errors.New("no authenticated route observation")
				}
				for _, requirement := range old.payload.Plan.Probes {
					if requirement.Hostname != host || requirement.Path != path || requirement.Address != address || requirement.State != state {
						continue
					}
					for _, fact := range originalFacts {
						if fact.ProbeID != requirement.ID {
							continue
						}
						proof := fact.Proof
						proof.BundleValidUntil = now.Add(-time.Minute)
						proof.CheckedAt, proof.ValidUntil = now, now.Add(time.Hour)
						if scenario == "different release" {
							binding := *proof.TrafficRelease
							binding.FencingToken++
							proof.TrafficRelease = &binding
						}
						return proof, nil
					}
				}
				return routeprobe.Proof{}, errors.New("unknown target")
			}
			got, err := s.refreshedDNSServingFacts(context.Background(), old, probe, reason, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.record, before) || !reflect.DeepEqual(old.record, before) {
				t.Fatal("runtime observation renewed or replaced configuration authority")
			}
			q := new(dns.Msg)
			q.SetQuestion("app.example.test.", dns.TypeA)
			answer := got.answer(q, "", time.Now())
			if scenario == "offline" {
				if answer.Rcode != dns.RcodeSuccess || len(answer.Answer) != 1 {
					t.Fatal("healthy LKG disappeared during control-plane outage", answer)
				}
				for _, fact := range got.facts {
					if fact.Proof.ValidUntil.Sub(fact.Proof.CheckedAt) > time.Duration(old.payload.Policy.DNSReadiness.FactFreshnessSeconds)*time.Second {
						t.Fatal("offline observation bypassed fresh physical probing")
					}
				}
				deadline := before.AppliedAt.Add(time.Duration(old.payload.Policy.MaxStaleSeconds) * time.Second)
				if got.answer(q, "", deadline.Add(time.Second)).Rcode != dns.RcodeServerFailure {
					t.Fatal("offline observation extended signed DNS authority")
				}
			} else if answer.Rcode != dns.RcodeServerFailure || len(answer.Answer) != 0 {
				t.Fatal("unavailable or unauthorized edge was admitted", answer)
			}
		})
	}
}
