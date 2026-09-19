package store

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func TestPlatformProducerGuard(t *testing.T) { testPlatformProducerGuard(t, "") }
func TestPlatformProducerGuardPostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback PostgreSQL")
	}
	testPlatformProducerGuard(t, dsn)
}

func testPlatformProducerGuard(t *testing.T, dsn string) {
	for _, scenario := range []string{"complete", "retry", "paused", "superseded", "policy frozen", "target frozen", "target changed", "tampered member", "member lineage", "wrong actor", "queued policy freeze", "queued target freeze", "static domains complete", "static domains missing", "static domains invalid", "static complete", "static digest", "static revoked validation", "static binding", "static tampered", "queued static invalidation", "dns complete", "dns digest", "dns invalidated", "dns tampered", "dns binding", "dns missing authority", "dns template", "queued dns invalidation"} {
		t.Run(scenario, func(t *testing.T) {
			if strings.HasPrefix(scenario, "queued") && dsn == "" {
				t.Skip("real PostgreSQL queue")
			}
			s := New(t.TempDir()+"/state.json", dsn)
			configureTestPlatformArtifactSigning(s)
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			if dsn != "" {
				t.Cleanup(func() { s.db.Close() })
				if _, err := s.db.Exec(`UPDATE fugue_platform_release_lanes SET frozen=false WHERE scope_key IN ('global','platform-config-producer')`); err != nil {
					t.Fatal(err)
				}
			}
			since := time.Now().UTC()
			principal := model.Principal{ActorType: model.ActorTypeBootstrap, ActorID: platformproducer.Actor, Scopes: map[string]struct{}{"platform.admin": {}}}
			var static, dnsInput model.PlatformArtifact
			dnsCase := strings.Contains(scenario, "dns")
			staticCase := strings.Contains(scenario, "static") || dnsCase
			if staticCase {
				gen := model.NewID("static")
				i := platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: gen, Routes: []platformconfig.RouteIntent{{Hostname: "static.example.test", UpstreamURL: "http://static:8080", Enabled: true}}}
				if scenario == "static domains complete" || scenario == "static domains invalid" {
					i.ApplicationDomains = &platformconfig.ApplicationDomainsIntent{AppBaseDomain: "example.test", ReservedHostnames: []string{}, DefaultDNSTTL: 180}
					if scenario == "static domains invalid" {
						i.ApplicationDomains.DefaultDNSTTL = 0
					}
				}
				if dnsCase {
					i.DNSConsumers = []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "edge-group-a", Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}
				}
				raw, _ := json.Marshal(i)
				var content map[string]any
				json.Unmarshal(raw, &content)
				var err error
				static, err = s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPlatformIntent, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: gen, Content: content})
				if err != nil {
					t.Fatal(err)
				}
				static, err = s.ValidatePlatformArtifact(static.ID, []model.PlatformArtifactValidationResult{{Name: "static", Pass: true}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if dnsCase {
				gen := model.NewID("dns-policy")
				probe := &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
				p := platformproducer.DNSPolicyInput{SchemaVersion: platformconfig.SchemaVersion, Generation: gen, Scope: "global", Authorities: []platformconfig.DNSAuthorityPolicy{{NodeID: "dns-a", Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}}, Clients: []platformconfig.DNSClientPolicy{{NodeID: "dns-a", Rules: []platformconfig.DNSClientRule{}}}, DNSReadiness: probe, TLSReadiness: probe, Cohorts: []platformconfig.TrafficRolloutCohort{{ID: "all", EdgeGroupIDs: []string{"edge-group-a"}}}}
				if scenario == "dns missing authority" {
					p.Authorities = nil
				}
				raw, _ := json.Marshal(p)
				var content map[string]any
				json.Unmarshal(raw, &content)
				var err error
				dnsInput, err = s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, Generation: gen, Content: content})
				if err != nil {
					t.Fatal(err)
				}
				dnsInput, err = s.ValidatePlatformArtifact(dnsInput.ID, []model.PlatformArtifactValidationResult{{Name: "dns", Pass: true}})
				if err != nil {
					t.Fatal(err)
				}
			}
			makePolicy := func(mode string) model.PlatformArtifactRelease {
				gen := model.NewID("producer-policy")
				policy := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: gen, Mode: mode, InputSource: "business-migration", TargetScope: "global", IntervalSeconds: 30, RefreshSeconds: 120}
				if staticCase {
					policy.InputSource = "business-static-intent"
					policy.RequireApplicationDomains = strings.HasPrefix(scenario, "static domains")
					policy.StaticIntentArtifactID = static.ID
					policy.StaticIntentDigest = static.ContentHash
					if scenario == "static digest" {
						policy.StaticIntentDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
					}
				}
				if dnsCase {
					policy.DNSPolicyArtifactID = dnsInput.ID
					policy.DNSPolicyDigest = dnsInput.ContentHash
					policy.HostedZoneTemplates = []platformproducer.HostedZoneTemplate{{NodeID: "dns-a", TemplateZone: "example.test"}}
					if scenario == "dns digest" {
						policy.DNSPolicyDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
					}
					if scenario == "dns template" {
						policy.HostedZoneTemplates[0].TemplateZone = "other.test"
					}
				}
				raw, _ := json.Marshal(policy)
				var content map[string]any
				json.Unmarshal(raw, &content)
				a, err := s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: platformproducer.Scope}, Generation: gen, Content: content})
				if err != nil {
					t.Fatal(err)
				}
				a, err = s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "policy", Pass: true}})
				if err != nil {
					t.Fatal(err)
				}
				_, r, _, _, err := s.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, principal)
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			authority := makePolicy("shadow")
			gen := model.NewID("producer-input")
			compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: gen, Scope: "global"}, Policy: platformconfig.PolicySnapshot{Generation: gen, Scope: "global"}})
			if err != nil {
				t.Fatal(err)
			}
			ids := []string{}
			for i, child := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
				if scenario == "member lineage" && i == 1 {
					child.Metadata["intent_digest"] = "sha256:wrong"
				}
				stored, err := s.CreatePlatformArtifact(child)
				if err != nil {
					t.Fatal(err)
				}
				stored, err = s.ValidatePlatformArtifact(stored.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true}})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, stored.ID)
			}
			// An older shadow is already active; the producer publishes a new
			// immutable generation rather than re-releasing that same generation.
			base := platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, ids, time.Now().UTC())
			base.Generation = model.NewID("baseline")
			base.Content["generation"] = base.Generation
			base, err = s.CreatePlatformArtifact(base)
			if err != nil {
				t.Fatal(err)
			}
			base, err = s.ValidatePlatformArtifact(base.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true}})
			if err != nil {
				t.Fatal(err)
			}
			_, baseline, _, _, err := s.ReleasePlatformArtifact(base.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, principal)
			if err != nil {
				t.Fatal(err)
			}
			parent := platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, ids, time.Now().UTC())
			parent.Metadata[platformproducer.PolicyReleaseMetadata] = authority.ID
			parent.Metadata[platformproducer.SourceDigestMetadata] = "sha256:source"
			if staticCase {
				parent.Metadata[platformproducer.StaticIntentIDMetadata] = static.ID
				parent.Metadata[platformproducer.StaticIntentDigestMetadata] = static.ContentHash
				if scenario == "static binding" {
					delete(parent.Metadata, platformproducer.StaticIntentIDMetadata)
				}
			}
			if dnsCase {
				parent.Metadata[platformproducer.DNSPolicyIDMetadata] = dnsInput.ID
				parent.Metadata[platformproducer.DNSPolicyDigestMetadata] = dnsInput.ContentHash
				if scenario == "dns binding" {
					delete(parent.Metadata, platformproducer.DNSPolicyIDMetadata)
				}
			}
			parent, err = s.CreatePlatformArtifact(parent)
			if err != nil {
				t.Fatal(err)
			}
			parent, err = s.ValidatePlatformArtifact(parent.ID, []model.PlatformArtifactValidationResult{{Name: "compiler", Pass: true}})
			if err != nil {
				t.Fatal(err)
			}
			freeze := func(scope string) {
				key := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindReleaseSet, scope, "shadow")
				if scope == platformproducer.Scope {
					key = platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, scope, "shadow")
				}
				if dsn != "" {
					if _, err := s.db.Exec(`UPDATE fugue_platform_release_lanes SET frozen=true WHERE lane_key=$1`, key); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := s.withLockedState(true, func(st *model.State) error {
						for i := range st.PlatformReleaseLanes {
							if st.PlatformReleaseLanes[i].LaneKey == key {
								st.PlatformReleaseLanes[i].Frozen = true
							}
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
			}
			switch scenario {
			case "dns invalidated":
				if _, err := s.ValidatePlatformArtifact(dnsInput.ID, []model.PlatformArtifactValidationResult{{Name: "invalid", Pass: false, Severity: model.RobustnessSeverityBlockPublish}}); err != nil {
					t.Fatal(err)
				}
			case "dns tampered":
				if dsn != "" {
					if _, err := s.db.Exec(`UPDATE fugue_platform_artifacts SET content_json=content_json || '{"tampered":true}'::jsonb WHERE id=$1`, dnsInput.ID); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := s.withLockedState(true, func(st *model.State) error {
						st.PlatformArtifacts[platformArtifactIndex(st.PlatformArtifacts, dnsInput.ID)].Content["tampered"] = true
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
			case "static revoked validation":
				if _, err := s.ValidatePlatformArtifact(static.ID, []model.PlatformArtifactValidationResult{{Name: "invalidated", Pass: false, Severity: model.RobustnessSeverityBlockPublish}}); err != nil {
					t.Fatal(err)
				}
			case "static tampered":
				if dsn != "" {
					if _, err := s.db.Exec(`UPDATE fugue_platform_artifacts SET content_json=content_json || '{"tampered":true}'::jsonb WHERE id=$1`, static.ID); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := s.withLockedState(true, func(st *model.State) error {
						st.PlatformArtifacts[platformArtifactIndex(st.PlatformArtifacts, static.ID)].Content["tampered"] = true
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
			case "paused":
				makePolicy("paused")
			case "superseded":
				makePolicy("shadow")
			case "policy frozen":
				freeze(platformproducer.Scope)
			case "target frozen":
				freeze("global")
			case "target changed":
				if _, _, _, _, err := s.ReleasePlatformArtifact(parent.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", IdempotencyKey: model.NewID("manual-replacement")}, principal); err != nil {
					t.Fatal(err)
				}
			case "tampered member":
				if dsn != "" {
					if _, err := s.db.Exec(`UPDATE fugue_platform_artifacts SET content_json=content_json || '{"tampered":true}'::jsonb WHERE id=$1`, ids[1]); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := s.withLockedState(true, func(st *model.State) error {
						st.PlatformArtifacts[platformArtifactIndex(st.PlatformArtifacts, ids[1])].Content["tampered"] = true
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
			case "wrong actor":
				principal.ActorID = "another-producer"
			}
			before, err := s.ListPlatformReleaseMessages(model.PlatformArtifactKindReleaseSet, "global", since, 200)
			if err != nil {
				t.Fatal(err)
			}
			publish := func() error {
				_, _, _, _, err := s.ReleaseProducedPlatformArtifact(parent.ID, authority.ID, baseline.ID, principal)
				return err
			}
			if strings.HasPrefix(scenario, "queued") {
				scope := "global"
				if scenario == "queued policy freeze" {
					scope = platformproducer.Scope
				}
				tx, err := s.db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if err = pgLockPromotionScope(context.Background(), tx, scope, true); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- publish() }()
				deadline := time.Now().Add(5 * time.Second)
				for {
					var n int
					if err = s.db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND wait_event='advisory'`).Scan(&n); err != nil {
						t.Fatal(err)
					}
					if n > 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("producer did not acquire scope locks before mutable reads")
					}
					time.Sleep(10 * time.Millisecond)
				}
				if scenario == "queued dns invalidation" {
					if _, err = tx.Exec(`UPDATE fugue_platform_artifacts SET status='invalid' WHERE id=$1`, dnsInput.ID); err != nil {
						t.Fatal(err)
					}
				} else if scenario == "queued static invalidation" {
					if _, err = tx.Exec(`UPDATE fugue_platform_artifacts SET status='invalid' WHERE id=$1`, static.ID); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err = tx.Exec(`UPDATE fugue_platform_release_lanes SET frozen=true WHERE scope_key=$1 AND release_channel='shadow'`, scope); err != nil {
						t.Fatal(err)
					}
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
				select {
				case err = <-done:
					if !errors.Is(err, ErrConflict) {
						t.Fatal("queued producer ignored freeze", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("producer deadlocked")
				}
			} else {
				err = publish()
			}
			after, e := s.ListPlatformReleaseMessages(model.PlatformArtifactKindReleaseSet, "global", since, 200)
			if e != nil {
				t.Fatal(e)
			}
			if scenario == "complete" || scenario == "retry" || scenario == "static complete" || scenario == "static domains complete" || scenario == "dns complete" {
				if err != nil || len(after) != len(before)+1 {
					t.Fatal("valid production failed", err)
				}
				if scenario == "retry" {
					if err = publish(); err != nil {
						t.Fatal("retry not idempotent", err)
					}
					again, _ := s.ListPlatformReleaseMessages(model.PlatformArtifactKindReleaseSet, "global", since, 200)
					if !reflect.DeepEqual(after, again) {
						t.Fatal("retry added another release")
					}
				}
			} else if !strings.HasPrefix(scenario, "queued") && !errors.Is(err, ErrConflict) || scenario != "complete" && scenario != "retry" && scenario != "static complete" && scenario != "static domains complete" && scenario != "dns complete" && !reflect.DeepEqual(before, after) {
				t.Fatal("failed producer mutated ledger", err)
			}
		})
	}
}
