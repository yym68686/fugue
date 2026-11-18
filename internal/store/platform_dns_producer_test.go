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

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/testfixture/celldns"
)

func TestDNSProducerPublicationGuard(t *testing.T) { testDNSProducerPublicationGuard(t, "") }

func TestDNSProducerPublicationGuardPostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable PostgreSQL required")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback PostgreSQL")
	}
	testDNSProducerPublicationGuard(t, dsn)
}

func testDNSProducerPublicationGuard(t *testing.T, dsn string) {
	for _, scenario := range []string{"complete", "retry", "paused", "source frozen", "wrong source fence", "changed TTL", "foreign topology", "wrong actor"} {
		t.Run(scenario, func(t *testing.T) {
			// Reuse the isolated database/schema fixture; its existing Cell LKG
			// also proves an unrelated authority remains unchanged.
			f := newReconfigurationFixture(t, dsn)
			s := f.s
			beforeLKG, err := s.GetPlatformLKG(f.lkg.ArtifactKind, f.lkg.ScopeKey)
			if err != nil || beforeLKG == nil {
				t.Fatal("fixture LKG unavailable", err)
			}
			req, policies, releases := prepareAuthorizedCellDNSPublication(t, s, true)
			celldns.RetainGlobalOnly(&req)
			for i, a := range policies {
				if a.ID != req.PreviousTrafficPublication.ProducerPolicy.ArtifactID {
					continue
				}
				if _, _, err := s.EnsurePlatformArtifact(a); err != nil {
					t.Fatal(err)
				}
				seedDNSRouteRelease(t, s, releases[i])
			}
			seedCellDNSRoutePublications(t, s, &req)
			p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "dns-owner", Mode: "shadow", InputSource: platformproducer.DNSProducerInputSource, PublicationRole: platformconfig.PublicationRoleCellDNS, AuthorityCellID: "cell-dns", TargetScope: "authority-cell:cell-dns", IntervalSeconds: 30, RefreshSeconds: 120,
				DNSSource: &platformproducer.DNSProducerSource{ProducerPolicy: *req.PreviousTrafficPublication.ProducerPolicy, PreviousTopology: req.Intent.RouteAuthorityTransition.PreviousTopology, DNSNodeIDs: []string{"dns-a"}}}
			if scenario == "paused" {
				p.Mode = "paused"
			}
			save := func(a model.PlatformArtifact) model.PlatformArtifact {
				t.Helper()
				stored, err := s.CreatePlatformArtifact(a)
				if err != nil {
					t.Fatal(err)
				}
				stored, err = s.ValidatePlatformArtifact(stored.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
				if err != nil {
					t.Fatal(err)
				}
				return stored
			}
			raw, _ := json.Marshal(p)
			content := map[string]any{}
			json.Unmarshal(raw, &content)
			a := save(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{Key: "platform-config-producer:cell-dns"}, Generation: p.Generation, Content: content})
			_, authority, _, _, err := s.ReleasePlatformArtifact(a.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
			if err != nil {
				t.Fatal(err)
			}
			_, source := celldns.TransitionRequestWithSource(t)
			projection, err := platformproducer.ComposeDNSProjection(p, *req.PreviousTrafficPublication, source.Intent, source.Policy, source.RuntimeSnapshot)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "changed TTL" {
				projection.Policy.DNSAnswerRules[0].TTLSeconds++
			}
			if scenario == "foreign topology" {
				projection.Intent.EdgeTopology.Edges[0].Labels = map[string]string{"country": "xx"}
				projection.Intent.RouteAuthorityTransition.PreviousTopology.Edges[0].Labels = map[string]string{"country": "xx"}
			}
			compiled, err := platformconfig.Compile(projection)
			if err != nil {
				t.Fatal(err)
			}
			prepareCompiledInputPin(t, s, compiled)
			if _, _, err := s.EnsurePlatformArtifact(compiled.PolicyArtifact); err != nil {
				t.Fatal(err)
			}
			child := save(compiled.DNSArtifact)
			parent := platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, []string{child.ID}, req.CreatedAt)
			parent.Metadata[platformproducer.PolicyReleaseMetadata] = authority.ID
			parent.Metadata[platformproducer.SourceDigestMetadata] = "sha256:" + strings.Repeat("a", 64)
			parent = save(parent)
			if scenario == "source frozen" || scenario == "wrong source fence" {
				if s.db != nil {
					query := `UPDATE fugue_platform_release_lanes SET frozen=true WHERE scope_key=$1`
					if scenario == "wrong source fence" {
						query = `UPDATE fugue_platform_release_lanes SET fencing_token=fencing_token+1 WHERE scope_key=$1`
					}
					if _, err := s.db.Exec(query, platformproducer.Scope); err != nil {
						t.Fatal(err)
					}
				} else if err := s.withLockedState(true, func(state *model.State) error {
					for i := range state.PlatformReleaseLanes {
						l := &state.PlatformReleaseLanes[i]
						if l.ScopeKey == platformproducer.Scope {
							if scenario == "source frozen" {
								l.Frozen = true
							} else {
								l.FencingToken++
							}
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			actor := model.Principal{ActorType: model.ActorTypeBootstrap, ActorID: platformproducer.Actor, Scopes: map[string]struct{}{"platform.admin": {}}}
			if scenario == "wrong actor" {
				actor.ActorID = "foreign"
			}
			if s.db == nil {
				if err := s.withLockedState(false, func(state *model.State) error {
					req, guard := producedShadowIntent(parent.ID, authority.ID, "")
					before, err := json.Marshal(state)
					if err != nil {
						t.Fatal(err)
					}
					want := validateProducerReleaseGuard(state, parent, req, actor, s.platformArtifactSigningKeyring(), guard)
					ctx, mutation := startPlatformMutationTrace(context.Background(), "platform-release")
					trace := beginPlatformProducerGuardTrace(ctx)
					got := validateProducerReleaseGuardWithTrace(state, parent, req, actor, s.platformArtifactSigningKeyring(), guard, trace)
					trace.finish()
					if want != got {
						t.Fatal("observation changed original guard error identity", want, got)
					}
					if (scenario == "complete" || scenario == "retry") != (got == nil) {
						t.Fatal("comparison did not exercise expected guard decision", got)
					}
					after, err := json.Marshal(state)
					if err != nil || string(before) != string(after) {
						t.Fatal("observation modified source state", err)
					}
					last := "lane"
					switch scenario {
					case "wrong actor":
						last = "parent_integrity"
					case "paused":
						last = "policy_authority"
					case "source frozen", "wrong source fence":
						last = "source_authority"
					case "changed TTL", "foreign topology":
						last = "dns_output"
					}
					observation := mutation.producerGuardObservation()
					if observation == nil || observation.FinalPhase != last || len(observation.Stages) > int(producerGuardPhaseCount) {
						t.Fatal("guard rejection did not retain its original phase", observation)
					}
					encoded, err := json.Marshal(observation)
					if err != nil || strings.Contains(string(encoded), parent.ID) || strings.Contains(string(encoded), authority.ID) || len(encoded) > 2048 {
						t.Fatal("guard observation retained input or exceeded budget", err)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			_, r, _, _, err := s.ReleaseProducedPlatformArtifact(parent.ID, authority.ID, "", actor)
			if s.db != nil {
				var observation *platformProducerGuardObservation
				for _, receipt := range platformMutationObservations.snapshot()["receipts"].([]platformMutationReceipt) {
					if receipt.Operation.Name == "platform-release" && receipt.ProducerGuard != nil {
						observation = receipt.ProducerGuard
					}
				}
				if observation == nil {
					t.Fatal("actual PostgreSQL producer guard omitted its phase receipt")
				}
				seen := map[string]bool{}
				for _, stage := range observation.Stages {
					seen[stage.Stage] = true
				}
				phases := []string{"read_lanes", "read_policy", "read_members", "read_dns_references", "parent_integrity"}
				if scenario != "wrong actor" {
					phases = append(phases, "policy_authority")
				}
				for _, phase := range phases {
					if !seen[phase] {
						t.Fatal("actual PostgreSQL guard omitted a reached phase", phase, observation)
					}
				}
			}
			if scenario != "complete" && scenario != "retry" {
				if !errors.Is(err, ErrConflict) {
					t.Fatal("unsafe DNS publication accepted", err)
				}
				if _, _, found, _ := s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, p.TargetScope, "shadow"); found {
					t.Fatal("failed publication wrote a lane")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "retry" {
					_, again, _, _, err := s.ReleaseProducedPlatformArtifact(parent.ID, authority.ID, "", actor)
					if err != nil || again.ID != r.ID {
						t.Fatal("retry advanced publication", err)
					}
				}
			}
			lkg, err := s.GetPlatformLKG(f.lkg.ArtifactKind, f.lkg.ScopeKey)
			if err != nil || lkg == nil || !reflect.DeepEqual(lkg, beforeLKG) {
				t.Fatal("DNS publication changed unrelated LKG", err)
			}
		})
	}
}
