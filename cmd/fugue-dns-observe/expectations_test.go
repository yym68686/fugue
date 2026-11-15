package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsobservation"
	"fugue/internal/model"
	"github.com/miekg/dns"
)

func expectationFixture() (configuration, dnsobservation.Expectations, time.Time) {
	now := time.Now().UTC()
	query := queryIntent{Hostname: "app.example.test", Type: "A", Minimum: 1, AnswerSource: dnsobservation.ServingPolicySource}
	config := configuration{Schema: "fugue.dns-public-observation/v2", Servers: []string{"192.0.2.53", "192.0.2.54"}, Queries: []queryIntent{query}, TimeoutSeconds: 1}
	expected := dnsobservation.Expectations{Schema: dnsobservation.ExpectationsSchema, DeclarationDigest: "sha256:" + strings.Repeat("a", 64)}
	for i, server := range config.Servers {
		expected.Servers = append(expected.Servers, dnsobservation.Server{Server: server, ServiceUID: "service", PodUID: "pod", Policy: dnsobservation.Policy{
			Schema: dnsobservation.PolicySchema, NodeID: "dns", CapturedAt: now, ValidUntil: now.Add(time.Minute), ParentDigest: "sha256:" + strings.Repeat("b", 64),
			Assignment: model.PlatformConsumerAssignment{ArtifactKind: "dns_answer_bundle", ArtifactID: "artifact", ArtifactReleaseID: "release", ReleaseSetID: "parent", ScopeKey: "global", FencingToken: 2, GenerationSequence: 3, ExpectedGeneration: "generation", ExpectedConsumerSetID: "consumers", ReleaseChannel: "full", ContentHash: "sha256:" + strings.Repeat("c", 64)},
			Records:    []dnsobservation.Record{{Query: dnsobservation.Query{Hostname: query.Hostname, Type: query.Type}, Allowed: []string{[]string{"192.0.2.1", "192.0.2.2"}[i]}, MaximumTTL: 60}}}})
	}
	config.Expectations = &expected
	return config, expected, now
}

func TestExpectationsBindDeclarationServersQueriesAndTime(t *testing.T) {
	config, expected, now := expectationFixture()
	if err := validate(config); err != nil {
		t.Fatal(err)
	}
	if err := validateExpectations(expected, expected.DeclarationDigest, config, now); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"declaration", "future", "expired", "extended", "missing", "duplicate", "foreign-server", "foreign-query", "family", "duplicate-address", "fence", "shadow", "static-fallback"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(expected)
			var changed dnsobservation.Expectations
			_ = json.Unmarshal(raw, &changed)
			declaration := expected.DeclarationDigest
			switch scenario {
			case "declaration":
				declaration = "sha256:" + strings.Repeat("d", 64)
			case "future":
				changed.Servers[0].Policy.CapturedAt = now.Add(time.Second)
			case "expired":
				changed.Servers[0].Policy.ValidUntil = now
			case "extended":
				changed.Servers[0].Policy.ValidUntil = now.Add(time.Hour)
			case "missing":
				changed.Servers = changed.Servers[:1]
			case "duplicate":
				changed.Servers[1] = changed.Servers[0]
			case "foreign-server":
				changed.Servers[0].Server = "192.0.2.99"
			case "foreign-query":
				changed.Servers[0].Policy.Records[0].Hostname = "other.example.test"
			case "family":
				changed.Servers[0].Policy.Records[0].Allowed = []string{"2001:db8::1"}
			case "duplicate-address":
				changed.Servers[0].Policy.Records[0].Allowed = []string{"192.0.2.1", "192.0.2.1"}
			case "fence":
				changed.Servers[0].Policy.Assignment.FencingToken = 0
			case "shadow":
				changed.Servers[0].Policy.Assignment.ReleaseChannel = "shadow"
			case "static-fallback":
				config.Queries = append([]queryIntent(nil), config.Queries...)
				config.Queries[0].Allowed = []string{"192.0.2.99"}
				if validate(config) == nil {
					t.Fatal("static fallback accepted")
				}
				return
			}
			if validateExpectations(changed, declaration, config, now) == nil {
				t.Fatal("invalid expectations accepted")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "expected.json")
	raw, _ := json.Marshal(expected)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadExpectations(path, expected.DeclarationDigest, config, now); err != nil {
		t.Fatal(err)
	}
}

func TestExpectationsKeepAuthoritativeScopesSeparateAndRecursiveUnionExplicit(t *testing.T) {
	config, _, now := expectationFixture()
	for _, recursive := range []bool{false, true} {
		query, ttl, _, err := expectedIntent(config, config.Queries[0], config.Servers[0], recursive, now)
		want := []string{"192.0.2.1"}
		if recursive {
			want = append(want, "192.0.2.2")
		}
		if err != nil || ttl != 60 || !reflect.DeepEqual(query.Allowed, want) {
			t.Fatal(query, ttl, err)
		}
		request := new(dns.Msg)
		request.SetQuestion("app.example.test.", dns.TypeA)
		request.RecursionDesired = recursive
		answer := new(dns.Msg)
		answer.SetReply(request)
		answer.Authoritative, answer.RecursionAvailable = !recursive, recursive
		answer.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: "app.example.test.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}, A: []byte{192, 0, 2, 2}}}
		result := observation{}
		evaluate(answer, request, query, &result)
		if result.Passed != recursive {
			t.Fatal("server policy boundary lost", result)
		}
	}
	if _, _, _, err := expectedIntent(config, config.Queries[0], config.Servers[0], false, now.Add(2*time.Minute)); err == nil {
		t.Fatal("expired expectations used")
	}
	config.Expectations = nil
	if _, _, _, err := expectedIntent(config, config.Queries[0], config.Servers[0], false, now); err == nil {
		t.Fatal("missing expectations used")
	}
}

func TestRecoveredAuthorizedAddressPassesWithoutLearningUnexpectedAnswers(t *testing.T) {
	config, expected, now := expectationFixture()
	expected.Servers[0].Policy.Records[0].Allowed = append(expected.Servers[0].Policy.Records[0].Allowed, "192.0.2.2")
	config.Expectations = &expected
	before, _ := json.Marshal(expected)
	query, _, _, err := expectedIntent(config, config.Queries[0], config.Servers[0], false, now)
	if err != nil {
		t.Fatal(err)
	}
	request := new(dns.Msg)
	request.SetQuestion("app.example.test.", dns.TypeA)
	request.RecursionDesired = false
	for _, last := range []byte{2, 99} {
		answer := new(dns.Msg)
		answer.SetReply(request)
		answer.Authoritative = true
		answer.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: "app.example.test.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}, A: []byte{192, 0, 2, last}}}
		result := observation{}
		evaluate(answer, request, query, &result)
		if result.Passed != (last == 2) {
			t.Fatal("recovery membership or unknown-answer rejection lost", result)
		}
	}
	after, _ := json.Marshal(config.Expectations)
	if string(before) != string(after) {
		t.Fatal("observations modified authenticated expectations")
	}
}
