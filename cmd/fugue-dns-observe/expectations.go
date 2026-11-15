package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"fugue/internal/dnsobservation"
)

var expectationDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func loadExpectations(path, declarationDigest string, config configuration, now time.Time) (*dnsobservation.Expectations, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var expected dnsobservation.Expectations
	decoder := json.NewDecoder(io.LimitReader(file, 256<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&expected) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid bounded expectations")
	}
	if err := validateExpectations(expected, declarationDigest, config, now); err != nil {
		return nil, err
	}
	return &expected, nil
}

func validateExpectations(expected dnsobservation.Expectations, declarationDigest string, config configuration, now time.Time) error {
	fail := errors.New("expectations do not match the current declaration or validity window")
	if config.Schema != "fugue.dns-public-observation/v2" || expected.Schema != dnsobservation.ExpectationsSchema || !expectationDigest.MatchString(declarationDigest) ||
		expected.DeclarationDigest != declarationDigest || len(expected.Servers) != len(config.Servers) {
		return fail
	}
	queries := map[string]queryIntent{}
	for _, query := range config.Queries {
		if query.AnswerSource != "" {
			key := strings.ToLower(strings.TrimSuffix(query.Hostname, ".")) + "\x00" + query.Type
			if _, duplicate := queries[key]; duplicate {
				return fail
			}
			queries[key] = query
		}
	}
	seen := map[string]bool{}
	for _, server := range expected.Servers {
		p := server.Policy
		a := p.Assignment
		if !slices.Contains(config.Servers, server.Server) || seen[server.Server] || server.ServiceUID == "" || server.PodUID == "" || p.NodeID == "" ||
			p.Schema != dnsobservation.PolicySchema || !expectationDigest.MatchString(p.ParentDigest) || !expectationDigest.MatchString(a.ContentHash) ||
			a.ArtifactKind != "dns_answer_bundle" || a.ArtifactID == "" || a.ArtifactReleaseID == "" || a.ReleaseSetID == "" || a.ScopeKey == "" ||
			a.FencingToken <= 0 || a.GenerationSequence <= 0 || a.ExpectedGeneration == "" || a.ExpectedConsumerSetID == "" ||
			!slices.Contains([]string{"gray", "full"}, a.ReleaseChannel) ||
			p.CapturedAt.IsZero() || p.CapturedAt.After(now) || !p.ValidUntil.After(now) || p.ValidUntil.Sub(p.CapturedAt) > 10*time.Minute || len(p.Records) != len(queries) {
			return fail
		}
		seen[server.Server] = true
		records := map[string]bool{}
		for _, record := range p.Records {
			key := record.Hostname + "\x00" + record.Type
			if _, exists := queries[key]; !exists || records[key] || len(record.Allowed) > 32 || len(record.Allowed) > 0 && record.MaximumTTL == 0 {
				return fail
			}
			records[key] = true
			addresses := map[string]bool{}
			for _, value := range record.Allowed {
				ip, err := netip.ParseAddr(value)
				if err != nil || ip.String() != value || ip.Is4() != (record.Type == "A") || addresses[value] {
					return fail
				}
				addresses[value] = true
			}
		}
	}
	return nil
}

func expectedIntent(config configuration, query queryIntent, server string, recursive bool, now time.Time) (queryIntent, uint32, []string, error) {
	if query.AnswerSource == "" {
		return query, 0, nil, nil
	}
	if config.Expectations == nil || validateExpectations(*config.Expectations, config.Expectations.DeclarationDigest, config, now) != nil {
		return query, 0, nil, errors.New("verified expectations unavailable")
	}
	query.Allowed = []string{}
	var maximumTTL uint32
	digests := []string{}
	for _, expected := range config.Expectations.Servers {
		if !recursive && expected.Server != server {
			continue
		}
		for _, record := range expected.Policy.Records {
			if record.Hostname == strings.ToLower(strings.TrimSuffix(query.Hostname, ".")) && record.Type == query.Type {
				query.Allowed = append(query.Allowed, record.Allowed...)
				maximumTTL = max(maximumTTL, record.MaximumTTL)
				digests = append(digests, expected.Policy.Assignment.ContentHash)
			}
		}
	}
	slices.Sort(query.Allowed)
	query.Allowed = slices.Compact(query.Allowed)
	if len(digests) == 0 {
		return query, 0, nil, errors.New("verified server expectation absent")
	}
	return query, maximumTTL, digests, nil
}
