package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"fugue/internal/dnsobservation"
	"github.com/miekg/dns"
)

type queryIntent struct {
	AnswerSource string   `json:"answer_source,omitempty"`
	Hostname     string   `json:"hostname"`
	Type         string   `json:"type"`
	Allowed      []string `json:"allowed_answers"`
	Minimum      int      `json:"minimum_answers"`
}

type configuration struct {
	Expectations     *dnsobservation.Expectations `json:"-"`
	Schema           string                       `json:"schema"`
	Servers          []string                     `json:"servers"`
	RecursiveServers []string                     `json:"recursive_servers,omitempty"`
	AuthorityPort    int                          `json:"authority_port,omitempty"`
	Queries          []queryIntent                `json:"queries"`
	TimeoutSeconds   int                          `json:"timeout_seconds"`
}

type observation struct {
	Server            string    `json:"server"`
	Source            string    `json:"source"`
	Transport         string    `json:"transport"`
	Hostname          string    `json:"hostname"`
	Type              string    `json:"type"`
	QueryID           uint16    `json:"query_id"`
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
	DurationMS        int64     `json:"duration_ms"`
	ResponseCode      int       `json:"response_code"`
	Authoritative     bool      `json:"authoritative"`
	Answers           []string  `json:"answers"`
	TTLs              []uint32  `json:"ttls"`
	Reason            string    `json:"reason"`
	Passed            bool      `json:"passed"`
	ExpectationSource string    `json:"expectation_source,omitempty"`
	PolicyDigests     []string  `json:"policy_digests,omitempty"`
}

func validate(config configuration) error {
	if !slices.Contains([]string{"fugue.dns-public-observation/v1", "fugue.dns-public-observation/v2"}, config.Schema) || len(config.Servers) < 1 || len(config.Servers)+len(config.RecursiveServers) > 8 || len(config.Queries) < 1 || len(config.Queries) > 32 || config.TimeoutSeconds < 1 || config.TimeoutSeconds > 5 {
		return errors.New("bounded DNS observation configuration required")
	}
	if config.AuthorityPort < 0 || config.AuthorityPort > 65535 || config.AuthorityPort != 0 && config.AuthorityPort != 53 &&
		(config.Schema != "fugue.dns-public-observation/v2" || len(config.RecursiveServers) != 0) {
		return errors.New("an alternate observation port requires an authoritative-only v2 declaration")
	}
	for _, server := range append(slices.Clone(config.Servers), config.RecursiveServers...) {
		if net.ParseIP(server) == nil {
			return errors.New("explicit DNS server address required")
		}
	}
	for _, query := range config.Queries {
		if _, valid := dns.IsDomainName(query.Hostname); !valid || query.Hostname == "" || !slices.Contains([]string{"A", "AAAA", "NS", "SOA"}, query.Type) || query.Minimum < 0 || query.Minimum > 32 || len(query.Allowed) > 32 {
			return errors.New("invalid bounded DNS observation query")
		}
		if query.AnswerSource != "" {
			if config.Schema != "fugue.dns-public-observation/v2" || query.AnswerSource != dnsobservation.ServingPolicySource || !slices.Contains([]string{"A", "AAAA"}, query.Type) || len(query.Allowed) != 0 {
				return errors.New("explicit serving-policy address source without static fallback required")
			}
		} else if (query.Type == "A" || query.Type == "AAAA") && query.Minimum > len(query.Allowed) {
			return errors.New("DNS address observations require explicit allowed values")
		}
	}
	return nil
}

func evaluate(answer *dns.Msg, request *dns.Msg, intent queryIntent, observed *observation) {
	if answer == nil || !answer.Response || answer.Id != request.Id || answer.Truncated || len(answer.Question) != 1 || answer.Question[0] != request.Question[0] {
		observed.Reason = "response_identity_or_completeness"
		return
	}
	observed.ResponseCode, observed.Authoritative = answer.Rcode, answer.Authoritative
	if answer.Rcode != dns.RcodeSuccess || request.RecursionDesired && !answer.RecursionAvailable || !request.RecursionDesired && !answer.Authoritative {
		observed.Reason = "non_authoritative_or_failed_answer"
		return
	}
	for _, record := range answer.Answer {
		if record.Header().Rrtype != request.Question[0].Qtype || !strings.EqualFold(record.Header().Name, request.Question[0].Name) {
			observed.Reason = "unexpected_record"
			return
		}
		value := record.String()
		switch typed := record.(type) {
		case *dns.A:
			value = typed.A.String()
		case *dns.AAAA:
			value = typed.AAAA.String()
		case *dns.NS:
			value = typed.Ns
		}
		observed.Answers = append(observed.Answers, value)
		observed.TTLs = append(observed.TTLs, record.Header().Ttl)
		if (intent.Type == "A" || intent.Type == "AAAA" || len(intent.Allowed) > 0) && !slices.Contains(intent.Allowed, value) {
			observed.Reason = "answer_outside_declared_set"
			return
		}
	}
	if len(observed.Answers) < intent.Minimum {
		observed.Reason = "answer_count_below_minimum"
		return
	}
	observed.Reason, observed.Passed = "verified_sample", true
}

func observe(ctx context.Context, config configuration) ([]observation, bool) {
	results := []observation{}
	passed := true
	for index, server := range append(slices.Clone(config.Servers), config.RecursiveServers...) {
		recursive := index >= len(config.Servers)
		port := "53"
		if !recursive && config.AuthorityPort != 0 {
			port = strconv.Itoa(config.AuthorityPort)
		}
		for _, transport := range []string{"udp", "tcp"} {
			for _, intent := range config.Queries {
				request := new(dns.Msg)
				request.SetQuestion(dns.Fqdn(intent.Hostname), dns.StringToType[intent.Type])
				request.RecursionDesired = recursive
				client := &dns.Client{Net: transport, Timeout: time.Duration(config.TimeoutSeconds) * time.Second}
				result := observation{Server: server, Transport: transport, Hostname: intent.Hostname, Type: intent.Type, QueryID: request.Id, StartedAt: time.Now().UTC(), ResponseCode: -1, Answers: []string{}, TTLs: []uint32{}}
				result.Source = "authoritative"
				if recursive {
					result.Source = "recursive_sample"
				}
				expected, maximumTTL, digests, expectationErr := expectedIntent(config, intent, server, recursive, result.StartedAt)
				if expectationErr != nil {
					result.Reason = "verified_expectation_unavailable"
					result.FinishedAt = time.Now().UTC()
					passed = false
					results = append(results, result)
					continue
				}
				result.ExpectationSource, result.PolicyDigests = intent.AnswerSource, digests
				answer, elapsed, err := client.ExchangeContext(ctx, request, net.JoinHostPort(server, port))
				result.FinishedAt, result.DurationMS = time.Now().UTC(), elapsed.Milliseconds()
				if err != nil {
					result.Reason = "transport_unavailable"
				} else {
					evaluate(answer, request, expected, &result)
					if intent.AnswerSource != "" {
						if _, _, _, err := expectedIntent(config, intent, server, recursive, result.FinishedAt); err != nil {
							result.Passed, result.Reason = false, "verified_expectation_expired"
						} else {
							for _, ttl := range result.TTLs {
								if ttl > maximumTTL {
									result.Passed, result.Reason = false, "answer_ttl_exceeds_signed_policy"
								}
							}
						}
					}
				}
				passed = passed && result.Passed
				results = append(results, result)
			}
		}
	}
	return results, passed
}

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Fprintln(os.Stderr, "DNS observation declaration and optional verified expectations required")
		os.Exit(1)
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "DNS observation declaration unavailable")
		os.Exit(1)
	}
	raw, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	_ = file.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, "DNS observation declaration unreadable")
		os.Exit(1)
	}
	var config configuration
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if len(raw) > 64<<10 || decoder.Decode(&config) != nil || decoder.Decode(&struct{}{}) != io.EOF || validate(config) != nil {
		fmt.Fprintln(os.Stderr, "DNS observation declaration invalid")
		os.Exit(1)
	}
	if len(os.Args) == 3 {
		config.Expectations, err = loadExpectations(os.Args[2], fmt.Sprintf("sha256:%x", sha256.Sum256(raw)), config, time.Now().UTC())
		if err != nil {
			fmt.Fprintln(os.Stderr, "DNS verified expectations invalid or expired")
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"schema": config.Schema, "passed": false, "authorizes_traffic": false, "reason": "verified_expectations_invalid_or_expired", "observations": []observation{}})
			os.Exit(1)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	results, passed := observe(ctx, config)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"schema": config.Schema, "passed": passed, "authorizes_traffic": false, "coverage": "declared_authoritative_and_recursive_samples_only", "selection_correctness": "not_verified_by_membership_observation", "client_traffic_share": "not_measured", "expectations": config.Expectations, "observations": results})
	if !passed {
		os.Exit(1)
	}
}
