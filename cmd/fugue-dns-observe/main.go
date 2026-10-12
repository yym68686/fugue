package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/miekg/dns"
)

type queryIntent struct {
	Hostname string   `json:"hostname"`
	Type     string   `json:"type"`
	Allowed  []string `json:"allowed_answers"`
	Minimum  int      `json:"minimum_answers"`
}

type configuration struct {
	Schema         string        `json:"schema"`
	Servers        []string      `json:"servers"`
	Queries        []queryIntent `json:"queries"`
	TimeoutSeconds int           `json:"timeout_seconds"`
}

type observation struct {
	Server        string    `json:"server"`
	Transport     string    `json:"transport"`
	Hostname      string    `json:"hostname"`
	Type          string    `json:"type"`
	QueryID       uint16    `json:"query_id"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	DurationMS    int64     `json:"duration_ms"`
	ResponseCode  int       `json:"response_code"`
	Authoritative bool      `json:"authoritative"`
	Answers       []string  `json:"answers"`
	TTLs          []uint32  `json:"ttls"`
	Reason        string    `json:"reason"`
	Passed        bool      `json:"passed"`
}

func validate(config configuration) error {
	if config.Schema != "fugue.dns-public-observation/v1" || len(config.Servers) < 1 || len(config.Servers) > 8 || len(config.Queries) < 1 || len(config.Queries) > 32 || config.TimeoutSeconds < 1 || config.TimeoutSeconds > 5 {
		return errors.New("bounded DNS observation configuration required")
	}
	for _, server := range config.Servers {
		if net.ParseIP(server) == nil {
			return errors.New("explicit DNS server address required")
		}
	}
	for _, query := range config.Queries {
		if _, valid := dns.IsDomainName(query.Hostname); !valid || query.Hostname == "" || !slices.Contains([]string{"A", "AAAA", "NS", "SOA"}, query.Type) || query.Minimum < 0 || query.Minimum > 32 || len(query.Allowed) > 32 {
			return errors.New("invalid bounded DNS observation query")
		}
		if (query.Type == "A" || query.Type == "AAAA") && query.Minimum > len(query.Allowed) {
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
	if answer.Rcode != dns.RcodeSuccess || !answer.Authoritative {
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
	for _, server := range config.Servers {
		for _, transport := range []string{"udp", "tcp"} {
			for _, intent := range config.Queries {
				request := new(dns.Msg)
				request.SetQuestion(dns.Fqdn(intent.Hostname), dns.StringToType[intent.Type])
				request.RecursionDesired = false
				client := &dns.Client{Net: transport, Timeout: time.Duration(config.TimeoutSeconds) * time.Second}
				result := observation{Server: server, Transport: transport, Hostname: intent.Hostname, Type: intent.Type, QueryID: request.Id, StartedAt: time.Now().UTC(), ResponseCode: -1, Answers: []string{}, TTLs: []uint32{}}
				answer, elapsed, err := client.ExchangeContext(ctx, request, net.JoinHostPort(server, "53"))
				result.FinishedAt, result.DurationMS = time.Now().UTC(), elapsed.Milliseconds()
				if err != nil {
					result.Reason = "transport_unavailable"
				} else {
					evaluate(answer, request, intent, &result)
				}
				passed = passed && result.Passed
				results = append(results, result)
			}
		}
	}
	return results, passed
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "one DNS observation declaration is required")
		os.Exit(1)
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "DNS observation declaration unavailable")
		os.Exit(1)
	}
	defer file.Close()
	var config configuration
	decoder := json.NewDecoder(io.LimitReader(file, 64<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(&struct{}{}) != io.EOF || validate(config) != nil {
		fmt.Fprintln(os.Stderr, "DNS observation declaration invalid")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	results, passed := observe(ctx, config)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"schema": config.Schema, "passed": passed, "authorizes_traffic": false, "coverage": "declared_authoritative_samples_only", "observations": results})
	if !passed {
		os.Exit(1)
	}
}
