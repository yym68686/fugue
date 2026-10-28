package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
)

const DNSAvailabilityVersion = "dns-availability-v1"
const DNSAvailabilityRedundantVersion = "dns-availability-redundant-v2"

// DNSAvailabilityPolicy is signed configuration, not a health or quality fact.
// Membership cannot add an endpoint to a record's route/TLS authorization.
type DNSAvailabilityPolicy struct {
	MaxAnswers         int      `json:"max_answers,omitempty"`
	Version            string   `json:"version"`
	OrderedEdgeIDs     []string `json:"ordered_edge_ids"`
	RecoverySuccesses  int      `json:"recovery_successes"`
	RecoveryMinSeconds int      `json:"recovery_min_seconds"`
}

func (p *DNSAvailabilityPolicy) UnmarshalJSON(raw []byte) error {
	type plain DNSAvailabilityPolicy
	var v plain
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return errors.New("complete DNS availability policy required")
	}
	var version string
	if json.Unmarshal(fields["version"], &version) != nil ||
		version == DNSAvailabilityVersion && (len(fields) != 4 || fields["max_answers"] != nil) ||
		version == DNSAvailabilityRedundantVersion && (len(fields) != 5 || fields["max_answers"] == nil || bytes.Equal(bytes.TrimSpace(fields["max_answers"]), []byte("null"))) {
		return errors.New("DNS availability fields differ from declared version")
	}
	for _, key := range []string{"version", "ordered_edge_ids", "recovery_successes", "recovery_min_seconds"} {
		if len(fields[key]) == 0 || bytes.Equal(bytes.TrimSpace(fields[key]), []byte("null")) {
			return errors.New("DNS availability policy has missing or null fields")
		}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("DNS availability policy has trailing data")
	}
	candidate := DNSAvailabilityPolicy(v)
	if err := ValidateDNSAvailabilityPolicy(&candidate); err != nil {
		return err
	}
	*p = candidate
	return nil
}

func ValidateDNSAvailabilityPolicy(p *DNSAvailabilityPolicy) error {
	if p == nil || p.RecoverySuccesses < 2 || p.RecoverySuccesses > 5 || p.RecoveryMinSeconds < 10 || p.RecoveryMinSeconds > 300 {
		return errors.New("invalid DNS availability policy or recovery bounds")
	}
	switch p.Version {
	case DNSAvailabilityVersion:
		if p.MaxAnswers != 0 {
			return errors.New("single-answer availability cannot authorize redundant answers")
		}
	case DNSAvailabilityRedundantVersion:
		if p.MaxAnswers < 2 || p.MaxAnswers > 8 || p.MaxAnswers > len(p.OrderedEdgeIDs) {
			return errors.New("redundant availability requires 2-8 answers within its explicit pool")
		}
	default:
		return errors.New("unsupported DNS availability version")
	}
	return ValidateDNSPhysicalOrder(&DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: p.OrderedEdgeIDs})
}

func CloneDNSAvailabilityPolicy(p *DNSAvailabilityPolicy) *DNSAvailabilityPolicy {
	if p == nil {
		return nil
	}
	out := *p
	out.OrderedEdgeIDs = slices.Clone(p.OrderedEdgeIDs)
	return &out
}
