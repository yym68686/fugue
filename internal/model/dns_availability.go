package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
)

const DNSAvailabilityVersion = "dns-availability-v1"

// DNSAvailabilityPolicy is signed configuration, not a health or quality fact.
// Membership cannot add an endpoint to a record's route/TLS authorization.
type DNSAvailabilityPolicy struct {
	Version            string   `json:"version"`
	OrderedEdgeIDs     []string `json:"ordered_edge_ids"`
	RecoverySuccesses  int      `json:"recovery_successes"`
	RecoveryMinSeconds int      `json:"recovery_min_seconds"`
}

func (p *DNSAvailabilityPolicy) UnmarshalJSON(raw []byte) error {
	type plain DNSAvailabilityPolicy
	var v plain
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 4 {
		return errors.New("complete DNS availability policy required")
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
	if p == nil || p.Version != DNSAvailabilityVersion || p.RecoverySuccesses < 2 || p.RecoverySuccesses > 5 || p.RecoveryMinSeconds < 10 || p.RecoveryMinSeconds > 300 {
		return errors.New("invalid DNS availability policy or recovery bounds")
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
