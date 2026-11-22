package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestUnreadyPredecessorModeNeverExportsServingAuthorization(t *testing.T) {
	for _, tc := range []struct {
		input, rejection string
	}{
		{`{"observation_queries":[{"hostname":"example.test","type":"A"}]}`, "queries require explicit policy export"},
		{`{"handoff_previous":{}}`, "cannot authorize a handoff"},
		{`{"availability_route":{"hostname":"example.test","policy":{"version":"dns-availability-v1","ordered_edge_ids":["edge-a"],"recovery_successes":2,"recovery_min_seconds":10}}}`, "cannot authorize a handoff"},
		{`{"checkpoint":{}}`, "checkpoint authentication failed"},
	} {
		var out bytes.Buffer
		if err := verify(strings.NewReader(tc.input), &out, unreadyBackendMode); err == nil || !strings.Contains(err.Error(), tc.rejection) || out.Len() != 0 {
			t.Fatalf("unready verifier did not reject %s with %q: err=%v output=%q", tc.input, tc.rejection, err, out.String())
		}
	}
}

func TestPairRequiresTwoBoundServingObservationsAndCannotAcceptAClockOrNegativeMode(t *testing.T) {
	for _, tc := range []struct{ input, reason string }{
		{`{"candidate":{}}`, "invalid bounded DNS pair"},
		{`{"candidate":{},"fallback":{},"now":"2001-01-01T00:00:00Z"}`, "invalid bounded DNS pair"},
		{`{"candidate":{"snapshot":{"plan_digest":"first"}},"fallback":{"snapshot":{"plan_digest":"other"}}}`, "pair assignment mismatch"},
		{`{"candidate":{"handoff_previous":{}},"fallback":{}}`, "only exact serving inputs"},
		{`{"candidate":{},"fallback":{}}`, "candidate: DNS observation checkpoint authentication failed"},
	} {
		var out bytes.Buffer
		if err := verify(strings.NewReader(tc.input), &out, servingPairMode); err == nil || !strings.Contains(err.Error(), tc.reason) || out.Len() != 0 {
			t.Fatalf("pair produced a receipt or incorrect rejection: %v %q", err, out.String())
		}
	}
}
