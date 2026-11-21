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
