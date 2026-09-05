package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

func TestActivationEvidenceReadRetriesTransportFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		failures  int
		response  string
		wantError bool
		attempts  string
		present   bool
	}{
		{"transient EOF", 1, `{"schema":"` + edgeActivationStateSchema + `","edge_group_id":"edge-group-test","generation":1,"authority":"` + edgeActivationAuthority + `","active_slot":"a"}`, false, "2", true},
		{"absent file after EOF", 1, "absent", false, "2", false},
		{"persistent EOF", 3, "absent", true, "2", false},
		{"invalid evidence", 0, `{}`, true, "1", false},
		{"wrong group", 0, `{"schema":"` + edgeActivationStateSchema + `","edge_group_id":"edge-group-other","generation":1,"authority":"` + edgeActivationAuthority + `","active_slot":"a"}`, true, "1", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			counter := filepath.Join(directory, "attempts")
			kubectl := filepath.Join(directory, "kubectl")
			program := fmt.Sprintf(`#!/bin/sh
set -eu
count=0
if test -f "$ACTIVATION_READ_COUNT"; then read -r count <"$ACTIVATION_READ_COUNT"; fi
count=$((count + 1))
printf '%%s\n' "$count" >"$ACTIVATION_READ_COUNT"
if test "$count" -le %d; then
  printf 'Error from server: error dialing backend: EOF\n' >&2
  exit 1
fi
printf '%%s\n' '%s'
`, test.failures, test.response)
			if err := os.WriteFile(kubectl, []byte(program), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("ACTIVATION_READ_COUNT", counter)
			cluster := &kubectlCluster{kubectl: kubectl, readAttempts: 2, readTimeout: time.Second, readRetryDelay: time.Millisecond}
			release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{Namespace: "control"}}
			transition := declarativerelease.EdgeGroupABTransition{GroupID: "edge-group-test", ActivationStatePath: "/state/activation.json"}
			state, exists, err := cluster.readEdgeActivationStateFromPod(context.Background(), release, transition, edgeGroupPod{Name: "front"}, "edge-front")
			if (err != nil) != test.wantError || exists != test.present {
				t.Fatalf("exists=%v err=%v", exists, err)
			}
			if exists && (state.GroupID != transition.GroupID || state.Generation != 1 || state.ActiveSlot != "a") {
				t.Fatalf("read changed activation evidence: %+v", state)
			}
			count, readErr := os.ReadFile(counter)
			if readErr != nil || strings.TrimSpace(string(count)) != test.attempts {
				t.Fatalf("attempts=%q err=%v", count, readErr)
			}
		})
	}
}
