package main

import (
	"context"
	"errors"
	"reflect"
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
		attempts  int
		present   bool
	}{
		{"transient EOF", 1, `{"schema":"` + edgeActivationStateSchema + `","edge_group_id":"edge-group-test","generation":1,"authority":"` + edgeActivationAuthority + `","active_slot":"a"}`, false, 2, true},
		{"absent file after EOF", 1, "absent", false, 2, false},
		{"persistent EOF", 3, "absent", true, 2, false},
		{"invalid evidence", 0, `{}`, true, 1, false},
		{"wrong group", 0, `{"schema":"` + edgeActivationStateSchema + `","edge_group_id":"edge-group-other","generation":1,"authority":"` + edgeActivationAuthority + `","active_slot":"a"}`, true, 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Count actual read invocations, not shell starts that can be killed
			// before writing their counter on a busy builder. Real process timeout
			// and read/write separation retain their own integration coverage.
			attempts := 0
			var readContexts []context.Context
			cluster := &kubectlCluster{kubectl: "kubectl", readAttempts: 2, readTimeout: 5 * time.Second, readRetryDelay: time.Millisecond}
			cluster.readCommand = func(ctx context.Context, input []byte, binary string, arguments ...string) ([]byte, error) {
				attempts++
				readContexts = append(readContexts, ctx)
				deadline, bounded := ctx.Deadline()
				if !bounded || time.Until(deadline) > 5*time.Second || ctx.Err() != nil {
					t.Fatal("read attempt lost its unchanged deadline")
				}
				want := []string{"exec", "--namespace", "control", "front", "--container", "edge-front", "--", "sh", "-ceu", `if [ ! -e "$1" ]; then printf 'absent\n'; exit 0; fi; cat "$1"`, "sh", "/state/activation.json"}
				if binary != "kubectl" || len(input) != 0 || !reflect.DeepEqual(arguments, want) {
					t.Fatalf("activation read command changed: %s %q", binary, arguments)
				}
				if attempts <= test.failures {
					return nil, errors.New("error dialing backend: EOF")
				}
				return []byte(test.response + "\n"), nil
			}
			release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{Namespace: "control"}}
			transition := declarativerelease.EdgeGroupABTransition{GroupID: "edge-group-test", ActivationStatePath: "/state/activation.json"}
			state, exists, err := cluster.readEdgeActivationStateFromPod(context.Background(), release, transition, edgeGroupPod{Name: "front"}, "edge-front")
			if (err != nil) != test.wantError || exists != test.present {
				t.Fatalf("exists=%v err=%v", exists, err)
			}
			if exists && (state.GroupID != transition.GroupID || state.Generation != 1 || state.ActiveSlot != "a") {
				t.Fatalf("read changed activation evidence: %+v", state)
			}
			if attempts != test.attempts {
				t.Fatalf("attempts=%d want=%d", attempts, test.attempts)
			}
			for _, readCtx := range readContexts {
				if !errors.Is(readCtx.Err(), context.Canceled) {
					t.Fatal("completed read attempt context was not canceled")
				}
			}
		})
	}
}
