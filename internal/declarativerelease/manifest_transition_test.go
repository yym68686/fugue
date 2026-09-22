package declarativerelease

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

type rejectingManifestBinder struct {
	*fakeCluster
	calls        int
	forward, lkg []byte
}

func (b *rejectingManifestBinder) BindManifestTransition(_ PlanRelease, forward, lkg []byte) error {
	b.calls++
	b.forward = forward
	b.lkg = lkg
	return errors.New("invalid manifest transition")
}
func TestManifestTransitionRejectedBeforePrepareExecuteOrRecoveryWrites(t *testing.T) {
	plan, receipt, rendered, prewrite, forward := executionFixture(t)
	rejected := &rejectingManifestBinder{fakeCluster: &fakeCluster{}}
	if _, err := PrepareExecution(context.Background(), rejected, plan, "api", receipt, rendered, time.Now()); err == nil || rejected.calls != 1 || len(rejected.observedManifests) != 0 {
		t.Fatal("prepare consulted runtime before rejecting transition", err)
	}
	fake := &fakeCluster{observations: []Observation{prewrite, prewrite}, health: []Observation{prewrite}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"execute", "rollback", "repair"} {
		t.Run(action, func(t *testing.T) {
			b := &rejectingManifestBinder{fakeCluster: &fakeCluster{observations: []Observation{forward}, cas: []Observation{forward}, health: []Observation{forward}}}
			var result ExecutionResult
			switch action {
			case "execute":
				result = Execute(context.Background(), b, plan, prepared, rendered.Forward, rendered.LKG)
			case "rollback":
				result = RestoreMonitoredLKG(context.Background(), b, plan, prepared, rendered.Forward, rendered.LKG, plan.Releases[0])
			case "repair":
				result = RepairMonitoredForward(context.Background(), b, plan, prepared, rendered.Forward, rendered.LKG, plan.Releases[0])
			}
			if result.Reason != "manifest-transition-invalid" || b.calls != 1 || b.applies != 0 || len(b.observedManifests) != 0 || !bytes.Equal(b.forward, rendered.Forward) || !bytes.Equal(b.lkg, rendered.LKG) {
				t.Fatal("transition rejection failed", result, b.calls)
			}
		})
	}
}
