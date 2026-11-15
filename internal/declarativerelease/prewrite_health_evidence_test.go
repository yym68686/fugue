package declarativerelease

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPrepareRejectionRetainsPredecessorHealthEvidenceWithoutWrites(t *testing.T) {
	for _, stage := range []string{"matched_predecessor", "observe_failure", "observe_degraded"} {
		t.Run(stage, func(t *testing.T) {
			plan, receipt, rendered, lkg, _ := executionFixture(t)
			cause := errors.New("bounded health evidence: phase=status http_status=503 pod_uid=worker-one")
			healthErr := fmt.Errorf("%w: %w", ErrDegradedPredecessorHealth, cause)
			fake := &fakeCluster{observations: []Observation{lkg}, healthErrors: []error{healthErr}}
			var forwardErr error
			switch stage {
			case "observe_failure":
				fake.observationErrors = []error{errors.New("transient observation failure")}
				forwardErr = errors.New("forward health unavailable")
				fake.healthErrors = append(fake.healthErrors, forwardErr)
			case "observe_degraded":
				fake.observationErrors = []error{healthErr}
				forwardErr = errors.New("forward health unavailable")
				fake.healthErrors = []error{forwardErr}
			}
			_, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
			if !errors.Is(err, cause) || !errors.Is(err, ErrDegradedPredecessorHealth) ||
				!strings.Contains(err.Error(), "requires an explicit same-LKG retry or failed-release successor") {
				t.Fatalf("health evidence or recovery refusal lost: %v", err)
			}
			if forwardErr != nil && !errors.Is(err, forwardErr) {
				t.Fatalf("final health failure lost: %v", err)
			}
			if len(fake.verifiedTargets) != 0 || len(fake.casManifests) != 0 || fake.dryRuns != 0 || fake.applies != 0 || fake.deleteCreated != 0 || len(fake.converged) != 0 {
				t.Fatalf("unauthorized recovery reached artifact/CAS/dry-run/write: %+v", fake)
			}
		})
	}
}
