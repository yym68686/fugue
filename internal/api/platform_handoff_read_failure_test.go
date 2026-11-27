package api

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestManagedHandoffReadFailureBoundariesAndRetry(t *testing.T) {
	s, base, b, _ := handoffPartitionFixture(t)
	ctx := context.Background()
	source, err := s.readRetainedTrafficSource(ctx, base.Intent.Scope)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(base)
	for _, scenario := range []string{"timeout", "no signed source", "negative source", "disabled", "explicit upstream", "other origin failure", "ready", "completed operation"} {
		t.Run(scenario, func(t *testing.T) {
			var p platformIntentProjectionResponse
			_ = json.Unmarshal(raw, &p)
			business := b
			business.Operations = append(business.Operations[:0:0], b.Operations...)
			old := source
			switch scenario {
			case "no signed source":
				old = retainedTrafficSource{}
			case "negative source":
				old.routes = nil
			case "disabled":
				p.Intent.Routes[0].Enabled = false
			case "explicit upstream":
				p.Intent.Routes[0].UpstreamURL = "http://changed:8080"
			case "other origin failure":
				p.RuntimeSnapshot.Origins[0].StatusReason = "service identity differs"
			case "ready":
				p.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusActive
			case "completed operation":
				business.Operations[0].Status = model.OperationStatusCompleted
			}
			readFailure := func() (retainedTrafficSource, error) { return old, context.DeadlineExceeded }
			err := s.captureManagedHandoffPartitionsWithSource(ctx, &p, business, readFailure)
			if scenario == "timeout" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("read failure was swallowed", err)
				}
				if len(p.RuntimeSnapshot.RetainedTrafficPartitions) != 0 {
					t.Fatal("failed proof authorized route reuse")
				}
				// A fresh successful retry retains the exact old path, allows the
				// independent peer/DNS changes, and keeps the original expiry.
				if err = s.captureManagedHandoffPartitions(ctx, &p, business); err != nil {
					t.Fatal(err)
				}
				if len(p.RuntimeSnapshot.RetainedTrafficPartitions) != 1 {
					t.Fatal("successful retry did not recover continuity")
				}
				part := p.RuntimeSnapshot.RetainedTrafficPartitions[0]
				if !part.ExpiresAt.Equal(source.expiry("tenant-a", "app-a")) || !reflect.DeepEqual(part.Routes, source.routes["tenant-a\x00app-a"]) {
					t.Fatal("retry rewrote or renewed old evidence")
				}
				if _, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: p.Intent, Policy: p.Policy, RuntimeSnapshot: p.RuntimeSnapshot}); err != nil {
					t.Fatal("peer/DNS changes did not compile after proof recovered", err)
				}
			} else if err != nil {
				t.Fatal("explicit recovery or nonhandoff input blocked", err)
			}
		})
	}
}

func TestRetainedSourceMemoizationDoesNotBypassPublicationAdmission(t *testing.T) {
	s, p, b, _ := handoffPartitionFixture(t)
	ctx := context.Background()
	readSource := s.newRetainedTrafficSourceReader(ctx, p.Intent.Scope)
	source, err := readSource()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.captureManagedHandoffPartitionsWithSource(ctx, &p, b, readSource); err != nil {
		t.Fatal(err)
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: p.Intent, Policy: p.Policy, RuntimeSnapshot: p.RuntimeSnapshot})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.store.ValidatePlatformArtifact(source.route.ID, []model.PlatformArtifactValidationResult{{Name: "revoked", Pass: false}}); err != nil {
		t.Fatal(err)
	}
	// The capture-local result is immutable; it is not a global cached trust
	// decision and cannot authorize publication after the source is revoked.
	if again, err := readSource(); err != nil || !reflect.DeepEqual(again, source) {
		t.Fatal("one capture did not share its complete proof", err)
	}
	if err = s.validateRetainedTrafficCompilation(ctx, compiled); err == nil {
		t.Fatal("capture-local memoization bypassed publication revalidation")
	}
}
