package api

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestManagedHandoffUnreadableInputDoesNotPublishNegativeRoute(t *testing.T) {
	s, p, b, path := handoffPartitionFixture(t)
	ctx := context.Background()
	source, err := s.readRetainedTrafficSource(ctx, p.Intent.Scope)
	if err != nil {
		t.Fatal(err)
	}
	// The route, its authority, LKG and consumer receipts remain valid, but
	// reconstructing the complete compiler input fails (for example a timed
	// out read). A partial proof must neither authorize reuse nor silently
	// turn that failure into an unavailable replacement.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state model.State
	if err = json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	found := false
	for i := range state.PlatformArtifactContents {
		if state.PlatformArtifactContents[i].ContentHash == source.parent.Metadata["input_snapshot_digest"] {
			state.PlatformArtifactContents[i].Content = nil
			found = true
		}
	}
	if !found {
		t.Fatal("fixture has no persisted compiler input")
	}
	raw, _ = json.Marshal(state)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	partial, err := s.readRetainedTrafficSource(ctx, p.Intent.Scope)
	if err == nil || !managedHandoffSourcePositive(partial, platformconfig.ManagedServingHandoff{TenantID: "tenant-a", AppID: "app-a", ReleaseID: "release-a"}, model.DefaultManagedRuntimeID) {
		t.Fatal("unreadable complete input was confused with no signed positive source", err)
	}
	before, _ := json.Marshal(p)
	if err = s.captureManagedHandoffPartitions(ctx, &p, b); err == nil || !strings.Contains(err.Error(), "handoff evidence unavailable") {
		t.Fatal("failed continuity proof allowed negative publication", err)
	}
	after, _ := json.Marshal(p)
	if string(before) != string(after) {
		t.Fatal("partial proof changed authority, invented readiness or renewed evidence")
	}
	active, release, ok, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
	if err != nil || !ok || active.ID != source.parent.ID || release.ID != source.release.ID {
		t.Fatal("failed capture changed the serving artifact", err)
	}
}

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
