package store

import (
	"errors"
	"testing"
)

func TestReleaseRejectionPreservesCauseAndSpecificStage(t *testing.T) {
	inner := platformReleaseRejected("routing_source_selection", ErrConflict)
	outer := platformReleaseRejected("capability_admission", inner)
	if !errors.Is(outer, ErrConflict) || outer.Error() != ErrConflict.Error() || PlatformReleaseFailureStage(outer) != "routing_source_selection" {
		t.Fatal("diagnostic wrapper changed semantics")
	}
	if platformReleaseRejected("phase", nil) != nil {
		t.Fatal("diagnostics invented failure")
	}
}
