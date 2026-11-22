package main

import (
	"errors"
	"fmt"
	"testing"
)

func TestOnlyMixedDurablePrepareRoutesToReadOnlyMemberObservation(t *testing.T) {
	mixed := fmt.Errorf("read active slot: %w", errEdgeMixedActivation)
	for _, tc := range []struct {
		args []string
		err  error
		code int
	}{
		{[]string{"prepare", "p", "c", "a", "d"}, mixed, 78},
		{[]string{"prepare", "p", "c", "a", "d"}, errors.New(errEdgeMixedActivation.Error()), 1},
		{[]string{"prepare", "p"}, mixed, 1},
		{[]string{"execute", "p", "c", "a", "d"}, mixed, 1},
		{[]string{"guardian-submit", "d"}, mixed, 1},
	} {
		if got := releaseFailureExitCode(tc.args, tc.err); got != tc.code {
			t.Fatalf("%v / %v: exit %d, want %d", tc.args, tc.err, got, tc.code)
		}
	}
}
