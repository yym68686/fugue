package controller

import (
	"testing"
	"time"
)

func waitForDeletedRefs(t *testing.T, refs <-chan string, want int) []string {
	t.Helper()

	got := make([]string, 0, want)
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()

	for len(got) < want {
		select {
		case ref := <-refs:
			got = append(got, ref)
		case <-timeout.C:
			t.Fatalf("timed out waiting for %d deleted refs, got %v", want, got)
		}
	}

	select {
	case ref := <-refs:
		t.Fatalf("unexpected extra deleted ref %q after %v", ref, got)
	case <-time.After(25 * time.Millisecond):
	}

	return got
}
