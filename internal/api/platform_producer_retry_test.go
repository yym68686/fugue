package api

import (
	"testing"
	"time"
)

func TestPlatformProducerRetryDelayIsBoundedAndResets(t *testing.T) {
	for _, scope := range []string{"scope-a", "scope-b", ""} {
		if platformProducerRetryDelay(scope, 1) != 30*time.Second || platformProducerRetryDelay(scope, 0) != 30*time.Second {
			t.Fatal("first failure or new policy delayed beyond normal polling")
		}
		prior := time.Duration(0)
		for failures := 1; failures <= 100; failures++ {
			delay := platformProducerRetryDelay(scope, failures)
			if delay < prior || delay > 130*time.Second {
				t.Fatal("retry storm or unbounded failure delay", failures, delay)
			}
			prior = delay
		}
	}
	if platformProducerRetryDelay("scope-a", 3) == platformProducerRetryDelay("scope-b", 3) {
		t.Fatal("independent authorities retry in lockstep")
	}
}

func TestPlatformProducerNextIntervalSeparatesRefreshAndFailureBackoff(t *testing.T) {
	if got := platformProducerNextInterval(15*time.Minute, "global", 0); got != 15*time.Minute {
		t.Fatalf("successful reconcile should keep the normal policy interval, got %s", got)
	}
	if got := platformProducerNextInterval(15*time.Minute, "global", 1); got != 30*time.Second {
		t.Fatalf("first transient failure should retry promptly, got %s", got)
	}
	if got := platformProducerNextInterval(15*time.Minute, "global", 4); got > 130*time.Second {
		t.Fatalf("retries should remain bounded, got %s", got)
	}
	if got := platformProducerNextInterval(30*time.Second, "global", 4); got < 120*time.Second || got > 130*time.Second {
		t.Fatalf("a short policy interval should not prevent repeated failures from backing off, got %s", got)
	}
}
