package api

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestLiveDNSIndependentMembersShareWindowWithoutSharingVerdicts(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	checks := []liveDNSBackendCheck{{consumer: "north"}, {consumer: "south"}, {consumer: "east"}, {consumer: "west"}}
	entered := make(chan string, len(checks))
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		checkLiveDNSBackends(ctx, checks, func(call context.Context, check liveDNSBackendCheck) int {
			if call != ctx {
				t.Error("reader did not inherit the shared observation window")
			}
			entered <- check.consumer
			select {
			case <-release:
			case <-call.Done():
				return http.StatusServiceUnavailable
			}
			if check.consumer == "south" {
				return http.StatusForbidden
			}
			return http.StatusOK
		})
	}()
	first := map[string]bool{}
	for range 2 {
		select {
		case who := <-entered:
			first[who] = true
		case <-ctx.Done():
			t.Fatal("one member starved the other's read")
		}
	}
	if !first["north"] || !first["south"] {
		t.Fatal("unexpected initial readers", first)
	}
	select {
	case <-entered:
		t.Fatal("started an unbounded third concurrent inspection")
	default:
	}
	close(release)
	<-done
	if len(entered) != 2 {
		t.Fatal("an observation was repeated or omitted")
	}
	for _, check := range checks {
		want := http.StatusOK
		if check.consumer == "south" {
			want = http.StatusForbidden
		}
		if check.code != want {
			t.Fatalf("wrong member verdict: %s got %d", check.consumer, check.code)
		}
	}
}

func TestLiveDNSExpiredWindowDoesNotPromoteLateOrUnobservedMember(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	checks := []liveDNSBackendCheck{{}, {}, {}}
	checkLiveDNSBackends(ctx, checks, func(context.Context, liveDNSBackendCheck) int {
		cancel()
		return http.StatusOK
	})
	for _, check := range checks {
		if check.code != http.StatusServiceUnavailable {
			t.Fatal("late or unobserved member received a pass")
		}
	}
	checkLiveDNSBackends(ctx, checks, func(context.Context, liveDNSBackendCheck) int {
		t.Fatal("started another observation after expiry")
		return http.StatusOK
	})
}
