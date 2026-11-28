package managedappstatus

import (
	"errors"
	"testing"
	"time"
)

func TestListFailureOnlyBacksOffListRefreshes(t *testing.T) {
	var refresh Refresh[string, int]
	unavailable := errors.New("dependency unavailable")
	if got, err := refresh.List(func() (int, error) { return 7, unavailable }); got != 0 || !errors.Is(err, unavailable) {
		t.Fatalf("failed list returned %d, %v", got, err)
	}
	if refresh.ListAllowed(time.Now()) {
		t.Fatal("failed list did not set the retry window")
	}
	var calls int
	fetch := func() (int, error) {
		calls++
		return 42, nil
	}
	if _, err := refresh.List(fetch); !errors.Is(err, ErrRefreshBackoff) || calls != 0 {
		t.Fatalf("backed-off list called fetch: calls=%d, error=%v", calls, err)
	}
	if got, err := refresh.Inventory(fetch); got != 42 || err != nil {
		t.Fatalf("inventory was affected by list backoff: %d, %v", got, err)
	}
	if got, err := refresh.App("one", func() (string, error) { return "app", nil }); got != "app" || err != nil {
		t.Fatalf("app was affected by list backoff: %q, %v", got, err)
	}
	refresh.ResetBackoff()
	if got, err := refresh.List(fetch); got != 42 || err != nil {
		t.Fatalf("list did not resume after invalidation: %d, %v", got, err)
	}
}
