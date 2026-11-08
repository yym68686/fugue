package main

import (
	"errors"
	"testing"
	"time"
)

func TestFailureLoggingReportsChangesRecoveryAndBoundedSummaries(t *testing.T) {
	var reporter failureLog
	now := time.Now()
	err := errors.New("edge unavailable")
	if emit, _ := reporter.observe("app-a", err, now); !emit {
		t.Fatal("first failure hidden")
	}
	for i := 1; i <= 29; i++ {
		if emit, _ := reporter.observe("app-a", err, now.Add(time.Duration(i)*10*time.Second)); emit {
			t.Fatal("unchanged failure flooded logs")
		}
	}
	if emit, count := reporter.observe("app-a", err, now.Add(5*time.Minute)); !emit || count != 29 {
		t.Fatal("periodic evidence lost", emit, count)
	}
	if emit, _ := reporter.observe("app-a", errors.New("identity changed"), now.Add(301*time.Second)); !emit {
		t.Fatal("new failure hidden")
	}
	if emit, _ := reporter.observe("app-a", nil, now.Add(302*time.Second)); !emit {
		t.Fatal("recovery hidden")
	}
	if emit, _ := reporter.observe("app-a", err, now.Add(303*time.Second)); !emit {
		t.Fatal("recurrence hidden")
	}
	reporter.retain(map[string]bool{"app-b": true})
	if len(reporter.entries) != 0 {
		t.Fatal("removed intents retained forever")
	}
}
