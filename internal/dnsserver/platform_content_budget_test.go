package dnsserver

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDNSContentBudgetDefersShadowAndCancelsServingWait(t *testing.T) {
	s := &Service{}
	shadow := &Service{shadowValidationParent: s, sharedContentBudget: s.contentBudget()}
	renewal := &Service{sharedContentBudget: s.contentBudget()}
	release, err := s.acquireDNSContent(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.acquireDNSContent(t.Context()); !errors.Is(err, errDNSContentBusy) {
		t.Fatal("shadow queued heavy work behind serving", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := renewal.acquireDNSContent(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("renewal wait ignored its deadline", err)
	}
	release()
	release, err = renewal.acquireDNSContent(t.Context())
	if err != nil {
		t.Fatal("permit leaked after cancellation", err)
	}
	release()
}
