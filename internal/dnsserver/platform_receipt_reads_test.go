package dnsserver

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestJoinedReceiptReadsRequireBothResults(t *testing.T) {
	denied := errors.New("changed authority")
	for _, failure := range []int{-1, 0, 1} {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		var entered = [2]chan struct{}{make(chan struct{}), make(chan struct{})}
		check := func(i int) func(context.Context) error {
			return func(ctx context.Context) error {
				close(entered[i])
				select {
				case <-entered[1-i]:
				case <-ctx.Done():
					return ctx.Err()
				}
				if i == failure {
					return denied
				}
				if failure != -1 {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
		}
		err := joinDNSReceiptReads(ctx, check(0), check(1))
		if ctx.Err() != nil || (failure < 0 && err != nil) || (failure >= 0 && !errors.Is(err, denied)) {
			t.Fatalf("failure=%d joined=%v budget=%v", failure, err, ctx.Err())
		}
		cancel()
	}
}
