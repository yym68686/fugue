package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPlatformMutationTraceDistinguishesValidationFromExpiredCommit(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, trace := startPlatformMutationTrace(ctx, "platform-lkg-verify")
	at := trace.started
	trace.now = func() time.Time { return at }
	trace.advance("read-artifact")
	at = at.Add(50 * time.Millisecond)
	trace.advance("verify-members")
	at = at.Add(11 * time.Second)
	trace.advance("commit")
	at = at.Add(time.Millisecond)
	err := trace.finish(sql.ErrTxDone)
	if !errors.Is(err, sql.ErrTxDone) {
		t.Fatal("observation changed underlying transaction error", err)
	}
	for _, part := range []string{"phase=commit", "deadline_exceeded=true", "read-artifact:50", "verify-members:11000", "commit:1", "elapsed_ms=11051"} {
		if !strings.Contains(err.Error(), part) {
			t.Fatal("missing exact failing-stage timing", part, err)
		}
	}
	if trace.finish(ErrConflict) != ErrConflict || trace.finish(ErrNotFound) != ErrNotFound {
		t.Fatal("observability changed ordinary sentinel identity")
	}
	if trace.finish(nil) != nil {
		t.Fatal("observation fabricated a successful-transaction error")
	}
}
