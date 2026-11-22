package dnsserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/platformconsumer"
)

func TestDNSRenewalRetiresOnlySupersededPhysicalRead(t *testing.T) {
	var pointer dnsServingPointer
	a, b := new(dnsServingState), new(dnsServingState)
	pointer.publish(a, dnsServingWriterCheckpoint)
	old := pointer.publication.Load()
	parent, end := context.WithTimeout(context.Background(), 3*time.Second)
	defer end()
	ctx, stop := old.renewalContext(parent)
	defer stop()
	entered, left := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(left)
	}))
	defer srv.Close()
	done := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(ctx, "GET", srv.URL, nil)
		if err == nil {
			var resp *http.Response
			resp, err = srv.Client().Do(req)
			if resp != nil {
				resp.Body.Close()
			}
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-parent.Done():
		t.Fatal("physical read did not start")
	}
	pointer.publish(b, dnsServingWriterCandidate)
	pointer.publish(a, dnsServingWriterRollback)
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(context.Cause(ctx), platformconsumer.ErrAssignmentChanged) ||
		!dnsProbeSweepInterrupted(ctx, err, time.Now().Add(time.Minute)) {
		t.Fatalf("discarded installation did not stop its read: %v, %v", err, context.Cause(ctx))
	}
	select {
	case <-left:
	case <-parent.Done():
		t.Fatal("server kept a canceled read")
	}
	late, releaseLate := old.renewalContext(parent)
	defer releaseLate()
	current, finish := pointer.publication.Load().renewalContext(parent)
	defer finish()
	if late.Err() == nil || current.Err() != nil || pointer.Load() != a || parent.Err() != nil {
		t.Fatal("rollback reused an old installation or canceled the current one")
	}
}

func TestDNSRenewalLocalCompletionDoesNotRetirePublication(t *testing.T) {
	var pointer dnsServingPointer
	pointer.publish(new(dnsServingState), dnsServingWriterCandidate)
	p := pointer.publication.Load()
	parent, end := context.WithCancel(context.Background())
	first, stop := p.renewalContext(parent)
	next, finish := p.renewalContext(parent)
	defer finish()
	stop()
	if first.Err() == nil || next.Err() != nil || p.lifetime.Err() != nil {
		t.Fatal("completed request retired a different observer")
	}
	end()
	if !errors.Is(context.Cause(next), context.Canceled) || errors.Is(context.Cause(next), platformconsumer.ErrAssignmentChanged) {
		t.Fatal("parent cancellation mislabeled as a publication change")
	}
}
