package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLogReadTransportMilestonesPreserveStreamAndExistingTrace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-credential" {
			t.Error("request identity changed")
		}
		io.WriteString(w, "private-body")
	}))
	defer server.Close()
	timing := &logReadTransportTrace{}
	ctx := context.WithValue(t.Context(), logReadTransportKey{}, timing)
	var seen bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotFirstResponseByte: func() { seen = true }})
	body, err := observedLogStream(ctx, "pod_log", func(ctx context.Context) (io.ReadCloser, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/private-path?token=private-query", nil)
		req.Header.Set("Authorization", "Bearer private-credential")
		response, err := server.Client().Do(req)
		if err != nil {
			return nil, err
		}
		return response.Body, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(body)
	if err != nil || string(raw) != "private-body" {
		t.Fatal("stream changed", err)
	}
	body.Close()
	fact := timing.snapshot()
	if !seen || fact.Attempts != 1 || !fact.ConnectionReady || !fact.RequestWritten || !fact.FirstResponseByte || !fact.StreamOpened || fact.BodyBytes != 12 || fact.BodyReadError || fact.ChainStreams != 1 || fact.ChainBodyBytes != 12 {
		t.Fatalf("missing HTTP milestones: %+v", fact)
	}
	encoded, _ := json.Marshal(fact)
	if strings.Contains(string(encoded), "private") || strings.Contains(string(encoded), server.URL) {
		t.Fatal("trace retained request or body values")
	}
	fact.Phase = "mutated"
	if timing.snapshot().Phase != "pod_log" {
		t.Fatal("snapshot shares mutable receipt")
	}
}

func TestLogReadTransportSelectsFailingOlderBodyNotLaterInventory(t *testing.T) {
	timing := &logReadTransportTrace{}
	ctx := context.WithValue(t.Context(), logReadTransportKey{}, timing)
	body, err := observedLogStream(ctx, "rotation_current", func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(io.MultiReader(strings.NewReader("first"), logStreamReader(func([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }))), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	listing, err := observedLogStream(ctx, "rotation_inventory_after", func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("directory")), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(listing)
	listing.Close()
	_, err = io.ReadAll(body)
	body.Close()
	fact := timing.snapshot()
	if !errors.Is(err, io.ErrUnexpectedEOF) || fact.Phase != "rotation_current" || fact.BodyBytes != 5 || !fact.BodyReadError || fact.ChainStreams != 2 || fact.ChainBodyBytes != 14 {
		t.Fatalf("failure attributed to the wrong outstanding stream: %+v %v", fact, err)
	}
}

func TestLogReadTransportTimingsSeparateChainBudgetFromFailingStream(t *testing.T) {
	now := time.Now()
	advance := func(d time.Duration) { now = now.Add(d) }
	timing := &logReadTransportTrace{clock: func() time.Time { return now }}
	parent, cancel := context.WithDeadline(t.Context(), now.Add(time.Hour))
	defer cancel()
	ctx := context.WithValue(parent, logReadTransportKey{}, timing)
	body, err := observedLogStream(ctx, "pod_log", func(ctx context.Context) (io.ReadCloser, error) {
		trace := httptrace.ContextClientTrace(ctx)
		advance(10 * time.Millisecond)
		trace.GetConn("private-host")
		advance(20 * time.Millisecond)
		trace.GotConn(httptrace.GotConnInfo{Reused: true})
		advance(5 * time.Millisecond)
		trace.WroteRequest(httptrace.WroteRequestInfo{})
		advance(40 * time.Millisecond)
		trace.GotFirstResponseByte()
		advance(5 * time.Millisecond)
		return io.NopCloser(logStreamReader(func(p []byte) (int, error) {
			advance(25 * time.Millisecond)
			return copy(p, "abc"), io.EOF
		})), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(body)
	body.Close()
	if err != nil || string(raw) != "abc" {
		t.Fatal("observations changed the response", string(raw), err)
	}
	fact := timing.snapshot()
	if fact.StartOffsetMillis != 0 || fact.DeadlineMillis != 3600000 || fact.ConnectionMillis != 30 || fact.RequestMillis != 35 || fact.FirstByteMillis != 75 || fact.OpenMillis != 80 || fact.BodyReadMillis != 25 {
		t.Fatalf("stream milestones are not relative to open entry: %+v", fact)
	}
	// Local decoding/admission is not a body Read. Its cost remains visible as
	// elapsed chain time, not falsely attributed to network read latency.
	advance(200 * time.Millisecond)
	_, err = observedLogStream(ctx, "rotation_inventory_before", func(ctx context.Context) (io.ReadCloser, error) {
		trace := httptrace.ContextClientTrace(ctx)
		advance(7 * time.Millisecond)
		trace.GetConn("private-host")
		advance(8 * time.Millisecond)
		trace.GotConn(httptrace.GotConnInfo{Reused: true})
		advance(5 * time.Millisecond)
		trace.WroteRequest(httptrace.WroteRequestInfo{})
		advance(10 * time.Millisecond)
		return nil, context.DeadlineExceeded
	})
	fact = timing.snapshot()
	if !errors.Is(err, context.DeadlineExceeded) || fact.Phase != "rotation_inventory_before" || fact.StartOffsetMillis != 305 || fact.DeadlineMillis != 3599695 || fact.OpenMillis != 30 || fact.BodyReadMillis != 0 || fact.FirstResponseByte || fact.FirstByteMillis != 0 || fact.ChainStreams != 2 || fact.ChainOpenMillis != 110 || fact.ChainReadMillis != 25 || fact.ChainBodyBytes != 3 {
		t.Fatalf("failed stream borrowed prior timings or lost chain totals: %+v %v", fact, err)
	}
}

func TestLogReadTransportRetryResetsMilestonesAndPreservesSubmillisecondReads(t *testing.T) {
	now := time.Now()
	timing := &logReadTransportTrace{clock: func() time.Time { return now }}
	// Use a deadline-free context to test its explicit sentinel independently
	// of the test runner's own deadline.
	ctx := context.WithValue(context.Background(), logReadTransportKey{}, timing)
	reads := 0
	body, err := observedLogStream(ctx, "rotation_retained", func(ctx context.Context) (io.ReadCloser, error) {
		trace := httptrace.ContextClientTrace(ctx)
		trace.GetConn("private-host")
		now = now.Add(time.Millisecond)
		trace.GotConn(httptrace.GotConnInfo{})
		trace.WroteRequest(httptrace.WroteRequestInfo{})
		trace.GotFirstResponseByte()
		trace.GetConn("private-retry")
		return io.NopCloser(logStreamReader(func(p []byte) (int, error) {
			now = now.Add(600 * time.Microsecond)
			reads++
			if reads == 2 {
				return copy(p, "y"), io.ErrUnexpectedEOF
			}
			return copy(p, "x"), nil
		})), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(body)
	body.Close()
	fact := timing.snapshot()
	if string(raw) != "xy" || !errors.Is(err, io.ErrUnexpectedEOF) || fact.Attempts != 2 || fact.ConnectionReady || fact.RequestWritten || fact.FirstResponseByte || fact.ConnectionMillis != 0 || fact.RequestMillis != 0 || fact.FirstByteMillis != 0 || fact.DeadlineMillis != -1 || fact.BodyReadMillis != 1 || fact.ChainReadMillis != 1 || fact.ChainBodyBytes != 2 || !fact.BodyReadError {
		t.Fatalf("retry retained stale milestones or rounded every Read: %+v %v", fact, err)
	}
}

func TestLogReadTransportConcurrentCallbacksAndCancellation(t *testing.T) {
	timing := &logReadTransportTrace{}
	parent, cancel := context.WithCancel(t.Context())
	cancel()
	ctx := context.WithValue(parent, logReadTransportKey{}, timing)
	_, err := observedLogStream(ctx, "private-phase", func(ctx context.Context) (io.ReadCloser, error) {
		if ctx.Err() != context.Canceled {
			t.Fatal("cancellation changed")
		}
		trace := httptrace.ContextClientTrace(ctx)
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					trace.GetConn("private-host")
					trace.GotConn(httptrace.GotConnInfo{Reused: true})
					trace.WroteRequest(httptrace.WroteRequestInfo{})
					trace.GotFirstResponseByte()
					timing.snapshot()
				}
			}()
		}
		wg.Wait()
		return nil, ctx.Err()
	})
	fact := timing.snapshot()
	if !errors.Is(err, context.Canceled) || fact.Phase != "other" || fact.Attempts != 400 || fact.StreamOpened {
		t.Fatalf("unbounded or incorrect trace: %+v %v", fact, err)
	}
}

func TestLogReadTransportRecoveryDoesNotKeepEarlierOpenFailure(t *testing.T) {
	timing := &logReadTransportTrace{}
	ctx := context.WithValue(t.Context(), logReadTransportKey{}, timing)
	_, err := observedLogStream(ctx, "rotation_retained", func(context.Context) (io.ReadCloser, error) {
		return nil, io.ErrUnexpectedEOF
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	body, err := observedLogStream(ctx, "rotation_retained", func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("recovered")), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(body)
	body.Close()
	if fact := timing.snapshot(); !fact.StreamOpened || fact.BodyBytes != 9 || fact.BodyReadError {
		t.Fatalf("retained an already recovered failure: %+v", fact)
	}
}

func TestSourceObservationRetainsFailureTransportMetadataAndClonesIt(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	fact := &logReadTransportObservation{Phase: "rotation_inventory_after", RequestWritten: true}
	p.observeLogSource(logSourceObservation{Identity: "source", Outcome: "rotation_recovery_error", LastErrorRead: fact})
	fact.Phase = "changed"
	p.observeLogSource(logSourceObservation{Identity: "source", Outcome: "drained"})
	value, _ := p.DiagnosticSources(t.Context())
	row := value.(map[string]any)["sources"].([]logSourceObservation)[0]
	if row.LastErrorRead == nil || row.LastErrorRead.Phase != "rotation_inventory_after" {
		t.Fatal("recovery erased last failure trace")
	}
	row.LastErrorRead.Phase = "changed"
	if p.sourceObservations["source"].LastErrorRead.Phase != "rotation_inventory_after" {
		t.Fatal("projection shares mutable evidence")
	}
	p.observeLogSource(logSourceObservation{Identity: "source", Outcome: "open_error"})
	if p.sourceObservations["source"].LastErrorRead != nil {
		t.Fatal("new untraced error borrowed an unrelated old receipt")
	}
}

func TestLogReadTransportTimingsFitSourceSnapshotResponseBudget(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	now := time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	fact := &logReadTransportObservation{Phase: "rotation_inventory_before", Attempts: 65535,
		ConnectionReady: true, ConnectionReused: true, RequestWritten: true, FirstResponseByte: true,
		StreamOpened: true, OpenMillis: 5000, BodyBytes: 64 << 20, BodyReadError: true,
		StartOffsetMillis: 15000, DeadlineMillis: 5000, ConnectionMillis: 5000,
		RequestMillis: 5000, FirstByteMillis: 5000, BodyReadMillis: 5000,
		ChainStreams: 65535, ChainOpenMillis: 15000, ChainBodyBytes: 64 << 20, ChainReadMillis: 15000}
	for i := range 512 {
		p.observeLogSource(logSourceObservation{
			Identity: fmt.Sprintf("namespace/pod-%04d/container/pod-uid/containerd://%s", i, strings.Repeat("a", 64)),
			Node:     "node-a", Previous: true, ObservedAt: now, CursorBefore: now, CursorAfter: now,
			PendingAt: now, NextAttempt: now, DrainedThrough: now, OpenMillis: 5000, TotalMillis: 15000,
			Outcome: "rotation_recovery_error", ErrorClass: "deadline", LastErrorRead: fact,
			RotationFiles: 5, RotationBytes: 64 << 20, RecoveryUnproven: true,
			GapEvents: 1, GapReason: "runtime_log_unavailable", GapLowerBound: now, GapUpperBound: now,
		})
	}
	value, err := p.DiagnosticSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 1<<20 {
		t.Fatal("fixed-size timings exceeded the owner snapshot response budget", len(raw), err)
	}
	if len(value.(map[string]any)["sources"].([]logSourceObservation)) != 512 {
		t.Fatal("response budget was met by dropping source rows")
	}
	t.Logf("512 timed sources and 64 gap receipts: %d bytes", len(raw))
}
