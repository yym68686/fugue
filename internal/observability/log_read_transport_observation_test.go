package observability

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
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
	if !seen || fact.Attempts != 1 || !fact.ConnectionReady || !fact.RequestWritten || !fact.FirstResponseByte || !fact.StreamOpened || fact.BodyBytes != 12 || fact.BodyReadError {
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
	if !errors.Is(err, io.ErrUnexpectedEOF) || fact.Phase != "rotation_current" || fact.BodyBytes != 5 || !fact.BodyReadError {
		t.Fatalf("failure attributed to the wrong outstanding stream: %+v %v", fact, err)
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
