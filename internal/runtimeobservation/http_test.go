package runtimeobservation

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

type testTransport func(*http.Request) (*http.Response, error)

func (f testTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func BenchmarkObservedHTTPNoBody(b *testing.B) {
	base := testTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	})
	var recorder HTTPRecorder
	observed := recorder.Wrap(base, func(r *http.Request) HTTPRequestClass {
		return HTTPRequestClass{Client: "fixture", Method: r.Method, Resource: "objects"}
	})
	for _, test := range []struct {
		name string
		rt   http.RoundTripper
	}{{"base", base}, {"observed", observed}} {
		b.Run(test.name, func(b *testing.B) {
			req, _ := http.NewRequest("GET", "http://unused.invalid", nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := test.rt.RoundTrip(req)
				if err != nil {
					b.Fatal(err)
				}
				resp.Body.Close()
			}
		})
	}
}

func TestHTTPObservationPreservesBodyAndCountsEOFOnlyOnce(t *testing.T) {
	var recorder HTTPRecorder
	rt := recorder.Wrap(testTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("payload"))}, nil
	}), func(r *http.Request) HTTPRequestClass {
		return HTTPRequestClass{Client: "fixture", Method: r.Method, Resource: "objects"}
	})
	req, _ := http.NewRequest("GET", "https://unused.invalid/private?token=private", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil || string(data) != "payload" {
		t.Fatal("body changed", err)
	}
	resp.Body.Close()
	resp.Body.Close()
	v, _ := recorder.Snapshot(context.Background())
	result := v.(map[string]any)
	rows := result["requests"].([]HTTPRequestFact)
	if result["completed_total"] != uint64(1) || result["in_flight"] != int64(0) || len(rows) != 1 || rows[0].BytesRead != 7 || !rows[0].CompletedBody || rows[0].ReadError {
		t.Fatalf("incorrect body observation: %+v", result)
	}
}

func TestHTTPObservationRetainsErrorsEarlyCloseAndBoundedConcurrentHistory(t *testing.T) {
	var recorder HTTPRecorder
	rt := recorder.Wrap(testTransport(func(r *http.Request) (*http.Response, error) {
		if r.Context().Err() != nil {
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("unconsumed"))}, nil
	}), func(r *http.Request) HTTPRequestClass {
		return HTTPRequestClass{Client: "fixture", Method: r.Method, Resource: "objects"}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://unused.invalid", nil)
	if _, err := rt.RoundTrip(req); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation changed", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 600; j++ {
				req, _ := http.NewRequest("GET", "http://unused.invalid", nil)
				resp, err := rt.RoundTrip(req)
				if err != nil {
					t.Error(err)
					return
				}
				resp.Body.Close()
				recorder.Snapshot(context.Background())
			}
		}()
	}
	wg.Wait()
	v, _ := recorder.Snapshot(context.Background())
	result := v.(map[string]any)
	rows := result["requests"].([]HTTPRequestFact)
	if result["completed_total"] != uint64(2401) || result["in_flight"] != int64(0) || result["overwritten"] != true || len(rows) != 2048 {
		t.Fatalf("unbounded or lost observations: %v %v %d", result["completed_total"], result["in_flight"], len(rows))
	}
	for _, row := range rows {
		if row.CompletedBody || row.BytesRead != 0 {
			t.Fatal("early close treated as body consumption")
		}
	}
}
