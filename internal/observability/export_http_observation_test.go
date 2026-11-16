package observability

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type exportHTTPTestTransport func(*http.Request) (*http.Response, error)

func (f exportHTTPTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type exportHTTPTestBody struct{ reads, closes int }

func (b *exportHTTPTestBody) Read([]byte) (int, error) { b.reads++; return 0, io.EOF }
func (b *exportHTTPTestBody) Close() error             { b.closes++; return nil }

func TestExportHTTPObservationPreservesRequestResponseAndExistingTrace(t *testing.T) {
	var o exportObservations
	var existing atomic.Int32
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotFirstResponseByte: func() { existing.Add(1) }})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://private.invalid/private?secret=private", strings.NewReader("private payload"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "private credential")
	body := &exportHTTPTestBody{}
	response := &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: body}
	var late *httptrace.ClientTrace
	client := &http.Client{Transport: exportHTTPTestTransport(func(got *http.Request) (*http.Response, error) {
		deadline, ok := got.Context().Deadline()
		originalDeadline, _ := ctx.Deadline()
		if !ok || !deadline.Equal(originalDeadline) || got.Body != req.Body || got.URL != req.URL || got.Header.Get("Authorization") != "private credential" || got.GetBody == nil {
			t.Fatal("HTTP observation changed request or deadline")
		}
		trace := httptrace.ContextClientTrace(got.Context())
		late = trace
		trace.GetConn("private.invalid")
		trace.DNSStart(httptrace.DNSStartInfo{Host: "private.invalid"})
		trace.DNSDone(httptrace.DNSDoneInfo{Err: errors.New("private DNS error")})
		trace.ConnectStart("private network", "private address")
		trace.ConnectDone("private network", "private address", errors.New("private connect error"))
		trace.TLSHandshakeStart()
		trace.TLSHandshakeDone(tls.ConnectionState{ServerName: "private.invalid"}, errors.New("private TLS error"))
		trace.GotConn(httptrace.GotConnInfo{Reused: true})
		trace.WroteRequest(httptrace.WroteRequestInfo{})
		trace.GotFirstResponseByte()
		return response, nil
	})}
	got, err := observedExportHTTP(client, req, &o, exportHTTPLogs)
	if err != nil || got != response || got.Body != body || body.reads != 0 || body.closes != 0 || existing.Load() != 1 {
		t.Fatal("instrumentation changed response, body lifecycle or existing trace", err, body, existing.Load())
	}
	if req.Context() != ctx || httptrace.ContextClientTrace(ctx).GetConn != nil {
		t.Fatal("instrumentation mutated caller context/trace")
	}
	row := o.httpSnapshot()[exportHTTPLogs]
	if row.Started != 1 || row.Completed != 1 || row.Success != 1 || row.Failed != 0 || row.Inflight != 0 || row.TotalNanos != row.MaxNanos || row.LatestFailed != nil {
		t.Fatal(row)
	}
	r := row.Latest
	if r == nil || r.StatusCode != 204 || r.Outcome != "http_success" || r.ConnectionAttempts != 1 || r.Connections != 1 || r.ReusedConnections != 1 || r.WriteError || len(r.Milestones) != len(exportHTTPMilestoneNames) || r.DurationNS <= 0 {
		t.Fatal(r)
	}
	for i, milestone := range r.Milestones {
		if milestone.Name != exportHTTPMilestoneNames[i] || milestone.AfterStartNS < 0 || milestone.AfterStartNS > r.DurationNS {
			t.Fatal("invalid milestone", milestone)
		}
	}
	before, _ := json.Marshal(o.httpSnapshot())
	// Late callbacks may race with observation snapshots, but cannot rewrite
	// the receipt for an already returned Do call.
	late.WroteRequest(httptrace.WroteRequestInfo{Err: errors.New("private late error")})
	late.GetConn("private late host")
	after, _ := json.Marshal(o.httpSnapshot())
	if string(before) != string(after) || strings.Contains(string(after), "private") {
		t.Fatal("late callbacks rewrote a receipt or private material leaked")
	}
	row.Latest.Milestones[0].Name = "private replacement"
	row.Latest.Outcome = "private replacement"
	own, _ := json.Marshal(o.httpSnapshot())
	if string(own) != string(after) {
		t.Fatal("snapshot mutation changed recorder")
	}
	_ = got.Body.Close()
	if body.closes != 1 {
		t.Fatal("body close changed")
	}
}

func TestExportHTTPObservationFailureRetentionNilAndMissingMilestones(t *testing.T) {
	var o exportObservations
	privateErr := errors.New("private transport error")
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://private.invalid/", nil)
	var calls int
	client := &http.Client{Transport: exportHTTPTestTransport(func(got *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			trace := httptrace.ContextClientTrace(got.Context())
			trace.GetConn("private")
			trace.WroteRequest(httptrace.WroteRequestInfo{Err: privateErr})
			return nil, privateErr
		}
		code := 503
		if calls > 2 {
			code = 204
		}
		return &http.Response{StatusCode: code, Body: http.NoBody, Header: make(http.Header)}, nil
	})}
	if _, err := observedExportHTTP(client, req, &o, -1); !errors.Is(err, privateErr) {
		t.Fatal("transport error changed", err)
	}
	row := o.httpSnapshot()[exportHTTPOther]
	if row.Latest.Outcome != "transport_error" || row.Latest.StatusCode != 0 || !row.Latest.WriteError || len(row.Latest.Milestones) != 2 {
		t.Fatal("failure milestones fabricated", row)
	}
	response, err := observedExportHTTP(client, req, &o, exportHTTPStageCount)
	if err != nil || response.StatusCode != 503 {
		t.Fatal("HTTP error response changed", response, err)
	}
	row = o.httpSnapshot()[exportHTTPOther]
	failed, _ := json.Marshal(row.LatestFailed)
	if row.Failed != 2 || row.Latest.Outcome != "http_error" || row.Latest.Milestones != nil {
		t.Fatal(row)
	}
	response, err = observedExportHTTP(client, req, &o, exportHTTPOther)
	if err != nil || response.StatusCode != 204 {
		t.Fatal(response, err)
	}
	row = o.httpSnapshot()[exportHTTPOther]
	retained, _ := json.Marshal(row.LatestFailed)
	if row.Success != 1 || row.Completed != 3 || string(failed) != string(retained) {
		t.Fatal("success erased failure evidence", row)
	}
	client.Transport = exportHTTPTestTransport(func(got *http.Request) (*http.Response, error) {
		if got != req {
			t.Fatal("nil observation changed request identity")
		}
		return response, nil
	})
	if got, err := observedExportHTTP(client, req, nil, exportHTTPLogs); got != response || err != nil {
		t.Fatal(got, err)
	}
}

func TestExportHTTPObservationConfiguredExportersAndQueriesAreSeparate(t *testing.T) {
	var o exportObservations
	var calls int
	client := &http.Client{Transport: exportHTTPTestTransport(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != http.MethodPost {
			t.Fatal("method changed")
		}
		if req.Body != nil {
			_, _ = io.Copy(io.Discard, req.Body)
			_ = req.Body.Close()
		}
		return &http.Response{StatusCode: 204, Body: http.NoBody, Header: make(http.Header)}, nil
	})}
	cfg := Config{MetricsRemoteWriteURL: "https://private.invalid/metrics", LokiURL: "https://private.invalid/logs", ClickHouseDSN: "https://private.invalid/?database=private"}
	e := newConfiguredObservedExporter(cfg, client, &o)
	events := []Event{prometheusMetricEvent(time.Now().UTC(), "1"), {Kind: EventKindLog, Attributes: map[string]string{"event_type": "app_event"}}, {Kind: EventKindSpan}}
	if err := e.Export(t.Context(), events); err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{exportHTTPMetrics, exportHTTPLogs, exportHTTPAppEvents, exportHTTPRequestSpans} {
		if row := o.httpSnapshot()[index]; row.Completed != 1 || row.Success != 1 {
			t.Fatal("configured exporter not observed", row)
		}
	}
	if calls != 4 {
		t.Fatal("request count changed", calls)
	}
	ch := NewClickHouseExporter(cfg.ClickHouseDSN, client)
	ch.observations = &o
	before, _ := json.Marshal(o.httpSnapshot())
	if _, err := ch.QueryJSONEachRow(t.Context(), "SELECT 1 FORMAT JSONEachRow", 1024); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(o.httpSnapshot())
	if calls != 5 || string(before) != string(after) {
		t.Fatal("query contaminated export observation")
	}
	for i, table := range []string{"request_facts", "request_spans", "app_events", "edge_route_decisions", "edge_route_decision_missing_links", "private custom table"} {
		if got := exportHTTPTableIndex(table); got != exportHTTPRequestFacts+i {
			t.Fatal("wrong fixed table purpose", got)
		}
	}
	p := NewPipeline(cfg, nil)
	for _, exporter := range p.exporter.(MultiExporter).exporters {
		switch v := exporter.(type) {
		case *PrometheusRemoteWriteExporter:
			if v.observations != &p.exportObservations {
				t.Fatal("missing pipeline recorder")
			}
		case LokiExporter:
			if v.observations != &p.exportObservations {
				t.Fatal("missing pipeline recorder")
			}
		case ClickHouseExporter:
			if v.observations != &p.exportObservations {
				t.Fatal("missing pipeline recorder")
			}
		default:
			t.Fatal("unexpected exporter")
		}
	}
}

func TestExportHTTPObservationConcurrentCallbacksInflightAndFixedBound(t *testing.T) {
	var o exportObservations
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	client := &http.Client{Transport: exportHTTPTestTransport(func(req *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(req.Context())
		close(entered)
		<-release
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					trace.GetConn("private")
					trace.GotConn(httptrace.GotConnInfo{Reused: true})
					trace.WroteRequest(httptrace.WroteRequestInfo{})
					trace.GotFirstResponseByte()
					_ = o.httpSnapshot()
				}
			}()
		}
		wg.Wait()
		return &http.Response{StatusCode: 204, Body: http.NoBody, Header: make(http.Header)}, nil
	})}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://private.invalid/", nil)
	go func() { _, err := observedExportHTTP(client, req, &o, exportHTTPLogs); done <- err }()
	<-entered
	row := o.httpSnapshot()[exportHTTPLogs]
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if row.Inflight != 1 || row.Completed != 0 || row.Latest != nil {
		t.Fatal("inflight request hidden", row)
	}
	row = o.httpSnapshot()[exportHTTPLogs]
	if row.Completed != 1 || row.Inflight != 0 || row.Latest.Connections != 800 || row.Latest.ReusedConnections != 800 || len(row.Latest.Milestones) != 4 {
		t.Fatal(row)
	}
	p := NewPipeline(Config{}, nil)
	// This maximum includes every stage populated with its largest receipt.
	maximum := *row.Latest
	maximum.Milestones = nil
	for _, name := range exportHTTPMilestoneNames {
		maximum.Milestones = append(maximum.Milestones, exportHTTPMilestone{Name: name, AfterStartNS: 1<<63 - 1})
	}
	maximum.DurationNS = 1<<63 - 1
	maximum.ConnectionAttempts, maximum.Connections, maximum.ReusedConnections = 65535, 65535, 65535
	for i := range p.exportObservations.http {
		p.exportObservations.http[i].latest = &maximum
		p.exportObservations.http[i].latestFailed = &maximum
	}
	value, _ := p.DiagnosticExports(context.Background())
	raw, _ := json.Marshal(value)
	if len(raw) > 32768 || strings.Contains(string(raw), "private") {
		t.Fatal("observation unbounded or private", len(raw))
	}
}
