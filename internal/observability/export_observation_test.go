package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type exportObservationTestExporter struct {
	name string
	run  func(context.Context, []Event) error
}

func (e exportObservationTestExporter) Name() string { return e.name }
func (e exportObservationTestExporter) Export(ctx context.Context, events []Event) error {
	return e.run(ctx, events)
}

type exportObservationTimeout struct{}

func (exportObservationTimeout) Error() string   { return "private error" }
func (exportObservationTimeout) Timeout() bool   { return true }
func (exportObservationTimeout) Temporary() bool { return true }

func exportObservationRows(t *testing.T, p *Pipeline) map[string]exportAttemptSnapshot {
	t.Helper()
	value, err := p.DiagnosticExports(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := value.(exportObservationSnapshot)
	if snapshot.Schema != "fugue.telemetry-export-observation/v1" || len(snapshot.Exporters) != 5 || snapshot.ObservedAt.IsZero() {
		t.Fatal("invalid bounded snapshot", snapshot)
	}
	rows := map[string]exportAttemptSnapshot{}
	for _, row := range snapshot.Exporters {
		rows[row.Exporter] = row
	}
	return rows
}

func TestExportObservationsClassifyWithoutChangingResultsOrInputs(t *testing.T) {
	privateErr := errors.New("private destination and error")
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"success", nil}, {"failed", privateErr},
		{"deadline", fmt.Errorf("private: %w", context.DeadlineExceeded)},
		{"canceled", fmt.Errorf("private: %w", context.Canceled)},
		{"timeout", &net.OpError{Op: "private", Err: exportObservationTimeout{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPipeline(Config{}, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			events := []Event{{Kind: EventKindLog, Message: "private payload", Attributes: map[string]string{"private": "private value"}}}
			before, _ := json.Marshal(events)
			exporter := exportObservationTestExporter{name: "private dynamic name", run: func(received context.Context, receivedEvents []Event) error {
				if received != ctx || &receivedEvents[0] != &events[0] {
					t.Fatal("observation changed the context or batch identity")
				}
				return tc.err
			}}
			if got := p.exportObservations.export(ctx, exporter, events, 2); got != tc.err {
				t.Fatal("observation changed the returned error")
			}
			row := exportObservationRows(t, p)["other"]
			if row.Started != 1 || row.Completed != 1 || row.Retries != 1 || row.Inflight != 0 || row.AttemptEvents != 1 || row.LastStartedAt == nil || row.LastFinishedAt == nil || row.LastFinishedAt.Before(*row.LastStartedAt) || row.TotalNanos != row.MaxNanos {
				t.Fatal("incorrect attempt counters", row)
			}
			outcomes := map[string]uint64{"success": row.Success, "failed": row.Failed, "deadline": row.Deadline, "canceled": row.Canceled, "timeout": row.Timeout}
			for name, count := range outcomes {
				if (name == tc.name && count != 1) || (name != tc.name && count != 0) {
					t.Fatal("incorrect fixed error classification", outcomes)
				}
			}
			after, _ := json.Marshal(events)
			value, _ := p.DiagnosticExports(ctx)
			raw, _ := json.Marshal(value)
			if string(before) != string(after) || strings.Contains(string(raw), "private") || len(raw) > 4096 {
				t.Fatal("diagnostic mutated inputs, leaked private material or exceeded fixed bound")
			}
		})
	}
	var disabled *exportObservations
	if got := disabled.export(t.Context(), exportObservationTestExporter{run: func(context.Context, []Event) error { return privateErr }}, nil, 1); got != privateErr {
		t.Fatal("nil observations changed export result")
	}
}

func TestExportObservationsExposeInflightAndRespectSnapshotCancellation(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	exporter := exportObservationTestExporter{name: "logs", run: func(context.Context, []Event) error {
		close(entered)
		<-release
		return nil
	}}
	go func() { done <- p.exportObservations.export(t.Context(), exporter, []Event{{}, {}}, 1) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("export did not start")
	}
	row := exportObservationRows(t, p)["logs"]
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if row.Started != 1 || row.Completed != 0 || row.Inflight != 1 || row.AttemptEvents != 2 || row.LastStartedAt == nil || row.LastFinishedAt != nil {
		t.Fatal("unfinished attempt was hidden", row)
	}
	row = exportObservationRows(t, p)["logs"]
	if row.Completed != 1 || row.Inflight != 0 || row.Success != 1 || row.LastFinishedAt == nil {
		t.Fatal("completion was not observed", row)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if value, err := p.DiagnosticExports(ctx); !errors.Is(err, context.Canceled) || value != nil {
		t.Fatal("canceled snapshot returned a value", value, err)
	}
}

func TestExportObservationsPreserveMultiRetryAndSinglePipelineSemantics(t *testing.T) {
	for _, multi := range []bool{false, true} {
		t.Run(fmt.Sprint(multi), func(t *testing.T) {
			p := NewPipeline(Config{RetryMaxAttempts: 3, ExportTimeout: time.Second}, nil)
			p.ctx = t.Context()
			flaky := &recordingExporter{name: "analytics", failures: 1}
			good := &recordingExporter{name: "metrics"}
			p.exporter = flaky
			if multi {
				p.exporter = MultiExporter{exporters: []Exporter{good, flaky}}
			}
			p.exportBatch([]Event{{Kind: EventKindLog}})
			if flaky.calls != 2 || (multi && good.calls != 1) || p.Snapshot().Exported != 1 || p.Snapshot().ExportErrors != 0 {
				t.Fatal("instrumentation changed batch or retry semantics", flaky.calls, good.calls, p.Snapshot())
			}
			rows := exportObservationRows(t, p)
			row := rows["analytics"]
			if row.Started != 2 || row.Completed != 2 || row.AttemptEvents != 2 || row.Retries != 1 || row.Failed != 1 || row.Success != 1 {
				t.Fatal("retry evidence missing", row)
			}
			if multi && (rows["metrics"].Started != 1 || rows["metrics"].Retries != 0) {
				t.Fatal("successful exporter was retried", rows["metrics"])
			}
		})
	}
}

func TestExportObservationsPreservePerExporterTimeoutAndDirectExport(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	slow, fast := &blockingExporter{name: "metrics"}, &contextRecordingExporter{name: "analytics"}
	multi := MultiExporter{exporters: []Exporter{slow, fast}, observations: &p.exportObservations}
	if err := multi.ExportWithRetryTimeout(t.Context(), nil, 1, 10*time.Millisecond); err == nil || fast.lastErr != nil || slow.calls != 1 || fast.calls != 1 {
		t.Fatal("per-exporter timeout changed", err, fast.lastErr)
	}
	rows := exportObservationRows(t, p)
	if rows["metrics"].Deadline != 1 || rows["analytics"].Success != 1 {
		t.Fatal(rows)
	}
	privateErr := errors.New("private export error")
	direct := MultiExporter{exporters: []Exporter{exportObservationTestExporter{name: "noop", run: func(context.Context, []Event) error { return privateErr }}}, observations: &p.exportObservations}
	if err := direct.Export(t.Context(), nil); !errors.Is(err, privateErr) {
		t.Fatal("direct export error changed", err)
	}
	if row := exportObservationRows(t, p)["noop"]; row.Completed != 1 || row.Failed != 1 || row.Retries != 0 {
		t.Fatal(row)
	}
}

func TestExportObservationsBoundedConcurrentSnapshotsOwnTheirValues(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	exporter := exportObservationTestExporter{name: "unbounded private label", run: func(context.Context, []Event) error { return nil }}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = p.exportObservations.export(t.Context(), exporter, nil, 1)
				_, _ = p.DiagnosticExports(t.Context())
			}
		}()
	}
	wg.Wait()
	row := exportObservationRows(t, p)["other"]
	if row.Started != 800 || row.Completed != 800 || row.Success != 800 || row.Inflight != 0 || row.MaxNanos > row.TotalNanos {
		t.Fatal(row)
	}
	before, _ := p.DiagnosticExports(t.Context())
	changed := before.(exportObservationSnapshot)
	changed.Exporters[4].Exporter = "private replacement"
	*changed.Exporters[4].LastStartedAt = time.Time{}
	after, _ := p.DiagnosticExports(t.Context())
	if reflect.DeepEqual(changed, after) || exportObservationRows(t, p)["other"].LastStartedAt.IsZero() {
		t.Fatal("snapshot mutation contaminated recorder")
	}
}
