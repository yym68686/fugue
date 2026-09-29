package staticedgeobserve

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func queryFixture(t testing.TB, index int, at time.Time, wait float64) Record {
	t.Helper()
	obs := New(example(), nil)
	obs.Finish(200, false, fmt.Sprintf("application-%d", index))
	r, ok := obs.Snapshot()
	if !ok {
		t.Fatal("fixture snapshot unavailable")
	}
	r.RequestID, r.SpanID, r.AttemptID = fmt.Sprintf("request-%d", index), fmt.Sprintf("span-%d", index), fmt.Sprintf("attempt-%d", index)
	r.StartedAt, r.ObservedAt, r.ElapsedMS = at.Add(-5*time.Second), at, 5000
	r.Body.ReadBlockMS, r.Body.MaxReadBlockMS = wait, wait
	r.Events = nil
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSlowQueryFindsOlderSlowRecordBehindNewerFastRecords(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := NewStore(StoreConfig{NodeID: "edge-a", Directory: dir, MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at := time.Now().Add(-time.Minute)
	want := queryFixture(t, 0, at, 2000)
	s.write(want)
	for i := 1; i <= 240; i++ {
		s.write(queryFixture(t, i, at.Add(time.Duration(i)*time.Millisecond), 0))
	}
	for _, indexed := range []bool{true, false} {
		if !indexed {
			s.invalidateDiskIndex(s.file.Name())
		}
		out, err := s.Query(Query{Since: at.Add(-time.Second), Until: time.Now(), MinReadMS: 1000, Limit: 10})
		if err != nil || !out.DiskScanComplete || out.Truncated || len(out.Records) != 1 || out.Records[0].SpanID != want.SpanID {
			t.Fatalf("older slow span omitted: indexed=%v err=%v complete=%v truncated=%v count=%d", indexed, err, out.DiskScanComplete, out.Truncated, len(out.Records))
		}
	}
}

func TestSlowQueryDoesNotReviveSupersededPendingWait(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, err := NewStore(StoreConfig{NodeID: "edge-a", Directory: dir, MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at := time.Now().Add(-time.Minute)
	r := queryFixture(t, 0, at, 2000)
	r.Sequence = 1
	s.write(r)
	r.Sequence = 2
	r.ObservedAt = at.Add(time.Second)
	r.Body.ReadBlockMS = 0
	r.Body.MaxReadBlockMS = 0
	s.write(r)
	// An older snapshot can appear after the newer one across recovery/segments.
	r.Sequence = 1
	r.ObservedAt = at
	r.Body.ReadBlockMS = 2000
	r.Body.MaxReadBlockMS = 2000
	s.write(r)
	for _, indexed := range []bool{true, false} {
		if !indexed {
			s.invalidateDiskIndex(s.file.Name())
		}
		out, err := s.Query(Query{Since: at.Add(-time.Second), Until: time.Now(), MinReadMS: 1000, Limit: 10})
		if err != nil || len(out.Records) != 0 || !out.DiskScanComplete {
			t.Fatal(indexed, out, err)
		}
	}
}

func BenchmarkExactDiskQuery(b *testing.B) {
	dir := b.TempDir()
	os.Chmod(dir, 0700)
	s, err := NewStore(StoreConfig{NodeID: "edge-a", Directory: dir, MaxRecords: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	at := time.Now().Add(-time.Minute)
	for i := 0; i < 12000; i++ {
		r := queryFixture(b, i, at, 0)
		for j := 0; j < 48; j++ {
			r.Events = append(r.Events, Event{Kind: "body_read_end", AtMS: float64(j), Bytes: 1024})
		}
		s.write(r)
	}
	q := Query{RequestID: "request-11000", Since: at.Add(-time.Second), Until: time.Now(), Limit: 10}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := s.Query(q)
		if err != nil {
			b.Fatal(err)
		}
		if !out.DiskScanComplete {
			b.ReportMetric(1, "incomplete/query")
		}
	}
}

func TestExactQueryAcceptsEscapedIDsAndReportsCorruption(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	r := queryFixture(t, 7, time.Now().Add(-time.Minute), 0)
	r.RequestID = "req-a"
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	json.Unmarshal(raw, &doc)
	doc["request_id"] = json.RawMessage(`"req-\u0061"`)
	raw, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "observations-00.jsonl")
	if err = os.WriteFile(file, append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := NewStore(StoreConfig{NodeID: "edge-a", Directory: dir, MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	q := Query{RequestID: "req-a", Since: r.ObservedAt.Add(-time.Second), Until: time.Now(), Limit: 5}
	out, err := s.Query(q)
	if err != nil || len(out.Records) != 1 || !out.DiskScanComplete {
		t.Fatal(out, err)
	}
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("{broken-record}\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	out, err = s.Query(q)
	if err != nil || out.DiskScanComplete || !out.Truncated {
		t.Fatal("corrupt unrelated line hidden", out, err)
	}
	s.write(queryFixture(t, 8, time.Now().Add(-time.Second), 0))
	out, err = s.Query(q)
	if err != nil || out.DiskScanComplete || !out.Truncated {
		t.Fatal("legitimate append hid old corruption", out, err)
	}
}

func TestDiskIndexRotationRestartAndApplicationLookup(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	cfg := StoreConfig{NodeID: "edge-a", Directory: dir, SegmentBytes: MaxRecordBytes, Segments: 2, MaxRecords: 1}
	s, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-time.Minute)
	for i := 0; i < 240; i++ {
		s.write(queryFixture(t, i, at.Add(time.Duration(i)*time.Millisecond), 0))
	}
	if s.segment < 2 {
		t.Fatal("fixture did not rotate")
	}
	q := Query{RequestID: "application-239", Since: at.Add(-time.Second), Until: time.Now(), Limit: 10}
	for phase := 0; phase < 2; phase++ {
		out, err := s.Query(q)
		if err != nil || !out.DiskScanComplete || len(out.Records) != 1 || out.Records[0].RequestID != "request-239" {
			t.Fatal(phase, out, err)
		}
		old := q
		old.RequestID = "request-0"
		out, err = s.Query(old)
		if err != nil || !out.DiskScanComplete || len(out.Records) != 0 {
			t.Fatal("rotated evidence resurrected", phase, out, err)
		}
		if phase == 0 {
			s.Close()
			s, err = NewStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	defer s.Close()
	if s.indexRecords > maxDiskIndexRecords {
		t.Fatal("index unbounded")
	}
}

func TestDiskIndexOverflowFallsBackWithoutSkippingRequest(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, err := NewStore(StoreConfig{NodeID: "edge-a", Directory: dir, MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at := time.Now().Add(-time.Minute)
	r := queryFixture(t, 1, at, 0)
	s.write(r)
	// Populate scalar index entries to its public hard limit, without doing a
	// large disk or runtime workload just to trigger capacity handling.
	path := filepath.Join(dir, "observations-00.jsonl")
	for s.indexRecords < maxDiskIndexRecords {
		r.SpanID = fmt.Sprintf("capacity-span-%d", s.indexRecords)
		s.indexRecord(path, r, 0, 1, nil)
	}
	s.write(queryFixture(t, 2, at.Add(time.Second), 2000))
	if s.indexRecords != maxDiskIndexRecords || s.Status()["disk_index_complete"] != false {
		t.Fatal("index limit not enforced")
	}
	s.write(queryFixture(t, 3, at.Add(2*time.Second), 0))
	q := Query{RequestID: "application-2", Since: at.Add(-time.Second), Until: time.Now(), Limit: 10}
	out, err := s.Query(q)
	if err != nil || !out.DiskScanComplete || len(out.Records) != 1 || out.Records[0].RequestID != "request-2" {
		t.Fatal("overflow skipped fallback", out, err)
	}
}

func TestCompactedIndexPreservesHistoricalWindowAndRestart(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	cfg := StoreConfig{NodeID: "edge-a", Directory: dir, MaxRecords: 1}
	s, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	at := time.Now().Add(-time.Minute)
	r := queryFixture(t, 1, at, 2000)
	r.Sequence = 1
	s.write(r)
	for seq := uint64(2); seq <= 12; seq++ {
		r.Sequence = seq
		r.ObservedAt = at.Add(time.Duration(seq) * time.Second)
		r.Body.ReadBlockMS = 0
		r.Body.MaxReadBlockMS = 0
		s.write(r)
	}
	for phase := 0; phase < 2; phase++ {
		if s.indexRecords != 1 {
			t.Fatal("snapshot not compacted", phase, s.indexRecords)
		}
		historical := Query{RequestID: r.RequestID, Since: at.Add(-time.Second), Until: at.Add(time.Second), MinReadMS: 1000, Limit: 10}
		out, e := s.Query(historical)
		if e != nil || !out.DiskScanComplete || out.Truncated || len(out.Records) != 1 || out.Records[0].Sequence != 1 {
			t.Fatal("historical snapshot omitted", phase, out, e)
		}
		historical.Until = time.Now()
		out, e = s.Query(historical)
		if e != nil || !out.DiskScanComplete || len(out.Records) != 0 {
			t.Fatal("superseded wait revived", phase, out, e)
		}
		historical.Until = at.Add(11 * time.Second)
		historical.MinReadMS = 0
		out, e = s.Query(historical)
		if e != nil || !out.DiskScanComplete || len(out.Records) != 1 || out.Records[0].Sequence != 11 {
			t.Fatal("recent in-flight query window missing", phase, out, e)
		}
		if phase == 0 {
			s.Close()
			s, err = NewStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestCompactedIndexKeepsApplicationIDPerSnapshot(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, e := NewStore(StoreConfig{NodeID: "edge-a", Directory: dir, MaxRecords: 1})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	at := time.Now().Add(-time.Minute)
	r := queryFixture(t, 1, at, 0)
	r.ApplicationRequestID = ""
	r.Sequence = 1
	s.write(r)
	r.Sequence = 2
	r.ObservedAt = at.Add(time.Second)
	r.ApplicationRequestID = "app-terminal"
	s.write(r)
	q := Query{RequestID: r.RequestID, Since: at.Add(-time.Second), Until: at.Add(500 * time.Millisecond), Limit: 10}
	out, e := s.Query(q)
	if e != nil || !out.DiskScanComplete || len(out.Records) != 1 || out.Records[0].ApplicationRequestID != "" {
		t.Fatal("snapshot acquired later application identity", out, e)
	}
	q.RequestID = "app-terminal"
	out, e = s.Query(q)
	if e != nil || !out.DiskScanComplete || len(out.Records) != 0 {
		t.Fatal("application identity appeared before terminal", out, e)
	}
	q.Until = time.Now()
	out, e = s.Query(q)
	if e != nil || !out.DiskScanComplete || len(out.Records) != 1 || out.Records[0].ApplicationRequestID != "app-terminal" {
		t.Fatal("terminal application lookup missing", out, e)
	}
}

func TestDiskIndexConcurrentRotationAndReads(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, err := NewStore(StoreConfig{NodeID: "edge-a", Directory: dir, SegmentBytes: MaxRecordBytes, Segments: 2, MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at := time.Now().Add(-time.Minute)
	records := make([]Record, 500)
	for i := range records {
		records[i] = queryFixture(t, i, at.Add(time.Duration(i)*time.Millisecond), 2000)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(done)
		for _, r := range records {
			s.write(r)
		}
	}()
	q := Query{Since: at.Add(-time.Second), Until: time.Now(), MinReadMS: 1000, Limit: 20}
	for i := 0; i < 30; i++ {
		out, err := s.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range out.Records {
			if r.Validate() != nil || r.Body.ReadBlockMS < 1000 {
				t.Fatal("invalid concurrent evidence")
			}
		}
		select {
		case <-done:
			i = 30
		default:
		}
	}
	wg.Wait()
	q.RequestID = "request-499"
	out, err := s.Query(q)
	if err != nil || !out.DiskScanComplete || len(out.Records) != 1 {
		t.Fatal("stable final record missing", out, err)
	}
}

func TestQueryBusyDoesNotBlockCollectorSubmission(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, err := NewStore(StoreConfig{NodeID: "edge-a", Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	s.queryMu.Lock()
	defer s.queryMu.Unlock()
	if !s.Submit(queryFixture(t, 1, time.Now(), 0)) {
		t.Fatal("query prevented submit")
	}
	out, err := s.Query(Query{Since: time.Now().Add(-time.Hour), Until: time.Now(), Limit: 1})
	if err != nil || out.Status != "storage_degraded" || !out.Truncated {
		t.Fatal(out, err)
	}
}

func TestRepeatedSnapshotsDoNotExhaustDiskIndex(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, err := NewStore(StoreConfig{NodeID: "edge-a", Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := queryFixture(t, 1, time.Now().Add(-time.Minute), 0)
	s.write(r)
	for i := 0; i < maxDiskIndexRecords+2; i++ {
		r.Sequence++
		s.indexRecord(s.file.Name(), r, 0, 1, nil)
	}
	if s.indexRecords != 1 || s.Status()["disk_index_complete"] != true {
		t.Fatalf("repeated snapshot positions exhausted bounded index: entries=%d complete=%v", s.indexRecords, s.Status()["disk_index_complete"])
	}
}
