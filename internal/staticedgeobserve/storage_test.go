package staticedgeobserve

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRestartAppendsAndDiskRecoversEvictedRequest(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	cfg := StoreConfig{NodeID: "edge-a", Directory: dir, MaxRecords: 1}
	s, e := NewStore(cfg)
	if e != nil {
		t.Fatal(e)
	}
	first := New(example(), nil)
	first.Finish(200, false, "")
	r, _ := first.Snapshot()
	s.write(r)
	second := New(example(), nil)
	second.Finish(200, false, "")
	r2, _ := second.Snapshot()
	s.write(r2)
	if _, e = NewStore(cfg); e == nil {
		t.Fatal("concurrent writer accepted")
	}
	s.Close()
	s, e = NewStore(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	third := New(example(), nil)
	third.Finish(200, false, "")
	r3, _ := third.Snapshot()
	s.write(r3)
	result, e := s.Query(Query{RequestID: r.RequestID, Since: time.Now().Add(-time.Hour), Until: time.Now(), Limit: 5})
	if e != nil || len(result.Records) != 1 || !result.DiskScanComplete {
		t.Fatalf("old request lost at restart %+v %v", result, e)
	}
}
func TestFailedDiskPreservesServingAndReportsDegradation(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, e := NewStore(StoreConfig{NodeID: "edge-a", Directory: dir})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	os.Mkdir(filepath.Join(dir, "observations-00.jsonl"), 0700)
	obs := New(example(), nil)
	body := obs.Body(io.NopCloser(&repeatReader{remaining: 1024}))
	io.Copy(io.Discard, body)
	obs.Finish(200, false, "")
	r, _ := obs.Snapshot()
	s.write(r)
	result, e := s.Query(Query{Since: time.Now().Add(-time.Hour), Until: time.Now(), Limit: 5})
	if e != nil || result.Status != "storage_degraded" || result.DiskErrors == 0 || len(result.Records) != 1 {
		t.Fatal(result, e)
	}
}

type repeatReader struct{ remaining int }

func (r *repeatReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	r.remaining -= n
	return n, nil
}
func TestLargeBodyCountersDoNotExhaustCriticalEventCapacity(t *testing.T) {
	obs := New(example(), nil)
	body := obs.Body(io.NopCloser(&repeatReader{remaining: 8 << 20}))
	buf := make([]byte, 1024)
	io.CopyBuffer(struct{ io.Writer }{io.Discard}, body, buf)
	obs.Finish(200, false, "")
	r, _ := obs.Snapshot()
	if r.Body.Bytes != 8<<20 || r.EventsDropped != 0 || len(r.Events) > 8 {
		t.Fatal(r.Body, r.EventsDropped, len(r.Events))
	}
}
func BenchmarkBodyReadObservation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		obs := New(example(), nil)
		body := obs.Body(io.NopCloser(&repeatReader{remaining: 1 << 20}))
		io.Copy(io.Discard, body)
		obs.Finish(200, false, "")
	}
}
func BenchmarkBodyReadBaseline(b *testing.B) {
	for i := 0; i < b.N; i++ {
		io.Copy(io.Discard, io.NopCloser(&repeatReader{remaining: 1 << 20}))
	}
}
func TestRemoteFailureHasNoEffectOnLocalEvidence(t *testing.T) {
	s := &RemoteSink{queue: make(chan Record, 1)}
	obs := New(example(), nil)
	obs.Finish(200, false, "")
	r, _ := obs.Snapshot()
	for i := 0; i < 100; i++ {
		s.Submit(r)
	}
	if s.dropped.Load() != 99 {
		t.Fatal("unbounded queue")
	}
}
