package staticedgeobserve

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type capture struct{ r Record }

func (s *capture) Submit(r Record) bool { s.r = r; return true }
func example() Record {
	return Record{NodeID: "edge-a", ProcessID: "process-a", RequestID: ID(), Hop: "ingress", Protocol: "HTTP/2.0", Build: "test", ConfigDigest: "sha256:abc", Correlation: "entry"}
}

func TestReadWaitPreservesBodyAndDoesNotInventRootCause(t *testing.T) {
	sink := &capture{}
	o := New(example(), sink)
	reader, writer := io.Pipe()
	body := o.Body(reader)
	go func() { time.Sleep(30 * time.Millisecond); writer.Write([]byte("synthetic-content")); writer.Close() }()
	data, e := io.ReadAll(body)
	if e != nil || string(data) != "synthetic-content" {
		t.Fatal(string(data), e)
	}
	body.Close()
	o.Finish(200, false, "")
	r := sink.r
	if r.Validate() != nil || r.Body.Bytes != 17 || r.Body.FirstByteMS == nil || *r.Body.FirstByteMS < 20 || !r.Body.EOF {
		t.Fatalf("bad observation %+v", r)
	}
	f := Explain(r)
	if f[0].State != "bounded" || f[0].Mechanism != "application_body_read_wait" {
		t.Fatal(f)
	}
	raw, _ := json.Marshal(r)
	if bytes.Contains(raw, data) {
		t.Fatal("payload retained")
	}
}

func TestAbsentBodySentinelIsPreserved(t *testing.T) {
	o := New(example(), nil)
	if o.Body(nil) != nil || o.Body(http.NoBody) != http.NoBody {
		t.Fatal("empty request framing changed")
	}
}

func TestBackpressureIsNotMistakenForReadDelay(t *testing.T) {
	o := New(example(), nil)
	r := o.Body(io.NopCloser(strings.NewReader("abcd")))
	buf := make([]byte, 2)
	r.Read(buf)
	time.Sleep(35 * time.Millisecond)
	r.Read(buf)
	o.Finish(200, false, "")
	v, _ := o.Snapshot()
	if v.Body.ReadBlockMS > 15 || v.ElapsedMS < 30 {
		t.Fatalf("consumer pause counted as Read wait: %+v", v)
	}
}

func TestExplicitProtocolWaitNeedsCompleteEvidence(t *testing.T) {
	o := New(example(), nil)
	o.AttachProtocol("conn", 1, true, true)
	o.Event("h2_window_wait_begin", 1, 0)
	time.Sleep(time.Millisecond)
	o.Event("h2_window_wait_end", 1, 1)
	r, _ := o.Snapshot()
	if len(Explain(r)) != 2 || Explain(r)[1].State != "confirmed" {
		t.Fatal(Explain(r))
	}
	r.EventsDropped = 1
	if len(Explain(r)) != 1 {
		t.Fatal("claimed protocol attribution despite loss")
	}
}

func TestObservationSaturationDoesNotBlockRead(t *testing.T) {
	s := &AsyncSink{queue: make(chan Record, 1)}
	o := New(example(), s)
	o.Emit()
	start := time.Now()
	for i := 0; i < 1000; i++ {
		o.Emit()
	}
	if time.Since(start) > time.Second {
		t.Fatal("blocked on full sink")
	}
	r, _ := o.Snapshot()
	if r.SnapshotsDropped == 0 {
		t.Fatal("missing loss evidence")
	}
}

func TestCorrelationRejectsSpoofedOrExpiredParent(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	p := Parent{RequestID: ID(), SpanID: ID(), NodeID: "edge", Issued: time.Now().Unix()}
	v, e := SignParent(p, key)
	if e != nil {
		t.Fatal(e)
	}
	got, e := VerifyParent(v, key, true, time.Now())
	if e != nil || got != p {
		t.Fatal(got, e)
	}
	if _, e = VerifyParent(v, key, false, time.Now()); e == nil {
		t.Fatal("untrusted peer accepted")
	}
	if _, e = VerifyParent(v, bytes.Repeat([]byte{8}, 32), true, time.Now()); e == nil {
		t.Fatal("bad MAC accepted")
	}
	if _, e = VerifyParent(v, key, true, time.Now().Add(25*time.Hour)); e == nil {
		t.Fatal("expired parent accepted")
	}
}

func TestCollectorRejectsPayloadAndBoundsResults(t *testing.T) {
	cfg := StoreConfig{NodeID: "edge-a", Directory: t.TempDir(), MaxRecords: 2, QueueSize: 2}
	if err := os.Chmod(cfg.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	s, e := NewStore(cfg)
	if e != nil {
		t.Fatal(e)
	}
	req := httptest.NewRequest(http.MethodPost, "/records", strings.NewReader(`{"body_text":"secret"}`))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	for i := 0; i < 3; i++ {
		o := New(example(), nil)
		o.Finish(200, false, "")
		r, _ := o.Snapshot()
		s.write(r)
	}
	q := Query{Since: time.Now().Add(-time.Hour), Until: time.Now(), Limit: 1}
	out, e := s.Query(q)
	if e != nil || !out.Truncated || len(out.Records) != 1 || s.evicted.Load() != 1 {
		t.Fatal(out, e)
	}
	s.Close()
	reopened, e := NewStore(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	out, e = reopened.Query(q)
	if e != nil || len(out.Records) != 1 {
		t.Fatal(out, e)
	}
}

func TestUnavailableCollectorDoesNotHoldServing(t *testing.T) {
	s, e := NewAsyncSink(t.TempDir()+"/missing.sock", 2)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	o := New(example(), s)
	start := time.Now()
	body := o.Body(io.NopCloser(strings.NewReader("abc")))
	io.ReadAll(body)
	o.Finish(200, false, "")
	if time.Since(start) > time.Second {
		t.Fatal("collector failure blocked body")
	}
}
