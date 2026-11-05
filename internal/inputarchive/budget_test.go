package inputarchive

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

type countedObjects struct {
	puts, gets int
	data       []byte
}

func (o *countedObjects) Put(_ context.Context, _ string, b []byte) error {
	o.puts++
	o.data = bytes.Clone(b)
	return nil
}
func (o *countedObjects) Get(_ context.Context, _ string) (io.ReadCloser, error) {
	o.gets++
	return io.NopCloser(bytes.NewReader(o.data)), nil
}

func TestCopyBudgetLimitsRoleSeparationAndCancellation(t *testing.T) {
	ctx := t.Context()
	reader, writer := &countedObjects{data: []byte("abcd")}, &countedObjects{}
	b, err := NewBudgetObjects(reader, writer, CopyBudget{MaxPutBytes: 4, MaxGetBytes: 5, MaxPutRequests: 1, MaxGetRequests: 1, BytesPerSecond: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Put(ctx, "x", []byte("abcde")); !errors.Is(err, ErrBudget) {
		t.Fatal(err)
	}
	if err = b.Put(ctx, "x", []byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if err = b.Put(ctx, "x", []byte("a")); !errors.Is(err, ErrBudget) {
		t.Fatal(err)
	}
	r, err := b.Get(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || string(got) != "abcd" {
		t.Fatal(string(got), err)
	}
	if _, err = b.Get(ctx, "x"); !errors.Is(err, ErrBudget) {
		t.Fatal(err)
	}
	if reader.puts != 0 || writer.gets != 0 || writer.puts != 1 || reader.gets != 1 {
		t.Fatal("reader/writer roles mixed")
	}
	u := b.Usage()
	if u.PutRequests != 1 || u.GetRequests != 1 || u.PutBytes != 4 || u.GetBytes != 4 {
		t.Fatal(u)
	}
	blocked, _ := NewBudgetObjects(reader, writer, CopyBudget{MaxPutBytes: 4, MaxGetBytes: 5, MaxPutRequests: 1, MaxGetRequests: 1, BytesPerSecond: 1})
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err = blocked.Put(cancelled, "x", []byte("abcd")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if writer.puts != 1 || blocked.Usage().PutRequests != 0 {
		t.Fatal("cancelled throttle issued PUT")
	}
	deadline, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()
	if err = blocked.Put(deadline, "x", []byte("abcd")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestCopyBudgetNeverReadsBeyondPayloadCeiling(t *testing.T) {
	o := &countedObjects{data: []byte("abcdef")}
	b, _ := NewBudgetObjects(o, o, CopyBudget{MaxPutBytes: 1, MaxGetBytes: 4, MaxPutRequests: 1, MaxGetRequests: 2, BytesPerSecond: 1 << 30})
	r, err := b.Get(t.Context(), "x")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if !errors.Is(err, ErrBudget) || string(got) != "abcd" || b.Usage().GetBytes != 4 {
		t.Fatal(string(got), b.Usage(), err)
	}
}

func TestCopyBudgetCountsBothCompleteGetPasses(t *testing.T) {
	raw := bytes.Repeat([]byte("synthetic archive input"), 8000)
	m := Manifest{Format: Format, Digest: sum(raw), RawBytes: int64(len(raw)), SegmentBytes: 65536, Prefix: "copy-budget-fixture"}
	o := &memoryBudgetObjects{values: map[string][]byte{}}
	n := int64((len(raw) + m.SegmentBytes - 1) / m.SegmentBytes)
	b, _ := NewBudgetObjects(o, o, CopyBudget{MaxPutBytes: 1 << 20, MaxGetBytes: 2 << 20, MaxPutRequests: n, MaxGetRequests: 2 * n, BytesPerSecond: 1 << 30})
	result, err := Copy(t.Context(), b, b.Source(t.Context(), bytes.NewReader(raw)), m, func(Segment) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	var stored int64
	for _, s := range result.Segments {
		stored += int64(s.StoredBytes)
	}
	u := b.Usage()
	if u.SourceBytes != int64(len(raw)) || u.PutBytes != stored || u.GetBytes != 2*stored || u.PutRequests != n || u.GetRequests != 2*n {
		t.Fatal(u, stored)
	}
}

type memoryBudgetObjects struct{ values map[string][]byte }

func (o *memoryBudgetObjects) Put(_ context.Context, k string, b []byte) error {
	o.values[k] = bytes.Clone(b)
	return nil
}
func (o *memoryBudgetObjects) Get(_ context.Context, k string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(o.values[k])), nil
}

func TestCopyBudgetShortReadsChargeActualBytes(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "source", true: "get"}[remote], func(t *testing.T) {
			o := &countedObjects{data: []byte("x")}
			b, _ := NewBudgetObjects(o, o, CopyBudget{MaxPutBytes: 1 << 20, MaxGetBytes: 1 << 20,
				MaxPutRequests: 1, MaxGetRequests: 1, BytesPerSecond: 64 << 10})
			ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
			defer cancel()
			var r io.Reader = b.Source(ctx, bytes.NewReader(o.data))
			if remote {
				body, err := b.Get(ctx, "x")
				if err != nil {
					t.Fatal(err)
				}
				defer body.Close()
				r = body
			}
			// The requested buffer would cost a second. One actual byte plus
			// EOF must fit without charging 64KiB of nonexistent payload.
			buffer := make([]byte, 64<<10)
			n, err := r.Read(buffer)
			if n != 1 || err != nil || buffer[0] != 'x' {
				t.Fatalf("short read: %d %v", n, err)
			}
			if n, err = r.Read(buffer); n != 0 || err != io.EOF {
				t.Fatalf("EOF: %d %v", n, err)
			}
			u := b.Usage()
			if remote && u.GetBytes != 1 || !remote && u.SourceBytes != 1 {
				t.Fatal(u)
			}
		})
	}
}

func TestCopyBudgetReadCancellationCountsTransferredBytes(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "source", true: "get"}[remote], func(t *testing.T) {
			o := &countedObjects{data: []byte("transferred")}
			b, _ := NewBudgetObjects(o, o, CopyBudget{MaxPutBytes: 32, MaxGetBytes: 32,
				MaxPutRequests: 1, MaxGetRequests: 1, BytesPerSecond: 1})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
			defer cancel()
			var r io.Reader = b.Source(ctx, bytes.NewReader(o.data))
			if remote {
				body, err := b.Get(ctx, "x")
				if err != nil {
					t.Fatal(err)
				}
				defer body.Close()
				r = body
			}
			buffer := make([]byte, 32)
			n, err := r.Read(buffer)
			if n != len(o.data) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("transfer: %d %v", n, err)
			}
			u := b.Usage()
			if remote && u.GetBytes != int64(n) || !remote && u.SourceBytes != int64(n) {
				t.Fatal(u)
			}
			if n, err = r.Read(buffer); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("canceled: %d %v", n, err)
			}
		})
	}
}

func TestCopyBudgetPacesActualReadPayload(t *testing.T) {
	o := &countedObjects{data: bytes.Repeat([]byte{'x'}, 64<<10)}
	b, _ := NewBudgetObjects(o, o, CopyBudget{MaxPutBytes: 1 << 20, MaxGetBytes: 1 << 20,
		MaxPutRequests: 1, MaxGetRequests: 1, BytesPerSecond: 64 << 10})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	buffer := make([]byte, 64<<10)
	// Removing all read pacing would incorrectly finish before the deadline.
	n, err := b.Source(ctx, bytes.NewReader(o.data)).Read(buffer)
	if n != len(buffer) || !errors.Is(err, context.DeadlineExceeded) || b.Usage().SourceBytes != int64(n) {
		t.Fatal(n, err, b.Usage())
	}
}

type copyBudgetCancelReader struct {
	cancel context.CancelFunc
	cause  error
}

func (r copyBudgetCancelReader) Read(p []byte) (int, error) {
	p[0] = 'x'
	r.cancel()
	return 1, r.cause
}

func TestCopyBudgetRetainsReadErrorWhenPacingCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cause := errors.New("synthetic read error")
	o := &countedObjects{}
	b, _ := NewBudgetObjects(o, o, CopyBudget{MaxPutBytes: 32, MaxGetBytes: 32,
		MaxPutRequests: 1, MaxGetRequests: 1, BytesPerSecond: 1})
	for _, remote := range []bool{false, true} {
		ctx, cancel = context.WithCancel(t.Context())
		source := copyBudgetCancelReader{cancel: cancel, cause: cause}
		var reader io.Reader = b.Source(ctx, source)
		if remote {
			reader = &budgetBody{ReadCloser: io.NopCloser(source), ctx: ctx, b: b}
		}
		n, err := reader.Read(make([]byte, 4))
		cancel()
		if n != 1 || !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
			t.Fatal(n, err)
		}
	}
}
