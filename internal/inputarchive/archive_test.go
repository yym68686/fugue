package inputarchive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

type memoryObjects struct {
	mu      sync.Mutex
	values  map[string][]byte
	corrupt bool
}

func (m *memoryObjects) Put(_ context.Context, key string, b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.values == nil {
		m.values = map[string][]byte{}
	}
	if _, ok := m.values[key]; ok {
		return errors.New("already exists")
	}
	m.values[key] = append([]byte(nil), b...)
	return nil
}
func (m *memoryObjects) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.values[key]
	if !ok {
		return nil, errors.New("not found")
	}
	b = append([]byte(nil), b...)
	if m.corrupt && len(b) > 0 {
		b[len(b)-1] ^= 1
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

type repeated struct {
	pattern []byte
	left    int64
	offset  int
}

func (r *repeated) Read(p []byte) (int, error) {
	if r.left == 0 {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), r.left))
	for i := 0; i < n; i++ {
		p[i] = r.pattern[(r.offset+i)%len(r.pattern)]
	}
	r.offset = (r.offset + n) % len(r.pattern)
	r.left -= int64(n)
	return n, nil
}

func digestRepeated(pattern []byte, n int64) string {
	h := sha256.New()
	_, _ = io.CopyN(h, &repeated{pattern: pattern, left: n}, n)
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func TestCopyAndReadStreamsBeyondLegacyLimit(t *testing.T) {
	const size = int64(128<<20) + 37
	pattern := []byte("stable compiler input bytes\n")
	planned := Manifest{Format: Format, Digest: digestRepeated(pattern, size), RawBytes: size, SegmentBytes: 1 << 20, Prefix: "compiler-input-archive/test-run"}
	objects := &memoryObjects{}
	checkpoints := map[int]Segment{}
	got, err := Copy(context.Background(), objects, &repeated{pattern: pattern, left: size}, planned, func(s Segment) error {
		if prior, ok := checkpoints[s.Ordinal]; ok && prior != s {
			return ErrIntegrity
		}
		checkpoints[s.Ordinal] = s
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = got.Validate(); err != nil {
		t.Fatal(err)
	}
	if int64(len(checkpoints)) != (size+int64(planned.SegmentBytes)-1)/int64(planned.SegmentBytes) {
		t.Fatalf("checkpoint count=%d", len(checkpoints))
	}
	r, err := Open(context.Background(), objects, got)
	if err != nil {
		t.Fatal(err)
	}
	count, err := io.Copy(io.Discard, r)
	if err != nil || count != size || !r.Verified() {
		t.Fatalf("stream verify bytes=%d verified=%v err=%v", count, r.Verified(), err)
	}
}

func TestCopyResumesFromFencedCheckpointAndRejectsCorruption(t *testing.T) {
	pattern := []byte("abcdefgh")
	const size = int64(2*(64<<10) + 11)
	planned := Manifest{Format: Format, Digest: digestRepeated(pattern, size), RawBytes: size, SegmentBytes: 64 << 10, Prefix: "archive/resume-unique"}
	objects := &memoryObjects{}
	var receipts []Segment
	stop := errors.New("simulated worker stop")
	_, err := Copy(context.Background(), objects, &repeated{pattern: pattern, left: size}, planned, func(s Segment) error {
		receipts = append(receipts, s)
		if len(receipts) == 1 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || len(receipts) != 1 {
		t.Fatalf("first attempt err=%v receipts=%d", err, len(receipts))
	}
	planned.Segments = append([]Segment(nil), receipts...)
	got, err := Copy(context.Background(), objects, &repeated{pattern: pattern, left: size}, planned, func(s Segment) error { receipts = append(receipts, s); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = Verify(context.Background(), objects, got); err != nil {
		t.Fatal(err)
	}
	objects.corrupt = true
	if err = Verify(context.Background(), objects, got); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("corruption not rejected: %v", err)
	}
}

func TestCopyRejectsWrongDigestAndInvalidRoutingMetadata(t *testing.T) {
	objects := &memoryObjects{}
	plan := Manifest{Format: Format, Digest: "sha256:" + fmt.Sprintf("%064d", 0), RawBytes: 3, SegmentBytes: 64 << 10, Prefix: "archive/test"}
	_, err := Copy(context.Background(), objects, bytes.NewBufferString("bad"), plan, func(Segment) error { return nil })
	if !errors.Is(err, ErrIntegrity) {
		t.Fatalf("wrong digest accepted: %v", err)
	}
	if len(objects.values) != 1 {
		t.Fatalf("expected orphan candidate object pending DB receipt, got %d", len(objects.values))
	}
	for _, prefix := range []string{"/rooted", "../escape", "a/../b", "a\\b", "a\x00b"} {
		bad := plan
		bad.Digest = digestRepeated([]byte("x"), 3)
		bad.Prefix = prefix
		if _, err = Open(context.Background(), objects, bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("unsafe prefix %q accepted: %v", prefix, err)
		}
	}
}
