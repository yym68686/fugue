// Package inputarchive stores an input's original canonical bytes without
// decoding or re-encoding JSON. Objects are bounded gzip segments; publishing
// their manifest and changing database readers remain the caller's job.
package inputarchive

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"path"
	"regexp"
	"strings"
)

const (
	Format                    = "compiler-input-archive-v1"
	MaxInputBytes       int64 = 1 << 30
	DefaultSegmentBytes       = 1 << 20
	MaxSegmentBytes           = 16 << 20
	MaxSegments               = 16384
	storedOverhead            = 1 << 20
)

var ErrIntegrity = errors.New("compiler input archive integrity failure")
var ErrInvalid = errors.New("invalid compiler input archive")
var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Objects deliberately has no delete/list API: migration cannot accidentally
// apply a backup retention policy or remove another operation's objects.
type Objects interface {
	// Put is create-only. Existing objects must never be replaced. An
	// ambiguous PUT can be recovered by reading and checking the exact key.
	Put(context.Context, string, []byte) error
	Get(context.Context, string) (io.ReadCloser, error)
}

type Segment struct {
	Ordinal     int    `json:"ordinal"`
	Key         string `json:"key"`
	RawBytes    int    `json:"raw_bytes"`
	StoredBytes int    `json:"stored_bytes"`
	RawHash     string `json:"raw_hash"`
	StoredHash  string `json:"stored_hash"`
}

type Manifest struct {
	Format       string    `json:"format"`
	Digest       string    `json:"digest"`
	RawBytes     int64     `json:"raw_bytes"`
	SegmentBytes int       `json:"segment_bytes"`
	Prefix       string    `json:"prefix"`
	Segments     []Segment `json:"segments"`
}

func validPrefix(prefix string) bool {
	return len(prefix) > 0 && len(prefix) <= 512 && path.Clean(prefix) == prefix &&
		!strings.HasPrefix(prefix, "/") && !strings.ContainsAny(prefix, "\\\x00\r\n") &&
		prefix != "." && prefix != ".." && !strings.HasPrefix(prefix, "../")
}

func validateHeader(m Manifest) error {
	if m.Format != Format || !digestPattern.MatchString(m.Digest) || m.RawBytes < 1 || m.RawBytes > MaxInputBytes ||
		m.SegmentBytes < 64<<10 || m.SegmentBytes > MaxSegmentBytes || !validPrefix(m.Prefix) ||
		(m.RawBytes+int64(m.SegmentBytes)-1)/int64(m.SegmentBytes) > MaxSegments {
		return ErrInvalid
	}
	return nil
}

func (m Manifest) Validate() error {
	if err := validateHeader(m); err != nil {
		return err
	}
	count := int((m.RawBytes + int64(m.SegmentBytes) - 1) / int64(m.SegmentBytes))
	if len(m.Segments) != count {
		return ErrInvalid
	}
	for i, s := range m.Segments {
		if err := validateSegment(m, s, i); err != nil {
			return err
		}
	}
	return nil
}

func validateSegment(m Manifest, s Segment, ordinal int) error {
	want := min(int64(m.SegmentBytes), m.RawBytes-int64(ordinal)*int64(m.SegmentBytes))
	if want <= 0 || s.Ordinal != ordinal || s.RawBytes != int(want) || s.StoredBytes < 1 || s.StoredBytes > s.RawBytes+storedOverhead ||
		!digestPattern.MatchString(s.RawHash) || !digestPattern.MatchString(s.StoredHash) || s.Key != segmentKey(m.Prefix, ordinal, s.StoredHash) {
		return ErrInvalid
	}
	return nil
}

func segmentKey(prefix string, ordinal int, digest string) string {
	return fmt.Sprintf("%s/%05d-%s.gz", prefix, ordinal, strings.TrimPrefix(digest, "sha256:"))
}

func sum(b []byte) string { h := sha256.Sum256(b); return "sha256:" + hex.EncodeToString(h[:]) }

// Copy resumes by re-reading the bounded source stream and checking prior
// segment receipts. No segment receipt is emitted until a full remote GET
// verifies it. No Manifest is returned until the full source digest matches
// and a separate complete remote reconstruction has also matched it.
// A caller must keep this prefix unique to this archive attempt and must
// persist receipts with its lease's fencing token before routing the result.
func Copy(ctx context.Context, objects Objects, source io.Reader, planned Manifest, checkpoint func(Segment) error) (Manifest, error) {
	if ctx == nil || objects == nil || source == nil || checkpoint == nil {
		return Manifest{}, ErrInvalid
	}
	if err := validateHeader(planned); err != nil {
		return Manifest{}, err
	}
	count := int((planned.RawBytes + int64(planned.SegmentBytes) - 1) / int64(planned.SegmentBytes))
	if len(planned.Segments) > count {
		return Manifest{}, ErrInvalid
	}
	for i, s := range planned.Segments {
		if err := validateSegment(planned, s, i); err != nil {
			return Manifest{}, err
		}
	}
	result := planned
	result.Segments = nil
	h := sha256.New()
	buffer := make([]byte, planned.SegmentBytes)
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return Manifest{}, err
		}
		n := int(min(int64(planned.SegmentBytes), planned.RawBytes-int64(i)*int64(planned.SegmentBytes)))
		raw := buffer[:n]
		if received, err := io.ReadFull(source, raw); err != nil {
			return Manifest{}, &sourceReadError{ordinal: i, received: received, cause: err}
		}
		h.Write(raw)
		if i < len(planned.Segments) {
			s := planned.Segments[i]
			// Resume across compressor/library upgrades using the original
			// immutable receipt, rather than demanding identical recompression.
			if s.RawHash != sum(raw) {
				return Manifest{}, fmt.Errorf("archive source/receipt differs: %w", ErrIntegrity)
			}
			if _, err := readSegment(ctx, objects, s); err != nil {
				return Manifest{}, err
			}
			if err := checkpoint(s); err != nil {
				return Manifest{}, err
			}
			result.Segments = append(result.Segments, s)
			continue
		}
		var compressed bytes.Buffer
		z, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
		if err != nil {
			return Manifest{}, err
		}
		if _, err = z.Write(raw); err != nil {
			return Manifest{}, err
		}
		if err = z.Close(); err != nil {
			return Manifest{}, err
		}
		stored := compressed.Bytes()
		s := Segment{Ordinal: i, RawBytes: n, StoredBytes: len(stored), RawHash: sum(raw), StoredHash: sum(stored)}
		s.Key = segmentKey(planned.Prefix, i, s.StoredHash)
		if err = validateSegment(planned, s, i); err != nil {
			return Manifest{}, err
		}
		{
			putErr := objects.Put(ctx, s.Key, stored)
			// A timeout/precondition conflict may mean a previous PUT succeeded.
			// Only GET + hash verification can resolve that ambiguity.
			if _, err = readSegment(ctx, objects, s); err != nil {
				if putErr != nil {
					return Manifest{}, fmt.Errorf("archive segment upload not confirmed: %w", putErr)
				}
				return Manifest{}, err
			}
		}
		if err = checkpoint(s); err != nil {
			return Manifest{}, err
		}
		result.Segments = append(result.Segments, s)
	}
	var extra [1]byte
	if n, err := io.ReadFull(source, extra[:]); n != 0 || err != io.EOF {
		return Manifest{}, fmt.Errorf("archive source exceeds declared length: %w", ErrIntegrity)
	}
	if "sha256:"+hex.EncodeToString(h.Sum(nil)) != planned.Digest {
		return Manifest{}, fmt.Errorf("archive source digest differs: %w", ErrIntegrity)
	}
	if err := verify(ctx, objects, result, checkpoint); err != nil {
		return Manifest{}, err
	}
	return result, nil
}

func readSegment(ctx context.Context, objects Objects, s Segment) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := objects.Get(ctx, s.Key)
	if err != nil {
		return nil, err
	}
	stored, err := io.ReadAll(io.LimitReader(body, int64(s.StoredBytes)+1))
	closeErr := body.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(stored) != s.StoredBytes || sum(stored) != s.StoredHash {
		return nil, fmt.Errorf("archive stored segment differs: %w", ErrIntegrity)
	}
	input := bytes.NewReader(stored)
	z, err := gzip.NewReader(input)
	if err != nil {
		return nil, fmt.Errorf("archive gzip header: %w", ErrIntegrity)
	}
	z.Multistream(false)
	raw, err := io.ReadAll(io.LimitReader(z, int64(s.RawBytes)+1))
	closeErr = z.Close()
	if err != nil || closeErr != nil || len(raw) != s.RawBytes || input.Len() != 0 || sum(raw) != s.RawHash {
		return nil, fmt.Errorf("archive raw segment differs: %w", ErrIntegrity)
	}
	return raw, nil
}

type Reader struct {
	ctx      context.Context
	objects  Objects
	manifest Manifest
	ordinal  int
	pending  []byte
	hash     hash.Hash
	size     int64
	err      error
	done     bool
	closed   bool
}

func Open(ctx context.Context, objects Objects, m Manifest) (*Reader, error) {
	if ctx == nil || objects == nil {
		return nil, ErrInvalid
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	m.Segments = append([]Segment(nil), m.Segments...)
	return &Reader{ctx: ctx, objects: objects, manifest: m, hash: sha256.New()}, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	if err := r.ctx.Err(); err != nil {
		r.err = err
		return 0, err
	}
	for len(r.pending) == 0 {
		if r.done {
			return 0, io.EOF
		}
		if r.ordinal == len(r.manifest.Segments) {
			r.done = true
			if r.size != r.manifest.RawBytes || "sha256:"+hex.EncodeToString(r.hash.Sum(nil)) != r.manifest.Digest {
				r.err = fmt.Errorf("archive full digest differs: %w", ErrIntegrity)
				return 0, r.err
			}
			return 0, io.EOF
		}
		r.pending, r.err = readSegment(r.ctx, r.objects, r.manifest.Segments[r.ordinal])
		if r.err != nil {
			return 0, r.err
		}
		r.ordinal++
		r.size += int64(len(r.pending))
		r.hash.Write(r.pending)
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (r *Reader) Verified() bool { return r.done && r.err == nil && !r.closed }
func (r *Reader) Close() error   { r.pending = nil; r.closed = true; return nil }

func Verify(ctx context.Context, objects Objects, m Manifest) error {
	return verify(ctx, objects, m, nil)
}

func verify(ctx context.Context, objects Objects, m Manifest, checkpoint func(Segment) error) error {
	r, err := Open(ctx, objects, m)
	if err != nil {
		return err
	}
	defer r.Close()
	// Drain one segment at a time so a slow full reread can renew the
	// caller's fenced lease without an unbounded background transaction.
	for _, segment := range m.Segments {
		if _, err = io.CopyN(io.Discard, r, int64(segment.RawBytes)); err != nil {
			return err
		}
		if checkpoint != nil {
			if err = checkpoint(segment); err != nil {
				return err
			}
		}
	}
	if _, err = io.Copy(io.Discard, r); err != nil {
		return err
	}
	if !r.Verified() {
		return ErrIntegrity
	}
	return nil
}
