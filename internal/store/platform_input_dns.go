package store

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
	"unicode/utf8"

	"fugue/internal/platformconfig"
)

const compilerDNSCacheBytes = 4 << 20
const compilerDNSCacheEntries = 8
const compilerDNSRangeLimit = 20000

// CompilerDNSSelectionSummary permits rejecting an irrelevant, expired or
// already newer selection without transferring its replay evidence. It conveys
// no serving authority; the caller still validates each returned receipt.
type CompilerDNSSelectionSummary struct {
	NodeID, Hostname, Type    string
	HasSelection, HasEvidence bool
	CapturedAt                time.Time
}
type CompilerInputDNSSelections struct {
	IntentGeneration, PolicyGeneration string
	DNSSelections                      []platformconfig.DNSSelectionObservation
	Omitted                            []CompilerDNSSelectionSummary
}
type compilerDNSRange struct {
	Summary        CompilerDNSSelectionSummary
	Offset, Length int64
}
type compilerDNSInput struct {
	Manifest       inputManifest
	Intent, Policy string
	Hashes         [][sha256.Size]byte
	Ranges         []compilerDNSRange
	Bytes          int
}
type compilerDNSInputCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	lru     list.List
	bytes   int
}

func (c *compilerDNSInputCache) get(digest string) *compilerDNSInput {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[digest]; e != nil {
		c.lru.MoveToFront(e)
		return e.Value.(*compilerDNSInput)
	}
	return nil
}
func (c *compilerDNSInputCache) put(v *compilerDNSInput) {
	if v.Bytes > compilerDNSCacheBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]*list.Element{}
	}
	if e := c.entries[v.Manifest.Digest]; e != nil {
		c.lru.MoveToFront(e)
		return
	}
	for len(c.entries) >= compilerDNSCacheEntries || c.bytes+v.Bytes > compilerDNSCacheBytes {
		e := c.lru.Back()
		old := e.Value.(*compilerDNSInput)
		delete(c.entries, old.Manifest.Digest)
		c.bytes -= old.Bytes
		c.lru.Remove(e)
	}
	c.entries[v.Manifest.Digest] = c.lru.PushFront(v)
	c.bytes += v.Bytes
}
func compilerDNSSelectionSummary(f platformconfig.DNSSelectionObservation) CompilerDNSSelectionSummary {
	s := CompilerDNSSelectionSummary{NodeID: f.NodeID, Hostname: f.Hostname, Type: f.Type, HasSelection: f.PhysicalSelection != nil, HasEvidence: len(f.PhysicalEvidence) > 0}
	if f.PhysicalSelection != nil {
		s.CapturedAt = f.PhysicalSelection.CapturedAt
	}
	return s
}
func (w *inputChunkWriter) ObserveRuntimeSnapshotDNSSelection(offset, length int64, f platformconfig.DNSSelectionObservation) {
	if w.dnsRangesOverflow {
		return
	}
	if len(w.dnsRanges) >= compilerDNSRangeLimit || !utf8.ValidString(f.NodeID) || !utf8.ValidString(f.Hostname) || !utf8.ValidString(f.Type) {
		w.dnsRanges = nil
		w.dnsRangesOverflow = true
		return
	}
	w.dnsRanges = append(w.dnsRanges, compilerDNSRange{Summary: compilerDNSSelectionSummary(f), Offset: offset, Length: length})
}
func (s *Store) rememberCompilerDNSInput(snapshot platformconfig.RuntimeSnapshot, m inputManifest, w *inputChunkWriter) {
	if !utf8.ValidString(snapshot.IntentGeneration) || !utf8.ValidString(snapshot.PolicyGeneration) || w.dnsRangesOverflow || w.count != m.Count || w.size != m.Size || "sha256:"+hex.EncodeToString(w.hash.Sum(nil)) != m.Digest {
		return
	}
	entry := &compilerDNSInput{Manifest: m, Intent: snapshot.IntentGeneration, Policy: snapshot.PolicyGeneration,
		Hashes: append([][sha256.Size]byte(nil), w.expected...), Ranges: append([]compilerDNSRange(nil), w.dnsRanges...)}
	entry.Bytes = 512 + len(entry.Intent) + len(entry.Policy) + 32*len(entry.Hashes)
	for _, r := range entry.Ranges {
		entry.Bytes += 256 + len(r.Summary.NodeID) + len(r.Summary.Hostname) + len(r.Summary.Type)
	}
	s.compilerDNSInputs.put(entry)
}

// GetPlatformCompilerInputDNSSelections verifies every current database chunk,
// but returns bodies only for the requested observations. The bounded cache
// holds byte positions and hashes, never receipt bodies or validation results.
// Cold/legacy inputs retain the full verified read path.
func (s *Store) GetPlatformCompilerInputDNSSelections(digest string, needed func(CompilerDNSSelectionSummary) bool) (CompilerInputDNSSelections, error) {
	if !inputDigestPattern.MatchString(digest) {
		return CompilerInputDNSSelections{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		m, err := readInputManifest(ctx, s.db, digest)
		if err != nil && !absentInputManifest(err) {
			return CompilerInputDNSSelections{}, err
		}
		if err == nil {
			if cached := s.compilerDNSInputs.get(digest); cached != nil && cached.Manifest.Format == m.Format && cached.Manifest.Size == m.Size && cached.Manifest.Count == m.Count && cached.Manifest.ChunkSize == m.ChunkSize {
				return s.readCompilerDNSSelections(ctx, m, cached, needed)
			}
		}
	}
	snapshot, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		return CompilerInputDNSSelections{}, err
	}
	// A cold reader builds an index only when canonical bytes reproduce the
	// verified digest. Noncanonical legacy representations remain readable.
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		m, err := readInputManifest(ctx, s.db, digest)
		cancel()
		if err == nil && m.Count <= inputMaxBytes/inputChunkBytes {
			w := &inputChunkWriter{digest: digest, hash: sha256.New(), chunk: make([]byte, 0, m.ChunkSize)}
			if platformconfig.WriteRuntimeSnapshot(w, snapshot) == nil && w.flush() == nil {
				s.rememberCompilerDNSInput(snapshot, m, w)
			}
		}
	}
	out := CompilerInputDNSSelections{IntentGeneration: snapshot.IntentGeneration, PolicyGeneration: snapshot.PolicyGeneration}
	for _, f := range snapshot.DNSSelections {
		summary := compilerDNSSelectionSummary(f)
		if needed == nil || needed(summary) {
			out.DNSSelections = append(out.DNSSelections, f)
		} else {
			out.Omitted = append(out.Omitted, summary)
		}
	}
	return out, nil
}

const compilerDNSSelectiveReadSQL = `SELECT ordinal,chunk_hash,octet_length(data),sha256(data),CASE WHEN ordinal = ANY($2::int[]) THEN data ELSE NULL END FROM fugue_platform_input_chunks WHERE content_hash=$1 ORDER BY ordinal`

func (s *Store) readCompilerDNSSelections(ctx context.Context, m inputManifest, c *compilerDNSInput, needed func(CompilerDNSSelectionSummary) bool) (CompilerInputDNSSelections, error) {
	out := CompilerInputDNSSelections{IntentGeneration: c.Intent, PolicyGeneration: c.Policy}
	selected := make([]bool, m.Count)
	ordinals := []int32{}
	ranges := []compilerDNSRange{}
	for _, r := range c.Ranges {
		if needed != nil && !needed(r.Summary) {
			out.Omitted = append(out.Omitted, r.Summary)
			continue
		}
		if r.Offset < 0 || r.Length <= 0 || r.Offset+r.Length > m.Size {
			return CompilerInputDNSSelections{}, ErrConflict
		}
		ranges = append(ranges, r)
		for i := int(r.Offset / int64(m.ChunkSize)); i <= int((r.Offset+r.Length-1)/int64(m.ChunkSize)); i++ {
			if !selected[i] {
				selected[i] = true
				ordinals = append(ordinals, int32(i))
			}
		}
	}
	rows, err := s.db.QueryContext(ctx, compilerDNSSelectiveReadSQL, m.Digest, ordinals)
	if err != nil {
		return CompilerInputDNSSelections{}, err
	}
	defer rows.Close()
	chunks := make([][]byte, m.Count)
	count := 0
	for rows.Next() {
		var ordinal, size int
		var declared string
		var actual, raw []byte
		if err := rows.Scan(&ordinal, &declared, &size, &actual, &raw); err != nil {
			return CompilerInputDNSSelections{}, err
		}
		want := m.ChunkSize
		if count == m.Count-1 {
			want = int(m.Size - int64(count)*int64(m.ChunkSize))
		}
		if count >= m.Count || ordinal != count || size != want || !bytes.Equal(actual, c.Hashes[count][:]) || declared != "sha256:"+hex.EncodeToString(c.Hashes[count][:]) {
			return CompilerInputDNSSelections{}, fmt.Errorf("compiler DNS input chunk identity/size/hash differs: %w", ErrConflict)
		}
		if selected[count] {
			h := sha256.Sum256(raw)
			if len(raw) != want || h != c.Hashes[count] {
				return CompilerInputDNSSelections{}, ErrConflict
			}
			chunks[count] = raw
		} else if raw != nil {
			return CompilerInputDNSSelections{}, ErrConflict
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return CompilerInputDNSSelections{}, err
	}
	if count != m.Count {
		return CompilerInputDNSSelections{}, ErrConflict
	}
	// Decode only after *all* selected and unselected bytes have been verified.
	for _, r := range ranges {
		raw := make([]byte, 0, r.Length)
		for offset, end := r.Offset, r.Offset+r.Length; offset < end; {
			ordinal := int(offset / int64(m.ChunkSize))
			start := int(offset % int64(m.ChunkSize))
			n := min(int64(len(chunks[ordinal])-start), end-offset)
			if n <= 0 {
				return CompilerInputDNSSelections{}, ErrConflict
			}
			raw = append(raw, chunks[ordinal][start:start+int(n)]...)
			offset += n
		}
		var f platformconfig.DNSSelectionObservation
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&f) != nil || decoder.Decode(new(any)) != io.EOF || !sameCompilerDNSSelectionSummary(compilerDNSSelectionSummary(f), r.Summary) {
			return CompilerInputDNSSelections{}, ErrConflict
		}
		out.DNSSelections = append(out.DNSSelections, f)
	}
	return out, nil
}

func sameCompilerDNSSelectionSummary(a, b CompilerDNSSelectionSummary) bool {
	return a.NodeID == b.NodeID && a.Hostname == b.Hostname && a.Type == b.Type && a.HasSelection == b.HasSelection && a.HasEvidence == b.HasEvidence && a.CapturedAt.Equal(b.CapturedAt)
}
