package staticedgeobserve

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type StoreConfig struct {
	NodeID           string `json:"node_id"`
	Directory        string `json:"directory"`
	MaxRecords       int    `json:"max_records"`
	QueueSize        int    `json:"queue_size"`
	SegmentBytes     int64  `json:"segment_bytes"`
	Segments         int    `json:"segments"`
	RetentionSeconds int64  `json:"retention_seconds"`
}

func (c *StoreConfig) Validate() error {
	if !ValidID(c.NodeID) || !filepath.IsAbs(c.Directory) {
		return errors.New("valid node identity and absolute evidence directory required")
	}
	if c.MaxRecords == 0 {
		c.MaxRecords = 512
	}
	if c.QueueSize == 0 {
		c.QueueSize = 128
	}
	if c.SegmentBytes == 0 {
		c.SegmentBytes = 8 << 20
	}
	if c.Segments == 0 {
		c.Segments = 8
	}
	if c.RetentionSeconds == 0 {
		c.RetentionSeconds = 86400
	}
	if c.MaxRecords < 1 || c.MaxRecords > 1024 || c.QueueSize < 1 || c.QueueSize > 256 || c.SegmentBytes < MaxRecordBytes || c.SegmentBytes > 8<<20 || c.Segments < 2 || c.Segments > 8 || c.RetentionSeconds < 60 || c.RetentionSeconds > 7*86400 {
		return errors.New("observation storage limits out of range")
	}
	return nil
}

type Store struct {
	cfg        StoreConfig
	mu         sync.RWMutex
	latest     map[string]Record
	queue      chan Record
	dropped    atomic.Uint64
	diskErrors atomic.Uint64
	evicted    atomic.Uint64
	loaded     atomic.Bool
	file       *os.File
	size       int64
	segment    int
	closed     atomic.Bool
	lock       *os.File
	opened     time.Time
	queryMu    sync.Mutex
	Remote     *RemoteSink // Set before Run; optional and independent.
}

func NewStore(cfg StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
		return nil, err
	}
	st, err := os.Lstat(cfg.Directory)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("evidence directory must be private and not a symlink")
	}
	s := &Store{cfg: cfg, latest: make(map[string]Record), queue: make(chan Record, cfg.QueueSize)}
	lockPath := filepath.Join(cfg.Directory, ".collector.lock")
	if info, err := os.Lstat(lockPath); err == nil && !info.Mode().IsRegular() {
		return nil, errors.New("invalid evidence lock")
	}
	s.lock, err = os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = lockEvidence(s.lock); err != nil {
		s.lock.Close()
		return nil, errors.New("evidence directory already owned by another collector")
	}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()
	// Recovery is bounded by the configured disk cap. Oversized or malformed
	// segments are never accepted as complete evidence.
	files, err := filepath.Glob(filepath.Join(cfg.Directory, "observations-*.jsonl"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	if len(files) > cfg.Segments {
		return nil, errors.New("unexpected evidence segment count")
	}
	var newest time.Time
	for _, p := range files {
		info, e := os.Lstat(p)
		if e != nil || !info.Mode().IsRegular() || info.Size() > cfg.SegmentBytes {
			return nil, errors.New("invalid evidence segment")
		}
		var index int
		if _, e = fmt.Sscanf(filepath.Base(p), "observations-%02d.jsonl", &index); e != nil || index < 0 || index >= cfg.Segments {
			return nil, errors.New("invalid evidence segment index")
		}
		if info.ModTime().After(newest) {
			newest = info.ModTime()
			s.segment = index
		}
		if time.Since(info.ModTime()) > time.Duration(cfg.RetentionSeconds)*time.Second {
			if e = os.Remove(p); e != nil {
				return nil, e
			}
			continue
		}
		f, e := os.Open(p)
		if e != nil {
			return nil, e
		}
		scanner := bufio.NewScanner(io.LimitReader(f, cfg.SegmentBytes))
		scanner.Buffer(make([]byte, 4096), MaxRecordBytes)
		for scanner.Scan() {
			var r Record
			if json.Unmarshal(scanner.Bytes(), &r) != nil || r.Validate() != nil || r.NodeID != cfg.NodeID {
				s.dropped.Add(1)
				continue
			}
			s.insert(r)
		}
		if scanner.Err() != nil {
			s.dropped.Add(1)
		}
		f.Close()
	}
	// Resume the newest segment; a restart must never truncate recent evidence.
	if !newest.IsZero() {
		p := filepath.Join(cfg.Directory, fmt.Sprintf("observations-%02d.jsonl", s.segment))
		s.file, err = os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
		info, e := s.file.Stat()
		if e != nil {
			return nil, e
		}
		s.size = info.Size()
		s.opened = newest
	}
	s.loaded.Store(true)
	ok = true
	return s, nil
}

// Close releases the exclusive directory lock after the sole writer stops.
func (s *Store) Close() {
	s.closed.Store(true)
	if s.file != nil {
		s.file.Sync()
		s.file.Close()
		s.file = nil
	}
	if s.lock != nil {
		s.lock.Close()
		s.lock = nil
	}
}

func recordKey(r Record) string { return r.ProcessID + ":" + r.SpanID }
func (s *Store) insert(r Record) {
	if time.Since(r.ObservedAt) > time.Duration(s.cfg.RetentionSeconds)*time.Second {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := recordKey(r)
	if prev, ok := s.latest[key]; ok && prev.Sequence >= r.Sequence {
		return
	}
	if _, ok := s.latest[key]; !ok && len(s.latest) >= s.cfg.MaxRecords {
		oldKey := ""
		var old time.Time
		for k, v := range s.latest {
			if oldKey == "" || v.ObservedAt.Before(old) {
				oldKey = k
				old = v.ObservedAt
			}
		}
		delete(s.latest, oldKey)
		s.evicted.Add(1)
	}
	s.latest[key] = r
}

func (s *Store) Submit(r Record) bool {
	if s.closed.Load() || r.NodeID != s.cfg.NodeID || r.Validate() != nil {
		s.dropped.Add(1)
		return false
	}
	// The submitter must own a snapshot; make the bounded slice ownership explicit.
	r.Events = append([]Event(nil), r.Events...)
	select {
	case s.queue <- r:
		return true
	default:
		s.dropped.Add(1)
		return false
	}
}

func (s *Store) write(r Record) {
	s.insert(r)
	if s.Remote != nil {
		s.Remote.Submit(r)
	}
	raw, e := json.Marshal(r)
	if e != nil || len(raw)+1 > MaxRecordBytes {
		s.dropped.Add(1)
		return
	}
	raw = append(raw, '\n')
	if s.file == nil || s.size+int64(len(raw)) > s.cfg.SegmentBytes || time.Since(s.opened) > time.Hour {
		if s.file != nil {
			s.file.Close()
			s.segment++
		}
		p := filepath.Join(s.cfg.Directory, fmt.Sprintf("observations-%02d.jsonl", s.segment%s.cfg.Segments))
		// Refuse links before opening; the directory is dedicated and 0700.
		if st, e := os.Lstat(p); e == nil && !st.Mode().IsRegular() {
			s.diskErrors.Add(1)
			return
		}
		s.file, e = os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if e != nil {
			s.diskErrors.Add(1)
			return
		}
		s.size = 0
		s.opened = time.Now()
	}
	n, e := s.file.Write(raw)
	s.size += int64(n)
	if e != nil || n != len(raw) {
		s.diskErrors.Add(1)
		s.file.Close()
		s.file = nil
		s.segment++ // Preserve the failed segment; never truncate it on retry.
	}
}

func (s *Store) Run(ctx context.Context) {
	defer s.Close()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case r := <-s.queue:
			s.write(r)
		case <-ticker.C:
			s.mu.Lock()
			for k, r := range s.latest {
				if time.Since(r.ObservedAt) > time.Duration(s.cfg.RetentionSeconds)*time.Second {
					delete(s.latest, k)
				}
			}
			s.mu.Unlock()
			// Segments rotate at least hourly. Expiry is bounded by retention
			// plus one segment interval; queries enforce record-level TTL.
			files, _ := filepath.Glob(filepath.Join(s.cfg.Directory, "observations-*.jsonl"))
			for _, p := range files {
				if s.file != nil && p == s.file.Name() {
					continue
				}
				if info, e := os.Lstat(p); e == nil && info.Mode().IsRegular() && time.Since(info.ModTime()) > time.Duration(s.cfg.RetentionSeconds)*time.Second {
					if os.Remove(p) != nil {
						s.diskErrors.Add(1)
					}
				}
			}
			if s.file != nil && time.Since(s.opened) > time.Hour {
				s.file.Close()
				s.file = nil
				s.segment++
			}
		case <-ctx.Done():
			// A bounded queue is drained by the collector, never the serving process.
			for {
				select {
				case r := <-s.queue:
					s.write(r)
				default:
					return
				}
			}
		}
	}
}

func (s *Store) Query(q Query) (Result, error) {
	if e := q.Validate(); e != nil {
		return Result{}, e
	}
	out := Result{Schema: Schema, NodeID: s.cfg.NodeID, Records: []Record{}, Findings: []Finding{}, Status: "available", RecordsDropped: s.dropped.Load(), RetentionSeconds: s.cfg.RetentionSeconds, MemoryEvictions: s.evicted.Load(), DiskErrors: s.diskErrors.Load()}
	// Only one bounded disk query at a time. This mutex is never used by the
	// writer or by request serving. Overload returns incomplete, not "no wait".
	if !s.queryMu.TryLock() {
		out.Status = "storage_degraded"
		out.Truncated = true
		return out, nil
	}
	defer s.queryMu.Unlock()
	matched := make(map[string]Record)
	consider := func(r Record) {
		if time.Since(r.ObservedAt) > time.Duration(s.cfg.RetentionSeconds)*time.Second || r.ObservedAt.Before(q.Since) || r.ObservedAt.After(q.Until) {
			return
		}
		if q.RequestID != "" && r.RequestID != q.RequestID && r.ApplicationRequestID != q.RequestID {
			return
		}
		key := recordKey(r)
		if prev, ok := matched[key]; ok && prev.Sequence >= r.Sequence {
			return
		}
		// Bound memory independently of disk size. Retain newest matching
		// spans; old snapshots cannot grow a management query without limit.
		if _, ok := matched[key]; !ok && len(matched) >= 200 {
			out.Truncated = true
			oldKey := ""
			var oldest time.Time
			for k, v := range matched {
				if oldKey == "" || v.ObservedAt.Before(oldest) {
					oldKey = k
					oldest = v.ObservedAt
				}
			}
			if !r.ObservedAt.After(oldest) {
				return
			}
			delete(matched, oldKey)
		}
		matched[key] = r
	}
	deadline := time.Now().Add(1500 * time.Millisecond)
	files, _ := filepath.Glob(filepath.Join(s.cfg.Directory, "observations-*.jsonl"))
	out.DiskScanComplete = true
	if len(files) > s.cfg.Segments {
		files = files[:s.cfg.Segments]
		out.DiskScanComplete = false
	}
	for _, p := range files {
		if time.Now().After(deadline) {
			out.DiskScanComplete = false
			break
		}
		info, e := os.Lstat(p)
		if e != nil || !info.Mode().IsRegular() || info.Size() > s.cfg.SegmentBytes {
			out.DiskScanComplete = false
			continue
		}
		f, e := os.Open(p)
		if e != nil {
			out.DiskScanComplete = false
			continue
		}
		scan := bufio.NewScanner(io.LimitReader(f, s.cfg.SegmentBytes))
		scan.Buffer(make([]byte, 4096), MaxRecordBytes)
		for scan.Scan() {
			if time.Now().After(deadline) {
				out.DiskScanComplete = false
				break
			}
			var r Record
			if json.Unmarshal(scan.Bytes(), &r) != nil || r.Validate() != nil || r.NodeID != s.cfg.NodeID {
				out.DiskScanComplete = false
				continue
			}
			consider(r)
		}
		if scan.Err() != nil {
			out.DiskScanComplete = false
		}
		f.Close()
		// Rotation/truncation during a read invalidates completeness.
		if after, e := os.Stat(p); e != nil || !os.SameFile(info, after) || after.Size() < info.Size() {
			out.DiskScanComplete = false
		}
	}
	s.mu.RLock()
	for _, r := range s.latest {
		if time.Since(r.ObservedAt) > time.Duration(s.cfg.RetentionSeconds)*time.Second {
			continue
		}
		if out.OldestRetained == nil || r.ObservedAt.Before(*out.OldestRetained) {
			v := r.ObservedAt
			out.OldestRetained = &v
		}
		consider(r)
	}
	s.mu.RUnlock()
	for _, r := range matched {
		wait := r.Body.ReadBlockMS
		if r.Body.ReadPendingSinceMS != nil {
			wait += r.ElapsedMS - *r.Body.ReadPendingSinceMS
		}
		if wait < q.MinReadMS {
			continue
		}
		copy := r
		copy.Events = append([]Event(nil), r.Events...)
		out.Records = append(out.Records, copy)
	}
	if !out.DiskScanComplete {
		out.Truncated = true
	}
	sort.Slice(out.Records, func(i, j int) bool { return out.Records[i].ObservedAt.After(out.Records[j].ObservedAt) })
	if len(out.Records) > q.Limit {
		out.Truncated = true
		out.Records = out.Records[:q.Limit]
	}
	// Bound management replies independently of the retention budget.
	size := 0
	for i, r := range out.Records {
		raw, _ := json.Marshal(r)
		size += len(raw)
		if size > 2<<20 {
			out.Truncated = true
			out.Records = out.Records[:i]
			break
		}
		out.Findings = append(out.Findings, Explain(r)...)
	}
	if len(out.Records) == 0 {
		out.Status = "not_observed"
	}
	if s.diskErrors.Load() > 0 {
		out.Status = "storage_degraded"
	}
	return out, nil
}

func (s *Store) Status() map[string]any {
	s.mu.RLock()
	n := len(s.latest)
	s.mu.RUnlock()
	out := map[string]any{"schema": Schema, "node_id": s.cfg.NodeID, "records": n, "max_records": s.cfg.MaxRecords, "queue_depth": len(s.queue), "queue_capacity": cap(s.queue), "records_dropped": s.dropped.Load(), "disk_errors": s.diskErrors.Load(), "memory_evictions": s.evicted.Load(), "retention_seconds": s.cfg.RetentionSeconds, "max_disk_bytes": s.cfg.SegmentBytes * int64(s.cfg.Segments), "ready": s.loaded.Load() && !s.closed.Load(), "external_root_cause_guarantee": false}
	if s.Remote != nil {
		out["remote"] = s.Remote.Status()
	}
	return out
}

// Handler must only be mounted on a dedicated permission-restricted Unix
// socket. Public access goes through the independent manager's mTLS grants.
func (s *Store) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/status" {
			json.NewEncoder(w).Encode(s.Status())
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		var target any
		var rec Record
		var q Query
		switch r.URL.Path {
		case "/records":
			target = &rec
		case "/query":
			target = &q
		default:
			http.NotFound(w, r)
			return
		}
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxRecordBytes))
		d.DisallowUnknownFields()
		if d.Decode(target) != nil || d.Decode(new(any)) != io.EOF {
			http.Error(w, "invalid observation envelope", 400)
			return
		}
		if r.URL.Path == "/records" {
			if rec.Validate() != nil || rec.NodeID != s.cfg.NodeID {
				http.Error(w, "invalid observation record", 400)
				return
			}
			if !s.Submit(rec) {
				http.Error(w, "observation queue full", 503)
				return
			}
			w.WriteHeader(202)
			return
		}
		out, e := s.Query(q)
		if e != nil {
			http.Error(w, "invalid bounded query", 400)
			return
		}
		json.NewEncoder(w).Encode(out)
	})
}

func UnixClient(socket string, timeout time.Duration) (*http.Client, error) {
	if !filepath.IsAbs(socket) || strings.ContainsAny(socket, "\r\n\x00") {
		return nil, errors.New("absolute Unix socket required")
	}
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: timeout}).DialContext(ctx, "unix", socket)
	}, MaxIdleConns: 2, MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second}
	return &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}
