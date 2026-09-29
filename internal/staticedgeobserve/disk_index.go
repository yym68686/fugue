package staticedgeobserve

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// The index owns only scalar metadata and offsets, never event arrays or bodies.
// Hitting this bound disables the fast path for that segment until rotation.
const maxDiskIndexRecords = 32768

type diskRecordRef struct {
	key, requestID, applicationID string
	sequence                      uint64
	observed                      time.Time
	waitMS                        float64
	offset                        int64
	length                        int
	previous                      [3]diskRecordLocation
	previousCount                 int
	discarded                     bool
	discardedFirst, discardedLast time.Time
	discardedSequence             uint64
}

type diskRecordLocation struct {
	sequence      uint64
	applicationID string
	observed      time.Time
	waitMS        float64
	offset        int64
	length        int
}

func (r diskRecordRef) location() diskRecordLocation {
	return diskRecordLocation{r.sequence, r.applicationID, r.observed, r.waitMS, r.offset, r.length}
}
func (r *diskRecordRef) setLocation(v diskRecordLocation) {
	r.sequence, r.observed, r.waitMS, r.offset, r.length = v.sequence, v.observed, v.waitMS, v.offset, v.length
	r.applicationID = v.applicationID
}
func (r *diskRecordRef) discard(v diskRecordLocation) {
	if !r.discarded || v.observed.Before(r.discardedFirst) {
		r.discardedFirst = v.observed
	}
	if !r.discarded || v.observed.After(r.discardedLast) {
		r.discardedLast = v.observed
	}
	if v.sequence > r.discardedSequence {
		r.discardedSequence = v.sequence
	}
	r.discarded = true
}

type diskSegmentIndex struct {
	info      os.FileInfo
	clean     bool
	records   []diskRecordRef
	positions map[string]int
}

type diskIndexSnapshot struct {
	path    string
	segment *diskSegmentIndex // identity detects reuse of a ring slot
	info    os.FileInfo
	refs    []diskRecordRef
}

func readWaitMS(r Record) float64 {
	wait := r.Body.ReadBlockMS
	if r.Body.ReadPendingSinceMS != nil {
		wait += r.ElapsedMS - *r.Body.ReadPendingSinceMS
	}
	return wait
}

func (s *Store) resetDiskIndex(path string, info os.FileInfo) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if old := s.diskIndex[path]; old != nil {
		s.indexRecords -= len(old.records)
	}
	s.diskIndex[path] = &diskSegmentIndex{info: info, clean: true, positions: make(map[string]int)}
}

func (s *Store) forgetDiskIndex(path string) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if old := s.diskIndex[path]; old != nil {
		s.indexRecords -= len(old.records)
		delete(s.diskIndex, path)
	}
}

func (s *Store) invalidateDiskIndex(path string) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if seg := s.diskIndex[path]; seg != nil {
		seg.clean = false
	}
}

func (s *Store) indexRecord(path string, r Record, offset int64, length int, info os.FileInfo) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	seg := s.diskIndex[path]
	if seg == nil {
		return
	}
	if info != nil {
		seg.info = info
	}
	if !seg.clean {
		return
	}
	ref := diskRecordRef{key: recordKey(r), requestID: r.RequestID,
		applicationID: r.ApplicationRequestID, sequence: r.Sequence, observed: r.ObservedAt,
		waitMS: readWaitMS(r), offset: offset, length: length}
	if position, ok := seg.positions[ref.key]; ok {
		old := seg.records[position]
		if old.requestID != ref.requestID ||
			(old.applicationID != "" && old.applicationID != ref.applicationID) ||
			(old.sequence >= ref.sequence && old.applicationID == "" && ref.applicationID != "") {
			seg.clean = false
			return
		}
		locations := [5]diskRecordLocation{old.location()}
		count := 1 + old.previousCount
		copy(locations[1:], old.previous[:old.previousCount])
		for i := 0; i < count; i++ {
			if locations[i].sequence == ref.sequence {
				if !locations[i].observed.Equal(ref.observed) || locations[i].waitMS != ref.waitMS || locations[i].applicationID != ref.applicationID {
					seg.clean = false
				}
				return
			}
		}
		locations[count] = ref.location()
		count++
		sort.Slice(locations[:count], func(i, j int) bool { return locations[i].sequence > locations[j].sequence })
		if count > 4 {
			old.discard(locations[4])
			count = 4
		}
		old.setLocation(locations[0])
		old.previousCount = count - 1
		copy(old.previous[:], locations[1:count])
		seg.records[position] = old
		return
	}
	if s.indexRecords >= maxDiskIndexRecords {
		seg.clean = false
		return
	}
	seg.positions[ref.key] = len(seg.records)
	seg.records = append(seg.records, ref)
	s.indexRecords++
}

func sameIndexedFile(want, got os.FileInfo) bool {
	return want != nil && got != nil && got.Mode().IsRegular() && os.SameFile(want, got) && want.Size() == got.Size() && want.ModTime().Equal(got.ModTime())
}

// indexedDiskQuery snapshots at most 8 segment indexes. Appends after this
// snapshot remain available to the next query; unknown file changes fall back
// to bounded validated scanning. No query lock is acquired by the disk writer.
func (s *Store) indexedDiskQuery(q Query, files []string, deadline time.Time, consider func(Record), out *Result) bool {
	s.indexMu.Lock()
	snapshots := make([]diskIndexSnapshot, 0, len(files))
	usable := true
	for _, path := range files {
		seg := s.diskIndex[path]
		if seg == nil || !seg.clean || seg.info == nil {
			usable = false
			break
		}
		snapshots = append(snapshots, diskIndexSnapshot{path: path, segment: seg, info: seg.info, refs: append([]diskRecordRef(nil), seg.records...)})
	}
	s.indexMu.Unlock()
	if !usable {
		return false
	}
	// Verify the index covers the files actually present, including same-size
	// replacement/edits. Never skip unvalidated externally appended lines.
	for _, snap := range snapshots {
		info, err := os.Lstat(snap.path)
		if err != nil || !sameIndexedFile(snap.info, info) {
			return false
		}
	}
	type selected struct {
		ref     diskRecordRef
		segment int
	}
	latest := make(map[string]selected)
	cutoff := time.Now().Add(-time.Duration(s.cfg.RetentionSeconds) * time.Second)
	for i, snap := range snapshots {
		for _, ref := range snap.refs {
			if time.Now().After(deadline) {
				out.DiskScanComplete = false
				return true
			}
			if q.RequestID != "" && ref.requestID != q.RequestID && ref.applicationID != q.RequestID {
				continue
			}
			locations := [4]diskRecordLocation{ref.location()}
			copy(locations[1:], ref.previous[:ref.previousCount])
			found := false
			for _, loc := range locations[:1+ref.previousCount] {
				if q.RequestID != "" && q.RequestID != ref.requestID && q.RequestID != loc.applicationID {
					continue
				}
				if !loc.observed.Before(cutoff) && !loc.observed.Before(q.Since) && !loc.observed.After(q.Until) {
					ref.setLocation(loc)
					found = true
					break
				}
			}
			// The latest four sequence locations cover normal live queries,
			// including snapshots arriving while the RPC is in flight. Older
			// historical windows conservatively use the validated scan.
			if ref.discarded && !ref.discardedFirst.After(q.Until) && !ref.discardedLast.Before(q.Since) && !ref.discardedLast.Before(cutoff) && (!found || ref.sequence <= ref.discardedSequence) {
				return false
			}
			if !found {
				continue
			}
			if old, ok := latest[ref.key]; ok && old.ref.sequence >= ref.sequence {
				continue
			}
			latest[ref.key] = selected{ref: ref, segment: i}
		}
	}
	chosen := make([]selected, 0, len(latest))
	for _, v := range latest {
		if v.ref.waitMS >= q.MinReadMS {
			chosen = append(chosen, v)
		}
	}
	sort.Slice(chosen, func(i, j int) bool { return chosen[i].ref.observed.After(chosen[j].ref.observed) })
	if len(chosen) > 200 {
		out.Truncated = true
		chosen = chosen[:200]
	}
	handles := make(map[int]*os.File)
	defer func() {
		for _, f := range handles {
			f.Close()
		}
	}()
	for _, v := range chosen {
		if time.Now().After(deadline) {
			out.DiskScanComplete = false
			break
		}
		snap := snapshots[v.segment]
		f := handles[v.segment]
		if f == nil {
			var err error
			f, err = os.Open(snap.path)
			if err != nil {
				out.DiskScanComplete = false
				continue
			}
			info, err := f.Stat()
			if err != nil || !sameIndexedFile(snap.info, info) {
				f.Close()
				out.DiskScanComplete = false
				continue
			}
			handles[v.segment] = f
		}
		raw := make([]byte, v.ref.length)
		if _, err := f.ReadAt(raw, v.ref.offset); err != nil {
			out.DiskScanComplete = false
			continue
		}
		var r Record
		if json.Unmarshal(raw, &r) != nil || r.Validate() != nil || r.NodeID != s.cfg.NodeID || recordKey(r) != v.ref.key || r.Sequence != v.ref.sequence || r.RequestID != v.ref.requestID || r.ApplicationRequestID != v.ref.applicationID || !r.ObservedAt.Equal(v.ref.observed) || readWaitMS(r) != v.ref.waitMS {
			out.DiskScanComplete = false
			continue
		}
		consider(r)
	}
	for _, snap := range snapshots {
		info, err := os.Lstat(snap.path)
		s.indexMu.Lock()
		current := s.diskIndex[snap.path]
		valid := err == nil && current == snap.segment && current.clean && current.info.Size() >= snap.info.Size() && sameIndexedFile(current.info, info)
		s.indexMu.Unlock()
		if !valid {
			out.DiskScanComplete = false
		}
	}
	// A new/removed segment changes the captured file set, so do not assert
	// completeness through rotation even if every selected record was readable.
	after, err := filepath.Glob(filepath.Join(s.cfg.Directory, "observations-*.jsonl"))
	if err != nil || len(after) != len(files) {
		out.DiskScanComplete = false
	} else {
		for i := range after {
			if after[i] != files[i] {
				out.DiskScanComplete = false
				break
			}
		}
	}
	return true
}
