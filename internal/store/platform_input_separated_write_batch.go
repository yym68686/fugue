package store

import "time"

// flushEvidence shares metadata round trips across at most sixteen evidence
// bodies / four MiB. One oversized value can occupy its own batch, subject to
// the existing per-input bound. The final stored readback verifies reused and
// new components equally; a metadata hit never replaces body verification.
func (w *separatedInputWriter) flushEvidence() error {
	if len(w.pendingEvidence) == 0 {
		return nil
	}
	keys := make([]string, 0, len(w.pendingEvidence))
	for key := range w.pendingEvidence {
		keys = append(keys, key)
	}
	rows, err := w.tx.QueryContext(w.ctx, `SELECT content_hash,size_bytes,chunk_count,created_at FROM fugue_platform_input_components WHERE content_hash=ANY($1::text[])`, keys)
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for rows.Next() {
		var key string
		var size int64
		var count int
		var created time.Time
		if err = rows.Scan(&key, &size, &count, &created); err != nil {
			break
		}
		if raw, ok := w.pendingEvidence[key]; !ok || size != int64(len(raw)) || count != int((size+inputChunkBytes-1)/inputChunkBytes) || created.IsZero() {
			err = ErrConflict
			break
		}
		existing[key] = true
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	var added []string
	var sizes []int64
	var counts []int32
	for _, key := range keys {
		if existing[key] {
			continue
		}
		m, _, e := stageInputComponent(w.pendingEvidence[key], key, w.batch)
		if e != nil {
			return e
		}
		added = append(added, key)
		sizes = append(sizes, m.Size)
		counts = append(counts, int32(m.Count))
	}
	if len(added) > 0 {
		// Recipe references are already deferred to the same transaction's
		// commit. All component manifests exist before roots are published.
		if _, err = w.tx.ExecContext(w.ctx, `INSERT INTO fugue_platform_input_components(content_hash,size_bytes,chunk_count,created_at) SELECT h,n,c,now() FROM unnest($1::text[],$2::bigint[],$3::int[]) AS v(h,n,c)`, added, sizes, counts); err != nil {
			return err
		}
	}
	w.pendingEvidence, w.pendingBytes = nil, 0
	return nil
}
