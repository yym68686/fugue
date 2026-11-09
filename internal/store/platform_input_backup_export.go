package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"io"
	"sort"
	"time"

	"fugue/internal/schemamigrate"
)

// RunCompilerInputBackup exports one exact separated input under the normal
// reader barrier. It does not migrate, classify, route, pin or sign anything.
// Pack decoding and the complete original SHA are checked by the offline
// decoder; a successful export alone is not a restore/readback receipt.
// On failure the caller discards/closes the stream. A blocked receiver write
// can finish after cancellation, without retaining the database reader lease.
func RunCompilerInputBackup(ctx context.Context, databaseURL string, c CompilerInputBackupContract, out io.Writer) (CompilerInputBackupReport, error) {
	r := CompilerInputBackupReport{Schema: CompilerInputBackupSchema, Digest: c.Digest, ReadOnly: true}
	if c.Validate() != nil || out == nil {
		return r, ErrInvalidInput
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return r, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return runCompilerInputBackup(ctx, db, c, out)
}

func runCompilerInputBackup(ctx context.Context, db *sql.DB, c CompilerInputBackupContract, out io.Writer) (report CompilerInputBackupReport, result error) {
	report = CompilerInputBackupReport{Schema: CompilerInputBackupSchema, Digest: c.Digest, ReadOnly: true}
	if c.Validate() != nil || out == nil {
		return report, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	s := &Store{db: db, compilerInputArchiveReadLeaseEnabled: true}
	tx, finish, err := s.BeginCompilerInputReadLease(ctx)
	if err != nil {
		return report, err
	}
	defer finish()
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return report, err
		}
	}
	if err = schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return report, err
	}
	var coherent bool
	if err = tx.QueryRowContext(ctx, `SELECT transaction_timestamp(),pg_current_snapshot()::text,current_setting('transaction_isolation')='repeatable read' AND current_setting('transaction_read_only')='on'`).Scan(&report.ObservedAt, &report.Snapshot, &coherent); err != nil {
		return report, err
	}
	if !coherent {
		return report, ErrConflict
	}
	m, err := readInputManifest(ctx, tx, c.Digest)
	if err != nil {
		return report, err
	}
	if m.Format != inputFormatSeparatedV4 || m.Size != c.ExpectedLogicalBytes {
		return report, ErrConflict
	}
	h := inputBackupHeader{Schema: c.Schema, Digest: c.Digest, LogicalBytes: m.Size, CreatedAt: m.Created}
	var layoutHash []byte
	if err = tx.QueryRowContext(ctx, `SELECT frame_hash,layout_hash,layout FROM fugue_platform_input_separated_roots WHERE content_hash=$1 AND octet_length(layout)<=$2`, c.Digest, 1+41*separatedPartLimit).Scan(&h.FrameDigest, &layoutHash, &h.Layout); err != nil {
		return report, err
	}
	h.LayoutHash = sharedHashBytes(layoutHash)
	if !inputDigestPattern.MatchString(h.FrameDigest) || h.LayoutHash != sharedHash(sha256.Sum256(h.Layout)) {
		return report, ErrConflict
	}
	parts, err := decodeSeparatedLayout(h.Layout)
	if err != nil {
		return report, err
	}
	wanted := map[string]bool{h.FrameDigest: true}
	var logical int64
	for _, p := range parts {
		logical += int64(p.Size)
		if p.Evidence {
			wanted[p.Digest] = true
		}
	}
	if logical != m.Size {
		return report, ErrConflict
	}
	// The exact root's references are bounded independently of stream output.
	rows, err := tx.QueryContext(ctx, `SELECT c.content_hash,c.size_bytes,c.chunk_count,c.created_at FROM fugue_platform_input_separated_refs r JOIN fugue_platform_input_components c ON c.content_hash=r.component_hash WHERE r.content_hash=$1 LIMIT 40003`, c.Digest)
	if err != nil {
		return report, err
	}
	manifests := map[string]inputManifest{}
	for rows.Next() {
		v := inputManifest{Format: inputFormatSharedV3, ChunkSize: inputChunkBytes}
		if err = rows.Scan(&v.Digest, &v.Size, &v.Count, &v.Created); err != nil {
			break
		}
		if !wanted[v.Digest] || manifests[v.Digest].Digest != "" || v.Size < 1 || v.Size > 128<<20 || v.Count != int((v.Size+inputChunkBytes-1)/inputChunkBytes) || v.Created.IsZero() || len(manifests) >= separatedPartLimit+1 {
			err = ErrConflict
			break
		}
		manifests[v.Digest] = v
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return report, err
	}
	if len(manifests) != len(wanted) || manifests[h.FrameDigest].Size > separatedFrameLimit {
		return report, ErrConflict
	}
	for _, p := range parts {
		if p.Evidence && manifests[p.Digest].Size != int64(p.Size) || !p.Evidence && uint64(p.Offset)+uint64(p.Size) > uint64(manifests[h.FrameDigest].Size) {
			return report, ErrConflict
		}
	}
	enc := &inputBackupEncoder{ctx: ctx, out: out, max: c.MaxEncodedBytes, hash: sha256.New()}
	defer func() { report.EncodedBytes = enc.bytes }()
	if err = enc.record(inputBackupRecord{Kind: "header", Header: &h}); err != nil {
		return report, err
	}
	keys := make([]string, 0, len(manifests))
	for key := range manifests {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	packs := map[int64]sharedPackWitness{}
	objects := map[int64]sharedLocation{}
	for start := 0; start < len(keys); {
		batch := map[string]inputManifest{}
		var bytes int64
		end := start
		for end < len(keys) && end-start < 16 {
			v := manifests[keys[end]]
			if end > start && bytes+v.Size > 4<<20 {
				break
			}
			batch[v.Digest], bytes = v, bytes+v.Size
			end++
		}
		readers, e := openComponentBatch(ctx, tx, batch)
		if e != nil {
			return report, e
		}
		for _, key := range keys[start:end] {
			r := readers[key]
			v := inputBackupComponent{Digest: key, Size: r.m.Size, CreatedAt: r.m.Created, Recipes: r.recipes}
			ids := make([]int64, 0, len(r.locations))
			for id := range r.locations {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			for _, id := range ids {
				o := r.locations[id]
				if prev, ok := objects[id]; ok && prev != o {
					return report, ErrConflict
				}
				objects[id] = o
				if len(objects) > sharedObjectLimit {
					return report, errSharedInputBudget
				}
				v.Objects = append(v.Objects, inputBackupObject{ID: id, Hash: sharedHash(o.Hash), PackID: o.PackID, Size: o.Size, Offset: o.Offset})
			}
			ids = ids[:0]
			for id := range r.packs {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			for _, id := range ids {
				p := r.packs[id]
				if prev, ok := packs[id]; ok && prev != p {
					return report, ErrConflict
				}
				packs[id] = p
				v.Packs = append(v.Packs, inputBackupPackWitness{ID: id, Hash: sharedHash(p.Hash), StoredHash: sharedHash(p.StoredHash), Size: p.Size, StoredSize: p.StoredSize})
			}
			if err = enc.record(inputBackupRecord{Kind: "component", Component: &v}); err != nil {
				return report, err
			}
		}
		start = end
	}
	ids := make([]int64, 0, len(packs))
	var payload int64
	for id, w := range packs {
		ids = append(ids, id)
		payload += int64((w.StoredSize+2)/3*4 + 128)
	}
	// Refuse before reading pack bodies if even their framed payload cannot fit.
	if payload+512 > enc.max-enc.bytes {
		return report, errSharedInputBudget
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for start := 0; start < len(ids); start += 16 {
		batch := ids[start:min(start+16, len(ids))]
		rows, err = tx.QueryContext(ctx, `SELECT pack_id,pack_hash,size_bytes,stored_hash,data FROM fugue_platform_input_shared_packs WHERE pack_id=ANY($1::bigint[]) AND octet_length(data)<=262144`, batch)
		if err != nil {
			return report, err
		}
		seen := map[int64]bool{}
		for rows.Next() {
			var id int64
			var hash, stored, data []byte
			var size int
			if err = rows.Scan(&id, &hash, &size, &stored, &data); err != nil {
				break
			}
			w, ok := packs[id]
			if !ok || seen[id] || len(hash) != 32 || len(stored) != 32 || sharedHashBytes(hash) != sharedHash(w.Hash) || sharedHashBytes(stored) != sharedHash(w.StoredHash) || size != w.Size || len(data) != w.StoredSize || sha256.Sum256(data) != w.StoredHash {
				err = ErrConflict
				break
			}
			seen[id] = true
			if err = enc.record(inputBackupRecord{Kind: "pack", Pack: &inputBackupPack{ID: id, Data: data}}); err != nil {
				break
			}
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return report, err
		}
		if len(seen) != len(batch) {
			return report, ErrConflict
		}
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	if err = enc.finish(); err != nil {
		return report, err
	}
	report.LogicalBytes, report.Components, report.Packs = m.Size, len(manifests), len(packs)
	report.Complete = true
	return report, nil
}
