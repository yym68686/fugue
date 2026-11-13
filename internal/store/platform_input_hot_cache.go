package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

const compilerInputHotCacheBytes = 8 << 20

var errCompilerInputHotPreparationStop = errors.New("hot metadata preparation bound")

// Diagnostics only; no earlier roots, snapshots, nodes or proof hashes are
// exported as final evidence. EncodedBytes is a retained-metadata charge, not a
// measurement of Go heap usage or physical database storage.
type CompilerInputHotPreparation struct {
	DurationUS        int64                `json:"duration_us"`
	Artifacts         int                  `json:"artifacts"`
	References        int                  `json:"references"`
	EncodedBytes      int                  `json:"encoded_bytes"`
	Stopped           string               `json:"stopped"`
	SessionValidated  bool                 `json:"session_validated"`
	Hits              int                  `json:"hits"`
	Misses            int                  `json:"misses"`
	VersionQueries    int                  `json:"version_queries"`
	VersionReadUS     int64                `json:"version_read_us"`
	ReusedSourceBytes int64                `json:"reused_source_bytes"`
	Cost              CompilerInputHotCost `json:"cost"`
}

// A cache lives on the stack of one operation and one *sql.Conn. It is never
// persisted, caller supplied, shared between connections or reused by serving,
// activation or retirement. Retain derived metadata, never source JSON.
type compilerInputHotCache struct {
	session    compilerInputHotCacheSession
	entries    map[string]compilerInputHotCacheEntry
	report     CompilerInputHotPreparation
	capture    bool
	stopBefore time.Time
	tx         *sql.Tx
}

type compilerInputHotCacheSession struct {
	Backend, Database, Relation, Filenode int64
	Started                               time.Time
	Schema                                string
	Role, SearchPath                      string
	Horizon                               int64
}

type compilerInputHotCacheVersion struct {
	Xmin, CTID, Kind, Scope, Generation, Hash string
	Relation                                  int64
}

type compilerInputHotCacheEntry struct {
	version       compilerInputHotCacheVersion
	node          inputarchive.HotNode
	sourceBytes   int64
	metadataBytes int
}

func compilerInputHotCacheSessionRead(ctx context.Context, tx *sql.Tx) (s compilerInputHotCacheSession, err error) {
	// Keep the relation's physical identity fixed until this RR ends, including
	// when a concurrent TRUNCATE/rewrite would otherwise change its filenode.
	if _, err = tx.ExecContext(ctx, `LOCK TABLE fugue_platform_artifacts IN ACCESS SHARE MODE`); err != nil {
		return s, err
	}
	err = tx.QueryRowContext(ctx, `SELECT pg_backend_pid(),pg_postmaster_start_time(),
 (SELECT oid::bigint FROM pg_database WHERE datname=current_database()),current_schema(),current_user,current_setting('search_path'),
 'fugue_platform_artifacts'::regclass::oid::bigint,pg_relation_filenode('fugue_platform_artifacts'::regclass)::bigint,
 pg_snapshot_xmax(pg_current_snapshot())::text::bigint
 WHERE current_setting('transaction_isolation')='repeatable read' AND current_setting('transaction_read_only')='on'`).Scan(
		&s.Backend, &s.Started, &s.Database, &s.Schema, &s.Role, &s.SearchPath, &s.Relation, &s.Filenode, &s.Horizon)
	return s, err
}

func (s compilerInputHotCacheSession) accepts(current compilerInputHotCacheSession) bool {
	return s.Backend > 0 && s.Database > 0 && s.Relation > 0 && s.Filenode > 0 && s.Schema != "" && s.Role != "" && s.SearchPath != "" && s.Horizon >= 3 &&
		s.Backend == current.Backend && s.Started.Equal(current.Started) && s.Database == current.Database &&
		s.Schema == current.Schema && s.Relation == current.Relation && s.Filenode == current.Filenode &&
		s.Role == current.Role && s.SearchPath == current.SearchPath &&
		current.Horizon >= s.Horizon && current.Horizon-s.Horizon < 1<<31
}

// An ancient frozen tuple can retain a 32-bit xmin that appears AHEAD of the
// preparation horizon. That value could be allocated again soon after wrap.
// Only cache the preceding half-range: its next reuse is more than 2^31 XIDs
// away, beyond the final session guard. Reject special/future/ambiguous XIDs.
func compilerInputHotCacheXIDSafe(xmin string, horizon int64) bool {
	xid, err := strconv.ParseUint(xmin, 10, 32)
	if err != nil || xid < 3 || horizon < 0 {
		return false
	}
	age := (uint64(horizon) - xid) & (1<<32 - 1)
	return age > 0 && age < 1<<31
}

func (c *compilerInputHotCache) bind(ctx context.Context, tx *sql.Tx) error {
	current, err := compilerInputHotCacheSessionRead(ctx, tx)
	if err != nil {
		return err
	}
	if c.capture {
		c.session = current
	} else {
		c.report.SessionValidated = c.session.accepts(current)
		if !c.report.SessionValidated {
			c.entries = nil
		}
	}
	c.tx = tx
	return nil
}

func compilerInputHotInventoryGuards(ctx context.Context, tx *sql.Tx) error {
	for _, q := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx)
}

// RunCompilerInputPreparedRetentionInventory explicitly opts into a separate
// read-only preparation pass (soft stop at 45s, hard RR60s), followed by the
// unchanged final RR60s proof. Both retain SQL5s/lock250ms/zero-temp and every
// graph/source/batch limit. A canceled/broken connection is never reacquired.
// The ordinary unprepared inventory remains available and unchanged.
func RunCompilerInputPreparedRetentionInventory(ctx context.Context, databaseURL string) (CompilerInputRetentionInventory, error) {
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return CompilerInputRetentionInventory{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return CompilerInputRetentionInventory{}, err
	}
	defer conn.Close()
	cache, err := prepareCompilerInputHotCache(ctx, conn)
	if err != nil {
		return CompilerInputRetentionInventory{Preparation: &cache.report}, &compilerInputHotInventoryError{"metadata-preparation", err}
	}
	proofCtx, proofCancel := context.WithTimeout(ctx, time.Minute)
	defer proofCancel()
	tx, err := conn.BeginTx(proofCtx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return CompilerInputRetentionInventory{Preparation: &cache.report}, err
	}
	defer tx.Rollback()
	if err = compilerInputHotInventoryGuards(proofCtx, tx); err != nil {
		return CompilerInputRetentionInventory{Preparation: &cache.report}, err
	}
	if err := cache.bind(proofCtx, tx); err != nil {
		return CompilerInputRetentionInventory{Preparation: &cache.report}, err
	}
	report, err := compilerInputRetentionInventorySnapshotCached(proofCtx, tx, cache)
	report.Preparation = &cache.report
	if err != nil {
		return report, err
	}
	if err := tx.Commit(); err != nil {
		// Commit failure must not leave an apparently complete inventory.
		report.UniverseComplete = false
		report.Entries, report.Totals, report.AuditSHA256 = nil, nil, ""
		return report, err
	}
	return report, nil
}

func prepareCompilerInputHotCache(ctx context.Context, conn *sql.Conn) (*compilerInputHotCache, error) {
	cache := &compilerInputHotCache{entries: map[string]compilerInputHotCacheEntry{}, capture: true, stopBefore: time.Now().Add(45 * time.Second)}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return cache, err
	}
	defer tx.Rollback()
	if err = compilerInputHotInventoryGuards(ctx, tx); err != nil {
		return cache, err
	}
	if err := cache.bind(ctx, tx); err != nil {
		return cache, err
	}
	started := time.Now()
	warm, warmErr := compilerInputHotInventorySnapshotCached(ctx, tx, false, cache)
	cache.report.DurationUS = time.Since(started).Microseconds()
	cache.report.Cost = warm.Cost
	cache.capture = false
	// Deadline cancellation may close pgx's physical connection. Refuse this
	// operation; don't silently reconnect and use process-local tuple evidence.
	if ctx.Err() != nil {
		return cache, ctx.Err()
	}
	if warmErr != nil && !errors.Is(warmErr, errCompilerInputHotPreparationStop) {
		return cache, warmErr
	}
	cache.report.Stopped = "traversed"
	if warmErr != nil {
		cache.report.Stopped = "preparation-bound"
	}
	// Explicit rollback precedes the final snapshot. No preparation root,
	// closure, input length or completeness flag is carried across this point.
	if err := tx.Rollback(); err != nil {
		return cache, err
	}
	return cache, nil
}

func (c *compilerInputHotCache) resolve(ctx context.Context, tx *sql.Tx, refs []inputarchive.HotReference, cost *CompilerInputHotCost, indexed bool) (map[inputarchive.HotReference]inputarchive.HotNode, error) {
	if tx != c.tx || len(refs) < 1 || len(refs) > 32 {
		return nil, ErrInvalidInput
	}
	if c.capture && !time.Now().Before(c.stopBefore) {
		return nil, errCompilerInputHotPreparationStop
	}
	ids := []string{}
	for _, ref := range refs {
		if ref.Kind == "artifact" {
			ids = append(ids, ref.ID)
		}
	}
	versions := map[string]compilerInputHotCacheVersion{}
	if len(ids) > 0 {
		started := time.Now()
		rows, err := tx.QueryContext(ctx, `SELECT id,xmin::text,ctid::text,tableoid::bigint,artifact_kind,scope_key,generation,content_hash
 FROM fugue_platform_artifacts WHERE id=ANY($1::text[]) AND content_json IS NOT NULL`, ids)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var v compilerInputHotCacheVersion
			if err = rows.Scan(&id, &v.Xmin, &v.CTID, &v.Relation, &v.Kind, &v.Scope, &v.Generation, &v.Hash); err != nil {
				break
			}
			versions[id] = v
		}
		err = errors.Join(err, rows.Err(), rows.Close())
		if !c.capture {
			c.report.VersionQueries++
			c.report.VersionReadUS += time.Since(started).Microseconds()
		}
		if err != nil {
			return nil, err
		}
	}
	out := map[inputarchive.HotReference]inputarchive.HotNode{}
	missing := make([]inputarchive.HotReference, 0, len(refs))
	charged := 0
	for _, ref := range refs {
		v, exists := versions[ref.ID]
		entry, cached := c.entries[ref.ID]
		if !c.capture && ref.Kind == "artifact" && exists && cached && v.Relation == c.session.Relation && v == entry.version {
			charged += int(entry.sourceBytes) + entry.metadataBytes
			if charged > 32<<20 {
				return nil, ErrConflict
			}
			node := entry.node
			node.References = append([]inputarchive.HotReference{}, node.References...)
			out[node.Identity] = node
			c.report.Hits++
			c.report.ReusedSourceBytes += entry.sourceBytes
		} else {
			missing = append(missing, ref)
			if !c.capture && ref.Kind == "artifact" {
				c.report.Misses++
			}
		}
	}
	if len(missing) == 0 {
		return out, nil
	}
	var capture func(inputarchive.HotNode, int64, int) error
	if c.capture {
		// Capture is always derived from original source projection, not a
		// separately persisted acceleration index.
		indexed = false
		capture = func(node inputarchive.HotNode, sourceBytes int64, metadataBytes int) error {
			v, exists := versions[node.Identity.ID]
			// Reference bodies have a second tuple identity. Until a versioned
			// cache binds both tuples, resolve those rows in the final snapshot.
			if !exists {
				return nil
			}
			if v.Relation != c.session.Relation {
				return ErrConflict
			}
			if !compilerInputHotCacheXIDSafe(v.Xmin, c.session.Horizon) {
				return nil // final RR will decode this source normally
			}
			if len(v.Kind) > 128 || len(v.Scope) > 512 || len(v.Generation) > 512 || len(v.Hash) > 128 {
				return nil // do not retain unbounded ledger envelope fields
			}
			raw, err := json.Marshal(node)
			if err != nil {
				return err
			}
			encodedBytes := len(raw) + len(v.Xmin) + len(v.CTID) + len(v.Kind) + len(v.Scope) + len(v.Generation) + len(v.Hash)
			if len(c.entries) >= 32768 || c.report.References+len(node.References) > 262144 || c.report.EncodedBytes+encodedBytes > compilerInputHotCacheBytes {
				return errCompilerInputHotPreparationStop
			}
			// Decode the bounded metadata again to own every retained string and
			// slice; no source JSON buffer or caller's graph slice can escape.
			var owned inputarchive.HotNode
			if err := json.Unmarshal(raw, &owned); err != nil {
				return err
			}
			c.entries[node.Identity.ID] = compilerInputHotCacheEntry{v, owned, sourceBytes, metadataBytes}
			c.report.Artifacts++
			c.report.References += len(node.References)
			c.report.EncodedBytes += encodedBytes
			return nil
		}
	}
	resolved, err := resolveCompilerInputHotBatchRead(ctx, tx, missing, cost, indexed, charged, capture)
	if err != nil {
		return nil, err
	}
	for ref, node := range resolved {
		out[ref] = node
	}
	return out, nil
}
