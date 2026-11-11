package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"fugue/internal/schemamigrate"
)

const compilerInputRetentionUniverseLimit = 32768

// CompilerInputRetentionEntry is one logical input, irrespective of the number
// of roots or physical representations. H means a native retained dependency;
// it does not assert a valid signature, full readback, or an independent copy.
// An input outside that closure remains U until its other retention authorities
// are resolved. Age, archive verification and absence from native roots cannot
// establish permission to classify it C or D.
type CompilerInputRetentionEntry struct {
	Digest              string   `json:"digest"`
	Class               string   `json:"class"`
	LogicalBytes        int64    `json:"logical_bytes"`
	Format              string   `json:"format"`
	Location            string   `json:"location"`
	LocalRepresentation string   `json:"local_representation"`
	InlineCopyPresent   bool     `json:"inline_copy_present"`
	NativeRoots         []string `json:"native_roots"`
}

type CompilerInputRetentionTotal struct {
	Inputs       int   `json:"inputs"`
	LogicalBytes int64 `json:"logical_bytes"`
}

// This audit binds a complete bounded input universe and native closure to the
// SAME read-only RR snapshot. It cannot be consumed by a mutation API. In
// particular, AuditSHA256 is not a retirement or enforcement plan hash.
type CompilerInputRetentionInventory struct {
	Action                 string                                 `json:"action"`
	Schema                 string                                 `json:"schema"`
	Database               string                                 `json:"database"`
	DatabaseSchema         string                                 `json:"database_schema"`
	Native                 CompilerInputHotInventory              `json:"native"`
	UniverseSources        []string                               `json:"universe_sources"`
	UniverseComplete       bool                                   `json:"universe_complete"`
	ClassificationComplete bool                                   `json:"classification_complete"`
	ExecutionAuthorized    bool                                   `json:"execution_authorized"`
	UncoveredSources       []string                               `json:"uncovered_sources"`
	Totals                 map[string]CompilerInputRetentionTotal `json:"totals"`
	Entries                []CompilerInputRetentionEntry          `json:"entries"`
	AuditSHA256            string                                 `json:"audit_sha256"`
	Preparation            *CompilerInputHotPreparation           `json:"preparation,omitempty"`
}

func RunCompilerInputRetentionInventory(ctx context.Context, databaseURL string) (CompilerInputRetentionInventory, error) {
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return CompilerInputRetentionInventory{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return CompilerInputRetentionInventory{}, err
	}
	defer tx.Rollback()
	for _, q := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return CompilerInputRetentionInventory{}, err
		}
	}
	if err := schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return CompilerInputRetentionInventory{}, err
	}
	report, err := compilerInputRetentionInventorySnapshot(ctx, tx)
	if err != nil {
		return report, err
	}
	if err := tx.Commit(); err != nil {
		return CompilerInputRetentionInventory{}, err
	}
	return report, nil
}

func compilerInputRetentionInventorySnapshot(ctx context.Context, tx *sql.Tx) (report CompilerInputRetentionInventory, err error) {
	return compilerInputRetentionInventorySnapshotCached(ctx, tx, nil)
}

func compilerInputRetentionInventorySnapshotCached(ctx context.Context, tx *sql.Tx, cache *compilerInputHotCache) (report CompilerInputRetentionInventory, err error) {
	report.Action = "archive-retention-inventory"
	report.Schema = "fugue.compiler-input-retention-inventory.v1"
	report.UniverseSources = []string{"input-manifests", "inline-input-contents"}
	// Partial native diagnostics may survive a refusal, but never a partial
	// complement, totals, or digest that could be mistaken for a full audit.
	defer func() {
		if err != nil {
			report.UniverseComplete = false
			report.Entries, report.Totals, report.AuditSHA256 = nil, nil, ""
		}
	}()
	if err = tx.QueryRowContext(ctx, `SELECT current_database(),current_schema()`).Scan(&report.Database, &report.DatabaseSchema); err != nil {
		return report, err
	}
	report.Native, err = compilerInputHotInventorySnapshotCached(ctx, tx, false, cache)
	if err != nil {
		return report, err
	}
	if !report.Native.Closure.MetadataComplete {
		return report, &compilerInputHotInventoryError{"retention-native-closure", ErrConflict}
	}
	report.UncoveredSources = append([]string(nil), report.Native.UncoveredSources...)
	report.UncoveredSources = append(report.UncoveredSources,
		"promised-rollback-and-offline-client-inventory", "unregistered-reader-and-writer-inventory",
		"cold-retention-and-discard-policy-dispositions", "physical-orphan-and-allocation-audit")
	entries, err := compilerInputRetentionUniverse(ctx, tx)
	if err != nil {
		return report, &compilerInputHotInventoryError{"retention-universe", err}
	}
	for _, node := range report.Native.Closure.Nodes {
		if node.Identity.Kind != "input" {
			continue
		}
		entry, found := entries[node.Identity.ID]
		if !found || node.LogicalBytes != entry.LogicalBytes || node.Format != entry.Format || node.Location != entry.Location || len(node.Roots) == 0 {
			return report, &compilerInputHotInventoryError{"retention-universe-binding", ErrConflict}
		}
		entry.Class = "H"
		entry.NativeRoots = append([]string(nil), node.Roots...)
		entries[entry.Digest] = entry
	}
	report.Totals = map[string]CompilerInputRetentionTotal{"H": {}, "C": {}, "D": {}, "U": {}}
	report.Entries = make([]CompilerInputRetentionEntry, 0, len(entries))
	for _, entry := range entries {
		total := report.Totals[entry.Class]
		if total.LogicalBytes > int64(^uint64(0)>>1)-entry.LogicalBytes {
			return report, &compilerInputHotInventoryError{"retention-total-overflow", ErrConflict}
		}
		total.Inputs++
		total.LogicalBytes += entry.LogicalBytes
		report.Totals[entry.Class] = total
		report.Entries = append(report.Entries, entry)
	}
	sort.Slice(report.Entries, func(i, j int) bool { return report.Entries[i].Digest < report.Entries[j].Digest })
	report.UniverseComplete = true
	// Bind audit data, not resolver durations or diagnostic counters. This is
	// stable when the same snapshot is inspected twice, but cannot splice a
	// complement or graph from another snapshot/database/schema.
	encoded, err := json.Marshal(struct {
		Schema, Database, DatabaseSchema, Snapshot string
		Closure                                    string
		UniverseSources                            []string
		Uncovered                                  []string
		Entries                                    []CompilerInputRetentionEntry
	}{report.Schema, report.Database, report.DatabaseSchema, report.Native.Snapshot,
		report.Native.Closure.PlanSHA256, report.UniverseSources, report.UncoveredSources, report.Entries})
	if err != nil {
		return report, err
	}
	sum := sha256.Sum256(encoded)
	report.AuditSHA256 = hex.EncodeToString(sum[:])
	return report, nil
}

func compilerInputRetentionUniverse(ctx context.Context, tx *sql.Tx) (map[string]CompilerInputRetentionEntry, error) {
	entries := make(map[string]CompilerInputRetentionEntry)
	// Both scans use their existing digest primary keys and the same snapshot.
	// LIMIT+1 refuses overflow, never pages across independent transactions.
	rows, err := tx.QueryContext(ctx, `SELECT content_hash,format,storage_location,size_bytes,chunk_size,chunk_count,created_at FROM fugue_platform_input_manifests ORDER BY content_hash LIMIT $1`, compilerInputRetentionUniverseLimit+1)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var m inputManifest
		if err = rows.Scan(&m.Digest, &m.Format, &m.Location, &m.Size, &m.ChunkSize, &m.Count, &m.Created); err != nil {
			break
		}
		if err = validateCompilerInputManifest(m); err != nil {
			break
		}
		storage := "separated-components"
		switch m.Format {
		case inputFormatV1, inputFormatCodecV2:
			storage = m.Location + "-chunks"
		case inputFormatSharedV3:
			storage = "shared-packs"
		}
		entries[m.Digest] = CompilerInputRetentionEntry{Digest: m.Digest, Class: "U", LogicalBytes: m.Size, Format: m.Format, Location: m.Location, LocalRepresentation: storage, NativeRoots: []string{}}
		if len(entries) > compilerInputRetentionUniverseLimit {
			err = ErrConflict
			break
		}
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT content_hash,size_bytes FROM fugue_platform_artifact_contents ORDER BY content_hash LIMIT $1`, compilerInputRetentionUniverseLimit+1)
	if err != nil {
		return nil, err
	}
	count := 0
	for rows.Next() {
		var digest string
		var size int64
		if err = rows.Scan(&digest, &size); err != nil {
			break
		}
		count++
		if count > compilerInputRetentionUniverseLimit || !inputDigestPattern.MatchString(digest) || size < 1 || size > inputMaxBytes {
			err = ErrConflict
			break
		}
		entry, found := entries[digest]
		if found && entry.LogicalBytes != size {
			err = ErrConflict
			break
		}
		if !found {
			entry = CompilerInputRetentionEntry{Digest: digest, Class: "U", LogicalBytes: size, Format: "legacy-inline", Location: "artifact-contents", LocalRepresentation: "legacy-inline", NativeRoots: []string{}}
		}
		entry.InlineCopyPresent = true
		entries[digest] = entry
		if len(entries) > compilerInputRetentionUniverseLimit {
			err = ErrConflict
			break
		}
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return nil, err
	}
	return entries, nil
}
