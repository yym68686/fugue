package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

// Offline, isolated PostgreSQL only. Report actual heap+TOAST, indexes, recipe
// bytes and WAL, not just compression. No production transcript is logged.
func TestCompilerInputBlockTotalCost(t *testing.T) {
	if os.Getenv("FUGUE_INPUT_BLOCK_COST_PROFILE") != "1" {
		t.Skip("explicit offline cost profile")
	}
	for _, churn := range []string{"stable-records", "sliding-window", "all-records-changing"} {
		for _, profile := range []compilerInputBlockPolicy{{1, 256, 0}, {1, 2048, 0}, {1, 4096, 0}, {1, 8192, 0}, {1, 256, 3}, {1, 256, 4}} {
			t.Run(fmt.Sprintf("%s/min-%d-bits-%d", churn, profile.MinimumBytes, profile.BoundaryBits), func(t *testing.T) {
				db, address, _ := hotInventoryTestDatabase(t)
				for _, migrate := range []func(context.Context, string) error{schemamigrate.MigrateCompilerInputCodec, schemamigrate.MigrateCompilerInputShared, schemamigrate.MigrateCompilerInputSeparated} {
					if err := migrate(t.Context(), address); err != nil {
						t.Fatal(err)
					}
				}
				s := &Store{db: db, databaseURL: address, dbReady: true}
				s.storagePolicy.value = StoragePolicy{Version: 1, CompilerInputChunks: true}
				s.storagePolicy.separated = compilerInputCodecPolicy{Version: 1, Enabled: true}
				s.storagePolicy.blocks = profile
				var wal string
				if err := db.QueryRow(`SELECT pg_current_wal_insert_lsn()::text`).Scan(&wal); err != nil {
					t.Fatal(err)
				}
				var logical int
				for generation := 0; generation < 6; generation++ {
					items := make([]map[string]any, 0, 2000)
					for index := 0; index < 2000; index++ {
						id, epoch := index, 0
						if churn == "sliding-window" {
							id += generation * 31
						}
						if churn == "all-records-changing" {
							epoch = generation
						}
						h := sha256.Sum256([]byte(fmt.Sprintf("record-%d-%d", id, epoch)))
						items = append(items, map[string]any{"id": fmt.Sprintf("observation-%08d", id), "observed_at": fmt.Sprintf("2026-01-01T00:00:%02dZ", epoch), "payload": fmt.Sprintf("%x", h) + strings.Repeat("bounded-neutral-evidence-", 18), "healthy": true})
					}
					raw, _ := json.Marshal(map[string]any{"captured_at": generation, "records": items})
					snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("cost-generation-%d", generation), PolicyGeneration: "cost-policy", DNSSelections: []platformconfig.DNSSelectionObservation{{Hostname: "service.example.test", Type: "A", PhysicalEvidence: raw}}}
					encoded, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
					if err != nil {
						t.Fatal(err)
					}
					logical += len(encoded)
					digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
					if err != nil {
						t.Fatal(err)
					}
					if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
						t.Fatal(err)
					}
					assertSeparatedEquivalent(t, s, snapshot, digest)
				}
				var heap, indexes, payload, objects, walBytes int64
				if err := db.QueryRow(`SELECT sum(pg_table_size(oid)),sum(pg_indexes_size(oid)) FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname IN ('fugue_platform_input_manifests','fugue_platform_input_shared_packs','fugue_platform_input_shared_objects','fugue_platform_input_components','fugue_platform_input_component_recipes','fugue_platform_input_component_refs','fugue_platform_input_separated_roots','fugue_platform_input_separated_refs')`).Scan(&heap, &indexes); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT COALESCE(sum(octet_length(data)),0) FROM fugue_platform_input_shared_packs`).Scan(&payload); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT count(*) FROM fugue_platform_input_shared_objects`).Scan(&objects); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn)::bigint`, wal).Scan(&walBytes); err != nil {
					t.Fatal(err)
				}
				// Counterfactual varint recipe bytes, NOT a measured new codec/WAL.
				var fixed, compact int
				rows, err := db.Query(`SELECT recipe FROM fugue_platform_input_component_recipes`)
				if err != nil {
					t.Fatal(err)
				}
				for rows.Next() {
					var raw []byte
					if err = rows.Scan(&raw); err != nil {
						t.Fatal(err)
					}
					fixed += len(raw)
					compact += 5
					refs, err := decodeSharedRecipe(raw, int(binary.BigEndian.Uint32(raw[1:5])))
					if err != nil {
						t.Fatal(err)
					}
					for _, ref := range refs {
						compact += len(binary.AppendUvarint(nil, uint64(ref.ID))) + len(binary.AppendUvarint(nil, uint64(ref.Size)))
					}
				}
				if err = rows.Err(); err != nil {
					t.Fatal(err)
				}
				rows.Close()
				t.Logf("logical=%d payload=%d heap_toast=%d indexes=%d relation_total=%d WAL=%d relation_plus_WAL=%d objects=%d fixed_recipe=%d estimated_varint_recipe=%d", logical, payload, heap, indexes, heap+indexes, walBytes, heap+indexes+walBytes, objects, fixed, compact)
			})
		}
	}
}
