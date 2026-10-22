package store

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"fugue/internal/schemamigrate"
	"hash"
	"math/rand"
	"testing"
	"time"

	"fugue/internal/platformconfig"
)

func syntheticCodecSnapshot() platformconfig.RuntimeSnapshot {
	random := rand.New(rand.NewSource(29))
	s := platformconfig.RuntimeSnapshot{IntentGeneration: "benchmark", PolicyGeneration: "policy", Facts: map[string]any{}}
	// Many signed-observation-shaped records, including high-entropy nonce and
	// digest fields. Synthetic reusable fixture, never production identities.
	records := make([]any, 12000)
	for i := range records {
		raw := make([]byte, 256)
		random.Read(raw)
		records[i] = map[string]any{"payload": base64.StdEncoding.EncodeToString(raw), "signature": fmt.Sprintf("sha256:%x", sha256.Sum256(raw)), "node_id": fmt.Sprintf("node-%d", i%7), "scope": "global", "status": "passed", "nonce": base64.StdEncoding.EncodeToString(raw[:24])}
	}
	s.Facts["observations"] = records
	return s
}
func BenchmarkCompilerInputCodecSnapshot(b *testing.B) {
	s := syntheticCodecSnapshot()
	for _, compressed := range []bool{false, true} {
		b.Run(fmt.Sprint(compressed), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var h hash.Hash = sha256.New()
				size := inputChunkBytes
				var c *inputChunkEncoder
				if compressed {
					size -= inputCodecHeaderBytes
					c = &inputChunkEncoder{}
				}
				w := &inputChunkWriter{hash: h, chunk: make([]byte, 0, size), codec: c}
				if err := platformconfig.WriteRuntimeSnapshot(w, s); err != nil {
					b.Fatal(err)
				}
				if err := w.flush(); err != nil {
					b.Fatal(err)
				}
				stored := int64(0)
				for _, n := range w.storedSizes {
					stored += int64(n)
				}
				if !compressed {
					stored = w.size
				}
				b.SetBytes(w.size)
				b.ReportMetric(float64(stored), "stored-B/op")
				b.ReportMetric(float64(w.size), "logical-B/op")
			}
		})
	}
}

func BenchmarkCompilerInputCodecPostgres(b *testing.B) {
	s := boundedContentTestStore(b)
	if err := schemamigrate.MigrateCompilerInputCodec(b.Context(), s.databaseURL); err != nil {
		b.Fatal(err)
	}
	for _, compressed := range []bool{false, true} {
		b.Run(fmt.Sprint(compressed), func(b *testing.B) {
			snapshot := syntheticCodecSnapshot()
			s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: compressed}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				snapshot.IntentGeneration = fmt.Sprintf("bench-%d-%d", time.Now().UnixNano(), i)
				digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				var logical, wire, datum int64
				if err = s.db.QueryRow(`SELECT m.size_bytes,sum(octet_length(c.data)),sum(pg_column_size(c.data)) FROM fugue_platform_input_manifests m JOIN fugue_platform_input_chunks c USING(content_hash) WHERE content_hash=$1 GROUP BY m.size_bytes`, digest).Scan(&logical, &wire, &datum); err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(logical), "logical-B/op")
				b.ReportMetric(float64(wire), "stored-wire-B/op")
				b.ReportMetric(float64(datum), "PG-datum-B/op")
				if _, err = s.db.Exec(`DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest); err != nil {
					b.Fatal(err)
				}
				if _, err = s.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}
