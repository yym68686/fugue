package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type inputProfileTracer struct {
	postgresReadTracer
	queries            atomic.Uint64
	metadata           atomic.Uint64
	componentManifests atomic.Uint64
	packBodies         atomic.Uint64
	delay              time.Duration
}

func (t *inputProfileTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	t.queries.Add(1)
	if strings.Contains(data.SQL, "size_bytes,stored_hash,data FROM fugue_platform_input_shared_packs") {
		t.packBodies.Add(1)
	}
	if strings.Contains(data.SQL, "fugue_platform_input_components") {
		t.componentManifests.Add(1)
	}
	if strings.Contains(data.SQL, "fugue_platform_input_component_recipes") || strings.Contains(data.SQL, "fugue_platform_input_component_refs") {
		t.metadata.Add(1)
	}
	if t.delay > 0 {
		timer := time.NewTimer(t.delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
		}
	}
	return t.postgresReadTracer.TraceQueryStart(ctx, conn, data)
}

// Explicit offline benchmark. Source bytes are consumed from a private pipe;
// only aggregates are logged. Use only a guarded disposable PostgreSQL.
func TestSharedInputProfileStream(t *testing.T) {
	if os.Getenv("FUGUE_INPUT_PROFILE_STREAM") != "1" && os.Getenv("FUGUE_PROFILE_SOURCE_DATABASE_URL") == "" {
		t.Skip("offline profile disabled")
	}
	var s *Store
	if os.Getenv("FUGUE_INPUT_PROFILE_FORMAT") == "v4" {
		s = separatedTestStore(t)
	} else {
		s = sharedInputTestStore(t)
	}
	tracer := &inputProfileTracer{}
	if d := os.Getenv("FUGUE_INPUT_PROFILE_QUERY_DELAY"); d != "" {
		var err error
		tracer.delay, err = time.ParseDuration(d)
		if err != nil {
			t.Fatal(err)
		}
	}
	config, err := pgx.ParseConfig(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.Tracer = tracer
	s.db.Close()
	s.db = stdlib.OpenDB(*config)
	t.Cleanup(func() { s.db.Close() })
	if os.Getenv("FUGUE_INPUT_PROFILE_FORMAT") == "v2" {
		s.storagePolicy.shared.Enabled = false
		s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: true}
	}
	var source *Store
	if dsn := os.Getenv("FUGUE_PROFILE_SOURCE_DATABASE_URL"); dsn != "" {
		u, err := url.Parse(dsn)
		if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
			t.Fatal("offline loopback source only")
		}
		db, err := openPostgresDatabase(dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		source = &Store{db: db, databaseURL: dsn, dbReady: true}
	}
	var encoder inputChunkEncoder
	samples := 2
	if os.Getenv("FUGUE_INPUT_PROFILE_FORMAT") == "v4" {
		samples = 1
	}
	for sample := 0; sample < samples; sample++ {
		var n uint32
		var raw []byte
		if source != nil {
			var key string
			if err := source.db.QueryRow(`SELECT content_hash FROM fugue_platform_input_manifests WHERE size_bytes>90000000 ORDER BY created_at LIMIT 1 OFFSET $1`, sample).Scan(&key); err != nil {
				t.Fatal(err)
			}
			snapshot, err := source.GetPlatformCompilerInput(key)
			if err != nil {
				t.Fatal(err)
			}
			raw, err = platformconfig.EncodeRuntimeSnapshot(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			n = uint32(len(raw))
		} else {
			if err := binary.Read(os.Stdin, binary.BigEndian, &n); err != nil {
				t.Fatal(err)
			}
			if n < 1 || n > 200<<20 {
				t.Fatal("sample outside bound")
			}
			raw = make([]byte, n)
			if _, err := io.ReadFull(os.Stdin, raw); err != nil {
				t.Fatal(err)
			}
		}
		snapshot, err := platformconfig.DecodeRuntimeSnapshotJSON(raw)
		if err != nil {
			t.Fatal("sample decode")
		}
		digest := sharedHash(sha256.Sum256(raw))
		if actual, err := platformconfig.RuntimeSnapshotDigest(snapshot); err != nil || actual != digest {
			t.Fatal("noncanonical sample")
		}
		if os.Getenv("FUGUE_INPUT_PROFILE_FRESH") == "1" {
			snapshot.IntentGeneration = fmt.Sprintf("offline-profile-%d", time.Now().UnixNano())
			raw, err = platformconfig.EncodeRuntimeSnapshot(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			digest = sharedHash(sha256.Sum256(raw))
		}
		start := time.Now()
		unique := map[[32]byte]bool{}
		for _, v := range snapshot.DNSSelections {
			unique[sha256.Sum256(v.PhysicalEvidence)] = true
		}
		t.Logf("format=%s logical=%d selections=%d unique_evidence=%d", os.Getenv("FUGUE_INPUT_PROFILE_FORMAT"), n, len(snapshot.DNSSelections), len(unique))
		v2 := 0
		for offset := 0; offset < len(raw); offset += inputChunkBytes - inputCodecHeaderBytes {
			b, err := encoder.encode(raw[offset:min(offset+inputChunkBytes-inputCodecHeaderBytes, len(raw))])
			if err != nil {
				t.Fatal(err)
			}
			v2 += len(b)
		}
		v2Time := time.Since(start)
		start = time.Now()
		tracer.queries.Store(0)
		if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
			t.Fatalf("write failed after %s queries=%d: %v", time.Since(start), tracer.queries.Load(), err)
		}
		writeTime := time.Since(start)
		t.Logf("write_queries=%d simulated_query_delay=%s", tracer.queries.Load(), tracer.delay)
		start = time.Now()
		got, err := s.GetPlatformCompilerInput(digest)
		if err != nil {
			t.Fatal(err)
		}
		readTime := time.Since(start)
		var rebuilt bytes.Buffer
		if platformconfig.WriteRuntimeSnapshot(&rebuilt, got) != nil || !bytes.Equal(rebuilt.Bytes(), raw) {
			t.Fatal("roundtrip bytes differ")
		}
		start = time.Now()
		_, err = s.GetPlatformCompilerInputDNSSelections(digest, func(CompilerDNSSelectionSummary) bool { return false })
		if err != nil {
			t.Fatal(err)
		}
		selectTime := time.Since(start)
		var packs, objects, recipes, relations int64
		if err = s.db.QueryRow(`SELECT COALESCE(sum(octet_length(data)),0) FROM fugue_platform_input_shared_packs`).Scan(&packs); err != nil {
			t.Fatal(err)
		}
		if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_shared_objects`).Scan(&objects); err != nil {
			t.Fatal(err)
		}
		if err = s.db.QueryRow(`SELECT COALESCE(sum(octet_length(recipe)),0) FROM fugue_platform_input_shared_recipes`).Scan(&recipes); err != nil {
			t.Fatal(err)
		}
		if err = s.db.QueryRow(`SELECT sum(pg_total_relation_size(t::regclass)) FROM unnest(ARRAY['fugue_platform_input_shared_packs','fugue_platform_input_shared_objects','fugue_platform_input_shared_recipes','fugue_platform_input_shared_refs','fugue_platform_input_chunks','fugue_platform_input_manifests']) t`).Scan(&relations); err != nil {
			t.Fatal(err)
		}
		t.Logf("sample=%d logical=%d v2_envelope_bytes=%d cumulative_pack_bytes=%d cumulative_objects=%d cumulative_recipe_bytes=%d allocated_shared_relations=%d v2_encode=%s v3_write=%s full_read=%s omitted_all_read=%s cache_present=%t", sample, n, v2, packs, objects, recipes, relations, v2Time, writeTime, readTime, selectTime, s.compilerDNSInputs.get(digest) != nil)
	}
}
