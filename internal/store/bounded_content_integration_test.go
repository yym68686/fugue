package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

func boundedContentTestStore(t testing.TB) *Store {
	t.Helper()
	dsn := os.Getenv("FUGUE_MEMORY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable memory test PostgreSQL not configured")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback database")
	}
	db, err := openPostgresDatabase(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS fugue_platform_artifact_contents(content_hash TEXT PRIMARY KEY,content_json JSONB NOT NULL,size_bytes BIGINT NOT NULL,created_at TIMESTAMPTZ NOT NULL,updated_at TIMESTAMPTZ NOT NULL);CREATE TABLE IF NOT EXISTS fugue_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`); err != nil {
		t.Fatal(err)
	}
	if err = schemamigrate.MigrateBoundedContent(t.Context(), dsn); err != nil {
		t.Fatal(err)
	}
	s := &Store{databaseURL: dsn, db: db, dbReady: true}
	s.storagePolicy.value = StoragePolicy{Version: 1, CompilerInputChunks: true, CheckpointPages: true}
	return s
}
func chunkedInputFixture(t testing.TB) (platformconfig.RuntimeSnapshot, string) {
	t.Helper()
	s := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("input-%d", time.Now().UnixNano()), PolicyGeneration: "policy"}
	for i := 0; i < 12; i++ {
		s.Origins = append(s.Origins, platformconfig.OriginObservation{Ref: fmt.Sprintf("origin-%d", i), RuntimeID: strings.Repeat("nested-evidence-", 22000)})
	}
	d, err := platformconfig.RuntimeSnapshotDigest(s)
	if err != nil {
		t.Fatal(err)
	}
	return s, d
}
func TestChunkedCompilerInputPostgresCompatibilityCorruptionAndRollback(t *testing.T) {
	s := boundedContentTestStore(t)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := platformconfig.RuntimeSnapshotDigest(got)
	if actual != digest {
		t.Fatal("digest changed")
	}
	legacyReader := &Store{databaseURL: s.databaseURL, db: s.db, dbReady: true}
	if _, err = legacyReader.GetPlatformCompilerInput(digest); err != nil {
		t.Fatal("reader with writers disabled cannot read new format", err)
	}
	content, err := legacyReader.GetPlatformArtifactContent(digest)
	if err != nil {
		t.Fatal(err)
	}
	actual, err = platformconfig.RuntimeSnapshotContentDigest(content.Content)
	if err != nil || actual != digest {
		t.Fatal("legacy API shape changed", err)
	}
	var maxChunk, count int
	var created time.Time
	if err = s.db.QueryRow(`SELECT max(octet_length(data)),count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest).Scan(&maxChunk, &count); err != nil {
		t.Fatal(err)
	}
	if maxChunk > inputChunkBytes || count < 2 {
		t.Fatal("not bounded chunks", maxChunk, count)
	}
	s.db.QueryRow(`SELECT created_at FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&created)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var after time.Time
	s.db.QueryRow(`SELECT created_at FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&after)
	if !after.Equal(created) {
		t.Fatal("retry changed manifest")
	}
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_chunks SET data=set_byte(data,0,33) WHERE content_hash=$1 AND ordinal=0`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetPlatformCompilerInput(digest); err == nil {
		t.Fatal("corrupt chunk accepted")
	}
	if err = s.EnsurePlatformCompilerInput(snapshot, digest); err == nil {
		t.Fatal("corrupt existing content silently replaced")
	}
	other, otherDigest := chunkedInputFixture(t)
	wrong := "sha256:" + strings.Repeat("b", 64)
	if err = s.EnsurePlatformCompilerInput(other, wrong); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong digest accepted", err)
	}
	var leaked int
	s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1`, wrong).Scan(&leaked)
	if leaked != 0 {
		t.Fatal("failed write leaked chunks")
	}
	if err = s.EnsurePlatformCompilerInput(other, otherDigest); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1 AND ordinal=1`, otherDigest); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetPlatformCompilerInput(otherDigest); err == nil {
		t.Fatal("missing chunk accepted")
	}
	old, oldDigest := chunkedInputFixture(t)
	raw, _ := platformconfig.EncodeRuntimeSnapshot(old)
	if _, err = s.db.Exec(`INSERT INTO fugue_platform_artifact_contents VALUES($1,$2::jsonb,$3,NOW(),NOW())`, oldDigest, raw, len(raw)); err != nil {
		t.Fatal(err)
	}
	if err = s.EnsurePlatformCompilerInput(old, oldDigest); err != nil {
		t.Fatal("legacy content no longer readable", err)
	}
	if _, err = s.GetPlatformCompilerInput(oldDigest); err != nil {
		t.Fatal(err)
	}
	var migrated bool
	s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM fugue_platform_input_manifests WHERE content_hash=$1)`, oldDigest).Scan(&migrated)
	if migrated {
		t.Fatal("legacy content rewritten")
	}
}

func TestObservationPagesPostgresAtomicCursorAndReplicaLock(t *testing.T) {
	s := boundedContentTestStore(t)
	key := fmt.Sprintf("scan-%d", time.Now().UnixNano())
	raw := []byte(`{"cursor":"legacy"}`)
	if _, err := s.db.Exec(`INSERT INTO fugue_meta(key,value) VALUES($1,$2)`, observationCheckpointKey(key), string(raw)); err != nil {
		t.Fatal(err)
	}
	s.db.SetMaxOpenConns(1)
	readCtx, cancelRead := context.WithTimeout(t.Context(), time.Second)
	_, legacyBefore, err := s.ReadObservationPages(readCtx, key)
	cancelRead()
	if err != nil || string(legacyBefore) != string(raw) {
		t.Fatal("legacy fallback blocked or changed", err)
	}
	s.db.SetMaxOpenConns(4)
	legacy := func([]byte) ([]byte, error) { t.Fatal("legacy writer used after activation"); return nil, nil }
	_, err = s.UpdateObservationPages(t.Context(), key, func(p ObservationPages, b []byte) (ObservationPages, error) {
		if string(b) != string(raw) {
			t.Fatal("legacy checkpoint lost")
		}
		return ObservationPages{Metadata: []byte(`{"cursor":"page2"}`), Pages: map[string][]byte{"a": []byte("page-a"), "b": []byte("page-b")}}, nil
	}, legacy)
	if err != nil {
		t.Fatal(err)
	}
	read, _, err := s.ReadObservationPages(t.Context(), key)
	if err != nil || string(read.Metadata) != `{"cursor":"page2"}` {
		t.Fatal(err)
	}
	var before string
	s.db.QueryRow(`SELECT xmin::text FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND entry_key='b'`, observationCheckpointKey(key)).Scan(&before)
	ctx, cancel := context.WithCancel(t.Context())
	_, err = s.UpdateObservationPages(ctx, key, func(p ObservationPages, _ []byte) (ObservationPages, error) {
		p.Metadata = []byte(`{"cursor":"bad"}`)
		p.Pages["a"] = []byte("changed")
		cancel()
		return p, nil
	}, legacy)
	if err == nil {
		t.Fatal("canceled page advanced cursor")
	}
	read, _, err = s.ReadObservationPages(t.Context(), key)
	if err != nil || string(read.Metadata) != `{"cursor":"page2"}` || string(read.Pages["a"]) != "page-a" {
		t.Fatal("partial checkpoint committed", err)
	}
	_, err = s.UpdateObservationPages(t.Context(), key, func(p ObservationPages, _ []byte) (ObservationPages, error) {
		ok, err := s.UpdateObservationPages(t.Context(), key, func(ObservationPages, []byte) (ObservationPages, error) {
			t.Fatal("replica entered held checkpoint")
			return ObservationPages{}, nil
		}, legacy)
		if err != nil || ok {
			t.Fatal("replica lock failed", ok, err)
		}
		p.Metadata = []byte(`{"cursor":"page3"}`)
		p.Pages["a"] = []byte("changed")
		return p, nil
	}, legacy)
	if err != nil {
		t.Fatal(err)
	}
	var unchanged string
	s.db.QueryRow(`SELECT xmin::text FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND entry_key='b'`, observationCheckpointKey(key)).Scan(&unchanged)
	if before != unchanged {
		t.Fatal("unchanged page rewritten")
	}
	var legacyValue string
	s.db.QueryRow(`SELECT value FROM fugue_meta WHERE key=$1`, observationCheckpointKey(key)).Scan(&legacyValue)
	if legacyValue != string(raw) {
		t.Fatal("legacy checkpoint overwritten")
	}
}

func TestChunkedCompilerInputDistinctConcurrentWriters(t *testing.T) {
	s := boundedContentTestStore(t)
	a, ad := chunkedInputFixture(t)
	b, bd := chunkedInputFixture(t)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, item := range []struct {
		snapshot platformconfig.RuntimeSnapshot
		digest   string
	}{{a, ad}, {b, bd}} {
		go func() { <-start; results <- s.EnsurePlatformCompilerInput(item.snapshot, item.digest) }()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	for _, digest := range []string{ad, bd} {
		if _, err := s.GetPlatformCompilerInput(digest); err != nil {
			t.Fatal(err)
		}
	}
}
