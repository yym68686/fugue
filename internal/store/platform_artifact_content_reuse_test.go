package store

import (
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPlatformArtifactContentReusePreservesOriginalAndRejectsCorruption(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "store.json"))
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	first, err := s.CreatePlatformArtifact(platformArtifactEnsureFixture("content-first", 100))
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.GetPlatformArtifactContent(first.ContentHash)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.CreatePlatformArtifact(platformArtifactEnsureFixture("content-second", 100))
	if err != nil || second.ID == first.ID || second.GenerationSequence != first.GenerationSequence+1 {
		t.Fatal("distinct signed generations must still be created", second, err)
	}
	after, err := s.GetPlatformArtifactContent(first.ContentHash)
	if err != nil || !after.CreatedAt.Equal(before.CreatedAt) || !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatal("reusing immutable bytes changed their original timestamps", err)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		for i := range state.PlatformArtifactContents {
			if state.PlatformArtifactContents[i].ContentHash == first.ContentHash {
				state.PlatformArtifactContents[i].Content = map[string]any{"corrupt": true}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreatePlatformArtifact(platformArtifactEnsureFixture("content-third", 100)); !errors.Is(err, ErrConflict) {
		t.Fatal("existing corrupt content was silently repaired", err)
	}
	after, err = s.GetPlatformArtifactContent(first.ContentHash)
	if err != nil || after.Content["corrupt"] != true {
		t.Fatal("a refused create overwrote the original stored evidence", err)
	}
}

// This test uses an existing disposable loopback database, rolls back the main
// transaction and removes its one committed concurrency fixture. It installs
// no schema and never touches compiler-input storage.
func TestPlatformArtifactContentPostgresDoesNotRewriteExistingBytes(t *testing.T) {
	dsn := os.Getenv("FUGUE_ARTIFACT_CONTENT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires an existing disposable loopback PostgreSQL database")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires an existing disposable loopback fugue_test database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var waiterPID int
	if err := tx.QueryRow(`SELECT pg_backend_pid()`).Scan(&waiterPID); err != nil {
		t.Fatal(err)
	}
	// An incompressible payload exercises TOAST, not just small inline rows.
	var payload strings.Builder
	for range 2048 {
		payload.WriteString(model.NewID("content"))
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	a := model.PlatformArtifact{Content: map[string]any{"payload": payload.String()}, CreatedAt: now, UpdatedAt: now}
	a.ContentHash, err = platformArtifactContentHash(a.Content)
	if err != nil {
		t.Fatal(err)
	}
	content := buildPlatformArtifactContent(a)
	first, err := pgUpsertPlatformArtifactContent(t.Context(), tx, content)
	if err != nil {
		t.Fatal(err)
	}
	var originalTuple, originalVersion string
	if err := tx.QueryRow(`SELECT ctid::text,xmin::text FROM fugue_platform_artifact_contents WHERE content_hash=$1`, content.ContentHash).Scan(&originalTuple, &originalVersion); err != nil {
		t.Fatal(err)
	}
	content.UpdatedAt = now.Add(time.Hour)
	content.CreatedAt = now.Add(time.Minute)
	for range 8 {
		got, err := pgUpsertPlatformArtifactContent(t.Context(), tx, content)
		if err != nil || !got.CreatedAt.Equal(first.CreatedAt) || !got.UpdatedAt.Equal(first.UpdatedAt) {
			t.Fatal("duplicate content changed original timestamps", err)
		}
	}
	var currentTuple, currentVersion string
	if err := tx.QueryRow(`SELECT ctid::text,xmin::text FROM fugue_platform_artifact_contents WHERE content_hash=$1`, content.ContentHash).Scan(&currentTuple, &currentVersion); err != nil {
		t.Fatal(err)
	}
	if currentTuple != originalTuple || currentVersion != originalVersion {
		t.Fatal("duplicate immutable content allocated a replacement heap tuple")
	}
	// A competing insert may commit after ON CONFLICT's statement snapshot.
	// The waiter must read that winner without replacing its bytes or times.
	concurrent, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer concurrent.Rollback()
	other := content
	other.Content = map[string]any{"concurrent": model.NewID("content")}
	other.ContentHash, err = platformArtifactContentHash(other.Content)
	if err != nil {
		t.Fatal(err)
	}
	other = buildPlatformArtifactContent(model.PlatformArtifact{ContentHash: other.ContentHash, Content: other.Content, CreatedAt: now, UpdatedAt: now})
	if _, err := pgUpsertPlatformArtifactContent(t.Context(), concurrent, other); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := db.Exec(`DELETE FROM fugue_platform_artifact_contents WHERE content_hash=$1`, other.ContentHash); err != nil {
			t.Error("remove exact owned test row", err)
		}
	}()
	type result struct {
		content model.PlatformArtifactContent
		err     error
	}
	complete := make(chan result, 1)
	other.UpdatedAt = now.Add(time.Hour)
	go func() {
		got, err := pgUpsertPlatformArtifactContent(t.Context(), tx, other)
		complete <- result{got, err}
	}()
	waiting := false
	for until := time.Now().Add(200 * time.Millisecond); time.Now().Before(until); {
		if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='transactionid' AND NOT granted)`, waiterPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !waiting {
		_ = concurrent.Rollback()
		<-complete
		t.Fatal("competing insert did not wait for the original transaction")
	}
	if err := concurrent.Commit(); err != nil {
		t.Fatal(err)
	}
	got := <-complete
	if got.err != nil || got.content.ContentHash != other.ContentHash || !got.content.CreatedAt.Equal(now) || !got.content.UpdatedAt.Equal(now) {
		t.Fatal("concurrent committed original was not reused", got.err)
	}
	for _, corrupt := range []string{"payload", "size"} {
		if _, err := tx.Exec(`SAVEPOINT content_corruption`); err != nil {
			t.Fatal(err)
		}
		statement := `UPDATE fugue_platform_artifact_contents SET content_json='{"corrupt":true}' WHERE content_hash=$1`
		if corrupt == "size" {
			statement = `UPDATE fugue_platform_artifact_contents SET size_bytes=size_bytes+1 WHERE content_hash=$1`
		}
		if _, err := tx.Exec(statement, content.ContentHash); err != nil {
			t.Fatal(err)
		}
		if _, err := pgUpsertPlatformArtifactContent(t.Context(), tx, content); !errors.Is(err, ErrConflict) {
			t.Fatal("corrupt existing "+corrupt+" was overwritten", err)
		}
		if _, err := tx.Exec(`ROLLBACK TO SAVEPOINT content_corruption`); err != nil {
			t.Fatal(err)
		}
	}
}
