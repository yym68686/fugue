package store

import (
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestReadObservationPagesReusesUnchangedBodiesAfterHashCheck(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := "observation-cache-test"
	cache := newObservationPageBodyCache(1<<20, 10)
	a, b := []byte("page-a"), []byte("page-b")
	hash := func(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
	head := regexp.QuoteMeta(`SELECT revision,metadata FROM fugue_observation_checkpoint_heads WHERE checkpoint_key=$1`)
	hashes := regexp.QuoteMeta(`SELECT entry_key,octet_length(value),encode(sha256(value),'hex') FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' ORDER BY entry_key`)
	bodies := regexp.QuoteMeta(`SELECT entry_key,value FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' AND entry_key IN ($2, $3)`)
	for i := 0; i < 2; i++ {
		mock.ExpectQuery(head).WithArgs(key).WillReturnRows(sqlmock.NewRows([]string{"revision", "metadata"}).AddRow(1, []byte(`{"cursor":"x"}`)))
		mock.ExpectQuery(hashes).WithArgs(key).WillReturnRows(sqlmock.NewRows([]string{"entry_key", "size", "hash"}).AddRow("a", len(a), hash(a)).AddRow("b", len(b), hash(b)))
		if i == 0 {
			mock.ExpectQuery(bodies).WithArgs(key, "a", "b").WillReturnRows(sqlmock.NewRows([]string{"entry_key", "value"}).AddRow("a", a).AddRow("b", b))
		}
		got, _, err := readObservationPagesWithCache(t.Context(), db, key, cache)
		if err != nil || string(got.Pages["a"]) != string(a) || string(got.Pages["b"]) != string(b) {
			t.Fatalf("read %d: pages=%v err=%v", i, got.Pages, err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReadObservationPagesRejectsInvalidOrMissingRows(t *testing.T) {
	for _, scenario := range []string{"bad_hash", "negative_size", "large_page", "long_key", "duplicate_reference", "total_budget", "reference_error", "missing_body", "duplicate_body", "unknown_body", "wrong_size", "body_error"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			cache := newObservationPageBodyCache(1<<20, 10)
			key, page, size := "negative-case", "a", 4
			sum := sha256.Sum256([]byte("body"))
			hash := hex.EncodeToString(sum[:])
			switch scenario {
			case "bad_hash":
				hash = "not-a-hash"
			case "negative_size":
				size = -1
			case "large_page":
				size = (64 << 10) + 1
			case "long_key":
				page = strings.Repeat("x", 4097)
			case "wrong_size":
				size = 3
			}
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT revision,metadata FROM fugue_observation_checkpoint_heads WHERE checkpoint_key=$1`)).WithArgs(key).WillReturnRows(sqlmock.NewRows([]string{"revision", "metadata"}).AddRow(1, []byte("cursor")))
			refs := sqlmock.NewRows([]string{"key", "size", "hash"}).AddRow(page, size, hash)
			if scenario == "duplicate_reference" {
				refs.AddRow(page, size, hash)
			}
			if scenario == "total_budget" {
				for i := 0; i < 257; i++ {
					refs.AddRow(fmt.Sprint(i), 64<<10, hash)
				}
			}
			if scenario == "reference_error" {
				refs.RowError(0, errors.New("synthetic row failure"))
			}
			mock.ExpectQuery(regexp.QuoteMeta(observationPageReferencesSQL)).WithArgs(key).WillReturnRows(refs).RowsWillBeClosed()
			switch scenario {
			case "missing_body", "duplicate_body", "unknown_body", "wrong_size", "body_error":
				rows := sqlmock.NewRows([]string{"key", "value"})
				if scenario != "missing_body" {
					rows.AddRow(page, []byte("body"))
				}
				if scenario == "duplicate_body" {
					rows.AddRow(page, []byte("body"))
				}
				if scenario == "unknown_body" {
					rows.AddRow("foreign", []byte("body"))
				}
				if scenario == "body_error" {
					rows.RowError(0, errors.New("synthetic row failure"))
				}
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT entry_key,value FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' AND entry_key IN ($2)`)).WithArgs(key, page).WillReturnRows(rows).RowsWillBeClosed()
			}
			if _, _, err := readObservationPagesWithCache(t.Context(), db, key, cache); err == nil {
				t.Fatal("invalid checkpoint accepted")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReadObservationPagesBatchesColdBodies(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := "batch-checkpoint"
	cache := newObservationPageBodyCache(1<<20, 200)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT revision,metadata FROM fugue_observation_checkpoint_heads WHERE checkpoint_key=$1`)).WithArgs(key).WillReturnRows(sqlmock.NewRows([]string{"revision", "metadata"}).AddRow(1, []byte("cursor")))
	refs := sqlmock.NewRows([]string{"key", "size", "hash"})
	for i := 0; i < 129; i++ {
		raw := []byte(fmt.Sprint(i))
		sum := sha256.Sum256(raw)
		refs.AddRow(fmt.Sprintf("page-%03d", i), len(raw), hex.EncodeToString(sum[:]))
	}
	mock.ExpectQuery(regexp.QuoteMeta(observationPageReferencesSQL)).WithArgs(key).WillReturnRows(refs).RowsWillBeClosed()
	for start := 0; start < 129; start += 128 {
		count := min(128, 129-start)
		args := []driver.Value{key}
		rows := sqlmock.NewRows([]string{"key", "value"})
		for i := start; i < start+count; i++ {
			name := fmt.Sprintf("page-%03d", i)
			args = append(args, name)
			rows.AddRow(name, []byte(fmt.Sprint(i)))
		}
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT entry_key,value FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' AND entry_key IN (` + sqlPlaceholderList(2, count) + `)`)).WithArgs(args...).WillReturnRows(rows).RowsWillBeClosed()
	}
	got, _, err := readObservationPagesWithCache(t.Context(), db, key, cache)
	if err != nil || len(got.Pages) != 129 || string(got.Pages["page-128"]) != "128" {
		t.Fatal("bounded read lost pages", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestObservationPageCacheOwnsBuffersAndEnforcesBounds(t *testing.T) {
	c := newObservationPageBodyCache(1024, 2)
	raw := []byte("original")
	c.put("a", "hash-a", raw)
	raw[0] = 'X'
	got, ok := c.get("a", "hash-a")
	if !ok || string(got) != "original" {
		t.Fatal("cache retained caller-owned buffer")
	}
	got[0] = 'Y'
	got, _ = c.get("a", "hash-a")
	if string(got) != "original" {
		t.Fatal("read mutated cached buffer")
	}
	if _, ok := c.get("a", "different"); ok {
		t.Fatal("stale hash accepted")
	}
	c.put("b", "hash-b", []byte("b"))
	c.get("a", "hash-a")
	c.put("c", "hash-c", []byte("c"))
	if _, ok := c.get("b", "hash-b"); ok {
		t.Fatal("entry bound not enforced")
	}
	c.put("huge", "hash", make([]byte, 1024))
	if _, ok := c.get("huge", "hash"); ok || c.bytes > c.maxBytes {
		t.Fatal("byte bound ignored overhead")
	}
	disabled := newObservationPageBodyCache(0, 0)
	disabled.put("empty", "hash", []byte{})
	if len(disabled.entries) != 0 {
		t.Fatal("disabled cache retained data")
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			key := fmt.Sprint(worker)
			for i := 0; i < 100; i++ {
				c.put(key, "same", []byte("body"))
				c.get(key, "same")
			}
		}(worker)
	}
	wg.Wait()
	if len(c.entries) > c.maxEntries || c.bytes > c.maxBytes {
		t.Fatal("concurrent writes exceed bounds")
	}
}

func TestReadObservationPagesRefreshesOnlyChangedMembership(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cache := newObservationPageBodyCache(1<<20, 10)
	key := "changing-checkpoint"
	hash := func(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
	cache.put(key+"\x00unchanged", hash([]byte("stable")), []byte("stable"))
	cache.put(key+"\x00changed", hash([]byte("before")), []byte("before"))
	cache.put(key+"\x00deleted", hash([]byte("gone")), []byte("gone"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT revision,metadata FROM fugue_observation_checkpoint_heads WHERE checkpoint_key=$1`)).WithArgs(key).WillReturnRows(sqlmock.NewRows([]string{"revision", "metadata"}).AddRow(2, []byte("new cursor")))
	mock.ExpectQuery(regexp.QuoteMeta(observationPageReferencesSQL)).WithArgs(key).WillReturnRows(sqlmock.NewRows([]string{"key", "size", "hash"}).AddRow("changed", 5, hash([]byte("after"))).AddRow("empty", 0, hash([]byte{})).AddRow("unchanged", 6, hash([]byte("stable"))))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT entry_key,value FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' AND entry_key IN ($2, $3)`)).WithArgs(key, "changed", "empty").WillReturnRows(sqlmock.NewRows([]string{"key", "value"}).AddRow("empty", []byte{}).AddRow("changed", []byte("after")))
	got, revision, err := readObservationPagesWithCache(t.Context(), db, key, cache)
	if err != nil || revision != 2 || string(got.Metadata) != "new cursor" || len(got.Pages) != 3 || string(got.Pages["changed"]) != "after" || string(got.Pages["unchanged"]) != "stable" {
		t.Fatalf("membership/body refresh failed: %v %d %v", got, revision, err)
	}
	if _, ok := got.Pages["empty"]; !ok {
		t.Fatal("empty page lost")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReadObservationPagesRejectsBodyHashMismatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := "observation-cache-hash-mismatch"
	expected := []byte("expected")
	sum := sha256.Sum256(expected)
	head := regexp.QuoteMeta(`SELECT revision,metadata FROM fugue_observation_checkpoint_heads WHERE checkpoint_key=$1`)
	hashes := regexp.QuoteMeta(`SELECT entry_key,octet_length(value),encode(sha256(value),'hex') FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' ORDER BY entry_key`)
	bodies := regexp.QuoteMeta(`SELECT entry_key,value FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' AND entry_key IN ($2)`)
	mock.ExpectQuery(head).WithArgs(key).WillReturnRows(sqlmock.NewRows([]string{"revision", "metadata"}).AddRow(1, []byte(`{"cursor":"x"}`)))
	mock.ExpectQuery(hashes).WithArgs(key).WillReturnRows(sqlmock.NewRows([]string{"entry_key", "size", "hash"}).AddRow("a", len(expected), hex.EncodeToString(sum[:])))
	mock.ExpectQuery(bodies).WithArgs(key, "a").WillReturnRows(sqlmock.NewRows([]string{"entry_key", "value"}).AddRow("a", []byte("changed!")))
	if _, _, err := readObservationPagesWithCache(t.Context(), db, key, newObservationPageBodyCache(1<<20, 10)); err == nil {
		t.Fatal("accepted page body whose hash differed from the database summary")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestObservationPagesCachePostgresFreshBytesAndSnapshot(t *testing.T) {
	s := boundedContentTestStore(t)
	s.db.SetMaxOpenConns(4)
	key := fmt.Sprintf("page-cache-%d", time.Now().UnixNano())
	dbKey := observationCheckpointKey(key)
	t.Cleanup(func() {
		s.db.Exec(`DELETE FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1`, dbKey)
		s.db.Exec(`DELETE FROM fugue_observation_checkpoint_heads WHERE checkpoint_key=$1`, dbKey)
	})
	legacy := func([]byte) ([]byte, error) { t.Fatal("unexpected legacy write"); return nil, nil }
	if _, err := s.UpdateObservationPages(t.Context(), key, func(ObservationPages, []byte) (ObservationPages, error) {
		return ObservationPages{Metadata: []byte("first"), Pages: map[string][]byte{"page": []byte("before")}}, nil
	}, legacy); err != nil {
		t.Fatal(err)
	}
	cache := newObservationPageBodyCache(1<<20, 10)
	if _, _, err := readObservationPagesWithCache(t.Context(), s.db, dbKey, cache); err != nil {
		t.Fatal(err)
	}
	// A content change without updating the head must still bypass cached bytes.
	if _, err := s.db.Exec(`UPDATE fugue_observation_checkpoint_entries SET value=$2 WHERE checkpoint_key=$1`, dbKey, []byte("tampered")); err != nil {
		t.Fatal(err)
	}
	got, revision, err := readObservationPagesWithCache(t.Context(), s.db, dbKey, cache)
	if err != nil || revision != 1 || string(got.Pages["page"]) != "tampered" {
		t.Fatalf("revision hid byte mutation: %v %d %v", got, revision, err)
	}
	tx, err := s.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	before, _, err := readObservationPagesWithCache(t.Context(), tx, dbKey, cache)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateObservationPages(t.Context(), key, func(p ObservationPages, _ []byte) (ObservationPages, error) {
		p.Metadata = []byte("second")
		p.Pages["page"] = []byte("new generation")
		return p, nil
	}, legacy); err != nil {
		t.Fatal(err)
	}
	current, _, err := readObservationPagesWithCache(t.Context(), s.db, dbKey, cache)
	if err != nil || string(current.Metadata) != "second" || string(current.Pages["page"]) != "new generation" {
		t.Fatal("new generation missing", err)
	}
	// Even after another read replaces the cache entry, this transaction must
	// fetch/return the old snapshot, never splice the newer cached body into it.
	pinned, pinnedRevision, err := readObservationPagesWithCache(t.Context(), tx, dbKey, cache)
	if err != nil || pinnedRevision != 1 || !reflect.DeepEqual(pinned, before) {
		t.Fatalf("mixed checkpoint generations: %v %d %v", pinned, pinnedRevision, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DELETE FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1`, dbKey); err != nil {
		t.Fatal(err)
	}
	deleted, _, err := readObservationPagesWithCache(t.Context(), s.db, dbKey, cache)
	if err != nil || len(deleted.Pages) != 0 {
		t.Fatal("cache hid deleted membership", err)
	}
}

func BenchmarkObservationPagesVerifiedReuse(b *testing.B) {
	s := boundedContentTestStore(b)
	key := fmt.Sprintf("page-benchmark-%d", time.Now().UnixNano())
	dbKey := observationCheckpointKey(key)
	b.Cleanup(func() {
		s.db.Exec("DELETE FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1", dbKey)
		s.db.Exec("DELETE FROM fugue_observation_checkpoint_heads WHERE checkpoint_key=$1", dbKey)
	})
	fixture := ObservationPages{Metadata: []byte("cursor"), Pages: map[string][]byte{}}
	for i := 0; i < 64; i++ {
		fixture.Pages[fmt.Sprintf("page-%03d", i)] = []byte(strings.Repeat("synthetic-page-", 4096))
	}
	_, err := s.UpdateObservationPages(b.Context(), key, func(ObservationPages, []byte) (ObservationPages, error) { return fixture, nil }, func([]byte) ([]byte, error) { return nil, nil })
	if err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"full_body", "verified_warm"} {
		b.Run(mode, func(b *testing.B) {
			cache := newObservationPageBodyCache(16<<20, 1024)
			if _, _, err := readObservationPagesWithCache(b.Context(), s.db, dbKey, cache); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				tx, err := s.db.BeginTx(b.Context(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
				if err != nil {
					b.Fatal(err)
				}
				if mode == "full_body" {
					var revision int64
					var metadata []byte
					err = tx.QueryRowContext(b.Context(), "SELECT revision,metadata FROM fugue_observation_checkpoint_heads WHERE checkpoint_key=$1", dbKey).Scan(&revision, &metadata)
					if err == nil {
						var rows *sql.Rows
						rows, err = tx.QueryContext(b.Context(), "SELECT entry_key,value FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' ORDER BY entry_key", dbKey)
						if err == nil {
							for rows.Next() {
								var k string
								var v []byte
								if err = rows.Scan(&k, &v); err != nil {
									break
								}
							}
							if err == nil {
								err = rows.Err()
							}
							rows.Close()
						}
					}
				} else {
					_, _, err = readObservationPagesWithCache(b.Context(), tx, dbKey, cache)
				}
				if err != nil {
					tx.Rollback()
					b.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
