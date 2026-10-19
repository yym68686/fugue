package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestCompilerInputWritesBoundedBatchesWithoutReusingEncoderBytes(t *testing.T) {
	for _, size := range []int{1, 15 * 64, 16 * 64, 16*64 + 1, 33*64 + 17} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			raw := make([]byte, size)
			for i := range raw {
				raw[i] = byte(i*37 + i/64)
			}
			sum := sha256.Sum256(raw)
			digest := "sha256:" + hex.EncodeToString(sum[:])
			count := (size + 63) / 64
			for first := 0; first < count; first += 16 {
				last := min(first+16, count)
				query := "INSERT INTO fugue_platform_input_chunks(content_hash,ordinal,chunk_hash,data) VALUES"
				args := []driver.Value{digest}
				for ordinal := first; ordinal < last; ordinal++ {
					index := ordinal - first
					if index > 0 {
						query += ","
					}
					query += fmt.Sprintf("($1,$%d,$%d,substring($%d::bytea from $%d::int for $%d::int))", 2+index*4, 3+index*4, 2+4*(last-first), 4+index*4, 5+index*4)
					data := raw[ordinal*64 : min((ordinal+1)*64, size)]
					h := sha256.Sum256(data)
					args = append(args, int64(ordinal), "sha256:"+hex.EncodeToString(h[:]), int64(index*64+1), int64(len(data)))
				}
				args = append(args, bytes.Clone(raw[first*64:min(last*64, size)]))
				mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, int64(last-first)))
			}
			w := &inputChunkWriter{ctx: context.Background(), tx: tx, digest: digest, hash: sha256.New(), chunk: make([]byte, 0, 64)}
			// Distinct short writes and overwritten caller buffers expose both encoder
			// and input-buffer aliasing, including exact and partial batch boundaries.
			for offset := 0; offset < len(raw); {
				end := min(offset+29, len(raw))
				part := bytes.Clone(raw[offset:end])
				n, err := w.Write(part)
				if err != nil || n != len(part) {
					t.Fatal("write failed", n, err)
				}
				clear(part)
				offset = end
				if len(w.pending) >= 16 {
					t.Fatal("completed batch retained")
				}
			}
			if err := w.flush(); err != nil {
				t.Fatal(err)
			}
			if w.count != count || w.size != int64(size) || len(w.pending) != 0 || !bytes.Equal(w.hash.Sum(nil), sum[:]) {
				t.Fatal("stream identity changed")
			}
			for i, h := range w.expected {
				if want := sha256.Sum256(raw[i*64 : min((i+1)*64, size)]); h != want {
					t.Fatal("chunk hash changed", i)
				}
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompilerInputBatchStatementFailureStopsEncoding(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("synthetic batch failure")
	mock.ExpectExec("INSERT INTO fugue_platform_input_chunks").WillReturnError(failure)
	w := &inputChunkWriter{ctx: context.Background(), tx: tx, digest: "input", hash: sha256.New(), chunk: make([]byte, 0, 64)}
	n, err := w.Write(bytes.Repeat([]byte{17}, 32*64))
	if !errors.Is(err, failure) || n != 16*64 || len(w.pending) != 16 {
		t.Fatalf("batch failure not propagated: bytes=%d pending=%d error=%v", n, len(w.pending), err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
