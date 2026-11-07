package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func verifiedPinAdmissionFixture(t *testing.T) (*sql.DB, string, compilerInputPinBinding) {
	t.Helper()
	db, address, _ := hotInventoryTestDatabase(t)
	digest := seedHotInventoryHistory(t, db)
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_hot_chunks SELECT * FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_input_manifests SET storage_location='hot' WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "true")
	pin, err := RunCompilerInputLocalPin(t.Context(), address, CompilerInputLocalPinOptions{Key: "activation/neutral", Digest: digest, MaxRawBytes: 1024, Apply: true})
	if err != nil || !pin.Verified {
		t.Fatal(pin, err)
	}
	return db, address, compilerInputPinBinding{Key: pin.Key, Token: pin.Token, Digest: digest}
}

func TestVerifiedPinAdmissionRejectsInvalidBindingsWithoutDatabase(t *testing.T) {
	valid := compilerInputPinBinding{Key: "activation/neutral", Token: 1, Digest: "sha256:" + strings.Repeat("a", 64)}
	if err := lockCompilerInputVerifiedPins(t.Context(), nil, []compilerInputPinBinding{valid}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("nil transaction accepted", err)
	}
	for _, bindings := range [][]compilerInputPinBinding{nil, {}, {{Key: "bad\nkey", Token: 1, Digest: valid.Digest}}, {{Key: valid.Key, Digest: valid.Digest}}, {{Key: valid.Key, Token: 1, Digest: "invalid"}}, {valid, valid}, make([]compilerInputPinBinding, 33)} {
		// An uninitialized transaction is intentional: any database access
		// here would panic instead of hiding behind the nil-tx check.
		if err := lockCompilerInputVerifiedPins(t.Context(), &sql.Tx{}, bindings); !errors.Is(err, ErrInvalidInput) {
			t.Fatal("invalid bindings reached database", err)
		}
	}
}

func TestVerifiedPinAdmissionPostgresDurableAcrossConnectionsAndNoReleaseRace(t *testing.T) {
	db, address, binding := verifiedPinAdmissionFixture(t)
	// A fresh connection consumes the committed readback, not process memory.
	reopened, err := openPostgresDatabase(address)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	tx, err := reopened.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = lockCompilerInputVerifiedPins(t.Context(), tx, []compilerInputPinBinding{binding}); err != nil {
		t.Fatal(err)
	}
	// Another activation can share the barrier; ordinary writers are not
	// serialized behind an exclusive activation lock.
	other, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Rollback()
	if err = lockCompilerInputVerifiedPins(t.Context(), other, []compilerInputPinBinding{binding}); err != nil {
		t.Fatal("shared activation blocked", err)
	}
	if err = other.Commit(); err != nil {
		t.Fatal(err)
	}
	release := CompilerInputLocalPinOptions{Key: binding.Key, Digest: binding.Digest, Apply: true, Release: true, ExpectedToken: binding.Token}
	started := time.Now()
	if _, err = RunCompilerInputLocalPin(t.Context(), address, release); err == nil || time.Since(started) > 2*time.Second {
		t.Fatal("pin released during protected transaction or lock budget exceeded", err)
	}
	var state string
	if err = tx.QueryRow(`SELECT state FROM fugue_platform_input_local_pins WHERE pin_key=$1`, binding.Key).Scan(&state); err != nil || state != "verified" {
		t.Fatal("failed release changed pin", state, err)
	}
	// Stand in for an activation's durable row mutation, in the SAME txn.
	if _, err = tx.Exec(`INSERT INTO fugue_meta(key,value) VALUES('pin-admission-test','committed')`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = RunCompilerInputLocalPin(t.Context(), address, release); err != nil {
		t.Fatal("pin not releasable after transaction ended", err)
	}
	var value string
	if err = db.QueryRow(`SELECT value FROM fugue_meta WHERE key='pin-admission-test'`).Scan(&value); err != nil || value != "committed" {
		t.Fatal(value, err)
	}
	// This primitive protects through commit, not for the future activation
	// lifetime. The eventual caller must also install its retention root.
	tx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = lockCompilerInputVerifiedPins(t.Context(), tx, []compilerInputPinBinding{binding}); !errors.Is(err, ErrConflict) {
		t.Fatal("released tombstone admitted", err)
	}
}

func TestVerifiedPinAdmissionPostgresRejectsEveryChangedWitness(t *testing.T) {
	for name, change := range map[string]string{
		"pending":          `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`,
		"epoch":            `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`,
		"representation":   `UPDATE fugue_platform_input_manifests SET chunk_size=chunk_size-1`,
		"legacy-source":    `UPDATE fugue_platform_input_manifests SET storage_location='legacy'`,
		"missing-pin":      `DELETE FROM fugue_platform_input_local_pins`,
		"schema-reference": `ALTER TABLE fugue_platform_input_local_pins DROP CONSTRAINT fugue_platform_input_local_pins_protected_content_hash_fkey`,
	} {
		t.Run(name, func(t *testing.T) {
			db, _, binding := verifiedPinAdmissionFixture(t)
			if _, err := db.Exec(change); err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err = lockCompilerInputVerifiedPins(t.Context(), tx, []compilerInputPinBinding{binding}); err == nil {
				t.Fatal("changed durable witness admitted")
			}
		})
	}
	for _, name := range []string{"token", "digest", "missing-key"} {
		t.Run(name, func(t *testing.T) {
			db, _, binding := verifiedPinAdmissionFixture(t)
			switch name {
			case "token":
				binding.Token++
			case "digest":
				binding.Digest = "sha256:" + strings.Repeat("b", 64)
			case "missing-key":
				binding.Key += "/missing"
			}
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err = lockCompilerInputVerifiedPins(t.Context(), tx, []compilerInputPinBinding{binding}); !errors.Is(err, ErrConflict) {
				t.Fatal("wrong exact pin admitted", err)
			}
		})
	}
}

func TestVerifiedPinAdmissionPostgresPreservesStricterBudgetsAndRejectsOldSnapshot(t *testing.T) {
	db, _, binding := verifiedPinAdmissionFixture(t)
	for _, options := range []*sql.TxOptions{{Isolation: sql.LevelRepeatableRead}, {ReadOnly: true}} {
		tx, err := db.BeginTx(t.Context(), options)
		if err != nil {
			t.Fatal(err)
		}
		err = lockCompilerInputVerifiedPins(t.Context(), tx, []compilerInputPinBinding{binding})
		_ = tx.Rollback()
		if !errors.Is(err, ErrConflict) {
			t.Fatal("old/read-only transaction admitted", err)
		}
	}
	for _, limit := range []string{"0", "100ms", "10s"} {
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(`SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$1,true)`, limit); err != nil {
			t.Fatal(err)
		}
		if err = lockCompilerInputVerifiedPins(t.Context(), tx, []compilerInputPinBinding{binding}); err != nil {
			t.Fatal(err)
		}
		var lockMS, statementMS int
		if err = tx.QueryRow(`SELECT (SELECT setting::integer FROM pg_settings WHERE name='lock_timeout'),(SELECT setting::integer FROM pg_settings WHERE name='statement_timeout')`).Scan(&lockMS, &statementMS); err != nil {
			t.Fatal(err)
		}
		wantLock, wantStatement := 250, 5000
		if limit == "100ms" {
			wantLock, wantStatement = 100, 100
		}
		if lockMS != wantLock || statementMS != wantStatement {
			t.Fatal("caller timeout relaxed", limit, lockMS, statementMS)
		}
		_ = tx.Rollback()
	}
}

func TestVerifiedPinAdmissionPostgresYieldsToWriterAndCancellation(t *testing.T) {
	db, _, binding := verifiedPinAdmissionFixture(t)
	writer, err := beginCompilerInputPinMutation(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = lockCompilerInputVerifiedPins(t.Context(), tx, []compilerInputPinBinding{binding})
	_ = tx.Rollback()
	if err == nil || time.Since(started) > 2*time.Second {
		t.Fatal("activation bypassed writer or retried", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	tx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = lockCompilerInputVerifiedPins(ctx, tx, []compilerInputPinBinding{binding}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled activation admitted", err)
	}
}

func TestVerifiedPinAdmissionPostgresMultiPinFailureRollsBackActivation(t *testing.T) {
	db, address, first := verifiedPinAdmissionFixture(t)
	second, err := RunCompilerInputLocalPin(t.Context(), address, CompilerInputLocalPinOptions{Key: "activation/second", Digest: first.Digest, MaxRawBytes: 1024, Apply: true})
	if err != nil || !second.Verified {
		t.Fatal(second, err)
	}
	bindings := []compilerInputPinBinding{{Key: second.Key, Token: second.Token, Digest: second.Digest}, first}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = lockCompilerInputVerifiedPins(t.Context(), tx, bindings); err != nil {
		t.Fatal("multiple independently owned pins rejected", err)
	}
	if bindings[0].Key != second.Key {
		t.Fatal("input bindings mutated by lock ordering")
	}
	_ = tx.Rollback()
	if _, err = db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE pin_key=$1`, second.Key); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO fugue_meta(key,value) VALUES('must-not-activate','pending')`); err != nil {
		t.Fatal(err)
	}
	if err = lockCompilerInputVerifiedPins(t.Context(), tx, bindings); !errors.Is(err, ErrConflict) {
		t.Fatal("partial verified subset accepted", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM fugue_meta WHERE key='must-not-activate')`).Scan(&exists); err != nil || exists {
		t.Fatal("failed admission persisted activation", exists, err)
	}
}
