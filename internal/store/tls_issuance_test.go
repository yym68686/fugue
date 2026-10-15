package store

import (
	"context"
	"errors"
	"fugue/internal/tlsissuance"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTLSPrivateStatePersistsAcrossIssuerTakeover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	a, b := New(path), New(path)
	ctx := context.Background()
	until := time.Now().Add(24 * time.Hour).UTC()
	if err := a.UpdateTLSIssuanceState(ctx, "owner-a", "https://ca.example/directory", "site.example.test", func(s *tlsissuance.State) error {
		if err := b.UpdateTLSIssuanceState(ctx, "owner-b", "https://ca.example/directory", "site.example.test", func(*tlsissuance.State) error { t.Fatal("concurrent owner bypassed lock"); return nil }); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
		s.NextAttempt = until
		s.AccountKeyPEM = "private-key-fixture"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := b.ReadTLSIssuanceState(ctx, "new-owner", "https://ca.example/directory", "site.example.test")
	if err != nil || !got.NextAttempt.Equal(until) || got.AccountKeyPEM != "private-key-fixture" {
		t.Fatal("takeover reset state", got, err)
	}
	file := a.tlsIssuanceRecordPath(tlsIssuanceStateKey("ignored", "https://ca.example/directory", "site.example.test"))
	info, err := os.Stat(file)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("private state permission", info, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	err = b.UpdateTLSIssuanceState(canceled, "owner", "https://ca.example/directory", "site.example.test", func(s *tlsissuance.State) error { cancel(); s.NextAttempt = time.Time{}; return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	got, _ = a.ReadTLSIssuanceState(ctx, "owner", "https://ca.example/directory", "site.example.test")
	if !got.NextAttempt.Equal(until) {
		t.Fatal("canceled transaction reset backoff")
	}
}
