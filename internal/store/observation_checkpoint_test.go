package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestObservationCheckpointRestartLockAndCanceledWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	a, b := New(path), New(path)
	ok, err := a.UpdateObservationCheckpoint(t.Context(), "scan", func(raw []byte) ([]byte, error) {
		if len(raw) != 0 {
			t.Fatal("unexpected initial value")
		}
		acquired, e := b.UpdateObservationCheckpoint(t.Context(), "scan", func([]byte) ([]byte, error) { t.Fatal("concurrent callback ran"); return nil, nil })
		if e != nil || acquired {
			t.Fatalf("lock: %v %v", acquired, e)
		}
		return []byte(`{"page":1}`), nil
	})
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	raw, err := b.ReadObservationCheckpoint(t.Context(), "scan")
	if err != nil || string(raw) != `{"page":1}` {
		t.Fatal(string(raw), err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	_, err = b.UpdateObservationCheckpoint(ctx, "scan", func([]byte) ([]byte, error) { cancel(); return []byte(`{"page":2}`), nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	raw, _ = a.ReadObservationCheckpoint(t.Context(), "scan")
	if string(raw) != `{"page":1}` {
		t.Fatal("canceled update changed cursor")
	}
}
