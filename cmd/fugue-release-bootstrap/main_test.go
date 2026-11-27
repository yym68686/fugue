package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/productioncontrol"
)

func TestBootstrapUsesTamperEvidentWitnessPathsAndSeparateModes(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "bootstrap.json")
	config := settings{Mode: "settle", Actor: "fixture-observer", JournalFile: "/fixture/journal.json",
		WitnessDirectory: dir, ObserverPublicKey: make(ed25519.PublicKey, ed25519.PublicKeySize)}
	write := func() {
		t.Helper()
		raw, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configPath, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	if _, err := readSettings(configPath, "settle"); err != nil {
		t.Fatal(err)
	}
	if _, err := readSettings(configPath, "publish"); err == nil {
		t.Fatal("settlement profile enabled publication")
	}
	config.RegistryAuthFile = "/fixture/registry.json"
	write()
	if _, err := readSettings(configPath, "settle"); err == nil {
		t.Fatal("settlement profile accepted publication credentials")
	}
	a, b := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	r, err := parseRequest([]string{"settle", a, "restored", "8", a, b})
	if err != nil || r.outcome != productioncontrol.Restored || r.version.Generation != 8 {
		t.Fatalf("could not parse an exact rollback settlement: %+v, %v", r, err)
	}
	if _, err := parseRequest([]string{"settle", a, "unknown", "8", a, b}); err == nil {
		t.Fatal("unknown outcome was treated as terminal")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	source := directoryWitnesses{root}
	witness := filepath.Join(dir, strings.TrimPrefix(a, "sha256:")+".json")
	if err := os.WriteFile(witness, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if raw, err := source.ReadExecutionWitness(context.Background(), a); err != nil || string(raw) != "{}" {
		t.Fatalf("could not read content-addressed witness: %s, %v", raw, err)
	}
	foreign := filepath.Join(t.TempDir(), "witness.json")
	if err := os.WriteFile(foreign, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(foreign, filepath.Join(dir, strings.TrimPrefix(b, "sha256:")+".json")); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"../../witness.json", b} {
		if _, err := source.ReadExecutionWitness(context.Background(), invalid); err == nil {
			t.Fatal("observer read outside the independent witness mount")
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBootstrapObservationCannotWriteThroughTransport(t *testing.T) {
	requests := 0
	transport := observeTransport{roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		return &http.Response{StatusCode: 200}, nil
	})}
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodDelete, "upgrade"} {
		req, err := http.NewRequest(method, "https://provider.fixture.invalid", nil)
		if err != nil {
			t.Fatal(err)
		}
		allowed := method == http.MethodGet
		if method == "upgrade" {
			req.Method = http.MethodGet
			req.Header.Set("Upgrade", "websocket")
		}
		if _, err := transport.RoundTrip(req); (err == nil) != allowed {
			t.Fatalf("method %s allowed=%v error=%v", method, allowed, err)
		}
	}
	if requests != 1 {
		t.Fatalf("observation dispatched %d requests instead of a single GET", requests)
	}
}
