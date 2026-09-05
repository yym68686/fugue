package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/config"
)

func TestRejectedRouteCandidatePreservesServingLKG(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"schema", "signature", "expired", "revoked-key", "wrong-group"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			now := time.Now().UTC()
			incoming := testEdgeControlCacheFile("candidate", 3, 0, now, time.Hour).Bundle
			switch failure {
			case "schema":
				incoming.SchemaVersion = "999.0"
			case "signature":
				incoming.Version = "tampered"
			case "expired":
				incoming = testEdgeControlCacheFile("candidate", 3, 0, now.Add(-2*time.Hour), time.Hour).Bundle
			case "revoked-key":
				incoming = bundleauth.SignEdgeRouteBundle(incoming, "retired-key-material-0123456789012", "retired-key", time.Hour)
			case "wrong-group":
				incoming.EdgeGroupID = "edge-group-other"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeTestEdgeControlBundle(w, incoming, testCacheEdgeGroupID, 3)
			}))
			defer server.Close()
			source := newTestEdgeControlRouteSource(t, server.URL, testCacheEdgeGroupID)
			if failure == "revoked-key" {
				raw, err := os.ReadFile(source.VerifierKeyringFile)
				if err != nil {
					t.Fatal(err)
				}
				var keyring edgeRouteVerifierKeyringFile
				if err := json.Unmarshal(raw, &keyring); err != nil {
					t.Fatal(err)
				}
				keyring.Group.RevokedKeyIDs = []string{"retired-key"}
				raw, err = json.Marshal(keyring)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(source.VerifierKeyringFile, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg := config.EdgeConfig{APIURL: server.URL, EdgeToken: "synthetic-token", EdgeGroupID: testCacheEdgeGroupID, CachePath: filepath.Join(t.TempDir(), "cache.json"), CacheArchiveLimit: 3, MaxStale: time.Hour}
			newService := func() *Service { return NewServiceWithRouteBundleSource(cfg, source, log.New(ioDiscard{}, "", 0)) }
			s := newService()
			for index, generation := range []string{"previous", "current"} {
				if err := s.writeCache(testEdgeControlCacheFile(generation, uint64(index+1), 0, now, time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.LoadCache(); err != nil {
				t.Fatal(err)
			}
			publication, bundle := s.currentRoutePublicationAndBundle()
			before, err := os.ReadFile(cfg.CachePath)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.SyncOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "verify edge route bundle") {
				t.Fatalf("want candidate verification rejection, got %v", err)
			}
			gotPublication, gotBundle := s.currentRoutePublicationAndBundle()
			if gotPublication != publication || gotBundle.Generation != bundle.Generation || s.Status().ServingGeneration != "current" || s.Status().LastError == "" {
				t.Fatalf("rejection changed serving LKG or lost failure: publication=%+v status=%+v", gotPublication, s.Status())
			}
			after, err := os.ReadFile(cfg.CachePath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("rejection changed durable LKG: %v", err)
			}
			restarted := newService()
			if err := restarted.LoadCache(); err != nil || restarted.Status().ServingGeneration != "current" {
				t.Fatalf("restart lost current LKG: %v %+v", err, restarted.Status())
			}
			// Historical recovery remains a separate, explicit operation.
			if err := restarted.LoadPreviousCache(); err != nil || restarted.Status().ServingGeneration != "previous" {
				t.Fatalf("explicit recovery unavailable: %v %+v", err, restarted.Status())
			}
			// Sync is not startup recovery, even when old cache files exist.
			empty := newService()
			if err := empty.SyncOnce(context.Background()); err == nil || empty.hasBundle() {
				t.Fatalf("candidate rejection implicitly loaded history: %v", err)
			}
		})
	}
}
