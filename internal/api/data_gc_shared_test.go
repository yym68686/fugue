package api

import (
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/store"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDataGCProtectsOtherTenantAliasAndPendingUpload(t *testing.T) {
	old := time.Now().Add(-10 * 24 * time.Hour).UTC().Truncate(time.Second).Add(741 * time.Millisecond)
	digests := []string{testDataDigest("other-workspace"), testDataDigest("upload"), testDataDigest("orphan")}
	deleted := ""
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET":
			fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated>")
			for _, d := range digests {
				fmt.Fprintf(w, "<Contents><Key>shared/%s</Key><Size>1</Size><ETag>\"original\"</ETag><LastModified>%s</LastModified></Contents>", model.DataObjectKey(d), old.Format(time.RFC3339Nano))
			}
			fmt.Fprint(w, "</ListBucketResult>")
		case r.Method == "HEAD":
			w.Header().Set("Content-Length", "1")
			w.Header().Set("Last-Modified", old.Format(http.TimeFormat))
			w.Header().Set("ETag", `"original"`)
		case r.Method == "POST":
			body, _ := io.ReadAll(r.Body)
			deleted += string(body)
			fmt.Fprint(w, "<DeleteResult/>")
		default:
			t.Errorf("unexpected request %s", r.Method)
		}
	}))
	defer remote.Close()
	statePath := filepath.Join(t.TempDir(), "state.json")
	s := store.New(statePath)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	a, _ := s.CreateTenant("a")
	b, _ := s.CreateTenant("b")
	create := func(tenant, name string) model.DataBackend {
		v, e := s.CreateDataBackend(model.DataBackend{TenantID: tenant, Name: name, Provider: model.DataBackendProviderS3, Endpoint: remote.URL, Bucket: "bucket", Prefix: "shared", Credentials: model.DataBackendCredentials{AccessKeyID: "key", SecretAccessKey: "secret"}})
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	first := create(a.ID, "first")
	alias := create(b.ID, "alias")
	w, _ := s.CreateDataWorkspace(model.DataWorkspace{TenantID: a.ID, Name: "a", StorageBackendID: first.ID})
	other, _ := s.CreateDataWorkspace(model.DataWorkspace{TenantID: b.ID, Name: "b", StorageBackendID: alias.ID})
	manifest := func(d string) model.DataManifest {
		return model.DataManifest{Entries: []model.DataManifestEntry{{Kind: "file", SHA256: d, Size: 1, AssetName: "asset", RelativePath: "file"}}}
	}
	if _, e := s.CreateDataSnapshot(model.DataSnapshot{WorkspaceID: other.ID, Version: "v1", Manifest: manifest(digests[0])}); e != nil {
		t.Fatal(e)
	}
	expires := old
	if _, e := s.CreateDataTransfer(model.DataTransfer{WorkspaceID: other.ID, Direction: "upload", Status: "running", Manifest: manifest(digests[1]), ExpiresAt: &expires}); e != nil {
		t.Fatal(e)
	}
	// Simulate a retained legacy row; current workspace writes require an ID.
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var state model.State
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	for i := range state.DataWorkspaces {
		if state.DataWorkspaces[i].ID == other.ID {
			state.DataWorkspaces[i].StorageBackendID = alias.Name
		}
	}
	raw, err = json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	server := NewServer(s, nil, nil, ServerConfig{})
	preview, err := server.sweepDataNamespaceGC(context.Background(), w, 7, true)
	if err != nil || len(preview.Candidates) != 1 || preview.Deleted != 0 || preview.DeletedBytes != 0 || deleted != "" {
		t.Fatalf("preview reported physical reclamation: %+v %v %s", preview, err, deleted)
	}
	out, e := server.sweepDataNamespaceGC(context.Background(), w, 7, false)
	if e != nil {
		t.Fatal(e)
	}
	if out.Deleted != 1 || out.DeletedBytes != 1 || !strings.Contains(deleted, digests[2]) || strings.Contains(deleted, digests[0]) || strings.Contains(deleted, digests[1]) {
		t.Fatalf("unsafe deletion: %+v %s", out, deleted)
	}
	// Names are tenant-scoped: an ambiguous legacy name cannot pick an
	// arbitrary backend while a platform-wide collector is marking references.
	create(a.ID, alias.Name)
	deleted = ""
	if _, err := server.sweepDataNamespaceGC(context.Background(), w, 7, false); err == nil || deleted != "" {
		t.Fatalf("ambiguous backend allowed collection: %v %s", err, deleted)
	}
}

func TestDataGCIdentityRequiresUnchangedETagForTimestampPrecision(t *testing.T) {
	stamp := time.Date(2026, 10, 18, 5, 55, 53, 741000000, time.UTC)
	listed := dataObjectInfo{Size: 13, ETag: `"original"`, LastModified: stamp}
	for _, tc := range []struct {
		name    string
		current dataObjectInfo
		matches bool
	}{
		{"HTTP-date precision", dataObjectInfo{Size: 13, ETag: listed.ETag, LastModified: stamp.Truncate(time.Second)}, true},
		{"replacement in same second", dataObjectInfo{Size: 13, ETag: `"replacement"`, LastModified: stamp.Truncate(time.Second)}, false},
		{"missing ETag", dataObjectInfo{Size: 13, LastModified: stamp.Truncate(time.Second)}, false},
		{"changed size", dataObjectInfo{Size: 14, ETag: listed.ETag, LastModified: stamp.Truncate(time.Second)}, false},
		{"changed timestamp", dataObjectInfo{Size: 13, ETag: listed.ETag, LastModified: stamp.Add(time.Second).Truncate(time.Second)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := dataGCObjectIdentityMatches(listed, tc.current); got != tc.matches {
				t.Fatalf("identity matches=%v want %v", got, tc.matches)
			}
		})
	}
	listed.ETag = ""
	if dataGCObjectIdentityMatches(listed, dataObjectInfo{Size: 13, LastModified: stamp.Truncate(time.Second)}) {
		t.Fatal("relaxed timestamp precision without ETag")
	}
}
