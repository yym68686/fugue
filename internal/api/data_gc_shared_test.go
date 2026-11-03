package api

import (
	"context"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/store"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDataGCProtectsOtherTenantAliasAndPendingUpload(t *testing.T) {
	old := time.Now().Add(-10 * 24 * time.Hour).UTC().Truncate(time.Second)
	digests := []string{testDataDigest("other-workspace"), testDataDigest("upload"), testDataDigest("orphan")}
	deleted := ""
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET":
			fmt.Fprint(w, "<ListBucketResult><IsTruncated>false</IsTruncated>")
			for _, d := range digests {
				fmt.Fprintf(w, "<Contents><Key>shared/%s</Key><Size>1</Size><LastModified>%s</LastModified></Contents>", model.DataObjectKey(d), old.Format(time.RFC3339))
			}
			fmt.Fprint(w, "</ListBucketResult>")
		case r.Method == "HEAD":
			w.Header().Set("Content-Length", "1")
			w.Header().Set("Last-Modified", old.Format(http.TimeFormat))
		case r.Method == "POST":
			body, _ := io.ReadAll(r.Body)
			deleted += string(body)
			fmt.Fprint(w, "<DeleteResult/>")
		default:
			t.Errorf("unexpected request %s", r.Method)
		}
	}))
	defer remote.Close()
	s := store.New(filepath.Join(t.TempDir(), "state.json"))
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
	server := NewServer(s, nil, nil, ServerConfig{})
	out, e := server.sweepDataNamespaceGC(context.Background(), w, 7, false)
	if e != nil {
		t.Fatal(e)
	}
	if out.Deleted != 1 || !strings.Contains(deleted, digests[2]) || strings.Contains(deleted, digests[0]) || strings.Contains(deleted, digests[1]) {
		t.Fatalf("unsafe deletion: %+v %s", out, deleted)
	}
}
