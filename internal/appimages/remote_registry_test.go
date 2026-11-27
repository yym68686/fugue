package appimages

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

func TestRemoteRegistryInspectionCacheAndDeletion(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	server := httptest.NewServer(registry.New())
	defer server.Close()
	repo := strings.TrimPrefix(server.URL, "http://") + "/images/test"
	tag := repo + ":latest"
	ref, err := parseReference(tag)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, empty.Image, remote.WithContext(ctx)); err != nil {
		t.Fatal(err)
	}
	digest, err := empty.Image.Digest()
	if err != nil {
		t.Fatal(err)
	}
	registry := NewRemoteRegistry()
	first, err := registry.InspectImage(ctx, " "+tag+"\x00 ")
	if err != nil {
		t.Fatal(err)
	}
	if !first.Exists || first.ImageRef != tag || first.Digest != digest.String() ||
		first.BlobSizes[first.Digest] <= 0 || first.SizeBytes != SumBlobSizes(first.BlobSizes) {
		t.Fatalf("incorrect manifest observation: %+v", first)
	}
	want := first.Clone()
	first.BlobSizes[first.Digest] = -1
	first.BlobSizes["not-from-registry"] = 9
	second, err := registry.InspectImage(ctx, tag)
	if err != nil || !reflect.DeepEqual(second, want) {
		t.Fatalf("cached observation = %+v, %v; want %+v", second, err, want)
	}
	exists, blobs, err := NewRemoteInspector().InspectImage(ctx, tag)
	if err != nil || !exists || !reflect.DeepEqual(blobs, want.BlobSizes) {
		t.Fatalf("maintenance observation = %v, %v, %v", exists, blobs, err)
	}

	// The local registry tracks digest and tag references separately.
	// Exercise deletion by digest, including invalidation of a cached hit.
	digestRef := repo + "@" + digest.String()
	observed, err := registry.InspectImage(ctx, digestRef)
	if err != nil || !observed.Exists {
		t.Fatalf("inspect digest = %+v, %v", observed, err)
	}
	deleted, err := registry.DeleteImage(ctx, digestRef)
	if err != nil || !deleted.Deleted || deleted.AlreadyMissing ||
		deleted.ImageRef != digestRef || deleted.Digest != digest.String() {
		t.Fatalf("delete digest = %+v, %v", deleted, err)
	}
	missing, err := registry.InspectImage(ctx, digestRef)
	if err != nil || missing.Exists || missing.ImageRef != digestRef ||
		missing.Digest != "" || missing.SizeBytes != 0 || missing.BlobSizes != nil {
		t.Fatalf("post-delete observation = %+v, %v", missing, err)
	}
	deleted, err = registry.DeleteImage(ctx, digestRef)
	if err != nil || deleted.Deleted || !deleted.AlreadyMissing {
		t.Fatalf("delete missing digest = %+v, %v", deleted, err)
	}
}

func TestRemoteRegistryPreservesErrorContext(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	ref := strings.TrimPrefix(server.URL, "http://") + "/images/test:latest"
	for _, tc := range []struct {
		name    string
		inspect func(context.Context, string) error
	}{
		{"inventory", func(ctx context.Context, image string) error {
			_, err := NewRemoteRegistry().InspectImage(ctx, image)
			return err
		}},
		{"maintenance", func(ctx context.Context, image string) error {
			_, _, err := NewRemoteInspector().InspectImage(ctx, image)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.inspect(t.Context(), " "); err == nil || err.Error() != "image_ref is required" {
				t.Fatalf("blank reference error = %v", err)
			}
			err := tc.inspect(t.Context(), ref)
			if err == nil {
				t.Fatal("expected registry error")
			}
			wrapped := strings.HasPrefix(err.Error(), "inspect image "+ref+": ")
			if wrapped != (tc.name == "inventory") {
				t.Fatalf("unexpected error context: %v", err)
			}
		})
	}
}

func TestInspectionCloneDetachesEmptyMap(t *testing.T) {
	original := Inspection{BlobSizes: map[string]int64{}}
	copy := original.Clone()
	copy.BlobSizes["changed"] = 10
	if len(original.BlobSizes) != 0 {
		t.Fatalf("clone changed original: %+v", original)
	}
}
