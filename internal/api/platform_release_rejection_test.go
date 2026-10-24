package api

import (
	"bytes"
	"errors"
	"fugue/internal/model"
	"fugue/internal/store"
	"log"
	"strings"
	"testing"
)

func TestReleaseRejectionLogDoesNotIncludeCredentialsOrRequestPayload(t *testing.T) {
	var output bytes.Buffer
	s := &Server{log: log.New(&output, "", 0)}
	s.logPlatformReleaseRejection(model.PlatformArtifact{ID: "private-id", ScopeKey: "private-scope", ContentHash: "sha256:" + strings.Repeat("a", 64)}, "gray", errors.Join(store.ErrConflict, errors.New("secret-database-credential")))
	got := output.String()
	if !strings.Contains(got, `"class":"conflict"`) || !strings.Contains(got, `"event":"platform_release_rejected"`) || strings.Contains(got, "private-id") || strings.Contains(got, "private-scope") || strings.Contains(got, "secret-database-credential") || len(got) > 1024 {
		t.Fatal("unsafe rejection diagnostics", got)
	}
}
