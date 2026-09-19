package store

import (
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestCompilerInputRetention(t *testing.T) { testCompilerInputRetention(t, "") }
func TestCompilerInputRetentionPostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback PostgreSQL")
	}
	testCompilerInputRetention(t, dsn)
}
func testCompilerInputRetention(t *testing.T, dsn string) {
	s := New(t.TempDir()+"/state.json", dsn)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if dsn != "" {
		defer s.db.Close()
	}
	now := time.Now().UTC()
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: model.NewID("intent"), PolicyGeneration: "policy", CapturedAt: &now, Facts: map[string]any{"source": "fixed", "weight": 0.25}}
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetPlatformArtifactContent(digest)
	if err != nil {
		t.Fatal(err)
	}
	got, err := platformconfig.DecodeRuntimeSnapshotContent(before.Content)
	if err != nil || got.CapturedAt == nil || !got.CapturedAt.Equal(now) || got.Facts["source"] != "fixed" {
		t.Fatal("stored input differs", err)
	}
	if actual, err := platformconfig.RuntimeSnapshotDigest(got); err != nil || actual != digest {
		t.Fatal("stored input cannot reproduce lineage", err)
	}
	if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal("retry failed", err)
	}
	after, err := s.GetPlatformArtifactContent(digest)
	if err != nil || !before.CreatedAt.Equal(after.CreatedAt) || !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatal("retry mutated input", err)
	}
	if dsn != "" {
		if _, err = s.db.Exec(`UPDATE fugue_platform_artifact_contents SET content_json=content_json || '{"unexpected":true}'::jsonb WHERE content_hash=$1`, digest); err != nil {
			t.Fatal(err)
		}
	} else {
		if err = s.withLockedState(true, func(st *model.State) error {
			for i := range st.PlatformArtifactContents {
				if st.PlatformArtifactContents[i].ContentHash == digest {
					st.PlatformArtifactContents[i].Content["unexpected"] = true
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.EnsurePlatformCompilerInput(snapshot, digest); err == nil {
		t.Fatal("corrupt input silently replaced")
	}
}
