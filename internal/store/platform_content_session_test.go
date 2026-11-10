package store

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPlatformReadSessionIsolatesRequestsAndRejectsMutation(t *testing.T) {
	raw := contentCacheFixture("session")
	source, _ := decodeJSONValue[map[string]any](raw)
	key := sha256.Sum256(raw)
	var session platformContentSession
	first := session.owned(key, source)
	second := session.owned(key, source)
	if reflect.ValueOf(first).Pointer() != reflect.ValueOf(second).Pointer() {
		t.Fatal("unchanged request body copied")
	}
	var other platformContentSession
	if reflect.ValueOf(other.owned(key, source)).Pointer() == reflect.ValueOf(first).Pointer() {
		t.Fatal("body shared across requests")
	}
	first["nested"].([]any)[0].(map[string]any)["enabled"] = false
	repaired := session.owned(key, source)
	if !reflect.DeepEqual(repaired, source) || reflect.ValueOf(repaired).Pointer() == reflect.ValueOf(first).Pointer() {
		t.Fatal("caller mutation passed fresh byte validation")
	}
	if len(session.values) != 1 || session.bytes != 256+platformContentSize(source) {
		t.Fatal("replacement accounting incorrect")
	}
	for i := 0; i < 50; i++ {
		session.owned(sha256.Sum256([]byte{byte(i)}), source)
	}
	if len(session.values) > 32 || session.bytes > 32<<20 {
		t.Fatal("session unbounded")
	}
}

func TestPlatformReadSessionPostgresFreshAuthorityAndActualBody(t *testing.T) {
	s := artifactReadTestStore(t)
	raw, _ := json.Marshal(map[string]any{"payload": strings.Repeat("original", 2048)})
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_artifacts(id,generation,content_json) VALUES('session-artifact','session-generation',$1)`, raw); err != nil {
		t.Fatal(err)
	}
	read := s.NewPlatformArtifactReadSession()
	if _, err := read("  "); err != ErrInvalidInput {
		t.Fatal("blank ID semantics changed", err)
	}
	first, err := read(" session-artifact ")
	if err != nil {
		t.Fatal(err)
	}
	second, err := read("session-artifact")
	if err != nil {
		t.Fatal(err)
	}
	if reflect.ValueOf(first.Content).Pointer() != reflect.ValueOf(second.Content).Pointer() {
		t.Fatal("body not reused")
	}
	if _, err := s.db.Exec(`UPDATE fugue_platform_artifacts SET status='draft',metadata_json='{"binding":"changed"}',provenance_json='{"signature":"revoked"}' WHERE id='session-artifact'`); err != nil {
		t.Fatal(err)
	}
	fresh, err := read("session-artifact")
	if err != nil || fresh.Status != "draft" || fresh.Metadata["binding"] != "changed" || fresh.Provenance.Signature != "revoked" {
		t.Fatal("authority cached", err)
	}
	first.Content["payload"] = "caller mutation"
	fresh, err = read("session-artifact")
	if err != nil || fresh.Content["payload"] == "caller mutation" {
		t.Fatal("mutation accepted", err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_platform_artifacts SET content_json='{"payload":"changed"}' WHERE id='session-artifact'`); err != nil {
		t.Fatal(err)
	}
	fresh, err = read("session-artifact")
	if err != nil || fresh.Content["payload"] != "changed" || fresh.ContentHash != first.ContentHash {
		t.Fatal("claimed hash hid actual content change", err)
	}
	if _, err := s.db.Exec(`DELETE FROM fugue_platform_artifacts WHERE id='session-artifact'`); err != nil {
		t.Fatal(err)
	}
	if _, err = read("session-artifact"); err != ErrNotFound {
		t.Fatal("deleted artifact cached", err)
	}
}

func BenchmarkPlatformReadSession(b *testing.B) {
	source := map[string]any{}
	for i := 0; i < 2048; i++ {
		source[strings.Repeat("x", i%16)+string(rune(i+100))] = map[string]any{"nested": []any{"value", float64(i), true}}
	}
	raw, _ := json.Marshal(source)
	key := sha256.Sum256(raw)
	for _, shared := range []bool{false, true} {
		name := "clone"
		if shared {
			name = "request-reuse"
		}
		b.Run(name, func(b *testing.B) {
			var session platformContentSession
			session.owned(key, source)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if shared {
					session.owned(key, source)
				} else {
					clonePlatformContent(source)
				}
			}
		})
	}
}
