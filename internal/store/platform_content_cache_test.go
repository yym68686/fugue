package store

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func contentCacheFixture(label string) []byte {
	raw, _ := json.Marshal(map[string]any{"payload": strings.Repeat(label, 2048), "nested": []any{map[string]any{"enabled": true, "number": 1.5, "empty": nil}, []any{"value"}}})
	return raw
}

func TestPlatformContentCacheOwnsMutableContainers(t *testing.T) {
	c := newPlatformContentCache(1<<20, 8)
	raw := contentCacheFixture("a")
	want, _ := decodeJSONValue[map[string]any](raw)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := c.decode(raw)
			if err != nil || !reflect.DeepEqual(value, want) {
				t.Errorf("decoded content differs: %v", err)
				return
			}
			value["payload"] = "changed"
			nested := value["nested"].([]any)
			nested[0].(map[string]any)["enabled"] = false
			nested[1].([]any)[0] = "changed"
			nested[0] = nil
		}()
	}
	wg.Wait()
	got, err := c.decode(raw)
	if err != nil || !reflect.DeepEqual(got, want) || len(c.entries) != 1 {
		t.Fatal("caller mutation escaped into cached content", err)
	}
}

func TestPlatformContentCacheUsesActualBytesAndRemainsBounded(t *testing.T) {
	a, b, d := contentCacheFixture("a"), contentCacheFixture("b"), contentCacheFixture("d")
	c := newPlatformContentCache(1<<20, 2)
	for _, raw := range [][]byte{a, b, a, d} {
		if _, err := c.decode(raw); err != nil {
			t.Fatal(err)
		}
	}
	if _, found := c.get(sha256.Sum256(b)); found {
		t.Fatal("least recently used content was not evicted")
	}
	if len(c.entries) != 2 || c.bytes > c.maxBytes {
		t.Fatal("entry bound exceeded")
	}
	for _, raw := range [][]byte{nil, []byte("null"), []byte(`{"small":true}`), []byte("{broken" + strings.Repeat(" ", 2048)), []byte(`{"payload":"` + strings.Repeat("a", (8<<20)+1) + `"}`)} {
		want, wantErr := decodeJSONValue[map[string]any](raw)
		got, gotErr := c.decode(raw)
		if !reflect.DeepEqual(got, want) || (gotErr == nil) != (wantErr == nil) || len(c.entries) != 2 {
			t.Fatal("bypass/error semantics changed", gotErr, wantErr)
		}
	}
	decoded, _ := decodeJSONValue[map[string]any](a)
	c = newPlatformContentCache(256+platformContentSize(decoded), 10)
	for _, raw := range [][]byte{a, b, d} {
		if _, err := c.decode(raw); err != nil {
			t.Fatal(err)
		}
		if len(c.entries) != 1 || c.bytes > c.maxBytes {
			t.Fatal("decoded byte budget exceeded")
		}
	}
	c = newPlatformContentCache(100, 10)
	if _, err := c.decode(a); err != nil || len(c.entries) != 0 {
		t.Fatal("oversized decoded content retained", err)
	}
}

func TestPlatformArtifactScanDoesNotCacheAuthorityOrClaimedHash(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	artifact := model.PlatformArtifact{ID: "artifact-a", ScopeKey: "global", Status: model.PlatformArtifactStatusValidated, ContentHash: "same-claimed-hash", Content: map[string]any{"payload": strings.Repeat("a", 2048)}, Metadata: map[string]string{"policy_digest": "original"}}
	for i := 0; i < 3; i++ {
		if i == 1 {
			artifact.Status = model.PlatformArtifactStatusDraft
			artifact.Metadata["policy_digest"] = "changed"
		}
		if i == 2 {
			artifact.Content["payload"] = strings.Repeat("b", 2048)
		}
		mock.ExpectQuery("SELECT artifact").WillReturnRows(platformArtifactEnsureRows(t, artifact))
		got, err := scanPlatformArtifact(db.QueryRow("SELECT artifact"))
		if err != nil || got.Status != artifact.Status || !reflect.DeepEqual(got.Metadata, artifact.Metadata) || !reflect.DeepEqual(got.Content, artifact.Content) {
			t.Fatal("database observation replaced by stale cached authority/content", err)
		}
		got.Content["payload"] = "caller mutation"
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPlatformContentDecode(b *testing.B) {
	for _, size := range []int{1 << 20, 4 << 20} {
		raw, _ := json.Marshal(map[string]any{"policy": map[string]any{"scope": "global"}, "payload": strings.Repeat("x", size)})
		for _, cached := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/cached=%t", size, cached), func(b *testing.B) {
				cache := newPlatformContentCache(64<<20, 64)
				_, _ = cache.decode(raw)
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var err error
					if cached {
						_, err = cache.decode(raw)
					} else {
						_, err = decodeJSONValue[map[string]any](raw)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
