package store

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDNSRouteSourceContentLengthMatchesWire(t *testing.T) {
	sizes := make(dnsRouteSourceContentSizes)
	first := model.PlatformArtifact{ID: "artifact", ContentHash: "untrusted-declared-hash", Content: map[string]any{
		"nested": map[string]any{"escaped": strings.Repeat("<>&\n\u2028é", 1024), "empty": []any{}, "number": float64(1e20)},
	}}
	second := first
	second.Metadata = map[string]string{"new": "envelope-only"}
	third := second
	third.Content = map[string]any{"nested": "changed body, same claimed digest"}
	for _, a := range []model.PlatformArtifact{first, second, third} {
		content, err := json.Marshal(a.Content)
		if err != nil {
			t.Fatal(err)
		}
		want, err := json.Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		n, err := sizes.encodedSize(a, sha256.Sum256(content))
		if err != nil || n != len(want) {
			t.Fatalf("encoded size %d versus actual %d: %v", n, len(want), err)
		}
	}
	if len(sizes) != 2 {
		t.Fatal("changed envelope should reuse the content length, changed content must not")
	}
	invalid := first
	invalid.CreatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	content, _ := json.Marshal(invalid.Content)
	if _, err := sizes.encodedSize(invalid, sha256.Sum256(content)); err == nil {
		t.Fatal("cached content length masked invalid envelope")
	}
}

func BenchmarkDNSRouteSourceContentLength(b *testing.B) {
	a := model.PlatformArtifact{ID: "bundle", Content: map[string]any{"body": strings.Repeat("route<&>\n", 1<<17)}}
	content, _ := json.Marshal(a.Content)
	digest := sha256.Sum256(content)
	b.Run("marshal-full", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(a); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("known-content-fresh-envelope", func(b *testing.B) {
		sizes := make(dnsRouteSourceContentSizes)
		if _, err := sizes.encodedSize(a, digest); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := sizes.encodedSize(a, digest); err != nil {
				b.Fatal(err)
			}
		}
	})
}
