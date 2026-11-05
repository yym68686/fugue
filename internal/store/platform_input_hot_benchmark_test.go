package store

import (
	"fmt"
	"strings"
	"testing"

	"fugue/internal/model"
)

// DNS answer bodies are intentionally not dependency identities. The real
// typed publication decoders must still run, without repeatedly encoding the
// same immutable answer body for each dependency view.
func BenchmarkCompilerInputHotDNSDependencies(b *testing.B) {
	for _, size := range []int{8 << 10, 64 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("body-%d", size), func(b *testing.B) {
			artifact := model.PlatformArtifact{
				ID: "neutral-dns-artifact", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle,
				ContentHash: "sha256:" + strings.Repeat("a", 64),
				Content:     map[string]any{"policy": map[string]any{}, "answers": strings.Repeat("x", size)},
			}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				node, err := compilerInputArtifactHotNode(artifact)
				if err != nil || len(node.References) != 0 {
					b.Fatal("invalid dependency benchmark", err)
				}
			}
		})
	}
}
