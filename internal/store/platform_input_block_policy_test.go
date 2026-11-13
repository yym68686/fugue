package store

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"fugue/internal/platformconfig"
)

func TestCompilerInputBlockProfilesPreserveExactStream(t *testing.T) {
	raw := []byte(`{"nested":[` + strings.Repeat(`{"literal":"中文\\\"}],[","facts":[{"a":42}]},`, 6000) + `{}]}`)
	for _, minimum := range []int{256, 2048, 4096, 8192} {
		for _, bits := range []int{0, 3, 4} {
			t.Run(fmt.Sprintf("%d/%d", minimum, bits), func(t *testing.T) {
				s := sharedSplitter{minimum: minimum, boundaryBits: bits}
				var restored []byte
				for offset := 0; offset < len(raw); offset += inputChunkBytes {
					if err := s.blocks(raw[offset:min(offset+inputChunkBytes, len(raw))], func(block []byte) error {
						if len(block) < 1 || len(block) > sharedBlockMax {
							t.Fatal("unbounded block")
						}
						restored = append(restored, block...)
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
				if !bytes.Equal(raw, restored) {
					t.Fatal("profile changed bytes")
				}
			})
		}
	}
	for _, raw := range []string{`{"version":1,"minimum_bytes":256,"boundary_bits":4}`, `{"version":1,"minimum_bytes":4096}`} {
		if _, err := parseCompilerInputBlockPolicy([]byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{`{"version":2,"minimum_bytes":256}`, `{"version":1,"minimum_bytes":0}`, `{"version":1,"minimum_bytes":256,"boundary_bits":5}`, `{"version":1,"minimum_bytes":256,"rewrite_history":true}`} {
		if _, err := parseCompilerInputBlockPolicy([]byte(raw)); err == nil {
			t.Fatal("unsafe profile accepted")
		}
	}
}

func TestCompilerInputAnchoredBlocksPostgresOldReader(t *testing.T) {
	s := separatedTestStore(t)
	s.storagePolicy.blocks = compilerInputBlockPolicy{Version: 1, MinimumBytes: 256, BoundaryBits: 4}
	snapshot := separatedFixture(t)
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	reader := &Store{db: s.db, databaseURL: s.databaseURL, dbReady: true}
	assertSeparatedEquivalent(t, reader, snapshot, digest)
}
