package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"

	"fugue/internal/model"
)

// Keys are hashes of the actual bytes freshly read from the transaction, not
// artifact IDs, mutable content maps, or declared hashes. No body is retained.
type dnsRouteSourceContentSizes map[[32]byte]int

func (sizes dnsRouteSourceContentSizes) readEncodedArtifact(ctx context.Context, db platformStateDB, id string) (model.PlatformArtifact, int, error) {
	var digest [32]byte
	var reusable bool
	artifact, err := pgGetPlatformArtifactWithScanner(ctx, db, id, false, func(scanner sqlScanner) (model.PlatformArtifact, error) {
		return scanPlatformArtifactWithDecoder(scanner, func(raw []byte) (map[string]any, error) {
			if len(raw) < 1024 || len(raw) > 8<<20 {
				return platformContentDecodings.decode(raw)
			}
			digest, reusable = sha256.Sum256(raw), true
			entry, err := platformContentDecodings.decodeImmutable(raw, digest)
			return clonePlatformContent(entry.content), err
		})
	})
	if err != nil {
		return artifact, 0, err
	}
	if !reusable || len(artifact.Content) == 0 {
		raw, err := json.Marshal(artifact)
		return artifact, len(raw), err
	}
	size, err := sizes.encodedSize(artifact, digest)
	return artifact, size, err
}

// The normal encoder renders Content as a native JSON object in a struct.
// Substitute a tiny nonempty object so omitempty still includes the field.
// Always encode the fresh envelope (and propagate its encoding errors).
func (sizes dnsRouteSourceContentSizes) encodedSize(artifact model.PlatformArtifact, digest [32]byte) (int, error) {
	n, ok := sizes[digest]
	if !ok {
		content, err := json.Marshal(artifact.Content)
		if err != nil {
			return 0, err
		}
		n = len(content)
		if len(sizes) < 128 {
			sizes[digest] = n
		}
	}
	artifact.Content = map[string]any{"": nil}
	envelope, err := json.Marshal(artifact)
	return len(envelope) - len(`{"":null}`) + n, err
}
