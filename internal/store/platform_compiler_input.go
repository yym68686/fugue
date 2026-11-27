package store

import (
	"context"

	"fugue/internal/platformconfig"
)

// The signed artifact's lineage authenticates this digest-addressed input.
// It has no independent release lane or serving authority.
func (s *Store) EnsurePlatformCompilerInput(snapshot platformconfig.RuntimeSnapshot, digest string) error {
	return s.EnsurePlatformCompilerInputContext(context.Background(), snapshot, digest)
}

func (s *Store) EnsurePlatformCompilerInputContext(ctx context.Context, snapshot platformconfig.RuntimeSnapshot, digest string) error {
	// PostgreSQL writes always use the current local store. Encoding policy
	// may change, but it cannot authorize legacy JSONB or old-chunk writes.
	return s.pgEnsureChunkedInput(ctx, snapshot, digest)
}
