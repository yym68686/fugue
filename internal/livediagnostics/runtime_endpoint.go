package livediagnostics

import (
	"context"
	"fugue/internal/livediagnostics/runtimeprofile"
)

const RuntimeSocketPath = runtimeprofile.RuntimeSocketPath

// StartRuntimeEndpoint keeps the existing diagnostic provider ABI.
func StartRuntimeEndpoint(ctx context.Context, component string) error {
	return runtimeprofile.StartRuntimeEndpoint(ctx, component)
}
