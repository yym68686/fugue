package sourceimport

import "context"

type artifactRecorderKey struct{}
type artifactRecorder func(context.Context, string, string, bool) error

// Unlike diagnostic evidence, these checkpoints are required for durable
// ownership. Registration failure prevents execution; commit failure leaves the
// registered intent available for an independent reconciliation attempt.
func WithBuildArtifactRecorder(ctx context.Context, record func(context.Context, string, string, bool) error) context.Context {
	return context.WithValue(ctx, artifactRecorderKey{}, artifactRecorder(record))
}
func recordBuildArtifact(ctx context.Context, job, ref string, completed bool) error {
	record, _ := ctx.Value(artifactRecorderKey{}).(artifactRecorder)
	if record == nil {
		return nil
	}
	return record(ctx, job, ref, completed)
}
