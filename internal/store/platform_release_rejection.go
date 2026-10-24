package store

import "errors"

// Fixed internal stages are safe to expose in operator logs. The underlying
// error remains unchanged for callers; request bodies and database errors are
// never copied into the diagnostic fields.
type platformReleaseRejection struct {
	stage string
	cause error
}

func (e *platformReleaseRejection) Error() string { return e.cause.Error() }
func (e *platformReleaseRejection) Unwrap() error { return e.cause }
func platformReleaseRejected(stage string, err error) error {
	if err == nil {
		return nil
	}
	var existing *platformReleaseRejection
	if errors.As(err, &existing) {
		return err
	}
	return &platformReleaseRejection{stage: stage, cause: err}
}
func PlatformReleaseFailureStage(err error) string {
	var failure *platformReleaseRejection
	if errors.As(err, &failure) {
		return failure.stage
	}
	return "unclassified"
}
