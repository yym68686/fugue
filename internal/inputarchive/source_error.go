package inputarchive

import "errors"

// Keep the source failure available to bounded classifiers without exposing
// database messages, URLs, or original input bytes in Error(). The copy still
// fails its integrity contract and never treats a short source as complete.
type sourceReadError struct {
	ordinal, received int
	cause             error
}

func (e *sourceReadError) Error() string   { return "compiler input archive source read failed" }
func (e *sourceReadError) Unwrap() []error { return []error{ErrIntegrity, e.cause} }

// SourceReadFailure returns only aggregate position, never the private cause.
func SourceReadFailure(err error) (ordinal, received int, ok bool) {
	var failure *sourceReadError
	if errors.As(err, &failure) {
		return failure.ordinal, failure.received, true
	}
	return -1, 0, false
}
