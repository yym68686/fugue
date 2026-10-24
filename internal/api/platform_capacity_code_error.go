package api

import (
	"errors"
	"fugue/internal/model"
)

type capacityCodeError struct {
	reason string
	detail string
	plan   *model.EdgeCodeRecoveryPlan
}

func (e *capacityCodeError) Error() string { return e.reason + ":" + e.detail }
func codeObservationError(reason, detail string) *capacityCodeError {
	return &capacityCodeError{reason: reason, detail: detail}
}

func capacityCodeFailureResult(err error) capacityRecoveryResult {
	var code *capacityCodeError
	if errors.As(err, &code) {
		return capacityRecoveryResult{reason: code.reason, codeDetail: code.detail, codePlan: code.plan}
	}
	// An unclassified transport, cancellation or decoding failure is unknown,
	// never proof that an old executable is running.
	return capacityRecoveryResult{reason: "code_observation_unavailable", codeDetail: "code_binding"}
}
