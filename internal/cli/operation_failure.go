package cli

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

// Only identity, terminal state and the redacted server-reported error are
// exposed. DesiredSpec, credentials and other operation payloads stay private.
type failedCommandOperation struct {
	ID           string `json:"id"`
	Type         string `json:"type"`
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type operationCommandFailure struct {
	Operation   failedCommandOperation
	NextActions []string
}

func newOperationCommandFailure(op model.Operation) *operationCommandFailure {
	reason := strings.TrimSpace(redactDiagnosticString(op.ErrorMessage))
	result := &operationCommandFailure{Operation: failedCommandOperation{
		ID: deploymentPublicID(op.ID), Type: strings.TrimSpace(op.Type),
		Status: strings.TrimSpace(op.Status), ErrorMessage: reason,
	}}
	if result.Operation.ID != "" {
		result.NextActions = []string{"fugue operation show " + result.Operation.ID + " --json", "fugue operation evidence " + result.Operation.ID + " --json"}
	}
	return result
}

func (e *operationCommandFailure) Error() string {
	if e.Operation.Status == "canceled" || e.Operation.Status == "cancelled" {
		message := fmt.Sprintf("operation %s was canceled", e.Operation.ID)
		if e.Operation.ErrorMessage != "" {
			message += ": " + e.Operation.ErrorMessage
		}
		return message
	}
	message := fmt.Sprintf("operation %s (%s) %s", e.Operation.ID, e.Operation.Type, e.Operation.Status)
	if e.Operation.ErrorMessage != "" {
		message += ": " + e.Operation.ErrorMessage
	} else {
		message += "; no failure reason was returned by the server"
	}
	return message
}

func (*operationCommandFailure) ExitCode() int { return ExitCodeSystemFault }
