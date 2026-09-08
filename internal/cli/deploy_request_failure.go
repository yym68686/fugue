package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
)

type deploymentRequestFailure struct {
	Stage      string `json:"stage"`
	HTTPStatus int    `json:"http_status,omitempty"`
	RequestID  string `json:"request_id,omitempty"`
	TraceID    string `json:"trace_id,omitempty"`
}

var deploymentEdgeRequestID = regexp.MustCompile(`^edge_[0-9a-f]+_[0-9a-f]+$`)
var deploymentTraceID = regexp.MustCompile(`^[0-9a-f]{32}$`)

func (c *CLI) deploymentRequestFailureResult(err error) (deploymentResult, int) {
	result := newDeploymentResult("unknown")
	result.Summary = "The deployment outcome could not be confirmed."
	request := &deploymentRequestFailure{Stage: "submission"}
	observing := c.deployment != nil && len(c.deployment.operations) > 0
	if observing {
		request.Stage = "observation"
	} else if c.deployment != nil && c.deployment.submissionStage == "upload" {
		request.Stage = "upload"
	}
	result.Request = request
	var headers http.Header
	var apiErr *apiServerError
	var responseErr *httpResponseError
	if errors.As(err, &apiErr) {
		request.HTTPStatus, headers = apiErr.StatusCode, apiErr.Headers
	} else if errors.As(err, &responseErr) {
		request.HTTPStatus, headers = responseErr.status, responseErr.headers
	}
	if value := headers.Get("X-Fugue-Edge-Request-Id"); len(value) <= 128 && deploymentEdgeRequestID.MatchString(value) {
		request.RequestID = value
	}
	if value := headers.Get("X-Fugue-Trace-Id"); deploymentTraceID.MatchString(value) {
		request.TraceID = value
	}
	cause := deploymentCause{Code: "request_failed", Message: "The request did not produce a confirmed deployment result.", Confidence: "reported"}
	var networkErr net.Error
	var syntaxErr *json.SyntaxError
	switch {
	case errors.Is(err, context.Canceled):
		cause.Code, cause.Message = "request_cancelled", "The client request was cancelled; server acceptance is unconfirmed."
	case errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkErr) && networkErr.Timeout():
		cause.Code, cause.Message = "request_timeout", "The client request exceeded its deadline; server acceptance is unconfirmed."
	case errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF):
		cause.Code, cause.Message = "response_incomplete", "The server response was incomplete; server acceptance is unconfirmed."
	case errors.As(err, &syntaxErr):
		cause.Code, cause.Message = "response_invalid", "The API response could not be decoded; server acceptance is unconfirmed."
	case request.HTTPStatus != 0:
		cause.Code, cause.Message = "http_error_response", "The API returned an HTTP error; inspect the recorded status and request ID."
	case errors.As(err, &networkErr):
		cause.Code, cause.Message = "network_error", "The network request failed; server acceptance is unconfirmed."
	}
	code := ExitCodeIndeterminate
	// A structured API rejection of this submission differs from losing a
	// response or failing to observe an operation that is already running.
	if !observing && apiErr != nil && deploymentSubmissionRejected(apiErr.StatusCode) {
		result.Outcome, result.FailedStage, code = "failed", request.Stage, ExitCodeSystemFault
		result.Summary = "The API rejected the deployment request during " + request.Stage + "."
		cause.Code, cause.Message = "request_rejected", "The API rejected this request. Correct the reported condition before submitting again."
		switch apiErr.StatusCode {
		case http.StatusRequestTimeout:
			cause.Code, cause.Message = "request_timeout", "The API timed out while receiving the request."
		case http.StatusRequestEntityTooLarge:
			cause.Code, cause.Message = "request_too_large", "The request exceeds the API size limit."
		case http.StatusTooManyRequests:
			cause.Code, cause.Message = "request_rate_limited", "The API could not admit the request because its concurrency or rate limit was reached."
		}
		result.NextActions = append(result.NextActions, "Resolve the request rejection before submitting again; no automatic deployment retry was performed.")
	} else if observing {
		result.MissingEvidence = append(result.MissingEvidence, "final_operation_state")
		result.NextActions = append(result.NextActions, "Read the tracked operation result before retrying; it may still be running.")
	} else {
		result.MissingEvidence = append(result.MissingEvidence, "submission_acceptance", "operation_id")
		result.NextActions = append(result.NextActions, "No operation ID was received. Preserve this result, the execution time and CLI version for request investigation before submitting another deployment.")
	}
	result.Causes = append(result.Causes, cause)
	if request.RequestID != "" {
		result.NextActions = append(result.NextActions, "Ask a platform administrator to inspect request "+request.RequestID+" with fugue admin request explain.")
	}
	return result, code
}

func deploymentSubmissionRejected(status int) bool {
	switch status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
		http.StatusMethodNotAllowed, http.StatusRequestTimeout, http.StatusRequestEntityTooLarge,
		http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity, http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}
