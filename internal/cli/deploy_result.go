package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

type deploymentCommandState struct {
	requestStarted bool
	lastProgressAt time.Time
	seenAttempts   map[string]bool
	client         *Client
	operations     []model.Operation
}
type deploymentCause struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	Confidence string `json:"confidence"`
	EvidenceID string `json:"evidence_id,omitempty"`
}
type deploymentAttempt struct {
	model.BuilderAttemptEvidence
	OperationID string `json:"operation_id"`
	EvidenceID  string `json:"evidence_id"`
}
type deploymentOperation struct {
	ID     string `json:"id"`
	AppID  string `json:"app_id,omitempty"`
	Type   string `json:"type"`
	Status string `json:"status"`
}
type deploymentServingState struct {
	Status        string     `json:"status"`
	ObservedAt    *time.Time `json:"observed_at,omitempty"`
	ReadyReplicas int        `json:"ready_replicas,omitempty"`
}
type deploymentResult struct {
	SchemaVersion   string                 `json:"schema_version"`
	Outcome         string                 `json:"outcome"`
	FailedStage     string                 `json:"failed_stage,omitempty"`
	Summary         string                 `json:"summary"`
	Operations      []deploymentOperation  `json:"operations"`
	Attempts        []deploymentAttempt    `json:"attempts"`
	Causes          []deploymentCause      `json:"causes"`
	MissingEvidence []string               `json:"missing_evidence"`
	ServingState    deploymentServingState `json:"serving_state"`
	NextActions     []string               `json:"next_actions"`
}
type deploymentResultError struct {
	Result deploymentResult
	code   int
}

func (e *deploymentResultError) Error() string { return e.Result.Summary }
func (e *deploymentResultError) ExitCode() int { return e.code }

var publicOperationIDPattern = regexp.MustCompile(`^(?:op|app|evid)_[a-zA-Z0-9_-]{1,100}$`)

func deploymentPublicID(id string) string {
	if publicOperationIDPattern.MatchString(id) {
		return id
	}
	return ""
}
func (c *CLI) rememberDeployOperation(op model.Operation) {
	if c.deployment == nil {
		return
	}
	for i, previous := range c.deployment.operations {
		if previous.ID == op.ID {
			c.deployment.operations[i] = op
			return
		}
	}
	c.deployment.operations = append(c.deployment.operations, op)
}
func newDeploymentResult(outcome string) deploymentResult {
	return deploymentResult{SchemaVersion: "fugue.deploy-result.v1", Outcome: outcome, Operations: []deploymentOperation{}, Attempts: []deploymentAttempt{}, Causes: []deploymentCause{}, MissingEvidence: []string{}, NextActions: []string{}, ServingState: deploymentServingState{Status: "unknown"}}
}
func deploymentStage(op model.Operation) string {
	if op.Type == model.OperationTypeImport {
		return "build"
	}
	return "deploy"
}
func publicOperation(op model.Operation) deploymentOperation {
	kind := "operation"
	if op.Type == model.OperationTypeImport || op.Type == model.OperationTypeDeploy {
		kind = op.Type
	}
	status := "unknown"
	switch op.Status {
	case "pending", "running", "completed", "failed", "canceled", "cancelled", "superseded":
		status = op.Status
	}
	return deploymentOperation{ID: deploymentPublicID(op.ID), AppID: deploymentPublicID(op.AppID), Type: kind, Status: status}
}
func (c *CLI) deploymentFailureResult(client *Client, op model.Operation) deploymentResult {
	result := newDeploymentResult("failed")
	result.FailedStage = deploymentStage(op)
	result.Summary = "Deployment failed during " + result.FailedStage + "."
	if (op.Status == "canceled" || op.Status == "cancelled") || op.Status == "superseded" {
		result.Summary = "Deployment did not complete: operation " + op.Status + "."
	}
	result.Operations = append(result.Operations, publicOperation(op))
	// Evidence reads have a total bounded budget and never change the operation.
	diagnosticClient := boundedDeploymentClient(client)
	if diagnosticClient != nil {
		evidence, err := diagnosticClient.GetOperationEvidence(op.ID, true)
		if err != nil {
			result.MissingEvidence = append(result.MissingEvidence, "operation_evidence_unavailable")
		} else {
			sort.SliceStable(evidence, func(i, j int) bool { return evidence[i].ObservedAt.Before(evidence[j].ObservedAt) })
			for _, item := range evidence {
				if item.OperationID != op.ID || item.AppID != "" && item.AppID != op.AppID {
					continue
				}
				if item.Type == model.OperationEvidenceTypeBuildAttempt && item.PayloadVersion == 1 {
					if fact, ok := decodePublicBuildAttempt(item.Payload["build_attempt"]); ok {
						result.Attempts = append(result.Attempts, deploymentAttempt{BuilderAttemptEvidence: fact, OperationID: deploymentPublicID(op.ID), EvidenceID: deploymentPublicID(item.ID)})
						for _, code := range fact.Causes {
							addDeploymentCause(&result, code, "evidence_backed", item.ID)
						}
						for _, missing := range fact.MissingEvidence {
							result.MissingEvidence = appendUniqueString(result.MissingEvidence, missing)
						}
					}
				}
			}
		}
	}
	if diagnosticClient != nil && op.AppID != "" {
		if app, readErr := diagnosticClient.GetApp(op.AppID); readErr == nil && app.ID == op.AppID {
			result.ServingState = publicServingState(app)
		}
	}
	if len(result.Attempts) == 0 && op.Type == model.OperationTypeImport {
		result.MissingEvidence = appendUniqueString(result.MissingEvidence, "build_attempt_history")
	}
	for _, code := range model.DeploymentCauseCodes(op.ErrorMessage) {
		if code == "build_timeout" && op.Type != model.OperationTypeImport {
			code = "operation_timeout"
		}
		addDeploymentCause(&result, code, "reported", "")
	}
	if op.Status == "canceled" || op.Status == "cancelled" {
		addDeploymentCause(&result, "operation_cancelled", "confirmed", "")
	}
	if op.Status == "superseded" {
		addDeploymentCause(&result, "operation_superseded", "confirmed", "")
	}
	if len(result.Causes) == 0 {
		addDeploymentCause(&result, "evidence_unavailable", "insufficient_evidence", "")
	}
	sort.SliceStable(result.Attempts, func(i, j int) bool { return result.Attempts[i].Attempt < result.Attempts[j].Attempt })
	if id := deploymentPublicID(op.ID); id != "" {
		result.NextActions = append(result.NextActions, "fugue operation result "+id+" --json")
	}
	result.NextActions = append(result.NextActions, "Resolve the reported cause before submitting another deployment; no retry was performed by this diagnostic.")
	return result
}
func boundedDeploymentClient(client *Client) *Client {
	if client == nil {
		return nil
	}
	copy := *client
	httpCopy := *client.httpClient
	httpCopy.Timeout = 5 * time.Second
	copy.httpClient = &httpCopy
	copy.readRetryCount = 0
	return &copy
}
func addDeploymentCause(result *deploymentResult, code, confidence, id string) {
	if result.FailedStage == "deploy" {
		switch code {
		case "build_command_failed":
			code = "application_startup_failure"
		case "memory_limit_exceeded":
			code = "runtime_memory_limit_exceeded"
		case "builder_cpu_unavailable", "builder_memory_unavailable", "builder_storage_unavailable", "builder_disk_pressure":
			code = "runtime_capacity_unavailable"
		case "placement_constraints_unsatisfied":
			code = "runtime_placement_unsatisfied"
		}
	}
	message := model.PublicDeploymentCause(code)
	if message == "" {
		return
	}
	for _, cause := range result.Causes {
		if cause.Code == code {
			return
		}
	}
	result.Causes = append(result.Causes, deploymentCause{Code: code, Message: message, Confidence: confidence, EvidenceID: deploymentPublicID(id)})
}
func decodePublicBuildAttempt(value any) (model.BuilderAttemptEvidence, bool) {
	var fact model.BuilderAttemptEvidence
	data, err := json.Marshal(value)
	if err != nil || len(data) > 16384 {
		return fact, false
	}
	if json.Unmarshal(data, &fact) != nil || fact.Attempt < 1 || fact.Attempt > 16 || (fact.Outcome != "failed" && fact.Outcome != "succeeded") || fact.StartedAt.IsZero() || fact.FinishedAt.Before(fact.StartedAt) {
		return fact, false
	}
	codes := []string{}
	for _, code := range fact.Causes {
		if model.PublicDeploymentCause(code) != "" {
			codes = appendUniqueString(codes, code)
		}
	}
	fact.Causes = codes
	missing := []string{}
	for _, key := range fact.MissingEvidence {
		switch key {
		case "build_snapshot", "scheduling_events", "additional_build_pods", "additional_scheduling_events", "temporary_storage_breakdown", "build_log_tail":
			missing = appendUniqueString(missing, key)
		}
	}
	fact.MissingEvidence = missing
	for _, v := range []int64{fact.MemoryRequestBytes, fact.MemoryLimitBytes, fact.EphemeralRequestBytes, fact.EphemeralLimitBytes} {
		if v < 0 {
			return fact, false
		}
	}
	return fact, true
}
func (c *CLI) renderDeploymentError(err error) error {
	if c.deployment != nil && !c.deployment.requestStarted && len(c.deployment.operations) == 0 {
		var apiErr *apiServerError
		var transportErr *url.Error
		if errors.As(err, &apiErr) || errors.As(err, &transportErr) {
			return withExitCode(fmt.Errorf("Deployment preflight could not be completed. Check access and API availability."), ExitCodeForError(err))
		}
		return err
	}
	var known *deploymentResultError
	result := newDeploymentResult("unknown")
	code := ExitCodeIndeterminate
	if errors.As(err, &known) {
		result = known.Result
		code = known.code
	} else {
		result.Summary = "The deployment outcome could not be confirmed."
		result.MissingEvidence = append(result.MissingEvidence, "final_operation_state")
		result.NextActions = append(result.NextActions, "Read the operation result before retrying; the server may still be processing the request.")
	}
	if c.deployment != nil {
		result.Operations = []deploymentOperation{}
		for _, op := range c.deployment.operations {
			result.Operations = append(result.Operations, publicOperation(op))
		}
		if len(result.Operations) > 0 {
			result.NextActions = appendUniqueString(result.NextActions, "fugue operation result "+result.Operations[0].ID+" --json")
		}
	}
	if c.wantsJSON() {
		if writeErr := writeJSON(c.stdout, map[string]any{"result": result}); writeErr != nil {
			return fmt.Errorf("could not write deployment result")
		}
	} else {
		if writeErr := renderDeploymentResult(c.stdout, result); writeErr != nil {
			return fmt.Errorf("could not write deployment result")
		}
	}
	return &deploymentResultError{Result: result, code: code}
}
func renderDeploymentResult(w io.Writer, result deploymentResult) error {
	if _, err := fmt.Fprintf(w, "outcome=%s\n%s\n", result.Outcome, result.Summary); err != nil {
		return err
	}
	for _, op := range result.Operations {
		if _, err := fmt.Fprintf(w, "operation=%s status=%s\n", op.ID, op.Status); err != nil {
			return err
		}
	}
	for _, a := range result.Attempts {
		if _, err := fmt.Fprintf(w, "attempt=%d outcome=%s\n", a.Attempt, a.Outcome); err != nil {
			return err
		}
		for _, code := range a.Causes {
			if _, err := fmt.Fprintf(w, "  %s\n", model.PublicDeploymentCause(code)); err != nil {
				return err
			}
		}
		if a.EphemeralLimitBytes > 0 {
			if _, err := fmt.Fprintf(w, "  temporary_storage_limit_bytes=%d\n", a.EphemeralLimitBytes); err != nil {
				return err
			}
		}
	}
	for _, cause := range result.Causes {
		if _, err := fmt.Fprintf(w, "cause=%s confidence=%s: %s\n", cause.Code, cause.Confidence, cause.Message); err != nil {
			return err
		}
	}
	if len(result.MissingEvidence) > 0 {
		if _, err := fmt.Fprintf(w, "missing_evidence=%s\n", strings.Join(result.MissingEvidence, ",")); err != nil {
			return err
		}
	}
	for _, action := range result.NextActions {
		if _, err := fmt.Fprintln(w, action); err != nil {
			return err
		}
	}
	return nil
}
func (c *CLI) successfulDeploymentResult(bundle importBundle, waited bool) deploymentResult {
	result := newDeploymentResult("accepted")
	result.Summary = "Deployment accepted; completion has not been verified."
	if waited {
		result.Outcome = "succeeded"
		result.Summary = "The tracked deployment operations completed."
		result.ServingState = publicServingState(bundle.PrimaryApp)
	}
	for _, op := range bundle.Operations {
		result.Operations = append(result.Operations, publicOperation(op))
	}
	return result
}
func (c *CLI) newOpsResultCommand() *cobra.Command {
	return &cobra.Command{Use: "result <operation>", Short: "Read a deployment outcome and durable failure reasons without cluster details", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return fmt.Errorf("Could not initialize operation result access.")
		}
		op, err := client.GetOperation(args[0])
		if err != nil {
			return withExitCode(fmt.Errorf("Could not read the operation result. Check the operation ID, access and API availability."), ExitCodeForError(err))
		}
		result := newDeploymentResult("accepted")
		result.Operations = append(result.Operations, publicOperation(op))
		result.Summary = "The operation has not completed."
		switch op.Status {
		case "failed", "canceled", "cancelled", "superseded":
			result = c.deploymentFailureResult(client, op)
		case "completed":
			result.Outcome = "succeeded"
			result.Summary = "The operation completed."
			if op.Type == model.OperationTypeImport {
				linked := explicitQueuedDeployOperationID(op)
				if linked == "" {
					result.Outcome = "unknown"
					result.Summary = "The build completed; no deployment link was recorded."
					result.MissingEvidence = append(result.MissingEvidence, "linked_deploy_operation")
				} else {
					child, readErr := client.GetOperation(linked)
					if readErr != nil || child.AppID != op.AppID || child.Type != model.OperationTypeDeploy {
						result.Outcome = "unknown"
						result.Summary = "The build completed; its deployment result is unavailable."
					} else {
						result.Operations = append(result.Operations, publicOperation(child))
						if child.Status == "failed" || (child.Status == "canceled" || child.Status == "cancelled") || child.Status == "superseded" {
							result = c.deploymentFailureResult(client, child)
							result.Operations = append([]deploymentOperation{publicOperation(op)}, result.Operations...)
						} else if child.Status != "completed" {
							result.Outcome = "accepted"
							result.Summary = "The build completed; deployment is still in progress."
						}
					}
				}
			}
		}
		if c.wantsJSON() {
			return writeJSON(c.stdout, map[string]any{"result": result})
		}
		return renderDeploymentResult(c.stdout, result)
	}}
}

func explicitQueuedDeployOperationID(op model.Operation) string {
	if op.QueuedDeployOperationID != "" {
		return deploymentPublicID(op.QueuedDeployOperationID)
	}
	return deploymentPublicID(queuedDeployOperationID(op.ResultMessage))
}

func publicServingState(app model.App) deploymentServingState {
	state := deploymentServingState{Status: "unknown"}
	observed := app.ObservedStatus
	if observed == nil || !observed.Fresh || observed.ObservedAt.IsZero() || time.Since(observed.ObservedAt) > 2*time.Minute || time.Until(observed.ObservedAt) > time.Minute {
		return state
	}
	state.ObservedAt = &observed.ObservedAt
	if observed.ReadyReplicas == nil {
		return state
	}
	state.ReadyReplicas = *observed.ReadyReplicas
	if *observed.ReadyReplicas > 0 && observed.Phase == "deployed" {
		state.Status = "ready"
	} else {
		state.Status = "not_ready"
	}
	return state
}

// Refresh bounded public progress at most every 30 seconds, including when the
// outer operation status remains running through several builder retries.
func (c *CLI) progressBuildAttempts(client *Client, op model.Operation) {
	if c.deployment == nil || c.wantsJSON() || op.Type != model.OperationTypeImport || op.Status != "running" {
		return
	}
	now := time.Now()
	if c.deployment.lastProgressAt.IsZero() {
		c.deployment.lastProgressAt = now
		return
	}
	if now.Sub(c.deployment.lastProgressAt) < 30*time.Second {
		return
	}
	c.deployment.lastProgressAt = now
	evidence, err := boundedDeploymentClient(client).GetOperationEvidence(op.ID, true)
	if err != nil {
		return
	}
	if c.deployment.seenAttempts == nil {
		c.deployment.seenAttempts = map[string]bool{}
	}
	for _, item := range evidence {
		if item.OperationID != op.ID || item.Type != model.OperationEvidenceTypeBuildAttempt || item.PayloadVersion != 1 {
			continue
		}
		fact, ok := decodePublicBuildAttempt(item.Payload["build_attempt"])
		if !ok {
			continue
		}
		key := op.ID + "/" + fmt.Sprint(fact.Attempt)
		if c.deployment.seenAttempts[key] {
			continue
		}
		c.deployment.seenAttempts[key] = true
		c.progressf("build_attempt=%d outcome=%s", fact.Attempt, fact.Outcome)
		for _, code := range fact.Causes {
			c.progressf("build_cause=%s: %s", code, model.PublicDeploymentCause(code))
		}
	}
}
