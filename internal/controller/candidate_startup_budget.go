package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

const candidateStartupRetryLimit = 3
const candidateStartupHealthyReset = 10 * time.Minute

type candidateStartupObservation struct {
	Version       int            `json:"version"`
	DeploymentUID string         `json:"deployment_uid"`
	Restarts      map[string]int `json:"restarts"`
	Failures      int            `json:"failures"`
	HealthySince  time.Time      `json:"healthy_since,omitempty"`
}

type candidateStartupInputFailures struct {
	ResetOperation string          `json:"reset_operation,omitempty"`
	Operations     map[string]bool `json:"operations"`
}

type candidateStartupFailureLedger struct {
	Version int                                      `json:"version"`
	Inputs  map[string]candidateStartupInputFailures `json:"inputs"`
}

func candidateStartupLedgerKey(app model.App) string {
	return "candidate-startup-failures/v1/" + app.TenantID + "/" + app.ID
}

func decodeCandidateStartupLedger(raw []byte) (candidateStartupFailureLedger, error) {
	ledger := candidateStartupFailureLedger{Version: 1, Inputs: map[string]candidateStartupInputFailures{}}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &ledger); err != nil {
			return ledger, err
		}
	}
	if ledger.Version != 1 || ledger.Inputs == nil {
		return ledger, errors.New("unsupported candidate startup failure ledger")
	}
	return ledger, nil
}

// Hash executable input in memory; credentials, environment and file contents
// never enter observation checkpoints or logs. Mutable rollout tokens do not
// make the same failing image/configuration a new automatic retry budget.
func (s *Service) candidateStartupFingerprint(app model.App, digest string) string {
	app.Spec = *cloneControllerAppSpec(&app.Spec)
	app = s.Renderer.PrepareApp(app)
	app.Spec.Image = strings.ToLower(strings.TrimSpace(digest))
	app.Spec.RestartToken, app.Spec.RolloutIntent = "", ""
	app.Spec.CPURequestTemplateMilliCores = nil
	if app.Spec.Workspace != nil {
		app.Spec.Workspace.ResetToken = ""
	}
	raw, _ := json.Marshal(app.Spec)
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
}

func (s *Service) candidateStartupOperationFingerprint(app model.App, op model.Operation) (string, error) {
	attempt, found, err := s.Store.FindReleaseAttemptForOperation(op.ID)
	if err != nil {
		return "", err
	}
	digest := attempt.TargetDigest
	if !found || digest == "" {
		digest = model.ImageDigestFromReference(app.Spec.Image)
		if op.RequestedByID == model.OperationRequestedByImageTracking {
			tracking, err := s.Store.GetAppImageTracking(app.TenantID, false, app.ID)
			if err != nil {
				return "", err
			}
			if model.ImageDigestsMatch(digest, tracking.LastQueuedDigest) {
				digest = tracking.LastQueuedDigest
			}
		}
	}
	if digest == "" {
		digest = app.Spec.Image
	}
	return s.candidateStartupFingerprint(app, digest), nil
}

func (s *Service) candidateStartupRetrySuppression(ctx context.Context, app model.App, digest string) (string, error) {
	raw, err := s.Store.ReadObservationCheckpoint(ctx, candidateStartupLedgerKey(app))
	if err != nil {
		return "", err
	}
	ledger, err := decodeCandidateStartupLedger(raw)
	if err != nil {
		return "", err
	}
	if len(ledger.Inputs[s.candidateStartupFingerprint(app, digest)].Operations) >= candidateStartupRetryLimit {
		return "candidate startup retry budget exhausted: 3 failed releases for the same image/configuration; change input or explicitly deploy to retry", nil
	}
	return "", nil
}

func (s *Service) updateCandidateStartupFailures(ctx context.Context, app model.App, op model.Operation, reset bool) error {
	if reset && op.Status == model.OperationStatusCompleted {
		raw, err := s.Store.ReadObservationCheckpoint(ctx, candidateStartupLedgerKey(app))
		if err != nil || len(raw) == 0 {
			return err
		}
	}
	fingerprint, err := s.candidateStartupOperationFingerprint(app, op)
	if err != nil {
		return err
	}
	written, err := s.Store.UpdateObservationCheckpoint(ctx, candidateStartupLedgerKey(app), func(raw []byte) ([]byte, error) {
		ledger, err := decodeCandidateStartupLedger(raw)
		if err != nil {
			return nil, err
		}
		input := ledger.Inputs[fingerprint]
		if reset {
			receipt := op.ID
			if op.Status == model.OperationStatusCompleted {
				receipt += "/completed"
			}
			if input.ResetOperation == receipt || (op.Status == model.OperationStatusCompleted && len(input.Operations) == 0) {
				return nil, nil
			}
			input = candidateStartupInputFailures{ResetOperation: receipt, Operations: map[string]bool{}}
		} else {
			if input.Operations[op.ID] {
				return nil, nil
			}
			if input.Operations == nil {
				input.Operations = map[string]bool{}
			}
			if len(input.Operations) < candidateStartupRetryLimit {
				input.Operations[op.ID] = true
			}
		}
		if len(ledger.Inputs) >= 4096 {
			if _, exists := ledger.Inputs[fingerprint]; !exists {
				return nil, errors.New("candidate startup ledger requires archival; existing guards retained")
			}
		}
		ledger.Inputs[fingerprint] = input
		return json.Marshal(ledger)
	})
	if err != nil {
		return err
	}
	if !written {
		return fmt.Errorf("%w: candidate startup ledger is concurrently updating", errOperationPersistenceInterrupted)
	}
	return nil
}

// This gate owns retry policy only. Admission and withdrawal/retirement retain
// their existing ownership, traffic, connection-drain and scale/delete guards.
func (s *Service) checkCandidateStartupRetryBudget(ctx context.Context, app model.App, op model.Operation) error {
	automatic := op.RequestedByType == model.ActorTypeSystem || op.RequestedByID == model.OperationRequestedByImageTracking ||
		op.RequestedByID == model.OperationRequestedByGitHubSyncController || op.RequestedByID == model.OperationRequestedByImageRebuild
	if !automatic {
		if op.RequestedByType == model.ActorTypeAPIKey || op.RequestedByType == model.ActorTypeBootstrap {
			return s.updateCandidateStartupFailures(ctx, app, op, true)
		}
		return nil
	}
	fingerprint, err := s.candidateStartupOperationFingerprint(app, op)
	if err != nil {
		return err
	}
	raw, err := s.Store.ReadObservationCheckpoint(ctx, candidateStartupLedgerKey(app))
	if err != nil {
		return err
	}
	ledger, err := decodeCandidateStartupLedger(raw)
	if err != nil {
		return err
	}
	if len(ledger.Inputs[fingerprint].Operations) >= candidateStartupRetryLimit {
		return errors.New("candidate startup retry budget exhausted for this image/configuration; explicit deploy required")
	}
	return nil
}

func candidateStartupRetryable(status kubeContainerStatus) bool {
	if status.Ready {
		return false
	}
	if status.State.Waiting != nil && strings.EqualFold(status.State.Waiting.Reason, "CrashLoopBackOff") {
		return true
	}
	for _, state := range []*kubeStateDetail{status.State.Terminated, status.LastState.Terminated} {
		if state != nil && (isFailingManagedAppTermination(*state) || managedAppServiceProcessExitedSuccessfully(*state)) {
			return true
		}
	}
	return false
}

func observeCandidateStartup(state *candidateStartupObservation, pods []kubePod, deployment kubeDeployment, main string, replicas int, now time.Time) (string, []kubePod) {
	filtered := make([]kubePod, 0, len(pods))
	ready, matched, retrying := 0, 0, ""
	for _, pod := range pods {
		if !podHasDeploymentTemplateIdentity(pod, deployment) {
			continue
		}
		matched++
		pod.Status.ContainerStatuses = append([]kubeContainerStatus(nil), pod.Status.ContainerStatuses...)
		for i, status := range pod.Status.ContainerStatuses {
			if status.Name != main {
				continue
			}
			identity := pod.ObservedUID
			if identity == "" {
				identity = pod.Metadata.Name
			}
			previous, seen := state.Restarts[identity]
			if !seen && !status.Ready {
				previous = 0
			}
			if (seen || !status.Ready) && status.RestartCount > previous && candidateStartupRetryable(status) {
				state.Failures += status.RestartCount - previous
			}
			state.Restarts[identity] = status.RestartCount
			if status.Ready && kubePodReady(pod) {
				ready++
			}
			if candidateStartupRetryable(status) {
				// A newly restarted process still gets its startup-probe grace.
				// Its previous termination alone does not prove this start failed.
				if status.State.Running == nil {
					retrying = summarizeManagedAppPodFailure(pod)
				}
				// Give only the main process its bounded startup retries. Init,
				// sidecar, image/config and Pod-level failures remain fail-fast.
				copy := status
				copy.State.Terminated, copy.LastState.Terminated = nil, nil
				if copy.State.Waiting != nil && strings.EqualFold(copy.State.Waiting.Reason, "CrashLoopBackOff") {
					copy.State.Waiting = nil
				}
				if copy.LastState.Waiting != nil && strings.EqualFold(copy.LastState.Waiting.Reason, "CrashLoopBackOff") {
					copy.LastState.Waiting = nil
				}
				pod.Status.ContainerStatuses[i] = copy
			}
		}
		filtered = append(filtered, pod)
	}
	if replicas > 0 && ready == replicas && matched == replicas {
		if state.HealthySince.IsZero() || now.Before(state.HealthySince) {
			state.HealthySince = now
		}
		if now.Sub(state.HealthySince) >= candidateStartupHealthyReset {
			state.Failures = 0
		}
	} else {
		state.HealthySince = time.Time{}
	}
	if state.Failures >= candidateStartupRetryLimit && retrying != "" {
		return fmt.Sprintf("candidate startup retry budget exhausted (%d/%d): %s", state.Failures, candidateStartupRetryLimit, retrying), filtered
	}
	return "", filtered
}

func candidateStartupPodsReady(pods []kubePod, deployment kubeDeployment, replicas int) bool {
	ready := 0
	for _, pod := range pods {
		if podHasDeploymentTemplateIdentity(pod, deployment) && kubePodReady(pod) {
			ready++
		}
	}
	return replicas > 0 && ready >= replicas
}

func (s *Service) candidateStartupFailureMessage(ctx context.Context, app model.App, operationID string, deployment kubeDeployment, pods []kubePod, memory *candidateStartupObservation) (string, error) {
	var message string
	var filtered []kubePod
	changed := false
	observe := func(raw []byte) ([]byte, error) {
		state := candidateStartupObservation{Version: 1, DeploymentUID: deployment.Metadata.UID, Restarts: map[string]int{}}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &state); err != nil {
				return nil, err
			}
		}
		if state.Version != 1 || state.Restarts == nil || state.DeploymentUID != deployment.Metadata.UID {
			return nil, errors.New("candidate startup observation identity/version differs")
		}
		before, _ := json.Marshal(state)
		message, filtered = observeCandidateStartup(&state, pods, deployment, runtime.RuntimeResourceName(app.Name), app.Spec.Replicas, s.rolloutObservationTime())
		*memory = state
		after, err := json.Marshal(state)
		changed = !bytes.Equal(before, after)
		if !changed {
			return nil, err
		}
		return after, err
	}
	if s.Store != nil && operationID != "" {
		written, err := s.Store.UpdateObservationCheckpoint(ctx, "candidate-startup-observation/v1/"+app.TenantID+"/"+app.ID+"/"+operationID, observe)
		if err != nil || !written {
			return "", fmt.Errorf("%w: persist candidate startup observation: %v", errOperationPersistenceInterrupted, err)
		}
		if message != "" {
			op, err := s.Store.GetOperation(operationID)
			if err != nil {
				return "", fmt.Errorf("%w: read candidate startup operation: %v", errOperationPersistenceInterrupted, err)
			}
			if op.AppID != app.ID || op.TenantID != app.TenantID || op.Type != model.OperationTypeDeploy {
				return "", errors.New("candidate startup operation ownership differs")
			}
			if err := s.updateCandidateStartupFailures(ctx, app, op, false); err != nil {
				return "", fmt.Errorf("%w: record candidate startup failure: %v", errOperationPersistenceInterrupted, err)
			}
		}
	} else {
		raw, _ := json.Marshal(memory)
		if memory.Restarts == nil {
			raw = nil
		}
		if _, err := observe(raw); err != nil {
			return "", err
		}
	}
	if changed && s.Logger != nil {
		s.Logger.Printf("candidate startup observation operation=%s deployment=%s failures=%d budget=%d", operationID, deployment.Metadata.Name, memory.Failures, candidateStartupRetryLimit)
	}
	if message == "" {
		message = deploymentTemplatePodFailureMessage(filtered, deployment)
	}
	return message, nil
}
