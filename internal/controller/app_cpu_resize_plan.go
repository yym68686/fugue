package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type appCPUResizeWaiting struct{ reason string }

func (e *appCPUResizeWaiting) Error() string { return e.reason }

// Both enqueue and execution use this preflight. A terminating Pod remains in
// the cohort: excluding it cannot prove that a partitioned old writer stopped.
// Pending replacements remain eligible for restoring a previously verified CPU
// request; readiness is not a prerequisite for that narrow operation.
func (s *Service) planAppCPUResize(ctx context.Context, c *kubeClient, app model.App, cpu int64) (appCPUResizeJournal, string, bool, error) {
	var plan appCPUResizeJournal
	workload, err := appCPUResizeWorkload(ctx, c, app)
	if err != nil {
		return plan, "", false, err
	}
	deployment, found, err := c.getDeployment(ctx, runtime.NamespaceForTenant(app.TenantID), workload)
	if err != nil {
		return plan, "", false, err
	}
	if !found || deployment.Metadata.UID == "" || deployment.Metadata.DeletionTimestamp != "" || deployment.Status.ObservedGeneration < deployment.Metadata.Generation || deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != app.Spec.Replicas {
		return plan, "deployment-unstable", false, &appCPUResizeWaiting{reason: "app CPU resize requires a stable deployment"}
	}
	pods, err := listAppCPUResizePods(ctx, c, app, workload)
	if err != nil {
		return plan, "", false, err
	}
	facts := make([]any, 0, len(pods))
	for _, pod := range pods {
		// Deliberately omit resourceVersion and heartbeat timestamps. Updating
		// an operation or refreshing a heartbeat must not reset the retry budget.
		statuses := make([]any, 0, len(pod.Status.ContainerStatuses))
		for _, cs := range pod.Status.ContainerStatuses {
			statuses = append(statuses, []any{cs.Name, cs.Ready, cs.RestartCount, cs.Resources, cs.AllocatedResources, cs.State})
		}
		facts = append(facts, []any{pod.Metadata.UID, pod.Metadata.DeletionTimestamp, pod.Spec, pod.Status.Phase, statuses})
	}
	raw, _ := json.Marshal([]any{model.AppSpecSHA256(app.Spec), cpu, deployment.Metadata.UID, deployment.Metadata.Generation, facts})
	hash := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(hash[:])
	wait := func(reason string) (appCPUResizeJournal, string, bool, error) {
		return plan, fingerprint, false, &appCPUResizeWaiting{reason: reason}
	}
	if len(pods) != app.Spec.Replicas {
		return wait(fmt.Sprintf("app CPU resize requires the complete replica set (%d/%d)", len(pods), app.Spec.Replicas))
	}
	plan = appCPUResizeJournal{CreatedAt: s.cpuResizeNow(), Workload: workload, DeploymentUID: deployment.Metadata.UID, Generation: deployment.Metadata.Generation, CPU: cpu}
	needed := false
	for _, pod := range pods {
		if pod.Metadata.DeletionTimestamp != "" || pod.Status.Phase == "Failed" || pod.Status.Phase == "Succeeded" {
			return wait("app CPU resize is waiting for a live stable Pod cohort")
		}
		ob, err := observeAppCPUResizePod(pod, runtime.RuntimeResourceName(app.Name))
		if err != nil {
			return wait(err.Error())
		}
		if err = verifyAppCPUResizeOwner(ctx, c, app, pod, workload, deployment.Metadata.UID); err != nil {
			return plan, fingerprint, false, err
		}
		restoring := app.Spec.CPURequestTemplateMilliCores != nil && app.Spec.Resources != nil && app.Spec.Resources.CPUMilliCores == cpu
		pending := restoring && ob.Phase == "Pending"
		if err = assessAppCPURequestResize(ob, cpu, pending); err != nil {
			return wait(err.Error())
		}
		needed = needed || !resizeQuantitiesEqual(ob.DesiredResources.Requests["cpu"], strconv.FormatInt(cpu, 10)+"m")
		plan.Pods = append(plan.Pods, appCPUResizePodBaseline{Name: ob.PodName, UID: ob.PodUID, Node: ob.NodeName, Restarts: ob.RestartCount, StartedAt: ob.ContainerStartedAt, Resources: ob.DesiredResources, Pending: pending})
	}
	additional, deferred, err := additionalAppCPUResizeBaselines(ctx, c, app, workload, cpu)
	if err != nil {
		return plan, fingerprint, false, err
	}
	plan.Pods = append(plan.Pods, additional...)
	plan.Deferred = deferred
	// Additional revision identity/resources are relevant too, but wall-clock
	// journal creation times are not.
	raw, _ = json.Marshal([]any{fingerprint, additional, deferred})
	hash = sha256.Sum256(raw)
	return plan, hex.EncodeToString(hash[:]), needed || len(additional) > 0, nil
}

type appCPUResizeRetry struct {
	Fingerprint   string    `json:"fingerprint"`
	Reason        string    `json:"reason,omitempty"`
	Attempts      int       `json:"attempts"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
}

func (s *Service) cpuResizeNow() time.Time {
	if s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}
func appCPUResizeRetryKey(app model.App) string {
	return "app-cpu-request-reconcile/v1/" + app.TenantID + "/" + app.ID
}

func (s *Service) recordAppCPUResizeWait(ctx context.Context, app model.App, fingerprint, reason string) error {
	changed := false
	_, err := s.Store.UpdateObservationCheckpoint(ctx, appCPUResizeRetryKey(app), func(raw []byte) ([]byte, error) {
		var state appCPUResizeRetry
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &state); err != nil {
				return nil, err
			}
		}
		if state.Fingerprint == fingerprint && state.Reason == reason {
			return nil, nil
		}
		changed = true
		return json.Marshal(appCPUResizeRetry{Fingerprint: fingerprint, Reason: reason})
	})
	if err == nil && changed && s.Logger != nil {
		s.Logger.Printf("app CPU request restore waiting app_id=%s reason=%s", app.ID, reason)
	}
	return err
}

// Reserve a bounded retry before enqueue. Even a fast preflight race followed
// by terminal failure cannot create a notification-driven operation storm.
// The checkpoint survives leader changes and contains no mutation authority.
func (s *Service) admitAppCPUResizeReconcile(ctx context.Context, app model.App, fingerprint string) (bool, error) {
	admitted := false
	now := s.cpuResizeNow()
	locked, err := s.Store.UpdateObservationCheckpoint(ctx, appCPUResizeRetryKey(app), func(raw []byte) ([]byte, error) {
		var state appCPUResizeRetry
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &state); err != nil {
				return nil, err
			}
		}
		if state.Fingerprint != fingerprint {
			state = appCPUResizeRetry{Fingerprint: fingerprint}
		}
		if now.Before(state.NextAttemptAt) {
			return nil, nil
		}
		if state.Attempts < 7 {
			state.Attempts++
		}
		delay := 5 * time.Second * time.Duration(1<<uint(state.Attempts-1))
		if delay > 5*time.Minute {
			delay = 5 * time.Minute
		}
		state.Reason = ""
		state.NextAttemptAt = now.Add(delay)
		admitted = true
		return json.Marshal(state)
	})
	return locked && admitted, err
}
