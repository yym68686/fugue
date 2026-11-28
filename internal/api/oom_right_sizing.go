package api

import (
	"context"
	"errors"
	"fmt"

	"fugue/internal/model"
	"fugue/internal/oomrightsizing"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func (s *Server) startOOMRightSizingLoop(ctx context.Context) <-chan struct{} {
	if s == nil || s.store == nil || ctx == nil {
		return joinWarmerTasks()
	}
	return startWarmerTask(ctx, func() { s.oomRightSizing.Run(ctx, s.oomRightSizingPorts()) })
}

func (s *Server) applyOOMRightSizingOnce(ctx context.Context) {
	s.oomRightSizing.Sweep(ctx, s.oomRightSizingPorts())
}

func (s *Server) oomRightSizingPorts() oomrightsizing.SweepPorts {
	ports := oomrightsizing.SweepPorts{
		Apps: func() ([]model.App, error) { return s.store.ListApps("", true) },
		Managed: func(app model.App) bool {
			runtimeObj, err := s.store.GetRuntime(app.Spec.RuntimeID)
			return err == nil && runtimeObj.Type != model.RuntimeTypeExternalOwned
		},
		Namespace: func(app model.App) string { return runtime.NamespaceForTenant(app.TenantID) },
		Open:      s.openOOMRightSizingObserver,
		HasActive: s.appHasActiveDeployOperation,
		Enqueue:   s.queueOOMRightSizingDeploy,
	}
	if s.log != nil {
		ports.Logf = s.log.Printf
	}
	return ports
}

func (s *Server) openOOMRightSizingObserver(namespace string) (oomrightsizing.Observer, error) {
	client, err := s.newLogsClient(namespace)
	if err != nil {
		return oomrightsizing.Observer{}, err
	}
	observer := oomrightsizing.Observer{
		Terminations: func(ctx context.Context, app model.App, target string) ([]oomrightsizing.Termination, error) {
			selector, _, err := runtimeLogTarget(app, target)
			if err != nil {
				return nil, nil
			}
			pods, err := client.listPodsBySelector(ctx, namespace, selector)
			if err != nil {
				return nil, err
			}
			return oomTerminations(pods), nil
		},
	}
	if closer, ok := client.(interface{ closeIdleConnections() }); ok {
		observer.Close = closer.closeIdleConnections
	}
	return observer, nil
}

func oomTerminations(pods []kubePodInfo) []oomrightsizing.Termination {
	var observations []oomrightsizing.Termination
	for _, pod := range pods {
		for _, group := range [][]kubeContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
			for _, status := range group {
				for _, state := range []struct {
					name   string
					detail *kubeStateDetail
				}{{"state", status.State.Terminated}, {"last-state", status.LastState.Terminated}} {
					if state.detail != nil {
						observations = append(observations, oomrightsizing.Termination{
							Pod: pod.Metadata.Name, Container: status.Name, State: state.name, Reason: state.detail.Reason,
							Restarts: int(status.RestartCount), FinishedAt: state.detail.FinishedAt, PodCreatedAt: pod.Metadata.CreationTimestamp,
						})
					}
				}
			}
		}
	}
	return observations
}

func (s *Server) queueOOMRightSizingDeploy(app model.App, targets []oomrightsizing.Target) error {
	return oomrightsizing.Enqueue(app, targets, oomrightsizing.AdmissionPorts{
		HasActive: s.appHasActiveDeployOperation,
		Exists:    s.oomRightSizingOperationExists,
		Baseline:  s.recoverAppDeployBaseline,
		Validate:  s.validateAutoscalingTenantEnvelope,
		Submit: func(app model.App, requestID string, spec model.AppSpec, source *model.AppSource) error {
			_, err := s.store.EnqueueOOMRightSizingDeploy(app, requestID, spec, source)
			return err
		},
		IsConflict: func(err error) bool { return errors.Is(err, store.ErrConflict) },
	})
}

func (s *Server) oomRightSizingOperationExists(app model.App, requestedByID string) (bool, error) {
	operations, err := s.store.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		return false, err
	}
	for _, operation := range operations {
		if operation.RequestedByType == model.ActorTypeSystem &&
			operation.RequestedByID == requestedByID {
			return true, nil
		}
	}
	return false, nil
}

func (s *Server) validateAutoscalingTenantEnvelope(app model.App, desired model.AppSpec) error {
	total, err := s.store.GetTenantResourceCommitment(app.TenantID)
	if err != nil {
		return err
	}
	billing, err := s.store.GetTenantBillingSummary(app.TenantID)
	if err != nil {
		return err
	}
	if next, exceeds := oomrightsizing.ProjectTenantCommitment(app, desired, total, billing.ManagedCap); exceeds {
		return fmt.Errorf(
			"%w: autoscaling projection cpu=%dm/%dm memory=%dMi/%dMi across all tenant runtimes",
			store.ErrBillingCapExceeded,
			next.CPUMilliCores,
			billing.ManagedCap.CPUMilliCores,
			next.MemoryMebibytes,
			billing.ManagedCap.MemoryMebibytes,
		)
	}
	return nil
}
