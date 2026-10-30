package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

// Include already-current resources: an interrupted command may have committed
// placement while its workloads or database replicas are still converging.
func (c *CLI) waitForProjectMoveConvergence(client *Client, apps []model.App, services []model.BackingService, target string) error {
	parent := client.context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Minute)
	defer cancel()
	bounded := *client
	bounded.context = ctx
	for _, planned := range apps {
		if err := waitOfflineProjectAppConvergence(ctx, &bounded, planned.ID, target, planned.Spec.Replicas, time.Second); err != nil {
			return err
		}
	}
	for _, planned := range services {
		for {
			service, err := bounded.GetBackingService(planned.ID)
			if err != nil {
				return err
			}
			if projectMoveServiceConverged(service, target) {
				break
			}
			if err := waitRequestRetry(ctx, time.Second); err != nil {
				return fmt.Errorf("service %s has not converged on target runtime %s: status=%+v: %w", planned.Name, target, service.RuntimeStatus, err)
			}
		}
	}
	return nil
}

func projectMoveServiceConverged(service model.BackingService, target string) bool {
	if strings.TrimSpace(backingServiceRuntimeID(service)) != target || service.RuntimeStatus == nil {
		return false
	}
	status := service.RuntimeStatus
	if service.Spec.Postgres != nil && service.Spec.Postgres.Suspended {
		return status.Phase == model.ManagedPostgresRuntimePhaseSuspended && status.ReadyInstances == 0
	}
	return status.Phase == model.ManagedPostgresRuntimePhaseActive && status.DesiredInstances > 0 && status.ReadyInstances == status.DesiredInstances
}
