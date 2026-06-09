package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	oomRightSizingInitialDelay = 45 * time.Second
	oomRightSizingInterval     = time.Minute
	oomRightSizingLookback     = 15 * time.Minute
	oomRightSizingEventTTL     = 24 * time.Hour
)

type oomRightSizingTarget struct {
	kind      string
	id        string
	eventKeys []string
}

func (s *Server) startOOMRightSizingLoop(ctx context.Context) {
	if s == nil || s.store == nil || ctx == nil {
		return
	}
	go func() {
		timer := time.NewTimer(oomRightSizingInitialDelay)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				s.applyOOMRightSizingOnce(ctx)
				timer.Reset(oomRightSizingInterval)
			}
		}
	}()
}

func (s *Server) applyOOMRightSizingOnce(ctx context.Context) {
	apps, err := s.store.ListApps("", true)
	if err != nil {
		if s.log != nil {
			s.log.Printf("list apps for OOM right-sizing failed: %v", err)
		}
		return
	}
	now := time.Now().UTC()
	cutoff := now.Add(-oomRightSizingLookback)
	for _, app := range apps {
		rightSizing := model.AppRightSizingSpec{Mode: model.AppRightSizingModeAuto}
		if app.Spec.RightSizing != nil {
			rightSizing = model.NormalizeAppRightSizingSpec(*app.Spec.RightSizing)
		}
		if rightSizing.Mode != model.AppRightSizingModeAuto {
			continue
		}
		runtimeObj, err := s.store.GetRuntime(app.Spec.RuntimeID)
		if err != nil || runtimeObj.Type == model.RuntimeTypeExternalOwned {
			continue
		}
		targets, err := s.appOOMRightSizingTargets(ctx, app, cutoff, now)
		if err != nil {
			if s.log != nil {
				s.log.Printf("inspect OOM events for app=%s failed: %v", app.ID, err)
			}
			continue
		}
		eventKeys := oomTargetEventKeys(targets)
		if len(eventKeys) == 0 {
			continue
		}
		if hasActive, activeErr := s.appHasActiveDeployOperation(app); activeErr != nil || hasActive {
			if activeErr != nil && s.log != nil {
				s.log.Printf("check active deploy before OOM right-sizing for app=%s failed: %v", app.ID, activeErr)
			}
			continue
		}
		if !s.claimOOMRightSizingEvents(eventKeys, now) {
			continue
		}
		if err := s.queueOOMRightSizingDeploy(app, targets); err != nil {
			s.releaseOOMRightSizingEvents(eventKeys)
			if s.log != nil {
				s.log.Printf("OOM right-sizing for app=%s failed: %v", app.ID, err)
			}
		}
	}
}

func (s *Server) appOOMRightSizingTargets(ctx context.Context, app model.App, cutoff, now time.Time) ([]oomRightSizingTarget, error) {
	namespace := runtime.NamespaceForTenant(app.TenantID)
	client, err := s.newLogsClient(namespace)
	if err != nil {
		return nil, err
	}

	targets := make([]oomRightSizingTarget, 0, 2)
	appSelector, _, err := runtimeLogTarget(app, "app")
	if err == nil {
		pods, listErr := client.listPodsBySelector(ctx, namespace, appSelector)
		if listErr != nil {
			return nil, listErr
		}
		if keys := recentOOMEventKeys(pods, cutoff, now); len(keys) > 0 {
			targets = append(targets, oomRightSizingTarget{
				kind:      model.ClusterNodeWorkloadKindApp,
				id:        app.ID,
				eventKeys: keys,
			})
		}
	}

	postgresService := firstOwnedPostgresService(app)
	if postgresService == nil {
		return targets, nil
	}
	postgresSelector, _, err := runtimeLogTarget(app, "postgres")
	if err != nil {
		return targets, nil
	}
	pods, err := client.listPodsBySelector(ctx, namespace, postgresSelector)
	if err != nil {
		return nil, err
	}
	if keys := recentOOMEventKeys(pods, cutoff, now); len(keys) > 0 {
		targets = append(targets, oomRightSizingTarget{
			kind:      model.ClusterNodeWorkloadKindBackingService,
			id:        postgresService.ID,
			eventKeys: keys,
		})
	}
	return targets, nil
}

func recentOOMEventKeys(pods []kubePodInfo, cutoff, now time.Time) []string {
	keys := make([]string, 0)
	for _, pod := range pods {
		statuses := append([]kubeContainerStatus(nil), pod.Status.InitContainerStatuses...)
		statuses = append(statuses, pod.Status.ContainerStatuses...)
		for _, status := range statuses {
			for stateName, detail := range map[string]*kubeStateDetail{
				"state":      status.State.Terminated,
				"last-state": status.LastState.Terminated,
			} {
				if detail == nil || !strings.EqualFold(strings.TrimSpace(detail.Reason), "OOMKilled") {
					continue
				}
				observedAt := now
				if detail.FinishedAt != nil {
					observedAt = detail.FinishedAt.UTC()
				} else if !pod.Metadata.CreationTimestamp.IsZero() {
					observedAt = pod.Metadata.CreationTimestamp.UTC()
				}
				if observedAt.Before(cutoff) {
					continue
				}
				keys = append(keys, fmt.Sprintf(
					"%s/%s/%s/%d/%s",
					strings.TrimSpace(pod.Metadata.Name),
					strings.TrimSpace(status.Name),
					stateName,
					status.RestartCount,
					observedAt.Format(time.RFC3339Nano),
				))
			}
		}
	}
	return keys
}

func (s *Server) claimOOMRightSizingEvents(keys []string, now time.Time) bool {
	s.oomRightSizingMu.Lock()
	defer s.oomRightSizingMu.Unlock()
	if s.oomRightSizingEvents == nil {
		s.oomRightSizingEvents = map[string]time.Time{}
	}
	for key, observedAt := range s.oomRightSizingEvents {
		if now.Sub(observedAt) > oomRightSizingEventTTL {
			delete(s.oomRightSizingEvents, key)
		}
	}
	claimed := false
	for _, key := range keys {
		if _, exists := s.oomRightSizingEvents[key]; exists {
			continue
		}
		s.oomRightSizingEvents[key] = now
		claimed = true
	}
	return claimed
}

func (s *Server) releaseOOMRightSizingEvents(keys []string) {
	s.oomRightSizingMu.Lock()
	defer s.oomRightSizingMu.Unlock()
	for _, key := range keys {
		delete(s.oomRightSizingEvents, key)
	}
}

func (s *Server) queueOOMRightSizingDeploy(app model.App, targets []oomRightSizingTarget) error {
	if hasActive, err := s.appHasActiveDeployOperation(app); err != nil {
		return err
	} else if hasActive {
		return nil
	}
	requestedByID := oomRightSizingRequestedByID(targets)
	if exists, err := s.oomRightSizingOperationExists(app, requestedByID); err != nil {
		return err
	} else if exists {
		return nil
	}
	spec, source, err := s.recoverAppDeployBaseline(app)
	if err != nil {
		return err
	}
	changed := false
	for _, target := range targets {
		switch target.kind {
		case model.ClusterNodeWorkloadKindApp:
			current := spec.Resources
			if current == nil {
				defaults := model.DefaultManagedAppResources()
				current = &defaults
			}
			spec.Resources = oomExpandedResourceSpec(current, "")
			changed = true
		case model.ClusterNodeWorkloadKindBackingService:
			service := findBackingServiceByID(app.BackingServices, target.id)
			if service == nil || service.Spec.Postgres == nil {
				continue
			}
			if spec.Postgres == nil {
				postgres := *service.Spec.Postgres
				spec.Postgres = &postgres
			}
			current := service.Spec.Postgres.Resources
			if current == nil {
				defaults := model.DefaultManagedPostgresResources()
				current = &defaults
			}
			spec.Postgres.Resources = oomExpandedResourceSpec(current, model.BackingServiceTypePostgres)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := s.validateAutoscalingTenantEnvelope(app, spec); err != nil {
		return err
	}
	_, err = s.store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     model.ActorTypeSystem,
		RequestedByID:       requestedByID,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       source,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if errors.Is(err, store.ErrConflict) {
		if exists, lookupErr := s.oomRightSizingOperationExists(app, requestedByID); lookupErr == nil && exists {
			return nil
		}
	}
	return err
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

func oomRightSizingRequestedByID(targets []oomRightSizingTarget) string {
	keys := oomTargetEventKeys(targets)
	if len(keys) == 0 {
		for _, target := range targets {
			keys = append(keys, target.kind+"/"+target.id)
		}
	}
	sort.Strings(keys)
	sum := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return model.OperationRequestedByOOMRightSizing + "/" + hex.EncodeToString(sum[:12])
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
	currentTarget := autoscalingAppResourceCommitment(app, app.Spec)
	desiredTarget := autoscalingAppResourceCommitment(app, desired)
	currentPostgres := firstOwnedPostgresService(app)
	if currentPostgres != nil && currentPostgres.Spec.Postgres != nil {
		addAutoscalingResourceSpec(&currentTarget, currentPostgres.Spec.Postgres.Resources, model.DefaultManagedPostgresResources())
	}
	if desired.Postgres != nil {
		addAutoscalingResourceSpec(&desiredTarget, desired.Postgres.Resources, model.DefaultManagedPostgresResources())
	} else if currentPostgres != nil && currentPostgres.Spec.Postgres != nil {
		addAutoscalingResourceSpec(&desiredTarget, currentPostgres.Spec.Postgres.Resources, model.DefaultManagedPostgresResources())
	}
	next := model.BillingResourceSpec{
		CPUMilliCores:   maxInt64(0, total.CPUMilliCores-currentTarget.CPUMilliCores) + desiredTarget.CPUMilliCores,
		MemoryMebibytes: maxInt64(0, total.MemoryMebibytes-currentTarget.MemoryMebibytes) + desiredTarget.MemoryMebibytes,
	}
	if autoscalingResourcesExceed(next, billing.ManagedCap) && autoscalingResourcesExceed(next, total) {
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

func autoscalingAppResourceCommitment(app model.App, spec model.AppSpec) model.BillingResourceSpec {
	replicas := app.Status.CurrentReplicas
	if replicas <= 0 {
		replicas = spec.Replicas
	}
	if replicas <= 0 {
		return model.BillingResourceSpec{}
	}
	resources := model.DefaultManagedAppResources()
	if spec.Resources != nil {
		resources = *spec.Resources
	}
	return model.BillingResourceSpec{
		CPUMilliCores:   resources.CPUMilliCores * int64(replicas),
		MemoryMebibytes: resources.MemoryMebibytes * int64(replicas),
	}
}

func addAutoscalingResourceSpec(total *model.BillingResourceSpec, resources *model.ResourceSpec, defaults model.ResourceSpec) {
	if total == nil {
		return
	}
	effective := defaults
	if resources != nil {
		effective = *resources
	}
	total.CPUMilliCores += effective.CPUMilliCores
	total.MemoryMebibytes += effective.MemoryMebibytes
}

func autoscalingResourcesExceed(left, right model.BillingResourceSpec) bool {
	return left.CPUMilliCores > right.CPUMilliCores || left.MemoryMebibytes > right.MemoryMebibytes
}

func oomExpandedResourceSpec(current *model.ResourceSpec, serviceType string) *model.ResourceSpec {
	out := model.ResourceSpec{}
	if current != nil {
		out = *current
	}
	if out.MemoryMebibytes <= 0 {
		out.MemoryMebibytes = model.DefaultManagedAppMemoryMebibytes
		if serviceType == model.BackingServiceTypePostgres {
			out.MemoryMebibytes = model.DefaultManagedPostgresMemoryMebibytes
		}
	}
	currentLimit := out.MemoryLimitMebibytes
	if currentLimit < out.MemoryMebibytes {
		currentLimit = out.MemoryMebibytes
	}
	out.MemoryMebibytes = roundUpInt64(maxInt64(out.MemoryMebibytes*3/2, out.MemoryMebibytes+128), 16)
	out.MemoryLimitMebibytes = roundUpInt64(maxInt64(currentLimit*3/2, out.MemoryMebibytes+128), 16)
	return &out
}

func firstOwnedPostgresService(app model.App) *model.BackingService {
	for index := range app.BackingServices {
		service := &app.BackingServices[index]
		if service.OwnerAppID == app.ID && service.Type == model.BackingServiceTypePostgres && service.Spec.Postgres != nil {
			return service
		}
	}
	return nil
}

func oomTargetEventKeys(targets []oomRightSizingTarget) []string {
	var keys []string
	for _, target := range targets {
		for _, key := range target.eventKeys {
			keys = append(keys, target.kind+"/"+target.id+"/"+key)
		}
	}
	return keys
}
