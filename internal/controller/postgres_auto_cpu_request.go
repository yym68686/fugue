package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	"k8s.io/apimachinery/pkg/api/resource"
)

const managedPostgresAutoCPUActor = store.ManagedPostgresAutoCPURequestActorPrefix
const managedPostgresCPUUsageSampleKind = "right_sizing_backing_service_per_replica_v1"

func managedPostgresCPUEnrollment(cfg config.ManagedPostgresInPlaceResizeConfig, serviceID, namespace, cluster string) *config.ManagedPostgresCPURequestTarget {
	for _, target := range cfg.AutoCPURequestDownscaleTargets {
		if target.ServiceID == serviceID && target.Namespace == namespace && target.Cluster == cluster {
			copy := target
			return &copy
		}
	}
	return nil
}

func managedPostgresCPUEnrollmentMatches(target config.ManagedPostgresCPURequestTarget, snapshot managedPostgresResizeSnapshot) bool {
	return snapshot.Observation.Namespace == target.Namespace && snapshot.Cluster.Metadata.Name == target.Cluster &&
		snapshot.Cluster.Metadata.UID == target.ClusterUID && snapshot.Cluster.Status.SystemID == target.SystemID
}

func managedPostgresCPUActor(target config.ManagedPostgresCPURequestTarget) string {
	return managedPostgresAutoCPUActor + "/" + target.ClusterUID + "/" + target.SystemID
}

func validateManagedPostgresAutomaticCPUTarget(current, target kubeResourceRequirements) error {
	for _, values := range [][2]string{
		{current.Requests["memory"], target.Requests["memory"]},
		{current.Limits["memory"], target.Limits["memory"]},
		{current.Limits["cpu"], target.Limits["cpu"]},
	} {
		if !resizeQuantitiesEqual(values[0], values[1]) {
			return fmt.Errorf("automatic PostgreSQL CPU request operation cannot change memory or CPU limits; live envelope changed")
		}
	}
	currentCPU, err := resource.ParseQuantity(current.Requests["cpu"])
	if err != nil {
		return err
	}
	targetCPU, err := resource.ParseQuantity(target.Requests["cpu"])
	if err != nil {
		return err
	}
	if currentCPU.Cmp(targetCPU) < 0 {
		return fmt.Errorf("automatic PostgreSQL CPU request downscale cannot increase the live request")
	}
	return nil
}

// Follow the existing PostgreSQL request recommendation: P75, multiplier 1,
// round up to 5m, floor 1m. Preserve the live memory envelope and CPU limit;
// bootstrap resources may differ from the current in-place runtime resources.
func managedPostgresAutoCPURequestTarget(current kubeResourceRequirements, samples []model.ResourceUsageSample, policy model.AppRightSizingSpec, now time.Time) (*model.ResourceSpec, error) {
	values := make([]int64, 0, len(samples))
	var latest time.Time
	for _, sample := range samples {
		if sample.CPUMilliCores == nil || *sample.CPUMilliCores < 0 || sample.ObservedAt.After(now) || sample.ObservedAt.Before(now.Add(-time.Duration(policy.WindowHours)*time.Hour)) {
			continue
		}
		values = append(values, *sample.CPUMilliCores)
		if sample.ObservedAt.After(latest) {
			latest = sample.ObservedAt
		}
	}
	if len(values) < policy.MinSamples || latest.Before(now.Add(-15*time.Minute)) {
		return nil, nil
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	cpu := values[int(math.Ceil(0.75*float64(len(values))))-1]
	if cpu > 0 && cpu%5 != 0 {
		cpu += 5 - cpu%5
	}
	if cpu < 1 {
		cpu = 1
	}
	read := func(raw string, milli bool) (int64, error) {
		q, err := resource.ParseQuantity(raw)
		if err != nil {
			return 0, err
		}
		if milli {
			return q.MilliValue(), nil
		}
		if q.Value()%(1024*1024) != 0 {
			return 0, fmt.Errorf("live memory resource is not an exact MiB quantity")
		}
		return q.Value() / (1024 * 1024), nil
	}
	currentCPU, err := read(current.Requests["cpu"], true)
	if err != nil {
		return nil, err
	}
	// Keep the existing automatic request change thresholds (5m and 5%).
	if cpu >= currentCPU || currentCPU-cpu < 5 || float64(currentCPU-cpu)/float64(currentCPU) < 0.05 {
		return nil, nil
	}
	out := &model.ResourceSpec{CPUMilliCores: cpu}
	if out.MemoryMebibytes, err = read(current.Requests["memory"], false); err != nil {
		return nil, err
	}
	if out.MemoryLimitMebibytes, err = read(current.Limits["memory"], false); err != nil {
		return nil, err
	}
	if limit := current.Limits["cpu"]; limit != "" {
		if out.CPULimitMilliCores, err = read(limit, true); err != nil {
			return nil, err
		}
	}
	_, err = managedPostgresKubeResources(*out)
	return out, err
}

func (s *Service) reconcileManagedPostgresAutoCPURequest(ctx context.Context, client *kubeClient, app model.App) (bool, error) {
	cfg := s.Config.ManagedPostgresInPlaceResize
	if !s.Config.KubectlApply || !cfg.Enabled || len(cfg.AutoCPURequestDownscaleTargets) == 0 {
		return false, nil
	}
	policy := model.NormalizeAppRightSizingSpec(model.AppRightSizingSpec{Mode: model.AppRightSizingModeAuto})
	if app.Spec.RightSizing != nil {
		policy = model.NormalizeAppRightSizingSpec(*app.Spec.RightSizing)
	}
	if policy.Mode != model.AppRightSizingModeAuto {
		return false, nil
	}
	now := time.Now().UTC()
	for _, service := range app.BackingServices {
		if service.Spec.Postgres == nil || service.Type != model.BackingServiceTypePostgres || service.Provisioner != model.BackingServiceProvisionerManaged || service.Spec.Postgres.Suspended {
			continue
		}
		namespace := runtime.NamespaceForTenant(app.TenantID)
		enrollment := managedPostgresCPUEnrollment(cfg, service.ID, namespace, service.Spec.Postgres.ServiceName)
		if enrollment == nil {
			continue
		}
		samples, err := s.Store.ListResourceUsageSamples(app.TenantID, managedPostgresCPUUsageSampleKind, service.ID, now.Add(-time.Duration(policy.WindowHours)*time.Hour))
		if err != nil {
			return false, err
		}
		if len(samples) < policy.MinSamples {
			continue
		}
		snapshot, err := observeManagedPostgresResizeSnapshot(ctx, client, namespace, enrollment.Cluster)
		if err != nil {
			return false, err
		}
		if !managedPostgresCPUEnrollmentMatches(*enrollment, snapshot) {
			return false, fmt.Errorf("automatic PostgreSQL CPU request enrollment identity changed for service %s", service.ID)
		}
		target, err := managedPostgresAutoCPURequestTarget(snapshot.Observation.DesiredResources, samples, policy, now)
		if err != nil {
			return false, err
		}
		if target == nil {
			continue
		}
		ops, err := s.Store.ListOperationsByApp(app.TenantID, false, app.ID)
		if err != nil {
			return false, err
		}
		backoff := false
		for _, op := range ops {
			if op.ServiceID == service.ID && op.Type == model.OperationTypeDatabaseResize && strings.HasPrefix(op.RequestedByID, managedPostgresAutoCPUActor+"/") && op.UpdatedAt.After(now.Add(-6*time.Hour)) && op.Status == model.OperationStatusFailed {
				backoff = true
			}
		}
		if backoff {
			continue
		}
		op, result, err := s.Store.CreateOperationWithResult(model.Operation{
			TenantID: app.TenantID, AppID: app.ID, ServiceID: service.ID,
			Type: model.OperationTypeDatabaseResize, RequestedByType: model.ActorTypeSystem,
			RequestedByID: managedPostgresCPUActor(*enrollment),
			DesiredSpec:   &model.AppSpec{Postgres: &model.AppPostgresSpec{RuntimeResources: target}},
		})
		if errors.Is(err, store.ErrConflict) {
			return false, nil
		}
		if err == nil && result.Created && s.Logger != nil {
			s.Logger.Printf("automatic PostgreSQL CPU request downscale queued service=%s operation=%s cpu_millicores=%d", service.ID, op.ID, target.CPUMilliCores)
		}
		return err == nil, err
	}
	return false, nil
}
