package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	managedPostgresAutoCPULimitActor = store.ManagedPostgresAutoCPULimitActorPrefix
	// Kubernetes exec must close the stream even after the command returns.
	// Allow for transport delays and scheduling under a saturated CPU quota.
	managedPostgresAutoCPULimitExecTimeout = 30 * time.Second
)

func managedPostgresCPULimitEnrollment(cfg config.ManagedPostgresInPlaceResizeConfig, serviceID, namespace, cluster string) *config.ManagedPostgresCPULimitTarget {
	for _, entry := range cfg.AutoCPULimitUpscaleTargets {
		if entry.ServiceID == serviceID && entry.Namespace == namespace && entry.Cluster == cluster {
			return &entry
		}
	}
	return nil
}

func managedPostgresCPULimitAutoAllowed(app model.App) bool {
	return app.Spec.RightSizing == nil || model.NormalizeAppRightSizingSpec(*app.Spec.RightSizing).Mode == model.AppRightSizingModeAuto
}

func managedPostgresCPULimitActor(snapshot managedPostgresResizeSnapshot, source int64) string {
	// Reuse the process identity digest, but keep the operation actor separate
	// from memory and request downscale so their policies cannot authorize it.
	return strings.Replace(managedPostgresMemoryActor(snapshot, source), managedPostgresAutoMemoryActor, managedPostgresAutoCPULimitActor, 1)
}

func managedPostgresNextCPULimit(current, maxLimit int64) int64 {
	if current <= 0 || current >= maxLimit || maxLimit > 64000 {
		return 0
	}
	return min(current*2, maxLimit)
}

func validateManagedPostgresAutoCPULimit(actor string, enrollment config.ManagedPostgresCPULimitTarget, snapshot managedPostgresResizeSnapshot, target kubeResourceRequirements) error {
	parts := strings.Split(actor, "/")
	if len(parts) != 4 || snapshot.Cluster.Spec.Instances != 1 || !managedPostgresCPUEnrollmentMatches(enrollment.ManagedPostgresCPURequestTarget, snapshot) {
		return fmt.Errorf("automatic PostgreSQL CPU limit requires the enrolled single-instance database")
	}
	source, err := strconv.ParseInt(parts[3], 10, 64)
	o := snapshot.Observation
	if err != nil || o.PodUID == "" || o.ContainerStartedAt == "" || managedPostgresCPULimitActor(snapshot, source) != actor {
		return fmt.Errorf("automatic PostgreSQL CPU limit database or process identity changed")
	}
	current := o.DesiredResources
	if !resizeQuantitiesEqual(current.Requests["cpu"], target.Requests["cpu"]) ||
		!resizeQuantitiesEqual(current.Requests["memory"], target.Requests["memory"]) ||
		!resizeQuantitiesEqual(current.Limits["memory"], target.Limits["memory"]) {
		return fmt.Errorf("automatic PostgreSQL CPU limit cannot change CPU request or memory; live envelope changed")
	}
	next := managedPostgresNextCPULimit(source, enrollment.MaxLimitMilliCores)
	if next == 0 || !resizeQuantitiesEqual(target.Limits["cpu"], fmt.Sprintf("%dm", next)) ||
		(!resizeQuantitiesEqual(current.Limits["cpu"], fmt.Sprintf("%dm", source)) && !resizeQuantitiesEqual(current.Limits["cpu"], target.Limits["cpu"])) {
		return fmt.Errorf("automatic PostgreSQL CPU limit source or bounded target changed")
	}
	return nil
}

// Multiple API replicas can interleave samples. Require a contiguous span
// of elevated readings, not a particular gap between the newest two rows.
// Exact container cgroup throttling must independently confirm pressure.
func managedPostgresCPURecentlySaturated(samples []model.ResourceUsageSample, now time.Time, currentLimit int64) bool {
	if currentLimit <= 0 {
		return false
	}
	var latest time.Time
	for _, sample := range samples {
		if !sample.ObservedAt.After(now) && sample.ObservedAt.After(latest) {
			latest = sample.ObservedAt
		}
	}
	if latest.Before(now.Add(-8 * time.Minute)) {
		return false
	}
	var lastLow, earlierHigh time.Time
	for _, sample := range samples {
		gap := latest.Sub(sample.ObservedAt)
		if gap < 0 || gap > 8*time.Minute {
			continue
		}
		if sample.CPUMilliCores == nil || *sample.CPUMilliCores < currentLimit-currentLimit/10 {
			if sample.ObservedAt.After(lastLow) {
				lastLow = sample.ObservedAt
			}
		} else if gap >= 3*time.Minute && sample.ObservedAt.After(earlierHigh) {
			// Choose the most recent qualifying older observation, so a low
			// reading preceding it does not hide a newer sustained window.
			earlierHigh = sample.ObservedAt
		}
	}
	return earlierHigh.After(lastLow)
}

const managedPostgresReadCPUWindow = `
read -r quota period < /sys/fs/cgroup/cpu.max
printf '%s %s ' "$quota" "$period"
sample() {
  periods=
  throttled=
  while read -r key value; do
    case "$key" in
      nr_periods) periods=$value ;;
      nr_throttled) throttled=$value ;;
    esac
  done < /sys/fs/cgroup/cpu.stat
  test -n "$periods" && test -n "$throttled" || return 1
  printf '%s %s\n' "$periods" "$throttled"
}
sample
sleep 3
read -r quota2 period2 < /sys/fs/cgroup/cpu.max
test "$quota:$period" = "$quota2:$period2"
sample
`

func managedPostgresCPUWindowThrottled(raw string, limit int64) bool {
	fields := strings.Fields(raw)
	if len(fields) != 6 || limit <= 0 || limit > 64000 {
		return false
	}
	var values [6]int64
	for i, field := range fields {
		n, err := strconv.ParseInt(field, 10, 64)
		if err != nil || n < 0 {
			return false
		}
		values[i] = n
	}
	// Refuse unlimited, a mismatched quota, or a window that appears to span a
	// counter reset/restart. Linux cgroup bandwidth period is <= 1 second.
	quota, period := values[0], values[1]
	if quota <= 0 || period < 1000 || period > 1000000 || quota != limit*period/1000 {
		return false
	}
	periods, throttled := values[4]-values[2], values[5]-values[3]
	return periods >= 20 && throttled >= periods/2+periods%2 && throttled <= periods
}

func confirmManagedPostgresCPULimitPressure(ctx context.Context, client *kubeClient, o managedPostgresResizeObservation, limit int64) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, managedPostgresAutoCPULimitExecTimeout)
	defer cancel()
	var out bytes.Buffer
	if err := coldExec(ctx, client, o.Namespace, o.PodName, o.ContainerName, []string{"sh", "-ec", managedPostgresReadCPUWindow}, &out); err != nil {
		return false, err
	}
	return managedPostgresCPUWindowThrottled(out.String(), limit), nil
}

// The controller must not require general access to a tenant's PostgreSQL
// network. Probe the fenced primary via CNPG's local socket and roll back the
// temporary write; do not read or pass app credentials for this check.
func probeManagedPostgresAutoCPULimit(ctx context.Context, client *kubeClient, snapshot managedPostgresResizeSnapshot) error {
	const sql = `SET statement_timeout = '5s'; BEGIN;
CREATE TEMP TABLE fugue_cpu_limit_probe (value integer NOT NULL) ON COMMIT DROP;
INSERT INTO fugue_cpu_limit_probe VALUES (1);
SELECT pg_is_in_recovery(), current_setting('transaction_read_only'), system_identifier, value
FROM pg_control_system(), fugue_cpu_limit_probe;
ROLLBACK;`
	ctx, cancel := context.WithTimeout(ctx, managedPostgresAutoCPULimitExecTimeout)
	defer cancel()
	o := snapshot.Observation
	var out bytes.Buffer
	if err := coldExec(ctx, client, o.Namespace, o.PodName, o.ContainerName, []string{
		"psql", "-XAtq", "-h", "/controller/run", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", sql,
	}, &out); err != nil {
		return err
	}
	if strings.TrimSpace(out.String()) != "f|off|"+snapshot.Cluster.Status.SystemID+"|1" {
		return fmt.Errorf("automatic PostgreSQL CPU limit local probe did not verify the expected writable primary")
	}
	return nil
}

func managedPostgresCPULimitCoolingDown(ops []model.Operation, serviceID string, now time.Time) bool {
	for _, op := range ops {
		if op.ServiceID != serviceID || op.Type != model.OperationTypeDatabaseResize || !strings.HasPrefix(op.RequestedByID, managedPostgresAutoCPULimitActor+"/") {
			continue
		}
		window := 30 * time.Minute
		if op.Status == model.OperationStatusCompleted {
			window = 10 * time.Minute
		}
		if op.UpdatedAt.After(now.Add(-window)) {
			return true
		}
	}
	return false
}

func (s *Service) reconcileManagedPostgresAutoCPULimit(ctx context.Context, client *kubeClient, app model.App) (bool, error) {
	cfg := s.Config.ManagedPostgresInPlaceResize
	if !s.Config.KubectlApply || !cfg.Enabled || !cfg.CPULimitUpscaleEnabled || len(cfg.AutoCPULimitUpscaleTargets) == 0 {
		return false, nil
	}
	now := time.Now().UTC()
	for _, service := range app.BackingServices {
		pg := service.Spec.Postgres
		if pg == nil || pg.Suspended || service.OwnerAppID != app.ID || service.Type != model.BackingServiceTypePostgres || service.Provisioner != model.BackingServiceProvisionerManaged {
			continue
		}
		namespace := runtime.NamespaceForTenant(app.TenantID)
		enrollment := managedPostgresCPULimitEnrollment(cfg, service.ID, namespace, pg.ServiceName)
		if enrollment == nil {
			continue
		}
		logSkip := func(reason string) {
			if s.Logger != nil {
				s.Logger.Printf("automatic PostgreSQL CPU limit upscale skipped service=%s reason=%s", service.ID, reason)
			}
		}
		if !managedPostgresCPULimitAutoAllowed(app) {
			logSkip("app_right_sizing_disabled")
			continue
		}
		ops, err := s.Store.ListOperationSummariesFiltered(app.TenantID, false, store.OperationListFilter{
			AppID: app.ID, ServiceID: service.ID, Types: []string{model.OperationTypeDatabaseResize},
		})
		if err != nil {
			return false, err
		}
		if managedPostgresCPULimitCoolingDown(ops, service.ID, now) {
			logSkip("cooldown")
			continue
		}
		snapshot, err := observeManagedPostgresResizeSnapshot(ctx, client, namespace, enrollment.Cluster)
		if err != nil {
			return false, err
		}
		o := snapshot.Observation
		if snapshot.Cluster.Spec.Instances != 1 || !managedPostgresCPUEnrollmentMatches(enrollment.ManagedPostgresCPURequestTarget, snapshot) {
			return false, fmt.Errorf("automatic PostgreSQL CPU limit enrollment identity or instance count changed")
		}
		if err := validateManagedPostgresResizeLiveIdentity(namespace, enrollment.Cluster, snapshot.Cluster, o, false); err != nil {
			return false, err
		}
		if o.ActualResources == nil || !managedPostgresResizeResourcesEqual(*o.ActualResources, o.DesiredResources) || o.ContainerStartedAt == "" || !o.PodReady || !o.ContainerReady {
			logSkip("pod_resources_not_converged")
			continue
		}
		target, err := managedPostgresLiveResourceSpec(o.DesiredResources)
		if err != nil {
			return false, err
		}
		current := target.CPULimitMilliCores
		if target.CPULimitMilliCores = managedPostgresNextCPULimit(current, enrollment.MaxLimitMilliCores); target.CPULimitMilliCores == 0 {
			logSkip("no_bounded_increase")
			continue
		}
		samples, err := s.Store.ListResourceUsageSamples(app.TenantID, managedPostgresCPUUsageSampleKind, service.ID, now.Add(-15*time.Minute))
		if err != nil {
			return false, err
		}
		if !managedPostgresCPURecentlySaturated(samples, now, current) {
			logSkip("no_sustained_cpu_saturation")
			continue
		}
		resources, err := managedPostgresKubeResources(*target)
		if err != nil {
			return false, err
		}
		if result := assessManagedPostgresResize(o, resources, managedPostgresResizeSafetyOptions{AllowLimitChanges: true}); result.State != managedPostgresResizeStateReady {
			logSkip(result.Reason)
			continue
		}
		pressured, err := confirmManagedPostgresCPULimitPressure(ctx, client, o, current)
		if err != nil {
			return false, err
		}
		if !pressured {
			logSkip("container_throttling_not_confirmed")
			continue
		}
		op, result, err := s.Store.CreateOperationWithResult(model.Operation{
			TenantID: app.TenantID, AppID: app.ID, ServiceID: service.ID, Type: model.OperationTypeDatabaseResize,
			RequestedByType: model.ActorTypeSystem, RequestedByID: managedPostgresCPULimitActor(snapshot, current),
			DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{RuntimeResources: target}},
		})
		if errors.Is(err, store.ErrConflict) {
			logSkip("operation_admission_conflict")
			return false, nil
		}
		if err == nil && result.Created && s.Logger != nil {
			s.Logger.Printf("automatic PostgreSQL CPU limit upscale queued service=%s operation=%s from=%dm to=%dm", service.ID, op.ID, current, target.CPULimitMilliCores)
		}
		return err == nil, err
	}
	return false, nil
}
