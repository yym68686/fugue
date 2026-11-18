package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	"k8s.io/apimachinery/pkg/api/resource"
)

const managedPostgresAutoMemoryActor = store.ManagedPostgresAutoMemoryRequestActorPrefix

// The memory policy is independent of app right-sizing and CPU enrollment.
// Existing and new databases opt in by default through controller configuration.
// Preserve PostgreSQL's existing recommendation floor and P99 * 1.5 policy,
// with an additional peak guard. No creation-default (512Mi) floor is imposed.
func managedPostgresAutoMemoryRequestTarget(current kubeResourceRequirements, samples []model.ResourceUsageSample, startedAt, now time.Time) (*model.ResourceSpec, error) {
	values := make([]int64, 0, len(samples))
	seen := make(map[time.Time]bool, len(samples))
	var latest time.Time
	for _, sample := range samples {
		at := sample.ObservedAt.UTC()
		if sample.MemoryBytes == nil || *sample.MemoryBytes < 0 || at.After(now) || at.Before(now.Add(-168*time.Hour)) || at.Before(startedAt) || seen[at] {
			continue
		}
		seen[at] = true
		values = append(values, *sample.MemoryBytes)
		if at.After(latest) {
			latest = at
		}
	}
	if len(values) < 12 || latest.Before(now.Add(-15*time.Minute)) {
		return nil, nil
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	bytes := math.Max(float64(values[int(math.Ceil(.99*float64(len(values))))-1])*1.5, float64(values[len(values)-1])*1.2)
	// Reject overflow rather than converting an untrusted metric to a tiny target.
	if bytes >= float64(math.MaxInt64-16*1024*1024) {
		return nil, fmt.Errorf("automatic PostgreSQL memory metric exceeds the supported resource range")
	}
	memory := maxInt64(256, ((int64(math.Ceil(bytes/(1024*1024)))+15)/16)*16)
	out, err := managedPostgresLiveResourceSpec(current)
	if err != nil {
		return nil, err
	}
	before := out.MemoryMebibytes
	if memory == before || math.Abs(float64(memory-before))/float64(before) < .05 {
		return nil, nil
	}
	if memory < before {
		// A recent restart (including an OOM restart) is never a reason to shrink
		// the guarantee. Samples from a previous process are not reused either.
		if startedAt.IsZero() || startedAt.After(now.Add(-48*time.Hour)) {
			return nil, nil
		}
		memory = maxInt64(memory, int64(math.Ceil(float64(before)*.75)))
	}
	if memory > out.MemoryLimitMebibytes {
		return nil, fmt.Errorf("recommended memory request %dMi exceeds the unchanged memory limit %dMi; explicit limit authorization is required", memory, out.MemoryLimitMebibytes)
	}
	out.MemoryMebibytes = memory
	_, err = managedPostgresKubeResources(*out)
	return out, err
}

func managedPostgresLiveResourceSpec(current kubeResourceRequirements) (*model.ResourceSpec, error) {
	read := func(raw string, milli, optional bool) (int64, error) {
		if raw == "" && optional {
			return 0, nil
		}
		q, err := resource.ParseQuantity(raw)
		if err != nil || q.Sign() <= 0 {
			return 0, fmt.Errorf("automatic PostgreSQL memory resize requires positive live resources: %q", raw)
		}
		if milli {
			return q.MilliValue(), nil
		}
		if q.Value()%(1024*1024) != 0 {
			return 0, fmt.Errorf("live memory resource is not an exact MiB quantity")
		}
		return q.Value() / (1024 * 1024), nil
	}
	out := &model.ResourceSpec{}
	var err error
	if out.CPUMilliCores, err = read(current.Requests["cpu"], true, false); err != nil {
		return nil, err
	}
	if out.CPULimitMilliCores, err = read(current.Limits["cpu"], true, true); err != nil {
		return nil, err
	}
	if out.MemoryMebibytes, err = read(current.Requests["memory"], false, false); err != nil {
		return nil, err
	}
	if out.MemoryLimitMebibytes, err = read(current.Limits["memory"], false, false); err != nil {
		return nil, err
	}
	return out, nil
}

// The actor fences a queued recommendation to the exact database and process,
// not just a reusable Cluster name. The source memory request also fences drift
// between queueing and execution, while allowing our own target to converge.
func managedPostgresMemoryActor(snapshot managedPostgresResizeSnapshot, sourceMemory int64) string {
	o := snapshot.Observation
	identity, _ := json.Marshal([]any{o.Namespace, snapshot.Cluster.Metadata.Name, snapshot.Cluster.Metadata.UID,
		snapshot.Cluster.Status.SystemID, o.PodUID, o.ContainerStartedAt, o.RestartCount})
	sum := sha256.Sum256(identity)
	return managedPostgresAutoMemoryActor + "/" + hex.EncodeToString(sum[:]) + "/" + strconv.FormatInt(sourceMemory, 10)
}

func validateManagedPostgresAutomaticMemoryTarget(actor string, snapshot managedPostgresResizeSnapshot, target kubeResourceRequirements) error {
	parts := strings.Split(actor, "/")
	if len(parts) != 4 {
		return fmt.Errorf("invalid automatic PostgreSQL memory request actor")
	}
	source, err := strconv.ParseInt(parts[3], 10, 64)
	if snapshot.Cluster.Spec.Instances != 1 {
		return fmt.Errorf("automatic PostgreSQL memory request resize requires a single-instance database; replica-wide resize is not implemented")
	}
	if err != nil || source <= 0 || snapshot.Cluster.Status.SystemID == "" || managedPostgresMemoryActor(snapshot, source) != actor {
		return fmt.Errorf("automatic PostgreSQL memory request database or process identity changed")
	}
	current := snapshot.Observation.DesiredResources
	for _, pair := range [][2]string{{current.Requests["cpu"], target.Requests["cpu"]}, {current.Limits["memory"], target.Limits["memory"]}} {
		if !resizeQuantitiesEqual(pair[0], pair[1]) {
			return fmt.Errorf("automatic PostgreSQL memory request resize cannot change CPU or limits; live envelope changed")
		}
	}
	if !optionalResizeQuantitiesEqual(current.Limits["cpu"], target.Limits["cpu"]) {
		return fmt.Errorf("automatic PostgreSQL memory request resize cannot change the CPU limit; live envelope changed")
	}
	if !resizeQuantitiesEqual(current.Requests["memory"], fmt.Sprintf("%dMi", source)) && !resizeQuantitiesEqual(current.Requests["memory"], target.Requests["memory"]) {
		return fmt.Errorf("automatic PostgreSQL memory request changed since recommendation")
	}
	return nil
}

func optionalResizeQuantitiesEqual(left, right string) bool {
	if strings.TrimSpace(left) == "" || strings.TrimSpace(right) == "" {
		return strings.TrimSpace(left) == "" && strings.TrimSpace(right) == ""
	}
	return resizeQuantitiesEqual(left, right)
}

// Call only after validateManagedPostgresAutomaticMemoryTarget has authenticated
// the actor; malformed actors return zero and can never reach a mutation.
func automaticPostgresMemorySource(actor string) int64 {
	parts := strings.Split(actor, "/")
	if len(parts) != 4 {
		return 0
	}
	value, _ := strconv.ParseInt(parts[3], 10, 64)
	return value
}

func managedPostgresAutoMemoryCandidate(service model.BackingService, appID string) bool {
	return service.Type == model.BackingServiceTypePostgres && service.Provisioner == model.BackingServiceProvisionerManaged &&
		service.Spec.Postgres != nil && !service.Spec.Postgres.Suspended && (service.OwnerAppID == "" || service.OwnerAppID == appID)
}

func (s *Service) reconcileManagedPostgresAutoMemoryRequest(ctx context.Context, client *kubeClient, app model.App) (bool, error) {
	cfg := s.Config.ManagedPostgresInPlaceResize
	if !s.Config.KubectlApply || !cfg.Enabled || !cfg.AutoMemoryRequestResizeEnabled || (!cfg.MemoryRequestUpscaleEnabled && !cfg.MemoryRequestDownscaleEnabled) {
		return false, nil
	}
	var serviceIDs []string
	for _, service := range app.BackingServices {
		if managedPostgresAutoMemoryCandidate(service, app.ID) {
			serviceIDs = append(serviceIDs, service.ID)
		}
	}
	if len(serviceIDs) == 0 {
		return false, nil
	}
	active, err := s.appHasActiveOperation(app)
	if err != nil || active {
		return false, err
	}
	now := time.Now().UTC()
	backoffs, err := s.Store.ManagedPostgresAutoMemoryBackoffs(ctx, app.TenantID, app.ID, serviceIDs, now)
	if err != nil {
		return false, err
	}
	for _, service := range app.BackingServices {
		backoff := backoffs[service.ID]
		if !managedPostgresAutoMemoryCandidate(service, app.ID) || backoff.Blocked {
			continue
		}
		samples, err := s.Store.ListResourceUsageSamples(app.TenantID, managedPostgresCPUUsageSampleKind, service.ID, now.Add(-168*time.Hour))
		if err != nil {
			return false, err
		}
		if len(samples) < 12 {
			continue
		}
		namespace := runtime.NamespaceForTenant(app.TenantID)
		snapshot, err := observeManagedPostgresResizeSnapshot(ctx, client, namespace, service.Spec.Postgres.ServiceName)
		if err != nil {
			return false, err
		}
		if err := validateManagedPostgresResizeLiveIdentity(namespace, service.Spec.Postgres.ServiceName, snapshot.Cluster, snapshot.Observation, false); err != nil {
			return false, err
		}
		if snapshot.Cluster.Spec.Instances != 1 {
			continue
		}
		if snapshot.Cluster.Status.SystemID == "" {
			return false, fmt.Errorf("automatic PostgreSQL memory request requires the database system ID")
		}
		started, err := time.Parse(time.RFC3339Nano, snapshot.Observation.ContainerStartedAt)
		if err != nil {
			return false, err
		}
		target, err := managedPostgresAutoMemoryRequestTarget(snapshot.Observation.DesiredResources, samples, started, now)
		if err != nil {
			return false, err
		}
		if target == nil {
			continue
		}
		current, err := managedPostgresLiveResourceSpec(snapshot.Observation.DesiredResources)
		if err != nil {
			return false, err
		}
		downscale := target.MemoryMebibytes < current.MemoryMebibytes
		if downscale && (!cfg.MemoryRequestDownscaleEnabled || backoff.Downscale) || !downscale && !cfg.MemoryRequestUpscaleEnabled {
			continue
		}
		// Queue through the existing store admission and database lifecycle lock.
		// The executor records capability, init-floor, QoS and SQL failures as
		// durable evidence; failed attempts back off instead of retry-storming.
		op, result, err := s.Store.CreateOperationWithResult(model.Operation{
			TenantID: app.TenantID, AppID: app.ID, ServiceID: service.ID,
			Type: model.OperationTypeDatabaseResize, RequestedByType: model.ActorTypeSystem,
			RequestedByID: managedPostgresMemoryActor(snapshot, current.MemoryMebibytes),
			DesiredSpec:   &model.AppSpec{Postgres: &model.AppPostgresSpec{RuntimeResources: target}},
		})
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		if err == nil && result.Created && s.Logger != nil {
			s.Logger.Printf("automatic PostgreSQL memory request resize queued service=%s operation=%s before_mib=%d target_mib=%d memory_limit_mib=%d", service.ID, op.ID, current.MemoryMebibytes, target.MemoryMebibytes, target.MemoryLimitMebibytes)
		}
		return err == nil && result.Created, err
	}
	return false, nil
}

func validateManagedPostgresAutoMemoryNode(node kubeNode, found, downscale bool) error {
	if !found || !kubeNodeReady(node) {
		return fmt.Errorf("automatic PostgreSQL memory request requires a Ready node")
	}
	if downscale {
		for _, condition := range node.Status.Conditions {
			if condition.Type == "MemoryPressure" && condition.Status == "False" {
				return nil
			}
		}
		return fmt.Errorf("automatic PostgreSQL memory request downscale requires node MemoryPressure=False")
	}
	return nil
}
