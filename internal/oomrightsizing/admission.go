package oomrightsizing

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"fugue/internal/model"
)

// AdmissionPorts exposes a single command, not the general operation writer.
type AdmissionPorts struct {
	HasActive  func(model.App) (bool, error)
	Exists     func(model.App, string) (bool, error)
	Baseline   func(model.App) (model.AppSpec, *model.AppSource, error)
	Validate   func(model.App, model.AppSpec) error
	Submit     func(model.App, string, model.AppSpec, *model.AppSource) error
	IsConflict func(error) bool
}

func Enqueue(app model.App, targets []Target, ports AdmissionPorts) error {
	if hasActive, err := ports.HasActive(app); err != nil {
		return err
	} else if hasActive {
		return nil
	}
	requestedByID := oomRightSizingRequestedByID(targets)
	if exists, err := ports.Exists(app, requestedByID); err != nil {
		return err
	} else if exists {
		return nil
	}
	spec, source, err := ports.Baseline(app)
	if err != nil {
		return err
	}
	changed := false
	for _, target := range targets {
		switch target.Kind {
		case model.ClusterNodeWorkloadKindApp:
			current := spec.Resources
			if current == nil {
				defaults := model.DefaultManagedAppResources()
				current = &defaults
			}
			spec.Resources = oomExpandedResourceSpec(current, "")
			changed = true
		case model.ClusterNodeWorkloadKindBackingService:
			service := findBackingServiceByID(app.BackingServices, target.ID)
			if service == nil || service.Spec.Postgres == nil {
				continue
			}
			if spec.Postgres == nil {
				spec.Postgres = model.CloneAppPostgresSpec(service.Spec.Postgres)
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
	if err := ports.Validate(app, spec); err != nil {
		return err
	}
	err = ports.Submit(app, requestedByID, spec, source)
	if ports.IsConflict(err) {
		if exists, lookupErr := ports.Exists(app, requestedByID); lookupErr == nil && exists {
			return nil
		}
	}
	return err
}

func oomRightSizingRequestedByID(targets []Target) string {
	keys := eventKeys(targets)
	if len(keys) == 0 {
		for _, target := range targets {
			keys = append(keys, target.Kind+"/"+target.ID)
		}
	}
	sort.Strings(keys)
	sum := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return model.OperationRequestedByOOMRightSizing + "/" + hex.EncodeToString(sum[:12])
}

// ProjectTenantCommitment computes the next commitment and whether it crosses the allowance.
func ProjectTenantCommitment(app model.App, desired model.AppSpec, total, cap model.BillingResourceSpec) (model.BillingResourceSpec, bool) {
	currentTarget := autoscalingAppResourceCommitment(app, app.Spec)
	desiredTarget := autoscalingAppResourceCommitment(app, desired)
	currentPostgres := firstOwnedPostgresService(app)
	if currentPostgres != nil && currentPostgres.Spec.Postgres != nil {
		resources := model.ManagedPostgresRuntimeResources(*currentPostgres.Spec.Postgres)
		addAutoscalingResourceSpec(&currentTarget, &resources, model.DefaultManagedPostgresResources())
	}
	if desired.Postgres != nil {
		resources := model.ManagedPostgresRuntimeResources(*desired.Postgres)
		addAutoscalingResourceSpec(&desiredTarget, &resources, model.DefaultManagedPostgresResources())
	} else if currentPostgres != nil && currentPostgres.Spec.Postgres != nil {
		resources := model.ManagedPostgresRuntimeResources(*currentPostgres.Spec.Postgres)
		addAutoscalingResourceSpec(&desiredTarget, &resources, model.DefaultManagedPostgresResources())
	}
	next := model.BillingResourceSpec{
		CPUMilliCores:   max(0, total.CPUMilliCores-currentTarget.CPUMilliCores) + desiredTarget.CPUMilliCores,
		MemoryMebibytes: max(0, total.MemoryMebibytes-currentTarget.MemoryMebibytes) + desiredTarget.MemoryMebibytes,
	}
	return next, autoscalingResourcesExceed(next, cap) && autoscalingResourcesExceed(next, total)
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
	out.MemoryMebibytes = roundUpInt64(max(out.MemoryMebibytes*3/2, out.MemoryMebibytes+128), 16)
	out.MemoryLimitMebibytes = roundUpInt64(max(currentLimit*3/2, out.MemoryMebibytes+128), 16)
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

func eventKeys(targets []Target) []string {
	var keys []string
	for _, target := range targets {
		for _, key := range target.EventKeys {
			keys = append(keys, target.Kind+"/"+target.ID+"/"+key)
		}
	}
	return keys
}

func findBackingServiceByID(services []model.BackingService, id string) *model.BackingService {
	for index := range services {
		if strings.TrimSpace(services[index].ID) == strings.TrimSpace(id) {
			return &services[index]
		}
	}
	return nil
}

func roundUpInt64(value, step int64) int64 {
	if step <= 1 || value <= 0 {
		return value
	}
	remainder := value % step
	if remainder == 0 {
		return value
	}
	return value + step - remainder
}
