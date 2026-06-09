package store

import (
	"strings"

	"fugue/internal/model"
)

// GetTenantResourceCommitment returns compute requested by all live apps and
// managed backing services, including workloads placed on tenant-owned runtimes.
// It is an allocation guardrail and does not change which resources are billed.
func (s *Store) GetTenantResourceCommitment(tenantID string) (model.BillingResourceSpec, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return model.BillingResourceSpec{}, ErrInvalidInput
	}
	apps, err := s.ListAppsMetadata(tenantID, false)
	if err != nil {
		return model.BillingResourceSpec{}, err
	}
	services, err := s.ListBackingServices(tenantID, false)
	if err != nil {
		return model.BillingResourceSpec{}, err
	}

	total := model.BillingResourceSpec{}
	for _, app := range apps {
		replicas := app.Status.CurrentReplicas
		if replicas <= 0 {
			replicas = app.Spec.Replicas
		}
		if replicas <= 0 {
			continue
		}
		resources := model.DefaultManagedAppResources()
		if app.Spec.Resources != nil {
			resources = *app.Spec.Resources
		}
		total.CPUMilliCores += resources.CPUMilliCores * int64(replicas)
		total.MemoryMebibytes += resources.MemoryMebibytes * int64(replicas)
	}
	for _, service := range services {
		if service.Type != model.BackingServiceTypePostgres || service.Spec.Postgres == nil {
			continue
		}
		resources := model.DefaultManagedPostgresResources()
		if service.Spec.Postgres.Resources != nil {
			resources = *service.Spec.Postgres.Resources
		}
		total.CPUMilliCores += resources.CPUMilliCores
		total.MemoryMebibytes += resources.MemoryMebibytes
	}
	return total, nil
}
