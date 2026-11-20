package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const postgresRestoreRuntimeActor = store.ManagedPostgresRestoreRuntimeActorPrefix

// This is not policy recomputation. A replacement may replay only the latest
// completed resize of the same database and only when it started at the known
// bootstrap template. A foreign resource edit must not be overwritten.
func postgresRuntimeRestoreBaseline(serviceID string, pg model.AppPostgresSpec, snapshot managedPostgresResizeSnapshot) (kubeResourceRequirements, kubeResourceRequirements, error) {
	cluster, pod := snapshot.Cluster, snapshot.Observation
	if serviceID == "" || pg.Resources == nil || pg.RuntimeResources == nil || cluster.Spec.Instances != 1 || pg.Suspended ||
		cluster.Status.SystemID == "" || cluster.Metadata.Labels[runtime.FugueLabelBackingServiceID] != serviceID ||
		cluster.Metadata.DeletionTimestamp != "" || len(cluster.Status.DanglingPVC) > 0 {
		return kubeResourceRequirements{}, kubeResourceRequirements{}, fmt.Errorf("resource restoration requires the exact active single-instance database")
	}
	a := cluster.Metadata.Annotations
	if a[runtime.CloudNativePGHibernationAnno] == runtime.CloudNativePGHibernationOn ||
		a[coldReconciliationAnnotation] == coldReconciliationDisabled ||
		(a["cnpg.io/fencedInstances"] != "" && a["cnpg.io/fencedInstances"] != "[]") {
		return kubeResourceRequirements{}, kubeResourceRequirements{}, fmt.Errorf("resource restoration is blocked by database lifecycle state")
	}
	if len(pod.Containers) != 1 || pod.PodResources != nil {
		return kubeResourceRequirements{}, kubeResourceRequirements{}, fmt.Errorf("resource restoration requires a single database container without Pod-level resources")
	}
	bootstrap, err := managedPostgresKubeResources(*pg.Resources)
	if err != nil {
		return bootstrap, kubeResourceRequirements{}, err
	}
	target, err := managedPostgresKubeResources(*pg.RuntimeResources)
	if err != nil {
		return bootstrap, target, err
	}
	if !managedPostgresResizeResourcesEqual(cluster.Spec.Resources, bootstrap) {
		return bootstrap, target, fmt.Errorf("live CNPG template differs from the stored bootstrap resources")
	}
	return bootstrap, target, nil
}

func (s *Service) postgresRuntimeRestoreProof(app model.App, serviceID string, pg model.AppPostgresSpec, snapshot managedPostgresResizeSnapshot) (model.Operation, error) {
	if pg.RuntimeResources == nil {
		return model.Operation{}, fmt.Errorf("no persisted runtime target to restore")
	}
	ops, err := s.Store.ListOperationsFiltered(app.TenantID, false, store.OperationListFilter{
		AppID: app.ID, ServiceID: serviceID, Types: []string{model.OperationTypeDatabaseResize}, Statuses: []string{model.OperationStatusCompleted}, Limit: 1,
	})
	if err != nil {
		return model.Operation{}, err
	}
	if len(ops) != 1 {
		return model.Operation{}, fmt.Errorf("no completed resize for this database")
	}
	prior := ops[0]
	if prior.CompletedAt == nil || prior.DesiredSpec == nil || prior.DesiredSpec.Postgres == nil || prior.DesiredSpec.Postgres.RuntimeResources == nil ||
		*prior.DesiredSpec.Postgres.RuntimeResources != *pg.RuntimeResources {
		return model.Operation{}, fmt.Errorf("no latest completed resize matches the persisted resource target")
	}
	evidence, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{
		TenantID: app.TenantID, OperationID: prior.ID, Types: []string{model.OperationEvidenceTypePostgresResizeVerified}, Limit: 1,
	})
	if err != nil {
		return model.Operation{}, err
	}
	if len(evidence) != 1 || !verifiedPostgresRuntimeRestoreSource(prior, evidence[0], *pg.RuntimeResources, snapshot) {
		return model.Operation{}, fmt.Errorf("completed resize does not prove the same database on an earlier Pod")
	}
	return prior, nil
}

func verifiedPostgresRuntimeRestoreSource(prior model.Operation, evidence model.OperationEvidence, target model.ResourceSpec, snapshot managedPostgresResizeSnapshot) bool {
	var payload struct {
		ClusterName        string                   `json:"cluster_name"`
		ClusterUID         string                   `json:"cluster_uid"`
		SystemID           string                   `json:"system_id"`
		PodUID             string                   `json:"pod_uid"`
		ContainerStartedAt string                   `json:"container_started_at"`
		RestartCount       int                      `json:"restart_count"`
		Resources          kubeResourceRequirements `json:"resources"`
	}
	raw, err := json.Marshal(evidence.Payload)
	if err != nil || json.Unmarshal(raw, &payload) != nil {
		return false
	}
	cluster, pod := snapshot.Cluster, snapshot.Observation
	// Historical CPU operations recorded the exact Cluster UID and database
	// system ID in their system actor before this field was added to evidence.
	if payload.SystemID == "" && prior.RequestedByType == model.ActorTypeSystem &&
		prior.RequestedByID == managedPostgresAutoCPUActor+"/"+cluster.Metadata.UID+"/"+cluster.Status.SystemID {
		payload.SystemID = cluster.Status.SystemID
	}
	// Historical memory actors bind the same identity and the previous process.
	if payload.SystemID == "" && prior.RequestedByType == model.ActorTypeSystem && payload.ContainerStartedAt != "" {
		source := automaticPostgresMemorySource(prior.RequestedByID)
		previous := snapshot
		previous.Observation.PodUID = payload.PodUID
		previous.Observation.ContainerStartedAt = payload.ContainerStartedAt
		previous.Observation.RestartCount = payload.RestartCount
		if source > 0 && prior.RequestedByID == managedPostgresMemoryActor(previous, source) {
			payload.SystemID = cluster.Status.SystemID
		}
	}
	resources, err := managedPostgresKubeResources(target)
	return err == nil && evidence.OperationID == prior.ID && evidence.Type == model.OperationEvidenceTypePostgresResizeVerified &&
		evidence.PayloadVersion == managedPostgresResizeEvidencePayloadVersion &&
		evidence.SubjectNamespace == pod.Namespace && payload.ClusterName == cluster.Metadata.Name &&
		payload.ClusterUID == cluster.Metadata.UID && payload.SystemID != "" && payload.SystemID == cluster.Status.SystemID &&
		payload.PodUID != "" && payload.PodUID == evidence.SubjectUID && payload.PodUID != pod.PodUID &&
		managedPostgresResizeResourcesEqual(payload.Resources, resources)
}

func postgresRuntimeRestoreActor(snapshot managedPostgresResizeSnapshot, priorID string) string {
	return strings.Join([]string{postgresRestoreRuntimeActor, snapshot.Cluster.Metadata.UID, snapshot.Cluster.Status.SystemID, snapshot.Observation.PodUID, priorID}, "/")
}

func postgresRuntimeRestoreMatches(snapshot managedPostgresResizeSnapshot, actor string) bool {
	parts := strings.Split(actor, "/")
	return len(parts) == 6 && parts[5] != "" && actor == postgresRuntimeRestoreActor(snapshot, parts[5])
}

func postgresRuntimeRestoreAtBootstrap(snapshot managedPostgresResizeSnapshot, bootstrap kubeResourceRequirements) bool {
	pod := snapshot.Observation
	return pod.ActualResources != nil && managedPostgresResizeResourcesEqual(pod.DesiredResources, bootstrap) &&
		managedPostgresResizeResourcesEqual(*pod.ActualResources, bootstrap)
}

// Automatic restoration never changes limits. Only a planned restart can
// apply historical limits. Request changes still follow today's direction,
// node pressure, QoS, init-floor and fresh recommendation safeguards.
func (s *Service) postgresRuntimeRestoreRequestPolicy(ctx context.Context, client *kubeClient, app model.App, serviceID string, snapshot managedPostgresResizeSnapshot, bootstrap, target kubeResourceRequirements, gates config.ManagedPostgresInPlaceResizeConfig) error {
	pod := snapshot.Observation
	changed, _, decreased := compareResizeResources(bootstrap, target)
	if hasLimitChange(changed) {
		return fmt.Errorf("runtime restore would change limits; a planned database restart is required")
	}
	stages, err := planManagedPostgresResizeStages(bootstrap, target)
	if err != nil {
		return err
	}
	if len(stages) != 1 {
		return fmt.Errorf("runtime restore requires a single in-place stage")
	}
	if err := validateManagedPostgresResizePlanPolicy(pod, bootstrap, stages, gates); err != nil {
		return err
	}
	if !containsResizeResource(changed, "requests.memory") {
		return nil
	}
	node, found, err := client.getNode(ctx, pod.NodeName)
	if err != nil {
		return err
	}
	downscale := containsResizeResource(decreased, "requests.memory")
	if err := validateManagedPostgresAutoMemoryNode(node, found, downscale); err != nil {
		return err
	}
	if !downscale {
		return nil
	}
	started, err := time.Parse(time.RFC3339Nano, pod.ContainerStartedAt)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	samples, err := s.Store.ListResourceUsageSamples(app.TenantID, managedPostgresCPUUsageSampleKind, serviceID, now.Add(-168*time.Hour))
	if err != nil {
		return err
	}
	recommended, err := managedPostgresAutoMemoryRequestTarget(bootstrap, samples, started, now)
	if err != nil {
		return err
	}
	request, err := managedPostgresLiveResourceSpec(target)
	if err != nil {
		return err
	}
	if recommended == nil || recommended.MemoryMebibytes > request.MemoryMebibytes {
		return fmt.Errorf("current memory samples do not authorize the historical request downscale")
	}
	return nil
}

// Keep automatic recommendations from persisting a new target based on a
// re-created Pod still at its bootstrap resources. They can resume after the
// old target is restored, or after an explicit operator resize.
func (s *Service) reconcilePostgresRuntimeResources(ctx context.Context, client *kubeClient, app model.App) (model.App, bool) {
	cfg := s.Config.ManagedPostgresInPlaceResize
	if !s.Config.KubectlApply || !cfg.Enabled {
		return app, false
	}
	autoApp := app
	autoApp.BackingServices = nil
	for _, service := range app.BackingServices {
		pg := service.Spec.Postgres
		if !managedPostgresAutoMemoryCandidate(service, app.ID) || pg.RuntimeResources == nil || pg.Resources == nil || *pg.RuntimeResources == *pg.Resources {
			autoApp.BackingServices = append(autoApp.BackingServices, service)
			continue
		}
		namespace := runtime.NamespaceForTenant(app.TenantID)
		snapshot, err := observeManagedPostgresResizeSnapshot(ctx, client, namespace, pg.ServiceName)
		if err != nil {
			continue
		}
		target, err := managedPostgresKubeResources(*pg.RuntimeResources)
		if err != nil {
			continue
		}
		pod := snapshot.Observation
		if postgresRuntimeRestoreAtBootstrap(snapshot, target) {
			autoApp.BackingServices = append(autoApp.BackingServices, service)
			continue
		}
		bootstrap, _, err := postgresRuntimeRestoreBaseline(service.ID, *pg, snapshot)
		if err != nil || !cfg.RecoveryEnabled || !postgresRuntimeRestoreAtBootstrap(snapshot, bootstrap) ||
			validateManagedPostgresResizeLiveIdentity(namespace, pg.ServiceName, snapshot.Cluster, pod, false) != nil {
			continue
		}
		gates := cfg
		if enrollment := managedPostgresCPUEnrollment(gates, service.ID, namespace, pg.ServiceName); enrollment != nil && managedPostgresCPUEnrollmentMatches(*enrollment, snapshot) {
			gates.CPURequestDownscaleEnabled = true
		}
		prior, err := s.postgresRuntimeRestoreProof(app, service.ID, *pg, snapshot)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Printf("PostgreSQL runtime resource restoration unproven service=%s: %v", service.ID, err)
			}
			continue
		}
		if err := s.postgresRuntimeRestoreRequestPolicy(ctx, client, app, service.ID, snapshot, bootstrap, target, gates); err != nil {
			if s.Logger != nil {
				s.Logger.Printf("PostgreSQL runtime resource restoration waiting service=%s previous_resize=%s: %v", service.ID, prior.ID, err)
			}
			continue
		}
		actor := postgresRuntimeRestoreActor(snapshot, prior.ID)
		recent, err := s.Store.ListOperationSummariesFiltered(app.TenantID, false, store.OperationListFilter{
			AppID: app.ID, ServiceID: service.ID, ActorID: actor, Types: []string{model.OperationTypeDatabaseResize}, Limit: 1,
		})
		if err != nil {
			continue
		}
		if len(recent) > 0 && recent[0].UpdatedAt.After(time.Now().Add(-6*time.Hour)) {
			continue
		}
		op, result, err := s.Store.CreateOperationWithResult(model.Operation{
			TenantID: app.TenantID, AppID: app.ID, ServiceID: service.ID, Type: model.OperationTypeDatabaseResize,
			RequestedByType: model.ActorTypeSystem, RequestedByID: actor,
			DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{RuntimeResources: pg.RuntimeResources}},
		})
		if err == nil && result.Created {
			if result.Created && s.Logger != nil {
				s.Logger.Printf("PostgreSQL runtime resource restoration queued service=%s operation=%s previous_resize=%s", service.ID, op.ID, prior.ID)
			}
			return autoApp, true
		}
		if err != nil && !errors.Is(err, store.ErrConflict) && s.Logger != nil {
			s.Logger.Printf("PostgreSQL resource restoration deferred service=%s: %v", service.ID, err)
		}
	}
	return autoApp, false
}
