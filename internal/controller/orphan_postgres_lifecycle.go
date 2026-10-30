package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const orphanPostgresHibernateGracePeriod = 5 * time.Minute

// Initialize the retained desired state once, after a durable zero-workload
// observation. Subsequent explicit suspend/resume requests remain authoritative.
// Never infer database destruction from a missing control-plane app record.
func (s *Service) reconcileOrphanManagedPostgresLifecycle(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, app model.App) ([]runtime.ManagedBackingServiceStatus, error) {
	deployments := runtime.ManagedBackingServiceDeployments(app, managed.Spec.Scheduling)
	if len(deployments) == 0 {
		return nil, nil
	}
	switch managed.Metadata.Annotations[runtime.FugueAnnotationOrphanPostgresLifecycle] {
	case runtime.OrphanPostgresLifecycleAutomatic:
		if err := s.verifyOrphanPostgresHasNoStoreConsumers(app, deployments); err != nil {
			return nil, err
		}
		return s.reconcileMissingStoreManagedPostgresLifecycle(ctx, client, namespace, managed, app)
	case runtime.OrphanPostgresLifecycleExplicit:
		return s.reconcileMissingStoreManagedPostgresLifecycle(ctx, client, namespace, managed, app)
	case "":
	default:
		return nil, errors.New("unknown orphan PostgreSQL lifecycle intent")
	}
	now := time.Now().UTC()
	if s.now != nil {
		now = s.now().UTC()
	}
	if !orphanPostgresHibernateGraceElapsed(managed, now) {
		return managed.Status.BackingServices, nil
	}
	if err := s.verifyOrphanPostgresHasNoStoreConsumers(app, deployments); err != nil {
		return nil, err
	}

	var ops []map[string]any
	for index, service := range managed.Spec.BackingServices {
		if service.Spec.Postgres == nil || service.Type != model.BackingServiceTypePostgres || service.Provisioner != model.BackingServiceProvisionerManaged {
			continue
		}
		if owner := strings.TrimSpace(service.OwnerAppID); owner != "" && owner != app.ID {
			return nil, fmt.Errorf("orphan service %s has a different owner", service.ID)
		}
		// A recovery can change the desired cluster name while leaving the
		// original retained cluster alive. Repair only an unambiguous identity,
		// proved by both ownership UID and every resource ownership label.
		for _, deployment := range deployments {
			if deployment.ServiceID != service.ID {
				continue
			}
			name, err := resolveOrphanPostgresCluster(ctx, client, namespace, managed, deployment)
			if err != nil {
				return nil, err
			}
			if name != "" && name != deployment.ResourceName {
				ops = append(ops, map[string]any{"op": "add", "path": fmt.Sprintf("/spec/backingServices/%d/spec/postgres/service_name", index), "value": name})
			}
		}
		if !service.Spec.Postgres.Suspended {
			ops = append(ops, map[string]any{"op": "add", "path": fmt.Sprintf("/spec/backingServices/%d/spec/postgres/suspended", index), "value": true})
		}
	}
	if err := client.patchOrphanPostgresLifecycleIntent(ctx, namespace, managed, ops, runtime.OrphanPostgresLifecycleAutomatic); err != nil {
		return nil, err
	}
	// Re-observe the new generation and reverify the zero-workload proof on
	// the next pass before changing CNPG. This also fences concurrent adoption
	// and explicit lifecycle requests through the ManagedApp resource version.
	return managed.Status.BackingServices, nil
}

func orphanPostgresHibernateGraceElapsed(managed runtime.ManagedAppObject, now time.Time) bool {
	if managed.Status.ObservedGeneration != managed.Metadata.Generation || !managedAppOrphanWorkloadZeroVerified(managed.Status) {
		return false
	}
	for _, condition := range managed.Status.Conditions {
		if condition.Type == managedAppOrphanWorkloadZeroConditionType {
			observed, err := time.Parse(time.RFC3339Nano, condition.LastTransitionTime)
			return err == nil && now.Sub(observed) >= orphanPostgresHibernateGracePeriod
		}
	}
	return false
}

func (s *Service) verifyOrphanPostgresHasNoStoreConsumers(app model.App, deployments []runtime.ManagedBackingServiceDeployment) error {
	if s.Store == nil {
		return errors.New("orphan PostgreSQL store evidence unavailable")
	}
	if _, err := s.Store.GetApp(app.ID); !errors.Is(err, store.ErrNotFound) {
		if err == nil {
			return errors.New("orphan app was restored before PostgreSQL hibernation")
		}
		return fmt.Errorf("recheck orphan app absence: %w", err)
	}
	apps, err := s.Store.ListApps(app.TenantID, false)
	if err != nil {
		return fmt.Errorf("list orphan PostgreSQL consumers: %w", err)
	}
	for _, deployment := range deployments {
		for _, consumer := range apps {
			if controllerAppHasServiceBinding(consumer, deployment.ServiceID) {
				return fmt.Errorf("orphan PostgreSQL service %s still has consumer %s", deployment.ServiceID, consumer.ID)
			}
		}
		if _, err := s.Store.GetBackingService(deployment.ServiceID); !errors.Is(err, store.ErrNotFound) {
			if err == nil {
				return fmt.Errorf("orphan PostgreSQL service %s still exists in store", deployment.ServiceID)
			}
			return fmt.Errorf("recheck orphan PostgreSQL service %s: %w", deployment.ServiceID, err)
		}
	}
	return nil
}

func resolveOrphanPostgresCluster(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, deployment runtime.ManagedBackingServiceDeployment) (string, error) {
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, deployment.ResourceName)
	if err != nil {
		return "", err
	}
	if found {
		// Foreign ownership is reported by the observer; never redirect a
		// stale orphan onto a similarly named cluster belonging to another app.
		return deployment.ResourceName, nil
	}
	selector := fmt.Sprintf("%s=%s,%s=%s", runtime.FugueLabelOwnerAppID, managed.Spec.AppID, runtime.FugueLabelBackingServiceID, deployment.ServiceID)
	names, err := client.listCloudNativePGClusterNamesByLabel(ctx, namespace, selector)
	if err != nil {
		return "", err
	}
	resolved := ""
	for _, name := range names {
		cluster, found, err = client.getCloudNativePGCluster(ctx, namespace, name)
		if err != nil {
			return "", err
		}
		if !found || !cloudNativePGClusterOwnedByManagedApp(cluster, managed) || cluster.Metadata.DeletionTimestamp != "" {
			continue
		}
		if !managedPostgresClusterLabelsMatch(cluster, managed, deployment.ServiceID) {
			continue
		}
		if resolved != "" {
			return "", fmt.Errorf("orphan PostgreSQL service %s has multiple owned retained clusters", deployment.ServiceID)
		}
		resolved = name
	}
	return resolved, nil
}

func managedPostgresClusterLabelsMatch(cluster kubeCloudNativePGCluster, managed runtime.ManagedAppObject, serviceID string) bool {
	labels := cluster.Metadata.Labels
	return labels[runtime.FugueLabelTenantID] == managed.Spec.TenantID && labels[runtime.FugueLabelProjectID] == managed.Spec.ProjectID &&
		labels[runtime.FugueLabelOwnerAppID] == managed.Spec.AppID && labels[runtime.FugueLabelBackingServiceID] == serviceID &&
		labels[runtime.FugueLabelManagedBy] == runtime.FugueLabelManagedByValue && labels[runtime.FugueLabelBackingServiceType] == model.BackingServiceTypePostgres
}

func (c *kubeClient) patchOrphanPostgresLifecycleIntent(ctx context.Context, namespace string, managed runtime.ManagedAppObject, changes []map[string]any, intent string) error {
	if managed.Metadata.UID == "" || managed.Metadata.ResourceVersion == "" {
		return errors.New("orphan PostgreSQL intent requires exact ManagedApp UID and resource version")
	}
	annotations := make(map[string]string, len(managed.Metadata.Annotations)+1)
	for key, value := range managed.Metadata.Annotations {
		annotations[key] = value
	}
	delete(annotations, runtime.FugueAnnotationOrphanPostgresLifecycle)
	if intent != "" {
		annotations[runtime.FugueAnnotationOrphanPostgresLifecycle] = intent
	}
	ops := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": managed.Metadata.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": managed.Metadata.ResourceVersion},
		{"op": "add", "path": "/metadata/annotations", "value": annotations},
	}
	ops = append(ops, changes...)
	_, err := c.doRequest(ctx, http.MethodPatch, managedAppAPIPath(namespace, managed.Metadata.Name), "application/json-patch+json", ops, nil)
	return err
}
