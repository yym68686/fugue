package controller

import (
	"context"
	"fmt"
	"strconv"

	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/runtime"
)

// CNPG does not hibernate a Cluster before its first primary is healthy.
// For an orphan which never acquired storage, suspend its exact initdb Job
// instead. Retaining the unfinished Job prevents CNPG from reissuing bootstrap;
// neither the Cluster, PVC, nor its native hibernation status is rewritten.
func (s *Service) reconcileOrphanPostgresBootstrap(ctx context.Context, c *kubeClient, ns string, managed runtime.ManagedAppObject, app model.App, deployment runtime.ManagedBackingServiceDeployment, cluster kubeCloudNativePGCluster) (bool, error) {
	if cluster.Status.Phase != "Setting up primary" || cluster.Status.CurrentPrimary != "" ||
		cluster.Status.ReadyInstances != 0 || cluster.Status.SystemID != "" || cluster.Spec.Instances != 1 ||
		cluster.Metadata.DeletionTimestamp != "" || cluster.Metadata.UID == "" ||
		!managedPostgresClusterLabelsMatch(cluster, managed, deployment.ServiceID) {
		return false, nil
	}
	annotations := cluster.Metadata.Annotations
	if annotations[coldMigrationAnnotation] != "" || annotations[coldReconciliationAnnotation] == coldReconciliationDisabled ||
		(annotations["cnpg.io/fencedInstances"] != "" && annotations["cnpg.io/fencedInstances"] != "[]") {
		return false, nil
	}
	// The first initdb is the only attempt whose lack of execution is proven
	// by this path. Recovery/bootstrap imports and retries are not eligible.
	instance := cluster.Metadata.Name + "-" + strconv.Itoa(1)
	if cluster.Status.TargetPrimary != instance {
		return false, nil
	}
	if deployment.Suspended {
		condition, found := cloudNativePGHibernationCondition(cluster)
		if annotations[runtime.CloudNativePGHibernationAnno] != runtime.CloudNativePGHibernationOn ||
			!found || condition.Status != "False" || condition.Reason != "WaitingForHealthy" ||
			managed.Status.ObservedGeneration != managed.Metadata.Generation || !managedAppOrphanWorkloadZeroVerified(managed.Status) {
			return false, nil
		}
	}
	job, found, err := c.getRawObject(ctx, postgresJobPath(ns, instance+"-initdb"))
	if err != nil || !found {
		return false, err
	}
	if !postgresBootstrapJobMatches(job, cluster.Metadata.Name, cluster.Metadata.UID, instance, "initdb") {
		return false, nil
	}
	identity := "orphan:" + managed.Metadata.UID + ":" + cluster.Metadata.UID
	held := postgresBootstrapJobHeld(job, identity)
	if !deployment.Suspended {
		if !held || annotations[runtime.CloudNativePGHibernationAnno] != runtime.CloudNativePGHibernationOff {
			return false, nil
		}
		if err := s.recheckOrphanBootstrapIntent(ctx, c, ns, managed, app, deployment, cluster); err != nil {
			return false, err
		}
		return false, setPostgresBootstrapJobHold(ctx, c, ns, job, identity, false)
	}
	if postgresJobHasExecutionHistory(job) {
		return false, nil
	}
	// Enumerate by owner as well as by label: an extra WAL/data claim, an
	// unlabeled bound claim, or a label collision is not proof of an empty DB.
	claims, err := pgretire.RequiredList(ctx, c.getRawObject, pgretire.Path(ns, "persistentvolumeclaims", ""))
	if err != nil {
		return false, err
	}
	matching := 0
	var claim map[string]any
	for _, pvc := range claims {
		if !pgretire.Owned(pvc, cluster.Metadata.UID) && pgretire.Object(pgretire.Meta(pvc)["labels"])["cnpg.io/cluster"] != cluster.Metadata.Name {
			continue
		}
		matching++
		claim = pvc
	}
	if matching != 1 || !postgresUnboundInitializingClaim(claim, cluster.Metadata.Name, cluster.Metadata.UID, instance) {
		return false, nil
	}
	if delayed, err := postgresBootstrapClaimDelayedBinding(ctx, c, claim); err != nil || !delayed {
		return false, err
	}
	pods, err := postgresBootstrapJobPodCount(ctx, c, ns, job, instance)
	if err != nil {
		// A scheduled or unrelated claim user is not ours to stop. Leave
		// the regular CNPG suspension path and status in control.
		return false, err
	}
	jobs, err := pgretire.RequiredList(ctx, c.getRawObject, postgresJobPath(ns, ""))
	if err != nil {
		return false, err
	}
	for _, other := range jobs {
		if pgretire.Meta(other)["uid"] != pgretire.Meta(job)["uid"] &&
			(pgretire.Owned(other, cluster.Metadata.UID) || postgresPodSpecUsesClaim(postgresJobPodSpec(other), instance)) {
			return false, nil
		}
	}
	if err := s.recheckOrphanBootstrapIntent(ctx, c, ns, managed, app, deployment, cluster); err != nil {
		return false, err
	}
	if held {
		return postgresBootstrapJobStopped(job, pods), nil
	}
	if pgretire.Object(job["spec"])["suspend"] == true {
		return false, nil // do not take ownership of somebody else's hold
	}
	return false, setPostgresBootstrapJobHold(ctx, c, ns, job, identity, true)
}

func (s *Service) recheckOrphanBootstrapIntent(ctx context.Context, c *kubeClient, ns string, managed runtime.ManagedAppObject, app model.App, deployment runtime.ManagedBackingServiceDeployment, cluster kubeCloudNativePGCluster) error {
	fresh, found, err := c.getManagedApp(ctx, ns, managed.Metadata.Name)
	if err != nil || !found || fresh.Metadata.UID != managed.Metadata.UID || fresh.Metadata.ResourceVersion != managed.Metadata.ResourceVersion {
		return fmt.Errorf("orphan lifecycle intent changed before bootstrap Job reconciliation: %v", err)
	}
	if err := s.verifyOrphanPostgresHasNoStoreConsumers(app, []runtime.ManagedBackingServiceDeployment{deployment}); err != nil {
		return err
	}
	observed, found, err := c.getCloudNativePGCluster(ctx, ns, cluster.Metadata.Name)
	if err != nil || !found || observed.Metadata.UID != cluster.Metadata.UID || observed.Metadata.ResourceVersion != cluster.Metadata.ResourceVersion {
		return fmt.Errorf("orphan Cluster changed before bootstrap Job reconciliation: %v", err)
	}
	return nil
}
