package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/runtime"
)

// Record the pre-hold hibernation intent on the exact Cluster, including for
// standalone services which have no ManagedApp to restore them after release.
const postgresTenantHold = "fugue.pro/tenant-hold-previous-hibernation"

func (s *Service) reconcilePostgresTenantHold(ctx context.Context, c *kubeClient, meta metav1.ObjectMeta, phase string, db model.BackingService) error {
	tenant, err := s.Store.GetTenant(db.TenantID)
	if err != nil {
		return err
	}
	held := tenant.Status == model.TenantStatusAccessHeld
	previous := meta.Annotations[postgresTenantHold]
	if !held && (tenant.Status != "active" || previous == "") {
		return nil
	}
	if meta.UID == "" || meta.ResourceVersion == "" {
		return errors.New("missing live postgres mutation identity")
	}
	on, off := runtime.CloudNativePGHibernationOn, runtime.CloudNativePGHibernationOff
	current := meta.Annotations[runtime.CloudNativePGHibernationAnno]
	changes := make(map[string]any)
	if held {
		if previous == "" {
			previous = current
			if previous == "" {
				previous = off
			}
			changes[postgresTenantHold] = previous
		}
		if current != on {
			changes[runtime.CloudNativePGHibernationAnno] = on
		}
	} else {
		// Never wake a retired generation or override an explicit suspension.
		pg := db.Spec.Postgres
		desired := on
		if previous == off && !pg.Suspended && meta.Name == model.NormalizePostgresServiceName(pg.ServiceName, db.Name+"-postgres") {
			desired = off
		}
		if current != desired {
			changes[runtime.CloudNativePGHibernationAnno] = desired
		}
		changes[postgresTenantHold] = nil
	}

	// CNPG can wait indefinitely for its first primary before hibernating.
	// Quiesce an unscheduled bootstrap Job and retain it for release. A Job
	// that has run must finish initializing its data before CNPG shuts it down.
	// Restore Jobs before dropping our release marker so a transient API
	// failure retries. On hold, persist hibernation before inspecting Jobs.
	if !held {
		if err := reconcileTenantBootstrapJobs(ctx, c, meta, false); err != nil {
			return err
		}
	}
	if len(changes) != 0 {
		patch := map[string]any{"metadata": map[string]any{"uid": meta.UID, "resourceVersion": meta.ResourceVersion, "annotations": changes}}
		if _, err := c.doRequest(ctx, http.MethodPatch, cloudNativePGClusterAPIPath(meta.Namespace, meta.Name), "application/merge-patch+json", patch, nil); err != nil {
			return err
		}
	}
	if held && phase == "Setting up primary" {
		return reconcileTenantBootstrapJobs(ctx, c, meta, true)
	}
	return nil
}

func reconcileTenantBootstrapJobs(ctx context.Context, c *kubeClient, cluster metav1.ObjectMeta, held bool) error {
	path := postgresJobPath(cluster.Namespace, "") + "?labelSelector=" + url.QueryEscape("cnpg.io/cluster="+cluster.Name)
	jobs, err := pgretire.RequiredList(ctx, c.getRawObject, path)
	if err != nil {
		return err
	}
	identity := "tenant-hold:" + string(cluster.UID)
	for _, job := range jobs {
		meta := pgretire.Meta(job)
		labels := pgretire.Object(meta["labels"])
		instance, role := pgretire.String(labels, "cnpg.io/instanceName"), pgretire.String(labels, "cnpg.io/jobRole")
		if instance == "" || (role != "initdb" && role != "full-recovery" && role != "join") || !postgresBootstrapJobMatches(job, cluster.Name, string(cluster.UID), instance, role) {
			continue
		}
		if held {
			if pgretire.Object(job["spec"])["suspend"] == true || postgresJobHasExecutionHistory(job) {
				continue
			}
			count, err := postgresBootstrapJobPodCount(ctx, c, cluster.Namespace, job, instance)
			if err != nil || count == 0 {
				// Do not interrupt a started or otherwise unproven bootstrap.
				if err != nil {
					return fmt.Errorf("check bootstrap pods: %w", err)
				}
				continue
			}
		} else if !postgresBootstrapJobHeld(job, identity) {
			continue
		}
		if err := setPostgresBootstrapJobHold(ctx, c, cluster.Namespace, job, identity, held); err != nil {
			return err
		}
	}
	return nil
}
