package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/runtime"
)

const postgresStageWitnessAnnotation = "fugue.pro/postgres-placement-stage"
const postgresRollbackRetainedClaim = "fugue.pro/postgres-rollback-retained"

type postgresStageRollbackContextKey struct{}

type postgresStageWitness struct {
	Operation, UID, Primary string
	Instances, Serial       int
	Rollback                bool
}

func postgresStageAnnotation(cluster map[string]any, w *postgresStageWitness) map[string]any {
	a := cloneKubeMap(pgretire.Object(pgretire.Meta(cluster)["annotations"]))
	if a == nil {
		a = map[string]any{}
	}
	if w == nil {
		delete(a, postgresStageWitnessAnnotation)
	} else {
		encoded, _ := json.Marshal(w)
		a[postgresStageWitnessAnnotation] = string(encoded)
	}
	return a
}

func postgresStageWitnessFrom(cluster map[string]any, operation string) (*postgresStageWitness, error) {
	raw := pgretire.String(pgretire.Object(pgretire.Meta(cluster)["annotations"]), postgresStageWitnessAnnotation)
	if raw == "" {
		return nil, nil
	}
	var w postgresStageWitness
	if json.Unmarshal([]byte(raw), &w) != nil || w.Operation != operation || w.UID == "" ||
		w.UID != pgretire.String(pgretire.Meta(cluster), "uid") || w.Primary == "" || w.Instances < 1 || w.Serial < 1 {
		return nil, fmt.Errorf("database placement is guarded by a different or invalid staging witness")
	}
	return &w, nil
}

func postgresStageSerial(cluster map[string]any) int {
	return postgresStageInt(pgretire.Object(cluster["status"])["latestGeneratedNode"])
}

func postgresStageInt(v any) int {
	n, _ := strconv.Atoi(fmt.Sprint(v))
	return n
}

func postgresStagePrimaryMatches(cluster map[string]any, w postgresStageWitness) bool {
	status := pgretire.Object(cluster["status"])
	return pgretire.String(pgretire.Meta(cluster), "uid") == w.UID &&
		status["currentPrimary"] == w.Primary && status["targetPrimary"] == w.Primary
}

func postgresStageAnnotatePatch(patch []map[string]any, w *postgresStageWitness) {
	a := pgretire.Object(patch[len(patch)-1]["value"]) // managedPostgresPlacementOperationPatch ends with annotations.
	if w == nil {
		delete(a, postgresStageWitnessAnnotation)
		return
	}
	encoded, _ := json.Marshal(w)
	a[postgresStageWitnessAnnotation] = string(encoded)
}

func (s *Service) applyGuardedPostgresPlacement(ctx context.Context, c *kubeClient, op model.Operation, ns, name string, current, desired map[string]any, patch []map[string]any) error {
	path := cloudNativePGClusterAPIPath(ns, name)
	w, err := postgresStageWitnessFrom(current, op.ID)
	if err != nil {
		return err
	}
	_, rollback := ctx.Value(postgresStageRollbackContextKey{}).(bool)
	hold := pgretire.Object(pgretire.Meta(current)["annotations"])[runtime.CloudNativePGReconcilePodSpecAnno] == runtime.CloudNativePGReconcilePodSpecHold
	staging := normalizeKubeStringMap(pgretire.Meta(desired)["annotations"])[runtime.CloudNativePGReconcilePodSpecAnno] == runtime.CloudNativePGReconcilePodSpecHold
	if rollback {
		if w == nil {
			return fmt.Errorf("cannot verify a database rollback without the original placement witness")
		}
		return s.restoreGuardedPostgresPlacement(ctx, c, op, ns, name, current, desired, *w)
	}
	if w != nil && w.Rollback {
		return fmt.Errorf("database stage is already rolling back; finish rollback before staging")
	}
	if staging {
		if w == nil {
			status := pgretire.Object(current["status"])
			w = &postgresStageWitness{Operation: op.ID, UID: pgretire.String(pgretire.Meta(current), "uid"),
				Primary: pgretire.String(status, "currentPrimary"), Serial: postgresStageSerial(current),
				Instances: postgresStageInt(pgretire.Object(current["spec"])["instances"])}
			if hold || w.UID == "" || w.Primary == "" || w.Serial < 1 || w.Instances < 1 || !postgresStagePrimaryMatches(current, *w) {
				return fmt.Errorf("database staging requires an exact stable Cluster and primary generation")
			}
		}
		postgresStageAnnotatePatch(patch, w)
	} else if w != nil {
		postgresStageAnnotatePatch(patch, nil)
	}
	_, err = c.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil)
	return err
}

func (s *Service) restoreGuardedPostgresPlacement(ctx context.Context, c *kubeClient, op model.Operation, ns, name string, current, desired map[string]any, w postgresStageWitness) error {
	path := cloudNativePGClusterAPIPath(ns, name)
	if !postgresStagePrimaryMatches(current, w) || postgresStageInt(pgretire.Object(desired["spec"])["instances"]) != w.Instances {
		return fmt.Errorf("rollback cannot prove the original primary and replica count")
	}
	serial := postgresStageSerial(current)
	if serial < w.Serial || serial > w.Serial+1 {
		return fmt.Errorf("rollback cannot prove a single surplus replica generation")
	}
	if serial == w.Serial {
		// CNPG must CAS the Cluster status to allocate a new serial before
		// it creates a join; this write cannot race that allocation silently.
		patch, err := managedPostgresPlacementOperationPatch(current, desired)
		if err != nil {
			return err
		}
		postgresStageAnnotatePatch(patch, nil)
		_, err = c.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil)
		return err
	}

	instance := name + "-" + strconv.Itoa(serial)
	jobPath := postgresJobPath(ns, instance+"-join")
	claimPath := pgretire.Path(ns, "persistentvolumeclaims", instance)
	identity := "rollback:" + op.ID + ":" + w.UID
	claim, found, err := c.getRawObject(ctx, claimPath)
	if err != nil || !found {
		return fmt.Errorf("staged claim %s not observed for rollback: %v", instance, err)
	}
	retained := func(claim map[string]any) bool {
		meta := pgretire.Meta(claim)
		return pgretire.Object(meta["annotations"])[postgresRollbackRetainedClaim] == identity &&
			!postgresControllerOwns(claim, runtime.CloudNativePGAPIVersion, runtime.CloudNativePGClusterKind, name, w.UID) &&
			pgretire.Object(meta["labels"])["cnpg.io/cluster"] == nil
	}
	if !retained(claim) {
		if !postgresUnboundInitializingClaim(claim, name, w.UID, instance) {
			return fmt.Errorf("staged claim %s was selected, bound, or has unknown ownership; retained for verified recovery", instance)
		}
		if delayed, err := postgresBootstrapClaimDelayedBinding(ctx, c, claim); !delayed || err != nil {
			return fmt.Errorf("staged claim %s lacks verified delayed binding: %v", instance, err)
		}
	}

	job, found, err := c.getRawObject(ctx, jobPath)
	if err != nil {
		return err
	}
	if found && pgretire.String(pgretire.Meta(job), "deletionTimestamp") != "" && retained(claim) && w.Rollback {
		job, found, err = waitForPostgresRollbackJobRemoval(ctx, c, jobPath, pgretire.String(pgretire.Meta(job), "uid"))
		if err != nil {
			return err
		}
	}
	if !found && (!retained(claim) || !w.Rollback || pgretire.Object(pgretire.Meta(current)["annotations"])[runtime.CloudNativePGReconcilePodSpecAnno] != nil) {
		return fmt.Errorf("the staged join Job %s is missing or not yet observed; rollback not verified", instance)
	}
	if found {
		if !postgresBootstrapJobMatches(job, name, w.UID, instance, "join") || postgresJobHasExecutionHistory(job) {
			return fmt.Errorf("staged join Job %s has execution history or changed identity; rollback not verified", instance)
		}
		if _, err := postgresBootstrapJobPodCount(ctx, c, ns, job, instance); err != nil {
			return err
		}
		if !w.Rollback {
			w.Rollback = true
			if err := retirementPatch(ctx, c, path, current, map[string]any{
				"op": "add", "path": "/metadata/annotations", "value": postgresStageAnnotation(current, &w),
			}); err != nil {
				return err
			}
		}
		if !postgresBootstrapJobHeld(job, identity) {
			if err := setPostgresBootstrapJobHold(ctx, c, ns, job, identity, true); err != nil {
				return err
			}
		}
		jobUID := pgretire.String(pgretire.Meta(job), "uid")
		job, err = waitForStoppedPostgresRollbackJob(ctx, c, ns, jobPath, name, instance, identity, jobUID, w)
		if err != nil {
			return err
		}
		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return err
		}
		claim, err = pgretire.Required(ctx, c.getRawObject, claimPath)
		if err != nil {
			return err
		}
		if !retained(claim) {
			if !postgresUnboundInitializingClaim(claim, name, w.UID, instance) {
				return fmt.Errorf("staged claim %s changed during Job suspension; refused to alter placement", instance)
			}
			if err := retainPostgresRollbackClaim(ctx, c, ns, claimPath, job, claim, instance, identity); err != nil {
				return err
			}
		}
	}

	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	current, err = pgretire.Required(ctx, c.getRawObject, path)
	if err != nil {
		return err
	}
	fresh, err := postgresStageWitnessFrom(current, op.ID)
	if err != nil || fresh == nil || *fresh != w || postgresStageSerial(current) != serial || !postgresStagePrimaryMatches(current, w) {
		return fmt.Errorf("Cluster or rollback witness changed while cancelling the staged join: %v", err)
	}
	patch, err := managedPostgresPlacementOperationPatch(current, desired)
	if err != nil {
		return err
	}
	postgresStageAnnotatePatch(patch, &w)
	if _, err := c.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil); err != nil {
		return err
	}
	if found {
		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return err
		}
		if err := deleteStoppedPostgresBootstrapJob(ctx, c, ns, job); err != nil {
			return err
		}
		if _, _, err := waitForPostgresRollbackJobRemoval(ctx, c, jobPath, pgretire.String(pgretire.Meta(job), "uid")); err != nil {
			return err
		}
	}
	current, err = pgretire.Required(ctx, c.getRawObject, path)
	if err != nil {
		return err
	}
	fresh, err = postgresStageWitnessFrom(current, op.ID)
	if err != nil || fresh == nil || *fresh != w || postgresStageSerial(current) != serial || !postgresStagePrimaryMatches(current, w) ||
		postgresStageInt(pgretire.Object(current["spec"])["instances"]) != w.Instances {
		return fmt.Errorf("restored Cluster identity or topology changed before rollback verification: %v", err)
	}
	return retirementPatch(ctx, c, path, current, map[string]any{
		"op": "add", "path": "/metadata/annotations", "value": postgresStageAnnotation(current, nil),
	})
}

func retainPostgresRollbackClaim(ctx context.Context, c *kubeClient, ns, path string, job, claim map[string]any, instance, identity string) error {
	jobs, err := pgretire.RequiredList(ctx, c.getRawObject, postgresJobPath(ns, ""))
	if err != nil {
		return err
	}
	for _, candidate := range jobs {
		if pgretire.Meta(candidate)["uid"] != pgretire.Meta(job)["uid"] && postgresPodSpecUsesClaim(postgresJobPodSpec(candidate), instance) {
			return fmt.Errorf("staged claim %s has another Job consumer", instance)
		}
	}
	meta := pgretire.Meta(claim)
	labels := cloneKubeMap(pgretire.Object(meta["labels"]))
	for _, label := range []string{"cnpg.io/cluster", "cnpg.io/instanceName", "cnpg.io/pvcRole", "cnpg.io/instanceRole"} {
		delete(labels, label)
	}
	annotations := cloneKubeMap(pgretire.Object(meta["annotations"]))
	annotations[postgresRollbackRetainedClaim] = identity
	owners := make([]map[string]any, 0)
	for _, ref := range pgretire.Objects(meta["ownerReferences"]) {
		if ref["controller"] != true || ref["kind"] != runtime.CloudNativePGClusterKind || ref["apiVersion"] != runtime.CloudNativePGAPIVersion {
			owners = append(owners, ref)
		}
	}
	return retirementPatch(ctx, c, path, claim,
		map[string]any{"op": "add", "path": "/metadata/labels", "value": labels},
		map[string]any{"op": "add", "path": "/metadata/annotations", "value": annotations},
		map[string]any{"op": "add", "path": "/metadata/ownerReferences", "value": owners})
}

func waitForStoppedPostgresRollbackJob(ctx context.Context, c *kubeClient, ns, path, cluster, instance, identity, jobUID string, w postgresStageWitness) (map[string]any, error) {
	for {
		job, err := pgretire.Required(ctx, c.getRawObject, path)
		if err != nil {
			return nil, err
		}
		if pgretire.Meta(job)["uid"] != jobUID || !postgresBootstrapJobMatches(job, cluster, w.UID, instance, "join") ||
			!postgresBootstrapJobHeld(job, identity) || postgresJobHasExecutionHistory(job) {
			return nil, fmt.Errorf("staged join Job changed or has execution history during suspension")
		}
		pods, err := postgresBootstrapJobPodCount(ctx, c, ns, job, instance)
		if err != nil {
			return nil, err
		}
		if postgresBootstrapJobStopped(job, pods) {
			return job, nil
		}
		if err := postgresRollbackPoll(ctx); err != nil {
			return nil, fmt.Errorf("staged join suspension did not settle: %w", err)
		}
	}
}

func waitForPostgresRollbackJobRemoval(ctx context.Context, c *kubeClient, path, uid string) (map[string]any, bool, error) {
	for {
		job, found, err := c.getRawObject(ctx, path)
		if err != nil || !found {
			return job, found, err
		}
		if pgretire.Meta(job)["uid"] != uid {
			return nil, false, fmt.Errorf("staged join Job name was reused during rollback")
		}
		if err := postgresRollbackPoll(ctx); err != nil {
			return nil, false, fmt.Errorf("staged join removal did not settle: %w", err)
		}
	}
}

func postgresRollbackPoll(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
		return nil
	}
}
