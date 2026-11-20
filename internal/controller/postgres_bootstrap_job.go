package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"fugue/internal/pgretire"
)

const postgresBootstrapJobHold = "fugue.pro/postgres-bootstrap-job-hold"

func postgresJobPath(ns, name string) string {
	return "/apis/batch/v1/namespaces/" + url.PathEscape(ns) + "/jobs" + namedPathSuffix(name)
}

func namedPathSuffix(name string) string {
	if name == "" {
		return ""
	}
	return "/" + url.PathEscape(name)
}

func postgresControllerOwns(obj map[string]any, apiVersion, kind, name, uid string) bool {
	for _, ref := range pgretire.Objects(pgretire.Meta(obj)["ownerReferences"]) {
		if uid != "" && ref["controller"] == true && ref["apiVersion"] == apiVersion &&
			ref["kind"] == kind && ref["name"] == name && ref["uid"] == uid {
			return true
		}
	}
	return false
}

func postgresBootstrapJobMatches(job map[string]any, cluster, clusterUID, instance, role string) bool {
	meta := pgretire.Meta(job)
	labels := pgretire.Object(meta["labels"])
	if pgretire.String(meta, "name") != instance+"-"+role || pgretire.String(meta, "uid") == "" ||
		pgretire.String(meta, "resourceVersion") == "" || pgretire.String(meta, "deletionTimestamp") != "" ||
		!postgresControllerOwns(job, "postgresql.cnpg.io/v1", "Cluster", cluster, clusterUID) ||
		labels["cnpg.io/cluster"] != cluster || labels["cnpg.io/instanceName"] != instance || labels["cnpg.io/jobRole"] != role {
		return false
	}
	spec := postgresJobPodSpec(job)
	volumes := pgretire.Objects(spec["volumes"])
	claims := 0
	for _, v := range volumes {
		if ref := pgretire.Object(v["persistentVolumeClaim"]); ref != nil {
			if ref["claimName"] != instance || v["name"] != "pgdata" {
				return false
			}
			claims++
		}
	}
	containers := pgretire.Objects(spec["containers"])
	return claims == 1 && len(containers) == 1 && containers[0]["name"] == role
}

func postgresJobPodSpec(job map[string]any) map[string]any {
	return pgretire.Object(pgretire.Object(pgretire.Object(job["spec"])["template"])["spec"])
}

func postgresPodSpecUsesClaim(spec map[string]any, claim string) bool {
	for _, vol := range pgretire.Objects(spec["volumes"]) {
		if pgretire.Object(vol["persistentVolumeClaim"])["claimName"] == claim {
			return true
		}
	}
	return false
}

func postgresJobHasExecutionHistory(job map[string]any) bool {
	status := pgretire.Object(job["status"])
	for _, field := range []string{"succeeded", "failed", "completionTime", "completedIndexes", "failedIndexes"} {
		if value := status[field]; value != nil && value != "" && value != float64(0) {
			return true
		}
	}
	for _, cond := range pgretire.Objects(status["conditions"]) {
		if cond["status"] == "True" && cond["type"] != "Suspended" {
			return true
		}
	}
	unaccounted := pgretire.Object(status["uncountedTerminatedPods"])
	for _, field := range []string{"succeeded", "failed"} {
		if ids, _ := unaccounted[field].([]any); len(ids) > 0 {
			return true
		}
	}
	return false
}

func postgresPodNeverScheduled(pod map[string]any) bool {
	status := pgretire.Object(pod["status"])
	if pgretire.String(pgretire.Object(pod["spec"]), "nodeName") != "" || status["phase"] != "Pending" ||
		pgretire.String(status, "startTime") != "" || pgretire.String(status, "podIP") != "" {
		return false
	}
	for _, field := range []string{"containerStatuses", "initContainerStatuses", "ephemeralContainerStatuses"} {
		if len(pgretire.Objects(status[field])) != 0 {
			return false
		}
	}
	for _, cond := range pgretire.Objects(status["conditions"]) {
		if cond["type"] == "PodScheduled" && cond["status"] == "True" {
			return false
		}
	}
	return true
}

// Check the entire namespace for claim users, not only CNPG-labelled objects.
// Name/label matches discover objects; controller owner UIDs authorize them.
func postgresBootstrapJobPodCount(ctx context.Context, c *kubeClient, ns string, job map[string]any, claim string) (int, error) {
	pods, err := pgretire.RequiredList(ctx, c.getRawObject, pgretire.Path(ns, "pods", ""))
	if err != nil {
		return 0, err
	}
	count := 0
	for _, pod := range pods {
		owned := postgresControllerOwns(pod, "batch/v1", "Job", pgretire.String(pgretire.Meta(job), "name"), pgretire.String(pgretire.Meta(job), "uid"))
		if !owned && !postgresPodSpecUsesClaim(pgretire.Object(pod["spec"]), claim) {
			continue
		}
		if !owned || !postgresPodNeverScheduled(pod) {
			return 0, fmt.Errorf("claim %s has a scheduled, previously started or unrelated Pod", claim)
		}
		count++
	}
	return count, nil
}

func postgresUnboundInitializingClaim(pvc map[string]any, cluster, uid, instance string) bool {
	meta, spec := pgretire.Meta(pvc), pgretire.Object(pvc["spec"])
	labels, annotations := pgretire.Object(meta["labels"]), pgretire.Object(meta["annotations"])
	return pgretire.String(meta, "name") == instance && pgretire.String(meta, "uid") != "" &&
		pgretire.String(meta, "resourceVersion") != "" && pgretire.String(meta, "deletionTimestamp") == "" &&
		postgresControllerOwns(pvc, "postgresql.cnpg.io/v1", "Cluster", cluster, uid) &&
		labels["cnpg.io/cluster"] == cluster && labels["cnpg.io/instanceName"] == instance && labels["cnpg.io/pvcRole"] == "PG_DATA" &&
		annotations["cnpg.io/pvcStatus"] == "initializing" && pgretire.String(annotations, "volume.kubernetes.io/selected-node") == "" &&
		pgretire.String(annotations, "pv.kubernetes.io/bind-completed") == "" &&
		pgretire.String(spec, "volumeName") == "" && spec["dataSource"] == nil && spec["dataSourceRef"] == nil &&
		pgretire.Object(pvc["status"])["phase"] == "Pending"
}

func postgresBootstrapClaimDelayedBinding(ctx context.Context, c *kubeClient, pvc map[string]any) (bool, error) {
	name := pgretire.String(pgretire.Object(pvc["spec"]), "storageClassName")
	if name == "" {
		return false, nil
	}
	sc, found, err := c.getRawObject(ctx, "/apis/storage.k8s.io/v1/storageclasses/"+url.PathEscape(name))
	return found && sc["volumeBindingMode"] == "WaitForFirstConsumer", err
}

// Batch suspension keeps the exact Job as a barrier against CNPG generating
// another job. A subsequent observation, never the patch response, proves
// that the Job controller has actually removed the never-scheduled child.
func setPostgresBootstrapJobHold(ctx context.Context, c *kubeClient, ns string, job map[string]any, identity string, suspend bool) error {
	meta := pgretire.Meta(job)
	annotations := cloneKubeMap(pgretire.Object(meta["annotations"]))
	if annotations == nil {
		annotations = map[string]any{}
	}
	held := pgretire.String(annotations, postgresBootstrapJobHold)
	if suspend {
		if held != "" || pgretire.Object(job["spec"])["suspend"] == true {
			return fmt.Errorf("bootstrap Job already has a different suspension intent")
		}
		annotations[postgresBootstrapJobHold] = identity
	} else {
		if held != identity || pgretire.Object(job["spec"])["suspend"] != true {
			return fmt.Errorf("bootstrap Job does not have the exact Fugue hold")
		}
		delete(annotations, postgresBootstrapJobHold)
	}
	return retirementPatch(ctx, c, postgresJobPath(ns, pgretire.String(meta, "name")), job,
		map[string]any{"op": "add", "path": "/spec/suspend", "value": suspend},
		map[string]any{"op": "add", "path": "/metadata/annotations", "value": annotations})
}

func postgresBootstrapJobHeld(job map[string]any, identity string) bool {
	return pgretire.Object(job["spec"])["suspend"] == true &&
		pgretire.Object(pgretire.Meta(job)["annotations"])[postgresBootstrapJobHold] == identity
}

func postgresBootstrapJobStopped(job map[string]any, podCount int) bool {
	status := pgretire.Object(job["status"])
	for _, key := range []string{"active", "terminating"} {
		if status[key] != nil && status[key] != float64(0) {
			return false
		}
	}
	for _, cond := range pgretire.Objects(status["conditions"]) {
		if cond["status"] == "True" && cond["type"] == "Suspended" {
			return podCount == 0 && !postgresJobHasExecutionHistory(job)
		}
	}
	return false
}

func deleteStoppedPostgresBootstrapJob(ctx context.Context, c *kubeClient, ns string, job map[string]any) error {
	meta := pgretire.Meta(job)
	body := map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground",
		"preconditions": map[string]any{"uid": meta["uid"], "resourceVersion": meta["resourceVersion"]}}
	status, err := c.doJSON(ctx, http.MethodDelete, postgresJobPath(ns, pgretire.String(meta, "name")), body, nil)
	if status == http.StatusNotFound {
		return nil
	}
	return err
}
