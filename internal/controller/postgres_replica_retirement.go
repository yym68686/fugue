package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgretire"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const replicaRetirementIntent = "postgres_replica_retirement_intent"
const replicaRetirementAffinityAnnotation = "fugue.pro/replica-retirement-affinity"

var errPostgresReplicaRetirementUnsettled = errors.New("replica retirement awaits verified settlement")

func guardLocalizeStandbyRetirement(ctx context.Context, client *kubeClient, ns, clusterName string, op model.Operation) error {
	if op.Type != model.OperationTypeDatabaseLocalize {
		return nil
	}
	cluster, found, err := client.getCloudNativePGCluster(ctx, ns, clusterName)
	if err != nil || !found {
		return fmt.Errorf("verify topology before localizing PostgreSQL: cluster not available: %v", err)
	}
	// A previously staged relocation has already explicitly changed the
	// desired placement. Allow it to finish under its own convergence checks.
	if cluster.Spec.Instances > 1 && cluster.Metadata.Annotations[runtime.CloudNativePGReconcilePodSpecAnno] != runtime.CloudNativePGReconcilePodSpecHold {
		return fmt.Errorf("localize would remove a standby from the existing primary placement; use fugue service postgres retire-replica with the exact standby node")
	}
	return nil
}

type replicaRetirementPod struct {
	Name, UID, Node, IP, SpecHash, ContainerStartedAt string
	RestartCount                                      int
	Volume                                            model.PostgresSourceRetirementVolume
}

type replicaRetirementWitness struct {
	ClusterUID, ClusterSpecHash, PlacementKey, ConfigHash string
	Primary, Standby                                      replicaRetirementPod
	PostmasterStartedAt, StandbyReclaimPolicy             string
	OriginalSync                                          replicaRetirementSync
}

type replicaRetirementSync struct{ Min, Max int }

func replicaRetirementCluster(cluster map[string]any, app model.App, service model.BackingService) error {
	meta, annotations := pgretire.Meta(cluster), pgretire.Object(pgretire.Meta(cluster)["annotations"])
	labels := objectLabels(cluster)
	if labels[runtime.FugueLabelBackingServiceID] != service.ID ||
		labels[runtime.FugueLabelTenantID] != app.TenantID ||
		labels[runtime.FugueLabelProjectID] != app.ProjectID ||
		labels[runtime.FugueLabelOwnerAppID] != service.OwnerAppID ||
		pgretire.String(meta, "uid") == "" {
		return fmt.Errorf("replica retirement requires the existing service-owned Cluster")
	}
	if (pgretire.String(annotations, "cnpg.io/fencedInstances") != "" && annotations["cnpg.io/fencedInstances"] != "[]") ||
		annotations[coldReconciliationAnnotation] == coldReconciliationDisabled ||
		annotations[runtime.CloudNativePGReconcilePodSpecAnno] != nil {
		return fmt.Errorf("replica retirement refuses a fenced, migrating or placement-held Cluster")
	}
	if h := pgretire.String(annotations, runtime.CloudNativePGHibernationAnno); h != "" && h != runtime.CloudNativePGHibernationOff {
		return fmt.Errorf("replica retirement refuses a hibernating Cluster")
	}
	spec := pgretire.Object(cluster["spec"])
	// A synchronous write can block if the only acknowledged standby is
	// removed before PostgreSQL reloads its replication settings.
	params := pgretire.Object(pgretire.Object(spec["postgresql"])["parameters"])
	if pgretire.Object(pgretire.Object(spec["postgresql"])["synchronous"]) != nil ||
		pgretire.String(params, "synchronous_standby_names") != "" {
		return fmt.Errorf("online retirement cannot safely disable custom synchronous replication")
	}
	if spec["walStorage"] != nil || len(pgretire.Objects(spec["tablespaces"])) > 0 {
		return fmt.Errorf("replica retirement only supports a single PGDATA volume per instance")
	}
	return nil
}

func replicaRetirementSyncLimits(spec map[string]any) (replicaRetirementSync, error) {
	var limits replicaRetirementSync
	for field, limit := range map[string]*int{"minSyncReplicas": &limits.Min, "maxSyncReplicas": &limits.Max} {
		switch spec[field] {
		case nil, float64(0):
		case float64(1):
			*limit = 1
		default:
			return limits, fmt.Errorf("unsupported %s for two-instance PostgreSQL retirement", field)
		}
	}
	if limits.Min > limits.Max {
		return limits, fmt.Errorf("inconsistent live synchronous replica limits")
	}
	return limits, nil
}

func replicaRetirementClusterSpecHash(obj map[string]any) string {
	spec := cloneKubeMap(pgretire.Object(obj["spec"]))
	delete(spec, "instances")
	delete(spec, "minSyncReplicas")
	delete(spec, "maxSyncReplicas")
	return pgretire.Hash(spec)
}

func replicaRetirementPatch(ctx context.Context, client *kubeClient, path string, cluster map[string]any, key string, disableSync bool) error {
	annotations := cloneKubeMap(pgretire.Object(pgretire.Meta(cluster)["annotations"]))
	if annotations == nil {
		annotations = map[string]any{}
	}
	annotations[replicaRetirementAffinityAnnotation] = key
	changes := []map[string]any{
		{"op": "test", "path": "/spec/instances", "value": 2},
		{"op": "add", "path": "/metadata/annotations", "value": annotations},
	}
	if disableSync {
		changes = append(changes,
			map[string]any{"op": "add", "path": "/spec/minSyncReplicas", "value": 0},
			map[string]any{"op": "add", "path": "/spec/maxSyncReplicas", "value": 0})
	} else {
		changes = append(changes, map[string]any{"op": "replace", "path": "/spec/instances", "value": 1})
	}
	return retirementPatch(ctx, client, path, cluster, changes...)
}

func replicaRetirementAffinityKey(affinity any) string { return pgretire.Hash(affinity) }

// Even after operation completion, a normal apply must not "repair" the
// intentional difference between the active Cluster's placement envelope and
// the rendered single-instance affinity. The marker binds both exact specs
// to this Cluster UID. It cannot suppress an actual placement/topology change.
func preserveRetiredPostgresReplicaAffinity(current, desired map[string]any) {
	if !cloudNativePGObject(desired) {
		return
	}
	meta := pgretire.Meta(current)
	uid := pgretire.String(meta, "uid")
	before, after := pgretire.Object(current["spec"]), pgretire.Object(desired["spec"])
	if uid == "" || before["instances"] != float64(1) || fmt.Sprint(after["instances"]) != "1" {
		return
	}
	key := uid + ":" + replicaRetirementAffinityKey(after["affinity"]) + ":" + replicaRetirementAffinityKey(before["affinity"])
	operationID, matches := strings.CutPrefix(pgretire.String(pgretire.Object(meta["annotations"]), replicaRetirementAffinityAnnotation), key+":")
	if !matches || operationID == "" {
		return
	}
	if affinity, exists := before["affinity"]; exists {
		after["affinity"] = affinity
	} else {
		delete(after, "affinity")
	}
}

func readReplicaRetirementPod(ctx context.Context, get pgretire.Get, ns, clusterUID, name, role string) (replicaRetirementPod, string, error) {
	var out replicaRetirementPod
	pod, err := pgretire.Required(ctx, get, pgretire.Path(ns, "pods", name))
	if err != nil {
		return out, "", err
	}
	meta, spec, status := pgretire.Meta(pod), pgretire.Object(pod["spec"]), pgretire.Object(pod["status"])
	if !pgretire.Owned(pod, clusterUID) || pgretire.String(pgretire.Object(meta["labels"]), "cnpg.io/instanceRole") != role ||
		status["phase"] != "Running" || !pgretire.Ready(pod) {
		return out, "", fmt.Errorf("Pod %s is not a Ready Cluster-owned %s", name, role)
	}
	out.Name, out.UID = name, pgretire.String(meta, "uid")
	out.Node, out.IP = pgretire.String(spec, "nodeName"), pgretire.String(status, "podIP")
	out.SpecHash = pgretire.Hash(spec)
	for _, container := range pgretire.Objects(status["containerStatuses"]) {
		if container["name"] == managedPostgresMainContainerName && container["ready"] == true {
			out.ContainerStartedAt = pgretire.String(pgretire.Object(pgretire.Object(container["state"])["running"]), "startedAt")
			restarts, ok := container["restartCount"].(float64)
			if !ok {
				return out, "", fmt.Errorf("PostgreSQL restart count missing for %s", name)
			}
			out.RestartCount = int(restarts)
		}
	}
	if out.Node == "" || out.IP == "" || out.ContainerStartedAt == "" {
		return out, "", fmt.Errorf("PostgreSQL process or node identity missing for %s", name)
	}
	var claim string
	for _, volume := range pgretire.Objects(spec["volumes"]) {
		if source := pgretire.Object(volume["persistentVolumeClaim"]); source != nil {
			if claim != "" || pgretire.String(source, "claimName") != name || volume["name"] != "pgdata" {
				return out, "", fmt.Errorf("unrecognized PostgreSQL volume group for %s", name)
			}
			claim = name
		}
	}
	if claim == "" {
		return out, "", fmt.Errorf("PGDATA claim missing for %s", name)
	}
	pvc, err := pgretire.Required(ctx, get, pgretire.Path(ns, "persistentvolumeclaims", claim))
	if err != nil {
		return out, "", err
	}
	if !pgretire.Owned(pvc, clusterUID) {
		return out, "", fmt.Errorf("PGDATA claim %s is not owned by the Cluster", claim)
	}
	out.Volume, err = pgretire.Volume(ctx, get, ns, pvc)
	if err != nil {
		return out, "", err
	}
	pv, err := pgretire.Required(ctx, get, "/api/v1/persistentvolumes/"+url.PathEscape(out.Volume.PV))
	if err != nil {
		return out, "", err
	}
	pvSpec := pgretire.Object(pv["spec"])
	if pgretire.String(pgretire.Meta(pv), "uid") != out.Volume.PVUID || pgretire.PVSpecHash(pv) != out.Volume.PVSpecHash ||
		!replicaRetirementVolumePinnedToNode(pvSpec, out.Node) {
		return out, "", fmt.Errorf("PGDATA volume %s is not pinned to the observed node %s", out.Volume.PV, out.Node)
	}
	policy := pgretire.String(pvSpec, "persistentVolumeReclaimPolicy")
	if policy != "Delete" && policy != "Retain" {
		return out, "", fmt.Errorf("unrecognized reclaim policy for %s", out.Volume.PV)
	}
	return out, policy, nil
}

func replicaRetirementVolumePinnedToNode(spec map[string]any, node string) bool {
	terms := pgretire.Objects(pgretire.Object(pgretire.Object(spec["nodeAffinity"])["required"])["nodeSelectorTerms"])
	for _, term := range terms {
		pinned := false
		for _, expression := range pgretire.Objects(term["matchExpressions"]) {
			values, _ := expression["values"].([]any)
			if (expression["key"] == kubeHostnameLabelKey || expression["key"] == "openebs.io/nodename") &&
				expression["operator"] == "In" && len(values) == 1 && values[0] == node {
				pinned = true
			}
		}
		if !pinned {
			return false // Kubernetes node selector terms are ORed.
		}
	}
	return len(terms) > 0
}

func (s *Service) executePostgresReplicaRetirement(ctx context.Context, op model.Operation, app model.App) (runErr error) {
	evidence, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{replicaRetirementIntent}, Limit: 1})
	if err != nil {
		return fmt.Errorf("%w: read durable intent: %v", errPostgresReplicaRetirementUnsettled, err)
	}
	intent := len(evidence) == 1
	defer func() {
		if runErr != nil && intent && !errors.Is(runErr, errOperationNoLongerActive) {
			_, _ = s.Store.UpdateOperationProgress(op.ID, "standby retirement awaiting verification: "+runErr.Error())
			runErr = fmt.Errorf("%w: %v", errPostgresReplicaRetirementUnsettled, runErr)
		}
	}()
	if !s.Config.KubectlApply || op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return fmt.Errorf("replica retirement requires managed Kubernetes execution and an exact service intent")
	}
	request := op.DesiredSpec.Postgres
	target, err := store.ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil || target == nil || target.Service == nil {
		return fmt.Errorf("exact managed PostgreSQL target unavailable: %v", err)
	}
	pg := target.Postgres
	if request.RetireReplicaNode == "" || pg.RuntimeID != op.SourceRuntimeID || pg.RuntimeID != op.TargetRuntimeID ||
		pg.Suspended || pg.Instances != 2 || pg.PrimaryPlacementPendingRebalance {
		return fmt.Errorf("replica retirement requires the original active two-instance PostgreSQL configuration")
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	call, cancel := context.WithTimeout(ctx, 75*time.Second)
	defer cancel()
	ctx = call
	ns, name := runtime.NamespaceForTenant(app.TenantID), pg.ServiceName
	path := cloudNativePGClusterAPIPath(ns, name)
	configHash := pgretire.ConfigHash(app, *target.Service)
	var witness replicaRetirementWitness
	if intent {
		raw, err := json.Marshal(evidence[0].Payload)
		if err != nil || json.Unmarshal(raw, &witness) != nil ||
			pgretire.Hash(witness) != pgretire.Hash(evidence[0].Payload) ||
			witness.ClusterUID == "" || witness.ConfigHash != configHash ||
			witness.Standby.Node != request.RetireReplicaNode ||
			(request.RetireReplicaPodUID != "" && witness.Standby.UID != request.RetireReplicaPodUID) {
			return fmt.Errorf("durable standby or service identity no longer matches")
		}
	}
	primarySQL := func(cluster map[string]any, pod replicaRetirementPod) (string, string, error) {
		if pgretire.String(pgretire.Object(pgretire.Meta(cluster)["annotations"]), coldMigrationAnnotation) != "" {
			var typed kubeCloudNativePGCluster
			clusterJSON, _ := json.Marshal(cluster)
			if err := json.Unmarshal(clusterJSON, &typed); err != nil {
				return "", "", err
			}
			raw, err := pgretire.Required(ctx, client.getRawObject, pgretire.Path(ns, "pods", pod.Name))
			if err != nil || pgretire.String(pgretire.Meta(raw), "uid") != pod.UID {
				return "", "", fmt.Errorf("cannot verify original cold target primary: %v", err)
			}
			primaryJSON, _ := json.Marshal(raw)
			if err := client.verifyImageMigrationColdTarget(ctx, ns, op.ServiceID, pg, typed, primaryJSON); err != nil {
				return "", "", err
			}
		}
		serviceName := model.PostgresEndpointName(pg)
		ok, reason, err := managedPostgresRWEndpointContainsPod(ctx, client, ns, serviceName, pod.IP)
		if err != nil {
			return "", "", err
		}
		if !ok {
			return "", "", errors.New(reason)
		}
		return s.postgresContinuitySQLWitness(ctx, managedPostgresRWServiceHost(ns, serviceName), pod.IP, pg, true)
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if err := s.ensureOperationStillActiveContext(ctx, op.ID); err != nil {
			return err
		}
		cluster, err := pgretire.Required(ctx, client.getRawObject, path)
		if err != nil {
			return err
		}
		if err := replicaRetirementCluster(cluster, app, *target.Service); err != nil {
			return err
		}
		meta, spec, status := pgretire.Meta(cluster), pgretire.Object(cluster["spec"]), pgretire.Object(cluster["status"])
		syncLimits, err := replicaRetirementSyncLimits(spec)
		if err != nil {
			return err
		}
		uid := pgretire.String(meta, "uid")
		primaryName := pgretire.String(status, "currentPrimary")
		if primaryName == "" || (pgretire.String(status, "targetPrimary") != "" && status["targetPrimary"] != primaryName) {
			return fmt.Errorf("database primary is unknown or changing")
		}
		primary, _, err := readReplicaRetirementPod(ctx, client.getRawObject, ns, uid, primaryName, "primary")
		if err != nil {
			return err
		}
		if intent && (uid != witness.ClusterUID || primary != witness.Primary || replicaRetirementClusterSpecHash(cluster) != witness.ClusterSpecHash) {
			return fmt.Errorf("primary Pod, process, placement, volume or Cluster spec changed; retirement cannot be reported successful")
		}
		final := pg
		final.Instances, final.SynchronousReplicas, final.FailoverTargetRuntimeID = 1, 0, ""
		final.PrimaryNodeName = primary.Node
		switch spec["instances"] {
		case float64(2):
			standby, policy, err := replicaRetirementPreflight(ctx, client.getRawObject, ns, name, cluster, primary, *request)
			if err != nil {
				return err
			}
			matches, err := s.managedPostgresNodeMatchesRuntime(ctx, client, primary.Node, pg.RuntimeID)
			if err != nil || !matches || (pg.PrimaryNodeName != "" && pg.PrimaryNodeName != primary.Node) {
				return fmt.Errorf("original primary does not match desired placement: %v", err)
			}
			started, syncStandbys, err := primarySQL(cluster, primary)
			if err != nil {
				return err
			}
			placements, err := s.managedPostgresServicePlacements(ctx, app, name, app.Spec.RuntimeID, final)
			if err != nil || len(placements) == 0 {
				return fmt.Errorf("could not prove single-instance rendering of current placement: %v", err)
			}
			var desiredAffinity any
			if rendered := runtime.PostgresAffinity(final, placements); len(rendered) != 0 {
				desiredAffinity = rendered
			}
			key := uid + ":" + replicaRetirementAffinityKey(desiredAffinity) + ":" + replicaRetirementAffinityKey(spec["affinity"]) + ":" + op.ID
			observed := replicaRetirementWitness{
				ClusterUID: uid, ClusterSpecHash: replicaRetirementClusterSpecHash(cluster), PlacementKey: key, ConfigHash: configHash,
				Primary: primary, Standby: standby, PostmasterStartedAt: started, StandbyReclaimPolicy: policy, OriginalSync: syncLimits,
			}
			disableSync := syncLimits != (replicaRetirementSync{})
			syncPatchObserved := intent && witness.OriginalSync != (replicaRetirementSync{}) &&
				pgretire.String(pgretire.Object(meta["annotations"]), replicaRetirementAffinityAnnotation) == witness.PlacementKey
			if syncPatchObserved && disableSync {
				return fmt.Errorf("synchronous standby limits were restored during retirement")
			}
			if syncPatchObserved {
				observed.OriginalSync = witness.OriginalSync
			} else if !disableSync && syncStandbys != "" {
				return fmt.Errorf("primary reports synchronous standbys despite zero configured limits")
			}
			if intent && observed != witness {
				return fmt.Errorf("primary or standby identity differs from the confirmed retirement")
			}
			if !intent {
				if err := s.recordReplicaRetirementIntent(op, ns, name, observed); err != nil {
					return err
				}
				witness, intent = observed, true
			}
			if !disableSync && syncStandbys != "" {
				s.updateManagedPostgresTransitionProgress(op.ID, "waiting for primary to acknowledge asynchronous operation before standby removal")
			} else {
				if err := s.ensureOperationStillActiveContext(ctx, op.ID); err != nil {
					return err
				}
				if err := replicaRetirementPatch(ctx, client, path, cluster, key, disableSync); err != nil {
					return fmt.Errorf("submit guarded CNPG standby retirement: %w", err)
				}
				if disableSync {
					s.updateManagedPostgresTransitionProgress(op.ID, "synchronous standby disabled; verifying primary SQL before removing standby")
				} else {
					s.updateManagedPostgresTransitionProgress(op.ID, fmt.Sprintf("standby %s on %s retiring; verifying unchanged primary %s and volume settlement", standby.Name, standby.Node, primary.Name))
				}
			}
		case float64(1):
			if !intent || syncLimits != (replicaRetirementSync{}) ||
				pgretire.String(pgretire.Object(meta["annotations"]), replicaRetirementAffinityAnnotation) != witness.PlacementKey {
				return fmt.Errorf("single-instance Cluster is not the result of this confirmed retirement")
			}
			settled, err := replicaRetirementSettled(ctx, client.getRawObject, ns, name, status, witness)
			if err != nil {
				return err
			}
			if settled {
				started, syncStandbys, err := primarySQL(cluster, primary)
				if err != nil {
					return err
				}
				if started != witness.PostmasterStartedAt {
					return fmt.Errorf("PostgreSQL postmaster restarted during retirement")
				}
				if syncStandbys != "" {
					return fmt.Errorf("original primary still reports synchronous standbys after scale-down")
				}
				latest, err := pgretire.Required(ctx, client.getRawObject, path)
				if err != nil {
					return err
				}
				// Re-read after the SQL round trip. A resourceVersion advance
				// requires another observation, not a successful stale result.
				if pgretire.String(pgretire.Meta(latest), "resourceVersion") == pgretire.String(meta, "resourceVersion") {
					latestPod, _, err := readReplicaRetirementPod(ctx, client.getRawObject, ns, uid, primary.Name, "primary")
					if err != nil || latestPod != primary {
						return fmt.Errorf("primary changed after SQL verification: %v", err)
					}
					message := fmt.Sprintf("retired standby %s on %s; primary %s on %s unchanged; read-write SQL verified (standby volume policy: %s)",
						witness.Standby.Name, witness.Standby.Node, primary.Name, primary.Node, witness.StandbyReclaimPolicy)
					placement := managedPostgresPrimaryPlacement{RuntimeID: pg.RuntimeID, NodeName: primary.Node, PrimaryPod: primary.Name, PodIP: primary.IP}
					_, err = s.Store.CompleteManagedPostgresReplicaRetirement(op, message, managedPostgresPlacementMutation(app, *target, pg, final, placement))
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected live PostgreSQL instance count %v", spec["instances"])
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("standby retirement settlement not observed: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func replicaRetirementPreflight(ctx context.Context, get pgretire.Get, ns, name string, cluster map[string]any, primary replicaRetirementPod, request model.AppPostgresSpec) (replicaRetirementPod, string, error) {
	var empty replicaRetirementPod
	status := pgretire.Object(cluster["status"])
	if status["instances"] != float64(2) || status["readyInstances"] != float64(2) || status["phase"] != "Cluster in healthy state" {
		return empty, "", fmt.Errorf("online retirement requires two healthy observed instances")
	}
	if dangling, _ := status["danglingPVC"].([]any); len(dangling) > 0 {
		return empty, "", fmt.Errorf("online retirement refuses dangling PostgreSQL claims")
	}
	uid := pgretire.String(pgretire.Meta(cluster), "uid")
	pods, err := pgretire.RequiredList(ctx, get, pgretire.Path(ns, "pods", "")+"?labelSelector="+url.QueryEscape("cnpg.io/cluster="+name))
	if err != nil || len(pods) != 2 {
		return empty, "", fmt.Errorf("expected exactly two database Pods: %v", err)
	}
	var standbyName string
	for _, pod := range pods {
		metadata := pgretire.Meta(pod)
		podName := pgretire.String(metadata, "name")
		if !pgretire.Owned(pod, uid) || podName == "" {
			return empty, "", fmt.Errorf("unowned or unidentifiable database Pod")
		}
		if podName != primary.Name {
			standbyName = podName
		}
	}
	names, _ := status["instanceNames"].([]any)
	if standbyName == "" || len(names) != 2 ||
		!((names[0] == primary.Name && names[1] == standbyName) || (names[1] == primary.Name && names[0] == standbyName)) {
		return empty, "", fmt.Errorf("operator instance inventory differs from live primary and standby")
	}
	standby, policy, err := readReplicaRetirementPod(ctx, get, ns, uid, standbyName, "replica")
	if err != nil {
		return empty, "", err
	}
	rawStandby, err := pgretire.Required(ctx, get, pgretire.Path(ns, "pods", standby.Name))
	if err != nil || pgretire.String(pgretire.Meta(rawStandby), "uid") != standby.UID {
		return empty, "", fmt.Errorf("standby identity changed during preflight: %v", err)
	}
	serial := pgretire.String(pgretire.Object(pgretire.Meta(rawStandby)["annotations"]), "cnpg.io/nodeSerial")
	if parsed, err := strconv.Atoi(serial); err != nil || parsed <= 0 || standby.Name != name+"-"+serial ||
		standby.Node != request.RetireReplicaNode || standby.Node == primary.Node || standby.UID == primary.UID ||
		standby.Volume.PVUID == primary.Volume.PVUID ||
		(request.RetireReplicaPodUID != "" && standby.UID != request.RetireReplicaPodUID) {
		return empty, "", fmt.Errorf("observed standby does not match the requested distinct node, Pod UID or data volume")
	}
	return standby, policy, nil
}

func replicaRetirementSettled(ctx context.Context, get pgretire.Get, ns, cluster string, status map[string]any, witness replicaRetirementWitness) (bool, error) {
	if status["instances"] != float64(1) || status["readyInstances"] != float64(1) || status["phase"] != "Cluster in healthy state" {
		return false, nil
	}
	names, _ := status["instanceNames"].([]any)
	if len(names) != 1 || names[0] != witness.Primary.Name {
		return false, nil
	}
	pods, err := pgretire.RequiredList(ctx, get, pgretire.Path(ns, "pods", "")+"?labelSelector="+url.QueryEscape("cnpg.io/cluster="+cluster))
	if err != nil {
		return false, err
	}
	if len(pods) != 1 || pgretire.String(pgretire.Meta(pods[0]), "uid") != witness.Primary.UID {
		return false, nil
	}
	for _, path := range []string{pgretire.Path(ns, "pods", witness.Standby.Name), pgretire.Path(ns, "persistentvolumeclaims", witness.Standby.Volume.PVC)} {
		if _, found, err := get(ctx, path); err != nil || found {
			return false, err
		}
	}
	pv, found, err := get(ctx, "/api/v1/persistentvolumes/"+url.PathEscape(witness.Standby.Volume.PV))
	if err != nil {
		return false, err
	}
	if witness.StandbyReclaimPolicy == "Delete" {
		return !found, nil
	}
	if !found || pgretire.String(pgretire.Meta(pv), "uid") != witness.Standby.Volume.PVUID ||
		pgretire.Object(pv["spec"])["persistentVolumeReclaimPolicy"] != "Retain" {
		return false, fmt.Errorf("expected retained standby PV identity is unavailable")
	}
	return pgretire.Object(pv["status"])["phase"] == "Released", nil
}

func (s *Service) recordReplicaRetirementIntent(op model.Operation, ns, cluster string, witness replicaRetirementWitness) error {
	data, err := json.Marshal(witness)
	if err != nil {
		return err
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	evidence, err := s.Store.RecordOperationEvidence(model.OperationEvidence{TenantID: op.TenantID, AppID: op.AppID, OperationID: op.ID,
		Type: replicaRetirementIntent, SubjectKind: "Cluster", SubjectName: cluster, SubjectNamespace: ns, SubjectUID: witness.ClusterUID,
		Summary: "Verified sole standby, stable primary and exact bound data volumes before scale-down",
		Source:  model.OperationEvidenceSourceKubernetesAPI, Severity: model.OperationEvidenceSeverityInfo, Confidence: model.OperationEvidenceConfidenceConfirmed,
		PayloadVersion: 1, Payload: payload})
	if err != nil {
		return err
	}
	if pgretire.Hash(evidence.Payload) != pgretire.Hash(witness) {
		return fmt.Errorf("durable replica retirement intent was altered or truncated")
	}
	return nil
}
