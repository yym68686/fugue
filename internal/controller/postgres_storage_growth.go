package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5"
	"k8s.io/apimachinery/pkg/api/resource"
)

const storageGrowthBaselineEvidence = "postgres_storage_growth_baseline"

func (s *Service) resumingPostgresStorageGrowth(op model.Operation, current, desired *model.AppPostgresSpec, source, target string) (bool, error) {
	if op.ID == "" || current == nil || desired == nil || source == "" || source != target || current.StorageClassName != desired.StorageClassName {
		return false, nil
	}
	before, e1 := resource.ParseQuantity(current.StorageSize)
	after, e2 := resource.ParseQuantity(desired.StorageSize)
	if e1 != nil || e2 != nil || after.Cmp(before) < 0 {
		return false, nil
	}
	items, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{storageGrowthBaselineEvidence}, Limit: 1})
	return len(items) > 0, err
}

// Only immutable identities and digests are persisted, never the raw Cluster
// or Pod spec (which can contain credentials). A retry keeps the first witness.
type postgresStorageWitness struct {
	ClusterUID          string `json:"cluster_uid"`
	ClusterSpecDigest   string `json:"cluster_spec_digest"`
	PodName             string `json:"pod_name"`
	PodUID              string `json:"pod_uid"`
	PodSpecDigest       string `json:"pod_spec_digest"`
	NodeName            string `json:"node_name"`
	RestartCount        int    `json:"restart_count"`
	ContainerStartedAt  string `json:"container_started_at"`
	PostmasterStartedAt string `json:"postmaster_started_at"`
}

type postgresStorageSnapshot struct {
	Witness         postgresStorageWitness
	ResourceVersion string
	StorageSize     string
}

// Storage-only operations must NEVER enter applyManagedDesiredAppState. That
// reconciles unrelated scheduling/configuration drift and can restart a primary.
func (s *Service) executeManagedPostgresStorageGrowth(ctx context.Context, op model.Operation, app model.App,
	target store.ManagedPostgresOperationTarget, client *kubeClient, namespace, clusterName, size string) error {
	storageTarget := managedPostgresStorageTarget{StorageSize: size, StorageClassName: target.Postgres.StorageClassName}
	var baseline postgresStorageWitness
	var snapshot postgresStorageSnapshot
	guard := func() error {
		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return err
		}
		var err error
		snapshot, err = s.observePostgresStorageSnapshot(ctx, client, namespace, clusterName, target.Postgres)
		if err != nil {
			return err
		}
		if pin := strings.TrimSpace(target.Postgres.PrimaryNodeName); pin != "" && pin != snapshot.Witness.NodeName {
			return fmt.Errorf("storage growth blocked: live primary is not on the existing node pin %s", pin)
		}
		matches, err := s.managedPostgresNodeMatchesRuntime(ctx, client, snapshot.Witness.NodeName, op.TargetRuntimeID)
		if err != nil {
			return err
		}
		if !matches {
			return fmt.Errorf("storage growth blocked: live primary does not match the existing runtime")
		}
		baseline, err = s.storageGrowthBaseline(op, app, namespace, snapshot.Witness)
		if err != nil {
			return err
		}
		return verifyPostgresStorageWitness(baseline, snapshot.Witness)
	}
	// Run existing class, capacity and LocalPV reserve gates before any mutation.
	// The guard records a durable healthy baseline before even the first PVC patch.
	if err := s.prepareManagedPostgresInPlaceStorageExpansionWithPVCRequirement(ctx, client, namespace, clusterName, storageTarget, true, guard); err != nil {
		return fmt.Errorf("in-place storage preflight/resize: %w", err)
	}
	// PVC expansion may advance Cluster status/resourceVersion. Re-read it and
	// verify the durable witness before the capacity-only compare-and-swap.
	if err := guard(); err != nil {
		return err
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	// This compare-and-swap changes exactly one scalar field. It cannot adopt
	// new affinity, resources, images, replica counts, secrets or app deployments.
	if err := client.patchPostgresStorageSize(ctx, namespace, clusterName, snapshot, size); err != nil {
		return recoverableManagedPostgresTransitionError("capacity-only cluster patch", err)
	}
	timeout := s.Config.ManagedAppRolloutTimeout
	if timeout <= 0 {
		timeout = config.DefaultManagedAppRolloutTimeout
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return err
		}
		current, err := s.observePostgresStorageSnapshot(waitCtx, client, namespace, clusterName, target.Postgres)
		if err == nil {
			err = verifyPostgresStorageWitness(baseline, current.Witness)
		}
		if err != nil {
			s.recordOperationEvidenceBestEffort(model.OperationEvidence{TenantID: app.TenantID, AppID: app.ID, OperationID: op.ID,
				Type: "postgres_storage_growth_continuity_failed", Source: model.OperationEvidenceSourceKubernetesAPI,
				Severity: model.OperationEvidenceSeverityError, Confidence: model.OperationEvidenceConfidenceConfirmed,
				Summary: "Storage growth continuity verification failed; no restart or switchover fallback", Message: err.Error()})
			return fmt.Errorf("storage growth continuity could not be verified; capacity may already be enlarged; no automatic restart or rollback: %w", err)
		}
		converged, detail, err := inspectManagedPostgresStorageExpansion(waitCtx, client, namespace, clusterName, storageTarget)
		if err != nil {
			return err
		}
		if converged {
			// Filesystem inspection spans several API reads. Recheck identity
			// and SQL after that barrier, immediately before durable completion.
			if err := guard(); err != nil {
				return fmt.Errorf("final storage growth continuity check: %w", err)
			}
			if q, err := resource.ParseQuantity(snapshot.StorageSize); err != nil || q.Cmp(resource.MustParse(size)) != 0 {
				return fmt.Errorf("Cluster storage declaration differs from the operation target %s", size)
			}
			break
		}
		s.updateManagedPostgresTransitionProgress(op.ID, "expanding storage without replacing the primary: "+detail)
		select {
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	// Persist ONLY capacity. Legacy localize requests may carry an implicit HA
	// reset; it must not leak into a storage-only operation's completion.
	finalPostgres := *model.CloneAppPostgresSpec(&target.Postgres)
	finalPostgres.StorageSize = size
	finalSpec := app.Spec
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	if target.AppOwned || strings.TrimSpace(target.ServiceID) == "" {
		finalSpec.Postgres = &finalPostgres
	} else {
		if _, err := s.updateAppBackingServicePostgres(target.ServiceID, app, finalPostgres); err != nil {
			return err
		}
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	s.recordOperationEvidenceBestEffort(model.OperationEvidence{TenantID: app.TenantID, AppID: app.ID, OperationID: op.ID,
		Type: "postgres_storage_growth_verified", Source: model.OperationEvidenceSourceKubernetesAPI,
		Severity: model.OperationEvidenceSeverityInfo, Confidence: model.OperationEvidenceConfidenceConfirmed,
		SubjectKind: "Pod", SubjectNamespace: namespace, SubjectName: baseline.PodName, SubjectUID: baseline.PodUID,
		Summary: "PVC and filesystem capacity converged with unchanged primary, postmaster and non-storage configuration", Payload: map[string]any{"storage_size": size, "pod_uid": baseline.PodUID, "postmaster_started_at": baseline.PostmasterStartedAt}})
	_, err := s.Store.CompleteManagedOperationWithResult(op.ID, "", "managed postgres storage expanded in place; primary identity and SQL readiness verified", &finalSpec, nil)
	return err
}

func (s *Service) storageGrowthBaseline(op model.Operation, app model.App, namespace string, witness postgresStorageWitness) (postgresStorageWitness, error) {
	items, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: app.TenantID, OperationID: op.ID, Types: []string{storageGrowthBaselineEvidence}, Limit: 1})
	if err != nil {
		return postgresStorageWitness{}, err
	}
	if len(items) != 0 {
		data, err := json.Marshal(items[0].Payload)
		if err != nil {
			return postgresStorageWitness{}, err
		}
		var previous postgresStorageWitness
		if err := json.Unmarshal(data, &previous); err != nil {
			return previous, err
		}
		if previous.PodUID == "" || previous.ClusterUID == "" {
			return previous, fmt.Errorf("invalid durable storage growth baseline")
		}
		return previous, nil
	}
	data, _ := json.Marshal(witness)
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return witness, err
	}
	_, err = s.Store.RecordOperationEvidence(model.OperationEvidence{TenantID: app.TenantID, AppID: app.ID, ProjectID: app.ProjectID, OperationID: op.ID,
		Type: storageGrowthBaselineEvidence, Source: model.OperationEvidenceSourceKubernetesAPI, Severity: model.OperationEvidenceSeverityInfo,
		Confidence: model.OperationEvidenceConfidenceConfirmed, SubjectKind: "Pod", SubjectNamespace: namespace, SubjectName: witness.PodName,
		SubjectUID: witness.PodUID, Summary: "Capacity-only growth baseline; unrelated Cluster and Pod configuration must remain unchanged",
		Payload: payload, PayloadVersion: 1})
	return witness, err
}

func verifyPostgresStorageWitness(before, after postgresStorageWitness) error {
	if before != after {
		return fmt.Errorf("primary identity, restart state, postmaster or non-storage configuration changed (before=%+v after=%+v)", before, after)
	}
	return nil
}

func (s *Service) observePostgresStorageSnapshot(ctx context.Context, client *kubeClient, namespace, name string, postgres model.AppPostgresSpec) (postgresStorageSnapshot, error) {
	var out postgresStorageSnapshot
	var raw map[string]any
	path := "/apis/postgresql.cnpg.io/v1/namespaces/" + url.PathEscape(namespace) + "/clusters/" + url.PathEscape(name)
	if _, err := client.doJSON(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return out, err
	}
	data, _ := json.Marshal(raw)
	var cluster kubeCloudNativePGCluster
	if err := json.Unmarshal(data, &cluster); err != nil {
		return out, err
	}
	if cluster.Metadata.UID == "" || cluster.Metadata.ResourceVersion == "" || !managedBackingServiceClusterReady(cluster, true) || cluster.Metadata.DeletionTimestamp != "" {
		return out, fmt.Errorf("storage growth requires an identified, ready, non-terminating Cluster")
	}
	spec, ok := raw["spec"].(map[string]any)
	if !ok {
		return out, fmt.Errorf("Cluster spec is unavailable")
	}
	storage, ok := spec["storage"].(map[string]any)
	if !ok || storage["resizeInUseVolumes"] != true {
		return out, fmt.Errorf("storage growth requires resizeInUseVolumes=true; no restart fallback")
	}
	if postgres.StorageClassName != "" && cluster.Spec.Storage.StorageClass != postgres.StorageClassName {
		return out, fmt.Errorf("live Cluster storage class differs from expansion intent")
	}
	delete(storage, "size")
	digest, err := storageSpecDigest(spec)
	if err != nil {
		return out, err
	}
	var podRaw map[string]any
	podPath := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods/" + url.PathEscape(cluster.Status.CurrentPrimary)
	if _, err := client.doJSON(ctx, http.MethodGet, podPath, nil, &podRaw); err != nil {
		return out, err
	}
	data, _ = json.Marshal(podRaw)
	var pod kubeResizePod
	if err := json.Unmarshal(data, &pod); err != nil {
		return out, err
	}
	observed, err := observeManagedPostgresResize(pod, managedPostgresMainContainerName)
	if err != nil {
		return out, err
	}
	if observed.PodUID == "" || observed.NodeName == "" || observed.DeletionTimestamp != "" || !observed.PodReady || !observed.ContainerReady || observed.ContainerStartedAt == "" {
		return out, fmt.Errorf("storage growth requires an identified ready primary Pod and running postgres container")
	}
	podDigest, err := storageSpecDigest(podRaw["spec"])
	if err != nil {
		return out, err
	}
	status, _ := podRaw["status"].(map[string]any)
	podIP, _ := status["podIP"].(string)
	startedAt, err := s.storageGrowthSQLWitness(ctx, managedPostgresRWServiceHost(namespace, model.PostgresRWServiceName(name)), podIP, postgres)
	if err != nil {
		return out, err
	}
	out.Witness = postgresStorageWitness{ClusterUID: cluster.Metadata.UID, ClusterSpecDigest: digest, PodName: observed.PodName, PodUID: observed.PodUID,
		PodSpecDigest: podDigest, NodeName: observed.NodeName, RestartCount: observed.RestartCount, ContainerStartedAt: observed.ContainerStartedAt, PostmasterStartedAt: startedAt}
	out.ResourceVersion, out.StorageSize = cluster.Metadata.ResourceVersion, cluster.Spec.Storage.Size
	return out, nil
}

func storageSpecDigest(value any) (string, error) {
	if value == nil {
		return "", fmt.Errorf("missing specification for continuity digest")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func (s *Service) storageGrowthSQLWitness(ctx context.Context, host, podIP string, postgres model.AppPostgresSpec) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	connect := s.postgresPrimarySQLConnect
	if connect == nil {
		connect = func(ctx context.Context, url string) (managedPostgresPrimarySQLConnection, error) {
			return pgx.Connect(ctx, url)
		}
	}
	conn, err := connect(probeCtx, managedPostgresServiceDatabaseURL(host, postgres))
	if err != nil {
		return "", fmt.Errorf("storage growth RW service SQL probe: %w", err)
	}
	defer closeManagedPostgresPrimarySQLConnection(conn)
	var recovery bool
	var readOnly, address, started string
	err = conn.QueryRow(probeCtx, `SELECT pg_is_in_recovery(), current_setting('transaction_read_only'), COALESCE(host(inet_server_addr()), ''), pg_postmaster_start_time()::text`).Scan(&recovery, &readOnly, &address, &started)
	if err != nil {
		return "", err
	}
	if err := validateManagedPostgresPrimarySQL(address, podIP, readOnly, recovery); err != nil {
		return "", err
	}
	if started == "" {
		return "", fmt.Errorf("missing PostgreSQL postmaster start witness")
	}
	return started, nil
}

func (c *kubeClient) patchPostgresStorageSize(ctx context.Context, namespace, name string, baseline postgresStorageSnapshot, target string) error {
	before, err := resource.ParseQuantity(baseline.StorageSize)
	if err != nil {
		return err
	}
	after, err := resource.ParseQuantity(target)
	if err != nil {
		return err
	}
	if before.Cmp(after) > 0 {
		return fmt.Errorf("refuse to shrink live Cluster storage")
	}
	if before.Cmp(after) == 0 {
		return nil
	}
	if baseline.Witness.ClusterUID == "" || baseline.ResourceVersion == "" {
		return fmt.Errorf("missing Cluster compare-and-swap identity")
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": baseline.Witness.ClusterUID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": baseline.ResourceVersion},
		{"op": "test", "path": "/spec/storage/size", "value": baseline.StorageSize},
		{"op": "replace", "path": "/spec/storage/size", "value": target},
	}
	_, err = c.doRequest(ctx, http.MethodPatch, "/apis/postgresql.cnpg.io/v1/namespaces/"+url.PathEscape(namespace)+"/clusters/"+url.PathEscape(name), "application/json-patch+json", patch, nil)
	return err
}
