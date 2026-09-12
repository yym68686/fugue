package controller

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/model"
)

func validateDatabaseMigrationTopology(ctx context.Context, client *kubeClient, namespace, storageClass string, requiredInstances int) error {
	sc, found, err := client.getStorageClass(ctx, storageClass)
	if err != nil {
		return fmt.Errorf("read target storage class: %w", err)
	}
	if !found {
		return fmt.Errorf("target storage class %q not found", storageClass)
	}
	if sc.Provisioner != "driver.longhorn.io" {
		return nil
	}
	var list struct {
		Items []struct {
			Spec struct {
				AllowScheduling bool `json:"allowScheduling"`
			} `json:"spec"`
			Status struct {
				DiskStatus map[string]struct {
					Conditions []struct {
						Type   string `json:"type"`
						Status string `json:"status"`
					} `json:"conditions"`
				} `json:"diskStatus"`
			} `json:"status"`
		} `json:"items"`
	}
	if _, err := client.doJSON(ctx, http.MethodGet, "/apis/longhorn.io/v1beta2/nodes", nil, &list); err != nil {
		return fmt.Errorf("list Longhorn storage nodes: %w", err)
	}
	ready := 0
	for _, node := range list.Items {
		if !node.Spec.AllowScheduling {
			continue
		}
		for _, disk := range node.Status.DiskStatus {
			good, sched := false, false
			for _, c := range disk.Conditions {
				if c.Type == "Ready" && c.Status == "True" {
					good = true
				}
				if c.Type == "Schedulable" && c.Status == "True" {
					sched = true
				}
			}
			if good && sched {
				ready++
				break
			}
		}
	}
	if requiredInstances < 1 {
		requiredInstances = 1
	}
	if ready < requiredInstances {
		return fmt.Errorf("target Longhorn storage has %d ready schedulable nodes, but the database requires %d; add Longhorn storage nodes before migrating", ready, requiredInstances)
	}
	return nil
}

func databaseMigrationLonghornNodes(ctx context.Context, client *kubeClient, requiredInstances int) ([]string, error) {
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				AllowScheduling bool `json:"allowScheduling"`
			} `json:"spec"`
			Status struct {
				DiskStatus map[string]struct {
					Conditions []struct {
						Type   string `json:"type"`
						Status string `json:"status"`
					} `json:"conditions"`
				} `json:"diskStatus"`
			} `json:"status"`
		} `json:"items"`
	}
	if _, err := client.doJSON(ctx, http.MethodGet, "/apis/longhorn.io/v1beta2/nodes", nil, &list); err != nil {
		return nil, err
	}
	if requiredInstances < 1 {
		requiredInstances = 1
	}
	nodes := make([]string, 0, len(list.Items))
	for _, node := range list.Items {
		if !node.Spec.AllowScheduling {
			continue
		}
		for _, disk := range node.Status.DiskStatus {
			ready, sched := false, false
			for _, c := range disk.Conditions {
				if c.Type == "Ready" && c.Status == "True" {
					ready = true
				}
				if c.Type == "Schedulable" && c.Status == "True" {
					sched = true
				}
			}
			if ready && sched {
				nodes = append(nodes, node.Metadata.Name)
				break
			}
		}
	}
	if len(nodes) < requiredInstances {
		return nil, fmt.Errorf("only %d ready schedulable Longhorn nodes are available, but %d are required", len(nodes), requiredInstances)
	}
	return nodes, nil
}

func appendDatabaseMigrationNodeAffinity(existing map[string]any, nodeNames []string) map[string]any {
	if existing == nil {
		existing = map[string]any{}
	}
	copyMap := map[string]any{}
	for k, v := range existing {
		copyMap[k] = v
	}
	na, _ := copyMap["nodeAffinity"].(map[string]any)
	if na == nil {
		na = map[string]any{}
	}
	required, _ := na["requiredDuringSchedulingIgnoredDuringExecution"].(map[string]any)
	if required == nil {
		required = map[string]any{}
	}
	terms, _ := required["nodeSelectorTerms"].([]any)
	expr := map[string]any{"key": "kubernetes.io/hostname", "operator": "In", "values": nodeNames}
	if len(terms) == 0 {
		terms = []any{map[string]any{"matchExpressions": []any{expr}}}
	} else {
		for _, raw := range terms {
			if term, ok := raw.(map[string]any); ok {
				expressions, _ := term["matchExpressions"].([]any)
				term["matchExpressions"] = append(expressions, expr)
			}
		}
	}
	required["nodeSelectorTerms"] = terms
	na["requiredDuringSchedulingIgnoredDuringExecution"] = required
	copyMap["nodeAffinity"] = na
	return copyMap
}

const controlPlaneDatabaseMigrationKind = "managed-postgres"

func (s *Service) startDatabaseMigrationWorker(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			if err := s.processDatabaseMigrations(ctx); err != nil && s.Logger != nil {
				s.Logger.Printf("database migration worker: %v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *Service) processDatabaseMigrations(ctx context.Context) error {
	migrations, err := s.Store.ListPendingDatabaseMigrations()
	if err != nil {
		return err
	}
	for _, migration := range migrations {
		if err := s.executeDatabaseMigration(ctx, migration); err != nil && s.Logger != nil {
			s.Logger.Printf("database migration %s: %v", migration.ID, err)
		}
	}
	return nil
}

func (s *Service) executeDatabaseMigration(ctx context.Context, migration model.DatabaseMigration) error {
	if migration.Kind != controlPlaneDatabaseMigrationKind {
		return s.failDatabaseMigration(migration, "unsupported database migration kind")
	}
	client, err := s.kubeClient()
	if err != nil {
		return s.failDatabaseMigration(migration, err.Error())
	}
	namespace := strings.TrimSpace(migration.Namespace)
	clusterName := strings.TrimSpace(migration.ClusterName)
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, clusterName)
	if err != nil {
		return s.failDatabaseMigration(migration, err.Error())
	}
	if !found {
		return s.failDatabaseMigration(migration, "CNPG cluster not found")
	}
	if migration.ClusterUID != "" && migration.ClusterUID != cluster.Metadata.UID {
		return s.failDatabaseMigration(migration, "CNPG cluster identity changed during migration")
	}
	if migration.Status == model.DatabaseMigrationStatusPending {
		if err := validateDatabaseMigrationTopology(ctx, client, namespace, migration.TargetStorageClassName, cluster.Spec.Instances); err != nil {
			return s.failDatabaseMigration(migration, err.Error())
		}
		migration.ClusterUID = cluster.Metadata.UID
		migration.InitialInstances = cluster.Spec.Instances
		migration.InitialSystemID = cluster.Status.SystemID
		if migration.SourceStorageClassName == "" {
			migration.SourceStorageClassName = s.liveClusterStorageClass(ctx, client, namespace, clusterName)
		}
		if migration.SourceStorageClassName == migration.TargetStorageClassName {
			return s.completeDatabaseMigration(migration, "database already uses target storage class")
		}
		migration.Status = model.DatabaseMigrationStatusRunning
		migration.Phase = "prepare-standby"
		now := time.Now().UTC()
		migration.StartedAt = &now
		if err := s.Store.UpdateDatabaseMigration(migration); err != nil {
			return err
		}
		nodeNames, err := databaseMigrationLonghornNodes(ctx, client, cluster.Spec.Instances)
		if err != nil {
			return s.failDatabaseMigration(migration, err.Error())
		}
		affinity := appendDatabaseMigrationNodeAffinity(cluster.Spec.Affinity, nodeNames)
		temporaryReplicas := migration.TemporaryReplicaCount
		if temporaryReplicas < 0 {
			temporaryReplicas = 0
		}
		spec := map[string]any{"instances": cluster.Spec.Instances + temporaryReplicas, "storage": map[string]any{"size": firstNonEmpty(migration.StorageSize, cluster.Spec.Storage.Size), "storageClass": migration.TargetStorageClassName, "resizeInUseVolumes": true}, "affinity": affinity}
		if err := client.patchCloudNativePGClusterSpec(ctx, namespace, clusterName, spec); err != nil {
			return s.failDatabaseMigration(migration, err.Error())
		}
	}
	if migration.InitialInstances == 1 && migration.TemporaryReplicaCount == 0 && migration.Phase == "prepare-standby" {
		migration.Phase = "single-instance-cutover"
		migration.ResultMessage = "single-instance control-plane storage cutover pending"
		if err := s.Store.UpdateDatabaseMigration(migration); err != nil {
			return err
		}
	}
	if migration.Phase == "single-instance-cutover" {
		cluster, _, _ = client.getCloudNativePGCluster(ctx, namespace, clusterName)
		if strings.TrimSpace(cluster.Status.CurrentPrimary) != "" && s.podStorageClass(ctx, client, namespace, cluster.Status.CurrentPrimary) == migration.TargetStorageClassName {
			if migration.InitialSystemID != "" && cluster.Status.SystemID != "" && migration.InitialSystemID != cluster.Status.SystemID {
				return s.failDatabaseMigration(migration, "PostgreSQL system identifier changed during migration")
			}
			return s.completeDatabaseMigration(migration, "single-instance control-plane database migrated to "+migration.TargetStorageClassName)
		}
		return nil
	}
	if migration.Phase == "prepare-standby" || migration.Phase == "" {
		pods, _ := client.listPodsBySelector(ctx, namespace, "cnpg.io/cluster="+clusterName+",cnpg.io/instanceRole=replica")
		for _, pod := range pods {
			if !kubePodReady(pod) {
				continue
			}
			pvc := pod.Metadata.Name
			if migration.TargetStorageClassName == s.podStorageClass(ctx, client, namespace, pvc) {
				migration.Phase = "switchover"
				migration.ResultMessage = "standby ready"
				_ = s.Store.UpdateDatabaseMigration(migration)
				break
			}
		}
	}
	if migration.Phase == "switchover" {
		pods, _ := client.listPodsBySelector(ctx, namespace, "cnpg.io/cluster="+clusterName+",cnpg.io/instanceRole=replica")
		target := ""
		for _, pod := range pods {
			if kubePodReady(pod) && s.podStorageClass(ctx, client, namespace, pod.Metadata.Name) == migration.TargetStorageClassName {
				target = pod.Metadata.Name
				break
			}
		}
		if target != "" {
			if err := client.patchCloudNativePGClusterStatus(ctx, namespace, clusterName, target, "Switchover", "database storage migration"); err != nil {
				return s.failDatabaseMigration(migration, err.Error())
			}
			migration.Phase = "finalize"
			_ = s.Store.UpdateDatabaseMigration(migration)
		}
	}
	if migration.Phase == "finalize" {
		cluster, _, _ = client.getCloudNativePGCluster(ctx, namespace, clusterName)
		if strings.TrimSpace(cluster.Status.CurrentPrimary) != "" && s.podStorageClass(ctx, client, namespace, cluster.Status.CurrentPrimary) == migration.TargetStorageClassName {
			if migration.InitialInstances < 1 {
				migration.InitialInstances = cluster.Spec.Instances
			}
			if migration.InitialSystemID != "" && cluster.Status.SystemID != "" && migration.InitialSystemID != cluster.Status.SystemID {
				return s.failDatabaseMigration(migration, "PostgreSQL system identifier changed during migration")
			}
			spec := map[string]any{"instances": migration.InitialInstances, "storage": map[string]any{"size": firstNonEmpty(migration.StorageSize, cluster.Spec.Storage.Size), "storageClass": migration.TargetStorageClassName, "resizeInUseVolumes": true}}
			if cluster.Spec.Instances != migration.InitialInstances {
				if err := client.patchCloudNativePGClusterSpec(ctx, namespace, clusterName, spec); err != nil {
					return s.failDatabaseMigration(migration, err.Error())
				}
				return nil
			}
			pvcNames, err := client.listPersistentVolumeClaimNamesByLabel(ctx, namespace, "cnpg.io/cluster="+clusterName+",cnpg.io/pvcRole=PG_DATA")
			if err != nil {
				return err
			}
			if len(pvcNames) < migration.InitialInstances {
				return nil
			}
			for _, pvcName := range pvcNames {
				pvc, ok, err := client.getPersistentVolumeClaim(ctx, namespace, pvcName)
				if err != nil {
					return err
				}
				if !ok || strings.TrimSpace(pvc.Spec.StorageClassName) != migration.TargetStorageClassName {
					return nil
				}
			}
			return s.completeDatabaseMigration(migration, "control-plane database migrated to "+migration.TargetStorageClassName)
		}
	}
	return nil
}

func (s *Service) failDatabaseMigration(m model.DatabaseMigration, msg string) error {
	m.Status = model.DatabaseMigrationStatusFailed
	m.ErrorMessage = msg
	m.CompletedAt = ptrTime(time.Now().UTC())
	_ = s.Store.UpdateDatabaseMigration(m)
	return fmt.Errorf("database migration %s: %s", m.ID, msg)
}
func (s *Service) completeDatabaseMigration(m model.DatabaseMigration, msg string) error {
	m.Status = model.DatabaseMigrationStatusCompleted
	m.Phase = "completed"
	m.ResultMessage = msg
	m.CompletedAt = ptrTime(time.Now().UTC())
	return s.Store.UpdateDatabaseMigration(m)
}
func ptrTime(t time.Time) *time.Time { return &t }
func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}
func (s *Service) liveClusterStorageClass(ctx context.Context, c *kubeClient, ns, name string) string {
	pods, _ := c.listPodsBySelector(ctx, ns, "cnpg.io/cluster="+name+",cnpg.io/instanceRole=primary")
	if len(pods) > 0 {
		return s.podStorageClass(ctx, c, ns, pods[0].Metadata.Name)
	}
	return ""
}
func (s *Service) podStorageClass(ctx context.Context, c *kubeClient, ns, pod string) string {
	p, ok, _ := c.getPod(ctx, ns, pod)
	if !ok {
		return ""
	}
	for _, v := range p.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			pvc, ok, _ := c.getPersistentVolumeClaim(ctx, ns, v.PersistentVolumeClaim.ClaimName)
			if ok {
				return strings.TrimSpace(pvc.Spec.StorageClassName)
			}
		}
	}
	return ""
}
