package controller

import (
	"fmt"
	"strings"
)

const (
	cloudNativePGClusterLabel      = "cnpg.io/cluster"
	cloudNativePGLegacyRoleLabel   = "role"
	cloudNativePGInstanceRoleLabel = "cnpg.io/instanceRole"
)

type managedPostgresResizeInvariantBaseline struct {
	Namespace          string
	ClusterName        string
	ClusterUID         string
	ClusterGeneration  int64
	PrimaryPodName     string
	PodUID             string
	PodGeneration      int64
	NodeName           string
	ContainerName      string
	RestartCount       int
	ContainerStartedAt string
}

func captureManagedPostgresResizeInvariantBaseline(
	namespace, clusterName string,
	cluster kubeCloudNativePGCluster,
	observation managedPostgresResizeObservation,
) (managedPostgresResizeInvariantBaseline, error) {
	if err := validateManagedPostgresResizeLiveIdentity(namespace, clusterName, cluster, observation, false); err != nil {
		return managedPostgresResizeInvariantBaseline{}, err
	}
	return managedPostgresResizeInvariantBaseline{
		Namespace:          strings.TrimSpace(namespace),
		ClusterName:        strings.TrimSpace(clusterName),
		ClusterUID:         strings.TrimSpace(cluster.Metadata.UID),
		ClusterGeneration:  cluster.Metadata.Generation,
		PrimaryPodName:     strings.TrimSpace(cluster.Status.CurrentPrimary),
		PodUID:             strings.TrimSpace(observation.PodUID),
		PodGeneration:      observation.Generation,
		NodeName:           strings.TrimSpace(observation.NodeName),
		ContainerName:      strings.TrimSpace(observation.ContainerName),
		RestartCount:       observation.RestartCount,
		ContainerStartedAt: strings.TrimSpace(observation.ContainerStartedAt),
	}, nil
}

func validateManagedPostgresResizeInvariantBaseline(
	baseline managedPostgresResizeInvariantBaseline,
	cluster kubeCloudNativePGCluster,
	observation managedPostgresResizeObservation,
) error {
	if err := validateManagedPostgresResizeLiveIdentity(
		baseline.Namespace,
		baseline.ClusterName,
		cluster,
		observation,
		true,
	); err != nil {
		return err
	}
	for label, values := range map[string][2]string{
		"cluster UID":          {baseline.ClusterUID, strings.TrimSpace(cluster.Metadata.UID)},
		"primary Pod":          {baseline.PrimaryPodName, strings.TrimSpace(cluster.Status.CurrentPrimary)},
		"Pod UID":              {baseline.PodUID, strings.TrimSpace(observation.PodUID)},
		"node":                 {baseline.NodeName, strings.TrimSpace(observation.NodeName)},
		"container":            {baseline.ContainerName, strings.TrimSpace(observation.ContainerName)},
		"container started-at": {baseline.ContainerStartedAt, strings.TrimSpace(observation.ContainerStartedAt)},
	} {
		expected, actual := values[0], values[1]
		if expected == "" || actual != expected {
			return fmt.Errorf("managed postgres resize invariant changed %s: expected %q, got %q", label, expected, actual)
		}
	}
	if baseline.ClusterGeneration <= 0 || cluster.Metadata.Generation != baseline.ClusterGeneration {
		return fmt.Errorf(
			"managed postgres resize invariant changed cluster generation: expected %d, got %d",
			baseline.ClusterGeneration,
			cluster.Metadata.Generation,
		)
	}
	if baseline.PodGeneration <= 0 || observation.Generation != baseline.PodGeneration {
		return fmt.Errorf(
			"managed postgres resize invariant changed Pod generation: expected %d, got %d",
			baseline.PodGeneration,
			observation.Generation,
		)
	}
	if observation.RestartCount != baseline.RestartCount {
		return fmt.Errorf(
			"managed postgres resize invariant changed restart count: expected %d, got %d",
			baseline.RestartCount,
			observation.RestartCount,
		)
	}
	return nil
}

func validateManagedPostgresResizeLiveIdentity(
	namespace, clusterName string,
	cluster kubeCloudNativePGCluster,
	observation managedPostgresResizeObservation,
	allowResizeCondition bool,
) error {
	namespace = strings.TrimSpace(namespace)
	clusterName = strings.TrimSpace(clusterName)
	if namespace == "" || clusterName == "" {
		return fmt.Errorf("managed postgres resize requires namespace and cluster name")
	}
	if strings.TrimSpace(cluster.Metadata.Name) != clusterName ||
		strings.TrimSpace(cluster.Metadata.UID) == "" ||
		strings.TrimSpace(cluster.Metadata.ResourceVersion) == "" ||
		cluster.Metadata.Generation <= 0 {
		return fmt.Errorf("managed postgres resize requires an exact persisted CNPG Cluster identity")
	}
	if cluster.Spec.Instances <= 0 || cluster.Status.ReadyInstances != cluster.Spec.Instances ||
		!cloudNativePGResizeReadyCondition(cluster) {
		return fmt.Errorf(
			"managed postgres resize requires every CNPG instance Ready: ready=%d desired=%d",
			cluster.Status.ReadyInstances,
			cluster.Spec.Instances,
		)
	}
	primary := strings.TrimSpace(cluster.Status.CurrentPrimary)
	if primary == "" || primary != strings.TrimSpace(observation.PodName) {
		return fmt.Errorf("managed postgres resize target is not the exact CNPG current primary")
	}
	if targetPrimary := strings.TrimSpace(cluster.Status.TargetPrimary); targetPrimary != "" && targetPrimary != primary {
		return fmt.Errorf("managed postgres resize is blocked while CNPG primary transition targets %s", targetPrimary)
	}
	if strings.TrimSpace(observation.Namespace) != namespace ||
		strings.TrimSpace(observation.PodUID) == "" ||
		strings.TrimSpace(observation.ResourceVersion) == "" ||
		observation.Generation <= 0 ||
		observation.ObservedGeneration < observation.Generation {
		return fmt.Errorf("managed postgres resize requires an exact observed Pod identity and generation")
	}
	if strings.TrimSpace(observation.DeletionTimestamp) != "" {
		return fmt.Errorf("managed postgres resize target Pod is terminating")
	}
	if strings.TrimSpace(observation.NodeName) == "" ||
		strings.TrimSpace(observation.Phase) != "Running" ||
		!observation.PodReady || !observation.ContainerReady ||
		strings.TrimSpace(observation.ContainerStartedAt) == "" ||
		observation.ActualResources == nil {
		return fmt.Errorf("managed postgres resize requires a scheduled, Running, Ready Pod with actual resources")
	}
	if !allowResizeCondition && resizePendingCondition(observation.Conditions) != nil {
		return fmt.Errorf("managed postgres resize preflight found another resize already pending")
	}
	if strings.TrimSpace(observation.Labels[cloudNativePGClusterLabel]) != clusterName ||
		!strings.EqualFold(strings.TrimSpace(observation.Labels[cloudNativePGLegacyRoleLabel]), "primary") ||
		!strings.EqualFold(strings.TrimSpace(observation.Labels[cloudNativePGInstanceRoleLabel]), "primary") {
		return fmt.Errorf("managed postgres resize target Pod does not carry the exact CNPG primary labels")
	}
	if !managedPostgresResizePodOwnedByCluster(observation.OwnerReferences, clusterName, cluster.Metadata.UID) {
		return fmt.Errorf("managed postgres resize target Pod is not controller-owned by the exact CNPG Cluster")
	}
	return nil
}

func cloudNativePGResizeReadyCondition(cluster kubeCloudNativePGCluster) bool {
	for _, condition := range cluster.Status.Conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), "Ready") &&
			strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			return true
		}
	}
	return false
}

func managedPostgresResizePodOwnedByCluster(
	owners []kubeResizeOwnerReference,
	clusterName, clusterUID string,
) bool {
	clusterName = strings.TrimSpace(clusterName)
	clusterUID = strings.TrimSpace(clusterUID)
	for _, owner := range owners {
		if owner.Controller == nil || !*owner.Controller ||
			strings.TrimSpace(owner.APIVersion) != "postgresql.cnpg.io/v1" ||
			strings.TrimSpace(owner.Kind) != "Cluster" ||
			strings.TrimSpace(owner.Name) != clusterName ||
			strings.TrimSpace(owner.UID) != clusterUID {
			continue
		}
		return true
	}
	return false
}
