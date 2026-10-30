package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	runtimepkg "fugue/internal/runtime"
)

// Populate a separate primary PVC while its Cluster does not exist. The seed
// remains immutable. CNPG then uses its orphan-PVC restore path, which supports
// local provisioners as well as CSI without assuming volume cloning support.
func (s *Service) prepareColdPrimaryVolume(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState, rv *string, scheduling runtimepkg.SchedulingConstraints, operationID string) error {
	cluster, exists, err := c.getCloudNativePGCluster(ctx, ns, st.TargetName)
	if err != nil {
		return err
	}
	if exists {
		if !st.PrimaryPrepared || st.PrimaryPVCUID == "" || cluster.Metadata.Annotations[coldMigrationAnnotation] != coldStateName(st.ServiceID) || (st.TargetUID != "" && cluster.Metadata.UID != st.TargetUID) {
			return fmt.Errorf("target exists before verified direct primary; refusing recopy")
		}
		_, err := coldPrimaryClaim(ctx, c, ns, st, cluster.Metadata.UID)
		return err
	}
	if st.TargetUID != "" {
		return fmt.Errorf("recorded target disappeared; refusing direct primary replay")
	}
	if err := s.ensureOperationStillActive(operationID); err != nil {
		return err
	}
	name := st.TargetName + "-1"
	claimPath := "/api/v1/namespaces/" + url.PathEscape(ns) + "/persistentvolumeclaims/" + url.PathEscape(name)
	_, found, err := c.getRawObject(ctx, claimPath)
	if err != nil {
		return err
	}
	if !found {
		if st.PrimaryPVCUID != "" || st.PrimaryPrepared {
			return fmt.Errorf("recorded direct primary claim disappeared")
		}
		copyState := *st
		copyState.SeedName = name
		// No CNPG labels are attached until full physical verification. There
		// is no Cluster to consume this claim while it is being populated.
		if _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/namespaces/"+url.PathEscape(ns)+"/persistentvolumeclaims", "application/json", coldSeedPVC(ns, &copyState), nil); err != nil {
			return err
		}
	}
	claim, err := coldPrimaryClaim(ctx, c, ns, st, "")
	if err != nil {
		return err
	}
	st.PrimaryPVCUID = objectStringField(normalizeKubeMap(claim["metadata"]), "uid")
	wasPrepared := st.PrimaryPrepared
	if err := saveColdState(ctx, c, ns, st, rv); err != nil {
		return err
	}
	if !st.PrimaryPrepared {
		if err := coldPrimaryUnused(ctx, c, ns, name); err != nil {
			return err
		}
		ev, err := coldSourceEvidenceForState(ctx, c, ns, st)
		if err != nil {
			return err
		}
		if ev.SystemID != st.SystemID || ev.FileDigest != st.FileDigest || ev.ControlDigest != st.ControlDigest {
			return fmt.Errorf("retained source changed before direct primary copy")
		}
		copyState := *st
		copyState.SeedName, copyState.SeedUID = name, st.PrimaryPVCUID
		s.updateManagedPostgresTransitionProgress(operationID, "copying and verifying the destination primary volume without CSI cloning")
		if _, err := s.copyColdSeed(ctx, c, ns, &copyState, ev, scheduling); err != nil {
			return err
		}
		verified, err := verifyColdSeedFiles(ctx, c, ns, &copyState, ev, scheduling)
		if err != nil {
			return err
		}
		after, err := coldSourceEvidenceForState(ctx, c, ns, st)
		if err != nil {
			return err
		}
		if verified.FileDigest != st.FileDigest || verified.SystemID != st.SystemID || verified.ControlDigest != st.ControlDigest || after.FileDigest != st.FileDigest || after.ControlDigest != st.ControlDigest || after.SystemID != st.SystemID {
			return fmt.Errorf("direct primary physical files differ from the immutable verified source")
		}
		st.PrimaryPrepared = true
		if err := saveColdState(ctx, c, ns, st, rv); err != nil {
			return err
		}
	}
	if wasPrepared {
		// Do not trust an old verification receipt after an interruption before
		// activation: re-read the destination, without overwriting any bytes.
		ev, err := coldSourceEvidenceForState(ctx, c, ns, st)
		if err != nil {
			return err
		}
		copyState := *st
		copyState.SeedName, copyState.SeedUID = name, st.PrimaryPVCUID
		verified, err := verifyColdSeedFiles(ctx, c, ns, &copyState, ev, scheduling)
		if err != nil {
			return err
		}
		if ev.FileDigest != st.FileDigest || ev.ControlDigest != st.ControlDigest || ev.SystemID != st.SystemID || verified.FileDigest != st.FileDigest || verified.ControlDigest != st.ControlDigest || verified.SystemID != st.SystemID {
			return fmt.Errorf("prepared primary changed before CNPG handoff")
		}
	}
	// A crash after verification may retry the metadata handoff, but must not
	// recopy a prepared claim or any claim after the target Cluster exists.
	if _, exists, err := c.getCloudNativePGCluster(ctx, ns, st.TargetName); err != nil || exists {
		return fmt.Errorf("target appeared during direct primary preparation; refusing handoff")
	}
	if err := coldPrimaryUnused(ctx, c, ns, name); err != nil {
		return err
	}
	claim, err = coldPrimaryClaim(ctx, c, ns, st, "")
	if err != nil {
		return err
	}
	if err := s.ensureOperationStillActive(operationID); err != nil {
		return err
	}
	return handoffColdPrimaryClaim(ctx, c, claimPath, claim, st)
}

func coldPrimaryClaim(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState, targetUID string) (map[string]any, error) {
	obj, found, err := c.getRawObject(ctx, "/api/v1/namespaces/"+url.PathEscape(ns)+"/persistentvolumeclaims/"+url.PathEscape(st.TargetName+"-1"))
	if err != nil {
		return nil, err
	}
	meta, spec := normalizeKubeMap(obj["metadata"]), normalizeKubeMap(obj["spec"])
	labels := normalizeKubeMap(meta["labels"])
	uid := objectStringField(meta, "uid")
	if !found || uid == "" || objectStringField(meta, "resourceVersion") == "" || meta["deletionTimestamp"] != nil || (st.PrimaryPVCUID != "" && st.PrimaryPVCUID != uid) || labels[coldMigrationAnnotation] != coldStateName(st.ServiceID) || (labels["cnpg.io/cluster"] != nil && labels["cnpg.io/cluster"] != st.TargetName) || spec["storageClassName"] != st.TargetClass || spec["dataSource"] != nil || spec["dataSourceRef"] != nil || normalizeKubeMap(normalizeKubeMap(spec["resources"])["requests"])["storage"] != st.TargetSize {
		return nil, fmt.Errorf("direct primary claim identity or storage changed")
	}
	if !st.PrimaryPrepared && labels["cnpg.io/cluster"] != nil {
		return nil, fmt.Errorf("unverified primary claim was already exposed to a cluster")
	}
	for _, owner := range mapSlice(meta["ownerReferences"]) {
		if targetUID == "" || owner["uid"] != targetUID || owner["name"] != st.TargetName || owner["kind"] != "Cluster" || owner["apiVersion"] != runtimepkg.CloudNativePGAPIVersion || owner["controller"] != true {
			return nil, fmt.Errorf("direct primary claim has an unexpected owner")
		}
	}
	return obj, nil
}

func coldPrimaryUnused(ctx context.Context, c *kubeClient, ns, name string) error {
	pods, err := c.listPodsBySelector(ctx, ns, "")
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if managedPostgresPodFinished(pod) {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == name {
				return fmt.Errorf("direct primary claim is already in use by %s", pod.Metadata.Name)
			}
		}
	}
	var jobs struct {
		Items []struct {
			Spec struct {
				Template struct {
					Spec struct {
						Volumes []kubePodVolume `json:"volumes"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		} `json:"items"`
	}
	if _, err := c.doJSON(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+url.PathEscape(ns)+"/jobs", nil, &jobs); err != nil {
		return err
	}
	for _, job := range jobs.Items {
		for _, volume := range job.Spec.Template.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == name {
				return fmt.Errorf("direct primary claim is referenced by an existing Job")
			}
		}
	}
	return nil
}

func handoffColdPrimaryClaim(ctx context.Context, c *kubeClient, claimPath string, claim map[string]any, st *coldPostgresState) error {
	if !st.PrimaryPrepared || st.PrimaryPVCUID == "" {
		return fmt.Errorf("unverified direct primary may not be exposed to CNPG")
	}
	meta := normalizeKubeMap(claim["metadata"])
	labels, annotations := normalizeKubeMap(meta["labels"]), normalizeKubeMap(meta["annotations"])
	if labels == nil {
		labels = map[string]any{}
	}
	if annotations == nil {
		annotations = map[string]any{}
	}
	labels["cnpg.io/cluster"], labels["cnpg.io/instanceName"], labels["cnpg.io/instanceRole"], labels["role"] = st.TargetName, st.TargetName+"-1", "primary", "primary"
	annotations["cnpg.io/nodeSerial"], annotations["cnpg.io/pvcStatus"] = "1", "ready"
	patch := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": st.PrimaryPVCUID}, {"op": "test", "path": "/metadata/resourceVersion", "value": objectStringField(meta, "resourceVersion")}, {"op": "add", "path": "/metadata/labels", "value": labels}, {"op": "add", "path": "/metadata/annotations", "value": annotations}}
	_, err := c.doRequest(ctx, http.MethodPatch, claimPath, "application/json-patch+json", patch, nil)
	return err
}
