package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
	"k8s.io/apimachinery/pkg/api/resource"
)

const coldMigrationAnnotation = "fugue.pro/cold-migration"

type coldPostgresState struct {
	Version        int    `json:"version"`
	ServiceID      string `json:"service_id"`
	AppID          string `json:"app_id"`
	SourceName     string `json:"source_name"`
	SourceUID      string `json:"source_uid"`
	SourcePVC      string `json:"source_pvc"`
	SourcePVCUID   string `json:"source_pvc_uid"`
	SourcePV       string `json:"source_pv"`
	SourcePodUID   string `json:"source_pod_uid"`
	SourceNode     string `json:"source_node"`
	SourceSpecHash string `json:"source_spec_hash"`
	TargetRuntime  string `json:"target_runtime"`
	TargetNode     string `json:"target_node"`
	TargetClass    string `json:"target_class"`
	TargetSize     string `json:"target_size"`
	SeedName       string `json:"seed_name"`
	SeedUID        string `json:"seed_uid"`
	TargetName     string `json:"target_name"`
	TargetUID      string `json:"target_uid"`
	Endpoint       string `json:"endpoint"`
	Image          string `json:"image"`
	SourceImageID  string `json:"source_image_id"`
	SystemID       string `json:"system_id"`
	FileDigest     string `json:"file_digest"`
	StreamDigest   string `json:"stream_digest"`
	ControlDigest  string `json:"control_digest"`
	Phase          string `json:"phase"`
}

func coldStateName(serviceID string) string {
	h := sha256.Sum256([]byte(serviceID))
	return "pg-cold-" + hex.EncodeToString(h[:12])
}
func coldStatePath(ns, id string) string {
	return "/api/v1/namespaces/" + url.PathEscape(ns) + "/configmaps/" + coldStateName(id)
}

func loadColdState(ctx context.Context, c *kubeClient, ns, id string) (*coldPostgresState, string, error) {
	obj, found, err := c.getRawObject(ctx, coldStatePath(ns, id))
	if err != nil || !found {
		return nil, "", err
	}
	m := normalizeKubeMap(obj["metadata"])
	data := normalizeKubeMap(obj["data"])
	var st coldPostgresState
	raw, _ := data["state.json"].(string)
	if json.Unmarshal([]byte(raw), &st) != nil || st.Version != 1 || st.ServiceID != id || objectStringField(m, "resourceVersion") == "" {
		return nil, "", fmt.Errorf("cold recovery state is invalid")
	}
	return &st, objectStringField(m, "resourceVersion"), nil
}

func saveColdState(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState, rv *string) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	m := map[string]any{"name": coldStateName(st.ServiceID), "namespace": ns, "labels": map[string]string{"fugue.pro/cold-service": st.ServiceID}}
	method, path := http.MethodPost, "/api/v1/namespaces/"+url.PathEscape(ns)+"/configmaps"
	if *rv != "" {
		method, path = http.MethodPut, coldStatePath(ns, st.ServiceID)
		m["resourceVersion"] = *rv
	}
	var out map[string]any
	_, err = c.doRequest(ctx, method, path, "application/json", map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": m, "data": map[string]string{"state.json": string(raw)}}, &out)
	if err != nil {
		return fmt.Errorf("persist cold recovery phase %s: %w", st.Phase, err)
	}
	*rv = objectStringField(normalizeKubeMap(out["metadata"]), "resourceVersion")
	return nil
}

func (s *Service) prepareColdState(ctx context.Context, c *kubeClient, op model.Operation, app model.App, source kubeCloudNativePGCluster, pg model.AppPostgresSpec, targetRuntime model.Runtime) (*coldPostgresState, string, error) {
	ns := runtimepkg.NamespaceForTenant(app.TenantID)
	existing, rv, err := loadColdState(ctx, c, ns, op.ServiceID)
	if err != nil {
		return nil, "", err
	}
	if existing != nil {
		if existing.AppID != app.ID || existing.TargetRuntime != pg.RuntimeID || existing.TargetClass != pg.StorageClassName {
			return nil, "", fmt.Errorf("conflicting cold migration intent; retained state must be resolved first")
		}
		return existing, rv, nil
	}
	target, err := store.ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil || target == nil {
		return nil, "", fmt.Errorf("cold recovery requires an independent bound service")
	}
	if op.ServiceID == "" {
		return nil, "", fmt.Errorf("cold recovery supports independent services only")
	}
	pvc, found, err := c.getPersistentVolumeClaim(ctx, ns, source.Status.CurrentPrimary)
	if err != nil {
		return nil, "", err
	}
	if !found || pvc.Status.Phase != "Bound" || pvc.Metadata.UID == "" || pvc.Spec.VolumeName == "" || pvc.Metadata.Labels["cnpg.io/cluster"] != source.Metadata.Name {
		return nil, "", fmt.Errorf("source primary PVC identity is incomplete")
	}
	raw, found, err := c.getRawObject(ctx, cloudNativePGClusterAPIPath(ns, source.Metadata.Name))
	if err != nil || !found {
		return nil, "", fmt.Errorf("source cluster unavailable")
	}
	spec := normalizeKubeMap(raw["spec"])
	if spec["walStorage"] != nil || len(mapSlice(spec["tablespaces"])) > 0 || spec["replica"] != nil {
		return nil, "", fmt.Errorf("cold migration requires a single primary PGDATA volume without external WAL/tablespaces")
	}
	pod, found, err := c.getPod(ctx, ns, source.Status.CurrentPrimary)
	if err != nil || !found || pod.ObservedUID == "" || pod.Spec.NodeName == "" {
		return nil, "", fmt.Errorf("source primary Pod identity unavailable")
	}
	sourceNode, found, err := managedPostgresPVCNode(ctx, c, ns, pvc.Metadata.Name)
	if err != nil || !found || sourceNode != pod.Spec.NodeName {
		return nil, "", fmt.Errorf("source Pod/PVC node identity mismatch")
	}
	node, err := recoveryStorageTargetNode(ctx, c, targetRuntime, pg.StorageClassName, pg.PrimaryNodeName)
	if err != nil {
		return nil, "", err
	}
	// Destination-only recovery headroom. The source request and filesystem are
	// never changed; the final 10Gi live expansion is a separate operation.
	q, err := resource.ParseQuantity(pvc.Status.Capacity["storage"])
	if err != nil || q.Sign() <= 0 {
		return nil, "", fmt.Errorf("source capacity unavailable")
	}
	size, err := maximumRecoveryStorageSize(pg.StorageSize, resource.NewQuantity(q.Value()+(2<<30), resource.BinarySI).String())
	if err != nil {
		return nil, "", err
	}
	imageID := ""
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == "postgres" {
			imageID = cs.ImageID
		}
	}
	imageID = strings.TrimPrefix(imageID, "docker-pullable://")
	_, digest, ok := strings.Cut(imageID, "@sha256:")
	if !ok || len(digest) != 64 {
		return nil, "", fmt.Errorf("source postgres immutable image digest unavailable")
	}
	image := ""
	for _, container := range pod.Spec.Containers {
		if container.Name == "postgres" {
			ref, _, _ := strings.Cut(container.Image, "@")
			image = ref + "@sha256:" + digest
		}
	}
	if image == "" {
		return nil, "", fmt.Errorf("source postgres image reference unavailable")
	}
	st := &coldPostgresState{Version: 1, ServiceID: op.ServiceID, AppID: app.ID, SourceName: source.Metadata.Name, SourceUID: source.Metadata.UID, SourcePVC: pvc.Metadata.Name, SourcePVCUID: pvc.Metadata.UID, SourcePV: pvc.Spec.VolumeName, SourcePodUID: pod.ObservedUID, SourceNode: sourceNode, SourceSpecHash: store.PostgresSpecFingerprint(target.Postgres), TargetRuntime: pg.RuntimeID, TargetNode: node, TargetClass: pg.StorageClassName, TargetSize: size, Endpoint: model.PostgresEndpointName(target.Postgres), Image: image, SourceImageID: imageID, Phase: "planned"}
	h := sha256.Sum256([]byte(op.ServiceID + source.Metadata.UID + pg.RuntimeID + pg.StorageClassName))
	suffix := hex.EncodeToString(h[:8])
	st.SeedName = "pg-seed-" + suffix
	st.TargetName = "pg-restored-" + suffix
	if err := saveColdState(ctx, c, ns, st, &rv); err != nil {
		return nil, "", err
	}
	return st, rv, nil
}
