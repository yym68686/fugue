// Package pgretire contains the shared, read-only physical identity contract for
// explicit retirement of a completed cold migration's source. Names are never
// sufficient authority to remove a resource.
package pgretire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"fugue/internal/model"
)

const OperationType = "database-source-retire"
const IntentEvidence = "postgres_source_retirement_intent"
const VerifiedEvidence = "postgres_source_retirement_verified"
const RetainedUIDAnnotation = "fugue.pro/retired-source-cluster-uid"
const RetainedPlanAnnotation = "fugue.pro/source-retirement-plan"
const ClusterLabel = "cnpg.io/cluster"

type Get func(context.Context, string) (map[string]any, bool, error)

func Object(v any) map[string]any { m, _ := v.(map[string]any); return m }
func Objects(v any) []map[string]any {
	out := []map[string]any{}
	items, _ := v.([]any)
	for _, item := range items {
		if m := Object(item); m != nil {
			out = append(out, m)
		}
	}
	return out
}
func String(m map[string]any, k string) string { s, _ := m[k].(string); return s }
func Meta(m map[string]any) map[string]any     { return Object(m["metadata"]) }
func Hash(v any) string {
	b, _ := json.Marshal(v)
	// Canonicalize structs as JSON objects too: PostgreSQL jsonb and evidence
	// readers reorder keys, but must not invalidate a durable identity witness.
	var canonical any
	if json.Unmarshal(b, &canonical) == nil {
		b, _ = json.Marshal(canonical)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func PlanHash(p model.PostgresSourceRetirementPlan) string { p.PlanHash = ""; return Hash(p) }
func ConfigHash(app model.App, svc model.BackingService) string {
	bindings := []model.ServiceBinding{}
	for _, b := range app.Bindings {
		if b.ServiceID == svc.ID {
			bindings = append(bindings, b)
		}
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].ID < bindings[j].ID })
	return Hash([]any{app.ID, app.TenantID, app.ProjectID, svc.ID, svc.TenantID, svc.ProjectID, svc.OwnerAppID, svc.Spec, bindings})
}
func Path(ns, resource, name string) string {
	p := "/api/v1/namespaces/" + url.PathEscape(ns) + "/" + resource
	if resource == "clusters" {
		p = "/apis/postgresql.cnpg.io/v1/namespaces/" + url.PathEscape(ns) + "/clusters"
	}
	if name != "" {
		p += "/" + url.PathEscape(name)
	}
	return p
}
func MigrationName(serviceID string) string {
	h := sha256.Sum256([]byte(serviceID))
	return "pg-cold-" + hex.EncodeToString(h[:12])
}
func Required(ctx context.Context, get Get, path string) (map[string]any, error) {
	m, found, err := get(ctx, path)
	if err != nil {
		return nil, err
	}
	if !found || String(Meta(m), "uid") == "" || String(Meta(m), "resourceVersion") == "" || String(Meta(m), "deletionTimestamp") != "" {
		return nil, fmt.Errorf("required live identity unavailable or terminating: %s", path)
	}
	return m, nil
}
func Owned(m map[string]any, uid string) bool {
	for _, ref := range Objects(Meta(m)["ownerReferences"]) {
		if ref["uid"] == uid && ref["kind"] == "Cluster" && ref["apiVersion"] == "postgresql.cnpg.io/v1" {
			return true
		}
	}
	return false
}
func Ready(m map[string]any) bool {
	for _, c := range Objects(Object(m["status"])["conditions"]) {
		if c["type"] == "Ready" && c["status"] == "True" {
			return true
		}
	}
	return false
}
func NodeReady(ctx context.Context, get Get, node string) error {
	if node == "" {
		return fmt.Errorf("node execution state is unknown")
	}
	m, err := Required(ctx, get, "/api/v1/nodes/"+url.PathEscape(node))
	if err != nil {
		return err
	}
	if !Ready(m) {
		return fmt.Errorf("node %s is not freshly Ready; source execution state is unknown", node)
	}
	return nil
}
func PVSpecHash(pv map[string]any) string {
	spec := map[string]any{}
	for k, v := range Object(pv["spec"]) {
		if k != "persistentVolumeReclaimPolicy" {
			spec[k] = v
		}
	}
	return Hash(spec)
}
func Volume(ctx context.Context, get Get, ns string, pvc map[string]any) (model.PostgresSourceRetirementVolume, error) {
	var v model.PostgresSourceRetirementVolume
	v.PVC, v.PVCUID = String(Meta(pvc), "name"), String(Meta(pvc), "uid")
	v.PV = String(Object(pvc["spec"]), "volumeName")
	if v.PVC == "" || v.PVCUID == "" || v.PV == "" || Object(pvc["status"])["phase"] != "Bound" || String(Meta(pvc), "deletionTimestamp") != "" {
		return v, fmt.Errorf("source/target PVC is not an identified non-deleting Bound volume")
	}
	pv, err := Required(ctx, get, "/api/v1/persistentvolumes/"+url.PathEscape(v.PV))
	if err != nil {
		return v, err
	}
	ref := Object(Object(pv["spec"])["claimRef"])
	if ref["uid"] != v.PVCUID || ref["name"] != v.PVC || ref["namespace"] != ns {
		return v, fmt.Errorf("PV/PVC binding identity mismatch")
	}
	v.PVUID = String(Meta(pv), "uid")
	v.PVCSpecHash = Hash(pvc["spec"])
	v.PVSpecHash = PVSpecHash(pv)
	return v, nil
}

// Labels are discovery hints, not authority to mutate storage. An unowned,
// unused label collision is pinned separately and must remain entirely untouched.
func ProtectedVolume(ctx context.Context, get Get, ns string, pvc map[string]any) (model.PostgresSourceRetirementProtectedVolume, error) {
	var protected model.PostgresSourceRetirementProtectedVolume
	if len(Objects(Meta(pvc)["ownerReferences"])) != 0 {
		return protected, fmt.Errorf("label-colliding PVC has an unexpected owner")
	}
	v, err := Volume(ctx, get, ns, pvc)
	if err != nil {
		return protected, err
	}
	pv, err := Required(ctx, get, "/api/v1/persistentvolumes/"+url.PathEscape(v.PV))
	if err != nil {
		return protected, err
	}
	if len(Objects(Meta(pv)["ownerReferences"])) != 0 {
		return protected, fmt.Errorf("protected PV has an unexpected owner")
	}
	configuration := func(obj map[string]any) any {
		m := Meta(obj)
		return []any{m["labels"], m["annotations"], m["ownerReferences"], m["finalizers"]}
	}
	protected.PostgresSourceRetirementVolume = v
	protected.ConfigurationHash = Hash([]any{configuration(pvc), configuration(pv), pv["spec"]})
	return protected, nil
}

func Inspect(ctx context.Context, get Get, app model.App, svc model.BackingService) (model.PostgresSourceRetirementPlan, error) {
	p := model.PostgresSourceRetirementPlan{ServiceID: svc.ID, AppID: app.ID, Namespace: "fg-" + strings.ReplaceAll(svc.TenantID, "_", "-"), ConfigHash: ConfigHash(app, svc), PreserveStorage: true, Pods: []model.PostgresSourceRetirementPod{}, Volumes: []model.PostgresSourceRetirementVolume{}}
	if svc.Spec.Postgres == nil || svc.Spec.Postgres.Suspended || svc.Spec.Postgres.Instances != 1 {
		return p, fmt.Errorf("retirement requires an active single-instance destination")
	}
	cm, err := Required(ctx, get, Path(p.Namespace, "configmaps", MigrationName(svc.ID)))
	if err != nil {
		return p, err
	}
	var st struct {
		Version      int    `json:"version"`
		ServiceID    string `json:"service_id"`
		Phase        string `json:"phase"`
		SourceName   string `json:"source_name"`
		SourceUID    string `json:"source_uid"`
		SourcePVC    string `json:"source_pvc"`
		SourcePVCUID string `json:"source_pvc_uid"`
		SourcePV     string `json:"source_pv"`
		TargetName   string `json:"target_name"`
		TargetUID    string `json:"target_uid"`
		Endpoint     string `json:"endpoint"`
		SystemID     string `json:"system_id"`
	}
	if json.Unmarshal([]byte(String(Object(cm["data"]), "state.json")), &st) != nil || st.Version != 1 || st.Phase != "complete" || st.ServiceID != svc.ID || st.SourceName == "" || st.SourceUID == "" || st.TargetUID == "" || st.TargetUID == st.SourceUID || st.SourceName == st.TargetName || st.TargetName != svc.Spec.Postgres.ServiceName || st.Endpoint != model.PostgresEndpointName(*svc.Spec.Postgres) || st.SystemID == "" || st.SourcePVC == "" || st.SourcePVCUID == "" || st.SourcePV == "" {
		return p, fmt.Errorf("no exact completed cold migration matches the current database service")
	}
	p.MigrationUID, p.MigrationHash = String(Meta(cm), "uid"), Hash(cm["data"])
	p.SourceCluster, p.SourceUID, p.TargetCluster, p.TargetUID, p.Endpoint, p.SystemID = st.SourceName, st.SourceUID, st.TargetName, st.TargetUID, st.Endpoint, st.SystemID
	source, err := Required(ctx, get, Path(p.Namespace, "clusters", p.SourceCluster))
	if err != nil {
		return p, err
	}
	if String(Meta(source), "uid") != p.SourceUID || Object(Meta(source)["annotations"])["fugue.pro/cold-migration"] != MigrationName(svc.ID) || Object(Meta(source)["labels"])["fugue.pro/backing-service-id"] != svc.ID || Object(Meta(source)["labels"])["fugue.pro/tenant-id"] != svc.TenantID {
		return p, fmt.Errorf("source Cluster ownership/UID mismatch")
	}
	var fence []string
	if json.Unmarshal([]byte(String(Object(Meta(source)["annotations"]), "cnpg.io/fencedInstances")), &fence) != nil || len(fence) != 1 || fence[0] != "*" {
		return p, fmt.Errorf("completed migration source is not explicitly fenced")
	}
	p.SourceSpecHash = Hash(source["spec"])
	target, err := Required(ctx, get, Path(p.Namespace, "clusters", p.TargetCluster))
	if err != nil {
		return p, err
	}
	if String(Meta(target), "uid") != p.TargetUID || Object(Meta(target)["labels"])["fugue.pro/backing-service-id"] != svc.ID || !Ready(target) || String(Object(target["status"]), "systemID") != p.SystemID {
		return p, fmt.Errorf("destination identity, readiness or system ID differs")
	}
	p.TargetSpecHash = Hash(target["spec"])
	p.TargetPod = String(Object(target["status"]), "currentPrimary")
	pod, err := Required(ctx, get, Path(p.Namespace, "pods", p.TargetPod))
	if err != nil {
		return p, err
	}
	if !Owned(pod, p.TargetUID) || !Ready(pod) || Object(pod["status"])["phase"] != "Running" {
		return p, fmt.Errorf("destination primary is not Ready or correctly owned")
	}
	p.TargetPodUID, p.TargetNode = String(Meta(pod), "uid"), String(Object(pod["spec"]), "nodeName")
	if err = NodeReady(ctx, get, p.TargetNode); err != nil {
		return p, err
	}
	ep, err := Required(ctx, get, Path(p.Namespace, "services", p.Endpoint))
	if err != nil {
		return p, err
	}
	selector := Object(Object(ep["spec"])["selector"])
	if len(selector) != 2 || selector[ClusterLabel] != p.TargetCluster || selector["cnpg.io/instanceRole"] != "primary" || Owned(ep, p.SourceUID) {
		return p, fmt.Errorf("stable endpoint does not exclusively select the destination or is owned by the source")
	}
	p.EndpointUID, p.EndpointSpecHash = String(Meta(ep), "uid"), Hash(ep["spec"])
	claims, err := RequiredList(ctx, get, Path(p.Namespace, "persistentvolumeclaims", ""))
	if err != nil {
		return p, err
	}
	primaryFound := false
	for _, pvc := range claims {
		labels := Object(Meta(pvc)["labels"])
		if !Owned(pvc, p.SourceUID) && labels[ClusterLabel] != p.SourceCluster {
			continue
		}
		if !Owned(pvc, p.SourceUID) && len(Objects(Meta(pvc)["ownerReferences"])) == 0 && String(Meta(pvc), "name") != st.SourcePVC {
			v, e := ProtectedVolume(ctx, get, p.Namespace, pvc)
			if e != nil {
				return p, e
			}
			p.ProtectedVolumes = append(p.ProtectedVolumes, v)
			continue
		}
		if !Owned(pvc, p.SourceUID) || len(Objects(Meta(pvc)["ownerReferences"])) != 1 || labels[ClusterLabel] != p.SourceCluster {
			return p, fmt.Errorf("source PVC discovery/ownership mismatch")
		}
		v, e := Volume(ctx, get, p.Namespace, pvc)
		if e != nil {
			return p, e
		}
		if v.PVC == st.SourcePVC {
			if v.PVCUID != st.SourcePVCUID || v.PV != st.SourcePV {
				return p, fmt.Errorf("migration source storage identity changed")
			}
			primaryFound = true
		}
		p.Volumes = append(p.Volumes, v)
	}
	if !primaryFound || len(p.Volumes) == 0 {
		return p, fmt.Errorf("recorded source storage was not found")
	}
	pods, err := RequiredList(ctx, get, Path(p.Namespace, "pods", ""))
	if err != nil {
		return p, err
	}
	for _, pod := range pods {
		if Object(Meta(pod)["labels"])[ClusterLabel] != p.SourceCluster && !Owned(pod, p.SourceUID) {
			continue
		}
		if !Owned(pod, p.SourceUID) || String(Meta(pod), "uid") == "" || String(Meta(pod), "deletionTimestamp") != "" {
			return p, fmt.Errorf("unexpected or terminating source Pod; source retirement supports exact instance Pods only")
		}
		node := String(Object(pod["spec"]), "nodeName")
		if node != "" {
			if err = NodeReady(ctx, get, node); err != nil {
				return p, err
			}
		} else if Object(pod["status"])["phase"] != "Pending" {
			return p, fmt.Errorf("source Pod execution state is unknown")
		}
		p.Pods = append(p.Pods, model.PostgresSourceRetirementPod{Name: String(Meta(pod), "name"), UID: String(Meta(pod), "uid"), Node: node})
	}
	sort.Slice(p.Pods, func(i, j int) bool { return p.Pods[i].Name < p.Pods[j].Name })
	sort.Slice(p.Volumes, func(i, j int) bool { return p.Volumes[i].PVC < p.Volumes[j].PVC })
	sort.Slice(p.ProtectedVolumes, func(i, j int) bool { return p.ProtectedVolumes[i].PVC < p.ProtectedVolumes[j].PVC })
	p.PlanHash = PlanHash(p)
	if err = Validate(ctx, get, p, false); err != nil {
		return p, err
	}
	return p, nil
}

func RequiredList(ctx context.Context, get Get, path string) ([]map[string]any, error) {
	m, found, err := get(ctx, path)
	if err != nil {
		return nil, err
	}
	if !found || m["items"] == nil || String(Object(m["metadata"]), "continue") != "" {
		return nil, fmt.Errorf("complete inventory unavailable: %s", path)
	}
	return Objects(m["items"]), nil
}

// Validate is repeated before each mutation. Progress permits only the exact
// witnessed source objects to disappear or their PVC retention metadata to change.
func Validate(ctx context.Context, get Get, p model.PostgresSourceRetirementPlan, progress bool) error {
	if !p.PreserveStorage || p.PlanHash == "" || p.PlanHash != PlanHash(p) || p.SourceUID == p.TargetUID || p.SourceCluster == p.TargetCluster || len(p.Volumes) == 0 {
		return fmt.Errorf("invalid retirement witness")
	}
	cm, err := Required(ctx, get, Path(p.Namespace, "configmaps", MigrationName(p.ServiceID)))
	if err != nil {
		return err
	}
	if String(Meta(cm), "uid") != p.MigrationUID || Hash(cm["data"]) != p.MigrationHash {
		return fmt.Errorf("completed migration record changed")
	}
	target, err := Required(ctx, get, Path(p.Namespace, "clusters", p.TargetCluster))
	if err != nil {
		return err
	}
	if String(Meta(target), "uid") != p.TargetUID || Hash(target["spec"]) != p.TargetSpecHash || !Ready(target) || String(Object(target["status"]), "systemID") != p.SystemID || String(Object(target["status"]), "currentPrimary") != p.TargetPod {
		return fmt.Errorf("destination cluster changed or is unhealthy")
	}
	pod, err := Required(ctx, get, Path(p.Namespace, "pods", p.TargetPod))
	if err != nil {
		return err
	}
	if String(Meta(pod), "uid") != p.TargetPodUID || !Owned(pod, p.TargetUID) || !Ready(pod) || String(Object(pod["spec"]), "nodeName") != p.TargetNode {
		return fmt.Errorf("destination primary changed or is unhealthy")
	}
	if err = NodeReady(ctx, get, p.TargetNode); err != nil {
		return err
	}
	ep, err := Required(ctx, get, Path(p.Namespace, "services", p.Endpoint))
	if err != nil {
		return err
	}
	if String(Meta(ep), "uid") != p.EndpointUID || Hash(ep["spec"]) != p.EndpointSpecHash || Owned(ep, p.SourceUID) {
		return fmt.Errorf("stable destination Service changed")
	}
	endpoints, err := Required(ctx, get, Path(p.Namespace, "endpoints", p.Endpoint))
	if err != nil {
		return err
	}
	addresses := 0
	for _, subset := range Objects(endpoints["subsets"]) {
		for _, a := range Objects(subset["addresses"]) {
			ref := Object(a["targetRef"])
			if ref["name"] != p.TargetPod || ref["uid"] != p.TargetPodUID || a["ip"] != Object(pod["status"])["podIP"] {
				return fmt.Errorf("stable endpoint has an unexpected backend")
			}
			addresses++
		}
	}
	if addresses == 0 {
		return fmt.Errorf("stable destination Service has no ready backend")
	}
	source, found, err := get(ctx, Path(p.Namespace, "clusters", p.SourceCluster))
	if err != nil {
		return err
	}
	if (!found && !progress) || (found && (String(Meta(source), "uid") != p.SourceUID || Hash(source["spec"]) != p.SourceSpecHash)) {
		return fmt.Errorf("source cluster identity changed")
	}
	if found && (Object(Meta(source)["annotations"])["cnpg.io/fencedInstances"] != `["*"]` || Object(Meta(source)["annotations"])["fugue.pro/cold-migration"] != MigrationName(p.ServiceID)) {
		return fmt.Errorf("source fencing or migration identity changed")
	}
	for _, v := range p.Volumes {
		pvc, err := Required(ctx, get, Path(p.Namespace, "persistentvolumeclaims", v.PVC))
		if err != nil {
			return err
		}
		got, err := Volume(ctx, get, p.Namespace, pvc)
		if err != nil {
			return err
		}
		if got != v {
			return fmt.Errorf("source volume identity or specification changed")
		}
		retained := Object(Meta(pvc)["annotations"])[RetainedUIDAnnotation] == p.SourceUID && Object(Meta(pvc)["annotations"])[RetainedPlanAnnotation] == p.PlanHash
		owned := Owned(pvc, p.SourceUID) && len(Objects(Meta(pvc)["ownerReferences"])) == 1
		if !owned && !(progress && retained && len(Objects(Meta(pvc)["ownerReferences"])) == 0) {
			return fmt.Errorf("source PVC lost its identified ownership")
		}
	}
	for _, protected := range p.ProtectedVolumes {
		pvc, err := Required(ctx, get, Path(p.Namespace, "persistentvolumeclaims", protected.PVC))
		if err != nil {
			return err
		}
		got, err := ProtectedVolume(ctx, get, p.Namespace, pvc)
		if err != nil || got != protected || Object(Meta(pvc)["labels"])[ClusterLabel] != p.SourceCluster {
			return fmt.Errorf("protected unowned storage identity/configuration changed")
		}
	}
	claims, err := RequiredList(ctx, get, Path(p.Namespace, "persistentvolumeclaims", ""))
	if err != nil {
		return err
	}
	for _, claim := range claims {
		if !Owned(claim, p.SourceUID) && Object(Meta(claim)["labels"])[ClusterLabel] != p.SourceCluster {
			continue
		}
		known := false
		for _, v := range p.Volumes {
			known = known || String(Meta(claim), "uid") == v.PVCUID
		}
		for _, v := range p.ProtectedVolumes {
			known = known || String(Meta(claim), "uid") == v.PVCUID
		}
		if !known {
			return fmt.Errorf("unexpected source PVC appeared after planning")
		}
	}
	pods, err := RequiredList(ctx, get, Path(p.Namespace, "pods", ""))
	if err != nil {
		return err
	}
	for _, pod := range pods {
		known := false
		for _, id := range p.Pods {
			if String(Meta(pod), "name") == id.Name {
				if String(Meta(pod), "uid") != id.UID || String(Object(pod["spec"]), "nodeName") != id.Node {
					return fmt.Errorf("source Pod identity changed")
				}
				owners := Objects(Meta(pod)["ownerReferences"])
				if !(Owned(pod, p.SourceUID) && len(owners) == 1) && !(progress && !found && len(owners) == 0) {
					return fmt.Errorf("source Pod ownership changed")
				}
				spec := Object(pod["spec"])
				containers := Objects(spec["containers"])
				if len(containers) != 1 || containers[0]["name"] != "postgres" || len(Objects(spec["ephemeralContainers"])) > 0 {
					return fmt.Errorf("source Pod has containers outside the stopped-state proof")
				}
				for _, init := range Objects(spec["initContainers"]) {
					if init["restartPolicy"] == "Always" {
						return fmt.Errorf("source Pod has a native sidecar outside the stopped-state proof")
					}
				}
				known = true
				if id.Node != "" {
					if err = NodeReady(ctx, get, id.Node); err != nil {
						return err
					}
				}
			}
		}
		uses := false
		for _, volume := range Objects(Object(pod["spec"])["volumes"]) {
			for _, v := range p.ProtectedVolumes {
				if Object(volume["persistentVolumeClaim"])["claimName"] == v.PVC {
					return fmt.Errorf("protected unowned storage is referenced by a Pod")
				}
			}
			for _, v := range p.Volumes {
				uses = uses || Object(volume["persistentVolumeClaim"])["claimName"] == v.PVC
			}
		}
		if (uses || Owned(pod, p.SourceUID) || Object(Meta(pod)["labels"])[ClusterLabel] == p.SourceCluster) && !known {
			return fmt.Errorf("unexpected Pod references source storage/cluster")
		}
	}
	jobs, err := RequiredList(ctx, get, "/apis/batch/v1/namespaces/"+url.PathEscape(p.Namespace)+"/jobs")
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if Owned(job, p.SourceUID) || Object(Meta(job)["labels"])[ClusterLabel] == p.SourceCluster {
			return fmt.Errorf("source Cluster still has a Job")
		}
		for _, volume := range Objects(Object(Object(Object(job["spec"])["template"])["spec"])["volumes"]) {
			for _, v := range p.ProtectedVolumes {
				if Object(volume["persistentVolumeClaim"])["claimName"] == v.PVC {
					return fmt.Errorf("protected unowned storage is referenced by a Job")
				}
			}
			for _, v := range p.Volumes {
				if Object(volume["persistentVolumeClaim"])["claimName"] == v.PVC {
					return fmt.Errorf("Job still references retained source storage")
				}
			}
		}
	}
	return nil
}
