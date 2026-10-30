package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Decode only lifecycle and ownership, never executable Pod templates. These
// lists are fresh for each admission; retirement owns every shrink/delete.
type candidateAdmissionObject struct {
	Metadata metav1.ObjectMeta `json:"metadata"`
	Spec     struct {
		Replicas *int `json:"replicas"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration int64  `json:"observedGeneration"`
		Replicas           int    `json:"replicas"`
		ReadyReplicas      int    `json:"readyReplicas"`
		AvailableReplicas  int    `json:"availableReplicas"`
		Phase              string `json:"phase"`
	} `json:"status"`
}

func (s *Service) checkSafeRolloutCandidateAdmission(ctx context.Context, app model.App, currentID string) error {
	if !s.Config.KubectlApply || app.Spec.Replicas <= 0 {
		return nil
	}
	releases, err := s.Store.ListAppReleaseMetadataContext(ctx, model.AppReleaseFilter{TenantID: app.TenantID, AppID: app.ID, IncludeRetired: true})
	if err != nil {
		return fmt.Errorf("read candidate admission state: %w", err)
	}
	other := false
	for _, r := range releases {
		if r.ID != currentID && (r.Role == model.AppReleaseRoleCandidate || store.AppReleaseIsRetired(r)) {
			other = true
			break
		}
	}
	// Preparation can reject an occupied slot before changing canonical traffic
	// or creating a release row. Apply always scans, including orphan resources
	// with no release row. Per-app operation claiming serializes actual creation.
	if !other && currentID == "" {
		return nil
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	ns := url.PathEscape(runtime.NamespaceForTenant(app.TenantID))
	paths := []string{"/apis/apps/v1/namespaces/" + ns + "/deployments", "/apis/apps/v1/namespaces/" + ns + "/replicasets", "/api/v1/namespaces/" + ns + "/pods"}
	var objects [3][]candidateAdmissionObject
	for i, path := range paths {
		var list struct {
			Metadata metav1.ListMeta            `json:"metadata"`
			Items    []candidateAdmissionObject `json:"items"`
		}
		if _, err := client.doJSON(ctx, http.MethodGet, path, nil, &list); err != nil {
			return fmt.Errorf("read candidate admission inventory: %w", err)
		}
		if list.Metadata.Continue != "" {
			return fmt.Errorf("candidate admission inventory is incomplete")
		}
		objects[i] = list.Items
	}
	return candidateAdmissionBlock(app, currentID, releases, objects[0], objects[1], objects[2])
}

func candidateAdmissionBlock(app model.App, currentID string, releases []model.AppRelease, deployments, replicas, pods []candidateAdmissionObject) error {
	protected, candidates, names := map[string]bool{}, map[string]bool{}, map[string]string{}
	for _, r := range releases {
		if r.AppID != app.ID || r.TenantID != app.TenantID {
			continue
		}
		if r.ID == currentID || r.Role == model.AppReleaseRoleStable || r.Role == model.AppReleaseRolePrevious {
			protected[r.ID] = true
			continue
		}
		if r.Role != model.AppReleaseRoleCandidate && !store.AppReleaseIsRetired(r) {
			continue
		}
		candidates[r.ID] = true
		name := r.DeploymentName
		if r.RevisionWorkload != nil {
			name = r.RevisionWorkload.DeploymentName
		} else if name == "" || name == runtime.RuntimeAppResourceName(app) {
			// Legacy failed rows may still refer to the shared canonical alias.
			name = runtime.RuntimeAppResourceNameWithOptions(app, runtime.RenderOptions{Revision: safeRolloutCandidateRevision(r.ID)})
		}
		names[name] = r.ID
	}
	classify := func(m metav1.ObjectMeta) string {
		if m.Namespace != runtime.NamespaceForTenant(app.TenantID) {
			return ""
		}
		id := m.Labels[runtime.FugueLabelAppReleaseID]
		// Rendered role labels do not change when a candidate becomes stable or
		// previous. The durable lifecycle role, not its name, protects it.
		if protected[id] {
			return ""
		}
		if candidates[id] {
			return id
		}
		if m.Labels[runtime.FugueLabelAppID] == app.ID && m.Labels[runtime.FugueLabelTenantID] == app.TenantID &&
			m.Labels[runtime.FugueLabelManagedBy] == runtime.FugueLabelManagedByValue && m.Labels[runtime.FugueLabelAppReleaseRole] == runtime.AppRevisionRoleCandidate {
			if id == "" {
				return "unrecorded candidate"
			}
			return id
		}
		return ""
	}
	blocked := func(id, kind, name string) error {
		return fmt.Errorf("candidate admission blocked: release %s still has %s %s; wait for its replicas and Pods to exit before starting another candidate", id, kind, name)
	}
	deploymentOwners, replicaOwners := map[string]string{}, map[string]string{}
	for _, d := range deployments {
		id := classify(d.Metadata)
		if id == "" && !protected[d.Metadata.Labels[runtime.FugueLabelAppReleaseID]] && d.Metadata.Namespace == runtime.NamespaceForTenant(app.TenantID) {
			id = names[d.Metadata.Name]
		}
		if id == "" {
			continue
		}
		if d.Metadata.UID != "" {
			deploymentOwners[string(d.Metadata.UID)] = id
		}
		if !candidateAdmissionZeroReplicas(d) {
			return blocked(id, "Deployment", d.Metadata.Name)
		}
	}
	for _, rs := range replicas {
		id := classify(rs.Metadata)
		for _, owner := range rs.Metadata.OwnerReferences {
			if owner.Controller != nil && *owner.Controller && owner.Kind == "Deployment" && deploymentOwners[string(owner.UID)] != "" {
				id = deploymentOwners[string(owner.UID)]
			}
		}
		if id == "" {
			continue
		}
		if rs.Metadata.UID != "" {
			replicaOwners[string(rs.Metadata.UID)] = id
		}
		if !candidateAdmissionZeroReplicas(rs) {
			return blocked(id, "ReplicaSet", rs.Metadata.Name)
		}
	}
	for _, pod := range pods {
		id := classify(pod.Metadata)
		for _, owner := range pod.Metadata.OwnerReferences {
			if owner.Controller != nil && *owner.Controller && owner.Kind == "ReplicaSet" && replicaOwners[string(owner.UID)] != "" {
				id = replicaOwners[string(owner.UID)]
			}
		}
		if id != "" && pod.Status.Phase != "Succeeded" && pod.Status.Phase != "Failed" {
			return blocked(id, "Pod", pod.Metadata.Name)
		}
	}
	return nil
}

func candidateAdmissionZeroReplicas(obj candidateAdmissionObject) bool {
	return obj.Spec.Replicas != nil && *obj.Spec.Replicas == 0 && obj.Metadata.Generation > 0 &&
		obj.Status.ObservedGeneration >= obj.Metadata.Generation && obj.Status.Replicas == 0 &&
		obj.Status.ReadyReplicas == 0 && obj.Status.AvailableReplicas == 0
}
