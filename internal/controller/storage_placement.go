package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

const longhornCSIDriver = "driver.longhorn.io"

// These are observations scoped to one preflight, never a new source of
// serving intent. A fresh preflight is required at the child-object boundary.
type storagePlacement struct {
	claims        []storagePlacementClaim
	csiNodes      map[string]map[string]bool
	longhornNodes map[string]bool
}

type storagePlacementClaim struct {
	name          string
	driver        string
	pv            *corev1.PersistentVolume
	class         storagev1.StorageClass
	selectedNode  string
	attachedNodes map[string]bool
	singleNode    bool
}

type longhornPlacementVolume struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		NodeID string `json:"nodeID"`
	} `json:"spec"`
	Status struct {
		CurrentNodeID string `json:"currentNodeID"`
		State         string `json:"state"`
		Robustness    string `json:"robustness"`
	} `json:"status"`
}

type storageClaimRequest struct {
	Name  string
	Class string
}

// Read the actual rendered mounts, including legacy workspaces and shared
// claims. Do not assume that every workload will forever have exactly one PVC.
func appStorageClaimRequests(objects []map[string]any) ([]storageClaimRequest, error) {
	classes := map[string]string{}
	for _, obj := range objects {
		if objectStringField(obj, "kind") != "PersistentVolumeClaim" {
			continue
		}
		var pvc corev1.PersistentVolumeClaim
		data, err := json.Marshal(obj)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &pvc); err != nil {
			return nil, err
		}
		if pvc.Spec.StorageClassName != nil {
			classes[pvc.Name] = *pvc.Spec.StorageClassName
		}
	}
	names := map[string]bool{}
	for _, obj := range objects {
		if objectStringField(obj, "kind") != "Deployment" {
			continue
		}
		var workload struct {
			Spec struct {
				Template struct {
					Spec corev1.PodSpec `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		data, err := json.Marshal(obj)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &workload); err != nil {
			return nil, err
		}
		for _, volume := range workload.Spec.Template.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil {
				names[volume.PersistentVolumeClaim.ClaimName] = true
			}
		}
	}
	out := make([]storageClaimRequest, 0, len(names))
	for name := range names {
		out = append(out, storageClaimRequest{Name: name, Class: classes[name]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func loadStoragePlacement(ctx context.Context, client *kubeClient, namespace string, requests []storageClaimRequest) (*storagePlacement, error) {
	p := &storagePlacement{}
	classes := map[string]storagev1.StorageClass{}
	needsCSI, needsLonghorn, hasBoundSingleNode := false, false, false
	for _, request := range requests {
		var pvc corev1.PersistentVolumeClaim
		status, err := client.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+url.PathEscape(namespace)+"/persistentvolumeclaims/"+url.PathEscape(request.Name), nil, &pvc)
		if err != nil && status != http.StatusNotFound {
			return nil, fmt.Errorf("observe storage claim %s/%s: %w", namespace, request.Name, err)
		}
		if pvc.DeletionTimestamp != nil {
			return nil, fmt.Errorf("storage claim %s/%s is being deleted", namespace, request.Name)
		}
		className := request.Class
		if pvc.Spec.StorageClassName != nil {
			if className != "" && className != *pvc.Spec.StorageClassName {
				return nil, fmt.Errorf("storage claim %s is already assigned to class %q, requested %q", request.Name, *pvc.Spec.StorageClassName, className)
			}
			className = *pvc.Spec.StorageClassName
		}
		claim := storagePlacementClaim{name: request.Name, selectedNode: pvc.Annotations[pvcSelectedNodeAnnotation], singleNode: true, attachedNodes: map[string]bool{}}
		for _, mode := range pvc.Spec.AccessModes {
			if mode == corev1.ReadWriteMany || mode == corev1.ReadOnlyMany {
				claim.singleNode = false
			}
		}
		if pvc.Spec.VolumeName != "" {
			var pv corev1.PersistentVolume
			if _, err := client.doJSON(ctx, http.MethodGet, "/api/v1/persistentvolumes/"+url.PathEscape(pvc.Spec.VolumeName), nil, &pv); err != nil {
				return nil, fmt.Errorf("observe bound volume %s: %w", pvc.Spec.VolumeName, err)
			}
			if pv.DeletionTimestamp != nil || (pv.Spec.ClaimRef != nil && (pv.Spec.ClaimRef.Name != request.Name || pv.Spec.ClaimRef.Namespace != namespace || (pvc.UID != "" && pv.Spec.ClaimRef.UID != pvc.UID))) {
				return nil, fmt.Errorf("bound volume %s has a changed claim identity or is being deleted", pvc.Spec.VolumeName)
			}
			claim.pv = &pv
			if pv.Spec.CSI != nil {
				claim.driver = pv.Spec.CSI.Driver
			}
			if className == "" {
				className = pv.Spec.StorageClassName
			}
			if claim.singleNode {
				hasBoundSingleNode = true
			}
		}
		if className == "" && claim.pv == nil {
			var list storagev1.StorageClassList
			if _, err := client.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/storageclasses", nil, &list); err != nil {
				return nil, fmt.Errorf("resolve default storage class: %w", err)
			}
			for _, sc := range list.Items {
				if sc.Annotations["storageclass.kubernetes.io/is-default-class"] != "true" && sc.Annotations["storageclass.beta.kubernetes.io/is-default-class"] != "true" {
					continue
				}
				if className != "" {
					return nil, fmt.Errorf("storage claim %s requires an explicit class because multiple defaults exist", request.Name)
				}
				className = sc.Name
				classes[className] = sc
			}
			if className == "" {
				return nil, fmt.Errorf("storage claim %s has no assigned or default storage class", request.Name)
			}
		}
		if className != "" {
			sc, ok := classes[className]
			if !ok {
				if _, err := client.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/storageclasses/"+url.PathEscape(className), nil, &sc); err != nil {
					return nil, fmt.Errorf("observe storage class %s: %w", className, err)
				}
				classes[className] = sc
			}
			claim.class = sc
			if claim.pv == nil && !strings.Contains(sc.Provisioner, "/") {
				// StorageClass provisioners also include non-CSI controllers such
				// as NFS. Only an actual CSI driver requires node registration.
				var driver storagev1.CSIDriver
				status, err := client.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/csidrivers/"+url.PathEscape(sc.Provisioner), nil, &driver)
				if err != nil && status != http.StatusNotFound {
					return nil, fmt.Errorf("observe storage provisioner %s: %w", sc.Provisioner, err)
				}
				if status != http.StatusNotFound || sc.Provisioner == longhornCSIDriver {
					claim.driver = sc.Provisioner
				}
			}
		}
		needsCSI = needsCSI || claim.driver != ""
		needsLonghorn = needsLonghorn || claim.driver == longhornCSIDriver
		p.claims = append(p.claims, claim)
	}
	if needsCSI {
		var list storagev1.CSINodeList
		if _, err := client.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/csinodes", nil, &list); err != nil {
			return nil, fmt.Errorf("observe CSI attachment nodes: %w", err)
		}
		p.csiNodes = map[string]map[string]bool{}
		for _, node := range list.Items {
			drivers := map[string]bool{}
			for _, d := range node.Spec.Drivers {
				drivers[d.Name] = true
			}
			p.csiNodes[node.Name] = drivers
		}
	}
	if needsLonghorn {
		var err error
		p.longhornNodes, err = readyLonghornAttachmentNodes(ctx, client)
		if err != nil {
			return nil, err
		}
		for i := range p.claims {
			c := &p.claims[i]
			if c.driver != longhornCSIDriver || c.pv == nil || c.pv.Spec.CSI == nil {
				continue
			}
			handle := c.pv.Spec.CSI.VolumeHandle
			if handle == "" {
				return nil, fmt.Errorf("Longhorn volume for claim %s has no CSI handle", c.name)
			}
			// Do not assume Longhorn's installation namespace.
			var list struct {
				Items []longhornPlacementVolume `json:"items"`
			}
			path := "/apis/longhorn.io/v1beta2/volumes?fieldSelector=" + url.QueryEscape("metadata.name="+handle)
			if _, err := client.doJSON(ctx, http.MethodGet, path, nil, &list); err != nil {
				return nil, fmt.Errorf("observe Longhorn volume %s: %w", handle, err)
			}
			if len(list.Items) != 1 || list.Items[0].Metadata.Name != handle {
				return nil, fmt.Errorf("Longhorn volume %s is missing or ambiguous", handle)
			}
			volume := list.Items[0]
			if volume.Status.Robustness == "faulted" {
				return nil, fmt.Errorf("Longhorn volume %s is faulted; recovery required", handle)
			}
			if c.singleNode {
				if volume.Status.State != "detached" && volume.Spec.NodeID == "" && volume.Status.CurrentNodeID == "" {
					return nil, fmt.Errorf("Longhorn volume %s has unresolved attachment state %q", handle, volume.Status.State)
				}
				for _, node := range []string{volume.Spec.NodeID, volume.Status.CurrentNodeID} {
					if node != "" {
						c.attachedNodes[node] = true
					}
				}
			}
		}
	}
	if hasBoundSingleNode {
		var attachments storagev1.VolumeAttachmentList
		if _, err := client.doJSON(ctx, http.MethodGet, "/apis/storage.k8s.io/v1/volumeattachments", nil, &attachments); err != nil {
			return nil, fmt.Errorf("observe volume attachment ownership: %w", err)
		}
		pods, err := client.listPodsBySelector(ctx, namespace, "")
		if err != nil {
			return nil, fmt.Errorf("observe volume users: %w", err)
		}
		for i := range p.claims {
			c := &p.claims[i]
			if !c.singleNode || c.pv == nil {
				continue
			}
			for _, a := range attachments.Items {
				// A pending attach request can still acquire the volume after this
				// observation. Require it to be removed before moving a writer.
				if a.Spec.Source.PersistentVolumeName != nil && *a.Spec.Source.PersistentVolumeName == c.pv.Name {
					c.attachedNodes[a.Spec.NodeName] = true
				}
			}
			for _, pod := range pods {
				if managedPostgresPodFinished(pod) || pod.Spec.NodeName == "" || podWaitingForVolumes(pod) {
					continue
				}
				for _, v := range pod.Spec.Volumes {
					if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == c.name {
						c.attachedNodes[pod.Spec.NodeName] = true
					}
				}
			}
		}
	}
	return p, nil
}

// Longhorn's replica/disk scheduling flags do not define the nodes on which
// an existing volume may be attached. A diskless but Ready manager is valid.
func readyLonghornAttachmentNodes(ctx context.Context, client *kubeClient) (map[string]bool, error) {
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Status struct {
				Conditions []kubePodCondition `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if _, err := client.doJSON(ctx, http.MethodGet, "/apis/longhorn.io/v1beta2/nodes", nil, &list); err != nil {
		return nil, fmt.Errorf("observe Longhorn attachment nodes: %w", err)
	}
	out := map[string]bool{}
	for _, n := range list.Items {
		for _, c := range n.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				out[n.Metadata.Name] = true
			}
		}
	}
	return out, nil
}

func (p *storagePlacement) rejection(node kubeNode) string {
	if p == nil {
		return ""
	}
	for _, c := range p.claims {
		prefix := "claim " + c.name + ": "
		if c.pv == nil && c.selectedNode != "" && c.selectedNode != node.Metadata.Name {
			return prefix + "selected-node requires " + c.selectedNode
		}
		if c.pv != nil && c.pv.Spec.NodeAffinity != nil && c.pv.Spec.NodeAffinity.Required != nil && !storageNodeSelectorMatches(node, *c.pv.Spec.NodeAffinity.Required) {
			return prefix + "PV node affinity excludes target"
		}
		if c.pv == nil && len(c.class.AllowedTopologies) > 0 {
			match := false
			for _, term := range c.class.AllowedTopologies {
				ok := len(term.MatchLabelExpressions) > 0
				for _, expr := range term.MatchLabelExpressions {
					found := false
					for _, v := range expr.Values {
						if node.Metadata.Labels[expr.Key] == v {
							found = true
						}
					}
					ok = ok && found
				}
				match = match || ok
			}
			if !match {
				return prefix + "StorageClass allowedTopologies excludes target"
			}
		}
		if c.driver != "" && !p.csiNodes[node.Metadata.Name][c.driver] {
			return prefix + "CSI driver " + c.driver + " is not registered on target"
		}
		if c.driver == longhornCSIDriver && !p.longhornNodes[node.Metadata.Name] {
			return prefix + "target is not a Ready Longhorn attachment node"
		}
		for attached := range c.attachedNodes {
			if attached != node.Metadata.Name {
				return prefix + "volume is attached or in use on " + attached + "; fenced migration required"
			}
		}
	}
	return ""
}

func storageNodeSelectorMatches(node kubeNode, selector corev1.NodeSelector) bool {
	for _, term := range selector.NodeSelectorTerms {
		if len(term.MatchExpressions)+len(term.MatchFields) == 0 {
			continue
		}
		ok := true
		for _, expr := range term.MatchExpressions {
			if !storageLabelRequirementMatches(node.Metadata.Labels, expr) {
				ok = false
				break
			}
		}
		for _, expr := range term.MatchFields {
			if expr.Key != "metadata.name" || !storageLabelRequirementMatches(map[string]string{"metadata.name": node.Metadata.Name}, expr) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func storageLabelRequirementMatches(values map[string]string, expr corev1.NodeSelectorRequirement) bool {
	ops := map[corev1.NodeSelectorOperator]selection.Operator{corev1.NodeSelectorOpIn: selection.In, corev1.NodeSelectorOpNotIn: selection.NotIn, corev1.NodeSelectorOpExists: selection.Exists, corev1.NodeSelectorOpDoesNotExist: selection.DoesNotExist, corev1.NodeSelectorOpGt: selection.GreaterThan, corev1.NodeSelectorOpLt: selection.LessThan}
	op, ok := ops[expr.Operator]
	if !ok {
		return false
	}
	req, err := labels.NewRequirement(expr.Key, op, expr.Values)
	return err == nil && req.Matches(labels.Set(values))
}

func (s *Service) appStoragePlacement(ctx context.Context, client *kubeClient, app model.App, objects []map[string]any) (*storagePlacement, error) {
	if app.Spec.Replicas <= 0 {
		return nil, nil
	}
	requests, err := appStorageClaimRequests(objects)
	if err != nil {
		return nil, err
	}
	if len(requests) == 0 {
		return nil, nil
	}
	return loadStoragePlacement(ctx, client, runtime.NamespaceForTenant(app.TenantID), requests)
}

// Validate at the write boundary as well: online rollout, recovery, and
// rollback must not bypass the storage-aware placement decision.
func (s *Service) validateAppStoragePlacement(ctx context.Context, client *kubeClient, app model.App, scheduling runtime.SchedulingConstraints, objects []map[string]any) error {
	if app.Spec.Replicas <= 0 || (app.Spec.PersistentStorage == nil && app.Spec.Workspace == nil) {
		return nil
	}
	live, found, err := client.getDeployment(ctx, runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(app))
	if err != nil {
		return fmt.Errorf("observe serving workload before storage preflight: %w", err)
	}
	canonicalOnly := true
	for _, object := range objects {
		if objectStringField(object, "kind") == "Deployment" {
			metadata, _ := object["metadata"].(map[string]any)
			if objectStringField(metadata, "name") != runtime.RuntimeAppResourceName(app) {
				canonicalOnly = false
			}
		}
	}
	if canonicalOnly && found && deploymentTargetsExpectedRollout(live, s.expectedManagedAppReleaseKey(s.Renderer.PrepareApp(app), scheduling), strings.TrimSpace(app.Spec.Image)) && managedDeploymentStatusReady(live, app.Spec.Replicas) {
		// No replacement is needed. Missing inventory must not evict an
		// already verified serving release.
		return nil
	}
	storage, err := s.appStoragePlacement(ctx, client, app, objects)
	if err != nil {
		return err
	}
	if storage == nil {
		return nil
	}
	names, err := client.listNodeNames(ctx)
	if err != nil {
		return err
	}
	eligible := 0
	for _, name := range names {
		node, found, err := client.getNode(ctx, name)
		if err != nil {
			return err
		}
		if !found || !nodeLabelsMatchSelector(node.Metadata.Labels, scheduling.NodeSelector) || !kubeNodeReady(node) || node.Spec.Unschedulable || kubeNodeConditionTrue(node.Status.Conditions, "DiskPressure") || !kubeTaintsTolerated(node.Spec.Taints, scheduling.Tolerations) {
			continue
		}
		if reason := storage.rejection(node); reason != "" {
			return fmt.Errorf("storage placement rejected node %s: %s", name, reason)
		}
		eligible++
	}
	if eligible == 0 {
		return fmt.Errorf("storage placement has no eligible node within the requested runtime")
	}
	return nil
}
