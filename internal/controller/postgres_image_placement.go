package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"

	corev1 "k8s.io/api/core/v1"
)

// A location-shared runtime is a logical pool, not a Node runtime-id label.
// The existing hard hostname pin (as emitted by Fugue) is what prevents the
// restart from changing placement. Current membership alone cannot prove this.
func (c *kubeClient) imageMigrationSharedNode(ctx context.Context, placement model.Runtime, postgres model.AppPostgresSpec, spec map[string]any, name, expectedUID string) (kubeNode, error) {
	if placement.Type != model.RuntimeTypeManagedShared || placement.ID != postgres.RuntimeID || placement.Status != model.RuntimeStatusActive {
		return kubeNode{}, fmt.Errorf("image migration requires an active managed shared runtime for a hostname pin")
	}
	if name == "" || (strings.TrimSpace(postgres.PrimaryNodeName) != "" && strings.TrimSpace(postgres.PrimaryNodeName) != name) {
		return kubeNode{}, fmt.Errorf("image migration primary node differs from the configured shared placement")
	}
	var nodes kubeNodeList
	query := url.Values{"labelSelector": {kubeHostnameLabelKey + "=" + name}}
	if _, err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes?"+query.Encode(), nil, &nodes); err != nil {
		return kubeNode{}, err
	}
	if len(nodes.Items) != 1 || nodes.Items[0].Metadata.Name != name || nodes.Items[0].Metadata.Labels[kubeHostnameLabelKey] != name {
		return kubeNode{}, fmt.Errorf("image migration hostname %q does not uniquely select the current primary node", name)
	}
	node := nodes.Items[0]
	if node.Metadata.UID == "" || (expectedUID != "" && expectedUID != node.Metadata.UID) {
		return kubeNode{}, fmt.Errorf("image migration shared primary node identity is missing or changed")
	}
	for key, value := range runtime.ManagedSharedNodeSelector(placement) {
		if node.Metadata.Labels[key] != value {
			return kubeNode{}, fmt.Errorf("image migration node %q is outside the authorized shared runtime %q (selector %s mismatch)", name, placement.ID, key)
		}
	}
	if !imageMigrationSharedHostPinned(spec, node) {
		return kubeNode{}, fmt.Errorf("image migration requires existing hard affinity that allows only the current shared primary hostname %q", name)
	}
	return node, nil
}

func imageMigrationSharedHostPinned(spec map[string]any, node kubeNode) bool {
	var affinity struct {
		NodeSelector map[string]string    `json:"nodeSelector"`
		NodeAffinity *corev1.NodeAffinity `json:"nodeAffinity"`
	}
	if !decodeImagePlacement(spec["affinity"], &affinity) {
		return false
	}
	for key, value := range affinity.NodeSelector {
		if label, found := node.Metadata.Labels[key]; !found || label != value {
			return false
		}
	}
	hostPinned := affinity.NodeSelector[kubeHostnameLabelKey] == node.Metadata.Name
	if affinity.NodeAffinity == nil || affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return hostPinned
	}
	required := *affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	return imageMigrationNodeSelectorMatches(node, required) &&
		(hostPinned || imageMigrationTermsPinNode(required, node.Metadata.Name, kubeHostnameLabelKey))
}

func imageMigrationTermsPinNode(selector corev1.NodeSelector, name string, keys ...string) bool {
	// Every OR branch must have a single-value hard pin; preferred affinity
	// and other labels that happen to match only one Node today are not pins.
	for _, term := range selector.NodeSelectorTerms {
		pinned := false
		for _, expr := range term.MatchExpressions {
			for _, key := range keys {
				if expr.Key == key && expr.Operator == corev1.NodeSelectorOpIn && len(expr.Values) == 1 && expr.Values[0] == name {
					pinned = true
				}
			}
		}
		if !pinned {
			return false
		}
	}
	return len(selector.NodeSelectorTerms) > 0
}

func validateImageMigrationNode(node kubeNode, found bool) error {
	if !found || !kubeNodeReady(node) {
		return fmt.Errorf("image migration requires a Ready database node")
	}
	if node.Spec.Unschedulable {
		return fmt.Errorf("database node is cordoned")
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect == "NoSchedule" || taint.Effect == "NoExecute" {
			return fmt.Errorf("database node has a scheduling/execution taint; migration requires a stable node")
		}
	}
	return nil
}

func validateImageMigrationVolumeNode(pvSpec map[string]any, name string, node kubeNode) error {
	var affinity corev1.VolumeNodeAffinity
	if !decodeImagePlacement(pvSpec["nodeAffinity"], &affinity) {
		return fmt.Errorf("cannot verify PV %s node affinity", name)
	}
	if affinity.Required != nil && !imageMigrationNodeSelectorMatches(node, *affinity.Required) {
		return fmt.Errorf("PV %s node affinity excludes the original database node %q", name, node.Metadata.Name)
	}
	local := pvSpec["local"] != nil || pvSpec["hostPath"] != nil || imageString(imageObject(pvSpec["csi"]), "driver") == "local.csi.openebs.io"
	if local && (affinity.Required == nil || !imageMigrationTermsPinNode(*affinity.Required, node.Metadata.Name, kubeHostnameLabelKey, "openebs.io/nodename")) {
		return fmt.Errorf("local PV %s is not pinned to the original database node %q", name, node.Metadata.Name)
	}
	return nil
}

func imageMigrationNodeSelectorMatches(node kubeNode, selector corev1.NodeSelector) bool {
	// Unlike label selectors, the scheduler only supports single-value In
	// and NotIn on metadata.name in node-affinity matchFields.
	for _, term := range selector.NodeSelectorTerms {
		for _, field := range term.MatchFields {
			if field.Key != "metadata.name" || len(field.Values) != 1 || (field.Operator != corev1.NodeSelectorOpIn && field.Operator != corev1.NodeSelectorOpNotIn) {
				return false
			}
		}
	}
	return storageNodeSelectorMatches(node, selector)
}

func decodeImagePlacement(raw any, out any) bool {
	b, err := json.Marshal(raw)
	return err == nil && json.Unmarshal(b, out) == nil
}
