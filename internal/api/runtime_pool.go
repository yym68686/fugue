package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func (s *Server) handleSetRuntimePoolMode(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can manage runtime pool mode")
		return
	}

	runtimeObj, err := s.store.GetRuntime(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if runtimeObj.Type != model.RuntimeTypeManagedOwned || runtimeObj.TenantID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "only managed-owned runtimes can contribute to the shared pool")
		return
	}

	var req struct {
		PoolMode string `json:"pool_mode"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	switch strings.TrimSpace(req.PoolMode) {
	case model.RuntimePoolModeDedicated, model.RuntimePoolModeInternalShared:
	default:
		httpx.WriteError(w, http.StatusBadRequest, "invalid pool_mode")
		return
	}

	desiredRuntime := runtimeObj
	desiredRuntime.PoolMode = model.NormalizeRuntimePoolMode(runtimeObj.Type, req.PoolMode)

	nodeReconciled, err := s.reconcileRuntimeClusterNode(r.Context(), desiredRuntime)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	updatedRuntime, err := s.store.SetRuntimePoolMode(runtimeObj.ID, desiredRuntime.PoolMode)
	if err != nil {
		if nodeReconciled {
			if _, revertErr := s.reconcileRuntimeClusterNode(r.Context(), runtimeObj); revertErr != nil && s.log != nil {
				s.log.Printf("revert runtime pool mode node reconciliation for %s: %v", runtimeObj.ID, revertErr)
			}
		}
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "runtime.pool.mode", "runtime", updatedRuntime.ID, updatedRuntime.TenantID, map[string]string{
		"pool_mode": updatedRuntime.PoolMode,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"runtime":         updatedRuntime,
		"node_reconciled": nodeReconciled,
	})
}

func (s *Server) reconcileRuntimeClusterNode(ctx context.Context, runtimeObj model.Runtime) (bool, error) {
	nodeName := strings.TrimSpace(runtimeObj.ClusterNodeName)
	if nodeName == "" {
		return false, nil
	}

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return false, err
	}
	return client.reconcileRuntimeNode(ctx, runtimeObj)
}

func (c *clusterNodeClient) reconcileRuntimeNode(ctx context.Context, runtimeObj model.Runtime) (bool, error) {
	nodeName := strings.TrimSpace(runtimeObj.ClusterNodeName)
	if nodeName == "" {
		return false, nil
	}

	node, err := c.getNode(ctx, nodeName)
	if err != nil {
		if isKubernetesNodeNotFound(err) {
			return false, nil
		}
		return false, err
	}

	patch, changed := buildRuntimeNodeMergePatch(node, runtimeObj)
	if !changed {
		return true, nil
	}
	if err := c.patchNode(ctx, nodeName, patch); err != nil {
		if isKubernetesNodeNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *clusterNodeClient) getNode(ctx context.Context, nodeName string) (kubeNode, error) {
	var node kubeNode
	apiPath := "/api/v1/nodes/" + url.PathEscape(strings.TrimSpace(nodeName))
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &node); err != nil {
		return kubeNode{}, err
	}
	return node, nil
}

func (c *clusterNodeClient) patchNode(ctx context.Context, nodeName string, patch map[string]any) error {
	raw, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("encode kubernetes node patch: %w", err)
	}

	apiPath := "/api/v1/nodes/" + url.PathEscape(strings.TrimSpace(nodeName))
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.baseURL+apiPath, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/merge-patch+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("kubernetes request %s %s: %w", http.MethodPatch, apiPath, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("kubernetes request %s %s failed: status=%d body=%s", http.MethodPatch, apiPath, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func buildRuntimeNodeMergePatch(node kubeNode, runtimeObj model.Runtime) (map[string]any, bool) {
	labelsPatch, labelsChanged := buildRuntimeNodeLabelsPatch(node.Metadata.Labels, runtimeObj)
	taints, taintsChanged := buildRuntimeNodeTaints(node.Spec.Taints, runtimeObj)
	if !labelsChanged && !taintsChanged {
		return nil, false
	}

	patch := map[string]any{}
	if labelsChanged {
		patch["metadata"] = map[string]any{"labels": labelsPatch}
	}
	if taintsChanged {
		patch["spec"] = map[string]any{"taints": taints}
	}
	return patch, true
}

func buildRuntimeNodeLabelsPatch(current map[string]string, runtimeObj model.Runtime) (map[string]any, bool) {
	desired := runtimepkg.JoinNodeLabelMap(runtimeObj)
	patch := map[string]any{}
	changed := false
	for _, key := range managedRuntimeNodeLabelKeys() {
		currentValue, hasCurrent := "", false
		if current != nil {
			currentValue, hasCurrent = current[key]
		}
		desiredValue, hasDesired := desired[key]
		switch {
		case hasDesired:
			patch[key] = desiredValue
			if !hasCurrent || currentValue != desiredValue {
				changed = true
			}
		case hasCurrent:
			patch[key] = nil
			changed = true
		}
	}
	return patch, changed
}

func buildRuntimeNodeTaints(current []kubeNodeTaint, runtimeObj model.Runtime) ([]kubeNodeTaint, bool) {
	normalizedCurrent := normalizeKubeNodeTaints(current)
	desiredTenantTaint, hasDesiredTenantTaint := desiredRuntimeNodeTenantTaint(runtimeObj)

	next := make([]kubeNodeTaint, 0, len(normalizedCurrent)+1)
	desiredPresent := false
	for _, taint := range normalizedCurrent {
		if taint.Key != runtimepkg.TenantTaintKey {
			next = append(next, taint)
			continue
		}
		if hasDesiredTenantTaint && !desiredPresent && kubeNodeTaintEqual(taint, desiredTenantTaint) {
			next = append(next, desiredTenantTaint)
			desiredPresent = true
		}
	}
	if hasDesiredTenantTaint && !desiredPresent {
		next = append(next, desiredTenantTaint)
	}

	return next, !kubeNodeTaintSlicesEqual(normalizedCurrent, next)
}

func desiredRuntimeNodeTenantTaint(runtimeObj model.Runtime) (kubeNodeTaint, bool) {
	if strings.TrimSpace(runtimeObj.TenantID) == "" || model.NormalizeRuntimePoolMode(runtimeObj.Type, runtimeObj.PoolMode) == model.RuntimePoolModeInternalShared {
		return kubeNodeTaint{}, false
	}
	return kubeNodeTaint{
		Key:    runtimepkg.TenantTaintKey,
		Value:  runtimeObj.TenantID,
		Effect: "NoSchedule",
	}, true
}

func normalizeKubeNodeTaints(in []kubeNodeTaint) []kubeNodeTaint {
	if len(in) == 0 {
		return nil
	}
	out := make([]kubeNodeTaint, 0, len(in))
	for _, taint := range in {
		normalized := kubeNodeTaint{
			Key:    strings.TrimSpace(taint.Key),
			Value:  strings.TrimSpace(taint.Value),
			Effect: strings.TrimSpace(taint.Effect),
		}
		if normalized.Key == "" {
			continue
		}
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func kubeNodeTaintEqual(left, right kubeNodeTaint) bool {
	return left.Key == right.Key && left.Value == right.Value && left.Effect == right.Effect
}

func kubeNodeTaintSlicesEqual(left, right []kubeNodeTaint) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !kubeNodeTaintEqual(left[index], right[index]) {
			return false
		}
	}
	return true
}

func managedRuntimeNodeLabelKeys() []string {
	return []string{
		runtimepkg.RuntimeIDLabelKey,
		runtimepkg.TenantIDLabelKey,
		runtimepkg.NodeModeLabelKey,
		runtimepkg.SharedPoolLabelKey,
	}
}

func isKubernetesNodeNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "status=404")
}
