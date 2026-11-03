package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

const appFailoverSourceEvidence = "app_failover_source_identity"

type appFailoverSource struct {
	RuntimeID string            `json:"runtime_id"`
	Nodes     map[string]string `json:"nodes"` // Node name -> immutable UID.
}

// Capture before scaling down, and retain across operation recovery. A Lease,
// a zero replica count, or an absent API Pod is not physical node fencing.
func (s *Service) observeAppFailoverSource(ctx context.Context, c *kubeClient, app model.App, source string) (appFailoverSource, error) {
	proof := appFailoverSource{RuntimeID: source, Nodes: map[string]string{}}
	rt, err := s.Store.GetRuntime(source)
	if err != nil {
		return proof, err
	}
	if rt.Status == model.RuntimeStatusOffline {
		return proof, fmt.Errorf("failover blocked: source runtime execution is unknown; verified shutdown or physical fencing is required")
	}
	if rt.ClusterNodeName != "" {
		proof.Nodes[rt.ClusterNodeName] = ""
	}
	pods, err := c.listPodsBySelector(ctx, runtime.NamespaceForTenant(app.TenantID), managedAppPodLabelSelector(app))
	if err != nil {
		return proof, err
	}
	for _, pod := range pods {
		if pod.Spec.NodeName != "" && pod.Status.Phase != "Failed" && pod.Status.Phase != "Succeeded" {
			proof.Nodes[pod.Spec.NodeName] = ""
		}
	}
	if len(proof.Nodes) == 0 {
		return proof, fmt.Errorf("failover blocked: source node identity is unavailable; absence of API Pods does not prove termination")
	}
	for name := range proof.Nodes {
		node, found, err := c.getNode(ctx, name)
		if err != nil {
			return proof, err
		}
		if !found || !kubeNodeReady(node) || strings.TrimSpace(node.Metadata.UID) == "" {
			return proof, fmt.Errorf("failover blocked: source node %s execution is unknown; verified shutdown or physical fencing is required", name)
		}
		proof.Nodes[name] = node.Metadata.UID
	}
	return proof, nil
}

func (s *Service) appFailoverSourceJournal(ctx context.Context, c *kubeClient, op model.Operation, app model.App, source string) (appFailoverSource, error) {
	items, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: op.TenantID, OperationID: op.ID, Types: []string{appFailoverSourceEvidence}, Limit: 1})
	if err != nil {
		return appFailoverSource{}, err
	}
	if len(items) > 0 {
		raw, err := json.Marshal(items[0].Payload)
		var proof appFailoverSource
		if err == nil {
			err = json.Unmarshal(raw, &proof)
		}
		if err == nil && (proof.RuntimeID != source || proof.Nodes == nil) {
			err = fmt.Errorf("invalid failover source journal")
		}
		return proof, err
	}
	proof, err := s.observeAppFailoverSource(ctx, c, app, source)
	if err != nil {
		return proof, err
	}
	nodes := make(map[string]any, len(proof.Nodes))
	for name, uid := range proof.Nodes {
		nodes[name] = uid
	}
	_, err = s.Store.RecordOperationEvidence(model.OperationEvidence{TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID, Type: appFailoverSourceEvidence,
		Source: model.OperationEvidenceSourceKubernetesAPI, Severity: model.OperationEvidenceSeverityInfo, Confidence: model.OperationEvidenceConfidenceConfirmed, RedactionStatus: model.OperationEvidenceRedactionNone,
		Summary: "source node identities recorded before shutdown", PayloadVersion: 1, Payload: map[string]any{"runtime_id": source, "nodes": nodes}})
	return proof, err
}

func verifyAppFailoverSourceStopped(ctx context.Context, c *kubeClient, app model.App, proof appFailoverSource) error {
	for name, uid := range proof.Nodes {
		node, found, err := c.getNode(ctx, name)
		if err != nil {
			return err
		}
		if !found || !kubeNodeReady(node) || node.Metadata.UID != uid {
			return fmt.Errorf("failover blocked: source node %s termination cannot be confirmed", name)
		}
	}
	pods, err := c.listPodsBySelector(ctx, runtime.NamespaceForTenant(app.TenantID), managedAppPodLabelSelector(app))
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if pod.Status.Phase != "Failed" && pod.Status.Phase != "Succeeded" {
			return fmt.Errorf("failover blocked: source Pod %s has not confirmed termination", pod.Metadata.Name)
		}
	}
	return nil
}
