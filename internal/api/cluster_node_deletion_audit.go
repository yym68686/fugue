package api

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/model"
)

const (
	clusterNodeDeleteOutcomeDeleted       = "deleted"
	clusterNodeDeleteOutcomeAlreadyAbsent = "already_absent"
)

func (s *Server) deleteClusterNodeWithAudit(
	ctx context.Context,
	client *clusterNodeClient,
	principal model.Principal,
	nodeName, tenantID, reason string,
	metadata map[string]string,
) (string, error) {
	nodeName = strings.TrimSpace(nodeName)
	reason = strings.TrimSpace(reason)
	if client == nil || nodeName == "" || reason == "" {
		return "", fmt.Errorf("cluster node deletion requires client, node name, and reason")
	}

	operationID := model.NewID("nodedelete")
	auditMetadata := map[string]string{
		"operation_id":        operationID,
		"cluster_node_name":   nodeName,
		"reason":              reason,
		"kubernetes_resource": "nodes/" + nodeName,
	}
	for key, value := range metadata {
		if key = strings.TrimSpace(key); key != "" {
			auditMetadata[key] = strings.TrimSpace(value)
		}
	}
	if err := s.store.AppendAuditEvent(model.AuditEvent{
		TenantID:   tenantID,
		ActorType:  principal.ActorType,
		ActorID:    principal.ActorID,
		Action:     "cluster.node.delete.requested",
		TargetType: "cluster_node",
		TargetID:   nodeName,
		Metadata:   cloneStringMap(auditMetadata),
	}); err != nil {
		return "", fmt.Errorf("persist cluster node deletion intent: %w", err)
	}

	err := client.deleteNode(ctx, nodeName)
	switch {
	case err == nil:
		auditMetadata["outcome"] = clusterNodeDeleteOutcomeDeleted
		s.appendAudit(principal, "cluster.node.delete.succeeded", "cluster_node", nodeName, tenantID, cloneStringMap(auditMetadata))
		return clusterNodeDeleteOutcomeDeleted, nil
	case isKubernetesNodeNotFound(err):
		auditMetadata["outcome"] = clusterNodeDeleteOutcomeAlreadyAbsent
		s.appendAudit(principal, "cluster.node.delete.already_absent", "cluster_node", nodeName, tenantID, cloneStringMap(auditMetadata))
		return clusterNodeDeleteOutcomeAlreadyAbsent, nil
	default:
		auditMetadata["outcome"] = "failed"
		auditMetadata["error"] = truncateAuditValue(err.Error(), 600)
		s.appendAudit(principal, "cluster.node.delete.failed", "cluster_node", nodeName, tenantID, cloneStringMap(auditMetadata))
		return "", err
	}
}
