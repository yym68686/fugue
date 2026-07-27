package api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	nodeUpdaterRejoinCredentialClass = "short_lived_kubernetes_bootstrap_token"
	nodeUpdaterRejoinExpiryMargin    = 2 * time.Minute
)

type nodeUpdaterRejoinBootstrapToken struct {
	Token      string
	TokenID    string
	Generation string
	ExpiresAt  time.Time
}

func (s *Server) nodeUpdaterClusterRejoin(
	ctx context.Context,
	principal model.Principal,
	updater model.NodeUpdater,
) (*model.NodeUpdaterClusterRejoin, []string) {
	now := time.Now().UTC()
	nodeName := strings.TrimSpace(updater.ClusterNodeName)
	plan := &model.NodeUpdaterClusterRejoin{
		Status:     model.NodeUpdaterClusterRejoinStatusSuppressed,
		Reason:     "eligibility_not_established",
		NodeName:   nodeName,
		ObservedAt: now,
	}
	warnings := []string{}

	suppress := func(reason, warning string) (*model.NodeUpdaterClusterRejoin, []string) {
		plan.Status = model.NodeUpdaterClusterRejoinStatusSuppressed
		plan.Reason = reason
		if warning != "" {
			warnings = append(warnings, warning)
		}
		return plan, warnings
	}
	unavailable := func(reason, warning string) (*model.NodeUpdaterClusterRejoin, []string) {
		plan.Status = model.NodeUpdaterClusterRejoinStatusUnavailable
		plan.Reason = reason
		if warning != "" {
			warnings = append(warnings, warning)
		}
		return plan, warnings
	}

	if !nodeUpdaterHasCapability(updater, model.NodeUpdaterCapabilityRejoinK3SNode) {
		return suppress("capability_missing", "cluster rejoin suppressed: node updater does not advertise "+model.NodeUpdaterCapabilityRejoinK3SNode)
	}
	if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
		return suppress("node_updater_inactive", "cluster rejoin suppressed: node updater is not active")
	}
	if !s.clusterJoinConfigured() {
		return suppress("cluster_join_not_configured", "cluster rejoin suppressed: cluster join is not fully configured")
	}
	if nodeName == "" || strings.TrimSpace(updater.NodeKeyID) == "" || strings.TrimSpace(updater.MachineID) == "" {
		return suppress("identity_incomplete", "cluster rejoin suppressed: node updater identity is incomplete")
	}

	key, err := s.store.GetNodeKey(updater.NodeKeyID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return suppress("node_key_not_found", "cluster rejoin suppressed: bound node key no longer exists")
		}
		return unavailable("node_key_lookup_failed", "cluster rejoin unavailable: read bound node key: "+err.Error())
	}
	if key.RevokedAt != nil || strings.EqualFold(strings.TrimSpace(key.Status), model.NodeKeyStatusRevoked) {
		return suppress("node_key_revoked", "cluster rejoin suppressed: bound node key is revoked")
	}

	machine, err := s.store.GetMachineByClusterNodeName(nodeName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return suppress("machine_not_found", "cluster rejoin suppressed: bound machine no longer exists")
		}
		return unavailable("machine_lookup_failed", "cluster rejoin unavailable: read bound machine: "+err.Error())
	}
	if strings.TrimSpace(machine.ID) != strings.TrimSpace(updater.MachineID) ||
		strings.TrimSpace(machine.NodeKeyID) != strings.TrimSpace(updater.NodeKeyID) ||
		strings.TrimSpace(machine.RuntimeID) != strings.TrimSpace(updater.RuntimeID) ||
		!strings.EqualFold(strings.TrimSpace(machine.ClusterNodeName), nodeName) {
		return suppress("machine_binding_mismatch", "cluster rejoin suppressed: updater and machine bindings do not match")
	}

	if runtimeID := strings.TrimSpace(updater.RuntimeID); runtimeID != "" {
		runtimeObj, err := s.store.GetRuntime(runtimeID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return suppress("runtime_not_found", "cluster rejoin suppressed: bound runtime no longer exists")
			}
			return unavailable("runtime_lookup_failed", "cluster rejoin unavailable: read bound runtime: "+err.Error())
		}
		if strings.TrimSpace(runtimeObj.NodeKeyID) != strings.TrimSpace(updater.NodeKeyID) ||
			!strings.EqualFold(strings.TrimSpace(runtimeObj.ClusterNodeName), nodeName) {
			return suppress("runtime_binding_mismatch", "cluster rejoin suppressed: updater and runtime bindings do not match")
		}
	}

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return unavailable("kubernetes_client_unavailable", "cluster rejoin unavailable: create Kubernetes client: "+err.Error())
	}
	defer client.closeIdleConnections()

	if _, err := client.getNode(ctx, nodeName); err == nil {
		plan.Status = model.NodeUpdaterClusterRejoinStatusNotRequired
		plan.Reason = "node_present"
		deletedTokenIDs, cleanupErr := client.deleteNodeUpdaterRejoinTokens(ctx, updater)
		if cleanupErr != nil {
			warnings = append(warnings, "cluster rejoin credential cleanup failed: "+cleanupErr.Error())
			s.appendAudit(principal, "node_updater.cluster_rejoin.credentials_revoke_failed", "node_updater", updater.ID, updater.TenantID, map[string]string{
				"cluster_node_name": updater.ClusterNodeName,
				"node_key_id":       updater.NodeKeyID,
				"runtime_id":        updater.RuntimeID,
				"reason":            "kubernetes_node_present",
				"error":             truncateAuditValue(cleanupErr.Error(), 600),
			})
		} else if len(deletedTokenIDs) > 0 {
			s.appendAudit(principal, "node_updater.cluster_rejoin.credentials_revoked", "node_updater", updater.ID, updater.TenantID, map[string]string{
				"cluster_node_name": updater.ClusterNodeName,
				"node_key_id":       updater.NodeKeyID,
				"runtime_id":        updater.RuntimeID,
				"token_ids":         strings.Join(deletedTokenIDs, ","),
				"reason":            "kubernetes_node_present",
			})
		}
		return plan, warnings
	} else if !isKubernetesNodeNotFound(err) {
		return unavailable("kubernetes_node_lookup_failed", "cluster rejoin unavailable: verify Kubernetes Node presence: "+err.Error())
	}

	token, reused, err := client.ensureNodeUpdaterRejoinToken(
		ctx,
		updater,
		s.clusterJoinCAHash,
		s.clusterJoinBootstrapTokenTTL,
		now,
	)
	if err != nil {
		s.appendAudit(principal, "node_updater.cluster_rejoin.credential_issue_failed", "node_updater", updater.ID, updater.TenantID, map[string]string{
			"cluster_node_name": updater.ClusterNodeName,
			"node_key_id":       updater.NodeKeyID,
			"runtime_id":        updater.RuntimeID,
			"reason":            "kubernetes_node_not_found",
			"error":             truncateAuditValue(err.Error(), 600),
		})
		return unavailable("credential_issue_failed", "cluster rejoin unavailable: issue short-lived bootstrap credential: "+err.Error())
	}

	plan.Status = model.NodeUpdaterClusterRejoinStatusCredentialReady
	plan.Reason = "kubernetes_node_not_found"
	plan.Credential = &model.NodeUpdaterClusterRejoinCredential{
		Class:      nodeUpdaterRejoinCredentialClass,
		Token:      token.Token,
		TokenID:    token.TokenID,
		Generation: token.Generation,
		ExpiresAt:  token.ExpiresAt,
	}
	if !reused {
		s.appendAudit(principal, "node_updater.cluster_rejoin.credential_issued", "node_updater", updater.ID, updater.TenantID, map[string]string{
			"cluster_node_name":   updater.ClusterNodeName,
			"node_key_id":         updater.NodeKeyID,
			"runtime_id":          updater.RuntimeID,
			"machine_id":          updater.MachineID,
			"node_presence":       "absent",
			"observation":         "kubernetes_api_get_node_404",
			"credential_class":    nodeUpdaterRejoinCredentialClass,
			"credential_token_id": token.TokenID,
			"generation":          token.Generation,
			"expires_at":          token.ExpiresAt.Format(time.RFC3339),
		})
	}
	return plan, warnings
}

func nodeUpdaterHasCapability(updater model.NodeUpdater, capability string) bool {
	capability = strings.TrimSpace(capability)
	for _, candidate := range updater.Capabilities {
		if strings.EqualFold(strings.TrimSpace(candidate), capability) {
			return true
		}
	}
	return false
}

func (c *clusterNodeClient) ensureNodeUpdaterRejoinToken(
	ctx context.Context,
	updater model.NodeUpdater,
	caHash string,
	ttl time.Duration,
	now time.Time,
) (nodeUpdaterRejoinBootstrapToken, bool, error) {
	active, err := c.listNodeUpdaterRejoinTokens(ctx, updater, caHash, now.Add(nodeUpdaterRejoinExpiryMargin))
	if err != nil {
		return nodeUpdaterRejoinBootstrapToken{}, false, err
	}
	if len(active) > 0 {
		sort.Slice(active, func(i, j int) bool {
			return active[i].ExpiresAt.After(active[j].ExpiresAt)
		})
		return active[0], true, nil
	}

	token, tokenID, expiresAt, err := c.createBootstrapTokenWithLabels(
		ctx,
		updater.NodeKeyID,
		updater.RuntimeID,
		caHash,
		ttl,
		map[string]string{
			clusterJoinTokenLabelNodeUpdater: updater.ID,
			clusterJoinTokenLabelNodeName:    updater.ClusterNodeName,
			clusterJoinTokenLabelPurpose:     clusterJoinTokenPurposeNodeRejoin,
		},
		"fugue automatic Kubernetes node rejoin token",
	)
	if err != nil {
		return nodeUpdaterRejoinBootstrapToken{}, false, err
	}
	return nodeUpdaterRejoinBootstrapToken{
		Token:      token,
		TokenID:    tokenID,
		Generation: "bootstrap-token/" + tokenID,
		ExpiresAt:  expiresAt,
	}, false, nil
}

func (c *clusterNodeClient) listNodeUpdaterRejoinTokens(
	ctx context.Context,
	updater model.NodeUpdater,
	caHash string,
	validAfter time.Time,
) ([]nodeUpdaterRejoinBootstrapToken, error) {
	query := url.Values{}
	query.Set("labelSelector", strings.Join([]string{
		clusterJoinTokenLabelManaged + "=" + clusterJoinTokenLabelValue,
		clusterJoinTokenLabelNodeUpdater + "=" + strings.TrimSpace(updater.ID),
		clusterJoinTokenLabelPurpose + "=" + clusterJoinTokenPurposeNodeRejoin,
	}, ","))
	var secretList kubeSecretList
	apiPath := "/api/v1/namespaces/" + clusterJoinTokenNamespace + "/secrets?" + query.Encode()
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &secretList); err != nil {
		if isKubernetesNodeNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	tokens := make([]nodeUpdaterRejoinBootstrapToken, 0, len(secretList.Items))
	for _, secret := range secretList.Items {
		if strings.TrimSpace(secret.Metadata.Labels[clusterJoinTokenLabelNodeKey]) != strings.TrimSpace(updater.NodeKeyID) ||
			strings.TrimSpace(secret.Metadata.Labels[clusterJoinTokenLabelRuntime]) != strings.TrimSpace(updater.RuntimeID) ||
			!strings.EqualFold(strings.TrimSpace(secret.Metadata.Labels[clusterJoinTokenLabelNodeName]), strings.TrimSpace(updater.ClusterNodeName)) {
			continue
		}
		tokenID, err := decodeKubernetesSecretData(secret, "token-id")
		if err != nil {
			continue
		}
		tokenSecret, err := decodeKubernetesSecretData(secret, "token-secret")
		if err != nil {
			continue
		}
		if !validKubernetesBootstrapTokenComponent(tokenID, 6) ||
			!validKubernetesBootstrapTokenComponent(tokenSecret, 16) {
			continue
		}
		expiration, err := decodeKubernetesSecretData(secret, "expiration")
		if err != nil {
			continue
		}
		expiresAt, err := time.Parse(time.RFC3339, expiration)
		if err != nil || !expiresAt.After(validAfter) {
			continue
		}
		if tokenID != clusterJoinBootstrapTokenIDFromSecretName(secret.Metadata.Name) {
			continue
		}
		token := tokenID + "." + tokenSecret
		if normalizedHash := normalizeClusterJoinCAHash(caHash); normalizedHash != "" {
			token = "K10" + normalizedHash + "::" + token
		}
		tokens = append(tokens, nodeUpdaterRejoinBootstrapToken{
			Token:      token,
			TokenID:    tokenID,
			Generation: "bootstrap-token/" + tokenID,
			ExpiresAt:  expiresAt.UTC(),
		})
	}
	return tokens, nil
}

func (c *clusterNodeClient) deleteNodeUpdaterRejoinTokens(ctx context.Context, updater model.NodeUpdater) ([]string, error) {
	query := url.Values{}
	query.Set("labelSelector", strings.Join([]string{
		clusterJoinTokenLabelManaged + "=" + clusterJoinTokenLabelValue,
		clusterJoinTokenLabelNodeUpdater + "=" + strings.TrimSpace(updater.ID),
		clusterJoinTokenLabelPurpose + "=" + clusterJoinTokenPurposeNodeRejoin,
	}, ","))
	var secretList kubeSecretList
	apiPath := "/api/v1/namespaces/" + clusterJoinTokenNamespace + "/secrets?" + query.Encode()
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &secretList); err != nil {
		if isKubernetesNodeNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	deleted := make([]string, 0, len(secretList.Items))
	for _, secret := range secretList.Items {
		if strings.TrimSpace(secret.Metadata.Labels[clusterJoinTokenLabelNodeKey]) != strings.TrimSpace(updater.NodeKeyID) ||
			strings.TrimSpace(secret.Metadata.Labels[clusterJoinTokenLabelRuntime]) != strings.TrimSpace(updater.RuntimeID) ||
			!strings.EqualFold(strings.TrimSpace(secret.Metadata.Labels[clusterJoinTokenLabelNodeName]), strings.TrimSpace(updater.ClusterNodeName)) {
			continue
		}
		tokenID := clusterJoinBootstrapTokenIDFromSecretName(secret.Metadata.Name)
		if tokenID == "" {
			continue
		}
		if err := c.deleteBootstrapToken(ctx, tokenID); err != nil && !isKubernetesNodeNotFound(err) {
			return deleted, err
		}
		deleted = append(deleted, tokenID)
	}
	sort.Strings(deleted)
	return deleted, nil
}

func decodeKubernetesSecretData(secret kubeSecret, key string) (string, error) {
	encoded := strings.TrimSpace(secret.Data[key])
	if encoded == "" {
		return "", fmt.Errorf("secret %s is missing %s", secret.Metadata.Name, key)
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("decode secret %s field %s: %w", secret.Metadata.Name, key, err)
	}
	value := strings.TrimSpace(string(decoded))
	if value == "" {
		return "", fmt.Errorf("secret %s field %s is empty", secret.Metadata.Name, key)
	}
	return value, nil
}

func validKubernetesBootstrapTokenComponent(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}
