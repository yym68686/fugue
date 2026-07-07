package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/appimages"
	"fugue/internal/httpx"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	nodeUpdaterScriptVersion        = model.NodeUpdaterCurrentVersion
	staleNodeUpdateTaskTimeout      = 2 * time.Hour
	imageCachePruneDeleteTaskMaxAge = 45 * time.Minute
)

func (s *Server) handleNodeUpdaterInstallScript(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/x-shellscript; charset=utf-8")
	_, _ = fmt.Fprint(w, s.nodeUpdaterInstallScript(s.publicInstallAPIBaseURL(r)))
}

func (s *Server) handleNodeUpdaterEnroll(w http.ResponseWriter, r *http.Request) {
	req, wantsEnv, err := decodeNodeUpdaterEnrollRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	updater, token, err := s.store.EnrollNodeUpdater(
		req.NodeKey,
		coalesceNodeName(req.NodeName, req.RuntimeName),
		req.Endpoint,
		req.Labels,
		req.MachineName,
		req.MachineFingerprint,
		req.UpdaterVersion,
		req.JoinScriptVersion,
		req.Capabilities,
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(model.Principal{
		ActorType: model.ActorTypeNodeKey,
		ActorID:   updater.NodeKeyID,
		TenantID:  updater.TenantID,
	}, "node_updater.enroll", "node_updater", updater.ID, updater.TenantID, map[string]string{
		"cluster_node_name": updater.ClusterNodeName,
		"runtime_id":        updater.RuntimeID,
	})
	if wantsEnv {
		writeNodeUpdaterEnrollEnv(w, updater, token)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{
		"node_updater": updater,
		"token":        token,
	})
}

func (s *Server) handleNodeUpdaterHeartbeat(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	req, wantsEnv, err := decodeNodeUpdaterHeartbeatRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	updater, err := s.store.UpdateNodeUpdaterHeartbeat(principal.ActorID, model.NodeUpdater{
		Labels:              req.Labels,
		Capabilities:        req.Capabilities,
		UpdaterVersion:      req.UpdaterVersion,
		JoinScriptVersion:   req.JoinScriptVersion,
		K3SVersion:          req.K3SVersion,
		K3SServer:           req.K3SServer,
		K3SFallbackServers:  req.K3SFallbackServers,
		RegistryMirror:      req.RegistryMirror,
		LabelsHash:          req.LabelsHash,
		TaintsHash:          req.TaintsHash,
		EdgeEnvGeneration:   req.EdgeEnvGeneration,
		DNSEnvGeneration:    req.DNSEnvGeneration,
		ConfigHash:          req.ConfigHash,
		DiscoveryGeneration: req.DiscoveryGeneration,
		OS:                  req.OS,
		Arch:                req.Arch,
		LastError:           req.LastError,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if wantsEnv {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "FUGUE_NODE_UPDATER_ID=%s\n", shellQuote(updater.ID))
		fmt.Fprintf(w, "FUGUE_NODE_UPDATER_STATUS=%s\n", shellQuote(updater.Status))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_updater": updater})
}

func (s *Server) handleGetNodeUpdaterDesiredState(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.ActorType != model.ActorTypeNodeUpdater {
		httpx.WriteError(w, http.StatusForbidden, "node updater credentials required")
		return
	}
	state, err := s.nodeUpdaterDesiredState(r.Context(), r, principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"desired_state": state})
}

func (s *Server) nodeUpdaterDesiredState(ctx context.Context, r *http.Request, principal model.Principal) (model.NodeUpdaterDesiredState, error) {
	updater, err := s.nodeUpdaterByPrincipal(principal)
	if err != nil {
		return model.NodeUpdaterDesiredState{}, err
	}
	discovery, err := s.deriveDiscoveryBundle(r.WithContext(ctx), discoveryBundlePrincipal())
	if err != nil {
		return model.NodeUpdaterDesiredState{}, err
	}
	var nodePolicy *model.ClusterNodePolicyStatus
	warnings := []string{}
	statuses, err := s.loadClusterNodePolicyStatuses(ctx, principal)
	if err != nil {
		warnings = append(warnings, err.Error())
	} else {
		for i := range statuses {
			if strings.EqualFold(strings.TrimSpace(statuses[i].NodeName), strings.TrimSpace(updater.ClusterNodeName)) {
				status := statuses[i]
				nodePolicy = &status
				break
			}
		}
		if nodePolicy == nil {
			warnings = append(warnings, "node policy not found for current cluster node")
		}
	}
	edgeCredential, edgeWarnings, err := s.nodeUpdaterEdgeCredential(r, updater, nodePolicy)
	if err != nil {
		return model.NodeUpdaterDesiredState{}, err
	}
	nodePolicy = nodeUpdaterPolicyWithEdgeCredentialLabels(nodePolicy, edgeCredential)
	warnings = append(warnings, edgeWarnings...)
	return model.NodeUpdaterDesiredState{
		GeneratedAt:     time.Now().UTC(),
		NodeUpdater:     updater,
		DiscoveryBundle: discovery,
		NodePolicy:      nodePolicy,
		EdgeCredential:  edgeCredential,
		Warnings:        warnings,
	}, nil
}

func (s *Server) nodeUpdaterEdgeCredential(r *http.Request, updater model.NodeUpdater, nodePolicy *model.ClusterNodePolicyStatus) (*model.NodeUpdaterEdgeCredential, []string, error) {
	labels := updater.Labels
	if nodePolicy != nil && len(nodePolicy.Labels) > 0 {
		labels = nodePolicy.Labels
	}
	if !nodeUpdaterPolicyAllowsEdge(labels, nodePolicy) {
		return nil, nil, nil
	}
	edgeID := strings.TrimSpace(updater.ClusterNodeName)
	if edgeID == "" {
		return nil, []string{"edge credential not issued: cluster node name is empty"}, nil
	}
	warnings := []string{}
	publicIP := strings.TrimSpace(labels["fugue.io/public-ip"])
	country := strings.ToLower(strings.TrimSpace(labels["fugue.io/location-country-code"]))
	region := strings.TrimSpace(firstNodeLabel(labels, "topology.kubernetes.io/region", "failure-domain.beta.kubernetes.io/region"))
	edgeGroupID := derivedEdgeGroupIDForLabels(labels)
	if (edgeGroupID == "" || edgeGroupID == defaultEdgeGroupID) && country == "" && publicIP != "" {
		geoCountry, source, err := lookupCountryCodeForPublicIP(r.Context(), publicIP)
		if err != nil {
			warnings = append(warnings, "edge credential location inference failed: "+err.Error())
		} else if slug := edgeRouteSlug(geoCountry); slug != "" {
			country = geoCountry
			edgeGroupID = "edge-group-country-" + slug
			warnings = append(warnings, "edge credential location inferred from public IP via "+source)
		}
	}
	if (edgeGroupID == "" || edgeGroupID == defaultEdgeGroupID) && country != "" {
		if slug := edgeRouteSlug(country); slug != "" {
			edgeGroupID = "edge-group-country-" + slug
		}
	}
	if edgeGroupID == "" || edgeGroupID == defaultEdgeGroupID {
		warnings = append(warnings, "edge credential not issued: missing location country/region or explicit edge group")
		return nil, warnings, nil
	}
	workloadMode := nodeUpdaterEdgeWorkloadMode(labels, nodePolicy)
	credential := &model.NodeUpdaterEdgeCredential{
		EdgeID:          edgeID,
		EdgeGroupID:     edgeGroupID,
		WorkloadMode:    workloadMode,
		Country:         country,
		Region:          region,
		DesiredStateURL: strings.TrimRight(s.publicInstallAPIBaseURL(r), "/") + "/v1/edge/nodes/" + edgeID + "/desired-state",
	}
	if strings.Contains(publicIP, ":") {
		credential.PublicIPv6 = publicIP
	} else {
		credential.PublicIPv4 = publicIP
	}
	reportedTokenPrefix := nodeUpdaterEdgeEnvGenerationTokenPrefix(updater.EdgeEnvGeneration)
	needsToken := reportedTokenPrefix == ""
	if existing, _, err := s.store.GetEdgeNode(edgeID); err == nil {
		credential.TokenPrefix = existing.TokenPrefix
		existingPrefix := strings.TrimSpace(existing.TokenPrefix)
		if existingPrefix == "" || !strings.EqualFold(strings.TrimSpace(existing.EdgeGroupID), edgeGroupID) || !strings.EqualFold(reportedTokenPrefix, existingPrefix) {
			needsToken = true
		}
	} else if errors.Is(err, store.ErrNotFound) {
		needsToken = true
	} else {
		return nil, nil, err
	}
	if needsToken {
		node, token, err := s.store.CreateEdgeNodeToken(model.EdgeNode{
			ID:           edgeID,
			EdgeGroupID:  edgeGroupID,
			WorkloadMode: workloadMode,
			CanaryState:  model.EdgeCanaryStateJoined,
			CanaryWeight: 1,
			Region:       region,
			Country:      country,
			PublicIPv4:   credential.PublicIPv4,
			PublicIPv6:   credential.PublicIPv6,
			Status:       model.EdgeHealthUnknown,
		})
		if err != nil {
			return nil, nil, err
		}
		credential.Token = token
		credential.TokenPrefix = node.TokenPrefix
	}
	return credential, warnings, nil
}

func nodeUpdaterEdgeWorkloadMode(labels map[string]string, nodePolicy *model.ClusterNodePolicyStatus) string {
	if mode := normalizeNodeUpdaterEdgeWorkloadMode(labels[runtimepkg.EdgeWorkloadLabelKey]); mode != "" {
		return mode
	}
	if nodePolicy != nil && nodePolicy.Policy != nil {
		policy := nodePolicy.Policy
		if policy.AllowDNS || policy.EffectiveDNS {
			return runtimepkg.EdgeWorkloadStaticValue
		}
		if model.NormalizeMachineDedicatedMode(firstNonEmptyString(policy.DedicatedMode, policy.EffectiveDedicatedMode)) == model.MachineDedicatedModeEdge {
			return runtimepkg.EdgeWorkloadDynamicValue
		}
	}
	if strings.EqualFold(strings.TrimSpace(labels[runtimepkg.DNSRoleLabelKey]), runtimepkg.NodeRoleLabelValue) {
		return runtimepkg.EdgeWorkloadStaticValue
	}
	// Ambiguous legacy edge nodes predate dynamic DaemonSets. Keep them on the
	// static public edge unless a node key, policy, or explicit label opts in.
	return runtimepkg.EdgeWorkloadStaticValue
}

func normalizeNodeUpdaterEdgeWorkloadMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case runtimepkg.EdgeWorkloadStaticValue:
		return runtimepkg.EdgeWorkloadStaticValue
	case runtimepkg.EdgeWorkloadDynamicValue:
		return runtimepkg.EdgeWorkloadDynamicValue
	default:
		return ""
	}
}

func nodeUpdaterEdgeEnvGenerationTokenPrefix(generation string) string {
	generation = strings.TrimSpace(generation)
	if generation == "" {
		return ""
	}
	parts := strings.Split(generation, ":")
	if len(parts) >= 3 && parts[0] == "v2" {
		return strings.TrimSpace(parts[1])
	}
	return ""
}

func nodeUpdaterPolicyWithEdgeCredentialLabels(nodePolicy *model.ClusterNodePolicyStatus, credential *model.NodeUpdaterEdgeCredential) *model.ClusterNodePolicyStatus {
	if nodePolicy == nil || credential == nil {
		return nodePolicy
	}
	out := *nodePolicy
	out.Labels = cloneStringMap(nodePolicy.Labels)
	if out.Labels == nil {
		out.Labels = map[string]string{}
	}
	setIfEmpty := func(key, value string) {
		value = strings.TrimSpace(value)
		if key == "" || value == "" || strings.TrimSpace(out.Labels[key]) != "" {
			return
		}
		out.Labels[key] = value
	}
	edgePublicIP := strings.TrimSpace(firstNonEmptyString(credential.PublicIPv4, credential.PublicIPv6))
	setIfEmpty("fugue.io/public-ip", edgePublicIP)
	setIfEmpty("topology.kubernetes.io/region", credential.Region)
	setIfEmpty("fugue.io/location-country-code", strings.ToLower(strings.TrimSpace(credential.Country)))
	setIfEmpty("fugue.io/edge-group-id", credential.EdgeGroupID)
	workloadMode := strings.TrimSpace(strings.ToLower(credential.WorkloadMode))
	if workloadMode == "" {
		workloadMode = "dynamic"
	}
	switch workloadMode {
	case "static", "dynamic":
	default:
		workloadMode = "dynamic"
	}
	setIfEmpty("fugue.io/edge-workload", workloadMode)
	if nodePolicy.Policy != nil && (nodePolicy.Policy.AllowEdge || nodePolicy.Policy.EffectiveEdge) {
		setIfEmpty("fugue.io/role.edge", "true")
	}
	if strings.TrimSpace(out.Labels["fugue.io/edge-location-status"]) == "" {
		if strings.TrimSpace(out.Labels["fugue.io/edge-group-id"]) != "" || strings.TrimSpace(out.Labels["fugue.io/location-country-code"]) != "" || workloadMode == "static" {
			out.Labels["fugue.io/edge-location-status"] = "ready"
		} else {
			out.Labels["fugue.io/edge-location-status"] = "missing_location"
		}
	}
	return &out
}

func nodeUpdaterPolicyAllowsEdge(labels map[string]string, nodePolicy *model.ClusterNodePolicyStatus) bool {
	if nodePolicy != nil && nodePolicy.Policy != nil {
		if nodePolicy.Policy.EffectiveEdge || nodePolicy.Policy.AllowEdge {
			return true
		}
		return false
	}
	return strings.EqualFold(strings.TrimSpace(labels["fugue.io/role.edge"]), "true")
}

func (s *Server) nodeUpdaterByPrincipal(principal model.Principal) (model.NodeUpdater, error) {
	updaters, err := s.store.ListNodeUpdaters(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return model.NodeUpdater{}, err
	}
	for _, updater := range updaters {
		if strings.EqualFold(strings.TrimSpace(updater.ID), strings.TrimSpace(principal.ActorID)) {
			return updater, nil
		}
	}
	return model.NodeUpdater{}, store.ErrNotFound
}

func (s *Server) handleListNodeUpdaters(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	updaters, err := s.store.ListNodeUpdaters(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node_updaters": updaters})
}

func (s *Server) handleCreateNodeUpdateTask(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	var req struct {
		NodeUpdaterID   string            `json:"node_updater_id"`
		ClusterNodeName string            `json:"cluster_node_name"`
		RuntimeID       string            `json:"runtime_id"`
		Type            string            `json:"type"`
		Payload         map[string]string `json:"payload"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	task, err := s.store.CreateNodeUpdateTask(principal, req.NodeUpdaterID, req.ClusterNodeName, req.RuntimeID, req.Type, req.Payload)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "node_update_task.create", "node_update_task", task.ID, task.TenantID, map[string]string{
		"type":              task.Type,
		"node_updater_id":   task.NodeUpdaterID,
		"cluster_node_name": task.ClusterNodeName,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"task": task})
}

func (s *Server) handleListNodeUpdateTasks(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	tasks, err := s.store.ListNodeUpdateTasks(
		principal.TenantID,
		principal.IsPlatformAdmin(),
		r.URL.Query().Get("node_updater_id"),
		r.URL.Query().Get("status"),
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) handleNodeUpdaterTasks(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	limit := parsePositiveInt(r.URL.Query().Get("limit"), 10)
	if _, err := s.store.FailStaleRunningNodeUpdateTasks(principal.ActorID, staleNodeUpdateTaskTimeout); err != nil {
		s.writeStoreError(w, err)
		return
	}
	tasks, err := s.store.ListPendingNodeUpdateTasks(principal.ActorID, limit)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("format")), "env") {
		writeNodeUpdateTaskEnv(w, s.nodeUpdateTaskForDelivery(firstNodeUpdateTask(tasks)))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) handleNodeUpdaterClaimTask(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	current, err := s.store.GetNodeUpdateTaskForUpdater(r.PathValue("id"), principal.ActorID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if reason, err := s.refuseUnsafeNodeUpdateTaskClaim(r, current); err != nil {
		s.writeStoreError(w, err)
		return
	} else if reason != "" {
		failed, failErr := s.store.FailNodeUpdateTask(current.ID, principal.ActorID, "node update task refused before execution", reason)
		if failErr != nil {
			s.writeStoreError(w, failErr)
			return
		}
		s.appendNodeUpdateTaskMaintenanceAudit(principal, failed)
		httpx.WriteError(w, http.StatusConflict, reason)
		return
	}
	task, err := s.store.ClaimNodeUpdateTask(r.PathValue("id"), principal.ActorID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"task": task})
}

func (s *Server) refuseUnsafeNodeUpdateTaskClaim(r *http.Request, task model.NodeUpdateTask) (string, error) {
	if task.Type == model.NodeUpdateTaskTypeReplicateAppImage {
		return s.refuseUnsafeReplicateAppImageTaskClaim(task)
	}
	if task.Type != model.NodeUpdateTaskTypePruneImageCache {
		return "", nil
	}
	if !nodeUpdatePayloadBool(task.Payload["allow_delete"]) || nodeUpdatePayloadBool(task.Payload["dry_run"]) {
		return "", nil
	}
	targets, err := imageCachePruneTaskValidationTargets(task.Payload)
	if err != nil {
		return "", err
	}
	if len(targets) == 0 {
		if strings.TrimSpace(task.Payload["prune_reason"]) == "image-cache-orphan" && nodeUpdatePayloadBool(task.Payload["include_unreferenced_blobs"]) {
			plan, err := s.computeImageCachePrunePlanWithOptions(r, model.ImageCachePrunePlanFilter{
				NodeID:          task.MachineID,
				ClusterNodeName: task.ClusterNodeName,
				RuntimeID:       task.RuntimeID,
				Mode:            model.ImageCachePruneModeDelete,
			}, imageCachePrunePlanOptions{skipNodeUpdateTaskID: task.ID})
			if err != nil {
				return "", err
			}
			if plan.CandidateBlobCount > 0 || plan.CandidateBlobBytes > 0 {
				return "", nil
			}
			return "refuse prune-image-cache delete task before execution: latest plan has no unreferenced blob candidates", nil
		}
		if strings.TrimSpace(task.Payload["prune_reason"]) == "image-cache-orphan" {
			return "refuse prune-image-cache delete task before execution: orphan delete task has no explicit targets_json", nil
		}
		return "", nil
	}
	if !task.CreatedAt.IsZero() && time.Since(task.CreatedAt) > imageCachePruneDeleteTaskMaxAge {
		return fmt.Sprintf("refuse prune-image-cache delete task before execution: task age %s exceeds %s; recompute a fresh plan", time.Since(task.CreatedAt).Round(time.Second), imageCachePruneDeleteTaskMaxAge), nil
	}
	plan, err := s.computeImageCachePrunePlanWithOptions(r, model.ImageCachePrunePlanFilter{
		NodeID:          task.MachineID,
		ClusterNodeName: task.ClusterNodeName,
		RuntimeID:       task.RuntimeID,
		Mode:            model.ImageCachePruneModeDelete,
	}, imageCachePrunePlanOptions{skipNodeUpdateTaskID: task.ID})
	if err != nil {
		return "", err
	}
	for _, target := range targets {
		candidate, ok := imageCachePrunePlanCandidateForTaskTarget(plan.Candidates, target)
		if !ok {
			return fmt.Sprintf("refuse prune-image-cache delete task before execution: target %s is not present in the latest prune plan for node %s; it is stale or protected by current control-plane state", target.String(), firstNonEmptyImageAPIString(task.ClusterNodeName, task.MachineID, task.RuntimeID)), nil
		}
		if unsafe := imageCacheAutomaticDeleteUnsafeCandidateReason(candidate.Reason); unsafe != "" {
			return fmt.Sprintf("refuse prune-image-cache delete task before execution: target %s now has unsafe candidate reason %q", target.String(), candidate.Reason), nil
		}
		if candidate.Protected {
			return fmt.Sprintf("refuse prune-image-cache delete task before execution: target %s is protected by %q", target.String(), candidate.SkipReason), nil
		}
	}
	return "", nil
}

func (s *Server) refuseUnsafeReplicateAppImageTaskClaim(task model.NodeUpdateTask) (string, error) {
	imageID := strings.TrimSpace(task.Payload["image_id"])
	if imageID == "" {
		return "", nil
	}
	image, err := s.store.GetImage(imageID, "", true)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "refuse replicate-app-image task before execution: image no longer exists", nil
		}
		return "", err
	}
	priority := strings.TrimSpace(task.Payload["priority"])
	if priority != model.ImageReplicationPriorityDeployBlocking {
		switch strings.TrimSpace(image.LifecycleState) {
		case model.ImageLifecycleDeleting, model.ImageLifecycleDeleted, model.ImageLifecycleLost:
			return "refuse replicate-app-image task before execution: image generation is no longer eligible for replication", nil
		}
	}
	replicationTaskID := strings.TrimSpace(task.Payload["replication_task_id"])
	if replicationTaskID == "" {
		return "", nil
	}
	tasks, err := s.store.ListImageReplicationTasks(model.ImageReplicationTaskFilter{ImageID: imageID, PlatformAdmin: true})
	if err != nil {
		return "", err
	}
	for _, replicationTask := range tasks {
		if replicationTask.ID != replicationTaskID {
			continue
		}
		switch strings.TrimSpace(replicationTask.Status) {
		case model.ImageReplicationTaskStatusPending, model.ImageReplicationTaskStatusRunning:
			return "", nil
		default:
			return "refuse replicate-app-image task before execution: linked replication task is no longer pending", nil
		}
	}
	return "refuse replicate-app-image task before execution: linked replication task no longer exists", nil
}

type imageCachePruneTaskValidationTarget struct {
	Repo     string `json:"repo"`
	Target   string `json:"target"`
	Digest   string `json:"digest"`
	ImageRef string `json:"image_ref"`
}

func (target imageCachePruneTaskValidationTarget) String() string {
	if strings.TrimSpace(target.ImageRef) != "" {
		return strings.TrimSpace(target.ImageRef)
	}
	if strings.TrimSpace(target.Repo) != "" && strings.TrimSpace(target.Digest) != "" {
		return strings.Trim(strings.TrimSpace(target.Repo), "/") + "@" + strings.TrimSpace(target.Digest)
	}
	if strings.TrimSpace(target.Repo) != "" && strings.TrimSpace(target.Target) != "" {
		return strings.Trim(strings.TrimSpace(target.Repo), "/") + ":" + strings.TrimSpace(target.Target)
	}
	return strings.TrimSpace(target.Digest)
}

func imageCachePruneTaskValidationTargets(payload map[string]string) ([]imageCachePruneTaskValidationTarget, error) {
	out := []imageCachePruneTaskValidationTarget{}
	if raw := strings.TrimSpace(payload["targets_json"]); raw != "" {
		var targets []imageCachePruneTaskValidationTarget
		if err := json.Unmarshal([]byte(raw), &targets); err != nil {
			return nil, fmt.Errorf("decode image-cache prune targets_json: %w", err)
		}
		out = append(out, targets...)
	}
	if imageRef := strings.TrimSpace(payload["image_ref"]); imageRef != "" || strings.TrimSpace(payload["digest"]) != "" {
		out = append(out, imageCachePruneTaskValidationTarget{
			ImageRef: imageRef,
			Digest:   strings.TrimSpace(payload["digest"]),
		})
	}
	return out, nil
}

func imageCachePrunePlanCandidateForTaskTarget(candidates []model.ImageCachePruneCandidate, target imageCachePruneTaskValidationTarget) (model.ImageCachePruneCandidate, bool) {
	targetKeys := manifestReferenceKeys(target.Repo, target.Target, target.Digest, target.ImageRef)
	if len(targetKeys) == 0 {
		targetKeys = imageReferenceKeys(target.ImageRef, target.Digest)
	}
	for _, candidate := range candidates {
		candidateKeys := manifestReferenceKeys(candidate.Repo, candidate.Target, candidate.Digest, candidate.ImageRef)
		if keySetContainsAny(keySetFromValues(candidateKeys), targetKeys...) {
			return candidate, true
		}
	}
	return model.ImageCachePruneCandidate{}, false
}

func keySetFromValues(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out[value] = struct{}{}
	}
	return out
}

func imageCacheAutomaticDeleteUnsafeCandidateReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case "missing_control_plane_image", "lost_image", "deleted_image_generation", "stale_replica", "excess_replica":
		return ""
	case "":
		return "missing candidate reason"
	default:
		return "unsafe candidate reason " + reason
	}
}

func nodeUpdatePayloadBool(value string) bool {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func (s *Server) handleNodeUpdaterLogTask(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	message, err := decodeNodeUpdaterMessage(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	task, err := s.store.AppendNodeUpdateTaskLog(r.PathValue("id"), principal.ActorID, message)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"task": task})
}

func (s *Server) handleNodeUpdaterCompleteTask(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	req, err := decodeNodeUpdaterCompleteRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	task, err := s.store.CompleteNodeUpdateTask(r.PathValue("id"), principal.ActorID, req.Status, req.Message, req.ErrorMessage)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendNodeUpdateTaskMaintenanceAudit(principal, task)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"task": task})
}

func (s *Server) appendNodeUpdateTaskMaintenanceAudit(principal model.Principal, task model.NodeUpdateTask) {
	action := ""
	switch task.Type {
	case model.NodeUpdateTaskTypePruneImageCache:
		if task.Status == model.NodeUpdateTaskStatusCompleted && taskPayloadTruthy(task.Payload, "dry_run") {
			action = "image_cache_orphan_prune_dry_run_completed"
		} else if task.Status == model.NodeUpdateTaskStatusCompleted && taskPayloadTruthy(task.Payload, "allow_delete") {
			action = "image_cache_orphan_prune_delete_completed"
		} else if task.Status == model.NodeUpdateTaskStatusFailed {
			action = "image_cache_orphan_prune_failed"
		}
	case model.NodeUpdateTaskTypeDecommissionLocalPV:
		if task.Status == model.NodeUpdateTaskStatusCompleted && taskPayloadTruthy(task.Payload, "dry_run") {
			action = "localpv_decommission_dry_run_completed"
		} else if task.Status == model.NodeUpdateTaskStatusCompleted && taskPayloadTruthy(task.Payload, "allow_delete") {
			action = "localpv_decommission_completed"
		} else if task.Status == model.NodeUpdateTaskStatusFailed {
			action = "localpv_decommission_refused"
		}
	default:
		return
	}
	if action == "" {
		return
	}
	metadata := map[string]string{
		"task_id":           task.ID,
		"task_type":         task.Type,
		"status":            task.Status,
		"node_updater_id":   task.NodeUpdaterID,
		"node_id":           task.MachineID,
		"cluster_node_name": task.ClusterNodeName,
		"runtime_id":        task.RuntimeID,
		"dry_run":           task.Payload["dry_run"],
		"allow_delete":      task.Payload["allow_delete"],
	}
	for _, key := range []string{
		"prune_plan_id",
		"max_delete_bytes",
		"min_manifest_age",
		"include_unreferenced_blobs",
		"candidate_blob_count",
		"candidate_blob_bytes",
		"expected_image_size_bytes",
		"expected_lv_count",
		"expected_bound_pv_count",
		"allow_localpv_decommission",
	} {
		if value := strings.TrimSpace(task.Payload[key]); value != "" {
			metadata[key] = value
		}
	}
	if task.ResultMessage != "" {
		metadata["result_message"] = truncateAuditValue(task.ResultMessage, 600)
	}
	if task.ErrorMessage != "" {
		metadata["error_message"] = truncateAuditValue(task.ErrorMessage, 600)
	}
	s.appendAudit(principal, action, "node_update_task", task.ID, task.TenantID, metadata)
}

func taskPayloadTruthy(payload map[string]string, key string) bool {
	switch strings.TrimSpace(strings.ToLower(payload[key])) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func truncateAuditValue(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit] + "...truncated"
}

type nodeUpdaterEnrollRequest struct {
	NodeKey            string            `json:"node_key"`
	NodeName           string            `json:"node_name"`
	RuntimeName        string            `json:"runtime_name"`
	MachineName        string            `json:"machine_name"`
	MachineFingerprint string            `json:"machine_fingerprint"`
	Endpoint           string            `json:"endpoint"`
	Labels             map[string]string `json:"labels"`
	UpdaterVersion     string            `json:"updater_version"`
	JoinScriptVersion  string            `json:"join_script_version"`
	Capabilities       []string          `json:"capabilities"`
}

type nodeUpdaterHeartbeatRequest struct {
	Labels              map[string]string `json:"labels"`
	Capabilities        []string          `json:"capabilities"`
	UpdaterVersion      string            `json:"updater_version"`
	JoinScriptVersion   string            `json:"join_script_version"`
	K3SVersion          string            `json:"k3s_version"`
	K3SServer           string            `json:"k3s_server"`
	K3SFallbackServers  string            `json:"k3s_fallback_servers"`
	RegistryMirror      string            `json:"registry_mirror"`
	LabelsHash          string            `json:"labels_hash"`
	TaintsHash          string            `json:"taints_hash"`
	EdgeEnvGeneration   string            `json:"edge_env_generation"`
	DNSEnvGeneration    string            `json:"dns_env_generation"`
	ConfigHash          string            `json:"config_hash"`
	DiscoveryGeneration string            `json:"discovery_generation"`
	OS                  string            `json:"os"`
	Arch                string            `json:"arch"`
	LastError           string            `json:"last_error"`
}

func decodeNodeUpdaterEnrollRequest(r *http.Request) (nodeUpdaterEnrollRequest, bool, error) {
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return nodeUpdaterEnrollRequest{}, false, err
		}
		return nodeUpdaterEnrollRequest{
			NodeKey:            r.Form.Get("node_key"),
			NodeName:           r.Form.Get("node_name"),
			RuntimeName:        r.Form.Get("runtime_name"),
			MachineName:        r.Form.Get("machine_name"),
			MachineFingerprint: r.Form.Get("machine_fingerprint"),
			Endpoint:           r.Form.Get("endpoint"),
			Labels:             parseCSVLabels(r.Form.Get("labels")),
			UpdaterVersion:     r.Form.Get("updater_version"),
			JoinScriptVersion:  r.Form.Get("join_script_version"),
			Capabilities:       parseCSVList(r.Form.Get("capabilities")),
		}, true, nil
	}
	var req nodeUpdaterEnrollRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return nodeUpdaterEnrollRequest{}, false, err
	}
	return req, false, nil
}

func decodeNodeUpdaterHeartbeatRequest(r *http.Request) (nodeUpdaterHeartbeatRequest, bool, error) {
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return nodeUpdaterHeartbeatRequest{}, false, err
		}
		return nodeUpdaterHeartbeatRequest{
			Labels:              parseCSVLabels(r.Form.Get("labels")),
			Capabilities:        parseCSVList(r.Form.Get("capabilities")),
			UpdaterVersion:      r.Form.Get("updater_version"),
			JoinScriptVersion:   r.Form.Get("join_script_version"),
			K3SVersion:          r.Form.Get("k3s_version"),
			K3SServer:           r.Form.Get("k3s_server"),
			K3SFallbackServers:  r.Form.Get("k3s_fallback_servers"),
			RegistryMirror:      r.Form.Get("registry_mirror"),
			LabelsHash:          r.Form.Get("labels_hash"),
			TaintsHash:          r.Form.Get("taints_hash"),
			EdgeEnvGeneration:   r.Form.Get("edge_env_generation"),
			DNSEnvGeneration:    r.Form.Get("dns_env_generation"),
			ConfigHash:          r.Form.Get("config_hash"),
			DiscoveryGeneration: r.Form.Get("discovery_generation"),
			OS:                  r.Form.Get("os"),
			Arch:                r.Form.Get("arch"),
			LastError:           r.Form.Get("last_error"),
		}, true, nil
	}
	var req nodeUpdaterHeartbeatRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return nodeUpdaterHeartbeatRequest{}, false, err
	}
	return req, false, nil
}

func decodeNodeUpdaterMessage(r *http.Request) (string, error) {
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return "", err
		}
		return r.Form.Get("message"), nil
	}
	var req struct {
		Message string `json:"message"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return "", err
	}
	return req.Message, nil
}

func decodeNodeUpdaterCompleteRequest(r *http.Request) (struct {
	Status       string `json:"status"`
	Message      string `json:"message"`
	ErrorMessage string `json:"error_message"`
}, error) {
	var req struct {
		Status       string `json:"status"`
		Message      string `json:"message"`
		ErrorMessage string `json:"error_message"`
	}
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return req, err
		}
		req.Status = r.Form.Get("status")
		req.Message = r.Form.Get("message")
		req.ErrorMessage = r.Form.Get("error_message")
		return req, nil
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return req, err
	}
	return req, nil
}

func writeNodeUpdaterEnrollEnv(w http.ResponseWriter, updater model.NodeUpdater, token string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "FUGUE_NODE_UPDATER_ID=%s\n", shellQuote(updater.ID))
	fmt.Fprintf(w, "FUGUE_NODE_UPDATER_TOKEN=%s\n", shellQuote(token))
	fmt.Fprintf(w, "FUGUE_NODE_UPDATER_STATUS=%s\n", shellQuote(updater.Status))
	fmt.Fprintf(w, "FUGUE_NODE_UPDATER_RUNTIME_ID=%s\n", shellQuote(updater.RuntimeID))
	fmt.Fprintf(w, "FUGUE_NODE_UPDATER_CLUSTER_NODE_NAME=%s\n", shellQuote(updater.ClusterNodeName))
}

func (s *Server) nodeUpdateTaskForDelivery(task *model.NodeUpdateTask) *model.NodeUpdateTask {
	if task == nil || len(task.Payload) == 0 {
		return task
	}
	switch task.Type {
	case model.NodeUpdateTaskTypePrepullAppImages, model.NodeUpdateTaskTypeReplicateAppImage, model.NodeUpdateTaskTypeVerifyImageCache:
	default:
		return task
	}
	normalized := *task
	normalized.Payload = cloneStringMap(task.Payload)
	for _, key := range []string{"images", "image_ref"} {
		value := strings.TrimSpace(normalized.Payload[key])
		if value == "" {
			continue
		}
		normalized.Payload[key] = s.nodeUpdaterPrepullImageRefsForDelivery(value)
	}
	return &normalized
}

func (s *Server) nodeUpdaterPrepullImageRefsForDelivery(raw string) string {
	parts := strings.Fields(strings.NewReplacer(",", " ").Replace(raw))
	if len(parts) == 0 {
		return strings.TrimSpace(raw)
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		out = append(out, s.nodeUpdaterPrepullImageRefForDelivery(part))
	}
	return strings.Join(out, ",")
}

func (s *Server) nodeUpdaterPrepullImageRefForDelivery(imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	if s == nil || imageRef == "" {
		return imageRef
	}
	managedRef := strings.TrimSpace(appimages.ManagedRegistryRefFromRuntimeImageRef(imageRef, s.registryPushBase, s.registryPullBase))
	if managedRef == "" {
		return imageRef
	}
	runtimeRef := strings.TrimSpace(appimages.RuntimeImageRefFromManagedRef(managedRef, s.registryPushBase, s.registryPullBase))
	if runtimeRef == "" {
		return imageRef
	}
	return runtimeRef
}

func writeNodeUpdateTaskEnv(w http.ResponseWriter, task *model.NodeUpdateTask) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if task == nil {
		fmt.Fprintf(w, "FUGUE_NODE_UPDATE_TASK_ID=''\n")
		return
	}
	fmt.Fprintf(w, "FUGUE_NODE_UPDATE_TASK_ID=%s\n", shellQuote(task.ID))
	fmt.Fprintf(w, "FUGUE_NODE_UPDATE_TASK_TYPE=%s\n", shellQuote(task.Type))
	keys := make([]string, 0, len(task.Payload))
	for key := range task.Payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		envKey := "FUGUE_NODE_UPDATE_TASK_" + envKeyName(key)
		fmt.Fprintf(w, "%s=%s\n", envKey, shellQuote(task.Payload[key]))
	}
}

func firstNodeUpdateTask(tasks []model.NodeUpdateTask) *model.NodeUpdateTask {
	if len(tasks) == 0 {
		return nil
	}
	return &tasks[0]
}

func parseCSVList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	out := []string{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parsePositiveInt(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envKeyName(key string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(key) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - ('a' - 'A'))
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

func (s *Server) nodeUpdaterInstallScript(apiBase string) string {
	script := `#!/usr/bin/env bash
set -euo pipefail

FUGUE_API_BASE="${FUGUE_API_BASE:-__FUGUE_API_BASE__}"
FUGUE_NODE_UPDATER_SCRIPT_VERSION="v18"
FUGUE_NODE_UPDATER_VERSION="${FUGUE_NODE_UPDATER_SCRIPT_VERSION}"
FUGUE_NODE_UPDATER_CAPABILITIES="heartbeat,tasks,refresh-join-config,restart-k3s-agent,upgrade-k3s-agent,upgrade-node-updater,diagnose-node,install-nfs-client-tools,prepull-system-images,prepull-app-images,replicate-app-image,verify-image-cache,prune-image-cache,report-image-cache-inventory,report-lvm-localpv-inventory,decommission-lvm-localpv,verify-systemd-escape-hatch,time-sync"
FUGUE_NODE_UPDATER_WORK_DIR="${FUGUE_NODE_UPDATER_WORK_DIR:-/var/lib/fugue-node-updater}"
FUGUE_NODE_UPDATER_LAST_ERROR_FILE="${FUGUE_NODE_UPDATER_LAST_ERROR_FILE:-${FUGUE_NODE_UPDATER_WORK_DIR}/last-error}"
FUGUE_NODE_UPDATER_STATE_DIR="${FUGUE_NODE_UPDATER_STATE_DIR:-${FUGUE_NODE_UPDATER_WORK_DIR}}"
FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE="${FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE:-${FUGUE_NODE_UPDATER_STATE_DIR}/discovery-bundle.json}"
FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE="${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE:-${FUGUE_NODE_UPDATER_STATE_DIR}/discovery.env}"
FUGUE_NODE_UPDATER_DESIRED_STATE_FILE="${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE:-${FUGUE_NODE_UPDATER_STATE_DIR}/desired-state.json}"
FUGUE_NODE_UPDATER_STATE_ENV_FILE="${FUGUE_NODE_UPDATER_STATE_ENV_FILE:-${FUGUE_NODE_UPDATER_STATE_DIR}/state.env}"
FUGUE_NODE_UPDATER_K3S_CONFIG_FILE="${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE:-/etc/rancher/k3s/config.yaml}"
FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE="${FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE:-/etc/rancher/k3s/registries.yaml}"
FUGUE_NODE_UPDATER_EDGE_ENV_FILE="${FUGUE_NODE_UPDATER_EDGE_ENV_FILE:-/etc/fugue/fugue-edge.env}"
FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE="${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE:-/etc/fugue/edge-node.env}"
FUGUE_NODE_UPDATER_DNS_ENV_FILE="${FUGUE_NODE_UPDATER_DNS_ENV_FILE:-/etc/fugue/fugue-dns.env}"
FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_CONFIG_FILE="${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_CONFIG_FILE:-/etc/dnsmasq.d/fugue-node-dns-escape-hatch.conf}"
FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_SERVICE="${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_SERVICE:-fugue-node-dns-escape-hatch.service}"
FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_TIMER="${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_TIMER:-fugue-node-dns-escape-hatch.timer}"
FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_ENABLED="${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_ENABLED:-false}"
FUGUE_NODE_UPDATER_TIMESYNCD_DROPIN="${FUGUE_NODE_UPDATER_TIMESYNCD_DROPIN:-/etc/systemd/timesyncd.conf.d/10-fugue-managed.conf}"
FUGUE_NODE_UPDATER_TIMESYNCD_MIN_POLL_SEC="${FUGUE_NODE_UPDATER_TIMESYNCD_MIN_POLL_SEC:-32}"
FUGUE_NODE_UPDATER_TIMESYNCD_MAX_POLL_SEC="${FUGUE_NODE_UPDATER_TIMESYNCD_MAX_POLL_SEC:-64}"
FUGUE_NODE_UPDATER_CLOCK_SKEW_REPAIR_THRESHOLD_SEC="${FUGUE_NODE_UPDATER_CLOCK_SKEW_REPAIR_THRESHOLD_SEC:-5}"
FUGUE_LOCALPV_VG_NAME="${FUGUE_LOCALPV_VG_NAME:-fugue-vg}"
FUGUE_LOCALPV_IMAGE_PATH="${FUGUE_LOCALPV_IMAGE_PATH:-/var/lib/fugue/lvm-localpv/${FUGUE_LOCALPV_VG_NAME}.img}"
FUGUE_LOCALPV_LOOP_SERVICE="${FUGUE_LOCALPV_LOOP_SERVICE:-fugue-lvm-localpv-loop.service}"

log() {
  printf '[fugue-node-updater] %s\n' "$*" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 1
  }
}

k3s_version() {
  if command -v k3s >/dev/null 2>&1; then
    k3s --version 2>/dev/null | head -n 1 || true
  fi
}

last_error() {
  if [ -f "${FUGUE_NODE_UPDATER_LAST_ERROR_FILE}" ]; then
    head -c 2000 "${FUGUE_NODE_UPDATER_LAST_ERROR_FILE}" 2>/dev/null || true
  fi
}

clear_last_error() {
  rm -f "${FUGUE_NODE_UPDATER_LAST_ERROR_FILE}" 2>/dev/null || true
}

truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

write_file_if_changed() {
  local source_path="$1"
  local target_path="$2"
  if [ -f "${target_path}" ] && cmp -s "${source_path}" "${target_path}"; then
    rm -f "${source_path}"
    return 1
  fi
  install -m 0644 "${source_path}" "${target_path}"
  rm -f "${source_path}"
  return 0
}

write_secret_file_if_changed() {
  local source_path="$1"
  local target_path="$2"
  if [ -f "${target_path}" ] && cmp -s "${source_path}" "${target_path}"; then
    rm -f "${source_path}"
    chmod 0600 "${target_path}" 2>/dev/null || true
    return 1
  fi
  install -m 0600 "${source_path}" "${target_path}"
  rm -f "${source_path}"
  return 0
}

preserve_rollback_file() {
  local path="$1"
  if [ -r "${path}" ]; then
    cp -p "${path}" "${path}.rollback" 2>/dev/null || true
  fi
}

sha256_file() {
  local path="$1"
  if [ ! -r "${path}" ]; then
    return 1
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${path}" | awk '{print $1}'
    return 0
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${path}" | awk '{print $1}'
    return 0
  fi
  return 1
}

sha256_text() {
  local value="$1"
  local tmp=""
  tmp="$(mktemp)"
  printf '%s' "${value}" >"${tmp}"
  if sha256_file "${tmp}"; then
    rm -f "${tmp}"
    return 0
  fi
  rm -f "${tmp}"
  return 1
}

load_cached_env_file() {
  local path="$1"
  if [ -r "${path}" ]; then
    # shellcheck disable=SC1090
    . "${path}"
    return 0
  fi
  return 1
}

detect_public_ip() {
  if command -v curl >/dev/null 2>&1; then
    local ip=""
    ip="$(curl -fsS --max-time 5 https://api.ipify.org 2>/dev/null || true)"
    if [ -n "${ip}" ]; then
      printf '%s' "${ip}"
      return 0
    fi
  fi
  if command -v ip >/dev/null 2>&1; then
    ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}'
    return 0
  fi
  return 1
}

parse_yaml_list_value_hash() {
  local file="$1"
  local key="$2"
  local tmp=""
  if [ ! -r "${file}" ]; then
    return 1
  fi
  tmp="$(mktemp)"
  awk -v key="${key}" '
    $0 ~ "^[[:space:]]*" key ":[[:space:]]*$" { in_block = 1; next }
    in_block && $0 ~ "^[^[:space:]]" { exit }
    in_block && $0 ~ "^[[:space:]]*-[[:space:]]*" {
      line = $0
      sub(/^[[:space:]]*-[[:space:]]*"/, "", line)
      sub(/"$/, "", line)
      print line
    }
  ' "${file}" | sort >"${tmp}"
  if [ ! -s "${tmp}" ]; then
    rm -f "${tmp}"
    return 1
  fi
  if sha256_file "${tmp}"; then
    rm -f "${tmp}"
    return 0
  fi
  rm -f "${tmp}"
  return 1
}

json_quote_env() {
  local value="$1"
  value="${value//\'/\'\\\'\'}"
  printf "'%s'" "${value}"
}

render_discovery_env() {
  local bundle_file="$1"
  if command -v python3 >/dev/null 2>&1; then
    python3 - "${bundle_file}" <<'PY_DISCOVERY'
import base64
import hashlib
import hmac
import json
import os
import shlex
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    bundle = json.load(fh)

schema_version = str(bundle.get("schema_version", "")).strip()
if not schema_version:
    raise SystemExit("DiscoveryBundle schema_version is empty")
if schema_version.split(".", 1)[0] != "1":
    raise SystemExit(f"unsupported DiscoveryBundle schema_version: {schema_version}")
if not str(bundle.get("signature", "")).strip():
    raise SystemExit("DiscoveryBundle signature is empty")

def payload_for_signature(key_id, valid_until):
    payload = {}
    for key in (
        "schema_version",
        "generation",
        "previous_generation",
        "generated_at",
        "valid_until",
        "issuer",
        "key_id",
        "api_endpoints",
        "kubernetes",
        "registry",
        "edge_groups",
        "edge_nodes",
        "dns_nodes",
        "platform_routes",
        "public_runtime_env",
    ):
        value = bundle.get(key)
        if key == "valid_until":
            value = valid_until
        elif key == "key_id":
            value = key_id
        if value in ("", None, [], {}):
            continue
        payload[key] = value
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def verify_signature():
    keys = {}
    active_key = os.environ.get("FUGUE_BUNDLE_SIGNING_KEY", "").strip()
    active_key_id = os.environ.get("FUGUE_BUNDLE_SIGNING_KEY_ID", bundle.get("key_id", "")).strip()
    previous_key = os.environ.get("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY", "").strip()
    previous_key_id = os.environ.get("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID", "").strip()
    if active_key and active_key_id:
        keys[active_key_id.lower()] = active_key
    if previous_key and previous_key_id:
        keys[previous_key_id.lower()] = previous_key
    if not keys:
        return
    revoked = {item.strip().lower() for item in os.environ.get("FUGUE_BUNDLE_REVOKED_KEY_IDS", "").replace(";", ",").split(",") if item.strip()}
    candidates = [(bundle.get("key_id", ""), bundle.get("signature", ""), bundle.get("valid_until", ""))]
    for item in bundle.get("signatures") or []:
        candidates.append((item.get("key_id", ""), item.get("signature", ""), item.get("valid_until", bundle.get("valid_until", ""))))
    for key_id, signature, valid_until in candidates:
        key_id = str(key_id or "").strip()
        signature = str(signature or "").strip()
        if not key_id or not signature or key_id.lower() in revoked:
            continue
        key = keys.get(key_id.lower())
        if not key:
            continue
        digest = hmac.new(key.encode("utf-8"), payload_for_signature(key_id, valid_until), hashlib.sha256).digest()
        expected = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        if hmac.compare_digest(signature, expected):
            return
    raise SystemExit("DiscoveryBundle signature verification failed")

verify_signature()

def first_named(items, name):
    for item in items or []:
        if str(item.get("name", "")).strip() == name:
            return item
    return {}

def emit(key, value):
    if value is None:
        value = ""
    if isinstance(value, list):
        value = ",".join(str(item) for item in value)
    print(f"{key}={shlex.quote(str(value))}")

runtime = bundle.get("public_runtime_env") or {}
kube = first_named(bundle.get("kubernetes"), "cluster-join")
registry = first_named(bundle.get("registry"), "registry")
emit("FUGUE_DISCOVERY_SCHEMA_VERSION", bundle.get("schema_version", ""))
emit("FUGUE_DISCOVERY_GENERATION", bundle.get("generation", ""))
emit("FUGUE_DISCOVERY_GENERATED_AT", bundle.get("generated_at", ""))
emit("FUGUE_DISCOVERY_VALID_UNTIL", bundle.get("valid_until", ""))
emit("FUGUE_DISCOVERY_ISSUER", bundle.get("issuer", ""))
emit("FUGUE_DISCOVERY_API_URL", runtime.get("FUGUE_API_URL") or first_named(bundle.get("api_endpoints"), "public").get("url", ""))
emit("FUGUE_DISCOVERY_APP_BASE_DOMAIN", runtime.get("FUGUE_APP_BASE_DOMAIN", ""))
emit("FUGUE_DISCOVERY_REGISTRY_PUSH_BASE", runtime.get("FUGUE_REGISTRY_PUSH_BASE") or registry.get("push_base", ""))
emit("FUGUE_DISCOVERY_REGISTRY_PULL_BASE", runtime.get("FUGUE_REGISTRY_PULL_BASE") or registry.get("pull_base", ""))
emit("FUGUE_DISCOVERY_REGISTRY_MIRROR", runtime.get("FUGUE_CLUSTER_JOIN_REGISTRY_ENDPOINT") or registry.get("mirror", ""))
emit("FUGUE_DISCOVERY_K3S_SERVER", kube.get("server", ""))
emit("FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS", ",".join(kube.get("fallback_servers") or []))
emit("FUGUE_DISCOVERY_K3S_CA_HASH", kube.get("ca_hash", ""))
emit("FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT", kube.get("registry_endpoint", ""))
emit("FUGUE_DISCOVERY_SMOKE_URL", runtime.get("FUGUE_SMOKE_URL", ""))
PY_DISCOVERY
    return 0
  fi
  local generation=""
  local generated_at=""
  local schema_version=""
  schema_version="$(sed -n 's/.*"schema_version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${bundle_file}" | head -n 1)"
  case "${schema_version}" in
    1|1.*) ;;
    *) echo "unsupported DiscoveryBundle schema_version: ${schema_version:-missing}" >&2; return 1 ;;
  esac
  generation="$(sed -n 's/.*"generation"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${bundle_file}" | head -n 1)"
  generated_at="$(sed -n 's/.*"generated_at"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${bundle_file}" | head -n 1)"
  printf 'FUGUE_DISCOVERY_SCHEMA_VERSION=%s\n' "$(json_quote_env "${schema_version}")"
  printf 'FUGUE_DISCOVERY_GENERATION=%s\n' "$(json_quote_env "${generation}")"
  printf 'FUGUE_DISCOVERY_GENERATED_AT=%s\n' "$(json_quote_env "${generated_at}")"
}

discovery_bundle_not_older_than_cache() {
  local candidate="$1"
  local current="${FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE}"
  if [ ! -r "${current}" ] || ! command -v python3 >/dev/null 2>&1; then
    return 0
  fi
  python3 - "${current}" "${candidate}" <<'PY_DISCOVERY_ROLLBACK'
import json
import os
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    current = json.load(fh)
with open(sys.argv[2], "r", encoding="utf-8") as fh:
    candidate = json.load(fh)

current_generated_at = str(current.get("generated_at", "")).strip()
candidate_generated_at = str(candidate.get("generated_at", "")).strip()
if current_generated_at and candidate_generated_at and candidate_generated_at < current_generated_at:
    raise SystemExit("candidate DiscoveryBundle generated_at is older than cached bundle")
PY_DISCOVERY_ROLLBACK
}

fetch_discovery_bundle() {
  local tmp=""
  local env_tmp=""
  mkdir -p "${FUGUE_NODE_UPDATER_STATE_DIR}"
  tmp="$(mktemp)"
  if ! curl -fsSL --retry 3 --retry-delay 2 "${FUGUE_API_BASE}/v1/discovery/bundle" -o "${tmp}"; then
    rm -f "${tmp}"
    return 1
  fi
  env_tmp="$(mktemp)"
  if discovery_bundle_not_older_than_cache "${tmp}" && render_discovery_env "${tmp}" >"${env_tmp}"; then
    write_file_if_changed "${tmp}" "${FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE}" || true
    write_file_if_changed "${env_tmp}" "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}" || true
  else
    rm -f "${tmp}"
    rm -f "${env_tmp}"
    return 1
  fi
  rm -f "${tmp}" "${env_tmp}"
}

render_node_updater_state_env() {
  local tmp=""
  tmp="$(mktemp)"
  {
    printf 'FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE=%s\n' "$(json_quote_env "${FUGUE_NODE_UPDATER_DISCOVERY_BUNDLE_FILE}")"
    printf 'FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE=%s\n' "$(json_quote_env "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}")"
    printf 'FUGUE_NODE_UPDATER_DESIRED_STATE_FILE=%s\n' "$(json_quote_env "${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}")"
    printf 'FUGUE_NODE_UPDATER_STATE_ENV_FILE=%s\n' "$(json_quote_env "${FUGUE_NODE_UPDATER_STATE_ENV_FILE}")"
  } >"${tmp}"
  write_file_if_changed "${tmp}" "${FUGUE_NODE_UPDATER_STATE_ENV_FILE}" || true
}

fetch_node_policy_desired_state() {
  local tmp=""
  mkdir -p "${FUGUE_NODE_UPDATER_STATE_DIR}"
  tmp="$(mktemp)"
  if ! curl -fsSL --retry 3 --retry-delay 2 \
    -H "Authorization: Bearer ${FUGUE_NODE_UPDATER_TOKEN:?FUGUE_NODE_UPDATER_TOKEN is required}" \
    "${FUGUE_API_BASE}/v1/node-updater/desired-state" \
    -o "${tmp}"; then
    rm -f "${tmp}"
    return 1
  fi
  write_file_if_changed "${tmp}" "${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}" || true
  return 0
}

yaml_update_scalar() {
  local file="$1"
  local key="$2"
  local value="$3"
  local tmp=""
  local changed=1
  tmp="$(mktemp)"
  if [ ! -r "${file}" ]; then
    printf '%s: "%s"\n' "${key}" "${value}" >"${tmp}"
    write_file_if_changed "${tmp}" "${file}"
    return $?
  fi
  preserve_rollback_file "${file}"
  awk -v key="${key}" -v value="${value}" '
    BEGIN { done = 0 }
    $0 ~ "^[[:space:]]*" key ":[[:space:]]*" {
      print key ": \"" value "\""
      done = 1
      next
    }
    { print }
    END {
      if (!done) {
        print key ": \"" value "\""
      }
    }
  ' "${file}" >"${tmp}"
  write_file_if_changed "${tmp}" "${file}"
  changed=$?
  return "${changed}"
}

yaml_delete_scalar() {
  local file="$1"
  local key="$2"
  local tmp=""
  if [ ! -r "${file}" ]; then
    return 1
  fi
  if ! grep -Eq "^[[:space:]]*${key}:[[:space:]]*" "${file}"; then
    return 1
  fi
  preserve_rollback_file "${file}"
  tmp="$(mktemp)"
  awk -v key="${key}" '
    $0 ~ "^[[:space:]]*" key ":[[:space:]]*" { next }
    { print }
  ' "${file}" >"${tmp}"
  write_file_if_changed "${tmp}" "${file}"
}

yaml_append_list_block() {
  local target_file="$1"
  local key="$2"
  local values_file="$3"
  local value=""
  local escaped=""
  if [ ! -s "${values_file}" ]; then
    return 0
  fi
  printf '%s:\n' "${key}" >>"${target_file}"
  while IFS= read -r value; do
    [ -n "${value}" ] || continue
    escaped="${value//\\/\\\\}"
    escaped="${escaped//\"/\\\"}"
    printf '  - "%s"\n' "${escaped}" >>"${target_file}"
  done <"${values_file}"
}

yaml_update_node_policy_blocks() {
  local file="$1"
  local labels_file="$2"
  local taints_file="$3"
  local tmp=""
  tmp="$(mktemp)"
  if [ -r "${file}" ]; then
    preserve_rollback_file "${file}"
    awk '
      $0 ~ "^[[:space:]]*(node-label|node-taint):[[:space:]]*$" { in_block = 1; next }
      in_block && $0 ~ "^[^[:space:]]" { in_block = 0 }
      !in_block { print }
    ' "${file}" >"${tmp}"
  fi
  yaml_append_list_block "${tmp}" node-label "${labels_file}"
  yaml_append_list_block "${tmp}" node-taint "${taints_file}"
  write_file_if_changed "${tmp}" "${file}"
}

desired_node_policy_label() {
  local key="$1"
  local state_file="${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}"
  if [ ! -r "${state_file}" ] || ! command -v python3 >/dev/null 2>&1; then
    return 1
  fi
  python3 - "${state_file}" "${key}" <<'PY_NODE_LABEL'
import json
import os
import sys

state_path, key = sys.argv[1:3]
with open(state_path, "r", encoding="utf-8") as fh:
    envelope = json.load(fh)
state = envelope.get("desired_state") or {}
labels = ((state.get("node_policy") or {}).get("labels") or {})
edge_credential = state.get("edge_credential") or {}
value = str(labels.get(key) or "").strip()
if not value and key == "fugue.io/public-ip":
    value = str(edge_credential.get("public_ipv4") or edge_credential.get("public_ipv6") or "").strip()
if not value:
    raise SystemExit(1)
print(value)
PY_NODE_LABEL
}

render_desired_k3s_policy_lists() {
  local labels_file="$1"
  local taints_file="$2"
  local state_file="${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}"
  if [ ! -r "${state_file}" ] || ! command -v python3 >/dev/null 2>&1; then
    return 1
  fi
  FUGUE_DETECTED_PUBLIC_IP="$(detect_public_ip || true)" python3 - "${state_file}" "${labels_file}" "${taints_file}" <<'PY_NODE_POLICY'
import json
import os
import sys

state_path, labels_path, taints_path = sys.argv[1:4]
with open(state_path, "r", encoding="utf-8") as fh:
    envelope = json.load(fh)

state = envelope.get("desired_state") or {}
node_policy = state.get("node_policy") or {}
policy = node_policy.get("policy") or {}
current_labels = node_policy.get("labels") or {}
node_updater = state.get("node_updater") or {}
edge_credential = state.get("edge_credential") or {}

def truthy(value):
    return value is True or str(value).strip().lower() == "true"

def first(*values):
    for value in values:
        value = str(value or "").strip()
        if value:
            return value
    return ""

def edge_workload_mode():
    current = str(current_labels.get("fugue.io/edge-workload") or "").strip().lower()
    if current in {"static", "dynamic"}:
        return current
    if truthy(policy.get("allow_dns")) or truthy(policy.get("effective_dns")) or truthy(current_labels.get("fugue.io/role.dns")):
        return "static"
    credential = str(edge_credential.get("workload_mode") or "").strip().lower()
    if credential in {"static", "dynamic"}:
        return credential
    dedicated = first(policy.get("dedicated_mode"), policy.get("effective_dedicated_mode")).lower()
    if dedicated == "edge":
        return "dynamic"
    return "static"

labels = []
seen = set()

def add_label(key, value):
    value = str(value or "").strip()
    if not key or not value or key in seen:
        return
    labels.append(f"{key}={value}")
    seen.add(key)

runtime_id = first(node_policy.get("runtime_id"), node_updater.get("runtime_id"), current_labels.get("fugue.io/runtime-id"))
tenant_id = first(node_policy.get("tenant_id"), node_updater.get("tenant_id"), current_labels.get("fugue.io/tenant-id"))
machine_id = first(node_policy.get("machine_id"), node_updater.get("machine_id"), current_labels.get("fugue.io/machine-id"))
node_key_id = first(node_updater.get("node_key_id"), current_labels.get("fugue.io/node-key-id"))
node_mode = first(policy.get("node_mode"), current_labels.get("fugue.io/node-mode"))
machine_scope = first(current_labels.get("fugue.io/machine-scope"), "tenant-runtime" if runtime_id else "")
edge_public_ip = first(edge_credential.get("public_ipv4"), edge_credential.get("public_ipv6"))

add_label("fugue.io/machine-id", machine_id)
add_label("fugue.io/machine-scope", machine_scope)
add_label("fugue.io/node-key-id", node_key_id)
add_label("fugue.io/node-mode", node_mode)
add_label("fugue.io/runtime-id", runtime_id)
add_label("fugue.io/tenant-id", tenant_id)
add_label("topology.kubernetes.io/region", first(current_labels.get("topology.kubernetes.io/region"), edge_credential.get("region")))
add_label("failure-domain.beta.kubernetes.io/region", current_labels.get("failure-domain.beta.kubernetes.io/region"))
add_label("topology.kubernetes.io/zone", current_labels.get("topology.kubernetes.io/zone"))
add_label("failure-domain.beta.kubernetes.io/zone", current_labels.get("failure-domain.beta.kubernetes.io/zone"))
add_label("fugue.io/location-country-code", first(current_labels.get("fugue.io/location-country-code"), edge_credential.get("country")))
add_label("fugue.io/public-ip", first(os.environ.get("FUGUE_DETECTED_PUBLIC_IP"), current_labels.get("fugue.io/public-ip"), edge_public_ip))
add_label("fugue.io/edge-group-id", first(current_labels.get("fugue.io/edge-group-id"), edge_credential.get("edge_group_id")))

if truthy(policy.get("allow_builds")):
    add_label("fugue.io/build", "true")
    add_label("fugue.io/role.builder", "true")
if truthy(policy.get("allow_app_runtime")):
    add_label("fugue.io/role.app-runtime", "true")
if truthy(policy.get("allow_edge")):
    add_label("fugue.io/role.edge", "true")
    edge_workload = edge_workload_mode()
    edge_group_id = first(current_labels.get("fugue.io/edge-group-id"), edge_credential.get("edge_group_id"))
    country_code = first(current_labels.get("fugue.io/location-country-code"), edge_credential.get("country"))
    if country_code or edge_group_id or edge_workload == "static":
        add_label("fugue.io/edge-workload", edge_workload)
        add_label("fugue.io/edge-location-status", "ready")
    else:
        add_label("fugue.io/edge-location-status", "missing_location")
if truthy(policy.get("allow_dns")):
    add_label("fugue.io/role.dns", "true")
if truthy(policy.get("allow_internal_maintenance")):
    add_label("fugue.io/role.internal-maintenance", "true")
if truthy(policy.get("allow_shared_pool")):
    add_label("fugue.io/shared-pool", "internal")

control_plane_role = first(policy.get("desired_control_plane_role"))
if control_plane_role and control_plane_role != "none":
    add_label("fugue.io/control-plane-desired-role", control_plane_role)

taints = []
if tenant_id and not truthy(policy.get("allow_shared_pool")):
    taints.append(f"fugue.io/tenant={tenant_id}:NoSchedule")
dedicated_mode = first(policy.get("dedicated_mode"))
if dedicated_mode in {"edge", "dns", "internal"}:
    taints.append(f"fugue.io/dedicated={dedicated_mode}:NoSchedule")

with open(labels_path, "w", encoding="utf-8") as fh:
    for item in labels:
        fh.write(item + "\n")
with open(taints_path, "w", encoding="utf-8") as fh:
    for item in taints:
        fh.write(item + "\n")
PY_NODE_POLICY
}

reconcile_node_policy_k3s_config() {
  local labels_tmp=""
  local taints_tmp=""
  local changed=1
  labels_tmp="$(mktemp)"
  taints_tmp="$(mktemp)"
  if ! render_desired_k3s_policy_lists "${labels_tmp}" "${taints_tmp}"; then
    rm -f "${labels_tmp}" "${taints_tmp}"
    return 1
  fi
  if yaml_update_node_policy_blocks "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" "${labels_tmp}" "${taints_tmp}"; then
    changed=0
  fi
  rm -f "${labels_tmp}" "${taints_tmp}"
  return "${changed}"
}

yaml_list_block_hash() {
  parse_yaml_list_value_hash "$1" "$2" || return 1
}

current_file_hash() {
  local path="$1"
  sha256_file "${path}" || true
}

reconcile_k3s_config() {
  local server="${FUGUE_DISCOVERY_K3S_SERVER:-}"
  local fallback_servers="${FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS:-}"
  local node_public_ip=""
  local lb_cfg="/etc/fugue/k3s-api-lb.cfg"
  local changed=1
  local lb_changed=1

  if [ -z "${server}" ]; then
    log "no discovery server available for k3s reconciliation"
    return 1
  fi

  mkdir -p /etc/rancher/k3s /etc/fugue
  if [ -n "${fallback_servers}" ] && command -v haproxy >/dev/null 2>&1; then
    {
      echo "global"
      echo "  log stdout format raw local0"
      echo "  maxconn 128"
      echo "defaults"
      echo "  mode tcp"
      echo "  timeout connect 3s"
      echo "  timeout client 1m"
      echo "  timeout server 1m"
      echo "frontend k3s_api"
      echo "  bind 127.0.0.1:16443"
      echo "  default_backend k3s_servers"
      echo "backend k3s_servers"
      echo "  option tcp-check"
      echo "  balance first"
      echo "  server cp1 ${server#*://} check inter 2s fall 2 rise 1"
      local index=1
      local fallback=""
      for fallback in ${fallback_servers//,/ }; do
        index=$((index + 1))
        fallback="${fallback#*://}"
        echo "  server cp${index} ${fallback} check inter 2s fall 2 rise 1"
      done
    } >"${lb_cfg}.tmp"
    preserve_rollback_file "${lb_cfg}"
    if write_file_if_changed "${lb_cfg}.tmp" "${lb_cfg}"; then
      changed=0
      lb_changed=0
    fi
    server="https://127.0.0.1:16443"
    if [ "${lb_changed}" -eq 0 ] && systemctl list-unit-files 2>/dev/null | grep -q '^fugue-k3s-api-lb\.service'; then
      systemctl daemon-reload >/dev/null 2>&1 || true
      systemctl restart fugue-k3s-api-lb.service >/dev/null 2>&1 || true
    fi
  fi
  if yaml_update_scalar "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" server "${server}"; then
    changed=0
  fi
  node_public_ip="$(desired_node_policy_label "fugue.io/public-ip" || true)"
  if [ -n "${node_public_ip}" ] && yaml_update_scalar "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" node-external-ip "${node_public_ip}"; then
    changed=0
  fi
  if ! truthy "${FUGUE_NODE_UPDATER_USE_MESH_FOR_FLANNEL:-}" && yaml_delete_scalar "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" flannel-iface; then
    changed=0
  fi
  if reconcile_node_policy_k3s_config; then
    changed=0
  fi
  return "${changed}"
}

reconcile_registry_mirror() {
  local registry_base="${FUGUE_DISCOVERY_REGISTRY_PULL_BASE:-}"
  local endpoint="${FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT:-${FUGUE_DISCOVERY_REGISTRY_MIRROR:-}}"
  local tmp=""
  if [ -z "${registry_base}" ] || [ -z "${endpoint}" ]; then
    return 1
  fi
  mkdir -p /etc/rancher/k3s
  tmp="$(mktemp)"
  {
    printf 'mirrors:\n'
    printf '  "%s":\n' "${registry_base}"
    printf '    endpoint:\n'
    printf '      - "%s"\n' "${endpoint}"
    printf 'configs:\n'
    printf '  "%s":\n' "${registry_base}"
    printf '    tls:\n'
    printf '      insecure_skip_verify: true\n'
  } >"${tmp}"
  preserve_rollback_file "${FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE}"
  write_file_if_changed "${tmp}" "${FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE}"
}

flannel_mtu() {
  local subnet_file="/run/flannel/subnet.env"
  if [ ! -r "${subnet_file}" ]; then
    return 1
  fi
  awk -F= '$1 == "FLANNEL_MTU" { print $2; exit }' "${subnet_file}"
}

interface_mtu() {
  local iface="$1"
  local path="/sys/class/net/${iface}/mtu"
  if [ ! -r "${path}" ]; then
    return 1
  fi
  cat "${path}"
}

reconcile_cni_bridge_mtu() {
  local target_mtu=""
  local current_mtu=""
  local changed=1
  target_mtu="$(flannel_mtu || true)"
  case "${target_mtu}" in
    ""|*[!0-9]*)
      return 1
      ;;
  esac
  for iface in cni0; do
    current_mtu="$(interface_mtu "${iface}" || true)"
    if [ -z "${current_mtu}" ] || [ "${current_mtu}" = "${target_mtu}" ]; then
      continue
    fi
    if ip link set dev "${iface}" mtu "${target_mtu}"; then
      log "updated ${iface} mtu from ${current_mtu} to ${target_mtu}"
      changed=0
    fi
  done
  return "${changed}"
}

systemd_unit_file_exists() {
  local unit="$1"
  systemctl list-unit-files "${unit}" 2>/dev/null | awk '{print $1}' | grep -Fqx "${unit}"
}

active_time_sync_service() {
  local unit=""
  for unit in chrony.service chronyd.service systemd-timesyncd.service; do
    if systemctl is-active --quiet "${unit}" 2>/dev/null; then
      printf '%s' "${unit}"
      return 0
    fi
  done
  return 1
}

restart_time_sync_service() {
  local unit=""
  unit="$(active_time_sync_service || true)"
  if [ -z "${unit}" ] && systemd_unit_file_exists systemd-timesyncd.service; then
    unit="systemd-timesyncd.service"
  fi
  if [ -z "${unit}" ]; then
    return 1
  fi
  if systemctl restart "${unit}" >/dev/null 2>&1; then
    log "restarted ${unit} to refresh host clock"
    return 0
  fi
  return 1
}

reconcile_systemd_timesyncd_poll() {
  local active_unit=""
  local min_poll="${FUGUE_NODE_UPDATER_TIMESYNCD_MIN_POLL_SEC}"
  local max_poll="${FUGUE_NODE_UPDATER_TIMESYNCD_MAX_POLL_SEC}"
  local tmp=""

  case "${min_poll}" in
    ""|*[!0-9]*) min_poll=32 ;;
  esac
  case "${max_poll}" in
    ""|*[!0-9]*) max_poll=64 ;;
  esac
  if [ "${max_poll}" -lt "${min_poll}" ]; then
    max_poll="${min_poll}"
  fi
  if ! systemd_unit_file_exists systemd-timesyncd.service; then
    return 1
  fi
  active_unit="$(active_time_sync_service || true)"
  case "${active_unit}" in
    chrony.service|chronyd.service)
      log "${active_unit} is active; leaving systemd-timesyncd poll interval unchanged"
      return 1
      ;;
  esac

  mkdir -p "$(dirname "${FUGUE_NODE_UPDATER_TIMESYNCD_DROPIN}")"
  tmp="$(mktemp)"
  {
    printf '[Time]\n'
    printf 'PollIntervalMinSec=%ss\n' "${min_poll}"
    printf 'PollIntervalMaxSec=%ss\n' "${max_poll}"
  } >"${tmp}"
  if write_file_if_changed "${tmp}" "${FUGUE_NODE_UPDATER_TIMESYNCD_DROPIN}"; then
    systemctl restart systemd-timesyncd.service >/dev/null 2>&1 || timedatectl set-ntp true >/dev/null 2>&1 || true
    log "configured systemd-timesyncd poll interval min=${min_poll}s max=${max_poll}s"
    return 0
  fi
  return 1
}

control_plane_date_epoch() {
  local headers=""
  local http_date=""
  headers="$(mktemp)"
  if ! curl -fsSL --max-time 10 --retry 1 --retry-delay 1 -D "${headers}" -o /dev/null "${FUGUE_API_BASE}/readyz"; then
    rm -f "${headers}"
    return 1
  fi
  http_date="$(awk '
    {
      line = $0
      if (tolower(line) ~ /^date:[[:space:]]*/) {
        sub(/^[^:]+:[[:space:]]*/, "", line)
        sub(/\r$/, "", line)
        print line
        exit
      }
    }
  ' "${headers}")"
  rm -f "${headers}"
  if [ -z "${http_date}" ]; then
    return 1
  fi
  date -u -d "${http_date}" +%s 2>/dev/null || return 1
}

repair_clock_skew_from_control_plane() {
  local server_epoch=""
  local local_epoch=""
  local skew=0
  local abs_skew=0
  local threshold="${FUGUE_NODE_UPDATER_CLOCK_SKEW_REPAIR_THRESHOLD_SEC}"

  case "${threshold}" in
    ""|*[!0-9]*) threshold=5 ;;
  esac
  server_epoch="$(control_plane_date_epoch || true)"
  case "${server_epoch}" in
    ""|*[!0-9]*) return 1 ;;
  esac
  local_epoch="$(date -u +%s 2>/dev/null || true)"
  case "${local_epoch}" in
    ""|*[!0-9]*) return 1 ;;
  esac
  skew=$((server_epoch - local_epoch))
  abs_skew="${skew#-}"
  if [ "${abs_skew}" -le "${threshold}" ]; then
    return 1
  fi
  log "detected host clock skew ${skew}s relative to control plane Date header"
  if restart_time_sync_service; then
    return 0
  fi
  log "unable to restart a host time synchronization service"
  return 1
}

reconcile_time_sync() {
  local changed=1
  if reconcile_systemd_timesyncd_poll; then
    changed=0
  fi
  if repair_clock_skew_from_control_plane; then
    changed=0
  fi
  return "${changed}"
}

node_dns_escape_hatch_installed() {
  [ -e "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_CONFIG_FILE}" ] && return 0
  command -v systemctl >/dev/null 2>&1 || return 1
  systemctl list-unit-files "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_SERVICE}" >/dev/null 2>&1
}

dns_escape_hatch_enabled() {
  case "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_ENABLED:-false}" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

detect_dns_escape_hatch_cni_bridge_ip() {
  ip -4 addr show dev cni0 2>/dev/null | awk '/inet / {split($2, parts, "/"); print parts[1]; exit}'
}

detect_dns_escape_hatch_kube_dns_service_ip() {
  iptables-save 2>/dev/null | awk '
    /-A KUBE-SERVICES/ && /kube-system\/kube-dns:dns/ && /--dport 53/ {
      line = $0
      if (match(line, /-d [0-9.]+\/32/)) {
        ip = substr(line, RSTART + 3, RLENGTH - 3)
        sub(/\/32$/, "", ip)
        print ip
        exit
      }
    }
    /-A KUBE-SERVICES/ && /kube-system\/coredns:dns/ && /--dport 53/ {
      line = $0
      if (match(line, /-d [0-9.]+\/32/)) {
        ip = substr(line, RSTART + 3, RLENGTH - 3)
        sub(/\/32$/, "", ip)
        print ip
        exit
      }
    }
  '
}

delete_dns_escape_hatch_rule() {
  local table="$1"
  local chain="$2"
  shift 2
  local deleted=1
  while iptables -t "${table}" -C "${chain}" "$@" 2>/dev/null; do
    iptables -t "${table}" -D "${chain}" "$@" || break
    deleted=0
  done
  return "${deleted}"
}

cleanup_node_dns_escape_hatch_redirect_rules() {
  command -v iptables >/dev/null 2>&1 || return 1
  command -v iptables-save >/dev/null 2>&1 || return 1

  local cni_bridge_ip=""
  local kube_dns_service_ip=""
  local changed=1
  cni_bridge_ip="$(detect_dns_escape_hatch_cni_bridge_ip || true)"
  kube_dns_service_ip="$(detect_dns_escape_hatch_kube_dns_service_ip || true)"
  if [ -z "${cni_bridge_ip}" ] || [ -z "${kube_dns_service_ip}" ]; then
    return 1
  fi

  delete_dns_escape_hatch_rule nat PREROUTING -i cni0 -d "${kube_dns_service_ip}/32" -p udp --dport 53 -j DNAT --to-destination "${cni_bridge_ip}:53" && changed=0
  delete_dns_escape_hatch_rule nat PREROUTING -i cni0 -d "${kube_dns_service_ip}/32" -p tcp --dport 53 -j DNAT --to-destination "${cni_bridge_ip}:53" && changed=0
  delete_dns_escape_hatch_rule nat OUTPUT -d "${kube_dns_service_ip}/32" -p udp --dport 53 -j DNAT --to-destination "${cni_bridge_ip}:53" && changed=0
  delete_dns_escape_hatch_rule nat OUTPUT -d "${kube_dns_service_ip}/32" -p tcp --dport 53 -j DNAT --to-destination "${cni_bridge_ip}:53" && changed=0
  return "${changed}"
}

disable_node_dns_escape_hatch() {
  local changed=1
  if command -v systemctl >/dev/null 2>&1; then
    if systemctl is-active --quiet "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_TIMER}" || \
       systemctl is-enabled --quiet "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_TIMER}" 2>/dev/null; then
      systemctl disable --now "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_TIMER}" >/dev/null 2>&1 || true
      changed=0
    fi
    if systemctl is-active --quiet "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_SERVICE}" || \
       systemctl is-enabled --quiet "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_SERVICE}" 2>/dev/null; then
      systemctl disable --now "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_SERVICE}" >/dev/null 2>&1 || true
      changed=0
    fi
  fi
  if cleanup_node_dns_escape_hatch_redirect_rules; then
    changed=0
  fi
  return "${changed}"
}

reconcile_node_dns_escape_hatch() {
  local tmp=""
  local changed=1
  local dnsmasq_was_active=0

  node_dns_escape_hatch_installed || return 1
  if ! dns_escape_hatch_enabled; then
    disable_node_dns_escape_hatch || return 1
    log "disabled local DNS escape hatch so pod DNS uses Kubernetes CoreDNS"
    return 0
  fi
  if ! command -v dnsmasq >/dev/null 2>&1 || ! command -v systemctl >/dev/null 2>&1; then
    log "local DNS escape hatch is installed but dnsmasq/systemctl is unavailable"
    return 1
  fi
  if systemctl is-active --quiet dnsmasq.service; then
    dnsmasq_was_active=1
  fi

  mkdir -p "$(dirname "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_CONFIG_FILE}")" /var/lib/fugue-node-dns
  tmp="$(mktemp)"
  cat >"${tmp}" <<'EOF_DNSMASQ'
interface=cni0
bind-interfaces
listen-address=127.0.0.1
no-resolv
no-hosts
cache-size=1000
addn-hosts=/var/lib/fugue-node-dns/hosts.generated
server=1.1.1.1
server=8.8.8.8
EOF_DNSMASQ
  if write_file_if_changed "${tmp}" "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_CONFIG_FILE}"; then
    changed=0
  fi
  if systemctl reload-or-restart dnsmasq.service >/dev/null 2>&1 || systemctl restart dnsmasq.service >/dev/null 2>&1; then
    if [ "${dnsmasq_was_active}" -eq 0 ]; then
      changed=0
    fi
  else
    log "failed to start dnsmasq for local DNS escape hatch"
    return 1
  fi
  systemctl start "${FUGUE_NODE_UPDATER_DNS_ESCAPE_HATCH_SERVICE}" >/dev/null 2>&1 || true
  return "${changed}"
}

render_lkg_service_env() {
  local target="$1"
  local generation_key="$2"
  local tmp=""
  local api_url="${FUGUE_DISCOVERY_API_URL:-}"
  if [ -z "${api_url}" ]; then
    return 1
  fi
  mkdir -p "$(dirname "${target}")"
  tmp="$(mktemp)"
  if [ -r "${target}" ]; then
    grep -Ev '^(FUGUE_API_URL|FUGUE_EDGE_DISCOVERY_GENERATION|FUGUE_DNS_DISCOVERY_GENERATION|FUGUE_EDGE_TOKEN|FUGUE_DNS_TOKEN|FUGUE_BUNDLE_SIGNING_KEY|FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY)=' "${target}" >"${tmp}" || true
  fi
  {
    printf 'FUGUE_API_URL=%s\n' "$(json_quote_env "${api_url}")"
    printf '%s=%s\n' "${generation_key}" "$(json_quote_env "${FUGUE_DISCOVERY_GENERATION:-}")"
    if [ -n "${FUGUE_BUNDLE_SIGNING_KEY_ID:-}" ]; then
      printf 'FUGUE_BUNDLE_SIGNING_KEY_ID=%s\n' "$(json_quote_env "${FUGUE_BUNDLE_SIGNING_KEY_ID}")"
    fi
    if [ -n "${FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID:-}" ]; then
      printf 'FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID=%s\n' "$(json_quote_env "${FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID}")"
    fi
    if [ -n "${FUGUE_BUNDLE_REVOKED_KEY_IDS:-}" ]; then
      printf 'FUGUE_BUNDLE_REVOKED_KEY_IDS=%s\n' "$(json_quote_env "${FUGUE_BUNDLE_REVOKED_KEY_IDS}")"
    fi
  } >>"${tmp}"
  write_file_if_changed "${tmp}" "${target}" || true
}

reconcile_lkg_service_envs() {
  local changed=0
  if render_lkg_service_env "${FUGUE_NODE_UPDATER_EDGE_ENV_FILE}" FUGUE_EDGE_DISCOVERY_GENERATION; then
    changed=1
  fi
  if render_lkg_service_env "${FUGUE_NODE_UPDATER_DNS_ENV_FILE}" FUGUE_DNS_DISCOVERY_GENERATION; then
    changed=1
  fi
  [ "${changed}" -eq 1 ]
}

render_edge_node_env() {
  local target="$1"
  local state_file="${FUGUE_NODE_UPDATER_DESIRED_STATE_FILE}"
  if [ ! -r "${state_file}" ] || ! command -v python3 >/dev/null 2>&1; then
    return 1
  fi
  python3 - "${state_file}" "${target}" <<'PY_EDGE_NODE_ENV'
import json
import shlex
import sys

state_path, target_path = sys.argv[1:3]
with open(state_path, "r", encoding="utf-8") as fh:
    envelope = json.load(fh)

credential = ((envelope.get("desired_state") or {}).get("edge_credential") or {})
edge_id = str(credential.get("edge_id") or "").strip()
edge_group_id = str(credential.get("edge_group_id") or "").strip()
token = str(credential.get("token") or "").strip()
if not edge_id or not edge_group_id or not token:
    raise SystemExit(1)

def emit(key, value):
    value = "" if value is None else str(value)
    print(f"{key}={shlex.quote(value)}")

emit("FUGUE_EDGE_NODE_ID", edge_id)
emit("FUGUE_EDGE_ID", edge_id)
emit("FUGUE_EDGE_GROUP_ID", edge_group_id)
emit("FUGUE_EDGE_NODE_TOKEN", token)
emit("FUGUE_EDGE_TOKEN", token)
emit("FUGUE_EDGE_WORKLOAD_MODE", credential.get("workload_mode") or "dynamic")
emit("FUGUE_EDGE_DESIRED_STATE_URL", credential.get("desired_state_url") or "")
emit("FUGUE_EDGE_PUBLIC_IPV4", credential.get("public_ipv4") or "")
emit("FUGUE_EDGE_PUBLIC_IPV6", credential.get("public_ipv6") or "")
emit("FUGUE_EDGE_REGION", credential.get("region") or "")
emit("FUGUE_EDGE_COUNTRY", credential.get("country") or "")
PY_EDGE_NODE_ENV
}

restart_dynamic_edge_pods_for_credential_reload() {
  local node_name="${FUGUE_NODE_UPDATER_CLUSTER_NODE_NAME:-}"
  if [ -z "${node_name}" ]; then
    return 1
  fi
  if command -v k3s >/dev/null 2>&1; then
    k3s kubectl -n fugue-system delete pod \
      --field-selector "spec.nodeName=${node_name}" \
      -l "fugue.io/edge-workload=dynamic" \
      --ignore-not-found >/dev/null 2>&1 || return 1
    return 0
  fi
  if command -v kubectl >/dev/null 2>&1; then
    kubectl -n fugue-system delete pod \
      --field-selector "spec.nodeName=${node_name}" \
      -l "fugue.io/edge-workload=dynamic" \
      --ignore-not-found >/dev/null 2>&1 || return 1
    return 0
  fi
  return 1
}

reconcile_edge_node_env() {
  local env_tmp=""
  env_tmp="$(mktemp)"
  if ! render_edge_node_env "${env_tmp}" >"${env_tmp}"; then
    rm -f "${env_tmp}"
    return 1
  fi
  mkdir -p "$(dirname "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}")"
  if write_secret_file_if_changed "${env_tmp}" "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}"; then
    restart_dynamic_edge_pods_for_credential_reload || true
    return 0
  fi
  return 1
}

reconcile_node_state() {
  local k3s_runtime_config_changed=0
  mkdir -p "${FUGUE_NODE_UPDATER_STATE_DIR}"
  if reconcile_time_sync; then
    log "reconciled host time synchronization"
  fi
  if fetch_discovery_bundle; then
    log "refreshed discovery bundle cache"
  elif [ -r "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}" ]; then
    log "using cached discovery bundle state"
    # shellcheck disable=SC1090
    . "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}"
  fi
  if fetch_node_policy_desired_state; then
    log "refreshed desired node policy cache"
  fi
  if reconcile_edge_node_env; then
    log "updated node-scoped edge credential"
  fi
  if [ -r "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}" ]; then
    # shellcheck disable=SC1090
    . "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}"
  fi
  render_node_updater_state_env
  if reconcile_registry_mirror; then
    log "updated k3s registry mirror configuration"
    k3s_runtime_config_changed=1
  fi
  if reconcile_k3s_config; then
    log "updated k3s join configuration"
    k3s_runtime_config_changed=1
  fi
  if reconcile_cni_bridge_mtu; then
    log "reconciled CNI bridge MTU"
  fi
  if reconcile_node_dns_escape_hatch; then
    log "reconciled local DNS escape hatch"
  fi
  if reconcile_lkg_service_envs; then
    log "updated edge/dns non-secret environment generation"
  fi
  if [ "${k3s_runtime_config_changed}" -eq 1 ]; then
    restart_k3s_agent_for_config_reload
  fi
  return 0
}

current_k3s_server() {
  if [ -n "${FUGUE_DISCOVERY_K3S_SERVER:-}" ]; then
    printf '%s' "${FUGUE_DISCOVERY_K3S_SERVER}"
    return 0
  fi
  if [ -r "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" ]; then
    awk '
      $1 == "server:" {
        value = $2
        gsub(/"/, "", value)
        print value
        exit
      }
    ' "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}"
  fi
}

current_k3s_fallback_servers() {
  if [ -n "${FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS:-}" ]; then
    printf '%s' "${FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS}"
    return 0
  fi
  if [ -r "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}" ]; then
    # shellcheck disable=SC1090
    . "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}"
    printf '%s' "${FUGUE_DISCOVERY_K3S_FALLBACK_SERVERS:-}"
  fi
}

current_registry_mirror() {
  if [ -n "${FUGUE_DISCOVERY_REGISTRY_MIRROR:-}" ]; then
    printf '%s' "${FUGUE_DISCOVERY_REGISTRY_MIRROR}"
    return 0
  fi
  if [ -n "${FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT:-}" ]; then
    printf '%s' "${FUGUE_DISCOVERY_CLUSTER_JOIN_REGISTRY_ENDPOINT}"
    return 0
  fi
  if [ -r "${FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE}" ]; then
    awk '
      $1 == "endpoint:" { in_endpoints = 1; next }
      in_endpoints && $0 ~ /^[[:space:]]*-/ {
        value = $0
        sub(/^[[:space:]]*-[[:space:]]*"/, "", value)
        sub(/"$/, "", value)
        print value
        exit
      }
    ' "${FUGUE_NODE_UPDATER_K3S_REGISTRIES_FILE}"
  fi
}

current_labels_hash() {
  yaml_list_block_hash "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" node-label || true
}

current_taints_hash() {
  yaml_list_block_hash "${FUGUE_NODE_UPDATER_K3S_CONFIG_FILE}" node-taint || true
}

current_edge_node_token_prefix() {
  if [ ! -r "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}" ] || ! command -v python3 >/dev/null 2>&1; then
    return 0
  fi
  python3 - "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}" <<'PY_EDGE_TOKEN_PREFIX'
import shlex
import sys

path = sys.argv[1]
for line in open(path, "r", encoding="utf-8"):
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, raw = line.split("=", 1)
    if key not in {"FUGUE_EDGE_NODE_TOKEN", "FUGUE_EDGE_TOKEN"}:
        continue
    try:
        values = shlex.split(raw)
    except ValueError:
        values = [raw.strip("'\"")]
    token = (values[0] if values else "").strip()
    if token:
        print(token[:8])
        break
PY_EDGE_TOKEN_PREFIX
}

current_edge_env_generation() {
  local tmp=""
  local token_prefix=""
  tmp="$(mktemp)"
  token_prefix="$(current_edge_node_token_prefix)"
  {
    printf 'edge_env=%s\n' "$(current_file_hash "${FUGUE_NODE_UPDATER_EDGE_ENV_FILE}")"
    printf 'edge_node_env=%s\n' "$(current_file_hash "${FUGUE_NODE_UPDATER_EDGE_NODE_ENV_FILE}")"
  } >"${tmp}"
  printf 'v2:%s:%s\n' "${token_prefix}" "$(sha256_file "${tmp}" || true)"
  rm -f "${tmp}"
}

current_dns_env_generation() {
  current_file_hash "${FUGUE_NODE_UPDATER_DNS_ENV_FILE}"
}

current_config_hash() {
  local tmp=""
  tmp="$(mktemp)"
  {
    printf 'server=%s\n' "$(current_k3s_server)"
    printf 'fallbacks=%s\n' "$(current_k3s_fallback_servers)"
    printf 'registry_mirror=%s\n' "$(current_registry_mirror)"
    printf 'labels_hash=%s\n' "$(current_labels_hash)"
    printf 'taints_hash=%s\n' "$(current_taints_hash)"
    printf 'edge_env_generation=%s\n' "$(current_edge_env_generation)"
    printf 'dns_env_generation=%s\n' "$(current_dns_env_generation)"
    printf 'discovery_generation=%s\n' "${FUGUE_DISCOVERY_GENERATION:-}"
    printf 'timesyncd_dropin_hash=%s\n' "$(current_file_hash "${FUGUE_NODE_UPDATER_TIMESYNCD_DROPIN}")"
    printf 'timesyncd_poll_min=%s\n' "${FUGUE_NODE_UPDATER_TIMESYNCD_MIN_POLL_SEC}"
    printf 'timesyncd_poll_max=%s\n' "${FUGUE_NODE_UPDATER_TIMESYNCD_MAX_POLL_SEC}"
  } >"${tmp}"
  sha256_file "${tmp}" || true
  rm -f "${tmp}"
}

record_last_error() {
  mkdir -p "${FUGUE_NODE_UPDATER_WORK_DIR}"
  printf '%s\n' "$*" >"${FUGUE_NODE_UPDATER_LAST_ERROR_FILE}"
}

api_form() {
  local method="$1"
  local path="$2"
  shift 2
  curl -fsSL --retry 3 --retry-delay 2 -X "${method}" "${FUGUE_API_BASE}${path}" \
    -H "Authorization: Bearer ${FUGUE_NODE_UPDATER_TOKEN:?FUGUE_NODE_UPDATER_TOKEN is required}" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    "$@"
}

api_json() {
  local method="$1"
  local path="$2"
  local body="${3:-{}}"
  curl -fsSL --retry 3 --retry-delay 2 -X "${method}" "${FUGUE_API_BASE}${path}" \
    -H "Authorization: Bearer ${FUGUE_NODE_UPDATER_TOKEN:?FUGUE_NODE_UPDATER_TOKEN is required}" \
    -H "Content-Type: application/json" \
    --data "${body}"
}

heartbeat() {
  local current_k3s=""
  current_k3s="$(k3s_version)"
  api_form POST /v1/node-updater/heartbeat \
    --data-urlencode "updater_version=${FUGUE_NODE_UPDATER_VERSION}" \
    --data-urlencode "join_script_version=${FUGUE_JOIN_SCRIPT_VERSION:-}" \
    --data-urlencode "capabilities=${FUGUE_NODE_UPDATER_CAPABILITIES}" \
    --data-urlencode "k3s_version=${current_k3s}" \
    --data-urlencode "k3s_server=$(current_k3s_server)" \
    --data-urlencode "k3s_fallback_servers=$(current_k3s_fallback_servers)" \
    --data-urlencode "registry_mirror=$(current_registry_mirror)" \
    --data-urlencode "labels_hash=$(current_labels_hash)" \
    --data-urlencode "taints_hash=$(current_taints_hash)" \
    --data-urlencode "edge_env_generation=$(current_edge_env_generation)" \
    --data-urlencode "dns_env_generation=$(current_dns_env_generation)" \
    --data-urlencode "config_hash=$(current_config_hash)" \
    --data-urlencode "discovery_generation=${FUGUE_DISCOVERY_GENERATION:-}" \
    --data-urlencode "os=$(uname -s 2>/dev/null || true)" \
    --data-urlencode "arch=$(uname -m 2>/dev/null || true)" \
    --data-urlencode "last_error=$(last_error)" \
    >/dev/null
}

claim_task() {
  api_form POST "/v1/node-updater/tasks/${FUGUE_NODE_UPDATE_TASK_ID}/claim" >/dev/null
}

log_task() {
  local message="$1"
  api_form POST "/v1/node-updater/tasks/${FUGUE_NODE_UPDATE_TASK_ID}/log" \
    --data-urlencode "message=${message}" >/dev/null || true
}

complete_task() {
  local status="$1"
  local message="$2"
  local error_message="${3:-}"
  api_form POST "/v1/node-updater/tasks/${FUGUE_NODE_UPDATE_TASK_ID}/complete" \
    --data-urlencode "status=${status}" \
    --data-urlencode "message=${message}" \
    --data-urlencode "error_message=${error_message}" >/dev/null || true
}

wait_for_unit_active() {
  local unit="$1"
  local timeout_seconds="${2:-900}"
  local started_at=""
  local elapsed=0
  started_at="$(date +%s)"
  while [ "${elapsed}" -lt "${timeout_seconds}" ]; do
    if systemctl is-active --quiet "${unit}"; then
      return 0
    fi
    sleep 5
    elapsed=$(( $(date +%s) - started_at ))
  done
  systemctl status "${unit}" --no-pager || true
  return 1
}

restart_k3s_agent() {
  log_task "restarting k3s-agent"
  systemctl restart k3s-agent
  wait_for_unit_active k3s-agent 900
  log_task "k3s-agent is active"
}

restart_k3s_agent_for_config_reload() {
  if ! systemctl list-unit-files k3s-agent.service >/dev/null 2>&1; then
    log "k3s-agent unit is absent; skip containerd registry mirror reload"
    return 0
  fi
  log "restarting k3s-agent so containerd reloads updated join/registry configuration"
  systemctl restart k3s-agent
  wait_for_unit_active k3s-agent 900
  log "k3s-agent is active after join/registry configuration reload"
}

upgrade_k3s_agent() {
  local channel="${FUGUE_NODE_UPDATE_TASK_K3S_CHANNEL:-${FUGUE_K3S_CHANNEL:-stable}}"
  local target_version="${FUGUE_NODE_UPDATE_TASK_TARGET_K3S_VERSION:-}"
  local install_env=(INSTALL_K3S_EXEC=agent INSTALL_K3S_SKIP_START=true INSTALL_K3S_CHANNEL="${channel}")
  if [ -n "${target_version}" ]; then
    install_env+=(INSTALL_K3S_VERSION="${target_version}")
  fi
  log_task "installing k3s agent channel=${channel} target=${target_version:-latest}"
  curl -sfL https://get.k3s.io | env "${install_env[@]}" sh -
  restart_k3s_agent
  log_task "k3s agent version after upgrade: $(k3s_version)"
}

upgrade_node_updater() {
  local script_url="${FUGUE_NODE_UPDATE_TASK_NODE_UPDATER_SCRIPT_URL:-${FUGUE_API_BASE}/install/node-updater.sh}"
  local tmp=""
  tmp="$(mktemp)"
  log_task "downloading node updater script from ${script_url}"
  curl -fsSL "${script_url}" -o "${tmp}"
  install -m 0755 "${tmp}" /usr/local/bin/fugue-node-updater
  rm -f "${tmp}"
  log_task "node updater script installed"
}

diagnose_node() {
  log_task "hostname=$(hostname 2>/dev/null || true)"
  log_task "kernel=$(uname -a 2>/dev/null || true)"
  log_task "k3s=$(k3s_version)"
  log_task "k3s-agent=$(systemctl is-active k3s-agent 2>/dev/null || true)"
}

install_nfs_client_tools() {
  if command -v mount.nfs >/dev/null 2>&1; then
    log_task "NFS client tools are already installed"
    return 0
  fi
  if ! command -v apt-get >/dev/null 2>&1; then
    echo "apt-get is unavailable; cannot install nfs-common automatically" >&2
    return 2
  fi
  log_task "installing NFS client tools"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y --no-install-recommends nfs-common
  log_task "NFS client tools installed: $(command -v mount.nfs 2>/dev/null || true)"
}

pull_container_image() {
  local image="$1"
  if command -v crictl >/dev/null 2>&1; then
    crictl pull "${image}"
  elif command -v k3s >/dev/null 2>&1; then
    k3s ctr images pull "${image}"
  elif command -v ctr >/dev/null 2>&1; then
    ctr -n k8s.io images pull "${image}"
  else
    echo "no CRI image puller is available" >&2
    return 2
  fi
}

internal_registry_image() {
  local image="$1"
  case "${image}" in
    registry.fugue.internal/*|registry.fugue.internal:*/*|fugue-fugue-registry.*/*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

image_cache_manifest_path() {
  local image="$1"
  local ref="${image#*://}"
  local path_part=""
  local repo=""
  local target=""
  case "${ref}" in
    */*) path_part="${ref#*/}" ;;
    *) return 1 ;;
  esac
  case "${path_part}" in
    *@sha256:*)
      repo="${path_part%@sha256:*}"
      target="${path_part##*@}"
      ;;
    *)
      local last="${path_part##*/}"
      if [[ "${last}" == *:* ]]; then
        repo="${path_part%:*}"
        target="${path_part##*:}"
      else
        repo="${path_part}"
        target="latest"
      fi
      ;;
  esac
  [ -n "${repo}" ] && [ -n "${target}" ] || return 1
  printf '%s %s\n' "${repo}" "${target}"
}

first_registry_mirror_endpoint() {
  local endpoint=""
  for endpoint in $(current_registry_mirror); do
    break
  done
  if [ -z "${endpoint}" ]; then
    endpoint="http://127.0.0.1:5000"
  fi
  case "${endpoint}" in
    http://*|https://*) ;;
    *) endpoint="http://${endpoint}" ;;
  esac
  printf '%s' "${endpoint%/}"
}

verify_image_cache_manifest() {
  local image="$1"
  if ! internal_registry_image "${image}"; then
    return 0
  fi
  local parsed=""
  parsed="$(image_cache_manifest_path "${image}")" || return 1
  local repo="${parsed% *}"
  local target="${parsed##* }"
  local endpoint=""
  endpoint="$(first_registry_mirror_endpoint)"
  curl -fsSI --max-time 20 "${endpoint}/v2/${repo}/manifests/${target}" >/dev/null
}

report_image_location() {
  local image="$1"
  local status="$2"
  local message="${3:-}"
  api_form POST /v1/node-updater/image-locations \
    --data-urlencode "image_ref=${image}" \
    --data-urlencode "status=${status}" \
    --data-urlencode "app_id=${FUGUE_NODE_UPDATE_TASK_APP_ID:-}" \
    --data-urlencode "source_operation_id=${FUGUE_NODE_UPDATE_TASK_OPERATION_ID:-}" \
    --data-urlencode "last_error=${message}" \
    >/dev/null || true
}

prepull_system_images() {
  local raw="${FUGUE_NODE_UPDATER_SYSTEM_IMAGES:-${FUGUE_SYSTEM_IMAGES:-}}"
  local image=""
  if [ -z "${raw}" ]; then
    log_task "no system images configured for pre-pull"
    return 0
  fi
  for image in ${raw//,/ }; do
    [ -n "${image}" ] || continue
    case "${image}" in
      *@sha256:*) ;;
      *)
        echo "system image ${image} is not digest-pinned; refusing pre-pull" >&2
        return 2
        ;;
    esac
    pull_container_image "${image}"
    log_task "pre-pulled ${image}"
  done
}

prepull_app_image_missing_manifest() {
  local output=""
  output="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "${output}" in
    *"manifest_unknown"*|*"manifest unknown"*|*"unknown manifest"*|*"name_unknown"*|*"code = notfound"*|*" not found"*)
      return 0
      ;;
  esac
  return 1
}

prepull_app_images() {
  local raw="${FUGUE_NODE_UPDATE_TASK_IMAGES:-${FUGUE_NODE_UPDATE_TASK_IMAGE_REF:-}}"
  local image=""
  local pull_output=""
  local missing_count=0
  local present_count=0
  if [ -z "${raw}" ]; then
    log_task "no app images requested for pre-pull"
    return 0
  fi
  for image in ${raw//,/ }; do
    [ -n "${image}" ] || continue
    report_image_location "${image}" pulling ""
    if pull_output="$(pull_container_image "${image}" 2>&1)"; then
      if ! verify_image_cache_manifest "${image}"; then
        pull_output="pre-pull succeeded but node image cache does not serve registry manifest for ${image}"
        log_task "${pull_output}"
        report_image_location "${image}" failed "${pull_output}"
        return 1
      fi
      log_task "pre-pulled app image ${image}"
      report_image_location "${image}" present ""
      present_count=$((present_count + 1))
    else
      if prepull_app_image_missing_manifest "${pull_output}"; then
        log_task "skipping stale app image ${image}: registry manifest is missing"
        report_image_location "${image}" missing "${pull_output}"
        missing_count=$((missing_count + 1))
        continue
      fi
      log_task "failed to pre-pull app image ${image}: ${pull_output}"
      report_image_location "${image}" failed "${pull_output}"
      return 1
    fi
  done
  if [ "${missing_count}" -gt 0 ]; then
    log_task "pre-pull completed with ${missing_count} missing stale app image(s) and ${present_count} present app image(s)"
  fi
}

image_cache_api_endpoint() {
  first_registry_mirror_endpoint
}

image_cache_api_json() {
  local path="$1"
  local body="${2:-{}}"
  local endpoint=""
  endpoint="$(image_cache_api_endpoint)"
  curl -fsS --max-time 300 \
    -H "Content-Type: application/json" \
    -X POST \
    --data "${body}" \
    "${endpoint}${path}"
}

json_escape_shell() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

report_image_replica() {
  local image_id="$1"
  local digest="$2"
  local status="$3"
  local message="${4:-}"
  local endpoint=""
  endpoint="$(image_cache_api_endpoint)"
  api_form POST /v1/node-updater/image-replicas/report \
    --data-urlencode "image_id=${image_id}" \
    --data-urlencode "app_id=${FUGUE_NODE_UPDATE_TASK_APP_ID:-}" \
    --data-urlencode "digest=${digest}" \
    --data-urlencode "status=${status}" \
    --data-urlencode "cache_endpoint=${endpoint}" \
    --data-urlencode "last_error=${message}" \
    >/dev/null || true
}

replicate_app_image() {
  local image_id="${FUGUE_NODE_UPDATE_TASK_IMAGE_ID:-}"
  local image_ref="${FUGUE_NODE_UPDATE_TASK_IMAGE_REF:-${FUGUE_NODE_UPDATE_TASK_IMAGES:-}}"
  local digest="${FUGUE_NODE_UPDATE_TASK_DIGEST:-}"
  local source="${FUGUE_NODE_UPDATE_TASK_SOURCE_CACHE_ENDPOINT:-}"
  if [ -z "${image_id}" ] || [ -z "${image_ref}${digest}" ]; then
    echo "replicate-app-image requires image_id and image_ref or digest" >&2
    return 2
  fi
  local body
  body="{\"image_ref\":\"$(json_escape_shell "${image_ref}")\",\"digest\":\"$(json_escape_shell "${digest}")\",\"source_cache_endpoint\":\"$(json_escape_shell "${source}")\",\"task_id\":\"$(json_escape_shell "${FUGUE_NODE_UPDATE_TASK_ID:-}")\"}"
  report_image_replica "${image_id}" "${digest}" copying ""
  if image_cache_api_json /fugue/cache/v1/replicate "${body}" >/dev/null; then
    report_image_replica "${image_id}" "${digest}" present ""
    log_task "replicated app image ${image_ref:-${digest}}"
    return 0
  fi
  local rc=$?
  report_image_replica "${image_id}" "${digest}" failed "image-cache replication failed"
  return "${rc}"
}

verify_image_cache() {
  local image_id="${FUGUE_NODE_UPDATE_TASK_IMAGE_ID:-}"
  local image_ref="${FUGUE_NODE_UPDATE_TASK_IMAGE_REF:-${FUGUE_NODE_UPDATE_TASK_IMAGES:-}}"
  local digest="${FUGUE_NODE_UPDATE_TASK_DIGEST:-}"
  if [ -z "${image_id}" ] || [ -z "${image_ref}${digest}" ]; then
    echo "verify-image-cache requires image_id and image_ref or digest" >&2
    return 2
  fi
  local body
  body="{\"image_ref\":\"$(json_escape_shell "${image_ref}")\",\"digest\":\"$(json_escape_shell "${digest}")\"}"
  if image_cache_api_json /fugue/cache/v1/verify "${body}" >/dev/null; then
    report_image_replica "${image_id}" "${digest}" present ""
    log_task "verified image cache for ${image_ref:-${digest}}"
    return 0
  fi
  local rc=$?
  report_image_replica "${image_id}" "${digest}" missing "image-cache verify failed"
  return "${rc}"
}

report_image_cache_inventory() {
  local endpoint=""
  local inventory_file=""
  local chunk_dir=""
  local summary_file=""
  local chunk_file=""
  endpoint="$(image_cache_api_endpoint)"
  if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is required for image-cache inventory chunking" >&2
    return 2
  fi
  inventory_file="$(mktemp)"
  chunk_dir="$(mktemp -d)"
  summary_file="${chunk_dir}/summary.env"
  if ! curl -fsS --max-time 60 "${endpoint}/fugue/cache/v1/inventory" -o "${inventory_file}"; then
    rm -f "${inventory_file}"
    rm -rf "${chunk_dir}"
    echo "image-cache inventory endpoint failed" >&2
    return 1
  fi
  if ! python3 - "${inventory_file}" "${chunk_dir}" "${endpoint}" "${FUGUE_NODE_UPDATER_CLUSTER_NODE_NAME:-${FUGUE_JOIN_NODE_NAME:-$(hostname -s 2>/dev/null || hostname 2>/dev/null || true)}}" <<'PY_IMAGE_CACHE_INVENTORY'
import copy
import json
import os
import sys
from datetime import datetime, timezone

inventory_path, chunk_dir, endpoint, cluster_node = sys.argv[1:5]
with open(inventory_path, "r", encoding="utf-8") as fh:
    inventory = json.load(fh)

manifests = inventory.get("manifests") or []
if not isinstance(manifests, list):
    raise SystemExit("image-cache inventory manifests is not a list")

def as_int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0

normalized = []
for manifest in manifests:
    if not isinstance(manifest, dict):
        continue
    item = dict(manifest)
    if item.get("content_type") and not item.get("media_type"):
        item["media_type"] = item.get("content_type")
    if item.get("size_bytes") is not None and not item.get("manifest_size_bytes"):
        item["manifest_size_bytes"] = item.get("size_bytes")
    if item.get("referenced_blob_bytes") is not None and not item.get("total_blob_bytes"):
        item["total_blob_bytes"] = item.get("referenced_blob_bytes")
    if item.get("modified_at") and not item.get("created_at_observed"):
        item["created_at_observed"] = item.get("modified_at")
    normalized.append(item)

raw_unreferenced_blobs = inventory.get("unreferenced_blobs") or []
if not isinstance(raw_unreferenced_blobs, list):
    raw_unreferenced_blobs = []
unreferenced_blobs = []
unreferenced_blob_bytes = 0
for blob in raw_unreferenced_blobs:
    if not isinstance(blob, dict):
        continue
    digest = str(blob.get("digest") or "").strip()
    if not digest:
        continue
    size_bytes = as_int(blob.get("size_bytes") or blob.get("size") or blob.get("blob_size"))
    item = {
        "digest": digest,
        "size_bytes": size_bytes,
    }
    modified_at = str(blob.get("modified_at") or blob.get("last_seen_at") or "").strip()
    if modified_at:
        item["modified_at"] = modified_at
    unreferenced_blobs.append(item)
    unreferenced_blob_bytes += size_bytes
if unreferenced_blob_bytes == 0:
    unreferenced_blob_bytes = as_int(inventory.get("unreferenced_blob_bytes"))
unreferenced_blob_count = len(unreferenced_blobs)
if unreferenced_blob_count == 0:
    unreferenced_blob_count = as_int(inventory.get("unreferenced_blob_count"))

disk = inventory.get("disk") if isinstance(inventory.get("disk"), dict) else {}
pins = inventory.get("pins") if isinstance(inventory.get("pins"), list) else []
observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
cache_bytes = as_int(disk.get("cache_bytes"))
free_bytes = as_int(disk.get("free_bytes"))
chunk_size = 500
chunk_count = max(1, (len(normalized) + chunk_size - 1) // chunk_size)

base = copy.deepcopy(inventory)
base["endpoint"] = (inventory.get("endpoint") or endpoint).rstrip("/")
base["cluster_node"] = inventory.get("cluster_node") or cluster_node
base["observed_at"] = observed_at
base["manifest_total_count"] = len(normalized)
base["unreferenced_blobs"] = unreferenced_blobs
base["node"] = {
    "cluster_node_name": base["cluster_node"],
    "cache_endpoint": base["endpoint"],
    "filesystem_total_bytes": as_int(disk.get("total_bytes")),
    "filesystem_free_bytes": free_bytes,
    "filesystem_used_percent": float(disk.get("used_percent") or 0),
    "cache_bytes": cache_bytes,
    "manifest_count": len(normalized),
    "blob_count": as_int(inventory.get("blob_count") or disk.get("blob_count")),
    "unreferenced_blob_count": unreferenced_blob_count,
    "unreferenced_blob_bytes": unreferenced_blob_bytes,
    "pin_count": len(pins),
    "observed_at": observed_at,
    "status": "reported",
}

paths = []
for idx in range(chunk_count):
    payload = copy.deepcopy(base)
    payload["chunk_index"] = idx
    payload["chunk_count"] = chunk_count
    payload["manifests"] = normalized[idx * chunk_size:(idx + 1) * chunk_size]
    path = os.path.join(chunk_dir, f"chunk-{idx:05d}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    paths.append(path)

with open(os.path.join(chunk_dir, "chunks.list"), "w", encoding="utf-8") as fh:
    for path in paths:
        fh.write(path + "\n")
with open(os.path.join(chunk_dir, "summary.env"), "w", encoding="utf-8") as fh:
    fh.write(f"manifest_count={len(normalized)}\n")
    fh.write(f"chunk_count={chunk_count}\n")
    fh.write(f"cache_bytes={cache_bytes}\n")
    fh.write(f"unreferenced_blob_count={unreferenced_blob_count}\n")
    fh.write(f"unreferenced_blob_bytes={unreferenced_blob_bytes}\n")
    fh.write(f"filesystem_free_bytes={free_bytes}\n")
PY_IMAGE_CACHE_INVENTORY
  then
    rm -f "${inventory_file}"
    rm -rf "${chunk_dir}"
    echo "failed to normalize image-cache inventory" >&2
    return 1
  fi
  if ! . "${summary_file}"; then
    rm -f "${inventory_file}"
    rm -rf "${chunk_dir}"
    echo "failed to load image-cache inventory summary" >&2
    return 1
  fi
  local expected_chunks="${chunk_count:-0}"
  case "${expected_chunks}" in
    ''|*[!0-9]*|0)
      rm -f "${inventory_file}"
      rm -rf "${chunk_dir}"
      echo "image-cache inventory produced invalid chunk_count=${expected_chunks}" >&2
      return 1
      ;;
  esac
  if [ ! -s "${chunk_dir}/chunks.list" ]; then
    rm -f "${inventory_file}"
    rm -rf "${chunk_dir}"
    echo "image-cache inventory produced no chunks" >&2
    return 1
  fi
  local chunk_file_count="0"
  chunk_file_count="$(wc -l <"${chunk_dir}/chunks.list" | tr -d '[:space:]')"
  if [ "${chunk_file_count}" -ne "${expected_chunks}" ]; then
    rm -f "${inventory_file}"
    rm -rf "${chunk_dir}"
    echo "image-cache inventory chunk list count ${chunk_file_count} did not match expected ${expected_chunks}" >&2
    return 1
  fi
  local posted_chunks=0
  while IFS= read -r chunk_file; do
    [ -n "${chunk_file}" ] || continue
    if [ ! -s "${chunk_file}" ]; then
      rm -f "${inventory_file}"
      rm -rf "${chunk_dir}"
      echo "image-cache inventory chunk file is missing or empty: ${chunk_file}" >&2
      return 1
    fi
    local next_chunk_number=$((posted_chunks + 1))
    if ! curl -fsSL --retry 3 --retry-delay 2 -X POST "${FUGUE_API_BASE}/v1/node-updater/image-cache/inventory" \
      -H "Authorization: Bearer ${FUGUE_NODE_UPDATER_TOKEN:?FUGUE_NODE_UPDATER_TOKEN is required}" \
      -H "Content-Type: application/json" \
      --data-binary @"${chunk_file}" >/dev/null; then
      rm -f "${inventory_file}"
      rm -rf "${chunk_dir}"
      echo "image-cache inventory POST failed for chunk ${next_chunk_number}/${expected_chunks}" >&2
      return 1
    fi
    posted_chunks=$((posted_chunks + 1))
  done <"${chunk_dir}/chunks.list"
  if [ "${posted_chunks}" -ne "${expected_chunks}" ]; then
    rm -f "${inventory_file}"
    rm -rf "${chunk_dir}"
    echo "image-cache inventory posted ${posted_chunks} chunks, expected ${expected_chunks}" >&2
    return 1
  fi
  log_task "reported image-cache inventory manifests=${manifest_count:-0} chunks=${chunk_count:-0} cache_bytes=${cache_bytes:-0} unreferenced_blob_count=${unreferenced_blob_count:-0} unreferenced_blob_bytes=${unreferenced_blob_bytes:-0} filesystem_free_bytes=${filesystem_free_bytes:-0}"
  rm -f "${inventory_file}"
  rm -rf "${chunk_dir}"
}

prune_image_cache() {
  local dry_run="${FUGUE_NODE_UPDATE_TASK_DRY_RUN:-true}"
  local allow_delete="${FUGUE_NODE_UPDATE_TASK_ALLOW_DELETE:-false}"
  local image_id="${FUGUE_NODE_UPDATE_TASK_IMAGE_ID:-}"
  local image_ref="${FUGUE_NODE_UPDATE_TASK_IMAGE_REF:-${FUGUE_NODE_UPDATE_TASK_IMAGES:-}}"
  local digest="${FUGUE_NODE_UPDATE_TASK_DIGEST:-}"
  local max_delete_bytes="${FUGUE_NODE_UPDATE_TASK_MAX_DELETE_BYTES:-}"
  local min_manifest_age="${FUGUE_NODE_UPDATE_TASK_MIN_MANIFEST_AGE:-}"
  local include_unreferenced_blobs="${FUGUE_NODE_UPDATE_TASK_INCLUDE_UNREFERENCED_BLOBS:-false}"
  local targets_json="${FUGUE_NODE_UPDATE_TASK_TARGETS_JSON:-[]}"
  local body
  local response
  local planned_delete_bytes
  local deleted_bytes
  local candidate_count
  case "${dry_run}" in true|false) ;; *) dry_run=true ;; esac
  case "${allow_delete}" in true|false) ;; *) allow_delete=false ;; esac
  case "${include_unreferenced_blobs}" in true|false) ;; *) include_unreferenced_blobs=false ;; esac
  case "${targets_json}" in
    \[*\]) ;;
    *) targets_json="[]" ;;
  esac
  body="{\"dry_run\":${dry_run},\"allow_delete\":${allow_delete},\"image_ref\":\"$(json_escape_shell "${image_ref}")\",\"digest\":\"$(json_escape_shell "${digest}")\",\"max_delete_bytes\":\"$(json_escape_shell "${max_delete_bytes}")\",\"min_manifest_age\":\"$(json_escape_shell "${min_manifest_age}")\",\"include_unreferenced_blobs\":${include_unreferenced_blobs},\"targets\":${targets_json}}"
  response="$(image_cache_api_json /fugue/cache/v1/prune "${body}")"
  planned_delete_bytes="$(printf '%s' "${response}" | sed -n 's/.*"planned_delete_bytes"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1)"
  deleted_bytes="$(printf '%s' "${response}" | sed -n 's/.*"deleted_bytes"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1)"
  candidate_count="$(printf '%s' "${response}" | sed -n 's/.*"candidate_count"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1)"
  if printf '%s' "${response}" | grep -Eq '"deleted"[[:space:]]*:[[:space:]]*true'; then
    if [ -n "${image_id}" ]; then
      report_image_replica "${image_id}" "${digest}" missing "image-cache prune deleted local replica"
    fi
  fi
  if [ "${dry_run}" = "false" ] && [ "${allow_delete}" = "true" ]; then
    log_task "image-cache prune delete completed; reporting post-prune inventory"
    report_image_cache_inventory
  fi
  FUGUE_NODE_UPDATE_TASK_RESULT_MESSAGE="image-cache prune completed dry_run=${dry_run} allow_delete=${allow_delete} planned_delete_bytes=${planned_delete_bytes:-0} deleted_bytes=${deleted_bytes:-0} candidate_count=${candidate_count:-0}"
  export FUGUE_NODE_UPDATE_TASK_RESULT_MESSAGE
  log_task "${FUGUE_NODE_UPDATE_TASK_RESULT_MESSAGE}"
}

localpv_inventory_json() {
  local vg_name="${FUGUE_NODE_UPDATE_TASK_VG_NAME:-${FUGUE_LOCALPV_VG_NAME}}"
  local image_path="${FUGUE_NODE_UPDATE_TASK_IMAGE_PATH:-${FUGUE_LOCALPV_IMAGE_PATH}}"
  local cluster_node="${FUGUE_NODE_UPDATER_CLUSTER_NODE_NAME:-${FUGUE_JOIN_NODE_NAME:-$(hostname -s 2>/dev/null || hostname 2>/dev/null || true)}}"
  if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is required for LocalPV inventory" >&2
    return 2
  fi
  python3 - "${vg_name}" "${image_path}" "${cluster_node}" <<'PY_LOCALPV_INVENTORY'
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

vg_name, image_path, cluster_node = sys.argv[1:4]

def run(cmd):
    try:
        proc = subprocess.run(cmd, check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout

def load_json(raw):
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None

def lvm_rows(raw, key):
    data = load_json(raw)
    if not data:
        return []
    reports = data.get("report") or []
    if not reports:
        return []
    rows = reports[0].get(key) or []
    return rows if isinstance(rows, list) else []

def parse_int(value):
    if value is None:
        return 0
    text = str(value).strip()
    text = re.sub(r"[^0-9.]", "", text)
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0

def real(path):
    if not path:
        return ""
    try:
        return os.path.realpath(path)
    except OSError:
        return path

unsafe = []
node_roles = []
lv_names = []
active_lv_count = 0
pv_size_bytes = 0
pv_free_bytes = 0
loop_device = ""
loop_backing_file = ""
bound_pvc_refs = []

image_size_bytes = 0
if os.path.exists(image_path):
    image_size_bytes = os.stat(image_path).st_size
else:
    unsafe.append("image_path_missing")

vgs_raw = run(["vgs", vg_name, "--units", "b", "--nosuffix", "--reportformat", "json", "-o", "vg_name,vg_size,vg_free"])
lvs_raw = run(["lvs", "--reportformat", "json", "-o", "lv_name,vg_name,lv_active,lv_attr"])
pvs_raw = run(["pvs", "--units", "b", "--nosuffix", "--reportformat", "json", "-o", "pv_name,vg_name,pv_size,pv_free"])
if vgs_raw is None or lvs_raw is None or pvs_raw is None:
    unsafe.append("lvm_tools_unavailable_or_vg_missing")

for row in lvm_rows(vgs_raw, "vg"):
    if str(row.get("vg_name", "")).strip() == vg_name:
        pv_size_bytes = parse_int(row.get("vg_size"))
        pv_free_bytes = parse_int(row.get("vg_free"))
        break

for row in lvm_rows(lvs_raw, "lv"):
    if str(row.get("vg_name", "")).strip() != vg_name:
        continue
    name = str(row.get("lv_name", "")).strip()
    if name:
        lv_names.append(name)
    active = str(row.get("lv_active", "")).strip().lower()
    attr = str(row.get("lv_attr", "")).strip().lower()
    if active in ("active", "yes") or (len(attr) > 4 and attr[4] == "a"):
        active_lv_count += 1

loop_raw = run(["losetup", "--json"])
loop_data = load_json(loop_raw)
if loop_data is None:
    unsafe.append("losetup_unavailable")
else:
    wanted = real(image_path)
    for item in loop_data.get("loopdevices") or []:
        backing = item.get("back-file") or item.get("back_file") or item.get("backing_file") or ""
        if real(backing) == wanted:
            loop_device = item.get("name") or ""
            loop_backing_file = backing
            break
    if not loop_device and image_size_bytes > 0:
        unsafe.append("loop_device_missing")

if loop_device and real(loop_backing_file) != real(image_path):
    unsafe.append("loop_backing_mismatch")

node_raw = run(["kubectl", "get", "node", cluster_node, "-o", "json"]) if cluster_node else None
node_data = load_json(node_raw)
if node_data:
    labels = (node_data.get("metadata") or {}).get("labels") or {}
    for key, value in labels.items():
        if key.startswith("node-role.kubernetes.io/"):
            role = key.split("/", 1)[1]
            if role:
                node_roles.append(role)
        if key in ("fugue.io/node-role", "fugue.io/roles", "fugue.dev/node-role", "fugue.dev/roles"):
            node_roles.extend([part.strip() for part in str(value).replace(";", ",").split(",") if part.strip()])

pv_raw = run(["kubectl", "get", "pv", "-o", "json"])
pv_data = load_json(pv_raw)
pvc_data = load_json(run(["kubectl", "get", "pvc", "-A", "-o", "json"]))
if pv_data is None:
    unsafe.append("kubectl_pv_unavailable")
else:
    pvc_lookup = {}
    if pvc_data:
        for pvc in pvc_data.get("items") or []:
            meta = pvc.get("metadata") or {}
            spec = pvc.get("spec") or {}
            volume = spec.get("volumeName")
            if volume:
                pvc_lookup[volume] = f"{meta.get('namespace', '')}/{meta.get('name', '')}".strip("/")

    def pv_targets_node(pv):
        if not cluster_node:
            return False
        affinity = (((pv.get("spec") or {}).get("nodeAffinity") or {}).get("required") or {})
        terms = affinity.get("nodeSelectorTerms") or []
        for term in terms:
            for expr in term.get("matchExpressions") or []:
                key = expr.get("key")
                values = [str(v) for v in (expr.get("values") or [])]
                if key in ("kubernetes.io/hostname", "node.kubernetes.io/instance") and cluster_node in values:
                    return True
        return False

    for pv in pv_data.get("items") or []:
        spec = pv.get("spec") or {}
        status = pv.get("status") or {}
        phase = str(status.get("phase") or "")
        local_path = ((spec.get("local") or {}).get("path") or "")
        storage_class = str(spec.get("storageClassName") or "")
        if phase != "Bound":
            continue
        if not local_path and "local" not in storage_class.lower() and "lvm" not in storage_class.lower():
            continue
        if cluster_node and not pv_targets_node(pv):
            continue
        name = (pv.get("metadata") or {}).get("name") or ""
        ref = pvc_lookup.get(name)
        if not ref:
            claim = spec.get("claimRef") or {}
            ref = f"{claim.get('namespace', '')}/{claim.get('name', '')}".strip("/")
        bound_pvc_refs.append(ref or name)

if lv_names:
    unsafe.append("active_lvs_present")
if active_lv_count:
    unsafe.append("active_lvs_present")
if bound_pvc_refs:
    unsafe.append("bound_pvs_present")
if not loop_device and image_size_bytes > 0:
    unsafe.append("loop_device_missing")
unsafe = sorted(set(reason for reason in unsafe if reason))

inventory = {
    "cluster_node_name": cluster_node,
    "node_roles": sorted(set(node_roles)),
    "vg_name": vg_name,
    "image_path": image_path,
    "image_size_bytes": image_size_bytes,
    "loop_device": loop_device,
    "loop_backing_file": loop_backing_file,
    "pv_size_bytes": pv_size_bytes,
    "pv_free_bytes": pv_free_bytes,
    "lv_count": len(lv_names),
    "lv_names": sorted(lv_names),
    "active_lv_count": active_lv_count,
    "bound_pv_count": len(bound_pvc_refs),
    "bound_pvc_refs": sorted(set(bound_pvc_refs)),
    "safe_to_decommission": not unsafe,
    "unsafe_reasons": unsafe,
    "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
}
print(json.dumps({"inventory": inventory}, separators=(",", ":")))
PY_LOCALPV_INVENTORY
}

report_lvm_localpv_inventory() {
  local payload=""
  payload="$(localpv_inventory_json)"
  api_json POST /v1/node-updater/localpv/inventory "${payload}" >/dev/null
  log_task "reported LocalPV inventory"
}

localpv_decommission_decision_json() {
  local inventory_file="$1"
  local dry_run="$2"
  local allow_delete="$3"
  local allow_policy="$4"
  local expected_image_size="${5:-}"
  local expected_lv_count="${6:-}"
  local expected_bound_pv_count="${7:-}"
  python3 - "${inventory_file}" "${dry_run}" "${allow_delete}" "${allow_policy}" "${expected_image_size}" "${expected_lv_count}" "${expected_bound_pv_count}" <<'PY_LOCALPV_DECISION'
import json
import sys

path, dry_run_raw, allow_delete_raw, allow_policy_raw, expected_image_raw, expected_lv_raw, expected_bound_raw = sys.argv[1:8]
with open(path, "r", encoding="utf-8") as fh:
    inventory = (json.load(fh).get("inventory") or {})

def truthy(value):
    return str(value).strip().lower() in ("1", "true", "yes", "on")

def parse_expected(raw):
    raw = str(raw or "").strip()
    if raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None

dry_run = truthy(dry_run_raw)
allow_delete = truthy(allow_delete_raw)
allow_policy = truthy(allow_policy_raw) or str(allow_policy_raw).strip().lower() in ("localpv-removable", "allow-delete", "decommission-allowed")
unsafe = list(inventory.get("unsafe_reasons") or [])

if int(inventory.get("lv_count") or 0) != 0:
    unsafe.append("active_lvs_present")
if int(inventory.get("bound_pv_count") or 0) != 0:
    unsafe.append("bound_pvs_present")
if not inventory.get("image_path"):
    unsafe.append("image_path_missing")
if int(inventory.get("image_size_bytes") or 0) <= 0:
    unsafe.append("image_path_missing")
if not inventory.get("loop_device"):
    unsafe.append("loop_device_missing")
if os.path.realpath(str(inventory.get("loop_backing_file") or "")) != os.path.realpath(str(inventory.get("image_path") or "")):
    unsafe.append("loop_backing_mismatch")
if not allow_policy:
    unsafe.append("node_role_policy_not_allowed")

if not dry_run:
    if not allow_delete:
        unsafe.append("allow_delete_false")
    expected_image = parse_expected(expected_image_raw)
    expected_lv = parse_expected(expected_lv_raw)
    expected_bound = parse_expected(expected_bound_raw)
    if expected_image is None or expected_lv is None or expected_bound is None:
        unsafe.append("missing_expected_preflight")
    else:
        if expected_image != int(inventory.get("image_size_bytes") or 0):
            unsafe.append("expected_image_size_mismatch")
        if expected_lv != int(inventory.get("lv_count") or 0):
            unsafe.append("expected_lv_count_mismatch")
        if expected_bound != int(inventory.get("bound_pv_count") or 0):
            unsafe.append("expected_bound_pv_count_mismatch")

unsafe = sorted(set(reason for reason in unsafe if reason))
commands = [
    f"systemctl stop {inventory.get('service', 'fugue-lvm-localpv-loop.service')}",
    f"losetup -d {inventory.get('loop_device', '')}",
    f"systemctl disable {inventory.get('service', 'fugue-lvm-localpv-loop.service')}",
    f"rm -f -- {inventory.get('image_path', '')}",
]
print(json.dumps({
    "safe": not unsafe,
    "dry_run": dry_run,
    "allow_delete": allow_delete,
    "unsafe_reasons": unsafe,
    "expected_freed_bytes": int(inventory.get("image_size_bytes") or 0),
    "commands": commands,
}, separators=(",", ":")))
PY_LOCALPV_DECISION
}

decommission_lvm_localpv() {
  local dry_run="${FUGUE_NODE_UPDATE_TASK_DRY_RUN:-true}"
  local allow_delete="${FUGUE_NODE_UPDATE_TASK_ALLOW_DELETE:-false}"
  local allow_policy="${FUGUE_NODE_UPDATE_TASK_ALLOW_LOCALPV_DECOMMISSION:-${FUGUE_NODE_UPDATE_TASK_NODE_ROLE_POLICY:-false}}"
  local expected_image_size="${FUGUE_NODE_UPDATE_TASK_EXPECTED_IMAGE_SIZE_BYTES:-}"
  local expected_lv_count="${FUGUE_NODE_UPDATE_TASK_EXPECTED_LV_COUNT:-}"
  local expected_bound_pv_count="${FUGUE_NODE_UPDATE_TASK_EXPECTED_BOUND_PV_COUNT:-}"
  local service="${FUGUE_NODE_UPDATE_TASK_LOOP_SERVICE:-${FUGUE_LOCALPV_LOOP_SERVICE}}"
  local inventory_file=""
  local decision_file=""
  local payload=""
  case "${dry_run}" in true|false) ;; *) dry_run=true ;; esac
  case "${allow_delete}" in true|false) ;; *) allow_delete=false ;; esac
  inventory_file="$(mktemp)"
  decision_file="$(mktemp)"
  payload="$(localpv_inventory_json)"
  printf '%s' "${payload}" >"${inventory_file}"
  api_json POST /v1/node-updater/localpv/inventory "${payload}" >/dev/null || true
  localpv_decommission_decision_json "${inventory_file}" "${dry_run}" "${allow_delete}" "${allow_policy}" "${expected_image_size}" "${expected_lv_count}" "${expected_bound_pv_count}" >"${decision_file}"
  if ! grep -Eq '"safe"[[:space:]]*:[[:space:]]*true' "${decision_file}"; then
    log_task "LocalPV decommission refused $(cat "${decision_file}")"
    rm -f "${inventory_file}" "${decision_file}"
    return 1
  fi
  if [ "${dry_run}" = "true" ]; then
    log_task "LocalPV decommission dry-run $(cat "${decision_file}")"
    rm -f "${inventory_file}" "${decision_file}"
    return 0
  fi
  if [ "${allow_delete}" != "true" ]; then
    log_task "LocalPV decommission refused allow_delete=false"
    rm -f "${inventory_file}" "${decision_file}"
    return 1
  fi
  local loop_device=""
  local image_path=""
  loop_device="$(python3 -c 'import json,sys; print((json.load(open(sys.argv[1])).get("inventory") or {}).get("loop_device",""))' "${inventory_file}")"
  image_path="$(python3 -c 'import json,sys; print((json.load(open(sys.argv[1])).get("inventory") or {}).get("image_path",""))' "${inventory_file}")"
  if [ -z "${loop_device}" ] || [ -z "${image_path}" ]; then
    log_task "LocalPV decommission refused missing verified loop device or image path"
    rm -f "${inventory_file}" "${decision_file}"
    return 1
  fi
  systemctl stop "${service}"
  losetup -d "${loop_device}"
  systemctl disable "${service}"
  rm -f -- "${image_path}"
  payload="$(localpv_inventory_json || true)"
  if [ -n "${payload}" ]; then
    api_json POST /v1/node-updater/localpv/inventory "${payload}" >/dev/null || true
  fi
  log_task "LocalPV decommission completed $(cat "${decision_file}")"
  rm -f "${inventory_file}" "${decision_file}"
}

verify_systemd_escape_hatch() {
  local checked=0
  local unit=""
  for unit in fugue-edge.service fugue-dns.service; do
    if systemctl list-unit-files "${unit}" >/dev/null 2>&1; then
      checked=$((checked + 1))
      log_task "${unit} is installed"
    fi
  done
  for env_file in "${FUGUE_NODE_UPDATER_EDGE_ENV_FILE}" "${FUGUE_NODE_UPDATER_DNS_ENV_FILE}"; do
    if [ -r "${env_file}" ]; then
      checked=$((checked + 1))
      if grep -Eq '^(FUGUE_EDGE_TOKEN|FUGUE_DNS_TOKEN|FUGUE_BUNDLE_SIGNING_KEY)=' "${env_file}"; then
        log_task "$(basename "${env_file}") keeps secret environment entries locally"
      fi
      if grep -Eq '^(FUGUE_API_URL|FUGUE_EDGE_DISCOVERY_GENERATION|FUGUE_DNS_DISCOVERY_GENERATION)=' "${env_file}"; then
        log_task "$(basename "${env_file}") has non-secret discovery metadata"
      fi
    fi
  done
  if [ "${checked}" -eq 0 ]; then
    log_task "no host-level edge/dns escape hatch units or env files detected"
  fi
}

run_task() {
  case "${FUGUE_NODE_UPDATE_TASK_TYPE}" in
    refresh-join-config)
      log_task "refreshing join configuration from discovery bundle"
      if reconcile_node_state; then
        log_task "join configuration refreshed"
        return 0
      fi
      return 1
      ;;
    restart-k3s-agent)
      restart_k3s_agent
      ;;
    upgrade-k3s-agent)
      upgrade_k3s_agent
      ;;
    upgrade-node-updater)
      upgrade_node_updater
      ;;
    diagnose-node)
      diagnose_node
      ;;
    install-nfs-client-tools)
      install_nfs_client_tools
      ;;
    prepull-system-images)
      prepull_system_images
      ;;
    prepull-app-images)
      prepull_app_images
      ;;
    replicate-app-image)
      replicate_app_image
      ;;
    verify-image-cache)
      verify_image_cache
      ;;
    prune-image-cache)
      prune_image_cache
      ;;
    report-image-cache-inventory)
      report_image_cache_inventory
      ;;
    report-lvm-localpv-inventory)
      report_lvm_localpv_inventory
      ;;
    decommission-lvm-localpv)
      decommission_lvm_localpv
      ;;
    verify-systemd-escape-hatch)
      verify_systemd_escape_hatch
      ;;
    *)
      echo "unsupported node update task type: ${FUGUE_NODE_UPDATE_TASK_TYPE}" >&2
      return 2
      ;;
  esac
}

run_once() {
  local task_env=""
  local rc=0
  mkdir -p "${FUGUE_NODE_UPDATER_WORK_DIR}"
  if load_cached_env_file "${FUGUE_NODE_UPDATER_DISCOVERY_ENV_FILE}"; then
    log "loaded cached discovery env"
  fi
  if load_cached_env_file "${FUGUE_NODE_UPDATER_STATE_ENV_FILE}"; then
    log "loaded cached state env"
  fi
  if reconcile_node_state; then
    log "node state reconciled"
  else
    log "node state reconciliation did not make changes or could not complete"
  fi
  heartbeat || log "heartbeat failed"
  task_env="$(mktemp)"
  if ! api_form GET "/v1/node-updater/tasks?format=env&limit=1" >"${task_env}"; then
    record_last_error "task poll failed; continuing in degraded offline mode"
    log "task poll failed; continuing in degraded offline mode"
    rm -f "${task_env}"
    return 0
  fi
  # shellcheck disable=SC1090
  . "${task_env}"
  rm -f "${task_env}"
  if [ -z "${FUGUE_NODE_UPDATE_TASK_ID:-}" ]; then
    log "no pending task"
    return 0
  fi
  log "claiming task ${FUGUE_NODE_UPDATE_TASK_ID} (${FUGUE_NODE_UPDATE_TASK_TYPE})"
  claim_task
  FUGUE_NODE_UPDATE_TASK_RESULT_MESSAGE=""
  if run_task; then
    clear_last_error
    complete_task completed "${FUGUE_NODE_UPDATE_TASK_RESULT_MESSAGE:-node update task completed}"
    heartbeat || true
    return 0
  else
    rc=$?
    record_last_error "task ${FUGUE_NODE_UPDATE_TASK_ID} failed with exit code ${rc}"
    complete_task failed "node update task failed" "$(last_error)"
    heartbeat || true
    return "${rc}"
  fi
}

case "${1:-run-once}" in
  run-once)
    require_cmd curl
    require_cmd systemctl
    require_cmd cmp
    require_cmd awk
    require_cmd sed
    run_once
    ;;
  heartbeat)
    require_cmd curl
    heartbeat
    ;;
  version)
    printf '%s\n' "${FUGUE_NODE_UPDATER_VERSION}"
    ;;
  capabilities)
    printf '%s\n' "${FUGUE_NODE_UPDATER_CAPABILITIES}"
    ;;
  *)
    echo "usage: fugue-node-updater [run-once|heartbeat|version|capabilities]" >&2
    exit 2
    ;;
esac
`
	return strings.ReplaceAll(script, "__FUGUE_API_BASE__", apiBase)
}
