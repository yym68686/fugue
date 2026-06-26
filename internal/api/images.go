package api

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleListImages(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	images, err := s.store.ListImages(imageFilterFromRequest(r, principal, true))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"images": images})
}

func (s *Server) handleGetImage(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	image, err := s.store.GetImage(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"image": image})
}

func (s *Server) handleListImageReplicas(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	image, err := s.store.GetImage(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	filter := imageReplicaFilterFromRequest(r, principal, true)
	filter.ImageID = image.ID
	replicas, err := s.store.ListImageReplicas(filter)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"replicas": replicas})
}

func (s *Server) handleReportImageReplica(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	image, err := s.store.GetImage(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	replica, err := s.decodeImageReplicaReport(r, principal, image)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	replica, err = s.store.UpsertImageReplica(replica)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	_, _ = s.store.UpsertImageLocation(imageLocationFromReplica(image, replica))
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"replica": replica})
}

func (s *Server) handleVerifyImage(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	image, err := s.store.GetImage(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	replicas, err := s.store.ListImageReplicas(model.ImageReplicaFilter{
		ImageID:       image.ID,
		TenantID:      principal.TenantID,
		Status:        model.ImageReplicaStatusPresent,
		PlatformAdmin: principal.IsPlatformAdmin(),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"image":            image,
		"healthy_replicas": replicas,
		"healthy_count":    len(replicas),
	})
}

func (s *Server) handleCreateImagePin(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	image, err := s.store.GetImage(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var req struct {
		AppID       string     `json:"app_id"`
		OperationID string     `json:"operation_id"`
		Reason      string     `json:"reason"`
		MinReplicas int        `json:"min_replicas"`
		ExpiresAt   *time.Time `json:"expires_at"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	pin, err := s.store.UpsertImagePin(model.ImagePin{
		ImageID:     image.ID,
		TenantID:    image.TenantID,
		AppID:       strings.TrimSpace(req.AppID),
		OperationID: strings.TrimSpace(req.OperationID),
		Reason:      strings.TrimSpace(req.Reason),
		MinReplicas: req.MinReplicas,
		ExpiresAt:   req.ExpiresAt,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"pin": pin})
}

func (s *Server) handleDeleteImagePin(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if _, err := s.store.GetImage(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin()); err != nil {
		s.writeStoreError(w, err)
		return
	}
	if err := s.store.DeleteImagePin(r.PathValue("pin_id"), principal.TenantID, principal.IsPlatformAdmin()); err != nil {
		s.writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListImageReplicationTasks(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	tasks, err := s.store.ListImageReplicationTasks(imageReplicationTaskFilterFromRequest(r, principal, true))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) handleCreateImageReplicationTask(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	var req struct {
		ImageID               string `json:"image_id"`
		AppID                 string `json:"app_id"`
		SourceReplicaID       string `json:"source_replica_id"`
		SourceCacheEndpoint   string `json:"source_cache_endpoint"`
		TargetNodeID          string `json:"target_node_id"`
		TargetRuntimeID       string `json:"target_runtime_id"`
		TargetClusterNodeName string `json:"target_cluster_node_name"`
		Priority              string `json:"priority"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	image, err := s.store.GetImage(req.ImageID, principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	task, err := s.store.UpsertImageReplicationTask(model.ImageReplicationTask{
		ImageID:               image.ID,
		TenantID:              image.TenantID,
		AppID:                 firstNonEmptyImageAPIString(req.AppID, image.AppID),
		SourceReplicaID:       strings.TrimSpace(req.SourceReplicaID),
		SourceCacheEndpoint:   strings.TrimRight(strings.TrimSpace(req.SourceCacheEndpoint), "/"),
		TargetNodeID:          strings.TrimSpace(req.TargetNodeID),
		TargetRuntimeID:       strings.TrimSpace(req.TargetRuntimeID),
		TargetClusterNodeName: strings.TrimSpace(req.TargetClusterNodeName),
		Priority:              strings.TrimSpace(req.Priority),
		Status:                model.ImageReplicationTaskStatusPending,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"task": task})
}

func (s *Server) handleNodeUpdaterReportImageReplica(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.ActorType != model.ActorTypeNodeUpdater {
		httpx.WriteError(w, http.StatusForbidden, "node updater credentials required")
		return
	}
	updater, err := s.nodeUpdaterByPrincipal(principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var req struct {
		ImageID        string     `json:"image_id"`
		AppID          string     `json:"app_id"`
		Digest         string     `json:"digest"`
		CacheEndpoint  string     `json:"cache_endpoint"`
		FailureDomain  string     `json:"failure_domain"`
		Status         string     `json:"status"`
		LastVerifiedAt *time.Time `json:"last_verified_at"`
		LeaseExpiresAt *time.Time `json:"lease_expires_at"`
		SizeBytes      int64      `json:"size_bytes"`
		LastError      string     `json:"last_error"`
	}
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		req.ImageID = r.Form.Get("image_id")
		req.AppID = r.Form.Get("app_id")
		req.Digest = r.Form.Get("digest")
		req.CacheEndpoint = r.Form.Get("cache_endpoint")
		req.FailureDomain = r.Form.Get("failure_domain")
		req.Status = r.Form.Get("status")
		req.LastError = r.Form.Get("last_error")
	} else if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	image, err := s.store.GetImage(req.ImageID, "", true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	replica, err := s.store.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           firstNonEmptyImageAPIString(req.AppID, image.AppID),
		Digest:          firstNonEmptyImageAPIString(req.Digest, image.CanonicalDigest),
		NodeID:          updater.MachineID,
		RuntimeID:       updater.RuntimeID,
		ClusterNodeName: updater.ClusterNodeName,
		CacheEndpoint:   firstNonEmptyImageAPIString(strings.TrimRight(strings.TrimSpace(req.CacheEndpoint), "/"), s.nodeUpdaterImageCacheEndpoint(updater)),
		FailureDomain:   strings.TrimSpace(req.FailureDomain),
		Status:          strings.TrimSpace(req.Status),
		LastVerifiedAt:  firstTimePointer(req.LastVerifiedAt, time.Now().UTC()),
		LeaseExpiresAt:  req.LeaseExpiresAt,
		SizeBytes:       req.SizeBytes,
		LastError:       strings.TrimSpace(req.LastError),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	_, _ = s.store.UpsertImageLocation(imageLocationFromReplica(image, replica))
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"replica": replica})
}

func (s *Server) handleNodeUpdaterListImageReplicationTasks(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.ActorType != model.ActorTypeNodeUpdater {
		httpx.WriteError(w, http.StatusForbidden, "node updater credentials required")
		return
	}
	updater, err := s.nodeUpdaterByPrincipal(principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	filter := imageReplicationTaskFilterFromRequest(r, principal, false)
	filter.PlatformAdmin = true
	filter.TargetNodeID = updater.MachineID
	filter.TargetRuntimeID = updater.RuntimeID
	filter.TargetClusterNodeName = updater.ClusterNodeName
	tasks, err := s.store.ListImageReplicationTasks(filter)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func imageFilterFromRequest(r *http.Request, principal model.Principal, allowPlatformAdmin bool) model.ImageFilter {
	query := r.URL.Query()
	platformAdmin := allowPlatformAdmin && principal.IsPlatformAdmin()
	filter := model.ImageFilter{
		TenantID:        principal.TenantID,
		AppID:           strings.TrimSpace(query.Get("app_id")),
		ImageRef:        strings.TrimSpace(query.Get("image_ref")),
		CanonicalDigest: strings.TrimSpace(query.Get("digest")),
		LifecycleState:  strings.TrimSpace(query.Get("lifecycle_state")),
		PlatformAdmin:   platformAdmin,
	}
	if platformAdmin && strings.TrimSpace(query.Get("tenant_id")) != "" {
		filter.TenantID = strings.TrimSpace(query.Get("tenant_id"))
		filter.PlatformAdmin = false
	}
	return filter
}

func imageReplicaFilterFromRequest(r *http.Request, principal model.Principal, allowPlatformAdmin bool) model.ImageReplicaFilter {
	query := r.URL.Query()
	platformAdmin := allowPlatformAdmin && principal.IsPlatformAdmin()
	filter := model.ImageReplicaFilter{
		TenantID:        principal.TenantID,
		AppID:           strings.TrimSpace(query.Get("app_id")),
		Digest:          strings.TrimSpace(query.Get("digest")),
		NodeID:          strings.TrimSpace(query.Get("node_id")),
		RuntimeID:       strings.TrimSpace(query.Get("runtime_id")),
		ClusterNodeName: strings.TrimSpace(query.Get("cluster_node_name")),
		Status:          strings.TrimSpace(query.Get("status")),
		PlatformAdmin:   platformAdmin,
	}
	if platformAdmin && strings.TrimSpace(query.Get("tenant_id")) != "" {
		filter.TenantID = strings.TrimSpace(query.Get("tenant_id"))
		filter.PlatformAdmin = false
	}
	return filter
}

func imageReplicationTaskFilterFromRequest(r *http.Request, principal model.Principal, allowPlatformAdmin bool) model.ImageReplicationTaskFilter {
	query := r.URL.Query()
	platformAdmin := allowPlatformAdmin && principal.IsPlatformAdmin()
	filter := model.ImageReplicationTaskFilter{
		TenantID:              principal.TenantID,
		ImageID:               strings.TrimSpace(query.Get("image_id")),
		AppID:                 strings.TrimSpace(query.Get("app_id")),
		SourceReplicaID:       strings.TrimSpace(query.Get("source_replica_id")),
		TargetNodeID:          strings.TrimSpace(query.Get("target_node_id")),
		TargetRuntimeID:       strings.TrimSpace(query.Get("target_runtime_id")),
		TargetClusterNodeName: strings.TrimSpace(query.Get("target_cluster_node_name")),
		Priority:              strings.TrimSpace(query.Get("priority")),
		Status:                strings.TrimSpace(query.Get("status")),
		PlatformAdmin:         platformAdmin,
	}
	if platformAdmin && strings.TrimSpace(query.Get("tenant_id")) != "" {
		filter.TenantID = strings.TrimSpace(query.Get("tenant_id"))
		filter.PlatformAdmin = false
	}
	return filter
}

func (s *Server) decodeImageReplicaReport(r *http.Request, principal model.Principal, image model.Image) (model.ImageReplica, error) {
	var req struct {
		AppID           string     `json:"app_id"`
		Digest          string     `json:"digest"`
		NodeID          string     `json:"node_id"`
		RuntimeID       string     `json:"runtime_id"`
		ClusterNodeName string     `json:"cluster_node_name"`
		CacheEndpoint   string     `json:"cache_endpoint"`
		FailureDomain   string     `json:"failure_domain"`
		Status          string     `json:"status"`
		LastVerifiedAt  *time.Time `json:"last_verified_at"`
		LeaseExpiresAt  *time.Time `json:"lease_expires_at"`
		SizeBytes       int64      `json:"size_bytes"`
		LastError       string     `json:"last_error"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return model.ImageReplica{}, err
	}
	return model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           firstNonEmptyImageAPIString(req.AppID, image.AppID),
		Digest:          firstNonEmptyImageAPIString(req.Digest, image.CanonicalDigest),
		NodeID:          strings.TrimSpace(req.NodeID),
		RuntimeID:       strings.TrimSpace(req.RuntimeID),
		ClusterNodeName: strings.TrimSpace(req.ClusterNodeName),
		CacheEndpoint:   strings.TrimRight(strings.TrimSpace(req.CacheEndpoint), "/"),
		FailureDomain:   strings.TrimSpace(req.FailureDomain),
		Status:          strings.TrimSpace(req.Status),
		LastVerifiedAt:  firstTimePointer(req.LastVerifiedAt, time.Now().UTC()),
		LeaseExpiresAt:  req.LeaseExpiresAt,
		SizeBytes:       req.SizeBytes,
		LastError:       strings.TrimSpace(req.LastError),
	}, nil
}

func imageLocationFromReplica(image model.Image, replica model.ImageReplica) model.ImageLocation {
	return model.ImageLocation{
		TenantID:        replica.TenantID,
		AppID:           replica.AppID,
		ImageRef:        firstNonEmptyImageAPIString(image.ImageRef, image.CanonicalDigest),
		Digest:          firstNonEmptyImageAPIString(replica.Digest, image.CanonicalDigest),
		NodeID:          replica.NodeID,
		RuntimeID:       replica.RuntimeID,
		ClusterNodeName: replica.ClusterNodeName,
		CacheEndpoint:   replica.CacheEndpoint,
		Status:          imageLocationStatusFromReplica(replica.Status),
		LastSeenAt:      replica.LastVerifiedAt,
		SizeBytes:       replica.SizeBytes,
		LastError:       replica.LastError,
	}
}

func imageLocationStatusFromReplica(status string) string {
	switch strings.TrimSpace(status) {
	case model.ImageReplicaStatusPresent:
		return model.ImageLocationStatusPresent
	case model.ImageReplicaStatusCopying, model.ImageReplicaStatusVerifying, model.ImageReplicaStatusPlanned:
		return model.ImageLocationStatusPulling
	case model.ImageReplicaStatusMissing, model.ImageReplicaStatusStale:
		return model.ImageLocationStatusMissing
	case model.ImageReplicaStatusFailed:
		return model.ImageLocationStatusFailed
	default:
		return model.ImageLocationStatusPresent
	}
}

func firstNonEmptyImageAPIString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstTimePointer(existing *time.Time, fallback time.Time) *time.Time {
	if existing != nil {
		return existing
	}
	return &fallback
}
