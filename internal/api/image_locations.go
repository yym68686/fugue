package api

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleListImageLocations(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	locations, err := s.store.ListImageLocations(imageLocationFilterFromRequest(r, principal, true))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"image_locations": locations})
}

func (s *Server) handleReportImageLocation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	location, err := s.decodeImageLocationReport(r, principal)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	location, err = s.store.UpsertImageLocation(location)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"image_location": location})
}

func (s *Server) handleNodeUpdaterListImageLocations(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.ActorType != model.ActorTypeNodeUpdater {
		httpx.WriteError(w, http.StatusForbidden, "node updater credentials required")
		return
	}
	locations, err := s.store.ListImageLocations(imageLocationFilterFromRequest(r, principal, false))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"image_locations": locations})
}

func (s *Server) handleNodeUpdaterReportImageLocation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.ActorType != model.ActorTypeNodeUpdater {
		httpx.WriteError(w, http.StatusForbidden, "node updater credentials required")
		return
	}
	location, err := s.decodeImageLocationReport(r, principal)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	updater, err := s.nodeUpdaterByPrincipal(principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if location.TenantID == "" {
		location.TenantID = updater.TenantID
	}
	if location.NodeID == "" {
		location.NodeID = updater.MachineID
	}
	if location.RuntimeID == "" {
		location.RuntimeID = updater.RuntimeID
	}
	if location.ClusterNodeName == "" {
		location.ClusterNodeName = updater.ClusterNodeName
	}
	location, err = s.store.UpsertImageLocation(location)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"image_location": location})
}

func (s *Server) handleAgentListImageLocations(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.ActorType != model.ActorTypeRuntime {
		httpx.WriteError(w, http.StatusForbidden, "runtime credentials required")
		return
	}
	locations, err := s.store.ListImageLocations(imageLocationFilterFromRequest(r, principal, false))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"image_locations": locations})
}

func (s *Server) handleAgentReportImageLocation(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.ActorType != model.ActorTypeRuntime {
		httpx.WriteError(w, http.StatusForbidden, "runtime credentials required")
		return
	}
	location, err := s.decodeImageLocationReport(r, principal)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	runtimeObj, err := s.store.GetRuntime(principal.ActorID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if location.TenantID == "" {
		location.TenantID = runtimeObj.TenantID
	}
	if location.RuntimeID == "" {
		location.RuntimeID = runtimeObj.ID
	}
	if location.ClusterNodeName == "" {
		location.ClusterNodeName = runtimeObj.ClusterNodeName
	}
	location, err = s.store.UpsertImageLocation(location)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"image_location": location})
}

func (s *Server) decodeImageLocationReport(r *http.Request, principal model.Principal) (model.ImageLocation, error) {
	var req struct {
		TenantID          string     `json:"tenant_id"`
		AppID             string     `json:"app_id"`
		ImageRef          string     `json:"image_ref"`
		Digest            string     `json:"digest"`
		SourceOperationID string     `json:"source_operation_id"`
		NodeID            string     `json:"node_id"`
		RuntimeID         string     `json:"runtime_id"`
		ClusterNodeName   string     `json:"cluster_node_name"`
		CacheEndpoint     string     `json:"cache_endpoint"`
		Status            string     `json:"status"`
		LastSeenAt        *time.Time `json:"last_seen_at"`
		SizeBytes         int64      `json:"size_bytes"`
		LastError         string     `json:"last_error"`
	}
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			return model.ImageLocation{}, err
		}
		req.TenantID = r.Form.Get("tenant_id")
		req.AppID = r.Form.Get("app_id")
		req.ImageRef = r.Form.Get("image_ref")
		req.Digest = r.Form.Get("digest")
		req.SourceOperationID = r.Form.Get("source_operation_id")
		req.NodeID = r.Form.Get("node_id")
		req.RuntimeID = r.Form.Get("runtime_id")
		req.ClusterNodeName = r.Form.Get("cluster_node_name")
		req.CacheEndpoint = r.Form.Get("cache_endpoint")
		req.Status = r.Form.Get("status")
		req.LastError = r.Form.Get("last_error")
	} else if err := httpx.DecodeJSON(r, &req); err != nil {
		return model.ImageLocation{}, err
	}
	location := model.ImageLocation{
		TenantID:          strings.TrimSpace(req.TenantID),
		AppID:             strings.TrimSpace(req.AppID),
		ImageRef:          strings.TrimSpace(req.ImageRef),
		Digest:            strings.TrimSpace(req.Digest),
		SourceOperationID: strings.TrimSpace(req.SourceOperationID),
		NodeID:            strings.TrimSpace(req.NodeID),
		RuntimeID:         strings.TrimSpace(req.RuntimeID),
		ClusterNodeName:   strings.TrimSpace(req.ClusterNodeName),
		CacheEndpoint:     strings.TrimRight(strings.TrimSpace(req.CacheEndpoint), "/"),
		Status:            strings.TrimSpace(req.Status),
		LastSeenAt:        req.LastSeenAt,
		SizeBytes:         req.SizeBytes,
		LastError:         strings.TrimSpace(req.LastError),
	}
	if location.AppID != "" {
		app, err := s.store.GetApp(location.AppID)
		if err != nil {
			return model.ImageLocation{}, err
		}
		if !principal.IsPlatformAdmin() && app.TenantID != principal.TenantID {
			return model.ImageLocation{}, storeErrorNotFound()
		}
		location.TenantID = app.TenantID
	} else if !principal.IsPlatformAdmin() {
		location.TenantID = principal.TenantID
	}
	if location.TenantID == "" && !principal.IsPlatformAdmin() {
		return model.ImageLocation{}, errBadImageLocation("tenant_id is required")
	}
	if location.ImageRef == "" && location.Digest == "" {
		return model.ImageLocation{}, errBadImageLocation("image_ref or digest is required")
	}
	return location, nil
}

func imageLocationFilterFromRequest(r *http.Request, principal model.Principal, allowPlatformAdmin bool) model.ImageLocationFilter {
	query := r.URL.Query()
	platformAdmin := allowPlatformAdmin && principal.IsPlatformAdmin()
	filter := model.ImageLocationFilter{
		TenantID:        principal.TenantID,
		AppID:           strings.TrimSpace(query.Get("app_id")),
		ImageRef:        strings.TrimSpace(query.Get("image_ref")),
		Digest:          strings.TrimSpace(query.Get("digest")),
		Status:          strings.TrimSpace(query.Get("status")),
		NodeID:          strings.TrimSpace(query.Get("node_id")),
		RuntimeID:       strings.TrimSpace(query.Get("runtime_id")),
		ClusterNodeName: strings.TrimSpace(query.Get("cluster_node_name")),
		PlatformAdmin:   platformAdmin,
	}
	if platformAdmin && strings.TrimSpace(query.Get("tenant_id")) != "" {
		filter.TenantID = strings.TrimSpace(query.Get("tenant_id"))
		filter.PlatformAdmin = false
	}
	return filter
}

type badImageLocationError string

func (e badImageLocationError) Error() string {
	return string(e)
}

func errBadImageLocation(message string) error {
	return badImageLocationError(message)
}

func storeErrorNotFound() error {
	return badImageLocationError("resource not found")
}
