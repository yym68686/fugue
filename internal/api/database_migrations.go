package api

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleCreateDatabaseMigration(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "database storage migration requires platform administrator authorization")
		return
	}
	var req struct {
		Kind                   string `json:"kind"`
		ResourceID             string `json:"resource_id"`
		Namespace              string `json:"namespace"`
		ClusterName            string `json:"cluster_name"`
		TargetStorageClassName string `json:"target_storage_class_name"`
		StorageSize            string `json:"storage_size,omitempty"`
		TemporaryReplicaCount  int    `json:"temporary_replica_count,omitempty"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Kind) == "" {
		req.Kind = "managed-postgres"
	}
	if strings.TrimSpace(req.ResourceID) == "" {
		req.ResourceID = "control-plane-postgres"
	}
	if strings.TrimSpace(req.Namespace) == "" {
		req.Namespace = firstNonEmptyString(s.controlPlaneNamespace, "fugue-system")
	}
	if strings.TrimSpace(req.ClusterName) == "" {
		req.ClusterName = s.controlPlanePostgresClusterName
	}
	if req.ResourceID != "control-plane-postgres" || req.Kind != "managed-postgres" || req.Namespace != firstNonEmptyString(s.controlPlaneNamespace, "fugue-system") || req.ClusterName != s.controlPlanePostgresClusterName {
		httpx.WriteError(w, http.StatusBadRequest, "database resource is not declared by the control-plane database catalog")
		return
	}
	if strings.TrimSpace(req.TargetStorageClassName) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "target_storage_class_name is required")
		return
	}
	m := model.DatabaseMigration{ID: model.NewID("database-migration"), Kind: req.Kind, ResourceID: req.ResourceID, Namespace: req.Namespace, ClusterName: req.ClusterName, TargetStorageClassName: req.TargetStorageClassName, StorageSize: req.StorageSize, TemporaryReplicaCount: req.TemporaryReplicaCount, Status: model.DatabaseMigrationStatusPending, RequestedByType: p.ActorType, RequestedByID: p.ActorID, CreatedAt: time.Now().UTC()}
	created, err := s.store.CreateDatabaseMigration(m)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"migration": created})
}

func (s *Server) handleGetDatabaseMigration(w http.ResponseWriter, r *http.Request) {
	p := mustPrincipal(r)
	if !p.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "database storage migration requires platform administrator authorization")
		return
	}
	m, err := s.store.GetDatabaseMigration(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"migration": m})
}
