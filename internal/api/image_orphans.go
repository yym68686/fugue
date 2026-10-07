package api

import (
	"encoding/json"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"net/http"
	"sort"
	"time"
)

func (s *Server) handleAdminGetImageOrphanPolicy(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	p, err := s.store.GetImageOrphanPolicy()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": p})
}
func (s *Server) handleAdminPutImageOrphanPolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var in struct {
		SweepIntervalSeconds   int      `json:"sweep_interval_seconds"`
		MaxTargetsPerNode      int      `json:"max_targets_per_node"`
		NodeCooldownSeconds    int      `json:"node_cooldown_seconds"`
		ExpectedGeneration     int64    `json:"expected_generation"`
		Mode                   string   `json:"mode"`
		RepositoryPrefixes     []string `json:"repository_prefixes"`
		ExcludedNodes          []string `json:"excluded_nodes"`
		QuarantineSeconds      int      `json:"quarantine_seconds"`
		MinimumObservations    int      `json:"minimum_observations"`
		InventoryMaxAgeSeconds int      `json:"inventory_max_age_seconds"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid policy")
		return
	}
	p, err := s.store.UpdateImageOrphanPolicy(model.ImageOrphanPolicy{SweepIntervalSeconds: in.SweepIntervalSeconds, MaxTargetsPerNode: in.MaxTargetsPerNode, NodeCooldownSeconds: in.NodeCooldownSeconds, Mode: in.Mode, RepositoryPrefixes: in.RepositoryPrefixes, ExcludedNodes: in.ExcludedNodes, QuarantineSeconds: in.QuarantineSeconds, MinimumObservations: in.MinimumObservations, InventoryMaxAgeSeconds: in.InventoryMaxAgeSeconds}, in.ExpectedGeneration, principal.ActorType+":"+principal.ActorID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": p})
}
func (s *Server) handleAdminListImageOrphans(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	p, byID, complete, reason, err := s.store.ImageOrphanContext(time.Now().UTC())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	items := []model.ImageOrphanDecision{}
	for _, d := range byID {
		items = append(items, d)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"policy": p, "decisions": items, "coverage_complete": complete, "coverage_reason": reason})
}
