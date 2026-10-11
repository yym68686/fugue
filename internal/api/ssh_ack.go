package api

import (
	"net/http"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (server *Server) handleAcknowledgeEdgeSSHRoutes(writer http.ResponseWriter, request *http.Request) {
	authentication, allowed := server.authorizeEdgeRequest(writer, request)
	if !allowed {
		return
	}
	var input model.EdgeSSHAcknowledgementRequest
	request.Body = http.MaxBytesReader(writer, request.Body, 4<<20)
	if err := httpx.DecodeJSON(request, &input); err != nil || !input.Persisted || input.InstanceUID == "" || input.LoadedAt.IsZero() || input.Connections == nil {
		httpx.WriteError(writer, http.StatusBadRequest, "a persisted, timestamped instance acknowledgement is required")
		return
	}
	if !authentication.Scoped || authentication.EdgeID != input.EdgeID || authentication.EdgeGroupID != input.EdgeGroupID {
		httpx.WriteError(writer, http.StatusForbidden, "acknowledgement requires the exact node-scoped token")
		return
	}
	bundle, err := server.deriveEdgeSSHRouteBundle(store.AppSSHRouteOptions{EdgeID: input.EdgeID, EdgeGroupID: input.EdgeGroupID})
	if err != nil {
		server.writeStoreError(writer, err)
		return
	}
	if bundle.Version != input.BundleVersion || model.SSHRoutesDigest(bundle.Routes) != input.RouteDigest {
		httpx.WriteError(writer, http.StatusConflict, "acknowledged SSH routes do not match the current authorized bundle")
		return
	}
	now := time.Now().UTC()
	ack := model.EdgeSSHAcknowledgement{EdgeSSHAcknowledgementRequest: input, Routes: bundle.Routes, ObservedAt: now, ExpiresAt: now.Add(2 * time.Minute)}
	if err := server.store.RecordEdgeSSHAcknowledgement(ack); err != nil {
		server.writeStoreError(writer, err)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(writer, http.StatusOK, map[string]any{"accepted": true, "observed_at": ack.ObservedAt, "expires_at": ack.ExpiresAt})
}
