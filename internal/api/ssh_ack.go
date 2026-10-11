package api

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (server *Server) handleAdminGetSSHFrontAcknowledgement(writer http.ResponseWriter, request *http.Request) {
	if !mustPrincipal(request).IsPlatformAdmin() {
		httpx.WriteError(writer, http.StatusForbidden, "platform admin required")
		return
	}
	edgeID, instanceUID := request.PathValue("edge_id"), request.PathValue("instance_uid")
	for _, identifier := range []string{edgeID, instanceUID} {
		if len(identifier) == 0 || len(identifier) > 256 || strings.IndexFunc(identifier, func(character rune) bool {
			return !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("_.-", character))
		}) >= 0 {
			httpx.WriteError(writer, http.StatusBadRequest, "canonical edge and instance identities required")
			return
		}
	}
	ack, err := server.store.GetEdgeSSHAcknowledgement(edgeID, instanceUID)
	if err != nil {
		server.writeStoreError(writer, err)
		return
	}
	bundle, err := server.deriveEdgeSSHRouteBundle(store.AppSSHRouteOptions{EdgeID: edgeID, EdgeGroupID: ack.EdgeGroupID})
	if err != nil {
		server.writeStoreError(writer, err)
		return
	}
	now := time.Now().UTC()
	fresh := ack.Persisted && ack.ExpiresAt.After(now) && !ack.ObservedAt.After(now.Add(5*time.Second)) && !ack.LoadedAt.After(now.Add(5*time.Second)) && !ack.ExpiresAt.After(ack.ObservedAt.Add(2*time.Minute))
	matches := ack.BundleVersion == bundle.Version && ack.RouteDigest == model.SSHRoutesDigest(bundle.Routes)
	writer.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(writer, http.StatusOK, map[string]any{"acknowledgement": ack, "evaluated_at": now, "fresh": fresh, "current_routes_match": matches})
}

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
