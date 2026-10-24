package api

import (
	"encoding/json"
	"errors"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

func (s *Server) logPlatformReleaseRejection(a model.PlatformArtifact, channel string, err error) {
	if s.log == nil || err == nil {
		return
	}
	class := "store_error"
	switch {
	case errors.Is(err, store.ErrConflict):
		class = "conflict"
	case errors.Is(err, store.ErrInvalidInput):
		class = "invalid_input"
	case errors.Is(err, store.ErrNotFound):
		class = "not_found"
	}
	identity, _ := platformconfig.Digest(struct{ ID, Scope string }{a.ID, a.ScopeKey})
	data := map[string]any{"event": "platform_release_rejected", "stage": store.PlatformReleaseFailureStage(err), "class": class, "artifact_identity_digest": identity, "content_hash": a.ContentHash, "channel": channel}
	raw, encodeErr := json.Marshal(data)
	if encodeErr == nil && len(raw) <= 1024 {
		s.log.Print(string(raw))
	}
}
