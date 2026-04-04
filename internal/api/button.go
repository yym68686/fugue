package api

import (
	"net/http"

	"fugue/assets"
)

func (s *Server) handleButtonSVG(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/svg+xml; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_, _ = w.Write(assets.ButtonSVG)
}
