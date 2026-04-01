package api

import (
	"net/http"
)

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.registerOpenAPIRoutes(mux)
	s.registerGeneratedRoutes(mux)

	return loggingMiddleware(s.log, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.maybeHandleAppProxy(w, r) {
			return
		}
		mux.ServeHTTP(w, r)
	}))
}
