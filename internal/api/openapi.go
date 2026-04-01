package api

import (
	"net/http"

	"fugue/internal/apispec"
)

const openAPIDocsHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Fugue API Docs</title>
  <style>
    body { margin: 0; background: #f5f2eb; color: #1d1b18; font-family: "Iowan Old Style", "Palatino Linotype", serif; }
    header { padding: 18px 24px; border-bottom: 1px solid rgba(29,27,24,0.12); background: linear-gradient(135deg, #f7f0df, #f2efe8); }
    header h1 { margin: 0; font-size: 24px; font-weight: 600; }
    header p { margin: 6px 0 0; font-size: 14px; color: rgba(29,27,24,0.72); }
  </style>
  <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"></script>
</head>
<body>
  <header>
    <h1>Fugue API</h1>
    <p>Authoritative contract served from <code>/openapi.json</code> and <code>/openapi.yaml</code>.</p>
  </header>
  <redoc spec-url="/openapi.json"></redoc>
</body>
</html>
`

func (s *Server) registerOpenAPIRoutes(mux *http.ServeMux) {
	mux.Handle("GET /openapi.yaml", http.HandlerFunc(s.handleOpenAPIYAML))
	mux.Handle("GET /openapi.json", http.HandlerFunc(s.handleOpenAPIJSON))
	mux.Handle("GET /docs", http.HandlerFunc(s.handleOpenAPIDocs))
}

func (s *Server) handleOpenAPIYAML(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	_, _ = w.Write(apispec.YAML())
}

func (s *Server) handleOpenAPIJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(apispec.JSON())
}

func (s *Server) handleOpenAPIDocs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(openAPIDocsHTML))
}
