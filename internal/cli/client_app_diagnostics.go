package cli

import (
	"encoding/base64"
	"net/http"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"fugue/internal/model"
)

type appDatabaseQueryColumn struct {
	Name         string `json:"name"`
	DatabaseType string `json:"database_type"`
}

type appDatabaseQueryResponse struct {
	Database   string                   `json:"database"`
	Host       string                   `json:"host"`
	User       string                   `json:"user"`
	Columns    []appDatabaseQueryColumn `json:"columns"`
	Rows       []map[string]any         `json:"rows"`
	RowCount   int                      `json:"row_count"`
	MaxRows    int                      `json:"max_rows"`
	Truncated  bool                     `json:"truncated,omitempty"`
	ReadOnly   bool                     `json:"read_only"`
	DurationMS int64                    `json:"duration_ms"`
}

type appRequestOptions struct {
	Method         string
	Path           string
	Query          map[string][]string
	Headers        map[string][]string
	HeadersFromEnv map[string]string
	Body           []byte
	Timeout        time.Duration
	MaxBodyBytes   int
}

type appRequestStreamOptions struct {
	Method         string
	Path           string
	Query          map[string][]string
	Headers        map[string][]string
	HeadersFromEnv map[string]string
	Body           []byte
	Timeout        time.Duration
	Accepts        []string
	MaxChunks      int
	MaxChunkBytes  int
}

func (c *Client) QueryAppDatabase(id string, statement string, maxRows int, timeout time.Duration) (appDatabaseQueryResponse, error) {
	req := map[string]any{
		"sql": strings.TrimSpace(statement),
	}
	if maxRows > 0 {
		req["max_rows"] = maxRows
	}
	if timeout > 0 {
		req["timeout_ms"] = timeout.Milliseconds()
	}
	var response appDatabaseQueryResponse
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "database", "query"), req, &response); err != nil {
		return appDatabaseQueryResponse{}, err
	}
	return response, nil
}

func (c *Client) RequestAppInternalHTTP(id string, opts appRequestOptions) (rawHTTPDiagnostic, error) {
	body, encoding := encodeRequestBodyForAPI(opts.Body)
	req := map[string]any{
		"method": strings.ToUpper(strings.TrimSpace(opts.Method)),
		"path":   strings.TrimSpace(opts.Path),
	}
	if len(opts.Query) > 0 {
		req["query"] = cloneStringSliceMap(opts.Query)
	}
	if len(opts.Headers) > 0 {
		req["headers"] = cloneStringSliceMap(opts.Headers)
	}
	if len(opts.HeadersFromEnv) > 0 {
		req["headers_from_env"] = cloneStringMap(opts.HeadersFromEnv)
	}
	if body != "" {
		req["body"] = body
		req["body_encoding"] = encoding
	}
	if opts.Timeout > 0 {
		req["timeout_ms"] = opts.Timeout.Milliseconds()
	}
	if opts.MaxBodyBytes > 0 {
		req["max_body_bytes"] = opts.MaxBodyBytes
	}
	var response rawHTTPDiagnostic
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "request"), req, &response); err != nil {
		return rawHTTPDiagnostic{}, err
	}
	return response, nil
}

func (c *Client) RequestAppInternalHTTPStream(id string, opts appRequestStreamOptions) ([]model.HTTPStreamProbe, error) {
	body, encoding := encodeRequestBodyForAPI(opts.Body)
	req := map[string]any{
		"method": strings.ToUpper(strings.TrimSpace(opts.Method)),
		"path":   strings.TrimSpace(opts.Path),
	}
	if len(opts.Query) > 0 {
		req["query"] = cloneStringSliceMap(opts.Query)
	}
	if len(opts.Headers) > 0 {
		req["headers"] = cloneStringSliceMap(opts.Headers)
	}
	if len(opts.HeadersFromEnv) > 0 {
		req["headers_from_env"] = cloneStringMap(opts.HeadersFromEnv)
	}
	if body != "" {
		req["body"] = body
		req["body_encoding"] = encoding
	}
	if opts.Timeout > 0 {
		req["timeout_ms"] = opts.Timeout.Milliseconds()
	}
	if len(opts.Accepts) > 0 {
		req["accepts"] = append([]string(nil), opts.Accepts...)
	}
	if opts.MaxChunks > 0 {
		req["max_chunks"] = opts.MaxChunks
	}
	if opts.MaxChunkBytes > 0 {
		req["max_chunk_bytes"] = opts.MaxChunkBytes
	}
	var response struct {
		Probes []model.HTTPStreamProbe `json:"probes"`
	}
	if err := c.doJSON(http.MethodPost, path.Join("/v1/apps", id, "request-stream"), req, &response); err != nil {
		return nil, err
	}
	return response.Probes, nil
}

func encodeRequestBodyForAPI(body []byte) (string, string) {
	if len(body) == 0 {
		return "", "utf-8"
	}
	if utf8.Valid(body) {
		return string(body), "utf-8"
	}
	return base64.StdEncoding.EncodeToString(body), "base64"
}

func cloneStringSliceMap(values map[string][]string) map[string][]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string][]string, len(values))
	for key, entries := range values {
		out[key] = append([]string(nil), entries...)
	}
	return out
}
