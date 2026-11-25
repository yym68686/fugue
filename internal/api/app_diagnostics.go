package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

const (
	appDatabaseQueryDefaultMaxRows    = 100
	appDatabaseQueryDefaultTimeout    = 10 * time.Second
	appRequestDefaultTimeout          = 10 * time.Second
	appRequestDefaultMaxResponseBytes = 256 * 1024
	appDatabaseQueryDefaultDriver     = "pgx"
)

type appDatabaseConnection struct {
	ServiceID string
	DSN       string
	Host      string
	Port      string
	Database  string
	User      string
}

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

type appRequestTiming struct {
	DNS              string `json:"dns,omitempty"`
	Connect          string `json:"connect,omitempty"`
	TLS              string `json:"tls,omitempty"`
	TTFB             string `json:"ttfb,omitempty"`
	Total            string `json:"total"`
	ReusedConnection bool   `json:"reused_connection,omitempty"`
}

type appRequestDiagnostic struct {
	Method       string              `json:"method"`
	URL          string              `json:"url"`
	Status       string              `json:"status"`
	StatusCode   int                 `json:"status_code"`
	Headers      map[string][]string `json:"headers"`
	Body         string              `json:"body"`
	BodyEncoding string              `json:"body_encoding"`
	BodySize     int                 `json:"body_size"`
	Truncated    bool                `json:"truncated,omitempty"`
	ServerTiming string              `json:"server_timing,omitempty"`
	Timing       appRequestTiming    `json:"timing"`
}

func (s *Server) handleQueryAppDatabase(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		SQL       string `json:"sql"`
		MaxRows   int    `json:"max_rows"`
		TimeoutMS int    `json:"timeout_ms"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.SQL = strings.TrimSpace(req.SQL)
	if req.SQL == "" {
		httpx.WriteError(w, http.StatusBadRequest, "sql is required")
		return
	}
	if req.MaxRows <= 0 {
		req.MaxRows = appDatabaseQueryDefaultMaxRows
	}
	if req.MaxRows > 1000 {
		httpx.WriteError(w, http.StatusBadRequest, "max_rows cannot exceed 1000")
		return
	}

	timeout := appDatabaseQueryDefaultTimeout
	if req.TimeoutMS > 0 {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}

	requestID := model.NewID("dbquery")
	w.Header().Set("X-Request-ID", requestID)
	audit := map[string]string{"request_id": requestID, "source_app_id": app.ID, "source_tenant_id": app.TenantID}
	connection, err := authorizedDatabaseConnection(app)
	if err != nil {
		audit["reason"] = "no_authorized_database_binding"
		s.appendAudit(principal, "app.database.query.denied", "app", app.ID, app.TenantID, audit)
		httpx.WriteError(w, http.StatusForbidden, err.Error())
		return
	}

	audit["target_service_id"] = connection.ServiceID
	audit["target_tenant_id"] = app.TenantID
	audit["authorization"] = "managed_service_binding"
	dbOpener := s.openAppDatabase
	if dbOpener == nil {
		dbOpener = openAppDiagnosticDatabase
	}
	db, err := dbOpener(appDatabaseQueryDefaultDriver, connection.DSN)
	if err != nil {
		audit["reason"] = "open_failed"
		s.appendAudit(principal, "app.database.query.failed", "app", app.ID, app.TenantID, audit)
		httpx.WriteError(w, http.StatusServiceUnavailable, "managed database unavailable")
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	startedAt := time.Now()
	response, err := queryAppDatabase(ctx, db, connection, req.SQL, req.MaxRows)
	if err != nil {
		audit["reason"] = "query_failed"
		s.appendAudit(principal, "app.database.query.failed", "app", app.ID, app.TenantID, audit)
		httpx.WriteError(w, appDatabaseQueryErrorStatus(err), err.Error())
		return
	}
	response.DurationMS = time.Since(startedAt).Milliseconds()

	audit["database"] = connection.Database
	audit["host"] = connection.Host
	audit["max_rows"] = strconv.Itoa(req.MaxRows)
	s.appendAudit(principal, "app.database.query", "app", app.ID, app.TenantID, audit)
	httpx.WriteJSON(w, http.StatusOK, response)
}

func appDatabaseQueryErrorStatus(err error) int {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return http.StatusGatewayTimeout
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case "0A000", // feature_not_supported
			"22012", // division_by_zero
			"42601", // syntax_error
			"42P01", // undefined_table
			"42P02", // undefined_parameter
			"42703", // undefined_column
			"42883": // undefined_function
			return http.StatusBadRequest
		case "57014": // query_canceled (statement timeout/cancellation)
			return http.StatusGatewayTimeout
		}
	}
	return http.StatusServiceUnavailable
}

func openAppDiagnosticDatabase(driver, dsn string) (*sql.DB, error) {
	if driver != appDatabaseQueryDefaultDriver {
		return nil, fmt.Errorf("unsupported app database diagnostic driver %q", driver)
	}
	// Application URLs may configure pgxpool. Parse with its grammar before
	// opening the single diagnostic connection; pool-only keys must never be
	// sent to PostgreSQL as startup parameters. TLS and server parameters remain.
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return stdlib.OpenDB(*config.ConnConfig), nil
}

func (s *Server) handleRequestAppInternalHTTP(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		Method         string              `json:"method"`
		Path           string              `json:"path"`
		Query          map[string][]string `json:"query"`
		Headers        map[string][]string `json:"headers"`
		HeadersFromEnv map[string]string   `json:"headers_from_env"`
		Body           string              `json:"body"`
		BodyEncoding   string              `json:"body_encoding"`
		TimeoutMS      int                 `json:"timeout_ms"`
		MaxBodyBytes   int                 `json:"max_body_bytes"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Method) == "" {
		req.Method = http.MethodGet
	}
	if strings.TrimSpace(req.Path) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "path is required")
		return
	}
	if req.MaxBodyBytes <= 0 {
		req.MaxBodyBytes = appRequestDefaultMaxResponseBytes
	}

	spec := cloneAppSpec(app.Spec)
	if appDeployBaselineNeedsRecovery(spec, app.Source) {
		recoveredSpec, _, err := s.recoverAppDeployBaseline(app)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		spec = recoveredSpec
	}
	app.Spec = spec
	envDetails := mergedAppEnvDetails(app, spec)

	targetURL, err := s.resolveAppInternalRequestURL(r.Context(), app, req.Path, req.Query)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	bodyBytes, err := decodeAppRequestBody(req.Body, req.BodyEncoding)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	timeout := appRequestDefaultTimeout
	if req.TimeoutMS > 0 {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, strings.ToUpper(strings.TrimSpace(req.Method)), targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, fmt.Sprintf("build app request: %v", err))
		return
	}
	if len(bodyBytes) == 0 {
		httpReq.Body = nil
	}
	if err := applyAppRequestHeaders(httpReq.Header, req.Headers, req.HeadersFromEnv, envDetails.Env); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(httpReq.Header.Get("Accept")) == "" {
		httpReq.Header.Set("Accept", "*/*")
	}

	httpClient := s.appRequestHTTPClient
	if httpClient == nil {
		httpClient = newAppDiagnosticHTTPClient()
	}
	response, err := doObservedHTTPRequest(httpClient, httpReq, req.MaxBodyBytes)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	s.appendAudit(principal, "app.request", "app", app.ID, app.TenantID, map[string]string{
		"method": httpReq.Method,
		"path":   req.Path,
	})
	httpx.WriteJSON(w, http.StatusOK, response)
}

func queryAppDatabase(ctx context.Context, db *sql.DB, connection appDatabaseConnection, statement string, maxRows int) (appDatabaseQueryResponse, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return appDatabaseQueryResponse{}, fmt.Errorf("begin read-only transaction: %w", err)
	}
	rows, err := tx.QueryContext(ctx, statement)
	if err != nil {
		_ = tx.Rollback()
		return appDatabaseQueryResponse{}, fmt.Errorf("query database: %w", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		_ = tx.Rollback()
		return appDatabaseQueryResponse{}, fmt.Errorf("read result columns: %w", err)
	}
	columns := make([]appDatabaseQueryColumn, 0, len(columnTypes))
	columnNames := make([]string, 0, len(columnTypes))
	for _, columnType := range columnTypes {
		columnNames = append(columnNames, columnType.Name())
		columns = append(columns, appDatabaseQueryColumn{
			Name:         columnType.Name(),
			DatabaseType: columnType.DatabaseTypeName(),
		})
	}

	results := make([]map[string]any, 0)
	truncated := false
	for rows.Next() {
		if len(results) >= maxRows {
			truncated = true
			break
		}
		values := make([]any, len(columnNames))
		scanTargets := make([]any, len(columnNames))
		for index := range values {
			scanTargets[index] = &values[index]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			_ = tx.Rollback()
			return appDatabaseQueryResponse{}, fmt.Errorf("scan row: %w", err)
		}
		row := make(map[string]any, len(columnNames))
		for index, columnName := range columnNames {
			row[columnName] = normalizeDatabaseValue(values[index])
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return appDatabaseQueryResponse{}, fmt.Errorf("read rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return appDatabaseQueryResponse{}, fmt.Errorf("commit read-only transaction: %w", err)
	}

	return appDatabaseQueryResponse{
		Database:  connection.Database,
		Host:      connection.Host,
		User:      connection.User,
		Columns:   columns,
		Rows:      results,
		RowCount:  len(results),
		MaxRows:   maxRows,
		Truncated: truncated,
		ReadOnly:  true,
	}, nil
}

var postgresServiceLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// Resolve only from the platform's recorded binding. App and binding environment
// overrides are for the workload and must never configure a control-plane connection.
func authorizedDatabaseConnection(app model.App) (appDatabaseConnection, error) {
	bound, ok := firstManagedPostgresBinding(app)
	service := bound.Service
	if !ok || app.TenantID == "" || service.TenantID != app.TenantID || bound.Binding.TenantID != app.TenantID ||
		bound.Binding.AppID != app.ID || bound.Binding.ServiceID != service.ID || service.ID == "" || service.Spec.Postgres == nil {
		return appDatabaseConnection{}, errors.New("an authorized managed database binding is required")
	}
	pg := *service.Spec.Postgres
	name := model.PostgresEndpointName(pg)
	if !postgresServiceLabel.MatchString(name) || pg.Database == "" || pg.User == "" || pg.Password == "" || pg.Suspended {
		return appDatabaseConnection{}, errors.New("managed database connection unavailable")
	}
	host := name + "." + runtime.NamespaceForTenant(service.TenantID) + ".svc.cluster.local"
	u := url.URL{Scheme: "postgresql", Host: net.JoinHostPort(host, "5432"), Path: "/" + pg.Database, User: url.UserPassword(pg.User, pg.Password)}
	return appDatabaseConnection{ServiceID: service.ID, DSN: u.String(), Host: host, Port: "5432", Database: pg.Database, User: pg.User}, nil
}

func normalizeDatabaseValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []byte:
		if utf8.Valid(typed) {
			return string(typed)
		}
		return base64.StdEncoding.EncodeToString(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return typed
	}
}

func (s *Server) resolveAppInternalRequestURL(ctx context.Context, app model.App, requestPath string, query map[string][]string) (string, error) {
	internalService := buildAppInternalService(app)
	if internalService == nil || strings.TrimSpace(internalService.Host) == "" || internalService.Port <= 0 {
		return "", fmt.Errorf("app does not expose an internal HTTP service")
	}

	relative, err := url.Parse(strings.TrimSpace(requestPath))
	if err != nil {
		return "", fmt.Errorf("parse request path: %w", err)
	}
	if relative.IsAbs() || strings.TrimSpace(relative.Host) != "" {
		return "", fmt.Errorf("path must be relative to the app internal service")
	}

	base := s.serviceURLForApp(ctx, app)
	target, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("resolve app service URL: %w", err)
	}
	target.Path = ensureLeadingSlash(relative.Path)
	target.RawPath = target.Path

	values := target.Query()
	for key, existing := range relative.Query() {
		for _, value := range existing {
			values.Add(key, value)
		}
	}
	for key, existing := range query {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		for _, value := range existing {
			values.Add(key, value)
		}
	}
	target.RawQuery = values.Encode()
	return target.String(), nil
}

func applyAppRequestHeaders(headers http.Header, explicit map[string][]string, fromEnv map[string]string, env map[string]string) error {
	for key, values := range explicit {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		for _, value := range values {
			headers.Add(key, value)
		}
	}
	for headerName, envKey := range fromEnv {
		headerName = strings.TrimSpace(headerName)
		envKey = strings.TrimSpace(envKey)
		if headerName == "" || envKey == "" {
			continue
		}
		value, ok := env[envKey]
		if !ok {
			return fmt.Errorf("app env %q is not set", envKey)
		}
		headers.Add(headerName, value)
	}
	return nil
}

func decodeAppRequestBody(body, encoding string) ([]byte, error) {
	encoding = strings.TrimSpace(strings.ToLower(encoding))
	switch encoding {
	case "", "utf-8", "utf8", "text":
		return []byte(body), nil
	case "base64":
		data, err := base64.StdEncoding.DecodeString(strings.TrimSpace(body))
		if err != nil {
			return nil, fmt.Errorf("decode base64 body: %w", err)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("unsupported body_encoding %q", encoding)
	}
}

type observedHTTPMetrics struct {
	DNS              time.Duration
	Connect          time.Duration
	TLS              time.Duration
	TTFB             time.Duration
	Total            time.Duration
	ReusedConnection bool
}

func doObservedHTTPRequest(client *http.Client, httpReq *http.Request, maxBodyBytes int) (appRequestDiagnostic, error) {
	if client == nil {
		return appRequestDiagnostic{}, fmt.Errorf("http client is not configured")
	}

	startedAt := time.Now()
	metrics := observedHTTPMetrics{}
	var (
		dnsStartedAt     time.Time
		connectStartedAt time.Time
		tlsStartedAt     time.Time
		firstByteAt      time.Time
	)

	trace := &httptrace.ClientTrace{
		DNSStart: func(_ httptrace.DNSStartInfo) {
			dnsStartedAt = time.Now()
		},
		DNSDone: func(_ httptrace.DNSDoneInfo) {
			if !dnsStartedAt.IsZero() && metrics.DNS == 0 {
				metrics.DNS = time.Since(dnsStartedAt)
			}
		},
		ConnectStart: func(_, _ string) {
			if connectStartedAt.IsZero() {
				connectStartedAt = time.Now()
			}
		},
		ConnectDone: func(_, _ string, _ error) {
			if !connectStartedAt.IsZero() && metrics.Connect == 0 {
				metrics.Connect = time.Since(connectStartedAt)
			}
		},
		GotConn: func(info httptrace.GotConnInfo) {
			metrics.ReusedConnection = info.Reused
		},
		TLSHandshakeStart: func() {
			tlsStartedAt = time.Now()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			if !tlsStartedAt.IsZero() && metrics.TLS == 0 {
				metrics.TLS = time.Since(tlsStartedAt)
			}
		},
		GotFirstResponseByte: func() {
			if firstByteAt.IsZero() {
				firstByteAt = time.Now()
			}
		},
	}

	ctx := httpReq.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	httpReq = httpReq.Clone(httptrace.WithClientTrace(ctx, trace))

	resp, err := client.Do(httpReq)
	if err != nil {
		return appRequestDiagnostic{}, fmt.Errorf("perform request: %w", err)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return appRequestDiagnostic{}, fmt.Errorf("read response body: %w", err)
	}
	metrics.Total = time.Since(startedAt)
	if !firstByteAt.IsZero() {
		metrics.TTFB = firstByteAt.Sub(startedAt)
	}
	bodyEncoding, bodyPreview, truncated := encodeDiagnosticPayload(payload, maxBodyBytes)
	return appRequestDiagnostic{
		Method:       httpReq.Method,
		URL:          httpReq.URL.String(),
		Status:       resp.Status,
		StatusCode:   resp.StatusCode,
		Headers:      cloneHeaderValues(resp.Header),
		Body:         bodyPreview,
		BodyEncoding: bodyEncoding,
		BodySize:     len(payload),
		Truncated:    truncated,
		ServerTiming: strings.TrimSpace(resp.Header.Get("Server-Timing")),
		Timing:       toAppRequestTiming(metrics),
	}, nil
}

func encodeDiagnosticPayload(payload []byte, maxBytes int) (string, string, bool) {
	if maxBytes <= 0 {
		maxBytes = len(payload)
	}
	if len(payload) == 0 {
		return "utf-8", "", false
	}
	truncated := len(payload) > maxBytes
	visible := payload
	if truncated {
		visible = payload[:maxBytes]
	}
	if utf8.Valid(visible) {
		return "utf-8", string(visible), truncated
	}
	return "base64", base64.StdEncoding.EncodeToString(visible), truncated
}

func cloneHeaderValues(headers http.Header) map[string][]string {
	if headers == nil {
		return map[string][]string{}
	}
	out := make(map[string][]string, len(headers))
	for key, values := range headers {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func toAppRequestTiming(metrics observedHTTPMetrics) appRequestTiming {
	return appRequestTiming{
		DNS:              formatDuration(metrics.DNS),
		Connect:          formatDuration(metrics.Connect),
		TLS:              formatDuration(metrics.TLS),
		TTFB:             formatDuration(metrics.TTFB),
		Total:            formatDuration(metrics.Total),
		ReusedConnection: metrics.ReusedConnection,
	}
}

func formatDuration(value time.Duration) string {
	if value <= 0 {
		return ""
	}
	return value.String()
}

func ensureLeadingSlash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "/"
	}
	if strings.HasPrefix(value, "/") {
		return value
	}
	return "/" + value
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
