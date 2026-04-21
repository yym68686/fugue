package cli

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appRequestCompareOptions struct {
	appRequestCommandOptions
	RequireEnv []string
	Insecure   bool
}

type appRequestCompareResult struct {
	Summary               string                     `json:"summary"`
	Category              string                     `json:"category"`
	Evidence              []string                   `json:"evidence,omitempty"`
	RelatedObjects        []appRequestRelatedObject  `json:"related_objects,omitempty"`
	NextActions           []string                   `json:"next_actions,omitempty"`
	Request               appRequestPlan             `json:"request"`
	PublicRouteConfigured bool                       `json:"public_route_configured"`
	EnvRequirements       []appRequestEnvRequirement `json:"env_requirements,omitempty"`
	Public                appHTTPProbe               `json:"public"`
	Internal              appHTTPProbe               `json:"internal"`
}

type appRequestPlan struct {
	Method           string                      `json:"method"`
	Path             string                      `json:"path"`
	Query            map[string][]string         `json:"query,omitempty"`
	HeaderNames      []string                    `json:"header_names,omitempty"`
	HeaderInjections []appRequestHeaderInjection `json:"header_injections,omitempty"`
	BodyBytes        int                         `json:"body_bytes,omitempty"`
	ContentType      string                      `json:"content_type,omitempty"`
	Timeout          string                      `json:"timeout,omitempty"`
}

type appRequestHeaderInjection struct {
	Header  string `json:"header"`
	EnvKey  string `json:"env_key,omitempty"`
	Present bool   `json:"present"`
}

type appRequestEnvRequirement struct {
	Key       string   `json:"key"`
	Present   bool     `json:"present"`
	Source    string   `json:"source,omitempty"`
	SourceRef string   `json:"source_ref,omitempty"`
	UsedBy    []string `json:"used_by,omitempty"`
}

type appRequestRelatedObject struct {
	Kind string `json:"kind"`
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
	Ref  string `json:"ref,omitempty"`
}

type appHTTPProbe struct {
	Target string `json:"target"`
	Error  string `json:"error,omitempty"`
	rawHTTPDiagnostic
}

func (c *CLI) newAppRequestCompareCommand() *cobra.Command {
	opts := appRequestCompareOptions{
		appRequestCommandOptions: appRequestCommandOptions{
			rawRequestOptions: rawRequestOptions{MaxBodyBytes: defaultDiagnosticBodyLimit},
			Timeout:           10 * time.Second,
		},
	}
	cmd := &cobra.Command{
		Use:     "compare <app> [method] <path>",
		Aliases: []string{"diagnose", "diff"},
		Short:   "Compare the public route response with the app internal service response",
		Args:    cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			appRef, method, requestPath, err := parseAppRequestArgs(args)
			if err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, appRef)
			if err != nil {
				return err
			}
			fullApp, err := client.GetApp(app.ID)
			if err != nil {
				return err
			}
			result, err := c.compareAppRequest(client, fullApp, method, requestPath, opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, result)
			}
			return renderAppRequestCompareResult(c.stdout, result)
		},
	}
	cmd.Flags().StringVar(&opts.Body, "body", "", "Inline request body")
	cmd.Flags().StringVar(&opts.BodyFile, "body-file", "", "Read request body from a local path or '-' for stdin")
	cmd.Flags().StringVar(&opts.ContentType, "content-type", "", "Override request Content-Type")
	cmd.Flags().StringArrayVar(&opts.Headers, "header", nil, "Request header in 'Key: value' form (repeatable)")
	cmd.Flags().StringArrayVar(&opts.HeaderFromEnv, "header-from-env", nil, "Header to fill from effective app env in 'Header=ENV_KEY' form")
	cmd.Flags().StringArrayVar(&opts.Query, "query", nil, "Query parameter in 'key=value' form (repeatable)")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Request timeout")
	cmd.Flags().IntVar(&opts.MaxBodyBytes, "max-body-bytes", opts.MaxBodyBytes, "Maximum response bytes to print")
	cmd.Flags().StringArrayVar(&opts.RequireEnv, "require-env", nil, "Effective app env key that must be present before the probe runs (repeatable)")
	cmd.Flags().BoolVar(&opts.Insecure, "insecure", false, "Skip TLS certificate verification on the public-route probe")
	return cmd
}

func (c *CLI) compareAppRequest(client *Client, app model.App, method, requestPath string, opts appRequestCompareOptions) (appRequestCompareResult, error) {
	body, err := loadRawRequestBody(opts.rawRequestOptions)
	if err != nil {
		return appRequestCompareResult{}, err
	}
	headers, err := parseRawHeaders(opts.Headers)
	if err != nil {
		return appRequestCompareResult{}, err
	}
	headerFromEnv, err := parseSimpleStringAssignments(opts.HeaderFromEnv)
	if err != nil {
		return appRequestCompareResult{}, err
	}
	requiredEnv, err := normalizeEnvKeys(opts.RequireEnv)
	if err != nil {
		return appRequestCompareResult{}, err
	}

	contentType := strings.TrimSpace(opts.ContentType)
	if contentType == "" && len(body) > 0 {
		switch {
		case json.Valid(body):
			contentType = "application/json"
		default:
			contentType = "text/plain; charset=utf-8"
		}
	}

	requestHeaders := cloneHTTPHeaders(headers)
	if contentType != "" && len(body) > 0 && strings.TrimSpace(requestHeaders.Get("Content-Type")) == "" {
		requestHeaders.Set("Content-Type", contentType)
	}

	query := parseRepeatedStringAssignments(opts.Query)
	result := appRequestCompareResult{
		Request: appRequestPlan{
			Method:           strings.ToUpper(strings.TrimSpace(method)),
			Path:             strings.TrimSpace(requestPath),
			Query:            cloneStringSliceMap(query),
			HeaderNames:      collectAppRequestHeaderNames(requestHeaders, headerFromEnv),
			HeaderInjections: buildAppRequestHeaderInjections(headerFromEnv),
			BodyBytes:        len(body),
			ContentType:      contentType,
			Timeout:          opts.Timeout.String(),
		},
		PublicRouteConfigured: strings.TrimSpace(appPublicBaseURL(app)) != "",
		Public: appHTTPProbe{
			Target: "public_route",
		},
		Internal: appHTTPProbe{
			Target: "internal_service",
		},
	}

	var (
		envValues        map[string]string
		runtimeDiagnosis *appDiagnosis
	)
	if len(headerFromEnv) > 0 || len(requiredEnv) > 0 {
		envResponse, err := client.GetAppEnv(app.ID)
		if err != nil {
			return appRequestCompareResult{}, err
		}
		envValues = cloneStringMap(envResponse.Env)
		result.EnvRequirements = buildAppRequestEnvRequirements(envResponse.Entries, headerFromEnv, requiredEnv)
		result.Request.HeaderInjections = annotateAppRequestHeaderInjections(result.Request.HeaderInjections, result.EnvRequirements)
		if hasMissingAppRequestEnvRequirement(result.EnvRequirements) {
			result.Internal.Method = result.Request.Method
			if internalURL, urlErr := buildAppInternalRequestURL(app, requestPath, query); urlErr == nil {
				result.Internal.URL = internalURL
			}
			result.Internal.Error = "probe skipped because required app env is missing"
			result.Public.Method = result.Request.Method
			if publicURL, urlErr := buildAppPublicRequestURL(app, requestPath, query); urlErr == nil {
				result.Public.URL = publicURL
			}
			result.Public.Error = "probe skipped because required app env is missing"
			finalizeAppRequestCompareResult(&result, app, nil)
			return result, nil
		}
	}

	internalProbe, diagnosis := c.probeInternalAppHTTP(client, app, method, requestPath, query, requestHeaders, headerFromEnv, body, opts)
	result.Internal = internalProbe
	runtimeDiagnosis = diagnosis

	publicProbe := c.probePublicAppHTTP(app, method, requestPath, query, requestHeaders, headerFromEnv, envValues, body, opts)
	result.Public = publicProbe

	finalizeAppRequestCompareResult(&result, app, runtimeDiagnosis)
	return result, nil
}

func (c *CLI) probeInternalAppHTTP(client *Client, app model.App, method, requestPath string, query map[string][]string, headers http.Header, headerFromEnv map[string]string, body []byte, opts appRequestCompareOptions) (appHTTPProbe, *appDiagnosis) {
	probe := appHTTPProbe{
		Target: "internal_service",
		rawHTTPDiagnostic: rawHTTPDiagnostic{
			Method: strings.ToUpper(strings.TrimSpace(method)),
		},
	}
	if internalURL, err := buildAppInternalRequestURL(app, requestPath, query); err == nil {
		probe.URL = internalURL
	}

	response, err := client.RequestAppInternalHTTP(app.ID, appRequestOptions{
		Method:         method,
		Path:           requestPath,
		Query:          query,
		Headers:        headersToMap(headers),
		HeadersFromEnv: cloneStringMap(headerFromEnv),
		Body:           body,
		Timeout:        opts.Timeout,
		MaxBodyBytes:   opts.MaxBodyBytes,
	})
	if err == nil {
		probe.rawHTTPDiagnostic = response
		return probe, nil
	}
	probe.Error = strings.TrimSpace(err.Error())

	diagnosis, diagnosisErr := client.TryGetAppDiagnosis(app.ID, "app")
	if diagnosisErr == nil {
		return probe, diagnosis
	}
	return probe, nil
}

func (c *CLI) probePublicAppHTTP(app model.App, method, requestPath string, query map[string][]string, headers http.Header, headerFromEnv map[string]string, envValues map[string]string, body []byte, opts appRequestCompareOptions) appHTTPProbe {
	probe := appHTTPProbe{
		Target: "public_route",
		rawHTTPDiagnostic: rawHTTPDiagnostic{
			Method: strings.ToUpper(strings.TrimSpace(method)),
		},
	}
	publicURL, err := buildAppPublicRequestURL(app, requestPath, query)
	if err != nil {
		probe.Error = err.Error()
		return probe
	}
	probe.URL = publicURL

	httpClient := &http.Client{
		Timeout: opts.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if opts.Insecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	httpClient.Transport = transport

	var requestBody io.Reader
	if body != nil {
		requestBody = bytes.NewReader(body)
	}
	httpReq, err := http.NewRequest(strings.ToUpper(strings.TrimSpace(method)), publicURL, requestBody)
	if err != nil {
		probe.Error = fmt.Sprintf("build request: %v", err)
		return probe
	}
	for key, values := range headers {
		for _, value := range values {
			httpReq.Header.Add(key, value)
		}
	}
	for headerName, envKey := range headerFromEnv {
		value, ok := envValues[strings.TrimSpace(envKey)]
		if !ok {
			probe.Error = fmt.Sprintf("app env %q is not set", envKey)
			return probe
		}
		httpReq.Header.Add(strings.TrimSpace(headerName), value)
	}
	if strings.TrimSpace(httpReq.Header.Get("Accept")) == "" {
		httpReq.Header.Set("Accept", "*/*")
	}

	prepared, err := (&Client{
		httpClient: httpClient,
		observer:   c.observer,
	}).doPrepared(httpReq)
	if err != nil {
		probe.Error = err.Error()
		return probe
	}
	encoding, renderedBody, truncated := encodeBodyPreview(prepared.Payload, opts.MaxBodyBytes)
	probe.rawHTTPDiagnostic = rawHTTPDiagnostic{
		Method:       prepared.Method,
		URL:          prepared.URL,
		Status:       prepared.Status,
		StatusCode:   prepared.StatusCode,
		Headers:      headersToMap(prepared.Headers),
		Body:         renderedBody,
		BodyEncoding: encoding,
		BodySize:     len(prepared.Payload),
		Truncated:    truncated,
		ServerTiming: strings.TrimSpace(prepared.Headers.Get("Server-Timing")),
		Timing:       toTimingView(prepared.Timing),
	}
	return probe
}

func renderAppRequestCompareResult(w io.Writer, result appRequestCompareResult) error {
	requestQuery := encodeAppRequestQuery(result.Request.Query)
	pairs := []kvPair{
		{Key: "category", Value: strings.TrimSpace(result.Category)},
		{Key: "summary", Value: strings.TrimSpace(result.Summary)},
		{Key: "public_route_configured", Value: fmt.Sprintf("%t", result.PublicRouteConfigured)},
		{Key: "request_method", Value: strings.TrimSpace(result.Request.Method)},
		{Key: "request_path", Value: strings.TrimSpace(result.Request.Path)},
		{Key: "request_query", Value: requestQuery},
		{Key: "request_body_bytes", Value: formatInt(result.Request.BodyBytes)},
		{Key: "request_content_type", Value: strings.TrimSpace(result.Request.ContentType)},
		{Key: "request_timeout", Value: strings.TrimSpace(result.Request.Timeout)},
		{Key: "request_headers", Value: strings.Join(result.Request.HeaderNames, ", ")},
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	for _, injection := range result.Request.HeaderInjections {
		if _, err := fmt.Fprintf(w, "request_header_injection=%s\n", formatAppRequestHeaderInjection(injection)); err != nil {
			return err
		}
	}
	for _, requirement := range result.EnvRequirements {
		if _, err := fmt.Fprintf(w, "env_requirement=%s\n", formatAppRequestEnvRequirement(requirement)); err != nil {
			return err
		}
	}
	for _, relatedObject := range result.RelatedObjects {
		if _, err := fmt.Fprintf(w, "related_object=%s\n", formatAppRequestRelatedObject(relatedObject)); err != nil {
			return err
		}
	}
	for _, evidence := range result.Evidence {
		if _, err := fmt.Fprintf(w, "evidence=%s\n", evidence); err != nil {
			return err
		}
	}
	for _, nextAction := range result.NextActions {
		if _, err := fmt.Fprintf(w, "next_action=%s\n", nextAction); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "public_route"); err != nil {
		return err
	}
	if err := renderAppHTTPProbe(w, result.Public); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "internal_service"); err != nil {
		return err
	}
	return renderAppHTTPProbe(w, result.Internal)
}

func renderAppHTTPProbe(w io.Writer, probe appHTTPProbe) error {
	pairs := []kvPair{
		{Key: "target", Value: strings.TrimSpace(probe.Target)},
		{Key: "method", Value: strings.TrimSpace(probe.Method)},
		{Key: "url", Value: strings.TrimSpace(probe.URL)},
		{Key: "status", Value: strings.TrimSpace(probe.Status)},
		{Key: "error", Value: strings.TrimSpace(probe.Error)},
		{Key: "body_bytes", Value: formatInt(probe.BodySize)},
		{Key: "body_encoding", Value: strings.TrimSpace(probe.BodyEncoding)},
		{Key: "dns", Value: strings.TrimSpace(probe.Timing.DNS)},
		{Key: "connect", Value: strings.TrimSpace(probe.Timing.Connect)},
		{Key: "tls", Value: strings.TrimSpace(probe.Timing.TLS)},
		{Key: "ttfb", Value: strings.TrimSpace(probe.Timing.TTFB)},
		{Key: "total", Value: strings.TrimSpace(probe.Timing.Total)},
	}
	if probe.Timing.ReusedConnection {
		pairs = append(pairs, kvPair{Key: "reused_connection", Value: "true"})
	}
	if strings.TrimSpace(probe.ServerTiming) != "" {
		pairs = append(pairs, kvPair{Key: "server_timing", Value: strings.TrimSpace(probe.ServerTiming)})
	}
	if probe.Truncated {
		pairs = append(pairs, kvPair{Key: "body_truncated", Value: "true"})
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	if len(probe.Headers) == 0 && strings.TrimSpace(probe.Body) == "" {
		return nil
	}
	if len(probe.Headers) > 0 {
		if _, err := fmt.Fprintln(w, "\nheaders:"); err != nil {
			return err
		}
		if err := writeHeaderBlock(w, probe.Headers); err != nil {
			return err
		}
	}
	if strings.TrimSpace(probe.Body) == "" {
		return nil
	}
	if _, err := fmt.Fprintln(w, "\nbody:"); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w, strings.TrimRight(probe.Body, "\n"))
	return err
}

func finalizeAppRequestCompareResult(result *appRequestCompareResult, app model.App, runtimeDiagnosis *appDiagnosis) {
	if result == nil {
		return
	}
	result.RelatedObjects = buildAppRequestRelatedObjects(app)
	result.Evidence = buildAppRequestCompareEvidence(*result, app, runtimeDiagnosis)

	switch {
	case hasMissingAppRequestEnvRequirement(result.EnvRequirements):
		result.Category = "missing-env-prerequisite"
		result.Summary = fmt.Sprintf("the probe depends on missing app env %s, so Fugue skipped the request until the prerequisite is fixed", formatMissingAppRequestEnvKeys(result.EnvRequirements))
	case !result.PublicRouteConfigured:
		result.Category = "public-route-missing"
		result.Summary = "the app does not have a public route configured, so only the internal service can answer this path"
	case strings.TrimSpace(result.Internal.Error) != "":
		switch {
		case isGatewayStatus(result.Public.StatusCode) || strings.TrimSpace(result.Public.Error) != "":
			result.Category = "app-backend-unavailable"
			result.Summary = fmt.Sprintf("the internal service probe failed and the public route also did not return a healthy upstream response (%s)", displayAppHTTPProbeStatus(result.Public))
		case isSuccessfulStatus(result.Public.StatusCode):
			result.Category = "internal-probe-failed-public-ok"
			result.Summary = fmt.Sprintf("the public route returned %s, but the direct internal service probe failed before a response was returned", displayAppHTTPProbeStatus(result.Public))
		default:
			result.Category = "internal-probe-failed"
			result.Summary = "the direct internal service probe failed before a response was returned"
		}
	case strings.TrimSpace(result.Public.Error) != "":
		switch {
		case result.Internal.StatusCode > 0:
			result.Category = "public-route-unreachable"
			result.Summary = fmt.Sprintf("the public route probe failed before a response was returned, but the internal service returned %s", displayAppHTTPProbeStatus(result.Internal))
		default:
			result.Category = "request-probe-failed"
			result.Summary = "both the public-route and internal-service probes failed before any response was returned"
		}
	case isAuthStatus(result.Public.StatusCode) && !isAuthStatus(result.Internal.StatusCode):
		result.Category = "public-route-auth-failed"
		result.Summary = fmt.Sprintf("the public route returned %s while the internal service returned %s; auth is failing at the public edge or route middleware", displayAppHTTPProbeStatus(result.Public), displayAppHTTPProbeStatus(result.Internal))
	case isAuthStatus(result.Public.StatusCode) && isAuthStatus(result.Internal.StatusCode):
		result.Category = "auth-failed"
		result.Summary = fmt.Sprintf("both the public route and internal service rejected the request (%s vs %s)", displayAppHTTPProbeStatus(result.Public), displayAppHTTPProbeStatus(result.Internal))
	case result.Public.StatusCode == result.Internal.StatusCode:
		switch {
		case isSuccessfulStatus(result.Public.StatusCode):
			result.Category = "request-ok"
			result.Summary = fmt.Sprintf("the public route and internal service both returned %s for this path", displayAppHTTPProbeStatus(result.Public))
		case result.Public.StatusCode == http.StatusNotFound:
			result.Category = "app-route-missing"
			result.Summary = "the public route and internal service both returned 404, so the app itself does not expose this path"
		case isAuthStatus(result.Public.StatusCode):
			result.Category = "auth-failed"
			result.Summary = fmt.Sprintf("the public route and internal service both rejected the request with %s", displayAppHTTPProbeStatus(result.Public))
		default:
			result.Category = "app-returned-error"
			result.Summary = fmt.Sprintf("the public route and internal service both returned %s, so the failure is inside the app rather than the ingress layer", displayAppHTTPProbeStatus(result.Public))
		}
	case looksStaticFallback(result.Public, result.Internal, app):
		result.Category = "static-fallback"
		result.Summary = fmt.Sprintf("the public route returned %s with an HTML fallback while the internal service returned %s; a static-site rule likely consumed this path before it reached the app", displayAppHTTPProbeStatus(result.Public), displayAppHTTPProbeStatus(result.Internal))
	case isSuccessfulStatus(result.Internal.StatusCode):
		switch {
		case isGatewayStatus(result.Public.StatusCode):
			result.Category = "public-proxy-failure"
			result.Summary = fmt.Sprintf("the internal service returned %s, but the public route returned %s; the proxy layer is failing before the request reaches the app", displayAppHTTPProbeStatus(result.Internal), displayAppHTTPProbeStatus(result.Public))
		case result.Public.StatusCode == http.StatusNotFound:
			result.Category = "public-route-not-forwarding"
			result.Summary = fmt.Sprintf("the public route returned 404 while the internal service returned %s; the public ingress is not forwarding this path to the app", displayAppHTTPProbeStatus(result.Internal))
		default:
			result.Category = "public-internal-mismatch"
			result.Summary = fmt.Sprintf("the public route returned %s while the internal service returned %s; compare edge routing, auth middleware, and path handling", displayAppHTTPProbeStatus(result.Public), displayAppHTTPProbeStatus(result.Internal))
		}
	case isServerError(result.Internal.StatusCode) && isServerError(result.Public.StatusCode):
		result.Category = "app-returned-error"
		result.Summary = fmt.Sprintf("the public route returned %s and the internal service returned %s; the app backend is returning errors on both paths", displayAppHTTPProbeStatus(result.Public), displayAppHTTPProbeStatus(result.Internal))
	default:
		result.Category = "public-internal-mismatch"
		result.Summary = fmt.Sprintf("the public route returned %s while the internal service returned %s", displayAppHTTPProbeStatus(result.Public), displayAppHTTPProbeStatus(result.Internal))
	}

	result.NextActions = buildAppRequestNextActions(app, result.Category, result.EnvRequirements)
}

func buildAppRequestCompareEvidence(result appRequestCompareResult, app model.App, runtimeDiagnosis *appDiagnosis) []string {
	evidence := make([]string, 0, 8)
	for _, requirement := range result.EnvRequirements {
		if !requirement.Present {
			evidence = appendUniqueString(evidence, fmt.Sprintf("required env %s is missing", requirement.Key))
		}
	}
	evidence = appendUniqueString(evidence, describeAppHTTPProbe(result.Public, "public route"))
	evidence = appendUniqueString(evidence, describeAppHTTPProbe(result.Internal, "internal service"))

	publicContentType := firstHeaderValue(result.Public.Headers, "Content-Type")
	internalContentType := firstHeaderValue(result.Internal.Headers, "Content-Type")
	if strings.TrimSpace(publicContentType) != "" {
		evidence = appendUniqueString(evidence, "public response content-type "+publicContentType)
	}
	if strings.TrimSpace(internalContentType) != "" && !strings.EqualFold(strings.TrimSpace(internalContentType), strings.TrimSpace(publicContentType)) {
		evidence = appendUniqueString(evidence, "internal response content-type "+internalContentType)
	}

	if strategy := appRequestBuildStrategy(app); strategy != "" && strings.EqualFold(strategy, model.AppBuildStrategyStaticSite) {
		evidence = appendUniqueString(evidence, "app build strategy is static-site")
	}
	if runtimeDiagnosis != nil && !strings.EqualFold(strings.TrimSpace(runtimeDiagnosis.Category), "available") {
		evidence = appendUniqueString(evidence, "runtime diagnosis: "+strings.TrimSpace(runtimeDiagnosis.Summary))
	}
	return evidence
}

func buildAppRequestRelatedObjects(app model.App) []appRequestRelatedObject {
	objects := []appRequestRelatedObject{
		{Kind: "app", ID: strings.TrimSpace(app.ID), Name: strings.TrimSpace(app.Name)},
	}
	if value := strings.TrimSpace(appPublicBaseURL(app)); value != "" {
		objects = append(objects, appRequestRelatedObject{Kind: "route", Ref: value})
	}
	if app.InternalService != nil {
		objects = append(objects, appRequestRelatedObject{
			Kind: "internal_service",
			Name: strings.TrimSpace(app.InternalService.Name),
			Ref:  fmt.Sprintf("%s:%d", strings.TrimSpace(app.InternalService.Host), app.InternalService.Port),
		})
	}
	if runtimeID := firstNonEmptyTrimmed(strings.TrimSpace(app.Status.CurrentRuntimeID), strings.TrimSpace(app.Spec.RuntimeID)); runtimeID != "" {
		objects = append(objects, appRequestRelatedObject{Kind: "runtime", ID: runtimeID})
	}
	if operationID := strings.TrimSpace(app.Status.LastOperationID); operationID != "" {
		objects = append(objects, appRequestRelatedObject{Kind: "operation", ID: operationID})
	}
	if source := model.AppBuildSource(app); source != nil {
		objects = append(objects, appRequestRelatedObject{
			Kind: strings.TrimSpace(firstNonEmptyTrimmed(source.Type, "source")),
			Ref:  strings.TrimSpace(sourceRef(source)),
		})
	}
	return objects
}

func buildAppRequestNextActions(app model.App, category string, requirements []appRequestEnvRequirement) []string {
	appName := strings.TrimSpace(app.Name)
	switch strings.TrimSpace(category) {
	case "missing-env-prerequisite":
		missing := formatMissingAppRequestEnvKeys(requirements)
		return []string{
			fmt.Sprintf("fugue app env ls %s", appName),
			fmt.Sprintf("fugue app env set %s %s=<value>", appName, firstMissingAppRequestEnvKey(requirements)),
			fmt.Sprintf("re-run the same compare probe after fixing %s", missing),
		}
	case "public-route-missing", "public-route-not-forwarding", "public-proxy-failure", "static-fallback", "public-route-unreachable", "public-route-auth-failed", "public-internal-mismatch":
		return []string{
			fmt.Sprintf("fugue app route show %s", appName),
			fmt.Sprintf("fugue app overview %s", appName),
		}
	case "app-backend-unavailable", "internal-probe-failed", "internal-probe-failed-public-ok", "app-returned-error", "app-route-missing", "auth-failed":
		return []string{
			fmt.Sprintf("fugue app diagnose %s", appName),
			fmt.Sprintf("fugue app logs runtime %s --previous", appName),
		}
	default:
		return []string{
			fmt.Sprintf("fugue app overview %s", appName),
		}
	}
}

func buildAppRequestEnvRequirements(entries []model.AppEnvEntry, headerFromEnv map[string]string, required []string) []appRequestEnvRequirement {
	if len(headerFromEnv) == 0 && len(required) == 0 {
		return nil
	}
	entryByKey := make(map[string]model.AppEnvEntry, len(entries))
	for _, entry := range entries {
		key := strings.TrimSpace(entry.Key)
		if key == "" {
			continue
		}
		entryByKey[key] = entry
	}

	usageByKey := map[string][]string{}
	for headerName, envKey := range headerFromEnv {
		envKey = strings.TrimSpace(envKey)
		if envKey == "" {
			continue
		}
		usageByKey[envKey] = appendUniqueString(usageByKey[envKey], "header "+strings.TrimSpace(headerName))
	}
	for _, key := range required {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		usageByKey[key] = appendUniqueString(usageByKey[key], "request prerequisite")
	}

	keys := make([]string, 0, len(usageByKey))
	for key := range usageByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	requirements := make([]appRequestEnvRequirement, 0, len(keys))
	for _, key := range keys {
		requirement := appRequestEnvRequirement{
			Key:    key,
			UsedBy: append([]string(nil), usageByKey[key]...),
		}
		if entry, ok := entryByKey[key]; ok {
			requirement.Present = true
			requirement.Source = strings.TrimSpace(entry.Source)
			requirement.SourceRef = strings.TrimSpace(entry.SourceRef)
		}
		requirements = append(requirements, requirement)
	}
	return requirements
}

func buildAppRequestHeaderInjections(headerFromEnv map[string]string) []appRequestHeaderInjection {
	if len(headerFromEnv) == 0 {
		return nil
	}
	headers := make([]string, 0, len(headerFromEnv))
	for headerName := range headerFromEnv {
		headers = append(headers, headerName)
	}
	sort.Strings(headers)

	injections := make([]appRequestHeaderInjection, 0, len(headers))
	for _, headerName := range headers {
		injections = append(injections, appRequestHeaderInjection{
			Header: strings.TrimSpace(headerName),
			EnvKey: strings.TrimSpace(headerFromEnv[headerName]),
		})
	}
	return injections
}

func annotateAppRequestHeaderInjections(injections []appRequestHeaderInjection, requirements []appRequestEnvRequirement) []appRequestHeaderInjection {
	if len(injections) == 0 || len(requirements) == 0 {
		return injections
	}
	requirementByKey := make(map[string]appRequestEnvRequirement, len(requirements))
	for _, requirement := range requirements {
		requirementByKey[strings.TrimSpace(requirement.Key)] = requirement
	}
	annotated := append([]appRequestHeaderInjection(nil), injections...)
	for index := range annotated {
		requirement, ok := requirementByKey[strings.TrimSpace(annotated[index].EnvKey)]
		if !ok {
			continue
		}
		annotated[index].Present = requirement.Present
	}
	return annotated
}

func collectAppRequestHeaderNames(headers http.Header, headerFromEnv map[string]string) []string {
	set := map[string]struct{}{}
	for key := range headers {
		if value := strings.TrimSpace(key); value != "" {
			set[value] = struct{}{}
		}
	}
	for headerName := range headerFromEnv {
		if value := strings.TrimSpace(headerName); value != "" {
			set[value] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	names := make([]string, 0, len(set))
	for key := range set {
		names = append(names, key)
	}
	sort.Strings(names)
	return names
}

func buildAppInternalRequestURL(app model.App, requestPath string, query map[string][]string) (string, error) {
	if app.InternalService == nil || strings.TrimSpace(app.InternalService.Host) == "" || app.InternalService.Port <= 0 {
		return "", fmt.Errorf("app does not expose an internal HTTP service")
	}
	return buildAppRequestURL(fmt.Sprintf("http://%s:%d", strings.TrimSpace(app.InternalService.Host), app.InternalService.Port), requestPath, query)
}

func buildAppPublicRequestURL(app model.App, requestPath string, query map[string][]string) (string, error) {
	baseURL := appPublicBaseURL(app)
	if strings.TrimSpace(baseURL) == "" {
		return "", fmt.Errorf("app does not have a public route configured")
	}
	return buildAppRequestURL(baseURL, requestPath, query)
}

func appPublicBaseURL(app model.App) string {
	if app.Route == nil {
		return ""
	}
	if value := strings.TrimSpace(app.Route.PublicURL); value != "" {
		return value
	}
	if value := strings.TrimSpace(app.Route.Hostname); value != "" {
		return "https://" + value
	}
	return ""
}

func buildAppRequestURL(baseURL, requestPath string, query map[string][]string) (string, error) {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("parse base URL: %w", err)
	}
	relative, err := url.Parse(strings.TrimSpace(requestPath))
	if err != nil {
		return "", fmt.Errorf("parse request path: %w", err)
	}
	if relative.IsAbs() || strings.TrimSpace(relative.Host) != "" {
		return "", fmt.Errorf("path must be relative to the app route")
	}
	base.Path = joinAppRequestPath(base.Path, relative.Path)
	base.RawPath = ""
	values := base.Query()
	for key, entries := range relative.Query() {
		for _, value := range entries {
			values.Add(key, value)
		}
	}
	for key, entries := range query {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		for _, value := range entries {
			values.Add(key, value)
		}
	}
	base.RawQuery = values.Encode()
	base.Fragment = ""
	return base.String(), nil
}

func joinAppRequestPath(basePath, requestPath string) string {
	basePath = strings.TrimSpace(basePath)
	requestPath = strings.TrimSpace(requestPath)
	switch {
	case requestPath == "":
		if basePath == "" {
			return "/"
		}
		return basePath
	case strings.HasPrefix(requestPath, "/"):
		return requestPath
	case basePath == "" || basePath == "/":
		return "/" + requestPath
	default:
		return strings.TrimRight(basePath, "/") + "/" + requestPath
	}
}

func hasMissingAppRequestEnvRequirement(requirements []appRequestEnvRequirement) bool {
	for _, requirement := range requirements {
		if !requirement.Present {
			return true
		}
	}
	return false
}

func firstMissingAppRequestEnvKey(requirements []appRequestEnvRequirement) string {
	for _, requirement := range requirements {
		if !requirement.Present {
			return requirement.Key
		}
	}
	return "KEY"
}

func formatMissingAppRequestEnvKeys(requirements []appRequestEnvRequirement) string {
	keys := make([]string, 0, len(requirements))
	for _, requirement := range requirements {
		if !requirement.Present {
			keys = append(keys, requirement.Key)
		}
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

func encodeAppRequestQuery(values map[string][]string) string {
	if len(values) == 0 {
		return ""
	}
	query := url.Values{}
	for key, entries := range values {
		for _, value := range entries {
			query.Add(key, value)
		}
	}
	return query.Encode()
}

func formatAppRequestHeaderInjection(injection appRequestHeaderInjection) string {
	status := "missing"
	if injection.Present {
		status = "resolved"
	}
	return fmt.Sprintf("%s<-%s (%s)", strings.TrimSpace(injection.Header), strings.TrimSpace(injection.EnvKey), status)
}

func formatAppRequestEnvRequirement(requirement appRequestEnvRequirement) string {
	parts := []string{strings.TrimSpace(requirement.Key)}
	switch {
	case requirement.Present:
		parts = append(parts, "present")
	default:
		parts = append(parts, "missing")
	}
	if value := strings.TrimSpace(requirement.Source); value != "" {
		parts = append(parts, "source="+value)
	}
	if value := strings.TrimSpace(requirement.SourceRef); value != "" {
		parts = append(parts, "ref="+value)
	}
	if len(requirement.UsedBy) > 0 {
		parts = append(parts, "used_by="+strings.Join(requirement.UsedBy, ", "))
	}
	return strings.Join(parts, " ")
}

func formatAppRequestRelatedObject(object appRequestRelatedObject) string {
	parts := []string{strings.TrimSpace(object.Kind)}
	if value := strings.TrimSpace(object.Name); value != "" {
		parts = append(parts, value)
	}
	if value := strings.TrimSpace(object.ID); value != "" {
		parts = append(parts, "id="+value)
	}
	if value := strings.TrimSpace(object.Ref); value != "" {
		parts = append(parts, "ref="+value)
	}
	return strings.Join(parts, " ")
}

func describeAppHTTPProbe(probe appHTTPProbe, label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		label = strings.TrimSpace(probe.Target)
	}
	target := strings.TrimSpace(probe.URL)
	if target == "" {
		target = strings.TrimSpace(probe.Target)
	}
	if strings.TrimSpace(probe.Error) != "" {
		return fmt.Sprintf("%s %s -> error: %s", label, target, strings.TrimSpace(probe.Error))
	}
	return fmt.Sprintf("%s %s -> %s", label, target, displayAppHTTPProbeStatus(probe))
}

func displayAppHTTPProbeStatus(probe appHTTPProbe) string {
	switch {
	case strings.TrimSpace(probe.Error) != "":
		return "error: " + strings.TrimSpace(probe.Error)
	case strings.TrimSpace(probe.Status) != "":
		return strings.TrimSpace(probe.Status)
	case probe.StatusCode > 0:
		return formatInt(probe.StatusCode)
	default:
		return "no response"
	}
}

func looksStaticFallback(public, internal appHTTPProbe, app model.App) bool {
	if !probeLooksHTML(public) {
		return false
	}
	if probeLooksHTML(internal) && public.StatusCode == internal.StatusCode {
		return false
	}
	switch {
	case strings.EqualFold(appRequestBuildStrategy(app), model.AppBuildStrategyStaticSite):
		return true
	case public.StatusCode == http.StatusOK && !isSuccessfulStatus(internal.StatusCode):
		return true
	case public.StatusCode == http.StatusNotFound && internal.StatusCode != http.StatusNotFound:
		return true
	default:
		return false
	}
}

func appRequestBuildStrategy(app model.App) string {
	switch {
	case model.AppBuildSource(app) != nil:
		return strings.TrimSpace(model.AppBuildSource(app).BuildStrategy)
	case app.Source != nil:
		return strings.TrimSpace(app.Source.BuildStrategy)
	case app.OriginSource != nil:
		return strings.TrimSpace(app.OriginSource.BuildStrategy)
	default:
		return ""
	}
}

func probeLooksHTML(probe appHTTPProbe) bool {
	contentType := strings.ToLower(strings.TrimSpace(firstHeaderValue(probe.Headers, "Content-Type")))
	if strings.Contains(contentType, "text/html") {
		return true
	}
	body := strings.ToLower(strings.TrimSpace(probe.Body))
	return strings.HasPrefix(body, "<!doctype html") || strings.HasPrefix(body, "<html")
}

func firstHeaderValue(headers map[string][]string, key string) string {
	for currentKey, values := range headers {
		if !strings.EqualFold(strings.TrimSpace(currentKey), strings.TrimSpace(key)) || len(values) == 0 {
			continue
		}
		return strings.TrimSpace(values[0])
	}
	return ""
}

func isSuccessfulStatus(statusCode int) bool {
	return statusCode >= 200 && statusCode < 400
}

func isAuthStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return true
	default:
		return false
	}
}

func isServerError(statusCode int) bool {
	return statusCode >= 500 && statusCode < 600
}

func isGatewayStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
