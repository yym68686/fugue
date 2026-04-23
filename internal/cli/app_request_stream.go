package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/httpstream"
	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const (
	appRequestStreamSchemaVersion  = "fugue.app-request-stream.v1"
	defaultStreamChunkLimit        = 5
	defaultStreamChunkBytes        = 2048
	defaultAppRequestStreamTimeout = 15 * time.Second
)

type appRequestStreamCommandOptions struct {
	appRequestCommandOptions
	RequireEnv []string
	Insecure   bool
	Accepts    []string
	ChunkLimit int
	ChunkBytes int
}

type appRequestStreamResult struct {
	SchemaVersion         string                     `json:"schema_version"`
	Summary               string                     `json:"summary"`
	Category              string                     `json:"category"`
	Layer                 string                     `json:"layer,omitempty"`
	Evidence              []string                   `json:"evidence,omitempty"`
	RelatedObjects        []appRequestRelatedObject  `json:"related_objects,omitempty"`
	NextActions           []string                   `json:"next_actions,omitempty"`
	Request               appRequestPlan             `json:"request"`
	Accepts               []string                   `json:"accepts"`
	PublicRouteConfigured bool                       `json:"public_route_configured"`
	EnvRequirements       []appRequestEnvRequirement `json:"env_requirements,omitempty"`
	Probes                []model.HTTPStreamProbe    `json:"probes"`
}

func (c *CLI) newAppRequestStreamCommand() *cobra.Command {
	opts := appRequestStreamCommandOptions{
		appRequestCommandOptions: appRequestCommandOptions{
			rawRequestOptions: rawRequestOptions{MaxBodyBytes: defaultDiagnosticBodyLimit},
			Timeout:           defaultAppRequestStreamTimeout,
		},
		ChunkLimit: defaultStreamChunkLimit,
		ChunkBytes: defaultStreamChunkBytes,
	}
	cmd := &cobra.Command{
		Use:   "stream <app> [method] <path>",
		Short: "Compare public-route and internal-service streaming behavior for one HTTP endpoint",
		Args:  cobra.RangeArgs(2, 3),
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
			result, err := c.runAppRequestStream(client, fullApp, method, requestPath, opts)
			result = sanitizeAppRequestStreamResult(result, c.shouldRedact())
			if c.wantsJSON() {
				if writeErr := writeJSON(c.stdout, result); writeErr != nil {
					return writeErr
				}
			} else {
				if renderErr := renderAppRequestStreamResult(c.stdout, result); renderErr != nil {
					return renderErr
				}
			}
			if err != nil {
				return err
			}
			switch strings.TrimSpace(result.Category) {
			case "in_sync":
				return nil
			case "inconclusive":
				return withExitCode(errors.New(result.Summary), ExitCodeIndeterminate)
			default:
				return withExitCode(errors.New(result.Summary), ExitCodeSystemFault)
			}
		},
	}
	cmd.Flags().StringVar(&opts.Body, "body", "", "Inline request body")
	cmd.Flags().StringVar(&opts.BodyFile, "body-file", "", "Read request body from a local path or '-' for stdin")
	cmd.Flags().StringVar(&opts.ContentType, "content-type", "", "Override request Content-Type")
	cmd.Flags().StringArrayVar(&opts.Headers, "header", nil, "Request header in 'Key: value' form (repeatable)")
	cmd.Flags().StringArrayVar(&opts.HeaderFromEnv, "header-from-env", nil, "Header to fill from effective app env in 'Header=ENV_KEY' form")
	cmd.Flags().StringArrayVar(&opts.Query, "query", nil, "Query parameter in 'key=value' form (repeatable)")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Per-probe timeout")
	cmd.Flags().StringArrayVar(&opts.RequireEnv, "require-env", nil, "Effective app env key that must be present before the probe runs (repeatable)")
	cmd.Flags().StringArrayVar(&opts.Accepts, "accept", nil, "Accept header to test; defaults to */* and text/event-stream")
	cmd.Flags().IntVar(&opts.ChunkLimit, "chunk-limit", opts.ChunkLimit, "Number of initial response chunks or SSE frames to keep")
	cmd.Flags().IntVar(&opts.ChunkBytes, "chunk-bytes", opts.ChunkBytes, "Maximum bytes to keep per chunk sample")
	cmd.Flags().BoolVar(&opts.Insecure, "insecure", false, "Skip TLS certificate verification on the public-route probe")
	return cmd
}

func (c *CLI) runAppRequestStream(client *Client, app model.App, method, requestPath string, opts appRequestStreamCommandOptions) (appRequestStreamResult, error) {
	body, err := loadRawRequestBody(opts.rawRequestOptions)
	if err != nil {
		return appRequestStreamResult{}, err
	}
	headers, err := parseRawHeaders(opts.Headers)
	if err != nil {
		return appRequestStreamResult{}, err
	}
	headerFromEnv, err := parseSimpleStringAssignments(opts.HeaderFromEnv)
	if err != nil {
		return appRequestStreamResult{}, err
	}
	requiredEnv, err := normalizeEnvKeys(opts.RequireEnv)
	if err != nil {
		return appRequestStreamResult{}, err
	}
	accepts, err := normalizeCLIStreamAccepts(opts.Accepts)
	if err != nil {
		return appRequestStreamResult{}, err
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
	requestHeaders.Del("Accept")

	query := parseRepeatedStringAssignments(opts.Query)
	result := appRequestStreamResult{
		SchemaVersion: appRequestStreamSchemaVersion,
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
		Accepts:               append([]string(nil), accepts...),
		PublicRouteConfigured: strings.TrimSpace(appPublicBaseURL(app)) != "",
		Probes:                []model.HTTPStreamProbe{},
	}

	var envValues map[string]string
	if len(headerFromEnv) > 0 || len(requiredEnv) > 0 {
		envResponse, err := client.GetAppEnv(app.ID)
		if err != nil {
			return appRequestStreamResult{}, err
		}
		envValues = cloneStringMap(envResponse.Env)
		result.EnvRequirements = buildAppRequestEnvRequirements(envResponse.Entries, headerFromEnv, requiredEnv)
		result.Request.HeaderInjections = annotateAppRequestHeaderInjections(result.Request.HeaderInjections, result.EnvRequirements)
		if hasMissingAppRequestEnvRequirement(result.EnvRequirements) {
			result.Probes = append(result.Probes, skippedAppRequestStreamProbes("internal_service", accepts, buildSkippedProbeURL(app, requestPath, query, true))...)
			result.Probes = append(result.Probes, skippedAppRequestStreamProbes("public_route", accepts, buildSkippedProbeURL(app, requestPath, query, false))...)
			finalizeAppRequestStreamResult(&result, app)
			return result, nil
		}
	}

	internalProbes, err := client.RequestAppInternalHTTPStream(app.ID, appRequestStreamOptions{
		Method:         method,
		Path:           requestPath,
		Query:          query,
		Headers:        headersToMap(requestHeaders),
		HeadersFromEnv: cloneStringMap(headerFromEnv),
		Body:           body,
		Timeout:        opts.Timeout,
		Accepts:        accepts,
		MaxChunks:      opts.ChunkLimit,
		MaxChunkBytes:  opts.ChunkBytes,
	})
	if err != nil {
		return appRequestStreamResult{}, err
	}
	publicProbes := c.probePublicAppHTTPStream(app, method, requestPath, query, requestHeaders, headerFromEnv, envValues, body, accepts, opts)
	result.Probes = append(result.Probes, internalProbes...)
	result.Probes = append(result.Probes, publicProbes...)

	finalizeAppRequestStreamResult(&result, app)
	return result, nil
}

func (c *CLI) probePublicAppHTTPStream(app model.App, method, requestPath string, query map[string][]string, headers http.Header, headerFromEnv map[string]string, envValues map[string]string, body []byte, accepts []string, opts appRequestStreamCommandOptions) []model.HTTPStreamProbe {
	publicURL, err := buildAppPublicRequestURL(app, requestPath, query)
	if err != nil {
		return errorAppRequestStreamProbes("public_route", accepts, "", err.Error())
	}

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

	probes := make([]model.HTTPStreamProbe, 0, len(accepts))
	for _, accept := range accepts {
		var requestBody io.Reader
		if body != nil {
			requestBody = bytes.NewReader(body)
		}
		probeCtx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
		httpReq, buildErr := http.NewRequestWithContext(probeCtx, strings.ToUpper(strings.TrimSpace(method)), publicURL, requestBody)
		if buildErr != nil {
			cancel()
			probes = append(probes, model.HTTPStreamProbe{
				Target: "public_route",
				Accept: accept,
				URL:    publicURL,
				Error:  "build request: " + buildErr.Error(),
			})
			continue
		}
		for key, values := range headers {
			for _, value := range values {
				httpReq.Header.Add(key, value)
			}
		}
		for headerName, envKey := range headerFromEnv {
			value, ok := envValues[strings.TrimSpace(envKey)]
			if !ok {
				httpReq.Header.Del(strings.TrimSpace(headerName))
				continue
			}
			httpReq.Header.Add(strings.TrimSpace(headerName), value)
		}
		httpReq.Header.Del("Accept")
		httpReq.Header.Set("Accept", accept)
		probes = append(probes, httpstream.Probe(httpClient, httpReq, httpstream.ProbeOptions{
			Target:        "public_route",
			Accept:        accept,
			MaxChunks:     opts.ChunkLimit,
			MaxChunkBytes: opts.ChunkBytes,
		}))
		cancel()
	}
	return probes
}

func skippedAppRequestStreamProbes(target string, accepts []string, requestURL string) []model.HTTPStreamProbe {
	return errorAppRequestStreamProbes(target, accepts, requestURL, "probe skipped because required app env is missing")
}

func errorAppRequestStreamProbes(target string, accepts []string, requestURL, message string) []model.HTTPStreamProbe {
	probes := make([]model.HTTPStreamProbe, 0, len(accepts))
	for _, accept := range accepts {
		probes = append(probes, model.HTTPStreamProbe{
			Target: target,
			Accept: accept,
			URL:    requestURL,
			Error:  strings.TrimSpace(message),
		})
	}
	return probes
}

func buildSkippedProbeURL(app model.App, requestPath string, query map[string][]string, internal bool) string {
	var (
		url string
		err error
	)
	if internal {
		url, err = buildAppInternalRequestURL(app, requestPath, query)
	} else {
		url, err = buildAppPublicRequestURL(app, requestPath, query)
	}
	if err != nil {
		return ""
	}
	return url
}

func normalizeCLIStreamAccepts(values []string) ([]string, error) {
	if len(values) == 0 {
		return []string{"*/*", "text/event-stream"}, nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one non-empty accept header is required")
	}
	return out, nil
}

func finalizeAppRequestStreamResult(result *appRequestStreamResult, app model.App) {
	if result == nil {
		return
	}
	result.RelatedObjects = buildAppRequestRelatedObjects(app)
	result.Evidence = buildAppRequestStreamEvidence(*result)

	if hasMissingAppRequestEnvRequirement(result.EnvRequirements) {
		result.Category = "missing-env-prerequisite"
		result.Summary = fmt.Sprintf("the probe depends on missing app env %s, so Fugue skipped the request until the prerequisite is fixed", formatMissingAppRequestEnvKeys(result.EnvRequirements))
		result.Layer = "inconclusive"
		result.NextActions = buildAppRequestStreamNextActions(app, result.Category, result.Layer, result.EnvRequirements)
		return
	}
	if !result.PublicRouteConfigured {
		result.Category = "public-route-missing"
		result.Summary = "the app does not have a public route configured, so only the internal service could be probed for streaming behavior"
		result.Layer = "inconclusive"
		result.NextActions = buildAppRequestStreamNextActions(app, result.Category, result.Layer, result.EnvRequirements)
		return
	}

	for _, accept := range orderedStreamAccepts(result.Accepts) {
		internal := findStreamProbe(result.Probes, "internal_service", accept)
		public := findStreamProbe(result.Probes, "public_route", accept)

		switch {
		case internal != nil && !internal.Timing.HeadersObserved:
			result.Category = "no_headers"
			result.Layer = "app_internal"
			result.Summary = fmt.Sprintf("the internal service never returned response headers for Accept %q, so the stall is inside the app or runtime path before the public edge", accept)
			result.NextActions = buildAppRequestStreamNextActions(app, result.Category, result.Layer, result.EnvRequirements)
			return
		case internal != nil && internal.HeadersOnlyStall:
			result.Category = "headers_only_stall"
			result.Layer = "app_internal"
			result.Summary = fmt.Sprintf("the internal service returned headers for Accept %q but never produced a body byte before the timeout expired", accept)
			result.NextActions = buildAppRequestStreamNextActions(app, result.Category, result.Layer, result.EnvRequirements)
			return
		case internal != nil && streamAcceptExpectsSSE(accept) && internal.Timing.BodyByteObserved && !internal.Timing.SSEEventObserved && streamProbeTimedOut(*internal):
			result.Category = "sse_event_stall"
			result.Layer = "app_internal"
			result.Summary = fmt.Sprintf("the internal service started the HTTP body for Accept %q but never produced an SSE event before timeout", accept)
			result.NextActions = buildAppRequestStreamNextActions(app, result.Category, result.Layer, result.EnvRequirements)
			return
		}

		if internal == nil || public == nil || !streamProbeHealthy(*internal, streamAcceptExpectsSSE(accept)) {
			continue
		}
		layer := classifyPublicStreamLayer(*public)
		switch {
		case !public.Timing.HeadersObserved:
			result.Category = "no_headers"
			result.Layer = layer
			result.Summary = fmt.Sprintf("the internal service streamed successfully for Accept %q, but the public route never returned headers", accept)
			result.NextActions = buildAppRequestStreamNextActions(app, result.Category, result.Layer, result.EnvRequirements)
			return
		case public.HeadersOnlyStall:
			result.Category = "headers_only_stall"
			result.Layer = layer
			result.Summary = fmt.Sprintf("the public route returned 200-style headers for Accept %q but never produced a body byte before timeout, while the internal service did", accept)
			result.NextActions = buildAppRequestStreamNextActions(app, result.Category, result.Layer, result.EnvRequirements)
			return
		case streamAcceptExpectsSSE(accept) && internal.Timing.SSEEventObserved && public.Timing.BodyByteObserved && !public.Timing.SSEEventObserved && streamProbeTimedOut(*public):
			result.Category = "sse_event_stall"
			result.Layer = layer
			result.Summary = fmt.Sprintf("the internal service produced an SSE event for Accept %q, but the public route never produced one before timeout", accept)
			result.NextActions = buildAppRequestStreamNextActions(app, result.Category, result.Layer, result.EnvRequirements)
			return
		}
	}

	if streamResultInSync(*result) {
		result.Category = "in_sync"
		result.Layer = "in_sync"
		result.Summary = "the public route and internal service both reached headers, first body byte, and SSE delivery where expected"
		result.NextActions = nil
		return
	}

	result.Category = "inconclusive"
	result.Layer = "inconclusive"
	result.Summary = "the CLI could not localize the streaming stall with enough evidence"
	result.NextActions = buildAppRequestStreamNextActions(app, result.Category, result.Layer, result.EnvRequirements)
}

func buildAppRequestStreamEvidence(result appRequestStreamResult) []string {
	evidence := make([]string, 0, len(result.Probes)+2)
	for _, requirement := range result.EnvRequirements {
		if !requirement.Present {
			evidence = appendUniqueString(evidence, fmt.Sprintf("required env %s is missing", requirement.Key))
		}
	}
	for _, probe := range result.Probes {
		evidence = appendUniqueString(evidence, describeAppRequestStreamProbe(probe))
		if layer := classifyPublicStreamLayer(probe); probe.Target == "public_route" && layer == "external_cdn" {
			evidence = appendUniqueString(evidence, "public route response includes CDN headers")
		}
	}
	return evidence
}

func describeAppRequestStreamProbe(probe model.HTTPStreamProbe) string {
	status := strings.TrimSpace(probe.Status)
	if status == "" {
		status = "no headers"
	}
	headers := "none"
	if probe.Timing.HeadersObserved {
		headers = fmt.Sprintf("%dms", probe.Timing.TimeToHeadersMS)
	}
	firstBody := "none"
	if probe.Timing.BodyByteObserved {
		firstBody = fmt.Sprintf("%dms", probe.Timing.TimeToFirstBodyMS)
	}
	firstSSE := "none"
	if probe.Timing.SSEEventObserved {
		firstSSE = fmt.Sprintf("%dms", probe.Timing.TimeToFirstSSEMS)
	}
	detail := fmt.Sprintf("%s accept=%s -> %s headers=%s first_body=%s first_sse=%s total=%dms", probe.Target, probe.Accept, status, headers, firstBody, firstSSE, probe.Timing.TotalTimeMS)
	if probe.HeadersOnlyStall {
		detail += " headers_only_stall=true"
	}
	if message := strings.TrimSpace(probe.Error); message != "" {
		detail += " error=" + message
	}
	return detail
}

func orderedStreamAccepts(values []string) []string {
	out := append([]string(nil), values...)
	sort.SliceStable(out, func(i, j int) bool {
		leftSSE := streamAcceptExpectsSSE(out[i])
		rightSSE := streamAcceptExpectsSSE(out[j])
		if leftSSE != rightSSE {
			return leftSSE
		}
		return out[i] < out[j]
	})
	return out
}

func findStreamProbe(probes []model.HTTPStreamProbe, target, accept string) *model.HTTPStreamProbe {
	for index := range probes {
		if strings.EqualFold(strings.TrimSpace(probes[index].Target), strings.TrimSpace(target)) &&
			strings.EqualFold(strings.TrimSpace(probes[index].Accept), strings.TrimSpace(accept)) {
			return &probes[index]
		}
	}
	return nil
}

func streamAcceptExpectsSSE(accept string) bool {
	return strings.EqualFold(strings.TrimSpace(accept), "text/event-stream")
}

func streamProbeHealthy(probe model.HTTPStreamProbe, expectSSE bool) bool {
	if !probe.Timing.HeadersObserved || strings.TrimSpace(probe.Error) != "" && !probe.Timing.BodyByteObserved {
		return false
	}
	if expectSSE {
		return probe.Timing.SSEEventObserved
	}
	return probe.Timing.BodyByteObserved
}

func streamResultInSync(result appRequestStreamResult) bool {
	for _, accept := range result.Accepts {
		internal := findStreamProbe(result.Probes, "internal_service", accept)
		public := findStreamProbe(result.Probes, "public_route", accept)
		if internal == nil || public == nil {
			return false
		}
		if !streamProbeHealthy(*internal, streamAcceptExpectsSSE(accept)) || !streamProbeHealthy(*public, streamAcceptExpectsSSE(accept)) {
			return false
		}
	}
	return len(result.Accepts) > 0
}

func streamProbeTimedOut(probe model.HTTPStreamProbe) bool {
	if probe.HeadersOnlyStall {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(probe.Error))
	return strings.Contains(message, "deadline exceeded") || strings.Contains(message, "timeout")
}

func classifyPublicStreamLayer(probe model.HTTPStreamProbe) string {
	if !strings.EqualFold(strings.TrimSpace(probe.Target), "public_route") {
		return "app_internal"
	}
	for key := range probe.Headers {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "cf-ray", "cf-cache-status", "cdn-cache-control", "x-cache", "x-served-by", "x-amz-cf-id":
			return "external_cdn"
		}
	}
	server := strings.ToLower(firstHeaderValue(probe.Headers, "Server"))
	via := strings.ToLower(firstHeaderValue(probe.Headers, "Via"))
	if strings.Contains(server, "cloudflare") || strings.Contains(server, "fastly") || strings.Contains(server, "akamai") || strings.Contains(via, "cloudfront") {
		return "external_cdn"
	}
	return "edge_proxy"
}

func buildAppRequestStreamNextActions(app model.App, category, layer string, requirements []appRequestEnvRequirement) []string {
	appName := strings.TrimSpace(app.Name)
	switch strings.TrimSpace(category) {
	case "missing-env-prerequisite":
		return []string{
			fmt.Sprintf("fugue app env ls %s", appName),
			fmt.Sprintf("fugue app env set %s %s=<value>", appName, firstMissingAppRequestEnvKey(requirements)),
		}
	case "public-route-missing":
		return []string{
			fmt.Sprintf("fugue app route show %s", appName),
			fmt.Sprintf("fugue app request %s %s", appName, "/"),
		}
	case "no_headers", "headers_only_stall", "sse_event_stall":
		switch strings.TrimSpace(layer) {
		case "app_internal":
			return []string{
				fmt.Sprintf("fugue app diagnose %s", appName),
				fmt.Sprintf("fugue app logs runtime %s --previous", appName),
				fmt.Sprintf("fugue app logs pods %s", appName),
			}
		default:
			return []string{
				fmt.Sprintf("fugue app route show %s", appName),
				fmt.Sprintf("fugue app request compare %s %s", appName, "/"),
				fmt.Sprintf("fugue app request stream %s %s --accept text/event-stream", appName, "/"),
			}
		}
	default:
		return []string{
			fmt.Sprintf("fugue app overview %s", appName),
		}
	}
}

func renderAppRequestStreamResult(w io.Writer, result appRequestStreamResult) error {
	if err := writeKeyValues(w,
		kvPair{Key: "category", Value: strings.TrimSpace(result.Category)},
		kvPair{Key: "layer", Value: strings.TrimSpace(result.Layer)},
		kvPair{Key: "summary", Value: strings.TrimSpace(result.Summary)},
		kvPair{Key: "public_route_configured", Value: fmt.Sprintf("%t", result.PublicRouteConfigured)},
		kvPair{Key: "request_method", Value: strings.TrimSpace(result.Request.Method)},
		kvPair{Key: "request_path", Value: strings.TrimSpace(result.Request.Path)},
		kvPair{Key: "request_query", Value: encodeAppRequestQuery(result.Request.Query)},
		kvPair{Key: "request_timeout", Value: strings.TrimSpace(result.Request.Timeout)},
		kvPair{Key: "request_headers", Value: strings.Join(result.Request.HeaderNames, ", ")},
		kvPair{Key: "accepts", Value: strings.Join(result.Accepts, ", ")},
	); err != nil {
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
	if len(result.Probes) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if err := writeAppRequestStreamProbeTable(w, result.Probes); err != nil {
		return err
	}
	for _, probe := range result.Probes {
		if len(probe.FirstChunks) == 0 {
			continue
		}
		if _, err := fmt.Fprintf(w, "\nprobe target=%s accept=%s\n", probe.Target, probe.Accept); err != nil {
			return err
		}
		for _, sample := range probe.FirstChunks {
			if _, err := fmt.Fprintf(w, "chunk[%d] kind=%s encoding=%s size=%d", sample.Index, sample.Kind, sample.Encoding, sample.SizeBytes); err != nil {
				return err
			}
			if sample.Truncated {
				if _, err := fmt.Fprint(w, " truncated=true"); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
			if strings.TrimSpace(sample.Payload) == "" {
				continue
			}
			if _, err := fmt.Fprintln(w, strings.TrimRight(sample.Payload, "\n")); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeAppRequestStreamProbeTable(w io.Writer, probes []model.HTTPStreamProbe) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TARGET\tACCEPT\tSTATUS\tHEADERS_MS\tFIRST_BODY_MS\tFIRST_SSE_MS\tSTALL\tCHUNKS\tERROR"); err != nil {
		return err
	}
	for _, probe := range probes {
		headersMS := ""
		if probe.Timing.HeadersObserved {
			headersMS = fmt.Sprintf("%d", probe.Timing.TimeToHeadersMS)
		}
		firstBodyMS := ""
		if probe.Timing.BodyByteObserved {
			firstBodyMS = fmt.Sprintf("%d", probe.Timing.TimeToFirstBodyMS)
		}
		firstSSEMS := ""
		if probe.Timing.SSEEventObserved {
			firstSSEMS = fmt.Sprintf("%d", probe.Timing.TimeToFirstSSEMS)
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%t\t%d\t%s\n",
			probe.Target,
			probe.Accept,
			firstNonEmptyTrimmed(probe.Status, "no headers"),
			headersMS,
			firstBodyMS,
			firstSSEMS,
			probe.HeadersOnlyStall,
			len(probe.FirstChunks),
			probe.Error,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func sanitizeAppRequestStreamResult(result appRequestStreamResult, redact bool) appRequestStreamResult {
	if !redact {
		return result
	}
	result.Summary = redactDiagnosticString(result.Summary)
	result.Evidence = redactDiagnosticStringSlice(result.Evidence)
	result.NextActions = redactDiagnosticStringSlice(result.NextActions)
	for index := range result.EnvRequirements {
		result.EnvRequirements[index].SourceRef = redactDiagnosticString(result.EnvRequirements[index].SourceRef)
	}
	for index := range result.Probes {
		result.Probes[index].URL = redactDiagnosticString(result.Probes[index].URL)
		result.Probes[index].Error = redactDiagnosticString(result.Probes[index].Error)
		result.Probes[index].Headers = redactDiagnosticHeaders(result.Probes[index].Headers)
		for chunkIndex := range result.Probes[index].FirstChunks {
			result.Probes[index].FirstChunks[chunkIndex].Payload = redactDiagnosticString(result.Probes[index].FirstChunks[chunkIndex].Payload)
		}
	}
	return result
}
