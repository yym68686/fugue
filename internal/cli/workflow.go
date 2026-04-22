package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/spf13/cobra"
)

const (
	workflowSchemaVersion     = "fugue.workflow-run.v1"
	workflowBodyPreviewLimit  = 4096
	workflowDefaultTimeout    = 30 * time.Second
	workflowResultStatusOK    = "success"
	workflowResultStatusFail  = "failed"
	workflowStepStatusSkipped = "skipped"
)

type workflowSpec struct {
	Name      string                    `json:"name,omitempty" yaml:"name,omitempty"`
	BaseURLs  map[string]string         `json:"base_urls,omitempty" yaml:"base_urls,omitempty"`
	Variables map[string]string         `json:"variables,omitempty" yaml:"variables,omitempty"`
	Steps     []workflowStepSpec        `json:"steps" yaml:"steps"`
}

type workflowStepSpec struct {
	ID                string                  `json:"id,omitempty" yaml:"id,omitempty"`
	Name              string                  `json:"name,omitempty" yaml:"name,omitempty"`
	BaseURL           string                  `json:"base_url,omitempty" yaml:"base_url,omitempty"`
	URL               string                  `json:"url,omitempty" yaml:"url,omitempty"`
	Method            string                  `json:"method,omitempty" yaml:"method,omitempty"`
	Path              string                  `json:"path,omitempty" yaml:"path,omitempty"`
	Query             map[string]any          `json:"query,omitempty" yaml:"query,omitempty"`
	Headers           map[string]string       `json:"headers,omitempty" yaml:"headers,omitempty"`
	BearerToken       string                  `json:"bearer_token,omitempty" yaml:"bearer_token,omitempty"`
	Cookie            string                  `json:"cookie,omitempty" yaml:"cookie,omitempty"`
	BodyJSON          any                     `json:"body_json,omitempty" yaml:"body_json,omitempty"`
	BodyForm          map[string]any          `json:"body_form,omitempty" yaml:"body_form,omitempty"`
	BodyMultipart     []workflowMultipartPart `json:"body_multipart,omitempty" yaml:"body_multipart,omitempty"`
	ExpectStatus      []int                   `json:"expect_status,omitempty" yaml:"expect_status,omitempty"`
	Extract           []workflowExtractSpec   `json:"extract,omitempty" yaml:"extract,omitempty"`
	Timeout           string                  `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	ContinueOnFailure bool                    `json:"continue_on_failure,omitempty" yaml:"continue_on_failure,omitempty"`
}

type workflowMultipartPart struct {
	Name        string `json:"name" yaml:"name"`
	Value       string `json:"value,omitempty" yaml:"value,omitempty"`
	File        string `json:"file,omitempty" yaml:"file,omitempty"`
	Filename    string `json:"filename,omitempty" yaml:"filename,omitempty"`
	ContentType string `json:"content_type,omitempty" yaml:"content_type,omitempty"`
}

type workflowExtractSpec struct {
	Name     string `json:"name" yaml:"name"`
	From     string `json:"from,omitempty" yaml:"from,omitempty"`
	Path     string `json:"path,omitempty" yaml:"path,omitempty"`
	Header   string `json:"header,omitempty" yaml:"header,omitempty"`
	Cookie   string `json:"cookie,omitempty" yaml:"cookie,omitempty"`
	Required bool   `json:"required,omitempty" yaml:"required,omitempty"`
}

type workflowRunResult struct {
	SchemaVersion            string               `json:"schema_version"`
	Workflow                 string               `json:"workflow,omitempty"`
	SourceFile               string               `json:"source_file"`
	ObservedAt               time.Time            `json:"observed_at"`
	Status                   string               `json:"status"`
	Summary                  string               `json:"summary"`
	FailedStep               string               `json:"failed_step,omitempty"`
	FailureType              string               `json:"failure_type,omitempty"`
	CompletedSteps           []string             `json:"completed_steps,omitempty"`
	SuggestStateVerification bool                 `json:"suggest_state_verification,omitempty"`
	Redacted                 bool                 `json:"redacted"`
	Variables                map[string]string    `json:"variables,omitempty"`
	Steps                    []workflowStepResult `json:"steps"`
}

type workflowStepResult struct {
	ID                       string                      `json:"id"`
	Name                     string                      `json:"name,omitempty"`
	BaseURL                  string                      `json:"base_url,omitempty"`
	Method                   string                      `json:"method"`
	URL                      string                      `json:"url"`
	Status                   string                      `json:"status"`
	FailureType              string                      `json:"failure_type,omitempty"`
	Error                    string                      `json:"error,omitempty"`
	HTTPStatus               string                      `json:"http_status,omitempty"`
	HTTPStatusCode           int                         `json:"http_status_code,omitempty"`
	Duration                 string                      `json:"duration,omitempty"`
	Timing                   httpTimingView              `json:"timing"`
	HitEdge                  bool                        `json:"hit_edge,omitempty"`
	EdgeIndicators           []string                    `json:"edge_indicators,omitempty"`
	SuggestStateVerification bool                        `json:"suggest_state_verification,omitempty"`
	Extracted                map[string]string           `json:"extracted,omitempty"`
	Request                  workflowStepRequestSummary  `json:"request"`
	Response                 workflowStepResponseSummary `json:"response"`
}

type workflowStepRequestSummary struct {
	Query         map[string][]string `json:"query,omitempty"`
	Headers       map[string]string   `json:"headers,omitempty"`
	ContentType   string              `json:"content_type,omitempty"`
	BodyBytes     int                 `json:"body_bytes,omitempty"`
	BodyPreview   string              `json:"body_preview,omitempty"`
	BodyTruncated bool                `json:"body_truncated,omitempty"`
}

type workflowStepResponseSummary struct {
	Headers       map[string][]string `json:"headers,omitempty"`
	Cookies       map[string]string   `json:"cookies,omitempty"`
	ContentType   string              `json:"content_type,omitempty"`
	ServerTiming  string              `json:"server_timing,omitempty"`
	BodyEncoding  string              `json:"body_encoding,omitempty"`
	BodyBytes     int                 `json:"body_bytes,omitempty"`
	BodyPreview   string              `json:"body_preview,omitempty"`
	BodyTruncated bool                `json:"body_truncated,omitempty"`
}

func (c *CLI) newWorkflowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflow",
		Short: "Run declarative multi-step HTTP investigation workflows",
	}
	cmd.AddCommand(c.newWorkflowRunCommand())
	return cmd
}

func (c *CLI) newWorkflowRunCommand() *cobra.Command {
	opts := struct {
		File string
	}{}
	cmd := &cobra.Command{
		Use:   "run <workflow-file>",
		Short: "Execute a declarative HTTP workflow and preserve per-step evidence",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.File = strings.TrimSpace(args[0])
			spec, err := loadWorkflowSpec(opts.File)
			if err != nil {
				return withExitCode(err, ExitCodeUserInput)
			}
			result, runErr := c.runWorkflowSpec(opts.File, spec)
			if c.wantsJSON() {
				if err := writeJSON(c.stdout, sanitizeWorkflowRunResult(result, c.shouldRedact())); err != nil {
					return err
				}
			} else {
				if err := renderWorkflowRunResult(c.stdout, sanitizeWorkflowRunResult(result, c.shouldRedact())); err != nil {
					return err
				}
			}
			if runErr != nil {
				return runErr
			}
			return nil
		},
	}
	return cmd
}

func loadWorkflowSpec(filePath string) (workflowSpec, error) {
	data, err := os.ReadFile(strings.TrimSpace(filePath))
	if err != nil {
		return workflowSpec{}, fmt.Errorf("read workflow file %s: %w", filePath, err)
	}
	var spec workflowSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return workflowSpec{}, fmt.Errorf("parse workflow file %s: %w", filePath, err)
	}
	if len(spec.Steps) == 0 {
		return workflowSpec{}, fmt.Errorf("workflow must include at least one step")
	}
	for index := range spec.Steps {
		if strings.TrimSpace(spec.Steps[index].ID) == "" {
			spec.Steps[index].ID = fmt.Sprintf("step-%d", index+1)
		}
	}
	return spec, nil
}

func (c *CLI) runWorkflowSpec(sourceFile string, spec workflowSpec) (workflowRunResult, error) {
	result := workflowRunResult{
		SchemaVersion: workflowSchemaVersion,
		Workflow:      strings.TrimSpace(spec.Name),
		SourceFile:    sourceFile,
		ObservedAt:    time.Now().UTC(),
		Status:        workflowResultStatusOK,
		Redacted:      c.shouldRedact(),
		Variables:     map[string]string{},
	}
	vars := map[string]string{}
	for _, key := range sortedStringKeys(spec.Variables) {
		vars[key] = interpolateWorkflowString(spec.Variables[key], vars)
	}

	for _, step := range spec.Steps {
		stepResult, stepErr := c.executeWorkflowStep(spec, step, vars)
		result.Steps = append(result.Steps, stepResult)
		if strings.EqualFold(stepResult.Status, workflowResultStatusOK) {
			result.CompletedSteps = append(result.CompletedSteps, stepResult.ID)
			for key, value := range stepResult.Extracted {
				vars[key] = value
			}
			continue
		}

		result.Status = workflowResultStatusFail
		result.FailedStep = stepResult.ID
		result.FailureType = stepResult.FailureType
		result.SuggestStateVerification = stepResult.SuggestStateVerification
		result.Summary = fmt.Sprintf("workflow failed at step %s (%s)", stepResult.ID, firstNonEmptyTrimmed(stepResult.FailureType, "failed"))
		result.Variables = cloneStringMap(vars)
		if step.ContinueOnFailure {
			continue
		}
		return result, classifyWorkflowFailure(stepResult, stepErr)
	}

	result.Variables = cloneStringMap(vars)
	if result.Status == workflowResultStatusOK {
		result.Summary = fmt.Sprintf("workflow completed successfully across %d step(s)", len(result.Steps))
		return result, nil
	}
	if result.Summary == "" {
		result.Summary = "workflow completed with failures"
	}
	return result, withExitCode(fmt.Errorf("%s", result.Summary), ExitCodeIndeterminate)
}

func (c *CLI) executeWorkflowStep(spec workflowSpec, step workflowStepSpec, vars map[string]string) (workflowStepResult, error) {
	stepID := strings.TrimSpace(step.ID)
	method := strings.ToUpper(strings.TrimSpace(firstNonEmptyTrimmed(step.Method, http.MethodGet)))
	timeout, err := parseWorkflowTimeout(step.Timeout)
	if err != nil {
		return workflowStepResult{
			ID:          stepID,
			Name:        strings.TrimSpace(step.Name),
			Method:      method,
			Status:      workflowResultStatusFail,
			FailureType: "input",
			Error:       err.Error(),
		}, withExitCode(err, ExitCodeUserInput)
	}

	baseURL, targetURL, err := c.resolveWorkflowStepURL(spec, step, vars)
	if err != nil {
		return workflowStepResult{
			ID:          stepID,
			Name:        strings.TrimSpace(step.Name),
			BaseURL:     baseURL,
			Method:      method,
			URL:         targetURL,
			Status:      workflowResultStatusFail,
			FailureType: "input",
			Error:       err.Error(),
		}, withExitCode(err, ExitCodeUserInput)
	}

	queryValues := interpolateWorkflowQuery(step.Query, vars)
	headers := interpolateWorkflowHeaders(step.Headers, vars)
	if token := strings.TrimSpace(interpolateWorkflowString(step.BearerToken, vars)); token != "" {
		headers["Authorization"] = "Bearer " + token
	}
	if cookie := strings.TrimSpace(interpolateWorkflowString(step.Cookie, vars)); cookie != "" {
		headers["Cookie"] = cookie
	}

	bodyBytes, contentType, bodyPreview, bodyTruncated, err := buildWorkflowRequestBody(step, vars)
	if err != nil {
		return workflowStepResult{
			ID:          stepID,
			Name:        strings.TrimSpace(step.Name),
			BaseURL:     baseURL,
			Method:      method,
			URL:         targetURL,
			Status:      workflowResultStatusFail,
			FailureType: "input",
			Error:       err.Error(),
		}, withExitCode(err, ExitCodeUserInput)
	}
	if len(bodyBytes) > 0 && strings.TrimSpace(contentType) != "" {
		if _, ok := headers["Content-Type"]; !ok {
			headers["Content-Type"] = contentType
		}
	}

	requestSummary := workflowStepRequestSummary{
		Query:         cloneStringSliceMap(queryValues),
		Headers:       cloneStringMap(headers),
		ContentType:   strings.TrimSpace(firstNonEmptyTrimmed(headers["Content-Type"], contentType)),
		BodyBytes:     len(bodyBytes),
		BodyPreview:   bodyPreview,
		BodyTruncated: bodyTruncated,
	}

	collector := &timingCollector{}
	httpClient := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	client := &Client{
		baseURL:    baseURL,
		httpClient: httpClient,
		observer:   collector.observe,
	}
	response, err := client.DoRequest(method, workflowApplyQueryToTarget(targetURL, queryValues), encodeWorkflowHeaders(headers), bodyBytes, requestSummary.ContentType)
	stepResult := workflowStepResult{
		ID:      stepID,
		Name:    strings.TrimSpace(step.Name),
		BaseURL: baseURL,
		Method:  method,
		URL:     workflowApplyQueryToTarget(targetURL, queryValues),
		Status:  workflowResultStatusOK,
		Request: requestSummary,
	}
	if err != nil {
		if views := collector.views(); len(views) > 0 {
			stepResult.URL = firstNonEmptyTrimmed(views[0].URL, stepResult.URL)
			stepResult.Timing = views[0].Timing
			stepResult.Duration = strings.TrimSpace(views[0].Timing.Total)
		}
		stepResult.Status = workflowResultStatusFail
		stepResult.FailureType = "transport"
		stepResult.Error = err.Error()
		stepResult.SuggestStateVerification = workflowShouldSuggestStateVerification(method, 0, stepResult.FailureType)
		return stepResult, withExitCode(fmt.Errorf("workflow step %s: %w", stepID, err), ExitCodeSystemFault)
	}

	encoding, responseBody, responseTruncated := encodeBodyPreview(response.Payload, workflowBodyPreviewLimit)
	stepResult.HTTPStatus = strings.TrimSpace(response.Status)
	stepResult.HTTPStatusCode = response.StatusCode
	stepResult.Timing = toTimingView(response.Timing)
	stepResult.Duration = strings.TrimSpace(stepResult.Timing.Total)
	stepResult.Response = workflowStepResponseSummary{
		Headers:       headersToMap(response.Headers),
		Cookies:       workflowCookieMap(response.Headers),
		ContentType:   strings.TrimSpace(response.Headers.Get("Content-Type")),
		ServerTiming:  strings.TrimSpace(response.Headers.Get("Server-Timing")),
		BodyEncoding:  encoding,
		BodyBytes:     len(response.Payload),
		BodyPreview:   responseBody,
		BodyTruncated: responseTruncated,
	}
	stepResult.HitEdge, stepResult.EdgeIndicators = workflowEdgeIndicators(response.Headers)

	if !workflowMatchesExpectedStatus(response.StatusCode, step.ExpectStatus) {
		stepResult.Status = workflowResultStatusFail
		stepResult.FailureType = "unexpected_status"
		stepResult.Error = fmt.Sprintf("unexpected status %s", response.Status)
		stepResult.SuggestStateVerification = workflowShouldSuggestStateVerification(method, response.StatusCode, stepResult.FailureType)
		return stepResult, classifyWorkflowFailure(stepResult, nil)
	}

	extracted, err := workflowExtractValues(step.Extract, response)
	if err != nil {
		stepResult.Status = workflowResultStatusFail
		stepResult.FailureType = "extract"
		stepResult.Error = err.Error()
		stepResult.SuggestStateVerification = workflowShouldSuggestStateVerification(method, response.StatusCode, stepResult.FailureType)
		return stepResult, withExitCode(fmt.Errorf("workflow step %s: %w", stepID, err), ExitCodeIndeterminate)
	}
	stepResult.Extracted = extracted
	return stepResult, nil
}

func (c *CLI) resolveWorkflowStepURL(spec workflowSpec, step workflowStepSpec, vars map[string]string) (string, string, error) {
	if rawURL := strings.TrimSpace(interpolateWorkflowString(step.URL, vars)); rawURL != "" {
		parsed, err := url.Parse(rawURL)
		if err != nil {
			return "", "", fmt.Errorf("parse step url %q: %w", rawURL, err)
		}
		if parsed.Scheme == "" || parsed.Host == "" {
			return "", "", fmt.Errorf("step url %q must be absolute", rawURL)
		}
		return parsed.Scheme + "://" + parsed.Host, parsed.String(), nil
	}

	requestPath := strings.TrimSpace(interpolateWorkflowString(step.Path, vars))
	if requestPath == "" {
		return "", "", fmt.Errorf("step must set either url or path")
	}

	baseRef := strings.TrimSpace(interpolateWorkflowString(step.BaseURL, vars))
	if baseRef == "" {
		baseRef = "default"
	}
	baseURL := strings.TrimSpace(spec.BaseURLs[baseRef])
	if baseURL == "" {
		baseURL = baseRef
	}
	if strings.TrimSpace(baseURL) == "" || strings.EqualFold(baseURL, "default") {
		baseURL = c.effectiveBaseURL()
	}
	parsedBase, err := url.Parse(baseURL)
	if err != nil {
		return "", "", fmt.Errorf("parse base url %q: %w", baseURL, err)
	}
	if parsedBase.Scheme == "" || parsedBase.Host == "" {
		return "", "", fmt.Errorf("base url %q must be absolute", baseURL)
	}
	parsedPath, err := url.Parse(requestPath)
	if err != nil {
		return "", "", fmt.Errorf("parse step path %q: %w", requestPath, err)
	}
	if parsedPath.Scheme != "" || parsedPath.Host != "" {
		return "", "", fmt.Errorf("step path %q must be relative to the base url", requestPath)
	}
	parsedBase.Path = joinAppRequestPath(parsedBase.Path, parsedPath.Path)
	parsedBase.RawPath = ""
	parsedBase.RawQuery = parsedPath.RawQuery
	parsedBase.Fragment = ""
	return strings.TrimRight(baseURL, "/"), parsedBase.String(), nil
}

func parseWorkflowTimeout(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return workflowDefaultTimeout, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("timeout %q must be a Go duration like 30s or 2m", raw)
	}
	if value <= 0 {
		return 0, fmt.Errorf("timeout must be greater than zero")
	}
	return value, nil
}

func buildWorkflowRequestBody(step workflowStepSpec, vars map[string]string) ([]byte, string, string, bool, error) {
	bodyKinds := 0
	if step.BodyJSON != nil {
		bodyKinds++
	}
	if len(step.BodyForm) > 0 {
		bodyKinds++
	}
	if len(step.BodyMultipart) > 0 {
		bodyKinds++
	}
	if bodyKinds > 1 {
		return nil, "", "", false, fmt.Errorf("only one of body_json, body_form, or body_multipart may be set per step")
	}
	switch {
	case step.BodyJSON != nil:
		payload := interpolateWorkflowAny(step.BodyJSON, vars)
		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, "", "", false, fmt.Errorf("marshal body_json: %w", err)
		}
		_, preview, truncated := encodeBodyPreview(bodyBytes, workflowBodyPreviewLimit)
		return bodyBytes, "application/json", preview, truncated, nil
	case len(step.BodyForm) > 0:
		values := url.Values{}
		for _, key := range sortedAnyMapKeys(step.BodyForm) {
			values.Set(key, workflowStringValue(step.BodyForm[key], vars))
		}
		bodyBytes := []byte(values.Encode())
		_, preview, truncated := encodeBodyPreview(bodyBytes, workflowBodyPreviewLimit)
		return bodyBytes, "application/x-www-form-urlencoded", preview, truncated, nil
	case len(step.BodyMultipart) > 0:
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		parts := make([]string, 0, len(step.BodyMultipart))
		for _, part := range step.BodyMultipart {
			name := strings.TrimSpace(interpolateWorkflowString(part.Name, vars))
			if name == "" {
				return nil, "", "", false, fmt.Errorf("multipart part name is required")
			}
			if value := strings.TrimSpace(interpolateWorkflowString(part.Value, vars)); value != "" && strings.TrimSpace(part.File) != "" {
				return nil, "", "", false, fmt.Errorf("multipart part %s cannot set both value and file", name)
			}
			if filePath := strings.TrimSpace(interpolateWorkflowString(part.File, vars)); filePath != "" {
				fileBytes, err := os.ReadFile(filePath)
				if err != nil {
					return nil, "", "", false, fmt.Errorf("read multipart file %s: %w", filePath, err)
				}
				filename := strings.TrimSpace(interpolateWorkflowString(part.Filename, vars))
				if filename == "" {
					filename = filePath
				}
				fieldWriter, err := writer.CreateFormFile(name, filename)
				if err != nil {
					return nil, "", "", false, fmt.Errorf("create multipart file part %s: %w", name, err)
				}
				if _, err := fieldWriter.Write(fileBytes); err != nil {
					return nil, "", "", false, fmt.Errorf("write multipart file part %s: %w", name, err)
				}
				parts = append(parts, fmt.Sprintf("%s(file=%s)", name, filename))
				continue
			}
			value := interpolateWorkflowString(part.Value, vars)
			if err := writer.WriteField(name, value); err != nil {
				return nil, "", "", false, fmt.Errorf("write multipart field %s: %w", name, err)
			}
			parts = append(parts, name)
		}
		if err := writer.Close(); err != nil {
			return nil, "", "", false, fmt.Errorf("close multipart body: %w", err)
		}
		return body.Bytes(), writer.FormDataContentType(), "multipart parts: " + strings.Join(parts, ", "), false, nil
	default:
		return nil, "", "", false, nil
	}
}

func workflowStringValue(value any, vars map[string]string) string {
	switch typed := value.(type) {
	case string:
		return interpolateWorkflowString(typed, vars)
	case fmt.Stringer:
		return interpolateWorkflowString(typed.String(), vars)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return interpolateWorkflowString(fmt.Sprintf("%v", value), vars)
	}
}

func interpolateWorkflowQuery(values map[string]any, vars map[string]string) map[string][]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string][]string, len(values))
	for _, key := range sortedAnyMapKeys(values) {
		value := values[key]
		switch typed := value.(type) {
		case []any:
			for _, entry := range typed {
				out[key] = append(out[key], workflowStringValue(entry, vars))
			}
		default:
			out[key] = append(out[key], workflowStringValue(typed, vars))
		}
	}
	return out
}

func interpolateWorkflowHeaders(values map[string]string, vars map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for _, key := range sortedStringKeys(values) {
		out[key] = interpolateWorkflowString(values[key], vars)
	}
	return out
}

func interpolateWorkflowAny(value any, vars map[string]string) any {
	switch typed := value.(type) {
	case string:
		return interpolateWorkflowString(typed, vars)
	case []any:
		out := make([]any, 0, len(typed))
		for _, entry := range typed {
			out = append(out, interpolateWorkflowAny(entry, vars))
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, entry := range typed {
			out[key] = interpolateWorkflowAny(entry, vars)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, entry := range typed {
			out[fmt.Sprintf("%v", key)] = interpolateWorkflowAny(entry, vars)
		}
		return out
	default:
		return typed
	}
}

func interpolateWorkflowString(raw string, vars map[string]string) string {
	if raw == "" {
		return raw
	}
	return workflowInterpolationPattern.ReplaceAllStringFunc(raw, func(match string) string {
		key := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(match, "{{"), "}}"))
		switch {
		case strings.HasPrefix(strings.ToLower(key), "env."):
			return os.Getenv(strings.TrimSpace(key[4:]))
		case vars != nil:
			if value, ok := vars[key]; ok {
				return value
			}
		}
		return ""
	})
}

var workflowInterpolationPattern = regexpMustCompile(`\{\{\s*[^{}]+\s*\}\}`)

func workflowApplyQueryToTarget(target string, values map[string][]string) string {
	if len(values) == 0 {
		return target
	}
	parsed, err := url.Parse(target)
	if err != nil {
		return target
	}
	query := parsed.Query()
	for key, entries := range values {
		for _, value := range entries {
			query.Add(strings.TrimSpace(key), value)
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func encodeWorkflowHeaders(headers map[string]string) []string {
	if len(headers) == 0 {
		return nil
	}
	keys := sortedStringKeys(headers)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+": "+headers[key])
	}
	return out
}

func workflowMatchesExpectedStatus(statusCode int, allowed []int) bool {
	if len(allowed) == 0 {
		return statusCode >= 200 && statusCode < 400
	}
	for _, candidate := range allowed {
		if candidate == statusCode {
			return true
		}
	}
	return false
}

func workflowExtractValues(specs []workflowExtractSpec, response httpPreparedResponse) (map[string]string, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	var payload any
	if len(response.Payload) > 0 {
		_ = json.Unmarshal(response.Payload, &payload)
	}
	cookies := workflowCookieMap(response.Headers)
	out := map[string]string{}
	for _, spec := range specs {
		value, ok, err := workflowExtractValue(spec, payload, response.Headers, cookies)
		if err != nil {
			return nil, err
		}
		if !ok {
			if spec.Required {
				return nil, fmt.Errorf("required extraction %s was not found", strings.TrimSpace(spec.Name))
			}
			continue
		}
		out[strings.TrimSpace(spec.Name)] = value
	}
	return out, nil
}

func workflowExtractValue(spec workflowExtractSpec, payload any, headers http.Header, cookies map[string]string) (string, bool, error) {
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return "", false, fmt.Errorf("extract entries require name")
	}
	source := strings.ToLower(strings.TrimSpace(spec.From))
	if source == "" {
		switch {
		case strings.TrimSpace(spec.Header) != "":
			source = "header"
		case strings.TrimSpace(spec.Cookie) != "":
			source = "cookie"
		default:
			source = "body"
		}
	}
	switch source {
	case "body":
		path := strings.TrimSpace(firstNonEmptyTrimmed(spec.Path, spec.Header, spec.Cookie))
		if path == "" {
			return "", false, fmt.Errorf("extract %s requires path for body source", name)
		}
		value, ok := extractWorkflowJSONPath(payload, path)
		if !ok {
			return "", false, nil
		}
		return fmt.Sprintf("%v", value), true, nil
	case "header":
		key := strings.TrimSpace(firstNonEmptyTrimmed(spec.Header, spec.Path))
		if key == "" {
			return "", false, fmt.Errorf("extract %s requires header for header source", name)
		}
		value := strings.TrimSpace(headers.Get(key))
		if value == "" {
			return "", false, nil
		}
		return value, true, nil
	case "cookie":
		key := strings.TrimSpace(firstNonEmptyTrimmed(spec.Cookie, spec.Path))
		if key == "" {
			return "", false, fmt.Errorf("extract %s requires cookie for cookie source", name)
		}
		value := strings.TrimSpace(cookies[key])
		if value == "" {
			return "", false, nil
		}
		return value, true, nil
	default:
		return "", false, fmt.Errorf("extract %s source must be body, header, or cookie", name)
	}
}

func extractWorkflowJSONPath(payload any, rawPath string) (any, bool) {
	rawPath = strings.TrimSpace(rawPath)
	rawPath = strings.TrimPrefix(rawPath, "$.")
	rawPath = strings.TrimPrefix(rawPath, "$")
	rawPath = strings.TrimPrefix(rawPath, ".")
	if rawPath == "" {
		return payload, true
	}
	current := payload
	for _, segment := range strings.Split(rawPath, ".") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[segment]
			if !ok {
				return nil, false
			}
			current = next
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, false
			}
			current = typed[index]
		default:
			return nil, false
		}
	}
	return current, true
}

func workflowCookieMap(headers http.Header) map[string]string {
	response := &http.Response{Header: cloneHTTPHeaders(headers)}
	cookies := response.Cookies()
	if len(cookies) == 0 {
		return nil
	}
	out := make(map[string]string, len(cookies))
	for _, cookie := range cookies {
		if cookie == nil {
			continue
		}
		out[cookie.Name] = cookie.Value
	}
	return out
}

func workflowShouldSuggestStateVerification(method string, statusCode int, failureType string) bool {
	method = strings.ToUpper(strings.TrimSpace(method))
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	switch strings.TrimSpace(failureType) {
	case "transport", "extract":
		return true
	case "unexpected_status":
		return statusCode >= 500 || statusCode == http.StatusRequestTimeout
	default:
		return false
	}
}

func workflowEdgeIndicators(headers http.Header) (bool, []string) {
	if len(headers) == 0 {
		return false, nil
	}
	type headerIndicator struct {
		header string
		label  string
	}
	candidates := []headerIndicator{
		{header: "CF-Ray", label: "cf-ray"},
		{header: "CF-Cache-Status", label: "cf-cache-status"},
		{header: "Via", label: "via"},
		{header: "X-Cache", label: "x-cache"},
		{header: "X-Served-By", label: "x-served-by"},
		{header: "X-Amz-Cf-Id", label: "x-amz-cf-id"},
		{header: "Server", label: "server"},
	}
	indicators := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if strings.TrimSpace(headers.Get(candidate.header)) == "" {
			continue
		}
		indicators = append(indicators, candidate.label)
	}
	sort.Strings(indicators)
	return len(indicators) > 0, indicators
}

func classifyWorkflowFailure(step workflowStepResult, cause error) error {
	if cause != nil {
		return cause
	}
	message := strings.TrimSpace(firstNonEmptyTrimmed(step.Error, step.HTTPStatus))
	switch {
	case step.HTTPStatusCode == http.StatusUnauthorized || step.HTTPStatusCode == http.StatusForbidden || looksPermissionError(strings.ToLower(message)):
		return withExitCode(fmt.Errorf("workflow step %s: %s", step.ID, message), ExitCodePermissionDenied)
	case step.HTTPStatusCode == http.StatusNotFound || looksNotFoundError(strings.ToLower(message)):
		return withExitCode(fmt.Errorf("workflow step %s: %s", step.ID, message), ExitCodeNotFound)
	case step.FailureType == "transport" || step.HTTPStatusCode >= 500:
		return withExitCode(fmt.Errorf("workflow step %s: %s", step.ID, message), ExitCodeSystemFault)
	default:
		return withExitCode(fmt.Errorf("workflow step %s: %s", step.ID, message), ExitCodeIndeterminate)
	}
}

func sanitizeWorkflowRunResult(result workflowRunResult, redact bool) workflowRunResult {
	if !redact {
		result.Redacted = false
		return result
	}
	result.Redacted = true
	result.Variables = redactDiagnosticStringMap(result.Variables)
	for index := range result.Steps {
		step := &result.Steps[index]
		step.Error = redactDiagnosticString(step.Error)
		step.Extracted = redactDiagnosticStringMap(step.Extracted)
		step.Request.Headers = redactDiagnosticStringMap(step.Request.Headers)
		step.Request.BodyPreview = redactDiagnosticString(step.Request.BodyPreview)
		step.Response.Headers = redactDiagnosticHeaders(step.Response.Headers)
		if len(step.Response.Cookies) > 0 {
			for key := range step.Response.Cookies {
				step.Response.Cookies[key] = redactedSecretValue
			}
		}
		step.Response.BodyPreview = redactDiagnosticString(step.Response.BodyPreview)
	}
	return result
}

func renderWorkflowRunResult(w io.Writer, result workflowRunResult) error {
	pairs := []kvPair{
		{Key: "schema_version", Value: result.SchemaVersion},
		{Key: "workflow", Value: strings.TrimSpace(result.Workflow)},
		{Key: "source_file", Value: strings.TrimSpace(result.SourceFile)},
		{Key: "observed_at", Value: formatTime(result.ObservedAt)},
		{Key: "status", Value: strings.TrimSpace(result.Status)},
		{Key: "summary", Value: strings.TrimSpace(result.Summary)},
		{Key: "failed_step", Value: strings.TrimSpace(result.FailedStep)},
		{Key: "failure_type", Value: strings.TrimSpace(result.FailureType)},
		{Key: "completed_steps", Value: strings.Join(result.CompletedSteps, ",")},
		{Key: "suggest_state_verification", Value: strconv.FormatBool(result.SuggestStateVerification)},
		{Key: "redacted", Value: strconv.FormatBool(result.Redacted)},
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	if len(result.Variables) > 0 {
		if _, err := fmt.Fprintln(w, "\nvariables"); err != nil {
			return err
		}
		if err := writeStringMap(w, result.Variables); err != nil {
			return err
		}
	}
	for _, step := range result.Steps {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "step %s\n", strings.TrimSpace(step.ID)); err != nil {
			return err
		}
		stepPairs := []kvPair{
			{Key: "name", Value: strings.TrimSpace(step.Name)},
			{Key: "status", Value: strings.TrimSpace(step.Status)},
			{Key: "failure_type", Value: strings.TrimSpace(step.FailureType)},
			{Key: "method", Value: strings.TrimSpace(step.Method)},
			{Key: "url", Value: strings.TrimSpace(step.URL)},
			{Key: "http_status", Value: strings.TrimSpace(step.HTTPStatus)},
			{Key: "duration", Value: strings.TrimSpace(step.Duration)},
			{Key: "error", Value: strings.TrimSpace(step.Error)},
			{Key: "hit_edge", Value: strconv.FormatBool(step.HitEdge)},
			{Key: "edge_indicators", Value: strings.Join(step.EdgeIndicators, ",")},
			{Key: "suggest_state_verification", Value: strconv.FormatBool(step.SuggestStateVerification)},
		}
		if err := writeKeyValues(w, stepPairs...); err != nil {
			return err
		}
		for _, key := range sortedStringKeys(step.Extracted) {
			if _, err := fmt.Fprintf(w, "extracted=%s=%s\n", key, step.Extracted[key]); err != nil {
				return err
			}
		}
		importantHeaders := workflowImportantHeaders(step.Response.Headers)
		if len(importantHeaders) > 0 {
			if _, err := fmt.Fprintln(w, "\nresponse_headers:"); err != nil {
				return err
			}
			if err := writeHeaderBlock(w, importantHeaders); err != nil {
				return err
			}
		}
		if strings.TrimSpace(step.Response.BodyPreview) != "" {
			if _, err := fmt.Fprintln(w, "\nresponse_body:"); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(w, strings.TrimSpace(step.Response.BodyPreview)); err != nil {
				return err
			}
		}
	}
	return nil
}

func workflowImportantHeaders(headers map[string][]string) map[string][]string {
	if len(headers) == 0 {
		return nil
	}
	priority := []string{
		"Content-Type",
		"Location",
		"Server-Timing",
		"Via",
		"CF-Ray",
		"CF-Cache-Status",
		"X-Cache",
		"X-Request-Id",
		"X-Amz-Cf-Id",
	}
	out := map[string][]string{}
	for _, key := range priority {
		for currentKey, values := range headers {
			if strings.EqualFold(strings.TrimSpace(currentKey), key) {
				out[currentKey] = append([]string(nil), values...)
			}
		}
	}
	return out
}

func sortedStringKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedAnyMapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func regexpMustCompile(pattern string) *regexp.Regexp {
	return regexp.MustCompile(pattern)
}
