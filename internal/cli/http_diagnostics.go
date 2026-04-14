package cli

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/spf13/cobra"
)

const defaultDiagnosticBodyLimit = 256 * 1024

type rawRequestOptions struct {
	Body         string
	BodyFile     string
	ContentType  string
	Cookie       string
	Headers      []string
	MaxBodyBytes int
}

type rawHTTPDiagnostic struct {
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
	Timing       httpTimingView      `json:"timing"`
}

type httpTimingView struct {
	DNS              string `json:"dns,omitempty"`
	Connect          string `json:"connect,omitempty"`
	TLS              string `json:"tls,omitempty"`
	TTFB             string `json:"ttfb,omitempty"`
	Total            string `json:"total"`
	ReusedConnection bool   `json:"reused_connection,omitempty"`
}

type timingCollector struct {
	mu       sync.Mutex
	requests []httpObservedRequest
}

type timingCommandResult struct {
	Command  []string            `json:"command"`
	Requests []timingRequestView `json:"requests"`
	Error    string              `json:"error,omitempty"`
}

type timingRequestView struct {
	Method       string         `json:"method"`
	URL          string         `json:"url"`
	StatusCode   int            `json:"status_code,omitempty"`
	Error        string         `json:"error,omitempty"`
	ResponseSize int            `json:"response_size,omitempty"`
	ServerTiming string         `json:"server_timing,omitempty"`
	Timing       httpTimingView `json:"timing"`
}

func (c *CLI) newAPICommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "api",
		Short: "Inspect raw control-plane HTTP requests and responses",
	}
	cmd.AddCommand(c.newAPIRequestCommand())
	return cmd
}

func (c *CLI) newCurlCommand() *cobra.Command {
	cmd := c.newAPIRequestCommand()
	cmd.Use = "curl [method] <path-or-url>"
	cmd.Aliases = nil
	cmd.Short = "Compatibility alias for api request"
	return cmd
}

func (c *CLI) newAPIRequestCommand() *cobra.Command {
	opts := rawRequestOptions{MaxBodyBytes: defaultDiagnosticBodyLimit}
	cmd := &cobra.Command{
		Use:   "request [method] <path-or-url>",
		Short: "Send a raw HTTP request to the Fugue control-plane API",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			method, target, err := parseRequestMethodAndTarget(args)
			if err != nil {
				return err
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := executeRawRequest(client, method, target, opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderRawHTTPDiagnostic(c.stdout, response)
		},
	}
	bindRawRequestFlags(cmd, &opts, false)
	return cmd
}

func (c *CLI) newDiagnoseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diagnose",
		Short: "Run request-level troubleshooting workflows",
	}
	cmd.AddCommand(c.newDiagnoseTimingCommand())
	return cmd
}

func (c *CLI) newDiagnoseTimingCommand() *cobra.Command {
	opts := struct {
		Passthrough bool
	}{}
	cmd := &cobra.Command{
		Use:   "timing -- <fugue command...>",
		Short: "Trace per-request timing for any Fugue CLI command",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			args = trimCommandArgs(args)
			if len(args) == 0 {
				return fmt.Errorf("command is required after --")
			}

			collector := &timingCollector{}
			var innerStdout io.Writer = io.Discard
			var innerStderr io.Writer = io.Discard
			var stdoutCapture bytes.Buffer
			var stderrCapture bytes.Buffer
			if opts.Passthrough {
				innerStdout = &stdoutCapture
				innerStderr = &stderrCapture
			}

			child := newCLI(innerStdout, innerStderr)
			child.root = c.root
			child.observer = collector.observe
			childCmd := child.newRootCommand()
			childCmd.SetOut(innerStdout)
			childCmd.SetErr(innerStderr)
			childCmd.SetArgs(args)
			childCmd.SilenceErrors = true
			childCmd.SilenceUsage = true
			runErr := childCmd.Execute()

			if opts.Passthrough {
				if stdoutCapture.Len() > 0 {
					if _, err := io.Copy(c.stdout, &stdoutCapture); err != nil {
						return err
					}
				}
				if stderrCapture.Len() > 0 {
					if _, err := io.Copy(c.stderr, &stderrCapture); err != nil {
						return err
					}
				}
			}

			result := timingCommandResult{
				Command:  append([]string(nil), args...),
				Requests: collector.views(),
			}
			if runErr != nil {
				result.Error = runErr.Error()
			}

			if c.wantsJSON() {
				if err := writeJSON(c.stdout, result); err != nil {
					return err
				}
			} else {
				if err := renderTimingCommandResult(c.stdout, result); err != nil {
					return err
				}
			}
			if runErr != nil {
				return runErr
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&opts.Passthrough, "passthrough", false, "Also print the wrapped command output")
	return cmd
}

func (c *CLI) newWebCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "web",
		Short: "Inspect Fugue web page APIs and product-layer snapshots",
	}
	cmd.AddCommand(c.newWebDiagnoseCommand())
	return cmd
}

func (c *CLI) newWebDiagnoseCommand() *cobra.Command {
	opts := rawRequestOptions{MaxBodyBytes: defaultDiagnosticBodyLimit}
	cmd := &cobra.Command{
		Use:   "diagnose [method] <page-or-path>",
		Short: "Request a Fugue web page snapshot or arbitrary web route",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			method, target, err := parseRequestMethodAndTarget(args)
			if err != nil {
				return err
			}
			target = resolveWebDiagnosticTarget(target)
			client, err := c.newWebClient(opts.Cookie)
			if err != nil {
				return err
			}
			response, err := executeRawRequest(client, method, target, opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderRawHTTPDiagnostic(c.stdout, response)
		},
	}
	bindRawRequestFlags(cmd, &opts, true)
	return cmd
}

func bindRawRequestFlags(cmd *cobra.Command, opts *rawRequestOptions, includeCookie bool) {
	cmd.Flags().StringVar(&opts.Body, "body", "", "Inline request body")
	cmd.Flags().StringVar(&opts.BodyFile, "body-file", "", "Read request body from a local path or '-' for stdin")
	cmd.Flags().StringVar(&opts.ContentType, "content-type", "", "Override request Content-Type")
	cmd.Flags().StringArrayVar(&opts.Headers, "header", nil, "Request header in 'Key: value' form (repeatable)")
	cmd.Flags().IntVar(&opts.MaxBodyBytes, "max-body-bytes", opts.MaxBodyBytes, "Maximum response bytes to print")
	if includeCookie {
		cmd.Flags().StringVar(&opts.Cookie, "cookie", "", "Optional web session cookie header value")
	}
}

func parseRequestMethodAndTarget(args []string) (string, string, error) {
	switch len(args) {
	case 1:
		target := strings.TrimSpace(args[0])
		if target == "" {
			return "", "", fmt.Errorf("request target is required")
		}
		return http.MethodGet, target, nil
	case 2:
		method := strings.ToUpper(strings.TrimSpace(args[0]))
		target := strings.TrimSpace(args[1])
		if method == "" || target == "" {
			return "", "", fmt.Errorf("method and request target are required")
		}
		return method, target, nil
	default:
		return "", "", fmt.Errorf("expected [method] <path-or-url>")
	}
}

func executeRawRequest(client *Client, method, target string, opts rawRequestOptions) (rawHTTPDiagnostic, error) {
	body, err := loadRawRequestBody(opts)
	if err != nil {
		return rawHTTPDiagnostic{}, err
	}
	contentType := strings.TrimSpace(opts.ContentType)
	if contentType == "" && len(body) > 0 {
		if json.Valid(body) {
			contentType = "application/json"
		} else {
			contentType = "text/plain; charset=utf-8"
		}
	}

	response, err := client.DoRequest(method, target, opts.Headers, body, contentType)
	if err != nil {
		return rawHTTPDiagnostic{}, err
	}

	encoding, renderedBody, truncated := encodeBodyPreview(response.Payload, opts.MaxBodyBytes)
	return rawHTTPDiagnostic{
		Method:       response.Method,
		URL:          response.URL,
		Status:       response.Status,
		StatusCode:   response.StatusCode,
		Headers:      headersToMap(response.Headers),
		Body:         renderedBody,
		BodyEncoding: encoding,
		BodySize:     len(response.Payload),
		Truncated:    truncated,
		ServerTiming: strings.TrimSpace(response.Headers.Get("Server-Timing")),
		Timing:       toTimingView(response.Timing),
	}, nil
}

func loadRawRequestBody(opts rawRequestOptions) ([]byte, error) {
	hasBody := strings.TrimSpace(opts.Body) != ""
	hasBodyFile := strings.TrimSpace(opts.BodyFile) != ""
	switch {
	case hasBody && hasBodyFile:
		return nil, fmt.Errorf("--body and --body-file cannot be used together")
	case hasBody:
		return []byte(opts.Body), nil
	case hasBodyFile:
		bodyFile := strings.TrimSpace(opts.BodyFile)
		if bodyFile == "-" {
			return io.ReadAll(os.Stdin)
		}
		data, err := os.ReadFile(bodyFile)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", bodyFile, err)
		}
		return data, nil
	default:
		return nil, nil
	}
}

func (c *Client) DoRequest(method, target string, rawHeaders []string, body []byte, contentType string) (httpPreparedResponse, error) {
	if strings.TrimSpace(method) == "" {
		method = http.MethodGet
	}
	var requestBody io.Reader
	if body != nil {
		requestBody = bytes.NewReader(body)
	}
	httpReq, err := http.NewRequest(strings.ToUpper(strings.TrimSpace(method)), c.resolveURL(target), requestBody)
	if err != nil {
		return httpPreparedResponse{}, fmt.Errorf("build request: %w", err)
	}
	headers, err := parseRawHeaders(rawHeaders)
	if err != nil {
		return httpPreparedResponse{}, err
	}
	for key, values := range headers {
		for _, value := range values {
			httpReq.Header.Add(key, value)
		}
	}
	if strings.TrimSpace(contentType) != "" && len(body) > 0 && strings.TrimSpace(httpReq.Header.Get("Content-Type")) == "" {
		httpReq.Header.Set("Content-Type", contentType)
	}
	if strings.TrimSpace(httpReq.Header.Get("Accept")) == "" {
		httpReq.Header.Set("Accept", "*/*")
	}
	return c.doPrepared(httpReq)
}

func parseRawHeaders(values []string) (http.Header, error) {
	headers := http.Header{}
	for _, entry := range values {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		separator := strings.Index(entry, ":")
		if separator < 0 {
			separator = strings.Index(entry, "=")
		}
		if separator <= 0 {
			return nil, fmt.Errorf("header %q must use 'Key: value' form", entry)
		}
		key := strings.TrimSpace(entry[:separator])
		value := strings.TrimSpace(entry[separator+1:])
		if key == "" {
			return nil, fmt.Errorf("header %q is missing a key", entry)
		}
		headers.Add(key, value)
	}
	return headers, nil
}

func encodeBodyPreview(payload []byte, maxBytes int) (string, string, bool) {
	if maxBytes <= 0 {
		maxBytes = len(payload)
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

func headersToMap(headers http.Header) map[string][]string {
	if headers == nil {
		return map[string][]string{}
	}
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string][]string, len(headers))
	for _, key := range keys {
		out[key] = append([]string(nil), headers.Values(key)...)
	}
	return out
}

func toTimingView(metrics httpTimingMetrics) httpTimingView {
	return httpTimingView{
		DNS:              formatDuration(metrics.DNS),
		Connect:          formatDuration(metrics.Connect),
		TLS:              formatDuration(metrics.TLS),
		TTFB:             formatDuration(metrics.TTFB),
		Total:            formatDuration(metrics.Total),
		ReusedConnection: metrics.ReusedConnection,
	}
}

func renderRawHTTPDiagnostic(w io.Writer, response rawHTTPDiagnostic) error {
	pairs := []kvPair{
		{Key: "method", Value: response.Method},
		{Key: "url", Value: response.URL},
		{Key: "status", Value: response.Status},
		{Key: "body_bytes", Value: fmt.Sprintf("%d", response.BodySize)},
		{Key: "body_encoding", Value: response.BodyEncoding},
		{Key: "dns", Value: response.Timing.DNS},
		{Key: "connect", Value: response.Timing.Connect},
		{Key: "tls", Value: response.Timing.TLS},
		{Key: "ttfb", Value: response.Timing.TTFB},
		{Key: "total", Value: response.Timing.Total},
	}
	if response.Timing.ReusedConnection {
		pairs = append(pairs, kvPair{Key: "reused_connection", Value: "true"})
	}
	if response.ServerTiming != "" {
		pairs = append(pairs, kvPair{Key: "server_timing", Value: response.ServerTiming})
	}
	if response.Truncated {
		pairs = append(pairs, kvPair{Key: "body_truncated", Value: "true"})
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "\nheaders:"); err != nil {
		return err
	}
	if err := writeHeaderBlock(w, response.Headers); err != nil {
		return err
	}
	if strings.TrimSpace(response.Body) == "" {
		return nil
	}
	if _, err := fmt.Fprintln(w, "\nbody:"); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w, strings.TrimRight(response.Body, "\n"))
	return err
}

func writeHeaderBlock(w io.Writer, headers map[string][]string) error {
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values := headers[key]
		if _, err := fmt.Fprintf(w, "  %s: %s\n", key, strings.Join(values, ", ")); err != nil {
			return err
		}
	}
	return nil
}

func (c *timingCollector) observe(request httpObservedRequest) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, request)
}

func (c *timingCollector) views() []timingRequestView {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]timingRequestView, 0, len(c.requests))
	for _, request := range c.requests {
		out = append(out, timingRequestView{
			Method:       request.Method,
			URL:          request.URL,
			StatusCode:   request.StatusCode,
			Error:        request.Error,
			ResponseSize: request.ResponseSize,
			ServerTiming: request.ServerTiming,
			Timing:       toTimingView(request.Timing),
		})
	}
	return out
}

func renderTimingCommandResult(w io.Writer, result timingCommandResult) error {
	if _, err := fmt.Fprintf(w, "command=%s\n", strings.Join(result.Command, " ")); err != nil {
		return err
	}
	if result.Error != "" {
		if _, err := fmt.Fprintf(w, "error=%s\n", result.Error); err != nil {
			return err
		}
	}
	if len(result.Requests) == 0 {
		if _, err := fmt.Fprintln(w, "requests=0"); err != nil {
			return err
		}
		return nil
	}
	if _, err := fmt.Fprintf(w, "requests=%d\n", len(result.Requests)); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "#\tMETHOD\tSTATUS\tHOST\tPATH\tDNS\tCONNECT\tTLS\tTTFB\tTOTAL\tSERVER-TIMING"); err != nil {
		return err
	}
	for index, request := range result.Requests {
		host, requestPath := splitRequestURL(request.URL)
		status := ""
		switch {
		case request.Error != "":
			status = "error"
		case request.StatusCode > 0:
			status = fmt.Sprintf("%d", request.StatusCode)
		default:
			status = "-"
		}
		if _, err := fmt.Fprintf(
			tw,
			"%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			index+1,
			request.Method,
			status,
			host,
			requestPath,
			request.Timing.DNS,
			request.Timing.Connect,
			request.Timing.TLS,
			request.Timing.TTFB,
			request.Timing.Total,
			request.ServerTiming,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func splitRequestURL(raw string) (string, string) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", strings.TrimSpace(raw)
	}
	requestPath := parsed.EscapedPath()
	if parsed.RawQuery != "" {
		requestPath += "?" + parsed.RawQuery
	}
	return parsed.Host, requestPath
}

func formatDuration(value time.Duration) string {
	if value <= 0 {
		return ""
	}
	return value.Round(time.Microsecond).String()
}

func resolveWebDiagnosticTarget(target string) string {
	target = strings.TrimSpace(target)
	if target == "" {
		return target
	}
	if strings.HasPrefix(target, "/") || strings.Contains(target, "://") {
		return target
	}
	if resolved, ok := webDiagnosticTargetAliases[target]; ok {
		return resolved
	}
	if strings.HasPrefix(target, "api/") {
		return "/" + target
	}
	return target
}

var webDiagnosticTargetAliases = map[string]string{
	"admin-apps":            "/api/fugue/admin/pages/apps",
	"admin-apps-usage":      "/api/fugue/admin/pages/apps?include_usage=1",
	"admin-cluster":         "/api/fugue/admin/pages/cluster",
	"admin-users":           "/api/fugue/admin/pages/users",
	"admin-users-usage":     "/api/fugue/admin/pages/users?include_usage=1",
	"admin-users-enrich":    "/api/fugue/admin/pages/users/enrich",
	"console-api-keys":      "/api/fugue/console/pages/api-keys",
	"console-billing":       "/api/fugue/console/pages/billing",
	"console-cluster":       "/api/fugue/console/pages/cluster-nodes",
	"console-profile":       "/api/fugue/console/pages/settings/profile",
	"project-gallery-usage": "/api/fugue/console/gallery?include_usage=1",
}
