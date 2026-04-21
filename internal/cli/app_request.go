package cli

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type appRequestCommandOptions struct {
	rawRequestOptions
	Query         []string
	HeaderFromEnv []string
	Timeout       time.Duration
}

func (c *CLI) newAppRequestCommand() *cobra.Command {
	opts := appRequestCommandOptions{
		rawRequestOptions: rawRequestOptions{MaxBodyBytes: defaultDiagnosticBodyLimit},
		Timeout:           10 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "request <app> [method] <path>",
		Short: "Request an app internal HTTP endpoint with optional env-derived auth headers",
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
			body, err := loadRawRequestBody(opts.rawRequestOptions)
			if err != nil {
				return err
			}
			headers, err := parseRawHeaders(opts.Headers)
			if err != nil {
				return err
			}
			if strings.TrimSpace(opts.ContentType) != "" && len(body) > 0 && strings.TrimSpace(headers.Get("Content-Type")) == "" {
				headers.Set("Content-Type", strings.TrimSpace(opts.ContentType))
			}
			headersFromEnv, err := parseSimpleStringAssignments(opts.HeaderFromEnv)
			if err != nil {
				return err
			}
			response, err := client.RequestAppInternalHTTP(app.ID, appRequestOptions{
				Method:         method,
				Path:           requestPath,
				Query:          parseRepeatedStringAssignments(opts.Query),
				Headers:        headersToMap(headers),
				HeadersFromEnv: headersFromEnv,
				Body:           body,
				Timeout:        opts.Timeout,
				MaxBodyBytes:   opts.MaxBodyBytes,
			})
			if err != nil {
				diagnosis, diagnosisErr := client.TryGetAppDiagnosis(app.ID, "app")
				if diagnosisErr == nil {
					return wrapAppRequestErrorWithDiagnosis(err, diagnosis)
				}
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return renderRawHTTPDiagnostic(c.stdout, response)
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
	cmd.AddCommand(c.newAppRequestCompareCommand())
	return cmd
}

func parseAppRequestArgs(args []string) (string, string, string, error) {
	switch len(args) {
	case 2:
		return strings.TrimSpace(args[0]), http.MethodGet, strings.TrimSpace(args[1]), nil
	case 3:
		return strings.TrimSpace(args[0]), strings.ToUpper(strings.TrimSpace(args[1])), strings.TrimSpace(args[2]), nil
	default:
		return "", "", "", fmt.Errorf("expected <app> [method] <path>")
	}
}

func parseRepeatedStringAssignments(values []string) map[string][]string {
	if len(values) == 0 {
		return nil
	}
	out := map[string][]string{}
	for _, raw := range values {
		key, value, ok := strings.Cut(raw, "=")
		if !ok {
			key = strings.TrimSpace(raw)
			value = ""
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = append(out[key], value)
	}
	return out
}

func parseSimpleStringAssignments(values []string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := map[string]string{}
	for _, raw := range values {
		key, value, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("assignment %q must use Header=ENV_KEY", raw)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			return nil, fmt.Errorf("assignment %q must use Header=ENV_KEY", raw)
		}
		out[key] = value
	}
	return out, nil
}

func wrapAppRequestErrorWithDiagnosis(err error, diagnosis *appDiagnosis) error {
	if err == nil || diagnosis == nil || strings.EqualFold(strings.TrimSpace(diagnosis.Category), "available") {
		return err
	}
	lines := []string{strings.TrimSpace(err.Error())}
	if summary := strings.TrimSpace(diagnosis.Summary); summary != "" {
		lines = append(lines, "app diagnosis: "+summary)
	}
	if hint := strings.TrimSpace(diagnosis.Hint); hint != "" {
		lines = append(lines, "hint: "+hint)
	}
	if len(diagnosis.Evidence) > 0 {
		lines = append(lines, "evidence: "+strings.TrimSpace(diagnosis.Evidence[0]))
	}
	return fmt.Errorf("%s", strings.Join(lines, "\n"))
}
