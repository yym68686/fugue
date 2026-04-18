package cli

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

type projectVerifyResult struct {
	Service      string `json:"service"`
	App          string `json:"app"`
	PublicURL    string `json:"public_url"`
	Path         string `json:"path"`
	URL          string `json:"url"`
	StatusCode   int    `json:"status_code,omitempty"`
	OK           bool   `json:"ok"`
	Error        string `json:"error,omitempty"`
	ResponseTime string `json:"response_time,omitempty"`
}

func (c *CLI) newProjectVerifyCommand() *cobra.Command {
	opts := struct {
		Paths    []string
		Services []string
		Timeout  time.Duration
		Insecure bool
	}{Paths: []string{"/healthz"}, Timeout: 5 * time.Second}

	cmd := &cobra.Command{
		Use:   "verify <project>",
		Short: "Run basic HTTP acceptance checks against project routes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			_, detail, _, err := c.loadConsoleProjectOverview(client, args[0], true)
			if err != nil {
				return err
			}
			results, err := c.verifyProjectHTTP(detail, opts.Paths, opts.Services, opts.Timeout, opts.Insecure)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"checks": results})
			}
			return renderProjectVerifyResults(c.stdout, results)
		},
	}
	cmd.Flags().StringArrayVar(&opts.Paths, "path", opts.Paths, "HTTP path to verify on each public service route (repeatable)")
	cmd.Flags().StringArrayVar(&opts.Services, "service", nil, "Limit checks to specific service/app names (repeatable)")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "HTTP request timeout per check")
	cmd.Flags().BoolVar(&opts.Insecure, "insecure", false, "Skip TLS certificate verification for acceptance checks")
	return cmd
}

func (c *CLI) verifyProjectHTTP(detail consoleProjectDetailResponse, paths, services []string, timeout time.Duration, insecure bool) ([]projectVerifyResult, error) {
	filter := make(map[string]struct{}, len(services))
	for _, service := range services {
		if value := strings.TrimSpace(service); value != "" {
			filter[value] = struct{}{}
		}
	}

	normalizedPaths := make([]string, 0, len(paths))
	for _, raw := range paths {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if !strings.HasPrefix(raw, "/") {
			raw = "/" + raw
		}
		normalizedPaths = append(normalizedPaths, raw)
	}
	if len(normalizedPaths) == 0 {
		normalizedPaths = []string{"/healthz"}
	}

	httpClient := &http.Client{Timeout: timeout}
	if insecure {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	results := make([]projectVerifyResult, 0)
	for _, app := range detail.Apps {
		service := projectServiceLabel(app)
		if len(filter) > 0 {
			if _, ok := filter[service]; !ok && !hasServiceAlias(filter, app.Name) {
				continue
			}
		}
		if app.Route == nil || strings.TrimSpace(app.Route.PublicURL) == "" {
			continue
		}
		for _, checkPath := range normalizedPaths {
			target, err := resolveVerifyTarget(strings.TrimSpace(app.Route.PublicURL), checkPath)
			if err != nil {
				results = append(results, projectVerifyResult{
					Service:   service,
					App:       app.Name,
					PublicURL: strings.TrimSpace(app.Route.PublicURL),
					Path:      checkPath,
					URL:       strings.TrimSpace(app.Route.PublicURL),
					OK:        false,
					Error:     err.Error(),
				})
				continue
			}

			startedAt := time.Now()
			req, err := http.NewRequest(http.MethodGet, target, nil)
			if err != nil {
				results = append(results, projectVerifyResult{
					Service:   service,
					App:       app.Name,
					PublicURL: strings.TrimSpace(app.Route.PublicURL),
					Path:      checkPath,
					URL:       target,
					OK:        false,
					Error:     err.Error(),
				})
				continue
			}

			resp, err := httpClient.Do(req)
			duration := time.Since(startedAt).Round(time.Millisecond)
			if err != nil {
				results = append(results, projectVerifyResult{
					Service:      service,
					App:          app.Name,
					PublicURL:    strings.TrimSpace(app.Route.PublicURL),
					Path:         checkPath,
					URL:          target,
					OK:           false,
					Error:        err.Error(),
					ResponseTime: duration.String(),
				})
				continue
			}
			_ = resp.Body.Close()
			results = append(results, projectVerifyResult{
				Service:      service,
				App:          app.Name,
				PublicURL:    strings.TrimSpace(app.Route.PublicURL),
				Path:         checkPath,
				URL:          target,
				StatusCode:   resp.StatusCode,
				OK:           resp.StatusCode >= 200 && resp.StatusCode < 400,
				ResponseTime: duration.String(),
			})
		}
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no public project routes matched the requested verify scope")
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Service == results[j].Service {
			return results[i].Path < results[j].Path
		}
		return results[i].Service < results[j].Service
	})
	return results, nil
}

func renderProjectVerifyResults(w io.Writer, results []projectVerifyResult) error {
	okCount := 0
	for _, result := range results {
		if result.OK {
			okCount++
		}
	}
	if err := writeKeyValues(w,
		kvPair{Key: "checks", Value: fmt.Sprintf("%d", len(results))},
		kvPair{Key: "passed", Value: fmt.Sprintf("%d", okCount)},
		kvPair{Key: "failed", Value: fmt.Sprintf("%d", len(results)-okCount)},
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SERVICE\tPATH\tSTATUS\tDURATION\tRESULT"); err != nil {
		return err
	}
	for _, result := range results {
		status := "-"
		if result.StatusCode > 0 {
			status = fmt.Sprintf("%d", result.StatusCode)
		}
		outcome := "ok"
		if !result.OK {
			outcome = firstNonEmptyTrimmed(result.Error, "failed")
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			displayProjectStatusValue(result.Service),
			displayProjectStatusValue(result.Path),
			displayProjectStatusValue(status),
			displayProjectStatusValue(result.ResponseTime),
			displayProjectStatusValue(outcome),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func resolveVerifyTarget(baseURL, pathValue string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("parse route url %q: %w", baseURL, err)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + pathValue
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func hasServiceAlias(values map[string]struct{}, name string) bool {
	_, ok := values[strings.TrimSpace(name)]
	return ok
}
