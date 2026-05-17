package cli

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

type edgeCacheCheckOptions struct {
	EdgeIP    string
	EdgeGroup string
	Resolve   []string
	Insecure  bool
	Timeout   time.Duration
	Attempts  int
}

type edgeCacheCheckReport struct {
	URL        string                  `json:"url"`
	EdgeTarget string                  `json:"edge_target,omitempty"`
	Pass       bool                    `json:"pass"`
	Attempts   []edgeCacheCheckAttempt `json:"attempts"`
}

type edgeCacheCheckAttempt struct {
	Index        int           `json:"index"`
	StatusCode   int           `json:"status_code"`
	Status       string        `json:"status"`
	CacheStatus  string        `json:"cache_status,omitempty"`
	CacheControl string        `json:"cache_control,omitempty"`
	Duration     time.Duration `json:"duration"`
	Error        string        `json:"error,omitempty"`
}

func (c *CLI) newAdminEdgeCacheCheckCommand() *cobra.Command {
	opts := edgeCacheCheckOptions{Timeout: 10 * time.Second, Attempts: 2}
	cmd := &cobra.Command{
		Use:   "cache-check <url>",
		Short: "Probe an edge URL twice and verify cache behavior",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			targetURL := strings.TrimSpace(args[0])
			client, err := c.newClient()
			if err != nil {
				return err
			}
			report, err := c.checkEdgeCache(client, targetURL, opts)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				if err := writeJSON(c.stdout, report); err != nil {
					return err
				}
			} else {
				if err := writeEdgeCacheCheck(c.stdout, report); err != nil {
					return err
				}
			}
			if report.Pass {
				return nil
			}
			last := report.Attempts[len(report.Attempts)-1]
			return fmt.Errorf("cache check failed: attempt=%d cache=%s error=%s", last.Index, last.CacheStatus, firstNonEmpty(last.Error, "unexpected cache status"))
		},
	}
	cmd.Flags().StringVar(&opts.EdgeIP, "edge-ip", "", "Force the probe to connect to this edge IP while preserving Host/SNI")
	cmd.Flags().StringVar(&opts.EdgeGroup, "edge-group", "", "Force the probe through one healthy edge in this edge group")
	cmd.Flags().StringArrayVar(&opts.Resolve, "resolve", nil, "curl-style host:port:ip override for the probe (repeatable)")
	cmd.Flags().BoolVar(&opts.Insecure, "insecure", false, "Skip TLS certificate verification")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Per-request timeout")
	cmd.Flags().IntVar(&opts.Attempts, "attempts", opts.Attempts, "Number of requests to issue")
	return cmd
}

func (c *CLI) checkEdgeCache(client *Client, rawURL string, opts edgeCacheCheckOptions) (edgeCacheCheckReport, error) {
	parsedURL, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return edgeCacheCheckReport{}, fmt.Errorf("parse url: %w", err)
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return edgeCacheCheckReport{}, fmt.Errorf("url must include scheme and host")
	}
	override, err := c.resolveEdgeCacheOverride(client, parsedURL, opts)
	if err != nil {
		return edgeCacheCheckReport{}, err
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
	if override != nil {
		installAppRequestEdgeDialOverride(transport, *override)
	}
	httpClient.Transport = transport

	attempts := opts.Attempts
	if attempts <= 0 {
		attempts = 2
	}
	report := edgeCacheCheckReport{
		URL: parsedURL.String(),
	}
	if override != nil {
		report.EdgeTarget = strings.TrimSpace(override.ConnectIP)
	}

	for attempt := 1; attempt <= attempts; attempt++ {
		start := time.Now()
		req, err := http.NewRequest(http.MethodGet, parsedURL.String(), nil)
		if err != nil {
			return edgeCacheCheckReport{}, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Accept", "*/*")
		resp, err := httpClient.Do(req)
		entry := edgeCacheCheckAttempt{Index: attempt, Duration: time.Since(start)}
		if err != nil {
			entry.Error = err.Error()
			report.Attempts = append(report.Attempts, entry)
			continue
		}
		entry.StatusCode = resp.StatusCode
		entry.Status = resp.Status
		entry.CacheStatus = firstNonEmptyTrimmed(
			resp.Header.Get("X-Fugue-Cache"),
			resp.Header.Get("Cache-Status"),
			resp.Header.Get("CF-Cache-Status"),
			resp.Header.Get("X-Cache"),
		)
		entry.CacheControl = firstNonEmptyTrimmed(resp.Header.Get("Cache-Control"), resp.Header.Get("CDN-Cache-Control"))
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		report.Attempts = append(report.Attempts, entry)
	}

	report.Pass = len(report.Attempts) >= 2 && strings.EqualFold(strings.TrimSpace(report.Attempts[len(report.Attempts)-1].CacheStatus), "hit")
	return report, nil
}

func (c *CLI) resolveEdgeCacheOverride(client *Client, parsedURL *url.URL, opts edgeCacheCheckOptions) (*appRequestEdgeOverride, error) {
	modeCount := 0
	if strings.TrimSpace(opts.EdgeIP) != "" {
		modeCount++
	}
	if strings.TrimSpace(opts.EdgeGroup) != "" {
		modeCount++
	}
	if len(opts.Resolve) > 0 {
		modeCount++
	}
	if modeCount == 0 {
		return nil, nil
	}
	if modeCount > 1 {
		return nil, fmt.Errorf("set only one of --edge-ip, --edge-group, or --resolve")
	}
	host := strings.TrimSpace(parsedURL.Hostname())
	port := defaultURLPort(parsedURL)
	if host == "" || port == "" {
		return nil, fmt.Errorf("url must include a host and port")
	}
	if ip := strings.TrimSpace(opts.EdgeIP); ip != "" {
		if net.ParseIP(ip) == nil {
			return nil, fmt.Errorf("--edge-ip must be an IP address")
		}
		return &appRequestEdgeOverride{
			Host:      host,
			Port:      port,
			ConnectIP: ip,
			Source:    "edge_ip",
		}, nil
	}
	if groupID := strings.TrimSpace(opts.EdgeGroup); groupID != "" {
		response, err := client.ListEdgeNodes(groupID)
		if err != nil {
			return nil, err
		}
		node, ip, ok := selectAppRequestCompareEdgeNode(response.Nodes)
		if !ok {
			return nil, fmt.Errorf("no healthy edge node with a public IP found for edge group %s", groupID)
		}
		return &appRequestEdgeOverride{
			Host:      host,
			Port:      port,
			ConnectIP: ip,
			Source:    "edge_group:" + strings.TrimSpace(node.EdgeGroupID),
		}, nil
	}
	override, err := appRequestResolveOverrideForURL(opts.Resolve, parsedURL)
	if err != nil {
		return nil, err
	}
	return override, nil
}

func writeEdgeCacheCheck(w io.Writer, report edgeCacheCheckReport) error {
	if err := writeKeyValues(w,
		kvPair{Key: "url", Value: report.URL},
		kvPair{Key: "edge_target", Value: firstNonEmpty(report.EdgeTarget, "-")},
		kvPair{Key: "pass", Value: fmt.Sprintf("%t", report.Pass)},
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ATTEMPT\tSTATUS\tCACHE\tCACHE_CONTROL\tDURATION\tERROR"); err != nil {
		return err
	}
	for _, attempt := range report.Attempts {
		if _, err := fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n",
			attempt.Index,
			firstNonEmpty(attempt.Status, "-"),
			firstNonEmpty(attempt.CacheStatus, "-"),
			firstNonEmpty(attempt.CacheControl, "-"),
			attempt.Duration.Truncate(time.Millisecond),
			firstNonEmpty(attempt.Error, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}
