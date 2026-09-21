package diagnosticprobe

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

// LoopbackMetrics is the fixed child entrypoint used after nsenter. No user URL,
// credential, request body, redirect, filesystem path or arbitrary command.
func LoopbackMetrics(ctx context.Context, port int) ([]byte, error) {
	if port < 1024 || port > 65535 {
		return nil, errors.New("metrics port must be between 1024 and 65535")
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:"+strconv.Itoa(port)+"/metrics", nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics returned HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (2<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 2<<20 {
		return nil, errors.New("metrics response exceeds 2 MiB")
	}
	return raw, nil
}

func hostLoopbackMetrics(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if req.Target.Type != livediagnostics.TargetNodeProcess || c.Port < 1024 || c.Port > 65535 || len(c.Fields) == 0 || len(c.Fields) > 32 {
		return nil, errors.New("host metrics requires a frozen process target, explicit port and 1-32 metric families")
	}
	raw, cut, err := diagnosticCommand(ctx, 2<<20, "nsenter", "--net=/host/proc/1/ns/net", "--", "/usr/local/bin/fugue-diagnostic-probe", "--loopback-metrics", strconv.Itoa(c.Port))
	if err != nil {
		return nil, err
	}
	if cut {
		return nil, errors.New("host metrics response truncated")
	}
	return selectedMetrics(raw, c.Fields, c.Port)
}

var metricNamePattern = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

func selectedMetrics(raw []byte, names []string, port int) (any, error) {
	selected := map[string][]string{}
	for _, name := range names {
		if !metricNamePattern.MatchString(name) {
			return nil, errors.New("invalid metric family")
		}
		selected[name] = []string{}
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 4096), 256<<10)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		boundary := strings.IndexAny(line, "{ \t")
		if boundary < 0 {
			continue
		}
		name := line[:boundary]
		for _, family := range names {
			if name != family && name != family+"_bucket" && name != family+"_sum" && name != family+"_count" {
				continue
			}
			if count >= 6000 {
				return partialValue{Value: map[string]any{"port": port, "metrics": selected}, Gaps: []string{"metric sample limit reached"}, Truncated: true}, nil
			}
			selected[family] = append(selected[family], safeText(line))
			count++
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	missing := []string{}
	for _, name := range names {
		if len(selected[name]) == 0 {
			missing = append(missing, "metric family unavailable: "+name)
		}
	}
	result := map[string]any{"port": port, "path": "/metrics", "source": "host-loopback", "metrics": selected, "samples": count, "response_bytes": len(raw)}
	if len(missing) > 0 {
		return partialValue{Value: result, Gaps: missing}, nil
	}
	return result, nil
}
