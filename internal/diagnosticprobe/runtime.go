package diagnosticprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
	"fugue/internal/runtimeobservation"
)

func runtimeJSON(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if !strings.HasPrefix(c.Path, "/v1/snapshots/") || strings.ContainsAny(c.Path, "?#%") || strings.Contains(c.Path, "..") || len(c.Fields) == 0 {
		return nil, errors.New("runtime snapshot requires a fixed provider path and explicit fields")
	}
	before, err := profileProcessIdentities(ctx, req)
	if err != nil {
		return nil, err
	}
	pids := make([]int, 0, len(before))
	for pid := range before {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	rows := []any{}
	gaps := []string{}
	for _, pid := range pids {
		socket := filepath.Join(hostProc, strconv.Itoa(pid), "root", runtimeobservation.SocketPath)
		value, err := runtimeJSONAt(ctx, socket, c)
		if err != nil {
			gaps = append(gaps, fmt.Sprintf("PID %d: %s", pid, boundedError(err)))
			continue
		}
		rows = append(rows, map[string]any{"pid": pid, "start_ticks": before[pid], "snapshot": value})
		if value["truncated"] == true {
			gaps = append(gaps, "runtime source rows are truncated")
		}
	}
	after, err := profileProcessIdentities(ctx, req)
	if err != nil || len(before) != len(after) {
		gaps = append(gaps, "runtime process identities changed")
	}
	for pid, start := range before {
		if after[pid] != start {
			gaps = append(gaps, "runtime process identity changed")
			break
		}
	}
	result := map[string]any{"provider_path": c.Path, "processes": rows}
	if len(gaps) > 0 {
		return partialValue{Value: result, Gaps: gaps}, nil
	}
	return result, nil
}

func runtimeJSONAt(ctx context.Context, socket string, c Collector) (map[string]any, error) {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://runtime"+c.Path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("runtime observation returned HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 1<<20 {
		return nil, errors.New("runtime response byte limit exceeded")
	}
	var object map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return nil, err
	}
	result := map[string]any{}
	for _, field := range c.Fields {
		v, ok := jsonField(object, strings.Split(field, "."))
		if !ok {
			return nil, fmt.Errorf("runtime field unavailable: %s", field)
		}
		result[field] = redactJSON(map[string]any{field: v}).(map[string]any)[field]
	}
	return result, nil
}
