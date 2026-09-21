package diagnosticprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
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
	return runtimeJSONForProcesses(ctx, c, hostProc, before)
}

func runtimeJSONForProcesses(ctx context.Context, c Collector, procRoot string, before map[int]string) (any, error) {
	pids := make([]int, 0, len(before))
	for pid := range before {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	rows := []any{}
	gaps := []string{}
	seen := []os.FileInfo{}
	skipped := 0
	for _, pid := range pids {
		socket := filepath.Join(procRoot, strconv.Itoa(pid), "root", runtimeobservation.SocketPath)
		info, err := os.Stat(socket)
		if os.IsNotExist(err) {
			skipped++
			continue
		}
		if err != nil || info.Mode()&os.ModeSocket == 0 {
			gaps = append(gaps, fmt.Sprintf("PID %d: observation socket unavailable", pid))
			continue
		}
		duplicate := false
		for _, previous := range seen {
			if os.SameFile(previous, info) {
				duplicate = true
				break
			}
		}
		if duplicate {
			skipped++
			continue
		}
		peerPID := 0
		value, err := runtimeJSONWithPeer(ctx, socket, c, func(conn net.Conn) error {
			peer, err := runtimeSocketPeerPID(conn)
			if err != nil {
				return err
			}
			if before[peer] == "" {
				return errors.New("observation socket peer is outside the frozen target process set")
			}
			peerPID = peer
			return nil
		})
		if err != nil {
			gaps = append(gaps, fmt.Sprintf("PID %d: %s", pid, boundedError(err)))
			continue
		}
		seen = append(seen, info)
		rows = append(rows, map[string]any{"pid": peerPID, "start_ticks": before[peerPID], "snapshot": value})
		if value["truncated"] == true {
			gaps = append(gaps, "runtime source rows are truncated")
		}
		stat, readErr := readBounded(filepath.Join(procRoot, strconv.Itoa(peerPID), "stat"), 16<<10)
		current, parseErr := parseProcessStat(peerPID, stat)
		if readErr != nil || parseErr != nil || current.StartTicks != before[peerPID] {
			gaps = append(gaps, "runtime provider process identity changed")
		}
		peerSocket := filepath.Join(procRoot, strconv.Itoa(peerPID), "root", runtimeobservation.SocketPath)
		if current, err := os.Stat(peerSocket); err != nil || !os.SameFile(info, current) {
			gaps = append(gaps, "runtime observation socket changed")
		}
	}
	if len(rows) == 0 {
		gaps = append(gaps, "no runtime observation provider was captured")
	}
	result := map[string]any{"provider_path": c.Path, "processes": rows, "skipped_processes": skipped, "scope": "distinct observation sockets with a verified peer in the frozen process set"}
	if len(gaps) > 0 {
		return partialValue{Value: result, Gaps: gaps}, nil
	}
	return result, nil
}

func runtimeJSONAt(ctx context.Context, socket string, c Collector) (map[string]any, error) {
	return runtimeJSONWithPeer(ctx, socket, c, nil)
}

func runtimeJSONWithPeer(ctx context.Context, socket string, c Collector, validatePeer func(net.Conn) error) (map[string]any, error) {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", socket)
		if err == nil && validatePeer != nil {
			if err := validatePeer(conn); err != nil {
				conn.Close()
				return nil, err
			}
		}
		return conn, err
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
