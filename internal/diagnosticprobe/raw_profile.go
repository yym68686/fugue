package diagnosticprobe

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"fugue/internal/livediagnostics"
	"fugue/internal/livediagnostics/runtimeprofile"
)

// Export a single immutable profile in the existing retained report. Analysis
// is deliberately absent: a large pprof graph cannot exhaust the live target.
func rawRuntimeProfile(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if c.Path != "/v1/profile/heap" && c.Path != "/v1/profile/allocs" {
		return nil, errors.New("only heap and allocation profiles may be exported")
	}
	if c.IntervalSeconds < req.DurationSeconds || req.ContainerID == "" {
		return nil, errors.New("profile export requires one capture and an exact container")
	}
	identities, err := profileProcessIdentities(ctx, req)
	if err != nil {
		return nil, err
	}
	for pid := range identities {
		socket := filepath.Join(hostProc, strconv.Itoa(pid), "root", runtimeprofile.RuntimeSocketPath)
		info, err := os.Stat(socket)
		if err != nil || info.Mode()&os.ModeSocket == 0 {
			continue
		}
		peer := 0
		transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", socket)
			if err != nil {
				return nil, err
			}
			peer, err = runtimeSocketPeerPID(conn)
			if err != nil || identities[peer] == "" {
				conn.Close()
				return nil, errors.New("profile peer outside target")
			}
			return conn, nil
		}}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://runtime"+c.Path, nil)
		resp, err := client.Do(request)
		if err != nil {
			return nil, errors.New("profile transport unavailable")
		}
		// Base64 and envelope fit the 4MiB report; large profiles are rejected,
		// never silently truncated or expanded on the production node.
		limit := min(2800<<10, (req.MaxOutputBytes-(128<<10))*3/4)
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(limit+1)))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || readErr != nil {
			return nil, errors.New("profile capture unavailable")
		}
		if len(raw) > limit {
			return nil, errors.New("profile exceeds bounded export size")
		}
		stat, err := readBounded(filepath.Join(hostProc, strconv.Itoa(peer), "stat"), 16<<10)
		current, parseErr := parseProcessStat(peer, stat)
		after, socketErr := os.Stat(socket)
		if err != nil || parseErr != nil || current.StartTicks != identities[peer] || socketErr != nil || !os.SameFile(info, after) {
			return nil, errors.New("profile identity changed during capture")
		}
		if len(raw) < 2 || raw[0] != 0x1f || raw[1] != 0x8b {
			return nil, errors.New("profile is not a gzip protobuf")
		}
		return map[string]any{"schema": "fugue.diagnostic.raw_profile.v1", "pid": peer, "start_ticks": identities[peer], "container_id": req.ContainerID, "path": c.Path, "captured_at": time.Now().UTC(), "size_bytes": len(raw), "sha256": livediagnostics.Digest(raw), "encoding": "base64-gzip-protobuf", "chunks": profileChunks(raw), "analysis": "not performed; decode and verify digest before offline pprof"}, nil
	}
	return nil, fmt.Errorf("no runtime profile socket in target container")
}

func profileChunks(raw []byte) []string {
	chunks := []string{}
	for len(raw) > 0 {
		n := min(len(raw), 24<<10)
		chunks = append(chunks, base64.StdEncoding.EncodeToString(raw[:n]))
		raw = raw[n:]
	}
	return chunks
}
