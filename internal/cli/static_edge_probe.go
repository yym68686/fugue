package cli

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type staticEdgeProbeRequest struct {
	IP      string  `json:"ip"`
	Host    string  `json:"host"`
	Path    string  `json:"path"`
	Status  int     `json:"status"`
	EdgeID  string  `json:"edge_id"`
	Timeout float64 `json:"timeout"`
}
type staticEdgeProbeResult struct {
	Status    int    `json:"status"`
	EdgeID    string `json:"edge_id"`
	PeerIP    string `json:"peer_ip"`
	BodyBytes int    `json:"body_bytes"`
}

func verifyStaticEdgeProbe(req staticEdgeProbeRequest, result staticEdgeProbeResult) error {
	if result.PeerIP != req.IP {
		return errors.New("probe peer IP differs from candidate")
	}
	if result.Status != req.Status {
		return fmt.Errorf("probe %s%s via %s returned HTTP %d, expected %d", req.Host, req.Path, req.IP, result.Status, req.Status)
	}
	if req.EdgeID != "" && result.EdgeID != req.EdgeID {
		return fmt.Errorf("probe reached edge identity %q, expected %q; local TLS/SNI may be intercepted", result.EdgeID, req.EdgeID)
	}
	return nil
}

func probeStaticEdgeVerified(ctx context.Context, sshAlias, ip, host, path string, status int, edgeID string, timeout time.Duration) error {
	req := staticEdgeProbeRequest{ip, host, path, status, edgeID, timeout.Seconds()}
	if sshAlias != "" {
		payload, e := json.Marshal(req)
		if e != nil {
			return e
		}
		command := "python3 -c '" + strings.ReplaceAll(staticEdgePythonProbe, "'", "'\"'\"'") + "'"
		raw, e := staticEdgeSSHOutput(ctx, sshAlias, payload, command)
		if e != nil {
			return fmt.Errorf("probe from SSH vantage %s: %w", sshAlias, e)
		}
		var result staticEdgeProbeResult
		if e = json.Unmarshal(raw, &result); e != nil {
			return errors.New("invalid remote probe result")
		}
		return verifyStaticEdgeProbe(req, result)
	}
	var peer string
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		conn, e := (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, net.JoinHostPort(ip, "443"))
		if e == nil {
			peer, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
		}
		return conn, e
	}, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}, TLSHandshakeTimeout: timeout}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, e := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+host+path, nil)
	if e != nil {
		return e
	}
	response, e := client.Do(request)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	n, e := io.Copy(io.Discard, io.LimitReader(response.Body, 65537))
	if e != nil {
		return e
	}
	if n > 65536 {
		return errors.New("non-billable probe response exceeds 64KiB")
	}
	return verifyStaticEdgeProbe(req, staticEdgeProbeResult{response.StatusCode, response.Header.Get("X-Fugue-Static-Edge"), peer, int(n)})
}

// Fixed, read-only probe code. All variable input travels through JSON stdin,
// never through shell interpolation, and normal CA/hostname verification stays on.
const staticEdgePythonProbe = `import sys,json,ssl,socket,http.client
r=json.load(sys.stdin)
c=ssl.create_default_context()
c.minimum_version=ssl.TLSVersion.TLSv1_2
s=socket.create_connection((r["ip"],443),timeout=r["timeout"])
peer=s.getpeername()[0]
with c.wrap_socket(s,server_hostname=r["host"]) as t:
 request=("GET "+r["path"]+" HTTP/1.1\r\nHost: "+r["host"]+"\r\nConnection: close\r\nUser-Agent: fugue-static-edge-probe\r\n\r\n").encode("ascii")
 t.sendall(request)
 response=http.client.HTTPResponse(t)
 response.begin()
 body=response.read(65537)
 if len(body)>65536: raise ValueError("probe body exceeds 64KiB")
 print(json.dumps({"status":response.status,"edge_id":response.getheader("X-Fugue-Static-Edge", ""),"peer_ip":peer,"body_bytes":len(body)}))
`
