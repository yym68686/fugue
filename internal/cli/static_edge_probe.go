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
	"strconv"
	"strings"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type staticEdgeProbeRequest struct {
	IP                      string  `json:"ip"`
	Host                    string  `json:"host"`
	Path                    string  `json:"path"`
	Status                  int     `json:"status"`
	EdgeID                  string  `json:"edge_id"`
	Timeout                 float64 `json:"timeout"`
	RequireProtocolCoverage bool    `json:"require_protocol_coverage,omitempty"`
}
type staticEdgeProbeResult struct {
	Status    int    `json:"status"`
	EdgeID    string `json:"edge_id"`
	PeerIP    string `json:"peer_ip"`
	BodyBytes int    `json:"body_bytes"`
	AltSvc    string `json:"alt_svc,omitempty"`
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
	if req.RequireProtocolCoverage {
		for _, alternative := range strings.Split(result.AltSvc, ",") {
			protocol, _, ok := strings.Cut(strings.TrimSpace(alternative), "=")
			protocol = strings.Trim(strings.TrimSpace(protocol), "\"")
			if ok && (protocol == "h3" || strings.HasPrefix(protocol, "h3-")) {
				return fmt.Errorf("%s via %s advertises HTTP/3; the HTTPS/TCP probe cannot verify cached QUIC paths; refusing DNS cutover", req.Host, req.IP)
			}
		}
	}
	return nil
}

func probeStaticEdgeVerified(ctx context.Context, sshAlias, ip, host, path string, status int, edgeID string, timeout time.Duration, protocolCoverage ...bool) error {
	_, err := probeStaticEdgeVerifiedResult(ctx, sshAlias, ip, host, path, status, edgeID, timeout, protocolCoverage...)
	return err
}

func probeStaticEdgeVerifiedResult(ctx context.Context, sshAlias, ip, host, path string, status int, edgeID string, timeout time.Duration, protocolCoverage ...bool) (staticEdgeProbeResult, error) {
	req := staticEdgeProbeRequest{IP: ip, Host: host, Path: path, Status: status, EdgeID: edgeID, Timeout: timeout.Seconds()}
	if len(protocolCoverage) > 0 {
		req.RequireProtocolCoverage = protocolCoverage[0]
	}
	if sshAlias != "" {
		payload, e := json.Marshal(req)
		if e != nil {
			return staticEdgeProbeResult{}, e
		}
		command := "python3 -c '" + strings.ReplaceAll(staticEdgePythonProbe, "'", "'\"'\"'") + "'"
		raw, e := staticEdgeSSHOutput(ctx, sshAlias, payload, command)
		if e != nil {
			return staticEdgeProbeResult{}, fmt.Errorf("probe from SSH vantage %s: %w", sshAlias, e)
		}
		var result staticEdgeProbeResult
		if e = json.Unmarshal(raw, &result); e != nil {
			return result, errors.New("invalid remote probe result")
		}
		return result, verifyStaticEdgeProbe(req, result)
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
		return staticEdgeProbeResult{}, e
	}
	response, e := client.Do(request)
	if e != nil {
		return staticEdgeProbeResult{}, e
	}
	defer response.Body.Close()
	n, e := io.Copy(io.Discard, io.LimitReader(response.Body, 65537))
	if e != nil {
		return staticEdgeProbeResult{}, e
	}
	if n > 65536 {
		return staticEdgeProbeResult{}, errors.New("non-billable probe response exceeds 64KiB")
	}
	result := staticEdgeProbeResult{Status: response.StatusCode, EdgeID: response.Header.Get("X-Fugue-Static-Edge"), PeerIP: peer, BodyBytes: int(n), AltSvc: strings.Join(response.Header.Values("Alt-Svc"), ",")}
	return result, verifyStaticEdgeProbe(req, result)
}

func advertisedHTTP3Port(altSvc string) (int, bool, error) {
	for _, alternative := range strings.Split(altSvc, ",") {
		protocol, address, ok := strings.Cut(strings.TrimSpace(alternative), "=")
		protocol = strings.Trim(strings.TrimSpace(protocol), "\"")
		if !ok || (protocol != "h3" && !strings.HasPrefix(protocol, "h3-")) {
			continue
		}
		address = strings.TrimSpace(strings.SplitN(address, ";", 2)[0])
		address = strings.Trim(address, "\"")
		_, portText, err := net.SplitHostPort(address)
		if err != nil {
			if strings.HasPrefix(address, ":") {
				portText = strings.TrimPrefix(address, ":")
			} else {
				return 0, false, fmt.Errorf("invalid HTTP/3 Alt-Svc address %q", address)
			}
		}
		port, err := strconv.Atoi(portText)
		if err != nil || port < 1 || port > 65535 {
			return 0, false, fmt.Errorf("invalid HTTP/3 Alt-Svc port %q", portText)
		}
		return port, true, nil
	}
	return 0, false, nil
}

func probeStaticEdgeHTTP3(ctx context.Context, ip, host string, port, status int, edgeID, path string, timeout time.Duration) error {
	address := net.JoinHostPort(ip, strconv.Itoa(port))
	transport := &http3.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host, NextProtos: []string{"h3"}},
		Dial: func(ctx context.Context, _ string, tlsConfig *tls.Config, quicConfig *quic.Config) (*quic.Conn, error) {
			config := tlsConfig.Clone()
			config.ServerName = host
			config.NextProtos = []string{"h3"}
			return quic.DialAddrEarly(ctx, address, config, quicConfig)
		},
	}
	defer transport.Close()
	client := &http.Client{Transport: transport, Timeout: timeout}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+host+path, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("HTTP/3 probe %s via %s:%d: %w", host, ip, port, err)
	}
	defer response.Body.Close()
	n, err := io.Copy(io.Discard, io.LimitReader(response.Body, 65537))
	if err != nil {
		return err
	}
	if n > 65536 {
		return errors.New("HTTP/3 probe response exceeds 64KiB")
	}
	if response.StatusCode != status {
		return fmt.Errorf("HTTP/3 probe %s via %s:%d returned HTTP %d, expected %d", host, ip, port, response.StatusCode, status)
	}
	if edgeID != "" && response.Header.Get("X-Fugue-Static-Edge") != edgeID {
		return fmt.Errorf("HTTP/3 probe reached edge identity %q, expected %q", response.Header.Get("X-Fugue-Static-Edge"), edgeID)
	}
	return nil
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
 print(json.dumps({"status":response.status,"edge_id":response.getheader("X-Fugue-Static-Edge", ""),"peer_ip":peer,"body_bytes":len(body),"alt_svc":", ".join(response.headers.get_all("Alt-Svc", []))}))
`
