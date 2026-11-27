package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"reflect"
	"strings"
	"time"

	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
)

// nodeWorkerTLS owns one socket for its entire lifetime. Each request uses a
// persistent HTTP/1.1 reader, with a fresh nonce for positive route proofs.
type nodeWorkerTLS struct {
	conn        *tls.Conn
	source      netip.AddrPort
	destination netip.AddrPort
	host        string
	reader      *bufio.Reader
	budget      *io.LimitedReader
}

func openNodeWorkerTLS(ctx context.Context, target netip.AddrPort, host string, roots *x509.CertPool) (*nodeWorkerTLS, error) {
	if !target.IsValid() || target.Port() == 0 || target.Addr().Zone() != "" || target.Addr().IsUnspecified() ||
		target.Addr().IsMulticast() || host == "" || strings.ContainsAny(host, "/:@?#\r\n \t") {
		return nil, errors.New("invalid Front TLS observation endpoint")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", target.String())
	if err != nil {
		return nil, err
	}
	source, err := netip.ParseAddrPort(raw.LocalAddr().String())
	if err != nil {
		raw.Close()
		return nil, err
	}
	c := tls.Client(raw, &tls.Config{
		MinVersion: tls.VersionTLS12, ServerName: host, RootCAs: roots, NextProtos: []string{"http/1.1"},
	})
	if err := c.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, err
	}
	limited := &io.LimitedReader{R: c}
	return &nodeWorkerTLS{conn: c, source: source, destination: target, host: host,
		reader: bufio.NewReader(limited), budget: limited}, nil
}

func (s *nodeWorkerTLS) close() {
	_ = s.conn.NetConn().Close()
}

func (s *nodeWorkerTLS) request(ctx context.Context, method, path string, headers http.Header) (*http.Response, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	target, err := url.ParseRequestURI(path)
	if err != nil || !strings.HasPrefix(path, "/") || target.Host != "" || target.IsAbs() {
		return nil, nil, errors.New("invalid Front TLS canary path")
	}
	target.Scheme, target.Host = "https", s.host
	req, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header = headers
	deadline, _ := ctx.Deadline()
	if err := s.conn.SetDeadline(deadline); err != nil {
		return nil, nil, err
	}
	stop := context.AfterFunc(ctx, s.close)
	defer stop()
	// Bound wire reads, including response headers and chunk framing.
	const maxBody = 64 << 10
	s.budget.N = 2 * maxBody
	if err := req.Write(s.conn); err != nil {
		s.close()
		return nil, nil, err
	}
	resp, err := http.ReadResponse(s.reader, req)
	if err != nil {
		s.close()
		return nil, nil, err
	}
	if resp.ProtoMajor != 1 || resp.ProtoMinor != 1 || resp.Close {
		s.close()
		_ = resp.Body.Close()
		return nil, nil, errors.New("Front did not keep the original TLS socket open")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(body) > maxBody || ctx.Err() != nil {
		s.close()
		_ = resp.Body.Close()
		return nil, nil, errors.Join(errors.New("Front TLS response incomplete or oversized"), err, ctx.Err())
	}
	if err := resp.Body.Close(); err != nil {
		s.close()
		return nil, nil, err
	}
	return resp, body, nil
}

func (s *nodeWorkerTLS) proof(ctx context.Context, path, node, group string) (routeprobe.Proof, error) {
	var challenge [16]byte
	if _, err := rand.Read(challenge[:]); err != nil {
		return routeprobe.Proof{}, err
	}
	nonce := hex.EncodeToString(challenge[:])
	headers := make(http.Header)
	headers.Set(routeproof.RequestHeader, "1")
	headers.Set(routeproof.NonceHeader, nonce)
	resp, body, err := s.request(ctx, http.MethodHead, path, headers)
	if err != nil {
		return routeprobe.Proof{}, err
	}
	now := time.Now().UTC()
	proof, err := routeprobe.ParseResponse(resp, nonce, now)
	if err != nil || len(body) != 0 || proof.State != "" || proof.TrafficRelease == nil || proof.EdgeID != node || proof.GroupID != group ||
		len(resp.Header.Values("Cache-Control")) != 1 {
		return routeprobe.Proof{}, errors.Join(errors.New("Front TLS socket lacks a fresh positive route proof for the exact node"), err)
	}
	chains := s.conn.ConnectionState().VerifiedChains
	if len(chains) == 0 {
		return routeprobe.Proof{}, errors.New("Front TLS socket lacks a verified certificate chain")
	}
	for _, cert := range chains[0] {
		if now.Before(cert.NotBefore) {
			return routeprobe.Proof{}, errors.New("Front TLS certificate is not yet valid")
		}
		if cert.NotAfter.Before(proof.ValidUntil) {
			proof.ValidUntil = cert.NotAfter
		}
	}
	if !proof.ValidUntil.After(now.Add(15 * time.Second)) {
		return routeprobe.Proof{}, errors.New("Front TLS route proof expires too soon")
	}
	proof.CheckedAt = now
	return proof, nil
}

func (s *nodeWorkerTLS) serving(ctx context.Context, path, expectedDigest string) error {
	resp, body, err := s.request(ctx, http.MethodGet, path, http.Header{"Accept-Encoding": {"identity"}})
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK || len(body) == 0 || shaDigest(body) != expectedDigest {
		return errors.New("Front TLS socket did not serve the exact route canary")
	}
	return nil
}

func sameNodeWorkerProof(a, b routeprobe.Proof) bool {
	return a.Digest == b.Digest && a.Version == b.Version && a.EdgeID == b.EdgeID && a.GroupID == b.GroupID &&
		a.AppTrafficDigest == b.AppTrafficDigest && reflect.DeepEqual(a.TrafficRelease, b.TrafficRelease)
}
