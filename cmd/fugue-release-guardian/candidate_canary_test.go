package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/releaseguardian"
)

func TestParseCandidateCanaryProbesIsExactAndLoadsIndependentKey(t *testing.T) {
	directory := t.TempDir()
	keyPath := filepath.Join(directory, "canary-key")
	if err := os.WriteFile(keyPath, []byte("independent-candidate-canary-signing-key-material"), 0o600); err != nil {
		t.Fatal(err)
	}
	value := strings.Join([]string{
		"edge-pool-a", "192.0.2.1:18443", "192.0.2.1:28443", "canary.fugue.pro", "/route-canary",
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "10", "candidate-canary-v1", keyPath,
	}, ",")
	probes, err := parseCandidateCanaryProbes(value)
	if err != nil {
		t.Fatal(err)
	}
	if len(probes) != 1 || probes[0].GroupID != "edge-pool-a" || probes[0].Interval != 10*time.Second ||
		probes[0].SlotAddresses[releaseguardian.AuthoritySlotA] != "192.0.2.1:18443" || len(probes[0].SigningKey) < 32 {
		t.Fatalf("candidate canary config is not exact: %+v", probes)
	}
}

func TestParseCandidateCanaryProbesRejectsSelectorAndSharedAddress(t *testing.T) {
	directory := t.TempDir()
	keyPath := filepath.Join(directory, "canary-key")
	if err := os.WriteFile(keyPath, []byte("independent-candidate-canary-signing-key-material"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := "edge-pool-a,192.0.2.1:18443,192.0.2.1:28443,canary.fugue.pro,/route-canary,sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,10,candidate-canary-v1," + keyPath
	for _, value := range []string{
		strings.Replace(base, "/route-canary", "/route-canary?slot=b", 1),
		strings.Replace(base, "192.0.2.1:28443", "192.0.2.1:18443", 1),
		strings.Replace(base, ",10,", ",30,", 1),
	} {
		if _, err := parseCandidateCanaryProbes(value); err == nil {
			t.Fatalf("invalid candidate selector was accepted: %s", value)
		}
	}
}

func TestCandidateCanaryConfigurationIsOptionalUntilAuthorityPublisherExists(t *testing.T) {
	probes, err := parseCandidateCanaryProbes("")
	if err != nil || len(probes) != 0 {
		t.Fatalf("empty candidate canary config did not remain dormant: probes=%v err=%v", probes, err)
	}
}

func TestCandidateRouteUsesFrontCompatibleProxyPreambleBeforeTLS(t *testing.T) {
	template := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := template.TLS.Certificates[0]
	previousRoots := candidateTLSRootCAs
	candidateTLSRootCAs = template.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	defer func() { candidateTLSRootCAs = previousRoots }()
	template.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	preamble := make(chan string, 1)
	serverErr := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer connection.Close()
		reader := bufio.NewReader(connection)
		line, err := reader.ReadString('\n')
		if err != nil {
			serverErr <- err
			return
		}
		preamble <- line
		wrapped := &bufferedConn{Conn: connection, reader: reader}
		tlsConnection := tls.Server(wrapped, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
		if err := tlsConnection.Handshake(); err != nil {
			serverErr <- err
			return
		}
		request, err := http.ReadRequest(bufio.NewReader(tlsConnection))
		if err != nil {
			serverErr <- err
			return
		}
		if request.Host != "example.com" || request.URL.Path != "/route-canary" {
			serverErr <- fmt.Errorf("unexpected candidate request %s %s", request.Host, request.URL.Path)
			return
		}
		_, err = fmt.Fprint(tlsConnection, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Fugue-Candidate-Record-Digest: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\nConnection: close\r\n\r\nok")
		serverErr <- err
	}()
	status, body, headers, err := requestCandidateRoute(context.Background(), listener.Addr().String(), "example.com", "/route-canary")
	if err != nil || status != http.StatusOK || string(body) != "ok" {
		t.Fatalf("status=%d body=%q err=%v", status, body, err)
	}
	if headers.Get("X-Fugue-Candidate-Record-Digest") == "" {
		t.Fatal("candidate response attestation header was lost")
	}
	if line := <-preamble; !strings.HasPrefix(line, "PROXY TCP4 ") {
		t.Fatalf("missing exact PROXY preamble: %q", line)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (connection *bufferedConn) Read(value []byte) (int, error) {
	return connection.reader.Read(value)
}
