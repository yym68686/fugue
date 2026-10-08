package edgegroupfront

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/tcpdiag"
)

func frontObservationFixture(t *testing.T) (*Service, Config, frontnetwork.Request) {
	t.Helper()
	downstream, other := net.Pipe()
	t.Cleanup(func() { downstream.Close(); other.Close() })
	connection := edgeFrontActiveTCPConnection{ID: "connection-a", Protocol: ProtocolHTTPS, Slot: "b", ProxyProtocol: true, DownstreamRemote: "203.0.113.5:41000", Downstream: downstream, StartedAt: time.Now().Add(-time.Second)}
	return &Service{active: map[string]edgeFrontActiveTCPConnection{connection.ID: connection}}, Config{EdgeID: "edge-a", EdgeGroupID: "group-a"},
		frontnetwork.Request{Schema: frontnetwork.Schema, Nonce: strings.Repeat("a", 32), EdgeID: "edge-a", GroupID: "group-a", Slot: "b", RemoteAddr: connection.DownstreamRemote}
}

func TestPublicFrontObservationExactSocketOnly(t *testing.T) {
	service, cfg, query := frontObservationFixture(t)
	raw, _ := json.Marshal(query)
	writer := httptest.NewRecorder()
	reads := 0
	service.handleNetworkObservation(writer, httptest.NewRequest(http.MethodPost, frontnetwork.Path, bytes.NewReader(raw)), cfg, func(connection net.Conn) tcpdiag.Snapshot {
		reads++
		if connection != service.active["connection-a"].Downstream {
			t.Fatal("queried an internal or unrelated connection")
		}
		return tcpdiag.Snapshot{Available: true, RTTUsec: 185000, MinRTTUsec: 180000, RTTVarUsec: 4500, SegsOut: 100, TotalRetrans: 3}
	})
	var result frontnetwork.Response
	if writer.Code != http.StatusOK || json.Unmarshal(writer.Body.Bytes(), &result) != nil || reads != 1 || result.Nonce != query.Nonce || result.Sample.RTTMS == nil || *result.Sample.RTTMS != 185 || result.Sample.Scope != "tcp_peer:203.0.113.0/24" {
		t.Fatal(writer.Code, writer.Body.String(), reads)
	}
	if strings.Contains(writer.Body.String(), "203.0.113.5") || strings.Contains(writer.Body.String(), "41000") {
		t.Fatal("raw peer endpoint leaked into retained evidence")
	}
}

func TestPublicFrontObservationRejectsAmbiguousOrUntrustedConnections(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Service, *frontnetwork.Request)
	}{
		{"wrong_edge", func(_ *Service, query *frontnetwork.Request) { query.EdgeID = "other" }},
		{"wrong_group", func(_ *Service, query *frontnetwork.Request) { query.GroupID = "other" }},
		{"wrong_slot", func(_ *Service, query *frontnetwork.Request) { query.Slot = "a" }},
		{"wrong_peer", func(_ *Service, query *frontnetwork.Request) { query.RemoteAddr = "203.0.113.6:41000" }},
		{"private_peer", func(_ *Service, query *frontnetwork.Request) { query.RemoteAddr = "127.0.0.1:41000" }},
		{"closed", func(service *Service, _ *frontnetwork.Request) { service.active = nil }},
		{"duplicate", func(service *Service, _ *frontnetwork.Request) {
			service.active["duplicate"] = service.active["connection-a"]
		}},
		{"no_proxy_identity", func(service *Service, _ *frontnetwork.Request) {
			connection := service.active["connection-a"]
			connection.ProxyProtocol = false
			service.active["connection-a"] = connection
		}},
		{"http", func(service *Service, _ *frontnetwork.Request) {
			connection := service.active["connection-a"]
			connection.Protocol = ProtocolHTTP
			service.active["connection-a"] = connection
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, cfg, query := frontObservationFixture(t)
			test.edit(service, &query)
			raw, _ := json.Marshal(query)
			writer := httptest.NewRecorder()
			service.handleNetworkObservation(writer, httptest.NewRequest(http.MethodPost, frontnetwork.Path, bytes.NewReader(raw)), cfg, func(net.Conn) tcpdiag.Snapshot {
				t.Fatal("invalid identity reached TCP sampling")
				return tcpdiag.Snapshot{}
			})
			if writer.Code == http.StatusOK {
				t.Fatal(writer.Body.String())
			}
		})
	}
}

func TestPublicFrontObservationMissingTCPInfoAndIPv6(t *testing.T) {
	service, _, _ := frontObservationFixture(t)
	connection := service.active["connection-a"]
	connection.DownstreamRemote = "[2001:db8:1234:5::6]:41000"
	sample := publicClientNetworkSample(connection, tcpdiag.Snapshot{}, time.Now())
	if sample.RTTMS != nil || sample.TCPInfoAvailable || sample.Scope != "tcp_peer:2001:db8:1234::/48" {
		t.Fatal(sample)
	}
}

func TestPublicFrontObservationRateAndContentBounds(t *testing.T) {
	service, cfg, query := frontObservationFixture(t)
	raw, _ := json.Marshal(query)
	for index := 0; index < 2; index++ {
		writer := httptest.NewRecorder()
		service.handleNetworkObservation(writer, httptest.NewRequest(http.MethodPost, frontnetwork.Path, bytes.NewReader(raw)), cfg, func(net.Conn) tcpdiag.Snapshot { return tcpdiag.Snapshot{} })
		want := http.StatusOK
		if index == 1 {
			want = http.StatusTooManyRequests
		}
		if writer.Code != want {
			t.Fatal(writer.Code, writer.Body.String())
		}
	}
	for _, payload := range []string{strings.Repeat("x", 2049), string(raw) + `{}`, `{"unexpected":true}`} {
		writer := httptest.NewRecorder()
		service.handleNetworkObservation(writer, httptest.NewRequest(http.MethodPost, frontnetwork.Path, strings.NewReader(payload)), cfg, func(net.Conn) tcpdiag.Snapshot {
			t.Fatal("malformed query sampled connection")
			return tcpdiag.Snapshot{}
		})
		if writer.Code != http.StatusBadRequest {
			t.Fatal(writer.Code)
		}
	}
}

func TestPublicFrontObservationDoesNotScanUnboundedConnections(t *testing.T) {
	service, cfg, query := frontObservationFixture(t)
	for index := 0; index < 1025; index++ {
		service.active[fmt.Sprintf("connection-%d", index)] = edgeFrontActiveTCPConnection{}
	}
	raw, _ := json.Marshal(query)
	writer := httptest.NewRecorder()
	service.handleNetworkObservation(writer, httptest.NewRequest(http.MethodPost, frontnetwork.Path, bytes.NewReader(raw)), cfg, func(net.Conn) tcpdiag.Snapshot {
		t.Fatal("over-budget request sampled socket")
		return tcpdiag.Snapshot{}
	})
	if writer.Code != http.StatusServiceUnavailable {
		t.Fatal(writer.Code)
	}
}

func TestPublicFrontUnixObservationPermissionsAndNoClobber(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "front-network-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	service, cfg, query := frontObservationFixture(t)
	cfg.NetworkSocket = filepath.Join(directory, "network.sock")
	stop, err := service.startNetworkObservation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stop(context.Background()) })
	info, err := os.Stat(cfg.NetworkSocket)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	result, err := frontnetwork.Read(context.Background(), cfg.NetworkSocket, query.EdgeID, query.GroupID, query.Slot, query.RemoteAddr)
	if err != nil || result.Sample.ConnectionID != "connection-a" || result.Sample.TCPInfoAvailable {
		t.Fatal(result, err)
	}
	if _, err := service.startNetworkObservation(cfg); err == nil {
		t.Fatal("second listener replaced serving socket")
	}
	if err := stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cfg.NetworkSocket); !os.IsNotExist(err) {
		t.Fatal("closed socket was retained", err)
	}
	if err := os.WriteFile(cfg.NetworkSocket, []byte("owned"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := service.startNetworkObservation(cfg); err == nil {
		t.Fatal("regular file was overwritten")
	}
	file, _ := os.Open(cfg.NetworkSocket)
	defer file.Close()
	contents, _ := io.ReadAll(file)
	if string(contents) != "owned" {
		t.Fatal("unrelated state was modified")
	}
}

func TestPublicFrontUnixObservationRecoversOwnedStaleSocket(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "front-stale-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	service, cfg, _ := frontObservationFixture(t)
	cfg.NetworkSocket = filepath.Join(directory, "network.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: cfg.NetworkSocket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	stale.Close()
	stop, err := service.startNetworkObservation(cfg)
	if err != nil {
		t.Fatal("owned stale socket prevented optional observer recovery", err)
	}
	t.Cleanup(func() { stop(context.Background()) })
	if _, err := service.startNetworkObservation(cfg); err == nil {
		t.Fatal("second observer replaced active lock owner")
	}
}
