package edgegroupfront

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/tcpdiag"
)

type passiveTestConn struct {
	net.Conn
	addr net.Addr
}

func (c passiveTestConn) RemoteAddr() net.Addr { return c.addr }

func TestPassiveNetworkRecordsOnlyCoarsePublicPeersAndBoundsEachPrefix(t *testing.T) {
	cfg := Config{EdgeID: "node", EdgeGroupID: "edge-group", ProxyProtocol: true}
	s := NewService(cfg, nil)
	now := time.Now().UTC()
	makeConn := func(addr string) net.Conn {
		return passiveTestConn{addr: &net.TCPAddr{IP: net.ParseIP(addr), Port: 1234}}
	}
	for _, addr := range []string{"10.0.0.2", "100.64.0.9", "127.0.0.1", "fe80::1"} {
		if _, eligible := passiveConnection(cfg, ProtocolHTTPS, "a", "skip", makeConn(addr), now); eligible {
			t.Fatal("internal peer was measured", addr)
		}
	}
	snapshot := tcpdiag.Snapshot{Available: true, RTTUsec: 20000}
	for i := 0; i <= frontnetwork.PassivePrefixLimit; i++ {
		conn, ok := passiveConnection(cfg, ProtocolHTTPS, "a", "conn"+string(rune('a'+i)), makeConn("203.0.113.27"), now.Add(-time.Second))
		if !ok {
			t.Fatal("eligible public peer was rejected")
		}
		baseline := publicClientNetworkSample(conn, snapshot, now.Add(-time.Second))
		s.recordPassiveNetworkSample(conn, baseline, snapshot, now)
	}
	r := httptest.NewRecorder()
	s.handlePassiveNetworkSamples(r, httptest.NewRequest(http.MethodGet, frontnetwork.PassivePath, nil))
	var batch frontnetwork.PassiveBatch
	if r.Code != http.StatusOK || json.Unmarshal(r.Body.Bytes(), &batch) != nil || len(batch.Samples) != frontnetwork.PassivePrefixLimit {
		t.Fatal("incorrect bounded batch", r.Code, batch)
	}
	if strings.Contains(r.Body.String(), "203.0.113.27") || batch.Samples[0].Scope != "tcp_peer:203.0.113.0/24" {
		t.Fatal("exact client address escaped Front sampling")
	}
}
