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
	"fugue/internal/model"
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

func TestPassiveNetworkSamplesLongLivedConnectionWithoutIdleOrDuplicate(t *testing.T) {
	now := time.Now().UTC()
	cfg := Config{EdgeID: "node", EdgeGroupID: "group", ProxyProtocol: true}
	service := NewService(cfg, nil)
	conn, ok := passiveConnection(cfg, ProtocolHTTPS, "a", "live", passiveTestConn{
		addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.27"), Port: 1234},
	}, now.Add(-time.Minute))
	if !ok {
		t.Fatal("public socket wasn't eligible")
	}
	snap := tcpdiag.Snapshot{Available: true, RTTUsec: 20000, BytesReceived: 400, BytesSent: 500}
	service.trackPassiveConnection(conn, publicClientNetworkSample(conn, snap, now.Add(-20*time.Second)))
	read := func(net.Conn) tcpdiag.Snapshot { return snap }
	service.samplePassiveActive(read, func() time.Time { return now.Add(-10 * time.Second) })
	if service.passiveState.recorded.Load() != 0 {
		t.Fatal("idle connection was treated as new client traffic")
	}
	snap.BytesSent, snap.BytesAcked, snap.DataSegsOut, snap.BusyTimeUsec, snap.DeliveryRateBPS = 80500, 80000, 55, 90000, 2000000
	service.samplePassiveActive(read, func() time.Time { return now })
	service.samplePassiveActive(read, func() time.Time { return now.Add(time.Second) })
	if previous, sampled := service.finishPassiveConnection(conn.ID); previous.ObservedAt != now || !sampled || service.passiveState.recorded.Load() != 1 {
		t.Fatal("one live socket did not yield exactly one observation")
	}
	recorder := httptest.NewRecorder()
	service.handlePassiveNetworkSamples(recorder, httptest.NewRequest(http.MethodGet, frontnetwork.PassivePath+"?"+frontnetwork.PassiveStatsQuery, nil))
	var batch frontnetwork.PassiveBatch
	if json.Unmarshal(recorder.Body.Bytes(), &batch) != nil || len(batch.Samples) != 1 || batch.Stats == nil || batch.Stats.ActiveSamples != 1 {
		t.Fatalf("unexpected sampled batch: %+v", batch)
	}
	if dl, _ := model.EdgeClientDeliveryMetrics(&batch.Samples[0]); dl == nil || *dl <= 0 {
		t.Fatal("lost ACK delivery counters from the live sampling interval")
	}
	recorder = httptest.NewRecorder()
	clear := frontnetwork.PassiveBatch{}
	service.handlePassiveNetworkSamples(recorder, httptest.NewRequest(http.MethodGet, frontnetwork.PassivePath, nil))
	if json.Unmarshal(recorder.Body.Bytes(), &clear) != nil || clear.Stats != nil || len(clear.Samples) != 1 {
		t.Fatal("older API would not be able to read the original batch")
	}
}
