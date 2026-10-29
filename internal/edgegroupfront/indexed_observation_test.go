package edgegroupfront

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/tcpdiag"
)

type indexedObservationConn struct {
	net.Conn
	peer net.Addr
}

func (c indexedObservationConn) RemoteAddr() net.Addr { return c.peer }

func indexedObservationFixture(t testing.TB) (*Service, frontnetwork.Request, net.Conn, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	peer := indexedObservationConn{Conn: a, peer: &net.TCPAddr{IP: net.ParseIP("203.0.113.5"), Port: 41000}}
	service := &Service{Config: Config{EdgeID: "edge-a", EdgeGroupID: "group-a"}}
	service.startTCPConnection(ProtocolHTTPS, "b", "worker", peer, b, time.Now().Add(-time.Second), true)
	query := frontnetwork.Request{Schema: frontnetwork.Schema, Nonce: strings.Repeat("a", 32), EdgeID: "edge-a", GroupID: "group-a", Slot: "b", RemoteAddr: peer.RemoteAddr().String()}
	return service, query, a, b
}

func TestIndexedFrontObservationRejectsUntrustedOrAmbiguousIdentity(t *testing.T) {
	for _, scenario := range []string{"wrong_edge", "wrong_group", "wrong_slot", "wrong_port", "private_peer", "duplicate", "closed", "no_proxy", "http", "future", "missing_index", "stale_index"} {
		t.Run(scenario, func(t *testing.T) {
			service, query, _, upstream := indexedObservationFixture(t)
			var connection edgeFrontActiveTCPConnection
			for _, value := range service.active {
				connection = value
			}
			switch scenario {
			case "wrong_edge":
				query.EdgeID = "other"
			case "wrong_group":
				query.GroupID = "other"
			case "wrong_slot":
				query.Slot = "a"
			case "wrong_port":
				query.RemoteAddr = "203.0.113.5:41001"
			case "private_peer":
				query.RemoteAddr = "10.0.0.5:41000"
			case "duplicate":
				service.startTCPConnection(ProtocolHTTPS, "b", "worker", connection.Downstream, upstream, time.Now().Add(-time.Second), true)
			case "closed":
				service.finishTCPConnection(connection.ID)
			case "no_proxy":
				connection.ProxyProtocol = false
				service.active[connection.ID] = connection
			case "http":
				connection.Protocol = ProtocolHTTP
				service.active[connection.ID] = connection
			case "future":
				connection.StartedAt = time.Now().Add(time.Hour)
				service.active[connection.ID] = connection
			case "missing_index":
				service.connectionIndex = nil
			case "stale_index":
				delete(service.active, connection.ID)
			}
			writer := httptest.NewRecorder()
			service.handleIndexedTCPConnection(writer, indexedObservationRequest(query), func(net.Conn) tcpdiag.Snapshot {
				t.Fatal("untrusted or ambiguous identity reached kernel sampling")
				return tcpdiag.Snapshot{}
			})
			if writer.Code != http.StatusBadRequest && writer.Code != http.StatusNotFound {
				t.Fatal(writer.Code, writer.Body.String())
			}
		})
	}
}

func TestIndexedFrontObservationBoundedQueriesAndRate(t *testing.T) {
	for _, suffix := range []string{"?request=", "?unexpected=x", "?request=%zz", "?request={}", "?request=%7B%22unknown%22:true%7D", "?request=" + strings.Repeat("x", 2049), "?request=" + strings.Repeat("x", 8193)} {
		t.Run(fmt.Sprint(len(suffix), suffix[:min(len(suffix), 25)]), func(t *testing.T) {
			service, _, _, _ := indexedObservationFixture(t)
			writer := httptest.NewRecorder()
			service.handleTCPConnections(writer, httptest.NewRequest(http.MethodGet, "/edge/tcp-connections"+suffix, nil))
			if writer.Code != http.StatusBadRequest || writer.Header().Get("Cache-Control") != "no-store" {
				t.Fatal(writer.Code)
			}
		})
	}
	for _, edit := range []func(*http.Request){
		func(r *http.Request) { r.Method = http.MethodPost },
		func(r *http.Request) { r.URL.RawQuery += "&request={}" },
		func(r *http.Request) { r.URL.RawQuery += "&other=x" },
	} {
		service, query, _, _ := indexedObservationFixture(t)
		r := indexedObservationRequest(query)
		edit(r)
		writer := httptest.NewRecorder()
		service.handleTCPConnections(writer, r)
		if writer.Code != http.StatusBadRequest {
			t.Fatal(writer.Code)
		}
	}
	service, query, _, _ := indexedObservationFixture(t)
	for _, status := range []int{http.StatusOK, http.StatusTooManyRequests} {
		writer := httptest.NewRecorder()
		service.handleTCPConnections(writer, indexedObservationRequest(query))
		if writer.Code != status {
			t.Fatal(writer.Code, writer.Body.String())
		}
	}
	service.networkLast = time.Time{}
	service.mu.Lock()
	writer := httptest.NewRecorder()
	service.handleTCPConnections(writer, indexedObservationRequest(query))
	service.mu.Unlock()
	if writer.Code != http.StatusServiceUnavailable {
		t.Fatal(writer.Code)
	}
}

func TestIndexedFrontObservationLifecycleAndEstablishedState(t *testing.T) {
	for _, scenario := range []string{"established", "unavailable", "closing", "finished_during_sample", "replaced_during_sample"} {
		t.Run(scenario, func(t *testing.T) {
			service, query, _, upstream := indexedObservationFixture(t)
			var connection edgeFrontActiveTCPConnection
			for _, c := range service.active {
				connection = c
			}
			writer := httptest.NewRecorder()
			reads := 0
			service.handleIndexedTCPConnection(writer, indexedObservationRequest(query), func(c net.Conn) tcpdiag.Snapshot {
				reads++
				if c != connection.Downstream {
					t.Fatal("unrelated kernel socket sampled")
				}
				switch scenario {
				case "closing":
					return tcpdiag.Snapshot{Available: true, State: 5}
				case "finished_during_sample":
					service.finishTCPConnection(connection.ID)
				case "replaced_during_sample":
					service.finishTCPConnection(connection.ID)
					service.startTCPConnection(ProtocolHTTPS, "b", "worker", c, upstream, time.Now().Add(-time.Second), true)
				case "unavailable":
					return tcpdiag.Snapshot{}
				}
				return tcpdiag.Snapshot{Available: true, State: 1, RTTUsec: 15000, MinRTTUsec: 12000}
			})
			want := http.StatusOK
			if scenario == "closing" {
				want = http.StatusServiceUnavailable
			}
			if strings.Contains(scenario, "during_sample") {
				want = http.StatusNotFound
			}
			if reads != 1 || writer.Code != want {
				t.Fatal(reads, writer.Code, writer.Body.String())
			}
			if writer.Code == http.StatusOK {
				var result frontnetwork.Response
				if json.Unmarshal(writer.Body.Bytes(), &result) != nil {
					t.Fatal(writer.Body.String())
				}
				if scenario == "unavailable" && (result.Sample.TCPInfoAvailable || result.Sample.RTTMS != nil) {
					t.Fatal("fabricated TCP_INFO")
				}
				if scenario == "established" && (result.Sample.RTTMS == nil || *result.Sample.RTTMS != 15) {
					t.Fatal(result.Sample)
				}
			}
			for id := range service.active {
				service.finishTCPConnection(id)
			}
			if len(service.connectionIndex) != 0 || len(service.active) != 0 {
				t.Fatal("lifecycle index leaked")
			}
		})
	}
}

func TestIndexedFrontObservationIPv6AndDuplicateCleanup(t *testing.T) {
	service, query, a, b := indexedObservationFixture(t)
	for id := range service.active {
		service.finishTCPConnection(id)
	}
	peer := indexedObservationConn{Conn: a, peer: &net.TCPAddr{IP: net.ParseIP("2001:db8::5"), Port: 41000}}
	one := service.startTCPConnection(ProtocolHTTPS, "b", "worker", peer, b, time.Now().Add(-time.Second), true)
	two := service.startTCPConnection(ProtocolHTTPS, "b", "worker", peer, b, time.Now().Add(-time.Second), true)
	query.RemoteAddr = "[2001:0db8:0000:0000:0000:0000:0000:0005]:41000"
	writer := httptest.NewRecorder()
	service.handleTCPConnections(writer, indexedObservationRequest(query))
	if writer.Code != http.StatusNotFound {
		t.Fatal(writer.Code)
	}
	service.finishTCPConnection(two)
	service.networkLast = time.Time{}
	writer = httptest.NewRecorder()
	service.handleTCPConnections(writer, indexedObservationRequest(query))
	if writer.Code != http.StatusOK {
		t.Fatal(writer.Code, writer.Body.String())
	}
	service.finishTCPConnection(one)
	if len(service.connectionIndex) != 0 {
		t.Fatal("duplicate key not removed")
	}
}

func TestIndexedFrontObservationConcurrentLifecycle(t *testing.T) {
	service, query, a, b := indexedObservationFixture(t)
	private := indexedObservationConn{Conn: a, peer: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 41000}}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				id := service.startTCPConnection(ProtocolHTTPS, "a", "worker", private, b, time.Now().Add(-time.Second), true)
				writer := httptest.NewRecorder()
				service.handleTCPConnections(writer, indexedObservationRequest(query))
				if writer.Code != http.StatusOK && writer.Code != http.StatusTooManyRequests && writer.Code != http.StatusServiceUnavailable {
					t.Error(writer.Code)
				}
				service.finishTCPConnection(id)
			}
		}()
	}
	wg.Wait()
	if len(service.active) != 1 || len(service.connectionIndex) != 1 {
		t.Fatal("concurrent lifecycle leak")
	}
}

func BenchmarkFrontIndexedVersusInventory(b *testing.B) {
	for _, n := range []int{8, 2048, 32768} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			service, query, a, upstream := indexedObservationFixture(b)
			private := indexedObservationConn{Conn: a, peer: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 41000}}
			for i := 0; i < n; i++ {
				service.startTCPConnection(ProtocolHTTPS, "a", "worker", private, upstream, time.Now().Add(-time.Second), true)
			}
			peer, _ := frontnetwork.ParseRemote(query.RemoteAddr)
			key := connectionObservationKey{peer: peer, slot: query.Slot}
			b.Run("indexed", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, ok := service.indexedTCPConnection(key); !ok {
						b.Fatal("missing exact peer")
					}
				}
			})
			b.Run("legacy_inventory", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if len(service.activeTCPConnections()) != n+1 {
						b.Fatal("inventory changed")
					}
				}
			})
		})
	}
}

func indexedObservationRequest(query frontnetwork.Request) *http.Request {
	raw, _ := json.Marshal(query)
	return httptest.NewRequest(http.MethodGet, "/edge/tcp-connections?"+url.Values{"request": {string(raw)}}.Encode(), nil)
}

func TestIndexedFrontObservationUnaffectedByUnrelatedConnections(t *testing.T) {
	service, query, a, b := indexedObservationFixture(t)
	private := indexedObservationConn{Conn: a, peer: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 41000}}
	for i := 0; i < 2048; i++ {
		service.startTCPConnection(ProtocolHTTPS, "a", "worker", private, b, time.Now().Add(-time.Second), true)
	}
	writer := httptest.NewRecorder()
	service.handleTCPConnections(writer, indexedObservationRequest(query))
	var result frontnetwork.Response
	if writer.Code != http.StatusOK || json.Unmarshal(writer.Body.Bytes(), &result) != nil || result.Schema != query.Schema || result.Nonce != query.Nonce || result.EdgeID != query.EdgeID || result.GroupID != query.GroupID || result.Sample.Slot != query.Slot || result.Sample.ConnectionID == "" {
		t.Fatalf("exact observation must not depend on unrelated inventory size: status=%d bytes=%d schema=%q", writer.Code, writer.Body.Len(), result.Schema)
	}
	if writer.Body.Len() > 4096 || strings.Contains(writer.Body.String(), query.RemoteAddr) || strings.Contains(writer.Body.String(), "10.0.0.5") {
		t.Fatal("unbounded or raw peer inventory returned")
	}
}
