package edgegroupfront

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/tcpdiag"
)

type connectionObservationKey struct {
	peer netip.AddrPort
	slot string
}

func observationKey(connection edgeFrontActiveTCPConnection) (connectionObservationKey, bool) {
	if connection.Protocol != ProtocolHTTPS || !connection.ProxyProtocol || connection.Downstream == nil {
		return connectionObservationKey{}, false
	}
	peer, err := frontnetwork.ParseRemote(connection.DownstreamRemote)
	return connectionObservationKey{peer: peer, slot: connection.Slot}, err == nil
}

// The connection lifecycle holds s.mu for both maps. No observation rebuilds
// this index by scanning the active inventory, including when the index is nil.
func (s *Service) indexTCPConnection(connection edgeFrontActiveTCPConnection) {
	key, eligible := observationKey(connection)
	if !eligible {
		return
	}
	if s.connectionIndex == nil {
		s.connectionIndex = make(map[connectionObservationKey]map[string]struct{})
	}
	if s.connectionIndex[key] == nil {
		s.connectionIndex[key] = make(map[string]struct{})
	}
	s.connectionIndex[key][connection.ID] = struct{}{}
}

func (s *Service) unindexTCPConnection(connection edgeFrontActiveTCPConnection) {
	key, eligible := observationKey(connection)
	if !eligible {
		return
	}
	delete(s.connectionIndex[key], connection.ID)
	if len(s.connectionIndex[key]) == 0 {
		delete(s.connectionIndex, key)
	}
}

// Called with s.mu held. A duplicate key fails closed without iterating the
// duplicate set; absence or an inconsistent lifecycle index never falls back.
func (s *Service) indexedTCPConnection(key connectionObservationKey) (edgeFrontActiveTCPConnection, bool) {
	ids := s.connectionIndex[key]
	if len(ids) != 1 {
		return edgeFrontActiveTCPConnection{}, false
	}
	for id := range ids {
		connection, found := s.active[id]
		actual, eligible := observationKey(connection)
		return connection, found && connection.ID == id && eligible && actual == key
	}
	return edgeFrontActiveTCPConnection{}, false
}

func (s *Service) handleIndexedTCPConnection(writer http.ResponseWriter, request *http.Request, read func(net.Conn) tcpdiag.Snapshot) {
	writer.Header().Set("Cache-Control", "no-store")
	if request.Method != http.MethodGet || request.URL.Path != "/edge/tcp-connections" || len(request.URL.RawQuery) > 8192 {
		http.Error(writer, "invalid bounded indexed observation", http.StatusBadRequest)
		return
	}
	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil || len(values) != 1 || len(values["request"]) != 1 || len(values["request"][0]) > 2048 {
		http.Error(writer, "invalid bounded indexed query", http.StatusBadRequest)
		return
	}
	raw := []byte(values["request"][0])
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var query frontnetwork.Request
	if !json.Valid(raw) || decoder.Decode(&query) != nil || frontnetwork.ValidateRequest(query) != nil || s == nil || query.EdgeID != s.Config.EdgeID || query.GroupID != s.Config.EdgeGroupID {
		http.Error(writer, "indexed observation identity mismatch", http.StatusBadRequest)
		return
	}
	peer, _ := frontnetwork.ParseRemote(query.RemoteAddr)
	key := connectionObservationKey{peer: peer, slot: query.Slot}
	now := time.Now().UTC()
	if !s.mu.TryLock() {
		http.Error(writer, "observation busy", http.StatusServiceUnavailable)
		return
	}
	if now.Sub(s.networkLast) < 100*time.Millisecond {
		s.mu.Unlock()
		http.Error(writer, "observation rate limited", http.StatusTooManyRequests)
		return
	}
	s.networkLast = now
	connection, found := s.indexedTCPConnection(key)
	s.mu.Unlock()
	if !found || connection.StartedAt.After(now) {
		http.Error(writer, "exact indexed connection unavailable", http.StatusNotFound)
		return
	}
	// One kernel snapshot only; unrelated sockets are never inspected. Keep the
	// legacy API's established-state requirement and recheck lifecycle ownership
	// after sampling so a completed/replaced connection cannot be retained.
	network := read(connection.Downstream)
	if network.Available && network.State != 1 {
		http.Error(writer, "public TCP socket is not established", http.StatusServiceUnavailable)
		return
	}
	if !s.mu.TryLock() {
		http.Error(writer, "observation recheck busy", http.StatusServiceUnavailable)
		return
	}
	current, found := s.indexedTCPConnection(key)
	s.mu.Unlock()
	if !found || current.ID != connection.ID || !current.StartedAt.Equal(connection.StartedAt) {
		http.Error(writer, "indexed connection changed during observation", http.StatusNotFound)
		return
	}
	sample := publicClientNetworkSample(connection, network, now)
	if model.ValidateEdgeClientNetworkSample(&sample) != nil {
		http.Error(writer, "indexed connection observation invalid", http.StatusServiceUnavailable)
		return
	}
	payload, err := json.Marshal(frontnetwork.Response{Schema: frontnetwork.Schema, Nonce: query.Nonce, EdgeID: query.EdgeID, GroupID: query.GroupID, Sample: sample})
	if err != nil || len(payload) > 4096 {
		http.Error(writer, "indexed connection response exceeds bound", http.StatusServiceUnavailable)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(payload)
}
