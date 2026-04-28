package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultCellHTTPReadHeaderTimeout = 5 * time.Second
	defaultCellHTTPShutdownTimeout   = 5 * time.Second
	defaultCellPeerProbeTimeout      = 750 * time.Millisecond
	defaultCellPeerProbeLimit        = 16
	defaultCellPeerProbePort         = 7831
)

func (s *AgentService) startCellHTTPServer(ctx context.Context) (func(context.Context) error, error) {
	addr := strings.TrimSpace(s.Config.CellListenAddr)
	if addr == "" {
		return nil, nil
	}
	if err := s.ensureCellStore(); err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen cell http %s: %w", addr, err)
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           s.cellHTTPHandler(),
		ReadHeaderTimeout: defaultCellHTTPReadHeaderTimeout,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultCellHTTPShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			s.logf("cell http shutdown failed: %v", err)
		}
	}()
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.logf("cell http server stopped: %v", err)
		}
	}()
	return server.Shutdown, nil
}

func (s *AgentService) cellHTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /cell/health", s.handleCellHealth)
	mux.HandleFunc("GET /cell/snapshot", s.handleCellSnapshot)
	mux.HandleFunc("GET /cell/routes", s.handleCellRoutes)
	mux.HandleFunc("GET /cell/peers", s.handleCellPeers)
	mux.HandleFunc("GET /cell/bundle", s.handleCellBundle)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !cellRemoteAllowed(r.RemoteAddr) {
			http.Error(w, "cell endpoint is only available from loopback, private, or mesh addresses", http.StatusForbidden)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *AgentService) handleCellHealth(w http.ResponseWriter, r *http.Request) {
	snapshot, ok, err := s.storedOrFreshCellSnapshot(r.Context(), false)
	if err != nil {
		writeCellError(w, http.StatusInternalServerError, err)
		return
	}
	status := "ok"
	if !ok {
		status = "warming"
	}
	writeCellJSON(w, http.StatusOK, map[string]any{
		"status":              status,
		"runtime_id":          strings.TrimSpace(s.Config.RuntimeID),
		"runtime_name":        strings.TrimSpace(s.Config.RuntimeName),
		"mesh":                snapshot.Mesh,
		"observed_at":         snapshot.ObservedAt,
		"route_count":         snapshot.RouteCount,
		"outbox_pending":      snapshot.OutboxPending,
		"peer_count":          len(snapshot.Peers),
		"observed_peer_count": snapshot.ObservedPeerCount,
	})
}

func (s *AgentService) handleCellSnapshot(w http.ResponseWriter, r *http.Request) {
	refresh := r.URL.Query().Get("refresh") == "1"
	snapshot, _, err := s.storedOrFreshCellSnapshot(r.Context(), refresh)
	if err != nil {
		writeCellError(w, http.StatusInternalServerError, err)
		return
	}
	writeCellJSON(w, http.StatusOK, snapshot)
}

func (s *AgentService) handleCellRoutes(w http.ResponseWriter, _ *http.Request) {
	if err := s.ensureCellStore(); err != nil {
		writeCellError(w, http.StatusInternalServerError, err)
		return
	}
	routes, err := s.CellStore.ListRoutes()
	if err != nil {
		writeCellError(w, http.StatusInternalServerError, err)
		return
	}
	writeCellJSON(w, http.StatusOK, map[string]any{"routes": routes})
}

func (s *AgentService) handleCellPeers(w http.ResponseWriter, _ *http.Request) {
	if err := s.ensureCellStore(); err != nil {
		writeCellError(w, http.StatusInternalServerError, err)
		return
	}
	peers, err := s.CellStore.ListPeerObservations()
	if err != nil {
		writeCellError(w, http.StatusInternalServerError, err)
		return
	}
	writeCellJSON(w, http.StatusOK, map[string]any{"peers": peers})
}

func (s *AgentService) handleCellBundle(w http.ResponseWriter, r *http.Request) {
	snapshot, _, err := s.storedOrFreshCellSnapshot(r.Context(), false)
	if err != nil {
		writeCellError(w, http.StatusInternalServerError, err)
		return
	}
	routes, err := s.CellStore.ListRoutes()
	if err != nil {
		writeCellError(w, http.StatusInternalServerError, err)
		return
	}
	peers, err := s.CellStore.ListPeerObservations()
	if err != nil {
		writeCellError(w, http.StatusInternalServerError, err)
		return
	}
	writeCellJSON(w, http.StatusOK, map[string]any{
		"snapshot": snapshot,
		"routes":   routes,
		"peers":    peers,
	})
}

func (s *AgentService) storedOrFreshCellSnapshot(ctx context.Context, refresh bool) (CellSnapshot, bool, error) {
	if err := s.ensureCellStore(); err != nil {
		return CellSnapshot{}, false, err
	}
	if !refresh {
		snapshot, ok, err := s.CellStore.Snapshot()
		if err != nil {
			return CellSnapshot{}, false, err
		}
		if ok {
			return snapshot, true, nil
		}
	}
	snapshot, err := s.RefreshCellSnapshot(ctx)
	if err != nil {
		return CellSnapshot{}, false, err
	}
	return snapshot, true, nil
}

func (s *AgentService) RefreshCellPeers(ctx context.Context, snapshot CellSnapshot) error {
	if !s.Config.CellPeerProbe {
		return nil
	}
	if err := s.ensureCellStore(); err != nil {
		return err
	}
	port := s.Config.CellPeerProbePort
	if port <= 0 {
		port = defaultCellPeerProbePort
	}
	selfIP := strings.TrimSpace(snapshot.Mesh.IP)
	client := http.Client{Timeout: defaultCellPeerProbeTimeout}
	probed := 0
	for _, peer := range snapshot.Peers {
		if probed >= defaultCellPeerProbeLimit {
			break
		}
		peer.IP = strings.TrimSpace(peer.IP)
		if peer.IP == "" || peer.IP == selfIP {
			continue
		}
		if !peer.Online {
			continue
		}
		probed++
		peerSnapshot, err := fetchCellPeerSnapshot(ctx, &client, peer, port)
		if err != nil {
			if saveErr := s.CellStore.SavePeerObservation(peer, nil, "cell-http", err.Error()); saveErr != nil {
				return saveErr
			}
			continue
		}
		if err := s.CellStore.SavePeerObservation(peer, &peerSnapshot, "cell-http", ""); err != nil {
			return err
		}
	}
	return nil
}

func fetchCellPeerSnapshot(ctx context.Context, client *http.Client, peer CellPeer, port int) (CellSnapshot, error) {
	probeCtx, cancel := context.WithTimeout(ctx, defaultCellPeerProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, cellPeerSnapshotURL(peer.IP, port), nil)
	if err != nil {
		return CellSnapshot{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return CellSnapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return CellSnapshot{}, fmt.Errorf("cell peer %s returned status %d", peer.IP, resp.StatusCode)
	}
	var snapshot CellSnapshot
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	if err := decoder.Decode(&snapshot); err != nil {
		return CellSnapshot{}, err
	}
	return snapshot, nil
}

func cellPeerSnapshotURL(ip string, port int) string {
	return "http://" + net.JoinHostPort(strings.TrimSpace(ip), strconv.Itoa(port)) + "/cell/snapshot"
}

func cellRemoteAllowed(remoteAddr string) bool {
	remoteAddr = strings.TrimSpace(remoteAddr)
	if remoteAddr == "" {
		return true
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || isCGNAT(ip)
}

func isCGNAT(ip net.IP) bool {
	ip = ip.To4()
	if ip == nil {
		return false
	}
	return ip[0] == 100 && ip[1] >= 64 && ip[1] <= 127
}

func writeCellJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeCellError(w http.ResponseWriter, status int, err error) {
	writeCellJSON(w, status, map[string]any{"error": strings.TrimSpace(err.Error())})
}
