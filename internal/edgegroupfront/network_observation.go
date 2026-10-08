package edgegroupfront

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/tcpdiag"
)

func (s *Service) startNetworkObservation(cfg Config) (func(context.Context) error, error) {
	if !filepath.IsAbs(cfg.NetworkSocket) || len(cfg.NetworkSocket) > 100 {
		return nil, errors.New("bounded absolute Front network socket path required")
	}
	lockDescriptor, err := syscall.Open(cfg.NetworkSocket+".lock", syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(lockDescriptor), cfg.NetworkSocket+".lock")
	if err := syscall.Flock(lockDescriptor, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("Front network socket already owned")
	}
	owned := false
	defer func() {
		if !owned {
			lock.Close()
		}
	}()
	if info, err := os.Lstat(cfg.NetworkSocket); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("Front network socket path contains another file")
		}
		connection, dialErr := net.DialTimeout("unix", cfg.NetworkSocket, 100*time.Millisecond)
		if dialErr == nil {
			connection.Close()
			return nil, errors.New("Front network socket is already serving")
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, errors.New("Front network socket ownership cannot be established")
		}
		if err := os.Remove(cfg.NetworkSocket); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: cfg.NetworkSocket, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(cfg.NetworkSocket, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		s.handleNetworkObservation(writer, request, cfg, tcpdiag.SnapshotFromConn)
	}), ReadHeaderTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, IdleTimeout: time.Second, MaxHeaderBytes: 4096}
	go func() { _ = server.Serve(listener) }()
	owned = true
	var stopped sync.Once
	return func(ctx context.Context) error {
		var result error
		stopped.Do(func() {
			result = server.Shutdown(ctx)
			if result != nil {
				server.Close()
			}
			lock.Close()
		})
		return result
	}, nil
}

func (s *Service) handleNetworkObservation(writer http.ResponseWriter, request *http.Request, cfg Config, read func(net.Conn) tcpdiag.Snapshot) {
	writer.Header().Set("Cache-Control", "no-store")
	if request.Method != http.MethodPost || request.URL.Path != frontnetwork.Path || request.URL.RawQuery != "" {
		http.Error(writer, "unsupported observation request", http.StatusBadRequest)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, 2049))
	if err != nil || len(raw) > 2048 || !json.Valid(raw) {
		http.Error(writer, "invalid bounded observation request", http.StatusBadRequest)
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var query frontnetwork.Request
	if decoder.Decode(&query) != nil || frontnetwork.ValidateRequest(query) != nil || query.EdgeID != cfg.EdgeID || query.GroupID != cfg.EdgeGroupID {
		http.Error(writer, "observation identity mismatch", http.StatusBadRequest)
		return
	}
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
	if len(s.active) > 1024 {
		s.mu.Unlock()
		http.Error(writer, "connection observation budget exceeded", http.StatusServiceUnavailable)
		return
	}
	peer, _ := frontnetwork.ParseRemote(query.RemoteAddr)
	var matched edgeFrontActiveTCPConnection
	count := 0
	for _, connection := range s.active {
		remote, err := frontnetwork.ParseRemote(connection.DownstreamRemote)
		if err == nil && remote == peer && connection.Protocol == ProtocolHTTPS && connection.ProxyProtocol && connection.Slot == query.Slot {
			matched = connection
			count++
		}
	}
	s.mu.Unlock()
	if count != 1 || matched.Downstream == nil || matched.StartedAt.After(now) {
		http.Error(writer, "exact live connection unavailable", http.StatusNotFound)
		return
	}
	sample := publicClientNetworkSample(matched, read(matched.Downstream), now)
	if model.ValidateEdgeClientNetworkSample(&sample) != nil {
		http.Error(writer, "connection observation invalid", http.StatusServiceUnavailable)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(frontnetwork.Response{Schema: frontnetwork.Schema, Nonce: query.Nonce, EdgeID: cfg.EdgeID, GroupID: cfg.EdgeGroupID, Sample: sample})
}

func publicClientNetworkSample(connection edgeFrontActiveTCPConnection, network tcpdiag.Snapshot, now time.Time) model.EdgeClientNetworkSample {
	peer, _ := frontnetwork.ParseRemote(connection.DownstreamRemote)
	sample := model.EdgeClientNetworkSample{ConnectionID: connection.ID, Slot: connection.Slot, Scope: frontnetwork.Scope(peer.Addr()), StartedAt: connection.StartedAt, ObservedAt: now}
	if network.Available {
		sample.TCPInfoAvailable = true
		if network.RTTUsec > 0 {
			value := float64(network.RTTUsec) / 1000
			sample.RTTMS = &value
		}
		if network.MinRTTUsec > 0 {
			value := float64(network.MinRTTUsec) / 1000
			sample.MinRTTMS = &value
		}
		if network.RTTVarUsec > 0 {
			value := float64(network.RTTVarUsec) / 1000
			sample.RTTVarianceMS = &value
		}
		sample.SegmentsOut, sample.RetransmittedSegments = network.SegsOut, network.TotalRetrans
		sample.BytesSent, sample.BytesRetransmitted = network.BytesSent, network.BytesRetrans
	}
	return sample
}
