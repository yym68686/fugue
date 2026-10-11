package managedssh

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/model"

	"golang.org/x/crypto/ssh"
)

type Server struct {
	executor    Executor
	audit       AuditSink
	state       atomic.Pointer[snapshot]
	activation  sync.Mutex
	connections sync.Mutex
	active      int
	byIP        map[string]int
}

func NewServer(executor Executor, audit AuditSink) (*Server, error) {
	if executor == nil || audit == nil {
		return nil, ErrConfiguration
	}
	return &Server{executor: executor, audit: audit, byIP: map[string]int{}}, nil
}

func (server *Server) Activate(ctx context.Context, configuration Configuration, signer ssh.Signer) error {
	return server.activate(ctx, configuration, signer, nil)
}

func (server *Server) activate(ctx context.Context, configuration Configuration, signer ssh.Signer, persist func() error) error {
	if current := server.state.Load(); current != nil {
		encoded, err := json.Marshal(configuration)
		if err == nil {
			digest := sha256.Sum256(encoded)
			if current.digest == "sha256:"+hex.EncodeToString(digest[:]) && signer != nil && ssh.FingerprintSHA256(signer.PublicKey()) == current.configuration.Identity.Fingerprint && configuration.ValidUntil.After(time.Now()) {
				return nil
			}
		}
	}
	current := server.state.Load()
	preflight := current == nil || current.configuration.Target != configuration.Target || (configuration.AllowPTY && !current.configuration.AllowPTY) || (configuration.AllowSFTP && !current.configuration.AllowSFTP) || (len(configuration.ForwardRules) > 0 && len(current.configuration.ForwardRules) == 0)
	candidate, err := prepareSnapshot(ctx, configuration, signer, server.executor, time.Now().UTC(), preflight)
	if err != nil {
		return err
	}
	server.activation.Lock()
	defer server.activation.Unlock()
	previous := server.state.Load()
	if previous != nil {
		if previous.configuration.Identity.ID != configuration.Identity.ID || previous.configuration.Identity.Fingerprint != configuration.Identity.Fingerprint || previous.configuration.Identity.AppID != configuration.Identity.AppID || previous.configuration.Identity.TenantID != configuration.Identity.TenantID {
			return ErrIdentityChange
		}
		if candidate.configuration.Generation < previous.configuration.Generation || (candidate.configuration.Generation == previous.configuration.Generation && candidate.digest != previous.digest) {
			return ErrGeneration
		}
		if candidate.digest == previous.digest {
			return nil
		}
	}
	if err := server.record(ctx, candidate, "configuration_activation_admitted", "", "", "", "", 0); err != nil {
		return err
	}
	if persist != nil {
		if err := persist(); err != nil {
			return err
		}
	}
	server.state.Store(candidate)
	return nil
}

type Status struct {
	Digest            string    `json:"digest,omitempty"`
	Generation        uint64    `json:"generation,omitempty"`
	IdentityID        string    `json:"identity_id,omitempty"`
	Fingerprint       string    `json:"fingerprint,omitempty"`
	ValidUntil        time.Time `json:"valid_until,omitempty"`
	Serving           bool      `json:"serving"`
	ActiveConnections int       `json:"active_connections"`
	Draining          bool      `json:"draining"`
}

func (server *Server) Status() Status {
	status := Status{}
	if current := server.state.Load(); current != nil {
		status.Digest = current.digest
		status.Generation = current.configuration.Generation
		status.IdentityID = current.configuration.Identity.ID
		status.Fingerprint = current.configuration.Identity.Fingerprint
		status.ValidUntil = current.configuration.ValidUntil
		status.Serving = current.configuration.ValidUntil.After(time.Now())
		status.Draining = current.configuration.Draining
	}
	server.connections.Lock()
	status.ActiveConnections = server.active
	server.connections.Unlock()
	return status
}

func (server *Server) ConfigurationDigest() string {
	state := server.state.Load()
	if state == nil {
		return ""
	}
	return state.digest
}

func (server *Server) VerifyRuntime(ctx context.Context, serverPodUID string) (CandidateEvidence, error) {
	current := server.state.Load()
	if current == nil || serverPodUID == "" {
		return CandidateEvidence{}, ErrUnavailable
	}
	configuration := current.configuration
	capabilities, err := server.executor.Validate(ctx, configuration.Target)
	if err != nil {
		return CandidateEvidence{}, err
	}
	if !capabilities.Exec || !capabilities.Shell || !capabilities.Signals || (configuration.AllowPTY && !capabilities.PTY) || (configuration.AllowSFTP && !capabilities.SFTP) || (len(configuration.ForwardRules) > 0 && !capabilities.Forward) {
		return CandidateEvidence{}, ErrCapabilities
	}
	now := time.Now().UTC()
	if server.state.Load() != current || !configuration.ValidUntil.After(now.Add(5*time.Second)) {
		return CandidateEvidence{}, ErrUnavailable
	}
	expires := now.Add(time.Minute)
	if !configuration.ValidUntil.After(expires) {
		expires = configuration.ValidUntil.Add(-time.Second)
	}
	return CandidateEvidence{ArtifactDigest: current.digest, IdentityID: configuration.Identity.ID, Fingerprint: configuration.Identity.Fingerprint, ServerPodUID: serverPodUID, TargetPodUID: configuration.Target.PodUID, TargetContainerID: configuration.Target.ContainerID, RuntimeVerified: true, ObservedAt: now, ExpiresAt: expires}, nil
}

func (server *Server) Serve(ctx context.Context, listener net.Listener) error {
	if ctx == nil || listener == nil {
		return ErrConfiguration
	}
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	var workers sync.WaitGroup
	defer workers.Wait()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		state := server.state.Load()
		if state == nil || !state.configuration.ValidUntil.After(time.Now()) {
			_ = connection.Close()
			continue
		}
		host, _, err := net.SplitHostPort(connection.RemoteAddr().String())
		if err != nil || !server.acquire(state, host) {
			_ = connection.Close()
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer server.release(host)
			server.handleConnection(ctx, state, connection)
		}()
	}
}

func (server *Server) acquire(state *snapshot, host string) bool {
	server.connections.Lock()
	defer server.connections.Unlock()
	current := server.state.Load()
	if current == nil || current.configuration.Draining || !current.configuration.ValidUntil.After(time.Now()) || server.active >= current.configuration.MaxConnections || server.byIP[host] >= current.configuration.MaxConnectionsPerIP {
		return false
	}
	server.active++
	server.byIP[host]++
	return true
}

func (server *Server) release(host string) {
	server.connections.Lock()
	defer server.connections.Unlock()
	server.active--
	server.byIP[host]--
	if server.byIP[host] == 0 {
		delete(server.byIP, host)
	}
}

func (server *Server) handleConnection(ctx context.Context, initial *snapshot, transport net.Conn) {
	defer transport.Close()
	stop := context.AfterFunc(ctx, func() { _ = transport.Close() })
	defer stop()
	_ = transport.SetDeadline(time.Now().Add(10 * time.Second))
	configuration := &ssh.ServerConfig{
		MaxAuthTries: 3,
		PublicKeyCallback: func(metadata ssh.ConnMetadata, publicKey ssh.PublicKey) (*ssh.Permissions, error) {
			fingerprint := ssh.FingerprintSHA256(publicKey)
			current := server.state.Load()
			if !current.authorize(metadata.User(), fingerprint, time.Now().UTC()) {
				return nil, ErrUnauthorized
			}
			return &ssh.Permissions{Extensions: map[string]string{"fingerprint": fingerprint}}, nil
		},
	}
	configuration.AddHostKey(initial.signer)
	connection, channels, requests, err := ssh.NewServerConn(transport, configuration)
	if err != nil {
		_ = server.record(ctx, initial, "authentication_failed", "", "", "", "rejected", 0)
		return
	}
	defer connection.Close()
	_ = transport.SetDeadline(time.Time{})
	fingerprint := connection.Permissions.Extensions["fingerprint"]
	if !server.state.Load().authorize(connection.User(), fingerprint, time.Now().UTC()) {
		return
	}
	connectionID := model.NewID("sshconn")
	if err := server.record(ctx, initial, "connection_opened", connectionID, "", fingerprint, "", 0); err != nil {
		return
	}
	connectionContext, cancel := context.WithCancel(ctx)
	defer cancel()
	go server.watchAuthorization(connectionContext, connection, connection.User(), fingerprint)
	go func() {
		for request := range requests {
			_ = request.Reply(request.Type == "keepalive@openssh.com", nil)
		}
	}()
	var workers sync.WaitGroup
	semaphore := make(chan struct{}, initial.configuration.MaxChannels)
	for channel := range channels {
		current := server.state.Load()
		if !current.authorize(connection.User(), fingerprint, time.Now().UTC()) || current.configuration.Target != initial.configuration.Target {
			_ = channel.Reject(ssh.Prohibited, "authorization or workload changed")
			continue
		}
		select {
		case semaphore <- struct{}{}:
		default:
			_ = channel.Reject(ssh.ResourceShortage, "channel limit reached")
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-semaphore }()
			switch channel.ChannelType() {
			case "session":
				server.handleSession(connectionContext, initial, current, channel, connectionID, fingerprint)
			case "direct-tcpip":
				server.handleForward(connectionContext, initial, current, channel, connectionID, fingerprint)
			default:
				_ = channel.Reject(ssh.UnknownChannelType, "unsupported channel")
			}
		}()
	}
	cancel()
	workers.Wait()
	_ = server.record(context.WithoutCancel(ctx), initial, "connection_closed", connectionID, "", fingerprint, "", 0)
}

func (server *Server) watchAuthorization(ctx context.Context, connection *ssh.ServerConn, user, fingerprint string) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = connection.Close()
			return
		case <-ticker.C:
			if !server.state.Load().authorize(user, fingerprint, time.Now().UTC()) {
				_ = connection.Close()
				return
			}
		}
	}
}

func (server *Server) record(ctx context.Context, state *snapshot, kind, connectionID, sessionID, fingerprint, reason string, exitCode uint32) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return server.audit.Record(ctx, AuditEvent{
		Type: kind, TenantID: state.configuration.Target.TenantID, AppID: state.configuration.Target.AppID,
		IdentityID: state.configuration.Identity.ID, Generation: state.configuration.Generation,
		ConnectionID: connectionID, SessionID: sessionID, KeyFingerprint: fingerprint,
		TargetPodUID: state.configuration.Target.PodUID, TargetContainerID: state.configuration.Target.ContainerID,
		Reason: reason, ExitCode: exitCode, OccurredAt: time.Now().UTC(),
	})
}
