package managedssh

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

func (server *Server) handleForward(ctx context.Context, connectionState, policy *snapshot, incoming ssh.NewChannel, connectionID, fingerprint string) {
	var destination struct {
		Host       string
		Port       uint32
		Origin     string
		OriginPort uint32
	}
	if len(incoming.ExtraData()) > 4096 || ssh.Unmarshal(incoming.ExtraData(), &destination) != nil || !forwardAllowed(policy.configuration, destination.Host, destination.Port) {
		_ = incoming.Reject(ssh.Prohibited, "forward destination denied")
		return
	}
	forwardContext, cancel := context.WithCancel(ctx)
	defer cancel()
	current := server.state.Load()
	if !current.authorize(connectionState.configuration.Target.User, fingerprint, time.Now()) || !forwardAllowed(current.configuration, destination.Host, destination.Port) {
		_ = incoming.Reject(ssh.Prohibited, "forward denied")
		return
	}
	if _, err := server.executor.Validate(forwardContext, connectionState.configuration.Target); err != nil {
		_ = incoming.Reject(ssh.ConnectionFailed, "target changed")
		return
	}
	upstream, err := server.executor.Forward(forwardContext, connectionState.configuration.Target, destination.Host, destination.Port)
	if err != nil {
		_ = incoming.Reject(ssh.ConnectionFailed, "forward target unavailable")
		return
	}
	defer upstream.Close()
	if err := server.record(ctx, current, "forward_opened", connectionID, "", fingerprint, "authorized loopback target", 0); err != nil {
		_ = incoming.Reject(ssh.Prohibited, "audit unavailable")
		return
	}
	channel, requests, err := incoming.Accept()
	if err != nil {
		return
	}
	defer channel.Close()
	go ssh.DiscardRequests(requests)
	stop := context.AfterFunc(forwardContext, func() { _ = upstream.Close(); _ = channel.Close() })
	defer stop()
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-forwardContext.Done():
				return
			case <-ticker.C:
				latest := server.state.Load()
				if !latest.authorize(connectionState.configuration.Target.User, fingerprint, time.Now()) || !forwardAllowed(latest.configuration, destination.Host, destination.Port) {
					cancel()
					return
				}
			}
		}
	}()
	var copies sync.WaitGroup
	copies.Add(2)
	go func() {
		defer copies.Done()
		_, _ = io.Copy(upstream, channel)
		if half, ok := upstream.(interface{ CloseWrite() error }); ok {
			_ = half.CloseWrite()
		} else {
			_ = upstream.Close()
		}
	}()
	go func() { defer copies.Done(); _, _ = io.Copy(channel, upstream); _ = channel.CloseWrite() }()
	copies.Wait()
	_ = server.record(context.WithoutCancel(ctx), current, "forward_closed", connectionID, "", fingerprint, "", 0)
}

func forwardAllowed(configuration Configuration, host string, port uint32) bool {
	address := net.ParseIP(host)
	if address == nil || !address.IsLoopback() {
		return false
	}
	for _, rule := range configuration.ForwardRules {
		end := rule.EndPort
		if end == 0 {
			end = rule.Port
		}
		if rule.Port <= port && port <= end && address.Equal(net.ParseIP(rule.Host)) {
			return true
		}
	}
	return false
}
