// Package httpserver contains lifecycle helpers for the plain HTTP API listener.
package httpserver

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// WithInitialIdleTimeout gives a new, unused HTTP/1 TCP connection the same
// opportunity to wait for a request as an established keep-alive connection.
// net/http otherwise starts ReadHeaderTimeout immediately after Accept, even
// when an HTTP client has only made a speculative connection for its idle pool.
// The first positive read starts the original header budget exactly once.
// After that, net/http owns every deadline, including body and keep-alive reads.
//
// Use this only with a plain TCP listener and an HTTP/1 server. TLS and HTTP/2
// have their own connection startup protocols and must not use this adapter.
func WithInitialIdleTimeout(listener net.Listener, timeout time.Duration) net.Listener {
	if timeout <= 0 {
		return listener
	}
	return &initialIdleListener{Listener: listener, timeout: timeout}
}

type initialIdleListener struct {
	net.Listener
	timeout time.Duration
}

func (l *initialIdleListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("initial HTTP idle listener requires a plain TCP connection")
	}
	return &initialIdleConn{TCPConn: tcp, idleTimeout: l.timeout}, nil
}

// Embedding TCPConn preserves half-close and optimized response writes. No
// independent expiry callback can race the client's ownership of a connection.
type initialIdleConn struct {
	*net.TCPConn
	idleTimeout  time.Duration
	started      atomic.Bool
	mu           sync.Mutex
	deadlineSet  bool
	headerBudget time.Duration
}

func (c *initialIdleConn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Only net/http's first future header deadline is deferred. A later
	// deadline, including an already expired cancellation, takes precedence.
	budget := time.Until(deadline)
	if !c.started.Load() && !c.deadlineSet && !deadline.IsZero() && budget > 0 {
		c.deadlineSet = true
		c.headerBudget = budget
		return c.TCPConn.SetReadDeadline(time.Now().Add(c.idleTimeout))
	}
	c.deadlineSet = true
	c.headerBudget = 0
	return c.TCPConn.SetReadDeadline(deadline)
}

func (c *initialIdleConn) SetDeadline(deadline time.Time) error {
	// SetDeadline is an explicit whole-connection deadline, not net/http's
	// initial header setup. Never extend it or resurrect a canceled read.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlineSet = true
	c.headerBudget = 0
	return c.TCPConn.SetDeadline(deadline)
}

func (c *initialIdleConn) Read(p []byte) (int, error) {
	n, err := c.TCPConn.Read(p)
	if n == 0 || c.started.Load() {
		return n, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started.Swap(true) && c.headerBudget > 0 {
		budget := c.headerBudget
		c.headerBudget = 0
		if deadlineErr := c.TCPConn.SetReadDeadline(time.Now().Add(budget)); deadlineErr != nil {
			_ = c.TCPConn.Close()
			return 0, deadlineErr
		}
	}
	return n, err
}
