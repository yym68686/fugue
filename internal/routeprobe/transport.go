package routeprobe

import (
	"container/list"
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"time"
)

// Pool connections, never proofs. The exact address AND TLS hostname are part
// of the key: pooling by hostname alone could silently probe the wrong Edge.
// Each request still has its own deadline, nonce and complete proof validation.
type probeTransportPool struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	lru     list.List
	limit   int
}

type probeTransportEntry struct {
	key       string
	transport *http.Transport
}

var probeTransports = probeTransportPool{limit: 512}

func (p *probeTransportPool) get(host, address string) *http.Transport {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := host + "\x00" + address
	if entry := p.entries[key]; entry != nil {
		p.lru.MoveToFront(entry)
		return entry.Value.(probeTransportEntry).transport
	}
	transport := &http.Transport{
		Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host},
		MaxResponseHeaderBytes: 16 << 10, ForceAttemptHTTP2: true,
		MaxIdleConns: 1, MaxIdleConnsPerHost: 1, IdleConnTimeout: 45 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			// net/http may detach a shared dial from an individual request's
			// cancellation. Keep abandoned connection attempts bounded too.
			connection, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, net.JoinHostPort(address, "443"))
			if err != nil {
				return nil, connectionError{err: classifyTransportError(err)}
			}
			return connection, nil
		},
	}
	if p.entries == nil {
		p.entries = make(map[string]*list.Element)
	}
	p.entries[key] = p.lru.PushFront(probeTransportEntry{key, transport})
	if p.lru.Len() > max(1, p.limit) {
		old := p.lru.Back()
		entry := old.Value.(probeTransportEntry)
		delete(p.entries, entry.key)
		p.lru.Remove(old)
		// This leaves active observations untouched. Idle resources are bounded
		// even when untrusted target names churn through the pool.
		entry.transport.CloseIdleConnections()
	}
	return transport
}
