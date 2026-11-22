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
	mu         sync.Mutex
	entries    map[string]*list.Element
	lru        list.List
	limit      int
	perAddress int
	addresses  map[string]*list.List
}

type probeTransportEntry struct {
	key            string
	transport      *http.Transport
	createdAt      time.Time
	address        string
	addressElement *list.Element
}

const probeTransportMaxAge = time.Minute

var probeTransports = probeTransportPool{limit: 512}

// FanoutObserver shares one budget across the DNS serving, candidate, static
// and renewal scans in this process. Limits bound reusable transports, not the
// signed scheduler's concurrency or live TLS requests. Never copy an observer.
type FanoutObserver struct{ pool probeTransportPool }

func NewFanoutObserver() *FanoutObserver {
	return &FanoutObserver{pool: probeTransportPool{limit: 512, perAddress: 32}}
}

func (p *probeTransportPool) remove(element *list.Element) {
	entry := element.Value.(probeTransportEntry)
	delete(p.entries, entry.key)
	p.lru.Remove(element)
	if group := p.addresses[entry.address]; group != nil {
		group.Remove(entry.addressElement)
		if group.Len() == 0 {
			delete(p.addresses, entry.address)
		}
	}
	entry.transport.CloseIdleConnections()
}

func (p *probeTransportPool) get(host, address string) *http.Transport {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := host + "\x00" + address
	if entry := p.entries[key]; entry != nil {
		current := entry.Value.(probeTransportEntry)
		if time.Since(current.createdAt) < probeTransportMaxAge {
			p.lru.MoveToFront(entry)
			if group := p.addresses[address]; group != nil {
				group.MoveToFront(current.addressElement)
			}
			return current.transport
		}
		// IdleConnTimeout alone does not bound affinity when probes arrive
		// more frequently than that timeout. Retire aged idle connections while
		// allowing in-flight requests to finish on their existing transport.
		p.remove(entry)
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
	entry := probeTransportEntry{key: key, address: address, transport: transport, createdAt: time.Now()}
	if p.perAddress > 0 {
		if p.addresses == nil {
			p.addresses = make(map[string]*list.List)
		}
		group := p.addresses[address]
		if group != nil && group.Len() >= p.perAddress {
			p.remove(p.entries[group.Back().Value.(string)])
			group = p.addresses[address]
		}
		if group == nil {
			group = list.New()
			p.addresses[address] = group
		}
		entry.addressElement = group.PushFront(key)
	}
	p.entries[key] = p.lru.PushFront(entry)
	if p.lru.Len() > max(1, p.limit) {
		// This leaves active observations untouched. Idle resources are bounded
		// even when untrusted target names churn through the pool.
		p.remove(p.lru.Back())
	}
	return transport
}
