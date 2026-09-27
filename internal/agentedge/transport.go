package agentedge

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// NewHTTPClient preserves the configured logical origin and TLS identity while
// dialing only a currently authorized, measured Edge. Trust is supplied by an
// independent snapshot provider. Redirects and cross-Edge request replay are
// disabled, including for enrollment and operation-completion POST requests.
func NewHTTPClient(selector *Selector, keys func() map[string]TrustKey, timeout time.Duration) (*http.Client, error) {
	if selector == nil || keys == nil || timeout < time.Second || timeout > 30*time.Second {
		return nil, errors.New("Agent Edge client requires a selector, trust provider and bounded timeout")
	}
	return &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &transport{selector: selector, keys: keys, now: time.Now, pools: map[string]*http.Transport{}}}, nil
}

type transport struct {
	selector *Selector
	keys     func() map[string]TrustKey
	now      func() time.Time
	mu       sync.Mutex
	digest   string
	pools    map[string]*http.Transport
	// Tests supply a trusted local CA and map synthetic public endpoints to
	// local listeners. Production uses system roots and an ordinary dialer.
	roots *x509.CertPool
	dial  func(context.Context, string, string) (net.Conn, error)
}

func (t *transport) RoundTrip(request *http.Request) (*http.Response, error) {
	reject := func(err error) (*http.Response, error) {
		if request != nil && request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, err
	}
	keys, now := t.keys(), t.now()
	choice, err := t.selector.Current(keys, now)
	if err != nil {
		return reject(err)
	}
	if choice.Mode != "active" {
		return reject(errors.New("shadow Agent Edge policy cannot route control requests"))
	}
	origin, err := url.Parse(choice.Origin)
	if err != nil || request == nil || request.URL == nil || request.URL.Scheme != "https" || request.URL.Host != origin.Host || request.URL.User != nil ||
		request.URL.Opaque != "" || request.URL.Fragment != "" || request.Host != "" && request.Host != origin.Host {
		return reject(errors.New("Agent Edge request does not match its authorized TLS origin"))
	}
	if err := request.Context().Err(); err != nil {
		return reject(err)
	}
	readOnly := request.Method == http.MethodGet || request.Method == http.MethodHead || request.Method == http.MethodOptions
	pool := t.pool(choice, origin.Hostname(), readOnly)
	ctx, cancel := context.WithDeadline(request.Context(), choice.ValidUntil)
	copy := request.Clone(ctx)
	if !readOnly {
		copy.GetBody = nil
	}
	response, err := pool.RoundTrip(copy)
	if err != nil {
		cancel()
		if request.Context().Err() == nil {
			t.selector.TransportFailure(choice, keys, t.now())
		}
		return nil, err
	}
	response.Body = &leaseBody{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

func (t *transport) pool(choice Choice, hostname string, readOnly bool) *http.Transport {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.digest != choice.GrantDigest {
		for _, pool := range t.pools {
			pool.CloseIdleConnections()
		}
		t.digest, t.pools = choice.GrantDigest, map[string]*http.Transport{}
	}
	key := choice.Primary.EdgeID + "|" + choice.Primary.Address
	if !readOnly {
		key += "|mutation"
	}
	if pool := t.pools[key]; pool != nil {
		return pool
	}
	dial := t.dial
	if dial == nil {
		dial = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	pinned := net.JoinHostPort(choice.Primary.Address, "443")
	logical := net.JoinHostPort(hostname, "443")
	pool := &http.Transport{
		Proxy:           nil,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: hostname, RootCAs: t.roots},
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if address != logical {
				return nil, errors.New("Agent Edge dial escaped the configured origin")
			}
			return dial(ctx, network, pinned)
		},
		// A mutating request always uses a fresh connection. Go's transport may
		// retry a replayable request on a reused connection; this prevents that
		// behavior even when a caller provided an Idempotency-Key or GetBody.
		DisableKeepAlives: !readOnly, ForceAttemptHTTP2: readOnly,
		MaxIdleConns: 32, MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 20 * time.Second,
	}
	t.pools[key] = pool
	return pool
}

func (t *transport) CloseIdleConnections() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, pool := range t.pools {
		pool.CloseIdleConnections()
	}
}

type leaseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
}

func (b *leaseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.cancel)
	return err
}
