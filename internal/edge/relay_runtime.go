package edge

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/edge/relayroute"
	"fugue/internal/model"
	"fugue/internal/routeproof"
	"fugue/internal/tcpdiag"
)

const (
	relayPrefix         = "/_fugue/relay/v1/"
	relayPoll           = 5 * time.Second
	relayRemoteTTL      = 15 * time.Second
	relayControlTimeout = 1500 * time.Millisecond
)

type relayProbeKey struct{ version, digest string }
type relayRemote struct {
	binding  relayroute.Binding
	endpoint relayroute.Endpoint
	at       time.Time
	origin   relayroute.Health
	capacity int
	process  string
	link     relayroute.Window
}

type edgeOriginRelay struct {
	s           *Service
	policyFile  string
	key         ed25519.PublicKey
	certificate tls.Certificate
	roots       *x509.CertPool
	local       relayroute.Endpoint
	policy      atomic.Pointer[relayroute.Policy]
	generation  uint64 // written only by the refresh loop
	policyHash  [32]byte
	process     string
	mu          sync.Mutex
	probes      map[relayProbeKey]relayroute.Window
	remote      map[string]relayRemote
	clients     map[relayroute.Endpoint]*http.Client
	permits     map[string]relayPermit
	dispatches  map[relayDispatchKey]time.Time
	sweptAt     time.Time
	outbound    int
	inbound     int
}

func relayReadFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, errors.New("relay file unavailable or exceeds limit")
	}
	return raw, nil
}

// A failed optional relay listener must not take down public ingress.
func (s *Service) initOriginRelay() (*http.Server, net.Listener, error) {
	envPath := func(suffix string) string {
		return strings.TrimSpace(os.Getenv("FUGUE_EDGE_ORIGIN_RELAY_" + suffix))
	}
	policyFile := envPath("POLICY_FILE")
	if policyFile == "" {
		return nil, nil, nil
	}
	keyRaw, err := relayReadFile(envPath("POLICY_VERIFY_KEY_FILE"), 4096)
	if err != nil {
		return nil, nil, errors.New("relay verifier unavailable")
	}
	key, err := hex.DecodeString(strings.TrimSpace(string(keyRaw)))
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, nil, errors.New("relay verifier invalid")
	}
	certPEM, err := relayReadFile(envPath("TLS_CERT_FILE"), 64<<10)
	if err != nil {
		return nil, nil, errors.New("relay certificate unavailable")
	}
	keyPEM, err := relayReadFile(envPath("TLS_KEY_FILE"), 64<<10)
	if err != nil {
		return nil, nil, errors.New("relay private key unavailable")
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil || cert.Leaf == nil {
		return nil, nil, errors.New("relay certificate invalid")
	}
	ca, err := relayReadFile(envPath("CA_FILE"), 64<<10)
	pool := x509.NewCertPool()
	if err != nil || !pool.AppendCertsFromPEM(ca) {
		return nil, nil, errors.New("relay CA invalid")
	}
	e := &edgeOriginRelay{s: s, policyFile: policyFile, key: key, certificate: cert, roots: pool, process: relayRandomID(),
		probes: map[relayProbeKey]relayroute.Window{}, remote: map[string]relayRemote{}, clients: map[relayroute.Endpoint]*http.Client{},
		permits: map[string]relayPermit{}, dispatches: map[relayDispatchKey]time.Time{}}
	p, err := e.loadPolicy(time.Now())
	if err != nil {
		return nil, nil, err
	}
	local, ok := p.Endpoint(s.Config.EdgeID)
	if !ok || local.Slot != s.Config.EdgeSlot || !relayCertificateName(cert.Leaf, local.TLSName) {
		return nil, nil, errors.New("relay local slot identity not authorized")
	}
	// The mounted directory must pin the actual private listener, not a public
	// wildcard or a Service that can load balance to a different worker.
	listener, err := net.Listen("tcp", local.Address)
	if err != nil {
		return nil, nil, errors.New("relay private listener unavailable")
	}
	e.local = local
	e.generation, e.policyHash = p.Generation, p.Hash
	srv := &http.Server{Handler: e, ReadHeaderTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}}
	srv.Protocols = new(http.Protocols)
	srv.Protocols.SetHTTP1(true)
	s.originRelay = e
	return srv, listener, nil
}

func relayCertificateName(cert *x509.Certificate, expected string) bool {
	return cert != nil && slices.Contains(cert.DNSNames, expected) // exact SAN, not wildcard
}

func (e *edgeOriginRelay) loadPolicy(now time.Time) (*relayroute.Policy, error) {
	raw, err := relayReadFile(e.policyFile, relayroute.MaxPolicyBytes)
	if err != nil {
		return nil, errors.New("relay policy unavailable")
	}
	p, err := relayroute.Verify(raw, e.key, now)
	if err != nil {
		return nil, err
	}
	if p.Generation < e.generation || (p.Generation == e.generation && p.Hash != e.policyHash) {
		return nil, errors.New("relay policy generation regressed or conflicts")
	}
	return p, nil
}

func (e *edgeOriginRelay) refresh(now time.Time) {
	p, err := e.loadPolicy(now)
	if err != nil {
		e.policy.Store(nil)
		return
	}
	local, ok := p.Endpoint(e.s.Config.EdgeID)
	if !ok || local != e.local || now.Before(e.certificate.Leaf.NotBefore) || !now.Before(e.certificate.Leaf.NotAfter) {
		e.policy.Store(nil)
		return
	}
	if e.generation != p.Generation {
		e.generation, e.policyHash = p.Generation, p.Hash
	}
	e.policy.Store(p)
}

func (e *edgeOriginRelay) run(ctx context.Context) {
	defer func() {
		e.policy.Store(nil)
		e.mu.Lock()
		defer e.mu.Unlock()
		for _, client := range e.clients {
			client.CloseIdleConnections()
		}
	}()
	for {
		if ctx.Err() != nil {
			return
		}
		e.refresh(time.Now())
		if p := e.current(time.Now()); p != nil {
			e.probeOrigins(ctx, p)
			e.poll(ctx, p)
		}
		timer := time.NewTimer(relayPoll + time.Duration(rand.IntN(1000)-500)*time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Probe only specifically authorized targets using the same origin dial and
// route-proof checks as the existing observation, without exporting a second
// copy of these probe samples to the public quality ranking.
func (e *edgeOriginRelay) probeOrigins(ctx context.Context, p *relayroute.Policy) {
	index := e.s.currentRouteIndex()
	if !e.serving(index) {
		return
	}
	now := time.Now()
	jobs := map[relayProbeKey]model.EdgeNetworkSample{}
	for _, b := range p.Bindings {
		source := b.Source == e.s.Config.EdgeID
		if !source && b.Peer != e.s.Config.EdgeID {
			continue
		}
		bound, ok := e.bound(index, b, source, now)
		if !ok {
			continue
		}
		jobs[bound.probe] = bound.sample
	}
	if len(jobs) > 8 {
		return
	} // fail closed; configuration permits eight local routes
	dialer := net.Dialer{Timeout: 2 * time.Second}
	var wg sync.WaitGroup
	for _, sample := range jobs {
		wg.Go(func() {
			if sample, ok := e.s.measureOriginNetworkTarget(ctx, index, sample, rootedKubernetesServiceDialContext(dialer.DialContext), tcpdiag.SnapshotFromConn); ok {
				e.noteProbe(sample, uint64(now.UnixNano()), time.Now())
			}
		})
	}
	wg.Wait()
}

func (e *edgeOriginRelay) current(now time.Time) *relayroute.Policy {
	p := e.policy.Load()
	leaf := e.certificate.Leaf
	if !p.Current(now) || p.Mode == "off" || leaf == nil || now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return nil
	}
	return p
}

func (e *edgeOriginRelay) serving(index *edgeRouteIndex) bool {
	e.s.mu.Lock()
	draining := e.s.Config.Draining
	e.s.mu.Unlock()
	if draining || !e.s.appTrafficProofApplied(index) {
		return false
	}
	active, known := e.s.servingActiveForHeartbeat()
	return active && known
}

type relayBoundRoute struct {
	route    model.EdgeRouteBinding
	upstream model.EdgeRouteUpstream
	target   *url.URL
	probe    relayProbeKey
	sample   model.EdgeNetworkSample
}

func (e *edgeOriginRelay) bound(index *edgeRouteIndex, b relayroute.Binding, source bool, now time.Time) (relayBoundRoute, bool) {
	full, ok, fallback, version, _ := index.routeForRequest(b.Host, b.Path)
	if !ok || fallback || full.TenantID != b.Tenant || full.AppID != b.App || len(full.Upstreams) != 1 ||
		full.Upstreams[0].ReleaseID != b.Release || full.Upstreams[0].Weight != 100 ||
		full.Upstreams[0].Role != "stable" ||
		(full.Upstreams[0].Status != "" && full.Upstreams[0].Status != model.EdgeRouteStatusActive) {
		return relayBoundRoute{}, false
	}
	sample, ok := originNetworkProbeTarget(full, version, e.s.Config.EdgeID, index.edgeGroupID, now)
	want := b.PeerDigest
	if source {
		want = b.SourceDigest
	}
	if !ok || sample.RouteDigest != want {
		return relayBoundRoute{}, false
	}
	selected := edgeRouteWithUpstream(full, full.Upstreams[0])
	target, err := url.Parse(selected.UpstreamURL)
	if err != nil || target == nil || target.User != nil || target.RawQuery != "" || target.Fragment != "" ||
		(target.Path != "" && target.Path != "/") || target.String() != b.Origin {
		return relayBoundRoute{}, false
	}
	return relayBoundRoute{selected, full.Upstreams[0], target, relayProbeKey{version, sample.RouteDigest}, sample}, true
}

func (e *edgeOriginRelay) noteProbe(sample model.EdgeNetworkSample, round uint64, now time.Time) {
	p := e.current(now)
	if p == nil || sample.ServiceConnectFailed == nil {
		return
	}
	relevant := false
	for _, b := range p.Bindings {
		if (b.Source == e.s.Config.EdgeID && b.SourceDigest == sample.RouteDigest) ||
			(b.Peer == e.s.Config.EdgeID && b.PeerDigest == sample.RouteDigest) {
			relevant = true
			break
		}
	}
	if !relevant {
		return
	}
	k := relayProbeKey{sample.BundleVersion, sample.RouteDigest}
	e.mu.Lock()
	defer e.mu.Unlock()
	for key, window := range e.probes {
		if window.Health(now).State == relayroute.Unknown {
			delete(e.probes, key)
		}
	}
	if _, ok := e.probes[k]; !ok && len(e.probes) >= 128 {
		return
	}
	window := e.probes[k]
	var rtt time.Duration
	if sample.ServiceRTTMS != nil {
		rtt = time.Duration(*sample.ServiceRTTMS * float64(time.Millisecond))
	}
	window.Add(relayroute.Sample{At: sample.ObservedAt, Round: round, Failed: *sample.ServiceConnectFailed, RTT: rtt}, now)
	e.probes[k] = window
}

func (e *edgeOriginRelay) originHealth(k relayProbeKey, now time.Time) relayroute.Health {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.probes[k].Health(now)
}

type relayRequest struct {
	Binding    string `json:"binding"`
	PolicyHash string `json:"policy_hash"`
	Dispatch   string `json:"dispatch,omitempty"`
	QueryHash  string `json:"query_hash,omitempty"`
	Bytes      int64  `json:"bytes,omitempty"`
	BudgetMS   int64  `json:"budget_ms,omitempty"`
}

type relayReply struct {
	Process   string           `json:"process"`
	State     relayroute.State `json:"state,omitempty"`
	AgeMillis int64            `json:"age_ms,omitempty"`
	RTTMillis int64            `json:"rtt_ms,omitempty"`
	Samples   int              `json:"samples,omitempty"`
	Capacity  int              `json:"capacity,omitempty"`
	Permit    string           `json:"permit,omitempty"`
}

func relayQueryHash(query string) string {
	h := sha256.Sum256([]byte(query))
	return hex.EncodeToString(h[:])
}

func (e *edgeOriginRelay) client(endpoint relayroute.Endpoint) *http.Client {
	e.mu.Lock()
	defer e.mu.Unlock()
	if out := e.clients[endpoint]; out != nil {
		return out
	}
	t := &http.Transport{DialContext: (&net.Dialer{Timeout: relayControlTimeout, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: relayControlTimeout, MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second, DisableCompression: true,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, ServerName: endpoint.TLSName, RootCAs: e.roots, Certificates: []tls.Certificate{e.certificate},
			VerifyConnection: func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 || !relayCertificateName(state.PeerCertificates[0], endpoint.TLSName) {
					return errors.New("relay peer certificate identity mismatch")
				}
				return nil
			}}}
	t.Protocols = new(http.Protocols)
	t.Protocols.SetHTTP1(true)
	out := &http.Client{Transport: t, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	e.clients[endpoint] = out
	return out
}

func (e *edgeOriginRelay) control(ctx context.Context, endpoint relayroute.Endpoint, op string, request relayRequest) (relayReply, error) {
	ctx, cancel := context.WithTimeout(ctx, relayControlTimeout)
	defer cancel()
	raw, err := json.Marshal(request)
	if err != nil {
		return relayReply{}, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+endpoint.Address+relayPrefix+op, bytes.NewReader(raw))
	if err != nil {
		return relayReply{}, err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := e.client(endpoint).Do(r)
	if err != nil {
		return relayReply{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return relayReply{}, errors.New("relay control rejected")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	var reply relayReply
	if err != nil || len(data) > 4096 || relayroute.Decode(bytes.NewReader(data), &reply) != nil || !relayValidID(reply.Process) {
		return relayReply{}, errors.New("relay control invalid")
	}
	return reply, nil
}

func (e *edgeOriginRelay) poll(ctx context.Context, p *relayroute.Policy) {
	type job struct {
		b    relayroute.Binding
		peer relayroute.Endpoint
	}
	jobs := make([]job, 0, 8)
	allowed := map[string]bool{}
	for _, b := range p.Bindings {
		if b.Source != e.s.Config.EdgeID {
			continue
		}
		peer, ok := p.Endpoint(b.Peer)
		if ok {
			jobs = append(jobs, job{b, peer})
			allowed[b.ID] = true
		}
	}
	e.mu.Lock()
	for id := range e.remote {
		if !allowed[id] {
			delete(e.remote, id)
		}
	}
	for endpoint, client := range e.clients {
		if current, ok := p.Endpoint(endpoint.Edge); !ok || current != endpoint {
			client.CloseIdleConnections()
			delete(e.clients, endpoint)
		}
	}
	e.mu.Unlock()
	work := make(chan job, len(jobs))
	for _, j := range jobs {
		work <- j
	}
	close(work)
	var wg sync.WaitGroup
	// Don't let one dead peer stall refresh of other peers.
	for range min(8, len(jobs)) {
		wg.Go(func() {
			for j := range work {
				if ctx.Err() != nil || !p.Current(time.Now()) {
					return
				}
				e.pollPeer(ctx, p, j.b, j.peer)
			}
		})
	}
	wg.Wait()
}

func (e *edgeOriginRelay) pollPeer(ctx context.Context, p *relayroute.Policy, b relayroute.Binding, peer relayroute.Endpoint) {
	start := time.Now()
	reply, err := e.control(ctx, peer, "state", relayRequest{Binding: b.ID, PolicyHash: hex.EncodeToString(p.Hash[:])})
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	row := e.remote[b.ID]
	if row.binding != b || row.endpoint != peer || (err == nil && row.process != reply.Process) {
		row = relayRemote{binding: b, endpoint: peer}
	}
	row.at, row.capacity = now, 0
	row.link.Add(relayroute.Sample{At: now, Round: uint64(now.UnixNano()), Failed: err != nil, RTT: now.Sub(start)}, now)
	row.origin = relayroute.Health{State: relayroute.Unknown}
	if err == nil {
		row.process = reply.Process
	}
	if err == nil && reply.Samples >= 2 && reply.Samples <= 3 && reply.AgeMillis >= 0 &&
		reply.AgeMillis < relayroute.EvidenceTTL.Milliseconds() && reply.RTTMillis >= 0 && reply.RTTMillis <= 60000 &&
		reply.Capacity >= 0 && reply.Capacity <= 32 && reply.State == relayroute.Healthy {
		row.origin = relayroute.Health{State: reply.State, At: start.Add(-time.Duration(reply.AgeMillis) * time.Millisecond),
			Samples: reply.Samples, RTT: time.Duration(reply.RTTMillis) * time.Millisecond}
		row.capacity = reply.Capacity
	}
	e.remote[b.ID] = row
}

func relayRouteDigest(route model.EdgeRouteBinding) string {
	d, _ := routeproof.Digest(route)
	return d
}
