package tlsissuance

import (
	"context"
	"errors"
	"golang.org/x/crypto/acme"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testBackend struct {
	mu        sync.Mutex
	p         Policy
	states    map[string]State
	cert      Certificate
	publishes int
	offline   bool
}

func (b *testBackend) ReadPolicy(context.Context) (Policy, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.offline {
		return Policy{}, errors.New("offline")
	}
	return b.p, nil
}
func (b *testBackend) ReadState(_ context.Context, _ Policy, h string) (State, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.offline {
		return State{}, errors.New("offline")
	}
	return b.states[h], nil
}
func (b *testBackend) UpdateState(_ context.Context, _ Policy, h string, fn func(*State) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.offline {
		return errors.New("offline")
	}
	s := b.states[h]
	if err := fn(&s); err != nil {
		return err
	}
	b.states[h] = s
	return nil
}
func (b *testBackend) CurrentCertificate(context.Context, DomainPolicy) (Certificate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cert, nil
}
func (b *testBackend) PublishCertificate(_ context.Context, _ Policy, _ DomainPolicy, previous string, c Certificate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.offline {
		return errors.New("offline")
	}
	if b.cert.SHA256 != previous {
		return errors.New("CAS conflict")
	}
	b.cert = c
	b.publishes++
	return nil
}
func (b *testBackend) PresentDNS(context.Context, Policy, DomainPolicy, string, time.Time) (func(), error) {
	return func() {}, nil
}
func (b *testBackend) VerifyChallenges(context.Context, DomainPolicy, string, string) error {
	return nil
}
func fixtureBackend() *testBackend {
	return &testBackend{p: Policy{Schema: "fugue.tls-issuance-policy/v1", Generation: 1, Enabled: true, DirectoryURL: "https://ca.example/directory", IssuerID: "issuer-fixture", Contact: "mailto:ops@example.test", RenewBeforeHours: 720, RetryBaseSeconds: 3600, Domains: []DomainPolicy{{Hostname: "site.example.test", AppID: "app-fixture", TenantID: "tenant-fixture", Mode: "http-01", ValidationTargets: []string{"192.0.2.8"}}}}, states: map[string]State{}, cert: Certificate{SHA256: "old-valid", NotAfter: time.Now().Add(24 * time.Hour)}}
}

type transferredDomainBackend struct{ *testBackend }

func (b transferredDomainBackend) CurrentCertificate(ctx context.Context, d DomainPolicy) (Certificate, error) {
	if d.Hostname == "transferred.example.test" {
		return Certificate{}, errors.New("owner changed")
	}
	return b.testBackend.CurrentCertificate(ctx, d)
}

func TestTransferredDomainDoesNotStarveOtherRenewals(t *testing.T) {
	b := fixtureBackend()
	transferred := b.p.Domains[0]
	transferred.Hostname = "transferred.example.test"
	b.p.Domains = append([]DomainPolicy{transferred}, b.p.Domains...)
	var issued string
	err := (&Engine{Backend: transferredDomainBackend{b}, Owner: "worker", Issue: func(_ context.Context, _ Policy, d DomainPolicy, _ string) (Certificate, error) {
		issued = d.Hostname
		return Certificate{SHA256: "renewed", NotAfter: time.Now().Add(90 * 24 * time.Hour)}, nil
	}}).Reconcile(context.Background())
	if err == nil || issued != "site.example.test" || b.publishes != 1 {
		t.Fatalf("err=%v issued=%q publishes=%d", err, issued, b.publishes)
	}
}

func TestConcurrentIssuersDoOneJobAndKeepServingCertificate(t *testing.T) {
	b := fixtureBackend()
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	issue := func(context.Context, Policy, DomainPolicy, string) (Certificate, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return Certificate{SHA256: "renewed", NotAfter: time.Now().Add(90 * 24 * time.Hour)}, nil
	}
	done := make(chan error, 1)
	go func() { done <- (&Engine{Backend: b, Owner: "a", Issue: issue}).Reconcile(context.Background()) }()
	<-entered
	var wg sync.WaitGroup
	for _, owner := range []string{"b", "c", "d", "e", "f"} {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			err := (&Engine{Backend: b, Owner: owner, Issue: issue}).Reconcile(context.Background())
			if err != nil && !errors.Is(err, ErrDeferred) {
				t.Error(err)
			}
		}(owner)
	}
	wg.Wait()
	if b.cert.SHA256 != "old-valid" || calls.Load() != 1 {
		t.Fatal("old certificate replaced before issuance or duplicate work")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b.publishes != 1 || b.cert.SHA256 != "renewed" {
		t.Fatal("not published exactly once")
	}
}

func TestRateLimitSurvivesRestartAndDoesNotDeleteCertificate(t *testing.T) {
	b := fixtureBackend()
	now := time.Now().UTC()
	calls := 0
	limited := func(context.Context, Policy, DomainPolicy, string) (Certificate, error) {
		calls++
		return Certificate{}, &acme.Error{StatusCode: 429, Header: http.Header{"Retry-After": []string{"86400"}}}
	}
	first := &Engine{Backend: b, Owner: "first", Now: func() time.Time { return now }, Issue: limited}
	if err := first.Reconcile(context.Background()); err == nil {
		t.Fatal("expected CA error")
	}
	state := b.states["site.example.test"]
	if state.NextAttempt.Before(now.Add(24*time.Hour)) || state.Owner != "" {
		t.Fatal("backoff not retained", state)
	}
	next := &Engine{Backend: b, Owner: "replacement", Now: func() time.Time { return now.Add(23 * time.Hour) }, Issue: limited}
	if err := next.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || b.cert.SHA256 != "old-valid" || b.publishes != 0 {
		t.Fatal("restart retried early or lost valid certificate")
	}
}

func TestLeaseCrashAndControlPlaneOutagePreserveCertificate(t *testing.T) {
	b := fixtureBackend()
	now := time.Now().UTC()
	b.states["site.example.test"] = State{Owner: "crashed", LeaseUntil: now.Add(15 * time.Minute), NextAttempt: now.Add(time.Hour)}
	calls := 0
	issue := func(context.Context, Policy, DomainPolicy, string) (Certificate, error) {
		calls++
		return Certificate{SHA256: "renewed", NotAfter: now.Add(90 * 24 * time.Hour)}, nil
	}
	engine := &Engine{Backend: b, Owner: "replacement", Now: func() time.Time { return now.Add(20 * time.Minute) }, Issue: issue}
	if err := engine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("crash reset retry barrier")
	}
	engine.Now = func() time.Time { return now.Add(2 * time.Hour) }
	engine.Issue = func(context.Context, Policy, DomainPolicy, string) (Certificate, error) {
		b.mu.Lock()
		b.offline = true
		b.mu.Unlock()
		return Certificate{SHA256: "unpublishable"}, nil
	}
	if err := engine.Reconcile(context.Background()); err == nil {
		t.Fatal("outage accepted publication")
	}
	if b.cert.SHA256 != "old-valid" {
		t.Fatal("outage invalidated existing certificate")
	}
	b.offline = false
	engine.Now = func() time.Time { return now.Add(4 * time.Hour) }
	engine.Issue = issue
	if err := engine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b.cert.SHA256 != "renewed" {
		t.Fatal("issuer did not recover")
	}
}

func TestPolicyChangeFencesOldIssuer(t *testing.T) {
	b := fixtureBackend()
	e := &Engine{Backend: b, Owner: "first", Issue: func(context.Context, Policy, DomainPolicy, string) (Certificate, error) {
		b.mu.Lock()
		b.p.Generation++
		b.mu.Unlock()
		return Certificate{SHA256: "stale"}, nil
	}}
	if err := e.Reconcile(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatal(err)
	}
	if b.publishes != 0 {
		t.Fatal("stale issuer published")
	}
}

func TestRetryAfterHTTPDateIsNotCapped(t *testing.T) {
	now := time.Now().UTC()
	until := now.Add(7 * 24 * time.Hour).Truncate(time.Second)
	err := &acme.Error{StatusCode: 429, Header: http.Header{"Retry-After": []string{until.Format(http.TimeFormat)}}}
	if got := RetryAt(now, time.Hour, 1, err); !got.Equal(until) {
		t.Fatal(got, until)
	}
}
