package tlsissuance

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/crypto/acme"
)

// DNS proofs use the normal signed publication and gray/full observation path.
// Its configured rollout budget can be ten minutes, so an issuer must retain
// ownership and proof validity beyond that budget without weakening its gates.
const AttemptTimeout = 20 * time.Minute
const LeaseDuration = 30 * time.Minute

var ErrDeferred = errors.New("certificate issuance deferred")
var ErrLeaseLost = errors.New("certificate issuance lease lost")

type Certificate struct {
	PEM      string
	KeyPEM   string
	SHA256   string
	NotAfter time.Time
}

type Backend interface {
	ReadPolicy(context.Context) (Policy, error)
	ReadState(context.Context, Policy, string) (State, error)
	UpdateState(context.Context, Policy, string, func(*State) error) error
	CurrentCertificate(context.Context, DomainPolicy) (Certificate, error)
	PublishCertificate(context.Context, Policy, DomainPolicy, string, Certificate) error
	PresentDNS(context.Context, Policy, DomainPolicy, string, time.Time) (func(), error)
	VerifyChallenges(context.Context, DomainPolicy, string, string) error
}

type Engine struct {
	Backend    Backend
	Owner      string
	Now        func() time.Time
	HTTPClient *http.Client
	// Issue is injectable for deterministic failure/lease tests. Production uses ACME.
	Issue func(context.Context, Policy, DomainPolicy, string) (Certificate, error)
}

func (e *Engine) now() time.Time {
	if e.Now != nil {
		return e.Now().UTC()
	}
	return time.Now().UTC()
}

// Reconcile performs at most one bounded issuance per pass. Each domain has a
// durable lease and a persisted retry barrier before any CA request occurs.
func (e *Engine) Reconcile(ctx context.Context) error {
	p, err := e.Backend.ReadPolicy(ctx)
	if err != nil || !p.Enabled {
		return err
	}
	if err = p.Validate(); err != nil {
		return err
	}
	var observationErrors error
	for _, d := range p.Domains {
		if d.Mode == "external" || d.NotBefore != nil && d.NotBefore.After(e.now()) {
			continue
		}
		current, err := e.Backend.CurrentCertificate(ctx, d)
		if err != nil {
			// A removed or transferred domain must not starve unrelated renewals.
			// No CA request is made for a domain whose owner cannot be observed.
			observationErrors = errors.Join(observationErrors, fmt.Errorf("%s: %w", d.Hostname, err))
			continue
		}
		if current.NotAfter.After(e.now().Add(time.Duration(p.RenewBeforeHours) * time.Hour)) {
			continue
		}
		state, err := e.Backend.ReadState(ctx, p, d.Hostname)
		if err != nil {
			return err
		}
		if state.NextAttempt.After(e.now()) || state.LeaseUntil.After(e.now()) {
			continue
		}
		return errors.Join(observationErrors, e.issueDomain(ctx, p, d, current.SHA256))
	}
	return observationErrors
}

func (e *Engine) issueDomain(parent context.Context, p Policy, d DomainPolicy, previous string) (resultErr error) {
	ctx, cancel := context.WithTimeout(parent, AttemptTimeout)
	defer cancel()
	if e.Owner == "" {
		return errors.New("certificate issuer identity required")
	}
	now := e.now()
	err := e.Backend.UpdateState(ctx, p, d.Hostname, func(state *State) error {
		if state.NextAttempt.After(now) || state.LeaseUntil.After(now) {
			return ErrDeferred
		}
		state.Owner = e.Owner
		state.LeaseUntil = now.Add(LeaseDuration)
		state.NextAttempt = now.Add(time.Duration(p.RetryBaseSeconds) * time.Second)
		state.Token = ""
		state.KeyAuthorization = ""
		state.ChallengeExpires = time.Time{}
		return nil
	})
	if err != nil {
		return err
	}
	defer func() {
		finishCtx, done := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
		defer done()
		persistErr := e.Backend.UpdateState(finishCtx, p, d.Hostname, func(state *State) error {
			if state.Owner != e.Owner {
				return ErrLeaseLost
			}
			state.Owner = ""
			state.LeaseUntil = time.Time{}
			state.Token = ""
			state.KeyAuthorization = ""
			state.ChallengeExpires = time.Time{}
			if resultErr == nil {
				state.Failures = 0
				state.LastError = ""
				state.LastSuccess = e.now()
				return nil
			}
			state.Failures++
			state.LastError = FailureClass(resultErr)
			next := RetryAt(e.now(), time.Duration(p.RetryBaseSeconds)*time.Second, state.Failures, resultErr)
			if next.After(state.NextAttempt) {
				state.NextAttempt = next
			}
			return nil
		})
		if persistErr != nil {
			resultErr = errors.Join(resultErr, persistErr)
		}
	}()
	if err = e.fence(ctx, p, d); err != nil {
		return err
	}
	issue := e.Issue
	if issue == nil {
		issue = e.issueACME
	}
	cert, err := issue(ctx, p, d, previous)
	if err != nil {
		return err
	}
	if err = e.fence(ctx, p, d); err != nil {
		return err
	}
	return e.Backend.PublishCertificate(ctx, p, d, previous, cert)
}

func (e *Engine) fence(ctx context.Context, p Policy, d DomainPolicy) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := e.Backend.ReadPolicy(ctx)
	if err != nil {
		return err
	}
	if !current.Enabled || current.Generation != p.Generation || current.IssuerID != p.IssuerID || current.DirectoryURL != p.DirectoryURL {
		return ErrLeaseLost
	}
	state, err := e.Backend.ReadState(ctx, p, d.Hostname)
	if err != nil {
		return err
	}
	if state.Owner != e.Owner || !state.LeaseUntil.After(e.now().Add(time.Minute)) {
		return ErrLeaseLost
	}
	return nil
}

func RetryAt(now time.Time, base time.Duration, failures int, err error) time.Time {
	delay := base * time.Duration(1<<min(max(failures-1, 0), 4))
	if delay > 24*time.Hour {
		delay = 24 * time.Hour
	}
	next := now.Add(delay)
	var problem *acme.Error
	if errors.As(err, &problem) {
		raw := problem.Header.Get("Retry-After")
		if seconds, parseErr := strconv.ParseInt(raw, 10, 32); parseErr == nil && seconds > 0 {
			if until := now.Add(time.Duration(seconds) * time.Second); until.After(next) {
				next = until
			}
		} else if until, parseErr := http.ParseTime(raw); parseErr == nil && until.After(next) {
			next = until
		}
	}
	return next
}

func FailureClass(err error) string {
	var problem *acme.Error
	if errors.As(err, &problem) {
		if problem.StatusCode == 429 {
			return "ca_rate_limited"
		}
		return "ca_request_failed"
	}
	if errors.Is(err, ErrLeaseLost) {
		return "issuance_authority_changed"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "issuance_timeout"
	}
	return "issuance_or_validation_failed"
}

func (e *Engine) account(ctx context.Context, p Policy) (*acme.Client, error) {
	keyState, err := e.Backend.ReadState(ctx, p, "account")
	if err != nil {
		return nil, err
	}
	if keyState.NextAttempt.After(e.now()) {
		return nil, ErrDeferred
	}
	if keyState.AccountKeyPEM == "" {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, err
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, err
		}
		raw := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		err = e.Backend.UpdateState(ctx, p, "account", func(state *State) error {
			if state.AccountKeyPEM == "" {
				state.AccountKeyPEM = raw
			}
			keyState = *state
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	block, _ := pem.Decode([]byte(keyState.AccountKeyPEM))
	if block == nil {
		return nil, errors.New("stored ACME account key is invalid")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("unsupported stored ACME account key")
	}
	client := &acme.Client{Key: key, DirectoryURL: p.DirectoryURL, HTTPClient: e.HTTPClient, UserAgent: "Fugue centralized certificate issuer", KID: acme.KeyID(keyState.AccountURI), RetryBackoff: func(int, *http.Request, *http.Response) time.Duration { return 0 }}
	if client.KID == "" {
		var contacts []string
		if p.Contact != "" {
			contacts = []string{p.Contact}
		}
		account, err := client.Register(ctx, &acme.Account{Contact: contacts}, acme.AcceptTOS)
		if err != nil {
			persistCtx, done := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer done()
			persistErr := e.Backend.UpdateState(persistCtx, p, "account", func(state *State) error {
				state.Failures++
				state.LastError = FailureClass(err)
				if next := RetryAt(e.now(), time.Duration(p.RetryBaseSeconds)*time.Second, state.Failures, err); next.After(state.NextAttempt) {
					state.NextAttempt = next
				}
				return nil
			})
			if persistErr != nil {
				return nil, errors.Join(err, persistErr)
			}
			return nil, err
		}
		client.KID = acme.KeyID(account.URI)
		if err = e.Backend.UpdateState(ctx, p, "account", func(state *State) error { state.AccountURI = account.URI; return nil }); err != nil {
			return nil, err
		}
	}
	return client, nil
}

func (e *Engine) issueACME(ctx context.Context, p Policy, d DomainPolicy, _ string) (Certificate, error) {
	client, err := e.account(ctx, p)
	if err != nil {
		return Certificate{}, err
	}
	if err = e.fence(ctx, p, d); err != nil {
		return Certificate{}, err
	}
	order, err := client.AuthorizeOrder(ctx, acme.DomainIDs(d.Hostname))
	if err != nil {
		return Certificate{}, err
	}
	for _, authorizationURL := range order.AuthzURLs {
		auth, err := client.GetAuthorization(ctx, authorizationURL)
		if err != nil {
			return Certificate{}, err
		}
		if auth.Identifier.Value != d.Hostname {
			return Certificate{}, errors.New("CA authorization identity differs")
		}
		if auth.Status == acme.StatusValid {
			continue
		}
		var challenge *acme.Challenge
		for _, candidate := range auth.Challenges {
			if candidate.Type == d.Mode {
				challenge = candidate
				break
			}
		}
		if challenge == nil || !ValidToken(challenge.Token) {
			return Certificate{}, errors.New("CA did not offer configured challenge")
		}
		value, err := client.HTTP01ChallengeResponse(challenge.Token)
		if d.Mode == "dns-01" {
			value, err = client.DNS01ChallengeRecord(challenge.Token)
		}
		if err != nil {
			return Certificate{}, err
		}
		expires := e.now().Add(AttemptTimeout)
		if d.Mode == "http-01" {
			err = e.Backend.UpdateState(ctx, p, d.Hostname, func(state *State) error {
				if state.Owner != e.Owner {
					return ErrLeaseLost
				}
				state.Token = challenge.Token
				state.KeyAuthorization = value
				state.ChallengeExpires = expires
				return nil
			})
		} else {
			var cleanup func()
			cleanup, err = e.Backend.PresentDNS(ctx, p, d, value, expires)
			if cleanup != nil {
				defer cleanup()
			}
		}
		if err != nil {
			return Certificate{}, err
		}
		if err = e.Backend.VerifyChallenges(ctx, d, challenge.Token, value); err != nil {
			return Certificate{}, err
		}
		if err = e.fence(ctx, p, d); err != nil {
			return Certificate{}, err
		}
		if _, err = client.Accept(ctx, challenge); err != nil {
			return Certificate{}, err
		}
		if _, err = client.WaitAuthorization(ctx, authorizationURL); err != nil {
			return Certificate{}, err
		}
	}
	if _, err = client.WaitOrder(ctx, order.URI); err != nil {
		return Certificate{}, err
	}
	if err = e.fence(ctx, p, d); err != nil {
		return Certificate{}, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Certificate{}, err
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{DNSNames: []string{d.Hostname}}, key)
	if err != nil {
		return Certificate{}, err
	}
	chain, _, err := client.CreateOrderCert(ctx, order.FinalizeURL, csr, true)
	if err != nil {
		return Certificate{}, err
	}
	if len(chain) == 0 {
		return Certificate{}, errors.New("CA returned empty certificate chain")
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		return Certificate{}, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return Certificate{}, err
	}
	cert := Certificate{KeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), SHA256: fmt.Sprintf("%x", sha256.Sum256(leaf.Raw)), NotAfter: leaf.NotAfter}
	for _, der := range chain {
		cert.PEM += string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	}
	return cert, nil
}
