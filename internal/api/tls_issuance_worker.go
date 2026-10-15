package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
	"fugue/internal/tlscertificate"
	"fugue/internal/tlsissuance"

	"github.com/miekg/dns"
)

type tlsIssuanceBackend struct{ server *Server }

func (b tlsIssuanceBackend) ReadPolicy(ctx context.Context) (tlsissuance.Policy, error) {
	return b.server.store.ReadTLSIssuancePolicy(ctx)
}
func (b tlsIssuanceBackend) ReadState(ctx context.Context, p tlsissuance.Policy, host string) (tlsissuance.State, error) {
	return b.server.store.ReadTLSIssuanceState(ctx, p.IssuerID, p.DirectoryURL, host)
}
func (b tlsIssuanceBackend) UpdateState(ctx context.Context, p tlsissuance.Policy, host string, fn func(*tlsissuance.State) error) error {
	return b.server.store.UpdateTLSIssuanceState(ctx, p.IssuerID, p.DirectoryURL, host, fn)
}

func (b tlsIssuanceBackend) CurrentCertificate(ctx context.Context, d tlsissuance.DomainPolicy) (tlsissuance.Certificate, error) {
	if err := ctx.Err(); err != nil {
		return tlsissuance.Certificate{}, err
	}
	domain, err := b.server.store.GetAppDomain(d.Hostname)
	if err != nil {
		return tlsissuance.Certificate{}, err
	}
	if domain.AppID != d.AppID || domain.TenantID != d.TenantID || domain.Status != model.AppDomainStatusVerified {
		return tlsissuance.Certificate{}, errors.New("certificate owner changed")
	}
	c, err := b.server.store.GetEdgeTLSCertificate(d.Hostname)
	if errors.Is(err, store.ErrNotFound) {
		return tlsissuance.Certificate{}, nil
	}
	if err != nil {
		return tlsissuance.Certificate{}, err
	}
	if c.AppID != d.AppID || c.TenantID != d.TenantID {
		return tlsissuance.Certificate{}, errors.New("shared certificate belongs to different owner")
	}
	result := tlsissuance.Certificate{SHA256: c.CertificateSHA256}
	if leaf, validateErr := tlscertificate.ValidatePublic(d.Hostname, c.CertificatePEM, c.PrivateKeyPEM, time.Now(), nil); validateErr == nil {
		result.NotAfter = leaf.NotAfter
	}
	return result, nil
}

func (b tlsIssuanceBackend) PublishCertificate(ctx context.Context, p tlsissuance.Policy, d tlsissuance.DomainPolicy, previous string, c tlsissuance.Certificate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := b.ReadPolicy(ctx)
	if err != nil {
		return err
	}
	if !current.Enabled || current.Generation != p.Generation {
		return tlsissuance.ErrLeaseLost
	}
	leaf, err := tlscertificate.ValidatePublic(d.Hostname, c.PEM, c.KeyPEM, time.Now(), nil)
	if err != nil {
		return err
	}
	app, err := b.server.store.GetAppMetadata(d.AppID)
	if err != nil {
		return err
	}
	_, err = b.server.store.ImportAppDomainCertificate(model.EdgeTLSCertificate{Hostname: d.Hostname, AppID: d.AppID, TenantID: d.TenantID,
		CertificatePEM: c.PEM, PrivateKeyPEM: c.KeyPEM, CertificateSHA256: c.SHA256, NotAfter: &leaf.NotAfter, IssuerStorage: "fugue-central-acme", MetadataJSON: "{}"}, app.ProjectID, previous, []string{d.Hostname})
	return err
}

func (b tlsIssuanceBackend) PresentDNS(ctx context.Context, p tlsissuance.Policy, d tlsissuance.DomainPolicy, value string, expires time.Time) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	zone, err := b.server.store.GetHostedZoneByName(d.DNSZone)
	if err != nil || zone.TenantID != d.TenantID || zone.Status != "active" || zone.DelegationStatus != "ready" {
		return nil, errors.New("DNS challenge authority changed")
	}
	challenge, err := b.server.store.UpsertDNSACMEChallenge(model.DNSACMEChallenge{Zone: d.DNSZone, Name: "_acme-challenge." + d.Hostname, Value: value, TTL: 30, ExpiresAt: expires, Owner: p.IssuerID, CreatedBy: "tls-issuer"})
	if err != nil {
		return nil, err
	}
	return func() { _, _ = b.server.store.DeleteDNSACMEChallenge(challenge.ID) }, nil
}

func (b tlsIssuanceBackend) VerifyChallenges(ctx context.Context, d tlsissuance.DomainPolicy, token, value string) error {
	deadline, done := context.WithTimeout(ctx, 2*time.Minute)
	defer done()
	targets := append([]string(nil), d.ValidationTargets...)
	if d.Mode == "http-01" {
		// Include every currently advertised A/AAAA entry, including CDN
		// addresses, as well as the explicitly declared direct edge entries.
		addresses, err := net.DefaultResolver.LookupIPAddr(deadline, d.Hostname)
		if err != nil || len(addresses) == 0 {
			return errors.New("public HTTP challenge entries not observed")
		}
		seen := map[string]bool{}
		for _, target := range targets {
			seen[target] = true
		}
		for _, address := range addresses {
			ip := address.IP
			if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				return errors.New("HTTP challenge resolves to nonpublic address")
			}
			if !seen[ip.String()] {
				targets = append(targets, ip.String())
				seen[ip.String()] = true
			}
		}
		if len(targets) > 64 {
			return errors.New("too many public challenge entries")
		}
	}
	for {
		all := true
		for _, target := range targets {
			if err := verifyChallengeTarget(deadline, d, target, token, value); err != nil {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		timer := time.NewTimer(3 * time.Second)
		select {
		case <-deadline.Done():
			timer.Stop()
			return fmt.Errorf("challenge did not propagate to every declared entry: %w", deadline.Err())
		case <-timer.C:
		}
	}
}

func verifyChallengeTarget(ctx context.Context, d tlsissuance.DomainPolicy, target, token, value string) error {
	if d.Mode == "http-01" {
		transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil || host != d.Hostname || (port != "80" && port != "443") {
				return nil, errors.New("challenge redirect destination differs")
			}
			return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", net.JoinHostPort(target, port))
		}}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(next *http.Request, previous []*http.Request) error {
			if len(previous) > 2 || next.URL.Scheme != "https" || next.URL.Hostname() != d.Hostname || next.URL.User != nil || next.URL.Port() != "" && next.URL.Port() != "443" || next.URL.Path != "/.well-known/acme-challenge/"+token {
				return http.ErrUseLastResponse
			}
			return nil
		}}
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+d.Hostname+"/.well-known/acme-challenge/"+token, nil)
		if err != nil {
			return err
		}
		response, err := client.Do(r)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
		if err != nil {
			return err
		}
		if response.StatusCode != 200 || strings.TrimSpace(string(body)) != value {
			return errors.New("HTTP challenge proof differs")
		}
		return nil
	}
	for _, protocol := range []string{"udp", "tcp"} {
		message := new(dns.Msg)
		message.SetQuestion(dns.Fqdn("_acme-challenge."+d.Hostname), dns.TypeTXT)
		message.RecursionDesired = false
		answer, _, err := (&dns.Client{Net: protocol, Timeout: 3 * time.Second}).ExchangeContext(ctx, message, target)
		if err != nil {
			return err
		}
		found := false
		if answer != nil && answer.Rcode == dns.RcodeSuccess && answer.Authoritative && !answer.Truncated {
			for _, record := range answer.Answer {
				if txt, ok := record.(*dns.TXT); ok && strings.EqualFold(txt.Hdr.Name, message.Question[0].Name) && strings.Join(txt.Txt, "") == value {
					found = true
				}
			}
		}
		if !found {
			return errors.New("authoritative DNS challenge proof differs")
		}
	}
	return nil
}

func (s *Server) StartBackgroundTLSIssuance(ctx context.Context) {
	engine := &tlsissuance.Engine{Backend: tlsIssuanceBackend{s}, Owner: model.NewID("issuer"), HTTPClient: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	for {
		if err := engine.Reconcile(ctx); err != nil && !errors.Is(err, tlsissuance.ErrDeferred) && !errors.Is(err, store.ErrConflict) && ctx.Err() == nil {
			s.log.Printf("certificate issuance pass failed class=%s", tlsissuance.FailureClass(err))
		}
		timer := time.NewTimer(time.Minute)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
