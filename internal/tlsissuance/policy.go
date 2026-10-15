// Package tlsissuance separates certificate issuance intent and durable runtime
// facts from serving consumers. Existing certificates are never removed on error.
package tlsissuance

import (
	"errors"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type Policy struct {
	Schema           string         `json:"schema"`
	Generation       int64          `json:"generation"`
	Enabled          bool           `json:"enabled"`
	DirectoryURL     string         `json:"directory_url"`
	IssuerID         string         `json:"issuer_id"`
	Contact          string         `json:"contact"`
	RenewBeforeHours int            `json:"renew_before_hours"`
	RetryBaseSeconds int            `json:"retry_base_seconds"`
	Domains          []DomainPolicy `json:"domains"`
}

type DomainPolicy struct {
	Hostname          string     `json:"hostname"`
	AppID             string     `json:"app_id"`
	TenantID          string     `json:"tenant_id"`
	Mode              string     `json:"mode"`
	DNSZone           string     `json:"dns_zone,omitempty"`
	ValidationTargets []string   `json:"validation_targets,omitempty"`
	NotBefore         *time.Time `json:"not_before,omitempty"`
}

var hostnamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$`)
var identifierPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)
var tokenPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{16,256}$`)

func ValidHostname(host string) bool {
	if len(host) > 253 || !strings.Contains(host, ".") || !hostnamePattern.MatchString(host) {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return false
		}
	}
	return true
}

func ValidToken(token string) bool { return tokenPattern.MatchString(token) }

func (p Policy) Validate() error {
	u, err := url.Parse(p.DirectoryURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		p.Schema != "fugue.tls-issuance-policy/v1" || p.Generation < 1 || !identifierPattern.MatchString(p.IssuerID) ||
		p.Contact != "" && !strings.HasPrefix(p.Contact, "mailto:") || strings.ContainsAny(p.Contact, "\r\n ") ||
		p.RenewBeforeHours < 168 || p.RenewBeforeHours > 1440 || p.RetryBaseSeconds < 3600 || p.RetryBaseSeconds > 86400 || len(p.Domains) > 10000 {
		return errors.New("invalid certificate issuance policy")
	}
	seen := map[string]bool{}
	for _, d := range p.Domains {
		if !ValidHostname(d.Hostname) || d.AppID == "" || d.TenantID == "" || seen[d.Hostname] || len(d.ValidationTargets) > 32 {
			return errors.New("invalid or duplicate certificate owner")
		}
		seen[d.Hostname] = true
		switch d.Mode {
		case "external":
			if d.DNSZone != "" || len(d.ValidationTargets) != 0 {
				return errors.New("external issuer cannot configure local challenges")
			}
		case "http-01", "dns-01":
			if len(d.ValidationTargets) == 0 {
				return errors.New("all validation entries must be explicitly declared")
			}
			if d.Mode == "dns-01" && (!ValidHostname(d.DNSZone) || d.Hostname != d.DNSZone && !strings.HasSuffix(d.Hostname, "."+d.DNSZone)) {
				return errors.New("DNS challenge zone does not own hostname")
			}
			for _, target := range d.ValidationTargets {
				host := target
				if d.Mode == "dns-01" {
					var port string
					host, port, err = net.SplitHostPort(target)
					if err != nil || port != "53" {
						return errors.New("authoritative DNS target must use port 53")
					}
				}
				ip := net.ParseIP(host)
				if ip == nil || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
					return errors.New("invalid explicit challenge target")
				}
			}
		default:
			return errors.New("unsupported certificate challenge mode")
		}
	}
	return nil
}

func (p Policy) Domain(host string) (DomainPolicy, bool) {
	for _, d := range p.Domains {
		if d.Hostname == host {
			return d, true
		}
	}
	return DomainPolicy{}, false
}

// State is private issuance state, never a serving-authority artifact. The
// lease exceeds a bounded issuance attempt; crash recovery retains backoff.
type State struct {
	Owner            string    `json:"owner,omitempty"`
	LeaseUntil       time.Time `json:"lease_until,omitempty"`
	NextAttempt      time.Time `json:"next_attempt,omitempty"`
	Failures         int       `json:"failures,omitempty"`
	Token            string    `json:"token,omitempty"`
	KeyAuthorization string    `json:"key_authorization,omitempty"`
	ChallengeExpires time.Time `json:"challenge_expires,omitempty"`
	AccountKeyPEM    string    `json:"account_key_pem,omitempty"`
	AccountURI       string    `json:"account_uri,omitempty"`
	LastError        string    `json:"last_error,omitempty"`
	LastSuccess      time.Time `json:"last_success,omitempty"`
}
