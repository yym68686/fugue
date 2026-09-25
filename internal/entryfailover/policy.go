// Package entryfailover implements the independent, bounded DNS entry selector.
// It does not depend on Fugue's API, database, route publisher or edge manager.
package entryfailover

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"
	"time"
)

const PolicySchema = "fugue.entry-failover/v1"

var dnsName = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$`)
var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}$`)
var remoteKeyPath = regexp.MustCompile(`^/[a-zA-Z0-9_./-]{1,255}$`)

type Check struct {
	Hostname string `json:"hostname"`
	Path     string `json:"path"`
	Status   int    `json:"status"`
}

type Target struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"` // static-ip or fugue-domain
	Address  string `json:"address"`
	EdgeID   string `json:"edge_id,omitempty"`
	Priority int    `json:"priority"`
}

type Vantage struct {
	ID            string `json:"id"`
	Transport     string `json:"transport"` // local or ssh
	SSHHost       string `json:"ssh_host,omitempty"`
	BinaryPath    string `json:"binary_path,omitempty"`
	PublicKeyPath string `json:"public_key_path,omitempty"`
}

type Policy struct {
	Schema              string    `json:"schema"`
	PoolID              string    `json:"pool_id"`
	TenantID            string    `json:"tenant_id"`
	ProjectID           string    `json:"project_id"`
	Generation          uint64    `json:"generation"`
	ExpiresAt           time.Time `json:"expires_at"`
	Zone                string    `json:"zone"`
	ZoneID              string    `json:"zone_id"`
	Hostnames           []string  `json:"hostnames"`
	DNSBaseline         []Record  `json:"dns_baseline"`
	Checks              []Check   `json:"checks"`
	Targets             []Target  `json:"targets"`
	Vantages            []Vantage `json:"vantages"`
	Mode                string    `json:"mode"` // off, shadow or automatic
	FailureThreshold    int       `json:"failure_threshold"`
	SuccessThreshold    int       `json:"success_threshold"`
	IntervalSeconds     int       `json:"interval_seconds"`
	ProbeTimeoutSecs    int       `json:"probe_timeout_seconds"`
	MinTLSValidityHours int       `json:"min_tls_validity_hours"`
	Failback            string    `json:"failback"` // manual or automatic
}

func (p Policy) Validate() error {
	if p.Schema != PolicySchema || !identifier.MatchString(p.PoolID) || !identifier.MatchString(p.TenantID) || !identifier.MatchString(p.ProjectID) || p.Generation == 0 || p.ExpiresAt.IsZero() || p.ZoneID == "" || len(p.ZoneID) > 128 {
		return errors.New("supported schema, tenant/project/pool identity, generation, expiry and zone_id required")
	}
	if !validName(p.Zone) || len(p.Hostnames) == 0 || len(p.Hostnames) > 16 {
		return errors.New("valid zone and 1-16 hostnames required")
	}
	if p.Mode != "off" && p.Mode != "shadow" && p.Mode != "automatic" {
		return errors.New("mode must be off, shadow or automatic")
	}
	if p.Failback != "manual" && p.Failback != "automatic" {
		return errors.New("failback must be manual or automatic")
	}
	if p.FailureThreshold < 2 || p.FailureThreshold > 20 || p.SuccessThreshold < 2 || p.SuccessThreshold > 20 || p.IntervalSeconds < 5 || p.IntervalSeconds > 300 || p.ProbeTimeoutSecs < 1 || p.ProbeTimeoutSecs > 20 || p.ProbeTimeoutSecs >= p.IntervalSeconds {
		return errors.New("probe interval, timeout and consecutive thresholds are outside bounds")
	}
	if p.MinTLSValidityHours < 24 || p.MinTLSValidityHours > 720 {
		return errors.New("minimum TLS validity must be 24-720 hours")
	}
	hosts := map[string]bool{}
	for _, h := range p.Hostnames {
		if !validName(h) || (h != p.Zone && !strings.HasSuffix(h, "."+p.Zone)) || hosts[h] {
			return errors.New("hostnames must be unique, normalized and inside the zone")
		}
		hosts[h] = true
	}
	if len(p.DNSBaseline) != len(hosts) {
		return errors.New("one exact signed DNS baseline record per hostname required")
	}
	baselineNames, baselineIDs := map[string]bool{}, map[string]bool{}
	for _, r := range p.DNSBaseline {
		if !hosts[r.Name] || baselineNames[r.Name] || r.ID == "" || baselineIDs[r.ID] || r.Proxied || r.TTL < 1 || (r.Type != "A" && r.Type != "CNAME") || r.Content == "" {
			return errors.New("DNS baseline requires exact unique record IDs and DNS-only A/CNAME state")
		}
		baselineNames[r.Name], baselineIDs[r.ID] = true, true
	}
	if len(p.Checks) < len(hosts) || len(p.Checks) > 32 {
		return errors.New("at least one and at most 32 checks required for each hostname")
	}
	checked := map[string]bool{}
	for _, c := range p.Checks {
		if !hosts[c.Hostname] || !strings.HasPrefix(c.Path, "/") || strings.HasPrefix(c.Path, "//") || strings.ContainsAny(c.Path, "?#\r\n") || len(c.Path) > 256 || c.Status < 200 || c.Status > 399 {
			return errors.New("checks require an allowlisted hostname, safe path and HTTP 2xx/3xx status")
		}
		checked[c.Hostname] = true
	}
	if len(checked) != len(hosts) || len(p.Targets) < 2 || len(p.Targets) > 16 {
		return errors.New("every hostname needs a check and 2-16 targets are required")
	}
	if len(p.Vantages) == 0 || len(p.Vantages) > 8 || (p.Mode == "automatic" && len(p.Vantages) < 2) {
		return errors.New("1-8 vantages required; automatic mode needs at least two")
	}
	vantages, sshHosts, local := map[string]bool{}, map[string]bool{}, 0
	for _, v := range p.Vantages {
		if !identifier.MatchString(v.ID) || vantages[v.ID] {
			return errors.New("unique vantage IDs required")
		}
		vantages[v.ID] = true
		switch v.Transport {
		case "local":
			if v.SSHHost != "" || v.PublicKeyPath != "" || v.BinaryPath != "" {
				return errors.New("local vantage cannot have SSH host")
			}
			local++
		case "ssh":
			if !identifier.MatchString(v.SSHHost) || sshHosts[v.SSHHost] || !remoteKeyPath.MatchString(v.PublicKeyPath) || !remoteKeyPath.MatchString(v.BinaryPath) {
				return errors.New("unique safe SSH aliases required")
			}
			sshHosts[v.SSHHost] = true
		default:
			return errors.New("vantage transport must be local or ssh")
		}
	}
	if local != 1 {
		return errors.New("exactly one local vantage required")
	}
	ids, priorities, addresses := map[string]bool{}, map[int]bool{}, map[string]bool{}
	for _, t := range p.Targets {
		if !identifier.MatchString(t.ID) || ids[t.ID] || t.Priority < 0 || t.Priority > 10000 || priorities[t.Priority] || addresses[t.Kind+"/"+t.Address] {
			return errors.New("targets require unique IDs, priorities and addresses")
		}
		ids[t.ID], priorities[t.Priority], addresses[t.Kind+"/"+t.Address] = true, true, true
		switch t.Kind {
		case "static-ip":
			ip := net.ParseIP(t.Address)
			if ip == nil || ip.To4() == nil || ip.String() != t.Address || ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() || !identifier.MatchString(t.EdgeID) {
				return fmt.Errorf("target %s requires a public IPv4 address", t.ID)
			}
		case "fugue-domain":
			if !validName(t.Address) || hosts[t.Address] || t.Address == p.Zone || strings.HasSuffix(t.Address, "."+p.Zone) || t.EdgeID != "" {
				return fmt.Errorf("target %s requires an independent canonical DNS name", t.ID)
			}
		default:
			return fmt.Errorf("target %s has unsupported kind", t.ID)
		}
	}
	baselineTarget := ""
	for _, r := range p.DNSBaseline {
		matched := ""
		for _, target := range p.Targets {
			expected := targetRecord(r, target)
			if r.Type == expected.Type && r.Content == expected.Content {
				matched = target.ID
				break
			}
		}
		if matched == "" || (baselineTarget != "" && baselineTarget != matched) {
			return errors.New("signed DNS baseline must select one allowed target for all business hostnames")
		}
		baselineTarget = matched
	}
	return nil
}

func validName(s string) bool { return len(s) <= 253 && dnsName.MatchString(s) }

func (p Policy) SortedTargets() []Target {
	out := append([]Target(nil), p.Targets...)
	sort.Slice(out, func(i, j int) bool { return out[i].Priority < out[j].Priority })
	return out
}

func (p Policy) Digest() string {
	raw, _ := json.Marshal(p)
	h := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(h[:])
}

func (p Policy) Interval() time.Duration { return time.Duration(p.IntervalSeconds) * time.Second }
func (p Policy) ProbeTimeout() time.Duration {
	return time.Duration(p.ProbeTimeoutSecs) * time.Second
}
