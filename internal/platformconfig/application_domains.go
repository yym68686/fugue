package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// ApplicationDomainsIntent defines desired namespace membership. The business
// producer expands it to concrete routes and records before the pure compiler.
// A nil value is retained only for legacy migration inputs.
type ApplicationDomainsIntent struct {
	AppBaseDomain          string   `json:"app_base_domain"`
	CustomDomainBaseDomain string   `json:"custom_domain_base_domain"`
	ReservedHostnames      []string `json:"reserved_hostnames"`
	DefaultDNSTTL          int      `json:"default_dns_ttl"`
}

// Missing and null declarations must not silently become disabled namespaces.
func (in *ApplicationDomainsIntent) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	required := []string{"app_base_domain", "custom_domain_base_domain", "reserved_hostnames", "default_dns_ttl"}
	if len(fields) != len(required) {
		return fmt.Errorf("application domain declaration requires exactly four fields")
	}
	for _, key := range required {
		value, ok := fields[key]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("application domain declaration requires %s", key)
		}
	}
	type plain ApplicationDomainsIntent
	var value plain
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	*in = ApplicationDomainsIntent(value)
	return ValidateApplicationDomains(in)
}

func CloneApplicationDomains(in *ApplicationDomainsIntent) *ApplicationDomainsIntent {
	if in == nil {
		return nil
	}
	out := *in
	out.ReservedHostnames = append([]string{}, in.ReservedHostnames...)
	slices.Sort(out.ReservedHostnames)
	return &out
}

func ValidateApplicationDomains(in *ApplicationDomainsIntent) error {
	if in == nil {
		return nil
	}
	valid := func(host string) bool {
		if len(host) > 253 || host == "" || host != strings.ToLower(host) {
			return false
		}
		for _, label := range strings.Split(host, ".") {
			if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return false
			}
			for _, c := range label {
				if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
					return false
				}
			}
		}
		return true
	}
	if in.DefaultDNSTTL < 1 || in.DefaultDNSTTL > 86400 || len(in.ReservedHostnames) > 4096 {
		return fmt.Errorf("application domain TTL or namespace bound invalid")
	}
	for _, host := range []string{in.AppBaseDomain, in.CustomDomainBaseDomain} {
		if host != "" && !valid(host) {
			return fmt.Errorf("application base domain must be canonical")
		}
	}
	seen := map[string]bool{}
	for _, host := range in.ReservedHostnames {
		if !valid(host) || seen[host] {
			return fmt.Errorf("reserved application hostname invalid or duplicated")
		}
		seen[host] = true
	}
	return nil
}
