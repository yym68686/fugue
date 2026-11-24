package platformconfig

import (
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"reflect"
	"slices"
	"strings"

	"fugue/internal/model"
)

// ExternalPathTarget identifies the platform-side address target of a
// verified domain. The external CNAME itself is never an editable record.
// Callers issuing selections must bind this tuple to the current domain,
// tenant and certificate owner, under the selection transaction.
type ExternalPathTarget struct {
	Hostname string   `json:"hostname"`
	Target   string   `json:"target"`
	TenantID string   `json:"tenant_id"`
	AppID    string   `json:"app_id"`
	Members  []string `json:"members"`
}

// ExternalTargetConflict describes a rejected path-only plan. It is not
// evidence that the candidate was ever selected or served.
type ExternalTargetConflict struct {
	Target             string
	RecordType         string
	Field              string
	ExpectedAddresses  int
	CandidateAddresses int
}

func (e *ExternalTargetConflict) Error() string {
	return fmt.Sprintf("shared DNS target %s %s: different %s (expected addresses=%d candidate addresses=%d)", e.Target, e.RecordType, e.Field, e.ExpectedAddresses, e.CandidateAddresses)
}

// ComposeExternalPathRequirements returns only a dependency overlay. All
// addresses, TTLs, value expirations, topology and sibling probes come from
// the base. This produces no DNS answers and grants no publication authority.
func ComposeExternalPathRequirements(b ExternalPathTarget, base, want *DNSReadinessPlan, policy *DNSReadinessPolicy, before, after []string, baseRR, wantRR []DNSIntent) (*DNSReadinessPlan, HostedReadinessDelta, error) {
	bad := errors.New("external path edit has no unchanged, same-owner physical DNS target")
	if !validDNSConsumerZone(b.Hostname) || !validDNSConsumerZone(b.Target) || b.Hostname == b.Target || b.AppID == "" || b.TenantID == "" || b.AppID != strings.TrimSpace(b.AppID) || b.TenantID != strings.TrimSpace(b.TenantID) ||
		len(b.Members) == 0 || len(b.Members) > 128 || !slices.IsSorted(b.Members) || !slices.Contains(b.Members, b.Hostname) {
		return nil, HostedReadinessDelta{}, bad
	}
	type address struct{ family, value string }
	wire := func(rr []DNSIntent, plan *DNSReadinessPlan) (map[string]DNSIntent, bool) {
		if plan == nil || len(rr) > 10000 {
			return nil, false
		}
		rows := map[string]DNSIntent{}
		ips := map[address]bool{}
		for _, r := range rr {
			if r.Hostname == b.Hostname {
				return nil, false
			} // no platform zone ownership
			if r.Hostname != b.Target {
				continue
			}
			if r.Type != "A" && r.Type != "AAAA" {
				if r.Type == "TXT" || r.Type == "MX" || r.Type == "CAA" {
					continue
				}
				return nil, false
			}
			if _, dup := rows[r.Type]; dup || r.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget || r.TenantID != b.TenantID || r.AppID != b.AppID || ValidateDNSIntents([]DNSIntent{r}) != nil {
				return nil, false
			}
			for _, v := range r.Values {
				ip, e := netip.ParseAddr(v)
				k := address{r.Type, v}
				if e != nil || !PublicDNSFlattenIP(ip) || ip.String() != v || (r.Type == "A") != ip.Is4() || ips[k] {
					return nil, false
				}
				ips[k] = true
			}
			rows[r.Type] = r
		}
		seen := map[address]bool{}
		for _, r := range plan.Records {
			if r.Hostname != b.Target {
				continue
			}
			for _, t := range r.Targets {
				k := address{t.Family, t.Address}
				if seen[k] {
					return nil, false
				}
				seen[k] = true
			}
		}
		return rows, len(rows) > 0 && maps.Equal(ips, seen)
	}
	left, ok := wire(baseRR, base)
	right, yes := wire(wantRR, want)
	if !ok || !yes {
		return nil, HostedReadinessDelta{}, bad
	}
	for _, kind := range []string{"A", "AAAA"} {
		x, a := left[kind]
		y, present := right[kind]
		field := ""
		switch {
		case a != present:
			field = "record_presence"
		case !slices.Equal(slices.Sorted(slices.Values(x.Values)), slices.Sorted(slices.Values(y.Values))):
			field = "addresses"
		case x.TTL != y.TTL:
			field = "ttl"
		case !reflect.DeepEqual(x, y):
			field = "record_metadata"
		}
		if field != "" {
			return nil, HostedReadinessDelta{}, errors.Join(bad, &ExternalTargetConflict{b.Target, kind, field, len(x.Values), len(y.Values)})
		}
	}
	return composeHostedReadinessAt(b.Hostname, b.Target, base, want, policy, before, after)
}
