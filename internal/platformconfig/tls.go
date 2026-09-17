package platformconfig

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

// TLSDomainObservation preserves persisted lifecycle facts. Ready is the last
// recorded TLS state, not a new certificate probe; DNS placement independently
// requires positive route/TLS evidence with its own finite lease.
type TLSDomainObservation struct {
	Ref              string     `json:"ref"`
	Hostname         string     `json:"hostname"`
	AppID            string     `json:"app_id"`
	TenantID         string     `json:"tenant_id"`
	Status           string     `json:"status"`
	TLSStatus        string     `json:"tls_status,omitempty"`
	VerifiedAt       *time.Time `json:"verified_at,omitempty"`
	TLSLastCheckedAt *time.Time `json:"tls_last_checked_at,omitempty"`
	TLSReadyAt       *time.Time `json:"tls_ready_at,omitempty"`
}

func ValidateTLSIntents(tls []TLSIntent, routes []RouteIntent) error {
	if len(tls) > 10000 {
		return fmt.Errorf("too many TLS intents")
	}
	seen, refs := map[string]bool{}, map[string]bool{}
	owners := tlsDomainRouteOwners(routes)
	for _, desired := range tls {
		if desired.Hostname == "" || desired.Hostname != strings.Trim(strings.ToLower(strings.TrimSpace(desired.Hostname)), ".") ||
			desired.Policy == "" || desired.Policy != strings.TrimSpace(desired.Policy) || seen[desired.Hostname] {
			return fmt.Errorf("TLS intent identity is invalid")
		}
		seen[desired.Hostname] = true
		if desired.DomainRef == "" {
			if desired.AppID != "" || desired.TenantID != "" {
				return fmt.Errorf("TLS owner requires a domain reference")
			}
			continue
		}
		if desired.DomainRef != strings.TrimSpace(desired.DomainRef) || refs[desired.DomainRef] ||
			desired.AppID == "" || desired.TenantID == "" || desired.AppID != strings.TrimSpace(desired.AppID) || desired.TenantID != strings.TrimSpace(desired.TenantID) ||
			!owners[desired.Hostname].matches(desired.AppID, desired.TenantID) {
			return fmt.Errorf("TLS domain binding does not match route ownership")
		}
		refs[desired.DomainRef] = true
	}
	return nil
}

type tlsRouteOwners struct {
	tenant      string
	apps        map[string]bool
	conflicting bool
}

func (owner tlsRouteOwners) matches(app, tenant string) bool {
	return !owner.conflicting && owner.tenant == tenant && owner.apps[app]
}

func tlsDomainRouteOwners(routes []RouteIntent) map[string]tlsRouteOwners {
	owners := make(map[string]tlsRouteOwners)
	for _, route := range routes {
		host := strings.Trim(strings.ToLower(strings.TrimSpace(route.Hostname)), ".")
		owner, exists := owners[host]
		if !exists {
			owner = tlsRouteOwners{tenant: route.TenantID, apps: map[string]bool{}}
		}
		owner.conflicting = owner.conflicting || owner.tenant != route.TenantID
		owner.apps[route.AppID] = true
		owners[host] = owner
	}
	return owners
}

func CompileTLSDomains(intent PlatformIntent, snapshot RuntimeSnapshot) ([]model.EdgeTLSAllowlistEntry, error) {
	if err := ValidateTLSIntents(intent.TLS, intent.Routes); err != nil {
		return nil, err
	}
	if len(snapshot.TLSDomains) > 10000 || len(snapshot.TLSDomains) > 0 && (snapshot.CapturedAt == nil || snapshot.CapturedAt.IsZero()) {
		return nil, fmt.Errorf("TLS domain facts require a bounded fixed snapshot")
	}
	byRef := make(map[string]TLSDomainObservation, len(snapshot.TLSDomains))
	for _, fact := range snapshot.TLSDomains {
		if fact.Ref == "" || fact.Ref != strings.TrimSpace(fact.Ref) {
			return nil, fmt.Errorf("TLS domain fact reference is invalid")
		}
		if _, exists := byRef[fact.Ref]; exists {
			return nil, fmt.Errorf("duplicate TLS domain fact")
		}
		if fact.Status != model.AppDomainStatusPending && fact.Status != model.AppDomainStatusVerified ||
			fact.TLSStatus != "" && model.NormalizeAppDomainTLSStatus(fact.TLSStatus) != fact.TLSStatus {
			return nil, fmt.Errorf("TLS domain fact lifecycle is invalid")
		}
		for _, at := range []*time.Time{fact.VerifiedAt, fact.TLSLastCheckedAt, fact.TLSReadyAt} {
			if at != nil && (at.IsZero() || at.After(*snapshot.CapturedAt)) {
				return nil, fmt.Errorf("TLS domain event time is invalid")
			}
		}
		if fact.Status == model.AppDomainStatusVerified && fact.VerifiedAt == nil ||
			fact.TLSStatus == model.AppDomainTLSStatusReady && (fact.Status != model.AppDomainStatusVerified || fact.TLSReadyAt == nil) {
			return nil, fmt.Errorf("TLS domain lifecycle lacks original verification evidence")
		}
		byRef[fact.Ref] = fact
	}
	allowlist := []model.EdgeTLSAllowlistEntry{}
	for _, desired := range intent.TLS {
		if desired.DomainRef == "" {
			continue
		}
		fact, exists := byRef[desired.DomainRef]
		if !exists || fact.Hostname != desired.Hostname || fact.AppID != desired.AppID || fact.TenantID != desired.TenantID {
			return nil, fmt.Errorf("TLS domain fact does not match exact intent binding")
		}
		delete(byRef, desired.DomainRef)
		allowlist = append(allowlist, model.EdgeTLSAllowlistEntry{Hostname: fact.Hostname, AppID: fact.AppID, TenantID: fact.TenantID, Status: fact.Status, TLSStatus: fact.TLSStatus})
	}
	if len(byRef) != 0 {
		return nil, fmt.Errorf("TLS domain facts contain unreferenced authorization")
	}
	sort.Slice(allowlist, func(i, j int) bool { return allowlist[i].Hostname < allowlist[j].Hostname })
	return allowlist, nil
}

// ValidateTLSAllowlist binds the signed compiler output to its route owners.
// It validates lifecycle values without claiming fresh certificate readiness.
func ValidateTLSAllowlist(entries []model.EdgeTLSAllowlistEntry, routes []RouteIntent) error {
	if len(entries) > 10000 {
		return fmt.Errorf("TLS allowlist exceeds its bound")
	}
	seen := map[string]bool{}
	owners := tlsDomainRouteOwners(routes)
	for _, entry := range entries {
		if entry.Hostname == "" || entry.Hostname != strings.Trim(strings.ToLower(strings.TrimSpace(entry.Hostname)), ".") || seen[entry.Hostname] ||
			entry.AppID == "" || entry.TenantID == "" || !owners[entry.Hostname].matches(entry.AppID, entry.TenantID) ||
			(entry.Status != model.AppDomainStatusVerified && entry.Status != model.AppDomainStatusPending) ||
			(entry.TLSStatus != "" && model.NormalizeAppDomainTLSStatus(entry.TLSStatus) != entry.TLSStatus) ||
			(entry.TLSStatus == model.AppDomainTLSStatusReady && entry.Status != model.AppDomainStatusVerified) {
			return fmt.Errorf("TLS allowlist entry is not bound to valid route ownership")
		}
		seen[entry.Hostname] = true
	}
	return nil
}
