package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) ListAppDomains(appID string) ([]model.AppDomain, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListAppDomains(appID)
	}

	domains := make([]model.AppDomain, 0)
	err := s.withLockedState(false, func(state *model.State) error {
		for _, domain := range state.AppDomains {
			if domain.AppID != appID {
				continue
			}
			domains = append(domains, cloneAppDomain(domain))
		}
		sortAppDomains(domains)
		return nil
	})
	return domains, err
}

func (s *Store) ListVerifiedAppDomains() ([]model.AppDomain, error) {
	if s.usingDatabase() {
		return s.pgListVerifiedAppDomains()
	}

	domains := make([]model.AppDomain, 0)
	err := s.withLockedState(false, func(state *model.State) error {
		for _, domain := range state.AppDomains {
			if domain.Status != model.AppDomainStatusVerified {
				continue
			}
			domains = append(domains, cloneAppDomain(domain))
		}
		sort.Slice(domains, func(i, j int) bool {
			return domains[i].Hostname < domains[j].Hostname
		})
		return nil
	})
	return domains, err
}

func (s *Store) GetAppDomain(hostname string) (model.AppDomain, error) {
	hostname = normalizeAppDomainHostname(hostname)
	if hostname == "" {
		return model.AppDomain{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetAppDomain(hostname)
	}

	var domain model.AppDomain
	err := s.withLockedState(false, func(state *model.State) error {
		index := findAppDomain(state, hostname)
		if index < 0 {
			return ErrNotFound
		}
		domain = cloneAppDomain(state.AppDomains[index])
		return nil
	})
	return domain, err
}

func (s *Store) PutAppDomain(domain model.AppDomain) (model.AppDomain, error) {
	domain.Hostname = normalizeAppDomainHostname(domain.Hostname)
	domain.AppID = strings.TrimSpace(domain.AppID)
	domain.TenantID = strings.TrimSpace(domain.TenantID)
	domain.Status = normalizeAppDomainStatus(domain.Status)
	domain.DNSStatus = model.NormalizeAppDomainDNSStatus(domain.DNSStatus)
	domain.DNSRecordKind = model.NormalizeAppDomainDNSRecordKind(domain.DNSRecordKind)
	domain.TLSStatus = model.NormalizeAppDomainTLSStatus(domain.TLSStatus)
	domain.VerificationTXTName = normalizeTXTRecordName(domain.VerificationTXTName)
	domain.VerificationTXTValue = strings.TrimSpace(domain.VerificationTXTValue)
	domain.RouteTarget = normalizeAppDomainHostname(domain.RouteTarget)
	domain.LastMessage = strings.TrimSpace(domain.LastMessage)
	domain.DNSLastMessage = strings.TrimSpace(domain.DNSLastMessage)
	domain.TLSLastMessage = strings.TrimSpace(domain.TLSLastMessage)
	if domain.Hostname == "" || domain.AppID == "" {
		return model.AppDomain{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgPutAppDomain(domain)
	}

	var out model.AppDomain
	err := s.withLockedState(true, func(state *model.State) error {
		appIndex := findApp(state, domain.AppID)
		if appIndex < 0 {
			return ErrNotFound
		}
		app := state.Apps[appIndex]
		if isDeletedApp(app) {
			return ErrNotFound
		}
		if domain.TenantID == "" {
			domain.TenantID = app.TenantID
		}
		if domain.TenantID != app.TenantID {
			return ErrInvalidInput
		}
		for _, existingApp := range state.Apps {
			if isDeletedApp(existingApp) || existingApp.Route == nil {
				continue
			}
			if existingApp.ID == domain.AppID {
				continue
			}
			if appRouteClaimsHostnameRoot(existingApp.Route) && strings.EqualFold(strings.TrimSpace(existingApp.Route.Hostname), domain.Hostname) {
				return ErrConflict
			}
		}

		now := time.Now().UTC()
		index := findAppDomain(state, domain.Hostname)
		if index >= 0 {
			existing := state.AppDomains[index]
			if existing.AppID != domain.AppID {
				return ErrConflict
			}
			if domain.CreatedAt.IsZero() {
				domain.CreatedAt = existing.CreatedAt
			}
		} else if domain.CreatedAt.IsZero() {
			domain.CreatedAt = now
		}
		domain.UpdatedAt = now
		if domain.Status == model.AppDomainStatusVerified && domain.VerifiedAt == nil {
			verifiedAt := now
			domain.VerifiedAt = &verifiedAt
		}
		if domain.Status == model.AppDomainStatusVerified {
			if domain.DNSStatus == "" {
				domain.DNSStatus = model.AppDomainDNSStatusReady
			}
			if domain.DNSRecordKind == "" {
				domain.DNSRecordKind = model.AppDomainDNSRecordKindCNAME
			}
			if domain.TLSStatus == "" {
				domain.TLSStatus = model.AppDomainTLSStatusPending
			}
			if domain.TLSStatus == model.AppDomainTLSStatusReady {
				if domain.TLSReadyAt == nil {
					readyAt := now
					domain.TLSReadyAt = &readyAt
				}
			} else {
				domain.TLSReadyAt = nil
			}
		} else {
			if domain.DNSStatus == "" {
				domain.DNSStatus = model.AppDomainDNSStatusPending
			}
			if domain.DNSRecordKind == "" {
				domain.DNSRecordKind = model.AppDomainDNSRecordKindNone
			}
			domain.TLSStatus = ""
			domain.DNSLastMessage = strings.TrimSpace(domain.DNSLastMessage)
			domain.TLSLastMessage = ""
			domain.TLSLastCheckedAt = nil
			domain.TLSReadyAt = nil
		}

		if index >= 0 {
			state.AppDomains[index] = cloneAppDomain(domain)
		} else {
			state.AppDomains = append(state.AppDomains, cloneAppDomain(domain))
		}
		out = cloneAppDomain(domain)
		return nil
	})
	return out, err
}

func (s *Store) DeleteAppDomain(appID, hostname string) (model.AppDomain, error) {
	appID = strings.TrimSpace(appID)
	hostname = normalizeAppDomainHostname(hostname)
	if appID == "" || hostname == "" {
		return model.AppDomain{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteAppDomain(appID, hostname)
	}

	var removed model.AppDomain
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAppDomain(state, hostname)
		if index < 0 {
			return ErrNotFound
		}
		removed = cloneAppDomain(state.AppDomains[index])
		if removed.AppID != appID {
			return ErrNotFound
		}
		state.AppDomains = append(state.AppDomains[:index], state.AppDomains[index+1:]...)
		deleteEdgeTLSCertificateInState(state, hostname)
		return nil
	})
	return removed, err
}

func deleteAppDomainsByApp(state *model.State, appID string) {
	if state == nil || appID == "" || len(state.AppDomains) == 0 {
		return
	}
	filtered := state.AppDomains[:0]
	removedHosts := make(map[string]struct{})
	for _, domain := range state.AppDomains {
		if domain.AppID == appID {
			if host := normalizeAppDomainHostname(domain.Hostname); host != "" {
				removedHosts[host] = struct{}{}
			}
			continue
		}
		filtered = append(filtered, domain)
	}
	state.AppDomains = filtered
	if len(removedHosts) > 0 && len(state.EdgeTLSCertificates) > 0 {
		certs := state.EdgeTLSCertificates[:0]
		for _, cert := range state.EdgeTLSCertificates {
			if _, ok := removedHosts[normalizeAppDomainHostname(cert.Hostname)]; ok {
				continue
			}
			if strings.TrimSpace(cert.AppID) == appID {
				continue
			}
			certs = append(certs, cert)
		}
		state.EdgeTLSCertificates = certs
	}
}

func activeAppDomainByHostname(state *model.State, hostname string) (model.AppDomain, bool) {
	if state == nil {
		return model.AppDomain{}, false
	}
	hostname = normalizeAppDomainHostname(hostname)
	for _, domain := range state.AppDomains {
		if domain.Status != model.AppDomainStatusVerified {
			continue
		}
		if strings.EqualFold(domain.Hostname, hostname) {
			return cloneAppDomain(domain), true
		}
	}
	return model.AppDomain{}, false
}

func findAppDomain(state *model.State, hostname string) int {
	if state == nil {
		return -1
	}
	hostname = normalizeAppDomainHostname(hostname)
	for index, domain := range state.AppDomains {
		if strings.EqualFold(domain.Hostname, hostname) {
			return index
		}
	}
	return -1
}

func normalizeAppDomainHostname(hostname string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(hostname)), ".")
}

func normalizeTXTRecordName(name string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(name)), ".")
}

func normalizeAppDomainStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case model.AppDomainStatusVerified:
		return model.AppDomainStatusVerified
	default:
		return model.AppDomainStatusPending
	}
}

func appRouteClaimsHostnameRoot(route *model.AppRoute) bool {
	return route != nil &&
		normalizeAppDomainHostname(route.Hostname) != "" &&
		model.NormalizeAppRoutePathPrefix(route.PathPrefix) == "/"
}

func appRouteConflictsWithVerifiedAppDomain(state *model.State, route *model.AppRoute, exceptAppID string) bool {
	if state == nil || !appRouteClaimsHostnameRoot(route) {
		return false
	}
	hostname := normalizeAppDomainHostname(route.Hostname)
	exceptAppID = strings.TrimSpace(exceptAppID)
	for _, domain := range state.AppDomains {
		if domain.Status != model.AppDomainStatusVerified {
			continue
		}
		if exceptAppID != "" && domain.AppID == exceptAppID {
			continue
		}
		if strings.EqualFold(normalizeAppDomainHostname(domain.Hostname), hostname) {
			return true
		}
	}
	return false
}

func sortAppDomains(domains []model.AppDomain) {
	sort.Slice(domains, func(i, j int) bool {
		if domains[i].CreatedAt.Equal(domains[j].CreatedAt) {
			return domains[i].Hostname < domains[j].Hostname
		}
		return domains[i].CreatedAt.Before(domains[j].CreatedAt)
	})
}

func cloneAppDomain(in model.AppDomain) model.AppDomain {
	out := in
	out.DNSStatus = model.NormalizeAppDomainDNSStatus(out.DNSStatus)
	if out.DNSStatus == "" {
		if out.Status == model.AppDomainStatusVerified {
			out.DNSStatus = model.AppDomainDNSStatusReady
		} else {
			out.DNSStatus = model.AppDomainDNSStatusPending
		}
	}
	out.DNSRecordKind = model.NormalizeAppDomainDNSRecordKind(out.DNSRecordKind)
	if out.DNSRecordKind == "" {
		if out.Status == model.AppDomainStatusVerified {
			out.DNSRecordKind = model.AppDomainDNSRecordKindCNAME
		} else {
			out.DNSRecordKind = model.AppDomainDNSRecordKindNone
		}
	}
	out.TLSStatus = model.NormalizeAppDomainTLSStatus(out.TLSStatus)
	if out.Status == model.AppDomainStatusVerified && out.TLSStatus == "" {
		out.TLSStatus = model.AppDomainTLSStatusPending
	}
	if in.LastCheckedAt != nil {
		lastCheckedAt := *in.LastCheckedAt
		out.LastCheckedAt = &lastCheckedAt
	}
	if in.DNSLastCheckedAt != nil {
		dnsLastCheckedAt := *in.DNSLastCheckedAt
		out.DNSLastCheckedAt = &dnsLastCheckedAt
	}
	if in.VerifiedAt != nil {
		verifiedAt := *in.VerifiedAt
		out.VerifiedAt = &verifiedAt
	}
	if in.TLSLastCheckedAt != nil {
		tlsLastCheckedAt := *in.TLSLastCheckedAt
		out.TLSLastCheckedAt = &tlsLastCheckedAt
	}
	if in.TLSReadyAt != nil {
		tlsReadyAt := *in.TLSReadyAt
		out.TLSReadyAt = &tlsReadyAt
	}
	return out
}
