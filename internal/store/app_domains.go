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
	domain.VerificationTXTName = normalizeTXTRecordName(domain.VerificationTXTName)
	domain.VerificationTXTValue = strings.TrimSpace(domain.VerificationTXTValue)
	domain.RouteTarget = normalizeAppDomainHostname(domain.RouteTarget)
	domain.LastMessage = strings.TrimSpace(domain.LastMessage)
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
			if strings.EqualFold(strings.TrimSpace(existingApp.Route.Hostname), domain.Hostname) {
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
		return nil
	})
	return removed, err
}

func deleteAppDomainsByApp(state *model.State, appID string) {
	if state == nil || appID == "" || len(state.AppDomains) == 0 {
		return
	}
	filtered := state.AppDomains[:0]
	for _, domain := range state.AppDomains {
		if domain.AppID == appID {
			continue
		}
		filtered = append(filtered, domain)
	}
	state.AppDomains = filtered
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
	if in.LastCheckedAt != nil {
		lastCheckedAt := *in.LastCheckedAt
		out.LastCheckedAt = &lastCheckedAt
	}
	if in.VerifiedAt != nil {
		verifiedAt := *in.VerifiedAt
		out.VerifiedAt = &verifiedAt
	}
	return out
}
