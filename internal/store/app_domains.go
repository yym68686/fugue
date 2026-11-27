package store

import (
	"strings"

	"fugue/internal/model"
)

func (s *Store) ListAppDomains(appID string) ([]model.AppDomain, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListAppDomains(appID)
}

func (s *Store) ListVerifiedAppDomains() ([]model.AppDomain, error) {
	return s.pgListVerifiedAppDomains()
}

func (s *Store) GetAppDomain(hostname string) (model.AppDomain, error) {
	hostname = normalizeAppDomainHostname(hostname)
	if hostname == "" {
		return model.AppDomain{}, ErrInvalidInput
	}
	return s.pgGetAppDomain(hostname)
}

func (s *Store) PutAppDomain(domain model.AppDomain) (model.AppDomain, error) {
	domain.Hostname = normalizeAppDomainHostname(domain.Hostname)
	domain.AppID = strings.TrimSpace(domain.AppID)
	domain.TenantID = strings.TrimSpace(domain.TenantID)
	domain.Status = normalizeAppDomainStatus(domain.Status)
	domain.DNSMode = model.NormalizeAppDomainDNSMode(domain.DNSMode)
	domain.DNSZoneID = strings.TrimSpace(domain.DNSZoneID)
	domain.DNSRecordID = strings.TrimSpace(domain.DNSRecordID)
	domain.DNSRecordSource = strings.TrimSpace(domain.DNSRecordSource)
	domain.DNSStatus = model.NormalizeAppDomainDNSStatus(domain.DNSStatus)
	domain.DNSRecordKind = model.NormalizeAppDomainDNSRecordKind(domain.DNSRecordKind)
	domain.TLSStatus = model.NormalizeAppDomainTLSStatus(domain.TLSStatus)
	domain.VerificationTXTName = normalizeTXTRecordName(domain.VerificationTXTName)
	domain.VerificationTXTValue = strings.TrimSpace(domain.VerificationTXTValue)
	domain.RouteTarget = normalizeAppDomainHostname(domain.RouteTarget)
	domain.LastMessage = strings.TrimSpace(domain.LastMessage)
	domain.DNSLastMessage = strings.TrimSpace(domain.DNSLastMessage)
	domain.TLSLastMessage = strings.TrimSpace(domain.TLSLastMessage)
	if domain.Hostname == "" || domain.AppID == "" || domain.DNSMode == "" {
		return model.AppDomain{}, ErrInvalidInput
	}
	return s.pgPutAppDomain(domain)
}

func (s *Store) DeleteAppDomain(appID, hostname string) (model.AppDomain, error) {
	appID = strings.TrimSpace(appID)
	hostname = normalizeAppDomainHostname(hostname)
	if appID == "" || hostname == "" {
		return model.AppDomain{}, ErrInvalidInput
	}
	return s.pgDeleteAppDomain(appID, hostname)
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

func cloneAppDomain(in model.AppDomain) model.AppDomain {
	out := in
	out.DNSMode = model.NormalizeAppDomainDNSMode(out.DNSMode)
	if out.DNSMode == "" {
		out.DNSMode = model.AppDomainDNSModeExternal
	}
	out.DNSZoneID = strings.TrimSpace(out.DNSZoneID)
	out.DNSRecordID = strings.TrimSpace(out.DNSRecordID)
	out.DNSRecordSource = strings.TrimSpace(out.DNSRecordSource)
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
