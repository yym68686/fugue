package api

import (
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func projectDomainTLSLifecycle(result *platformIntentProjectionResponse, entries []model.EdgeTLSAllowlistEntry, domains []model.AppDomain) error {
	byHost := make(map[string]model.AppDomain, len(domains))
	for _, domain := range domains {
		host := normalizeExternalAppDomain(domain.Hostname)
		if _, exists := byHost[host]; exists || host == "" {
			return fmt.Errorf("domain snapshot identity is ambiguous")
		}
		byHost[host] = domain
	}
	tlsIndex := make(map[string]int, len(result.Intent.TLS))
	for i, tls := range result.Intent.TLS {
		tlsIndex[tls.Hostname] = i
	}
	result.RuntimeSnapshot.TLSDomains = nil
	for _, entry := range entries {
		host := normalizeExternalAppDomain(entry.Hostname)
		domain, exists := byHost[host]
		index, hasTLS := tlsIndex[host]
		if !exists || !hasTLS || domain.AppID != entry.AppID || domain.TenantID != entry.TenantID || domain.Status != entry.Status || domain.TLSStatus != entry.TLSStatus {
			return fmt.Errorf("domain lifecycle does not bind the frozen TLS source")
		}
		digest, err := platformconfig.Digest([]string{host, domain.AppID, domain.TenantID})
		if err != nil {
			return err
		}
		ref := "domain_" + strings.TrimPrefix(digest, "sha256:")
		desired := &result.Intent.TLS[index]
		desired.DomainRef, desired.AppID, desired.TenantID = ref, domain.AppID, domain.TenantID
		// These are original event times, including absent/old TLS checks.
		// Reading a frozen row cannot manufacture fresh certificate readiness.
		result.RuntimeSnapshot.TLSDomains = append(result.RuntimeSnapshot.TLSDomains, platformconfig.TLSDomainObservation{
			Ref: ref, Hostname: host, AppID: domain.AppID, TenantID: domain.TenantID, Status: domain.Status, TLSStatus: domain.TLSStatus,
			VerifiedAt: domain.VerifiedAt, TLSLastCheckedAt: domain.TLSLastCheckedAt, TLSReadyAt: domain.TLSReadyAt,
		})
	}
	result.Intent = platformconfig.NormalizePlatformIntent(result.Intent)
	sort.Slice(result.RuntimeSnapshot.TLSDomains, func(i, j int) bool {
		return result.RuntimeSnapshot.TLSDomains[i].Ref < result.RuntimeSnapshot.TLSDomains[j].Ref
	})
	gen, err := platformconfig.PlatformIntentGeneration(result.Intent)
	if err != nil {
		return err
	}
	result.Intent.Generation, result.RuntimeSnapshot.IntentGeneration = gen, gen
	if _, err := platformconfig.CompileTLSDomains(result.Intent, result.RuntimeSnapshot); err != nil {
		result.Issues = append(result.Issues, platformProjectionIssue{Code: "tls_domain_lifecycle_evidence_requires_repair", Reason: err.Error()})
	}
	return nil
}
