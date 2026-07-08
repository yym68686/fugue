package model

import (
	"strings"
	"time"
)

const (
	HostedZoneStatusPendingDelegation = "pending_delegation"
	HostedZoneStatusActive            = "active"
	HostedZoneStatusDegraded          = "degraded"
	HostedZoneStatusSuspended         = "suspended"
	HostedZoneStatusDeleted           = "deleted"
)

const (
	HostedZoneDelegationStatusPending = "pending"
	HostedZoneDelegationStatusReady   = "ready"
	HostedZoneDelegationStatusError   = "error"
)

const (
	DNSRecordStatusActive   = "active"
	DNSRecordStatusDegraded = "degraded"
	DNSRecordStatusDisabled = "disabled"
	DNSRecordStatusConflict = "conflict"
)

const (
	DNSRecordSourceUser      = "user"
	DNSRecordSourceAppDomain = "app_domain"
	DNSRecordSourceSystem    = "system"
	DNSRecordSourceACME      = "acme"
)

const (
	DNSRecordSourceRefTypeAppDomain = "app_domain"
)

const (
	DNSRecordTypeA        = "A"
	DNSRecordTypeAAAA     = "AAAA"
	DNSRecordTypeCAA      = "CAA"
	DNSRecordTypeCNAME    = "CNAME"
	DNSRecordTypeMX       = "MX"
	DNSRecordTypeNS       = "NS"
	DNSRecordTypeTXT      = "TXT"
	DNSRecordTypeSRV      = "SRV"
	DNSRecordTypeALIAS    = "ALIAS"
	DNSRecordTypeANAME    = "ANAME"
	DNSRecordTypeFUGUEAPP = "FUGUE_APP"
)

const (
	DNSRecordFlattenModeNone   = "none"
	DNSRecordFlattenModeApex   = "apex"
	DNSRecordFlattenModeAlways = "always"
	DNSRecordFlattenModeApp    = "app"
)

const (
	DNSRecordFlattenIPPolicyAuto              = "auto"
	DNSRecordFlattenIPPolicyIPv4Only          = "ipv4_only"
	DNSRecordFlattenIPPolicyIPv6Only          = "ipv6_only"
	DNSRecordFlattenIPPolicyDualStackRequired = "dual_stack_required"
)

const (
	DNSRecordFlattenTTLPolicyTarget  = "target"
	DNSRecordFlattenTTLPolicyRecord  = "record"
	DNSRecordFlattenTTLPolicyMin     = "min"
	DNSRecordFlattenTTLPolicyBounded = "bounded"
)

const (
	DNSRecordFlattenFallbackStaleIfError = "stale_if_error"
	DNSRecordFlattenFallbackFailClosed   = "fail_closed"
	DNSRecordFlattenFallbackEmptyNoError = "empty_noerror"
)

const (
	DNSRecordFlattenStatusPending  = "pending"
	DNSRecordFlattenStatusResolved = "resolved"
	DNSRecordFlattenStatusStale    = "stale"
	DNSRecordFlattenStatusDegraded = "degraded"
	DNSRecordFlattenStatusError    = "error"
)

type HostedZone struct {
	ID                  string     `json:"id"`
	TenantID            string     `json:"tenant_id,omitempty"`
	ProjectID           string     `json:"project_id,omitempty"`
	ZoneName            string     `json:"zone_name"`
	Status              string     `json:"status"`
	DelegationStatus    string     `json:"delegation_status,omitempty"`
	ParentNameservers   []string   `json:"parent_nameservers,omitempty"`
	ExpectedNameservers []string   `json:"expected_nameservers,omitempty"`
	CreatedBy           string     `json:"created_by,omitempty"`
	LastCheckedAt       *time.Time `json:"last_checked_at,omitempty"`
	LastMessage         string     `json:"last_message,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

type DNSRecord struct {
	ID                    string     `json:"id"`
	ZoneID                string     `json:"zone_id"`
	TenantID              string     `json:"tenant_id,omitempty"`
	Name                  string     `json:"name"`
	FQDN                  string     `json:"fqdn"`
	Type                  string     `json:"type"`
	Values                []string   `json:"values,omitempty"`
	TTL                   int        `json:"ttl,omitempty"`
	FlattenMode           string     `json:"flatten_mode,omitempty"`
	FlattenTarget         string     `json:"flatten_target,omitempty"`
	FlattenIPv4Policy     string     `json:"flatten_ipv4_policy,omitempty"`
	FlattenIPv6Policy     string     `json:"flatten_ipv6_policy,omitempty"`
	FlattenTTLPolicy      string     `json:"flatten_ttl_policy,omitempty"`
	FlattenFallbackPolicy string     `json:"flatten_fallback_policy,omitempty"`
	FlattenStatus         string     `json:"flatten_status,omitempty"`
	FlattenedA            []string   `json:"flattened_a,omitempty"`
	FlattenedAAAA         []string   `json:"flattened_aaaa,omitempty"`
	LastResolvedAt        *time.Time `json:"last_resolved_at,omitempty"`
	ResolveError          string     `json:"resolve_error,omitempty"`
	Source                string     `json:"source"`
	SourceRefType         string     `json:"source_ref_type,omitempty"`
	SourceRefID           string     `json:"source_ref_id,omitempty"`
	Status                string     `json:"status"`
	CreatedBy             string     `json:"created_by,omitempty"`
	LastPublishedAt       *time.Time `json:"last_published_at,omitempty"`
	LastMessage           string     `json:"last_message,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

type DNSRecordPatch struct {
	Values                *[]string
	TTL                   *int
	FlattenMode           *string
	FlattenTarget         *string
	FlattenIPv4Policy     *string
	FlattenIPv6Policy     *string
	FlattenTTLPolicy      *string
	FlattenFallbackPolicy *string
	Status                *string
}

func NormalizeHostedZoneStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "", HostedZoneStatusPendingDelegation:
		return HostedZoneStatusPendingDelegation
	case HostedZoneStatusActive:
		return HostedZoneStatusActive
	case HostedZoneStatusDegraded:
		return HostedZoneStatusDegraded
	case HostedZoneStatusSuspended:
		return HostedZoneStatusSuspended
	case HostedZoneStatusDeleted:
		return HostedZoneStatusDeleted
	default:
		return ""
	}
}

func NormalizeHostedZoneDelegationStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "", HostedZoneDelegationStatusPending:
		return HostedZoneDelegationStatusPending
	case HostedZoneDelegationStatusReady:
		return HostedZoneDelegationStatusReady
	case HostedZoneDelegationStatusError:
		return HostedZoneDelegationStatusError
	default:
		return ""
	}
}

func NormalizeDNSRecordStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "", DNSRecordStatusActive:
		return DNSRecordStatusActive
	case DNSRecordStatusDegraded:
		return DNSRecordStatusDegraded
	case DNSRecordStatusDisabled:
		return DNSRecordStatusDisabled
	case DNSRecordStatusConflict:
		return DNSRecordStatusConflict
	default:
		return ""
	}
}

func NormalizeDNSRecordSource(source string) string {
	switch strings.TrimSpace(strings.ToLower(source)) {
	case "", DNSRecordSourceUser:
		return DNSRecordSourceUser
	case DNSRecordSourceAppDomain:
		return DNSRecordSourceAppDomain
	case DNSRecordSourceSystem:
		return DNSRecordSourceSystem
	case DNSRecordSourceACME:
		return DNSRecordSourceACME
	default:
		return ""
	}
}

func NormalizeDNSRecordType(recordType string) string {
	switch strings.ToUpper(strings.TrimSpace(recordType)) {
	case DNSRecordTypeA:
		return DNSRecordTypeA
	case DNSRecordTypeAAAA:
		return DNSRecordTypeAAAA
	case DNSRecordTypeCAA:
		return DNSRecordTypeCAA
	case DNSRecordTypeCNAME:
		return DNSRecordTypeCNAME
	case DNSRecordTypeMX:
		return DNSRecordTypeMX
	case DNSRecordTypeNS:
		return DNSRecordTypeNS
	case DNSRecordTypeTXT:
		return DNSRecordTypeTXT
	case DNSRecordTypeSRV:
		return DNSRecordTypeSRV
	case DNSRecordTypeALIAS:
		return DNSRecordTypeALIAS
	case DNSRecordTypeANAME:
		return DNSRecordTypeANAME
	case DNSRecordTypeFUGUEAPP:
		return DNSRecordTypeFUGUEAPP
	default:
		return ""
	}
}

func NormalizeDNSRecordFlattenMode(mode string) string {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case "", DNSRecordFlattenModeNone:
		return DNSRecordFlattenModeNone
	case DNSRecordFlattenModeApex:
		return DNSRecordFlattenModeApex
	case DNSRecordFlattenModeAlways:
		return DNSRecordFlattenModeAlways
	case DNSRecordFlattenModeApp:
		return DNSRecordFlattenModeApp
	default:
		return ""
	}
}

func NormalizeDNSRecordFlattenIPPolicy(policy string) string {
	switch strings.TrimSpace(strings.ToLower(policy)) {
	case "", DNSRecordFlattenIPPolicyAuto:
		return DNSRecordFlattenIPPolicyAuto
	case DNSRecordFlattenIPPolicyIPv4Only:
		return DNSRecordFlattenIPPolicyIPv4Only
	case DNSRecordFlattenIPPolicyIPv6Only:
		return DNSRecordFlattenIPPolicyIPv6Only
	case DNSRecordFlattenIPPolicyDualStackRequired:
		return DNSRecordFlattenIPPolicyDualStackRequired
	default:
		return ""
	}
}

func NormalizeDNSRecordFlattenTTLPolicy(policy string) string {
	switch strings.TrimSpace(strings.ToLower(policy)) {
	case "", DNSRecordFlattenTTLPolicyBounded:
		return DNSRecordFlattenTTLPolicyBounded
	case DNSRecordFlattenTTLPolicyTarget:
		return DNSRecordFlattenTTLPolicyTarget
	case DNSRecordFlattenTTLPolicyRecord:
		return DNSRecordFlattenTTLPolicyRecord
	case DNSRecordFlattenTTLPolicyMin:
		return DNSRecordFlattenTTLPolicyMin
	default:
		return ""
	}
}

func NormalizeDNSRecordFlattenFallbackPolicy(policy string) string {
	switch strings.TrimSpace(strings.ToLower(policy)) {
	case "", DNSRecordFlattenFallbackStaleIfError:
		return DNSRecordFlattenFallbackStaleIfError
	case DNSRecordFlattenFallbackFailClosed:
		return DNSRecordFlattenFallbackFailClosed
	case DNSRecordFlattenFallbackEmptyNoError:
		return DNSRecordFlattenFallbackEmptyNoError
	default:
		return ""
	}
}

func NormalizeDNSRecordFlattenStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "", DNSRecordFlattenStatusPending:
		return DNSRecordFlattenStatusPending
	case DNSRecordFlattenStatusResolved:
		return DNSRecordFlattenStatusResolved
	case DNSRecordFlattenStatusStale:
		return DNSRecordFlattenStatusStale
	case DNSRecordFlattenStatusDegraded:
		return DNSRecordFlattenStatusDegraded
	case DNSRecordFlattenStatusError:
		return DNSRecordFlattenStatusError
	default:
		return ""
	}
}
