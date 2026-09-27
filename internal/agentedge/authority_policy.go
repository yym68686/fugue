package agentedge

import (
	"encoding/json"
	"errors"
	"net/netip"
	"net/url"
	"slices"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/staticedgecontract"
)

const AuthorityPolicySchema = "fugue.agent-edge-policy/v1"

// AuthorityPolicy is independently published configuration. It does not
// replace traffic or DNS policy, own endpoint inventory, or fabricate health.
type AuthorityPolicy struct {
	SchemaVersion            string                                 `json:"schema_version"`
	Generation               string                                 `json:"generation"`
	Scope                    string                                 `json:"scope"`
	Mode                     string                                 `json:"mode"`
	Origin                   string                                 `json:"origin"`
	SigningKeyID             string                                 `json:"signing_key_id"`
	TopologyIntentArtifactID string                                 `json:"topology_intent_artifact_id"`
	TopologyIntentDigest     string                                 `json:"topology_intent_digest"`
	Constraint               platformconfig.EdgeSelectionConstraint `json:"constraint"`
	Selection                Policy                                 `json:"selection"`
	Capacity                 CapacityPolicy                         `json:"capacity"`
	GrantTTLSeconds          int                                    `json:"grant_ttl_seconds"`
	MinimumLeaseSeconds      int                                    `json:"minimum_lease_seconds"`
}

type CapacityPolicy struct {
	MaxNodeCPUPercent    int `json:"max_node_cpu_percent"`
	MaxNodeMemoryPercent int `json:"max_node_memory_percent"`
	FactMaxAgeSeconds    int `json:"fact_max_age_seconds"`
}

func (p AuthorityPolicy) Validate() error {
	u, err := url.Parse(p.Origin)
	if p.SchemaVersion != AuthorityPolicySchema || !identifier.MatchString(p.Generation) || p.Scope != PolicyScope ||
		(p.Mode != "shadow" && p.Mode != "active") || !identifier.MatchString(p.SigningKeyID) ||
		!identifier.MatchString(p.TopologyIntentArtifactID) || !digestPattern.MatchString(p.TopologyIntentDigest) ||
		err != nil || u.Scheme != "https" || p.Origin != "https://"+u.Hostname() || !hostnamePattern.MatchString(u.Hostname()) ||
		p.Constraint.OwnerKind != "platform" || p.Constraint.TenantID != "" || p.Constraint.Hostname != u.Hostname() ||
		!slices.Contains(p.Constraint.RequiredCapabilities, "http") || !slices.Contains(p.Constraint.RequiredCapabilities, "tls") ||
		platformconfig.ValidateEdgeSelectionConstraints([]platformconfig.EdgeSelectionConstraint{p.Constraint}) != nil || p.Selection.Validate() != nil ||
		p.Constraint.FactMaxAgeSeconds != p.Selection.FactMaxAgeSeconds || p.Constraint.MinCandidates > p.Selection.MaxCandidates ||
		p.Constraint.MinDistinctCells > p.Selection.DesiredDistinctCells ||
		p.Capacity.MaxNodeCPUPercent < 1 || p.Capacity.MaxNodeCPUPercent > 100 || p.Capacity.MaxNodeMemoryPercent < 1 || p.Capacity.MaxNodeMemoryPercent > 100 ||
		p.Capacity.FactMaxAgeSeconds < 10 || p.Capacity.FactMaxAgeSeconds > p.Selection.FactMaxAgeSeconds ||
		p.GrantTTLSeconds < 15 || p.GrantTTLSeconds > p.Selection.FactMaxAgeSeconds || p.GrantTTLSeconds > 300 ||
		p.MinimumLeaseSeconds < p.Selection.ProbeIntervalSeconds+(p.Selection.MaxCandidates+3)/4*((p.Selection.ProbeTimeoutMilliseconds+999)/1000) ||
		p.MinimumLeaseSeconds > p.GrantTTLSeconds {
		return errors.New("Agent Edge authority policy is invalid or internally inconsistent")
	}
	if _, err := netip.ParseAddr(u.Hostname()); err == nil {
		return errors.New("Agent Edge authority requires a TLS hostname, not an IP origin")
	}
	return nil
}

// DecodeAuthorityPolicy performs structural and identity checks. The caller
// must additionally verify the stored artifact's provenance and publication.
func DecodeAuthorityPolicy(a model.PlatformArtifact) (AuthorityPolicy, error) {
	var p AuthorityPolicy
	raw, err := json.Marshal(a.Content)
	if err != nil || len(raw) > 64<<10 || staticedgecontract.StrictJSON(raw, &p) != nil || p.Validate() != nil ||
		a.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || a.ScopeKey != PolicyScope || a.Generation != p.Generation {
		return p, errors.New("Agent Edge authority policy artifact is invalid")
	}
	digest, err := platformconfig.Digest(a.Content)
	if err != nil || digest != a.ContentHash {
		return p, errors.New("Agent Edge authority policy artifact content hash is invalid")
	}
	typedDigest, err := platformconfig.Digest(p)
	if err != nil || a.Metadata["policy_digest"] != "" && a.Metadata["policy_digest"] != typedDigest {
		return p, errors.New("Agent Edge authority policy digest does not match its metadata")
	}
	return p, nil
}

func (p AuthorityPolicy) MinimumLease() time.Duration {
	return time.Duration(p.MinimumLeaseSeconds) * time.Second
}
