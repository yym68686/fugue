package platformproducer

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"fugue/internal/model"
)

const (
	Schema                     = "fugue.platform.producer/v1"
	Scope                      = "platform-config-producer"
	Actor                      = "platform-config-producer"
	PolicyReleaseMetadata      = "producer_policy_release_id"
	SourceDigestMetadata       = "producer_source_digest"
	StaticIntentIDMetadata     = "producer_static_intent_id"
	StaticIntentDigestMetadata = "producer_static_intent_digest"
	DNSPolicyIDMetadata        = "producer_dns_policy_id"
	DNSPolicyDigestMetadata    = "producer_dns_policy_digest"
)

type Policy struct {
	Serving                   *ServingPolicy       `json:"serving,omitempty"`
	RequireDNSQueryPolicy     bool                 `json:"require_dns_query_policy,omitempty"`
	RequireRouteDefaults      bool                 `json:"require_route_defaults,omitempty"`
	RequireApplicationDomains bool                 `json:"require_application_domains,omitempty"`
	SchemaVersion             string               `json:"schema_version"`
	Generation                string               `json:"generation"`
	Mode                      string               `json:"mode"`
	InputSource               string               `json:"input_source"`
	TargetScope               string               `json:"target_scope"`
	IntervalSeconds           int                  `json:"interval_seconds"`
	RefreshSeconds            int                  `json:"refresh_seconds"`
	StaticIntentArtifactID    string               `json:"static_intent_artifact_id,omitempty"`
	StaticIntentDigest        string               `json:"static_intent_digest,omitempty"`
	DNSPolicyArtifactID       string               `json:"dns_policy_artifact_id,omitempty"`
	DNSPolicyDigest           string               `json:"dns_policy_digest,omitempty"`
	HostedZoneTemplates       []HostedZoneTemplate `json:"hosted_zone_templates,omitempty"`
}

type ServingPolicy struct {
	CanaryRuleRef         string `json:"canary_rule_ref"`
	GrayMinSeconds        int    `json:"gray_min_seconds"`
	FullMinSeconds        int    `json:"full_min_seconds"`
	RolloutTimeoutSeconds int    `json:"rollout_timeout_seconds"`
}

var servingCohortRef = regexp.MustCompile(`^cohort=[a-z0-9][a-z0-9_-]{0,63}$`)

type HostedZoneTemplate struct {
	NodeID       string `json:"node_id"`
	TemplateZone string `json:"template_zone"`
}

func Decode(artifact model.PlatformArtifact) (Policy, error) {
	var p Policy
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return p, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, fmt.Errorf("producer policy schema: %w", err)
	}
	if artifact.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || artifact.ScopeKey != Scope || p.SchemaVersion != Schema || strings.TrimSpace(p.Generation) == "" || p.Generation != artifact.Generation {
		return p, fmt.Errorf("producer policy identity or scope invalid")
	}
	if p.Mode != "paused" && p.Mode != "shadow" && p.Mode != "serving" || p.TargetScope != "global" || p.IntervalSeconds < 30 || p.IntervalSeconds > 900 || p.RefreshSeconds < 120 || p.RefreshSeconds > 3600 || p.RefreshSeconds < p.IntervalSeconds {
		return p, fmt.Errorf("producer policy mode, source or schedule invalid")
	}
	if p.Mode == "serving" && (p.Serving == nil || p.InputSource != "business-static-intent" || !p.RequireApplicationDomains || !p.RequireRouteDefaults || !p.RequireDNSQueryPolicy) {
		return p, fmt.Errorf("serving producer requires explicit promotion policy and complete pinned inputs")
	}
	if v := p.Serving; v != nil {
		if !servingCohortRef.MatchString(v.CanaryRuleRef) || v.GrayMinSeconds < 1 || v.GrayMinSeconds > 1800 || v.FullMinSeconds < 1 || v.FullMinSeconds > 1800 || v.RolloutTimeoutSeconds < 30 || v.RolloutTimeoutSeconds > 3600 || v.RolloutTimeoutSeconds < max(v.GrayMinSeconds, v.FullMinSeconds)+p.IntervalSeconds {
			return p, fmt.Errorf("producer serving policy is outside bounded limits")
		}
	}
	switch p.InputSource {
	case "business-static-intent":
		if p.StaticIntentArtifactID == "" || strings.TrimSpace(p.StaticIntentArtifactID) != p.StaticIntentArtifactID || !ValidDigest(p.StaticIntentDigest) {
			return p, fmt.Errorf("static intent source requires exact identity and digest")
		}
	default:
		return p, fmt.Errorf("producer source unsupported")
	}
	if (p.RequireRouteDefaults || p.RequireDNSQueryPolicy) && (p.InputSource != "business-static-intent" || p.DNSPolicyArtifactID == "" || !ValidDigest(p.DNSPolicyDigest)) {
		return p, fmt.Errorf("route defaults require paired pinned policy reference")
	}
	if p.RequireApplicationDomains && p.InputSource != "business-static-intent" {
		return p, fmt.Errorf("application domains require pinned static intent")
	}
	if p.DNSPolicyArtifactID != "" || p.DNSPolicyDigest != "" || len(p.HostedZoneTemplates) > 0 {
		if p.InputSource != "business-static-intent" || p.DNSPolicyArtifactID == "" || strings.TrimSpace(p.DNSPolicyArtifactID) != p.DNSPolicyArtifactID || !ValidDigest(p.DNSPolicyDigest) || len(p.HostedZoneTemplates) > 256 {
			return p, fmt.Errorf("DNS input requires paired exact policy reference")
		}
		seen := map[string]bool{}
		for _, t := range p.HostedZoneTemplates {
			if t.NodeID == "" || t.TemplateZone == "" || seen[t.NodeID] {
				return p, fmt.Errorf("invalid hosted zone template")
			}
			seen[t.NodeID] = true
		}
	}
	return p, nil
}

func ValidDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	b, err := hex.DecodeString(value[7:])
	return err == nil && hex.EncodeToString(b) == value[7:]
}
