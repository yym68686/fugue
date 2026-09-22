package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

const (
	edgeDNSArtifactControllerInterval = time.Minute
	edgeDNSArtifactControllerLockName = "edge-dns-artifact-controller"
	edgeDNSArtifactFreshMargin        = 30 * time.Second
)

type edgeDNSImmutableArtifactContent struct {
	ScopeKey          string              `json:"scope_key"`
	Zone              string              `json:"zone"`
	DNSNodeID         string              `json:"dns_node_id"`
	EdgeGroupID       string              `json:"edge_group_id"`
	AnswerIPs         []string            `json:"answer_ips"`
	RouteAAnswerIPs   []string            `json:"route_a_answer_ips,omitempty"`
	Version           string              `json:"version"`
	ETag              string              `json:"etag"`
	SourceFingerprint string              `json:"source_fingerprint"`
	Bundle            model.EdgeDNSBundle `json:"bundle"`
	GeneratedAt       time.Time           `json:"generated_at"`
	ValidUntil        time.Time           `json:"valid_until,omitempty"`
}

type edgeDNSBundleArtifact struct {
	ScopeKey          string
	Zone              string
	DNSNodeID         string
	EdgeGroupID       string
	AnswerIPs         []string
	RouteAAnswerIPs   []string
	Version           string
	ETag              string
	SourceFingerprint string
	Bundle            model.EdgeDNSBundle
	GeneratedAt       time.Time
	ValidUntil        time.Time
	ActivatedAt       time.Time
	UpdatedAt         time.Time
}

func edgeDNSImmutablePlatformGeneration(content map[string]any) (string, error) {
	raw, err := json.Marshal(content)
	if err != nil {
		return "", fmt.Errorf("marshal immutable edge DNS artifact generation: %w", err)
	}
	sum := sha256.Sum256(raw)
	return "dns_content_sha256_" + hex.EncodeToString(sum[:]), nil
}

func edgeDNSImmutableArtifactContentMap(artifact edgeDNSBundleArtifact) (map[string]any, error) {
	content := edgeDNSImmutableArtifactContent{
		ScopeKey:          strings.TrimSpace(artifact.ScopeKey),
		Zone:              normalizeExternalAppDomain(artifact.Zone),
		DNSNodeID:         strings.TrimSpace(artifact.DNSNodeID),
		EdgeGroupID:       strings.TrimSpace(artifact.EdgeGroupID),
		AnswerIPs:         uniqueSortedStrings(artifact.AnswerIPs),
		RouteAAnswerIPs:   uniqueSortedStrings(artifact.RouteAAnswerIPs),
		Version:           strings.TrimSpace(artifact.Version),
		ETag:              strings.TrimSpace(artifact.ETag),
		SourceFingerprint: strings.TrimSpace(artifact.SourceFingerprint),
		Bundle:            artifact.Bundle,
		GeneratedAt:       canonicalEdgeDNSArtifactTime(artifact.GeneratedAt),
		ValidUntil:        canonicalEdgeDNSArtifactTime(artifact.ValidUntil),
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("marshal immutable edge DNS artifact content: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("canonicalize immutable edge DNS artifact content: %w", err)
	}
	return out, nil
}

func canonicalEdgeDNSArtifactTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Truncate(time.Microsecond)
}

func edgeDNSBundleArtifactFromPlatformArtifact(artifact model.PlatformArtifact) (edgeDNSBundleArtifact, error) {
	if artifact.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || artifact.Content == nil {
		return edgeDNSBundleArtifact{}, errors.New("platform artifact is not a DNS answer bundle")
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return edgeDNSBundleArtifact{}, fmt.Errorf("marshal platform DNS artifact: %w", err)
	}
	var content edgeDNSImmutableArtifactContent
	if err := json.Unmarshal(raw, &content); err != nil {
		return edgeDNSBundleArtifact{}, fmt.Errorf("decode platform DNS artifact: %w", err)
	}
	if strings.TrimSpace(content.ScopeKey) == "" || strings.TrimSpace(content.Version) == "" || strings.TrimSpace(content.Bundle.Version) == "" {
		return edgeDNSBundleArtifact{}, errors.New("platform DNS artifact content is incomplete")
	}
	return edgeDNSBundleArtifact{
		ScopeKey:          content.ScopeKey,
		Zone:              content.Zone,
		DNSNodeID:         content.DNSNodeID,
		EdgeGroupID:       content.EdgeGroupID,
		AnswerIPs:         content.AnswerIPs,
		RouteAAnswerIPs:   content.RouteAAnswerIPs,
		Version:           content.Version,
		ETag:              content.ETag,
		SourceFingerprint: content.SourceFingerprint,
		Bundle:            content.Bundle,
		GeneratedAt:       content.GeneratedAt,
		ValidUntil:        content.ValidUntil,
	}, nil
}

func edgeDNSBundleArtifactsEquivalent(left, right edgeDNSBundleArtifact) bool {
	leftContent, leftErr := edgeDNSImmutableArtifactContentMap(left)
	rightContent, rightErr := edgeDNSImmutableArtifactContentMap(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	leftJSON, leftErr := json.Marshal(leftContent)
	rightJSON, rightErr := json.Marshal(rightContent)
	return leftErr == nil && rightErr == nil && slices.Equal(leftJSON, rightJSON)
}

func edgeDNSArtifactNodePublishable(node model.DNSNode) bool {
	if !node.Healthy {
		return false
	}
	switch model.NormalizeEdgeHealthStatus(node.Status) {
	case model.EdgeHealthHealthy, model.EdgeHealthDegraded:
		return true
	default:
		return false
	}
}

func (s *Server) edgeDNSBundleOptionsForDNSNode(node model.DNSNode) (edgeDNSBundleOptions, bool) {
	zone := normalizeExternalAppDomain(node.Zone)
	if zone == "" {
		zone = normalizeExternalAppDomain(s.customDomainBaseDomain)
	}
	answerIPs := []string{}
	answerIPs = appendEdgeDNSUniqueIP(answerIPs, node.PublicIPv4)
	answerIPs = appendEdgeDNSUniqueIP(answerIPs, node.PublicIPv6)
	if zone == "" || len(answerIPs) == 0 {
		return edgeDNSBundleOptions{}, false
	}
	ttl := s.dnsBundleTTL
	if ttl <= 0 || ttl > 3600 {
		ttl = defaultEdgeDNSTTL
	}
	return edgeDNSBundleOptions{
		DNSNodeID:       strings.TrimSpace(node.ID),
		EdgeGroupID:     strings.TrimSpace(node.EdgeGroupID),
		Zone:            zone,
		AnswerIPs:       answerIPs,
		RouteAAnswerIPs: append([]string(nil), s.dnsRouteAAnswerIPs...),
		TTL:             ttl,
	}, true
}

func (s *Server) edgeDNSBundleArtifactForOptions(options edgeDNSBundleOptions, now time.Time) (model.EdgeDNSBundle, bool, error) {
	if s == nil || s.store == nil {
		return model.EdgeDNSBundle{}, false, nil
	}
	artifact, release, found, err := s.store.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		edgeDNSBundleArtifactScopeKey(options),
		model.PlatformArtifactReleaseChannelFull,
	)
	if err != nil {
		return model.EdgeDNSBundle{}, false, err
	}
	if !found {
		return s.edgeDNSBundleVerifiedLKGForOptions(options, now)
	}
	if err := s.validateEdgeDNSFullRelease(artifact, release); err != nil {
		if fallback, fallbackFound, fallbackErr := s.edgeDNSBundleVerifiedLKGForOptions(options, now); fallbackErr != nil {
			return model.EdgeDNSBundle{}, false, fallbackErr
		} else if fallbackFound {
			return fallback, true, nil
		}
		return model.EdgeDNSBundle{}, false, fmt.Errorf("validate immutable full release: %w", err)
	}
	projected, err := edgeDNSBundleArtifactFromPlatformArtifact(artifact)
	if err != nil {
		return model.EdgeDNSBundle{}, false, fmt.Errorf("decode immutable full artifact: %w", err)
	}
	projected.ActivatedAt = release.ReleasedAt
	projected.UpdatedAt = release.UpdatedAt
	if err := s.validateEdgeDNSBundleArtifact(projected, options, now); err != nil {
		return model.EdgeDNSBundle{}, false, fmt.Errorf("validate activated immutable edge DNS artifact: %w", err)
	}
	return projected.Bundle, true, nil
}

func (s *Server) edgeDNSBundleVerifiedLKGForOptions(options edgeDNSBundleOptions, now time.Time) (model.EdgeDNSBundle, bool, error) {
	artifact, found, err := s.verifiedPlatformArtifactForScope(model.PlatformArtifactKindDNSAnswerBundle, edgeDNSBundleArtifactScopeKey(options))
	if err != nil || !found {
		return model.EdgeDNSBundle{}, found, err
	}
	projected, err := edgeDNSBundleArtifactFromPlatformArtifact(artifact)
	if err != nil {
		return model.EdgeDNSBundle{}, true, fmt.Errorf("decode verified DNS LKG artifact: %w", err)
	}
	if err := s.validateEdgeDNSBundleArtifact(projected, options, now); err != nil {
		return model.EdgeDNSBundle{}, true, fmt.Errorf("validate verified DNS LKG artifact: %w", err)
	}
	return projected.Bundle, true, nil
}

func (s *Server) validateEdgeDNSFullRelease(artifact model.PlatformArtifact, release model.PlatformArtifactRelease) error {
	if artifact.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle ||
		artifact.Status != model.PlatformArtifactStatusValidated ||
		strings.TrimSpace(artifact.ID) == "" ||
		strings.TrimSpace(artifact.Generation) == "" ||
		strings.TrimSpace(artifact.ScopeKey) == "" {
		return errors.New("platform DNS artifact is not validated")
	}
	if release.ArtifactID != artifact.ID ||
		release.ArtifactKind != artifact.ArtifactKind ||
		release.ScopeKey != artifact.ScopeKey ||
		release.Generation != artifact.Generation ||
		release.ReleaseChannel != model.PlatformArtifactReleaseChannelFull ||
		release.Status != model.PlatformArtifactReleaseStatusActive ||
		release.FencingToken <= 0 ||
		strings.TrimSpace(release.LaneKey) == "" ||
		strings.TrimSpace(release.PinnedRollbackGeneration) == "" ||
		strings.TrimSpace(release.RollbackTargetGeneration) != strings.TrimSpace(release.PinnedRollbackGeneration) ||
		strings.TrimSpace(release.CandidateGeneration) != artifact.Generation ||
		release.ReleasedAt.IsZero() {
		return errors.New("platform DNS full release identity is inconsistent")
	}
	switch release.VerificationState {
	case model.PlatformArtifactVerificationStateServingUnverified:
		if strings.TrimSpace(release.ServingUnverifiedGeneration) != artifact.Generation {
			return errors.New("unverified full release does not identify its serving generation")
		}
	case model.PlatformArtifactVerificationStateVerified:
		if strings.TrimSpace(release.ServingUnverifiedGeneration) != "" ||
			strings.TrimSpace(release.VerifiedLKGGeneration) != artifact.Generation ||
			release.VerifiedAt == nil || release.VerifiedAt.IsZero() {
			return errors.New("verified full release does not identify its verified generation")
		}
	default:
		return errors.New("platform DNS full release has unsupported verification state")
	}
	if strings.TrimSpace(release.OverrideMode) != model.PlatformArtifactOverrideModeNone || len(release.BypassedInvariants) != 0 {
		return errors.New("platform DNS full release bypassed an invariant")
	}
	if err := s.store.VerifyPlatformArtifactIntegrity(artifact); err != nil {
		return fmt.Errorf("verify platform artifact integrity: %w", err)
	}
	return nil
}

func newEdgeDNSBundleArtifact(options edgeDNSBundleOptions, bundle model.EdgeDNSBundle, activatedAt time.Time) edgeDNSBundleArtifact {
	return edgeDNSBundleArtifact{
		ScopeKey:          edgeDNSBundleArtifactScopeKey(options),
		Zone:              options.Zone,
		DNSNodeID:         options.DNSNodeID,
		EdgeGroupID:       options.EdgeGroupID,
		AnswerIPs:         options.AnswerIPs,
		RouteAAnswerIPs:   options.RouteAAnswerIPs,
		Version:           bundle.Version,
		ETag:              edgeRouteBundleETag(bundle.Version),
		SourceFingerprint: edgeDNSBundleArtifactSourceFingerprint(options, bundle),
		Bundle:            bundle,
		GeneratedAt:       bundle.GeneratedAt,
		ValidUntil:        bundle.ValidUntil,
		ActivatedAt:       activatedAt,
		UpdatedAt:         activatedAt,
	}
}

func (s *Server) validateEdgeDNSBundleArtifact(artifact edgeDNSBundleArtifact, options edgeDNSBundleOptions, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expectedScopeKey := edgeDNSBundleArtifactScopeKey(options)
	if strings.TrimSpace(artifact.ScopeKey) != expectedScopeKey {
		return errors.New("scope key does not match the request")
	}
	if artifact.ActivatedAt.IsZero() {
		return errors.New("artifact is not activated")
	}
	bundle := artifact.Bundle
	if strings.TrimSpace(artifact.Version) == "" || strings.TrimSpace(artifact.Version) != strings.TrimSpace(bundle.Version) ||
		strings.TrimSpace(bundle.Generation) == "" {
		return errors.New("artifact version identity is inconsistent")
	}
	if strings.TrimSpace(artifact.ETag) != edgeRouteBundleETag(bundle.Version) {
		return errors.New("artifact ETag does not match its version")
	}
	if strings.TrimSpace(artifact.SourceFingerprint) != edgeDNSBundleArtifactSourceFingerprint(options, bundle) {
		return errors.New("artifact source fingerprint does not match the request")
	}
	if normalizeExternalAppDomain(artifact.Zone) != normalizeExternalAppDomain(options.Zone) ||
		strings.TrimSpace(artifact.DNSNodeID) != strings.TrimSpace(options.DNSNodeID) ||
		strings.TrimSpace(artifact.EdgeGroupID) != strings.TrimSpace(options.EdgeGroupID) ||
		!slices.Equal(uniqueSortedStrings(artifact.AnswerIPs), uniqueSortedStrings(options.AnswerIPs)) ||
		!slices.Equal(uniqueSortedStrings(artifact.RouteAAnswerIPs), uniqueSortedStrings(options.RouteAAnswerIPs)) {
		return errors.New("artifact scope material does not match the request")
	}
	if normalizeExternalAppDomain(bundle.Zone) != normalizeExternalAppDomain(options.Zone) ||
		strings.TrimSpace(bundle.DNSNodeID) != strings.TrimSpace(options.DNSNodeID) ||
		strings.TrimSpace(bundle.EdgeGroupID) != strings.TrimSpace(options.EdgeGroupID) {
		return errors.New("bundle identity does not match the request")
	}
	if !edgeDNSArtifactTimesMatch(artifact.GeneratedAt, bundle.GeneratedAt) ||
		!edgeDNSArtifactTimesMatch(artifact.ValidUntil, bundle.ValidUntil) {
		return errors.New("artifact validity metadata does not match the bundle")
	}
	if len(bundle.Records) == 0 {
		return errors.New("artifact contains no DNS records")
	}
	if !edgeDNSBundleHasSignature(bundle) {
		return bundleauth.ErrMissingSignature
	}
	if err := bundleauth.VerifyEdgeDNSBundleWithKeyring(bundle, s.bundleKeyring(), now); err != nil {
		return fmt.Errorf("verify bundle signature: %w", err)
	}
	if !edgeDNSBundleArtifactFresh(artifact, now) {
		return errors.New("artifact is not fresh enough to activate")
	}
	return nil
}

func edgeDNSBundleHasSignature(bundle model.EdgeDNSBundle) bool {
	if strings.TrimSpace(bundle.KeyID) != "" && strings.TrimSpace(bundle.Signature) != "" {
		return true
	}
	for _, signature := range bundle.Signatures {
		if strings.TrimSpace(signature.KeyID) != "" && strings.TrimSpace(signature.Signature) != "" {
			return true
		}
	}
	return false
}

func edgeDNSArtifactTimesMatch(stored, bundled time.Time) bool {
	if stored.IsZero() || bundled.IsZero() {
		return false
	}
	delta := stored.Sub(bundled)
	if delta < 0 {
		delta = -delta
	}
	return delta <= time.Microsecond
}

func edgeDNSBundleArtifactFresh(artifact edgeDNSBundleArtifact, now time.Time) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if strings.TrimSpace(artifact.Bundle.Version) == "" || strings.TrimSpace(artifact.Version) != strings.TrimSpace(artifact.Bundle.Version) {
		return false
	}
	validUntil := firstNonZeroTime(artifact.ValidUntil, artifact.Bundle.ValidUntil)
	if validUntil.IsZero() {
		return true
	}
	return now.Add(edgeDNSArtifactFreshMargin).Before(validUntil)
}

type edgeDNSBundleArtifactScopeMaterial struct {
	DNSNodeID       string   `json:"dns_node_id,omitempty"`
	EdgeGroupID     string   `json:"edge_group_id,omitempty"`
	Zone            string   `json:"zone"`
	AnswerIPs       []string `json:"answer_ips"`
	RouteAAnswerIPs []string `json:"route_a_answer_ips,omitempty"`
	TTL             int      `json:"ttl"`
}

func edgeDNSBundleArtifactScopeKey(options edgeDNSBundleOptions) string {
	material := edgeDNSBundleArtifactScopeMaterial{
		DNSNodeID:       strings.TrimSpace(options.DNSNodeID),
		EdgeGroupID:     strings.TrimSpace(options.EdgeGroupID),
		Zone:            normalizeExternalAppDomain(options.Zone),
		AnswerIPs:       uniqueSortedStrings(options.AnswerIPs),
		RouteAAnswerIPs: uniqueSortedStrings(options.RouteAAnswerIPs),
		TTL:             options.TTL,
	}
	if material.TTL <= 0 {
		material.TTL = defaultEdgeDNSTTL
	}
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return "edge_dns_bundle:" + hex.EncodeToString(sum[:])
}

func edgeDNSBundleArtifactSourceFingerprint(options edgeDNSBundleOptions, bundle model.EdgeDNSBundle) string {
	material := struct {
		ScopeKey string `json:"scope_key"`
		Version  string `json:"version"`
		KeyID    string `json:"key_id,omitempty"`
	}{
		ScopeKey: edgeDNSBundleArtifactScopeKey(options),
		Version:  strings.TrimSpace(bundle.Version),
		KeyID:    strings.TrimSpace(bundle.KeyID),
	}
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
