package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"time"

	"fugue/internal/auth"
	"fugue/internal/edgequality"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/passiveasn"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

func (s *Server) handleGetPlatformConsumerPassiveASN(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	claims, ok := auth.PlatformComponentIdentityFromContext(r.Context())
	if !ok || claims.Component != model.PlatformConsumerComponentDNSServer || claims.AuthorityID == "" ||
		claims.ScopeKey != platformconfig.AuthorityCellScope(claims.AuthorityID) {
		httpx.WriteError(w, http.StatusForbidden, "scoped DNS consumer identity required")
		return
	}
	ids := r.URL.Query()["expected_consumer_set_id"]
	if len(ids) != 1 || ids[0] == "" || strings.TrimSpace(ids[0]) != ids[0] || r.PathValue("artifact_id") == "" {
		httpx.WriteError(w, http.StatusBadRequest, "exact DNS artifact and expected set required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	loadArtifact := s.store.NewPlatformArtifactReadSession()
	items, err := s.resolvePlatformConsumerAssignmentsWithReaderContext(ctx, claims, newConsumerArtifactReader(loadArtifact))
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "passive ASN assignment unavailable")
		return
	}
	var selected *consumerArtifactLookup
	for j := range items {
		item := &items[j]
		if item.Artifact.ID == r.PathValue("artifact_id") && item.Assignment.ExpectedConsumerSetID == ids[0] &&
			item.Artifact.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle && item.Release.ReleaseChannel == model.PlatformArtifactReleaseChannelFull {
			if found, err := s.consumerAssignmentIsSelected(*item, claims.AuthorityID, newConsumerArtifactReader(loadArtifact)); err == nil && found {
				selected = item
			}
			break
		}
	}
	if selected == nil || !platformsafety.EvaluateArtifactIntegrity(selected.Artifact, s.bundleKeyring()).Pass || !passiveASNAuthorized(selected.Artifact) {
		httpx.WriteError(w, http.StatusNotFound, "passive ASN not authorized for this assignment")
		return
	}
	now := time.Now().UTC()
	publication, ok := s.makePassiveASNPublication(ctx, selected.Assignment, claims.NodeID, now)
	if !ok {
		httpx.WriteError(w, http.StatusServiceUnavailable, "fresh passive ASN mapping unavailable")
		return
	}
	if err := s.checkConsumerInputReadsWithLoader(ctx, claims, []consumerArtifactLookup{*selected}, newConsumerArtifactReader(loadArtifact), true, loadArtifact); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "DNS observation assignment changed")
		return
	}
	signed, err := passiveasn.Sign(publication, s.bundleKeyring())
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "passive ASN signature unavailable")
		return
	}
	raw, err := json.Marshal(signed)
	if err != nil || len(raw) > 4<<20 {
		httpx.WriteError(w, http.StatusServiceUnavailable, "passive ASN publication exceeds bound")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func passiveASNAuthorized(artifact model.PlatformArtifact) bool {
	raw, err := json.Marshal(artifact.Content["policy"])
	var p platformconfig.PolicySnapshot
	if err != nil || json.Unmarshal(raw, &p) != nil || p.DNSQueryPolicy == nil || p.DNSQueryPolicy.DynamicQuality == nil {
		return false
	}
	return platformconfig.ValidateDNSQueryPolicy(p.DNSQueryPolicy) == nil && p.DNSQueryPolicy.DynamicQuality.Mode == "all_dynamic"
}

func (s *Server) makePassiveASNPublication(ctx context.Context, a model.PlatformConsumerAssignment, node string, now time.Time) (passiveasn.Publication, bool) {
	p := passiveasn.Publication{Mode: "shadow", DNSArtifactID: a.ArtifactID, DNSDigest: a.ContentHash, ReleaseSetID: a.ReleaseSetID,
		ConsumerSetID: a.ExpectedConsumerSetID, FencingToken: a.FencingToken, NodeID: node}
	resolvers := s.passiveASN.resolvers.Load()
	if resolvers == nil || now.Sub(resolvers.PublishedAt) > 8*24*time.Hour {
		return p, false
	}
	raw, err := s.store.ReadObservationCheckpoint(ctx, passiveASNCheckpointKey)
	var cp passiveASNCheckpoint
	if err != nil || len(raw) > 4<<20 || json.Unmarshal(raw, &cp) != nil || cp.ObservedAt.IsZero() ||
		cp.ObservedAt.After(now) || now.Sub(cp.ObservedAt) >= passiveasn.PublicationTTL {
		return p, false
	}
	p.ObservedAt, p.ExpiresAt = cp.ObservedAt, cp.ObservedAt.Add(passiveasn.PublicationTTL)
	p.Resolvers, p.ResolverSourceAt = resolvers.Prefixes, resolvers.PublishedAt
	p.ExpiresAt = minPassiveASNTime(p.ExpiresAt, resolvers.PublishedAt.Add(8*24*time.Hour))
	policies, err := s.passiveASNSignedMappings(ctx)
	if err != nil || len(policies) == 0 {
		return p, false
	}
	if passiveASNServingReady(cp.DNSProofs, policies, a, node) {
		p.Mode = "active"
	}
	lookup, _, err := buildPassiveASNResolver(policies)
	if err != nil {
		return p, false
	}
	if cp.Mapping == "ripe-ris-v1" {
		index := s.passiveASN.ris.Load()
		for _, policy := range policies {
			if len(policy.Rules) != 0 {
				return p, false
			}
		}
		if index == nil || now.Sub(index.SourceAt) > passiveasn.MaxSourceAge {
			return p, false
		}
		lookup = index.Lookup
		p.ExpiresAt = minPassiveASNTime(p.ExpiresAt, index.SourceAt.Add(passiveasn.MaxSourceAge))
	} else if digest, err := platformconfig.Digest(policies); err != nil || digest != cp.Mapping {
		return p, false
	}
	added := map[netip.Prefix]uint32{}
	add := func(prefix netip.Prefix, asn uint32) bool {
		if existing := added[prefix]; existing != 0 {
			return existing == asn
		}
		if len(added) >= passiveasn.MaxBindings || asn == 0 || lookup(prefix) != asn {
			return false
		}
		added[prefix] = asn
		return true
	}
	// Keep provenance of the last good comparison before admitting extra
	// current sample prefixes. A capacity cap preserves stable selection.
	pairs := make([]edgequality.PassiveASNComparison, 0, len(cp.Comparisons))
	for _, pair := range cp.Comparisons {
		pairs = append(pairs, pair)
	}
	slices.SortFunc(pairs, func(a, b edgequality.PassiveASNComparison) int { return strings.Compare(a.Key(), b.Key()) })
	for _, pair := range pairs {
		if !passiveASNMatchesMapping(pair, lookup) {
			continue
		}
		valid := true
		for _, prefix := range pair.Prefixes {
			valid = add(prefix, pair.ASN) && valid
		}
		if valid {
			p.Pairs = append(p.Pairs, pair)
		}
	}
	for _, cohort := range cp.Cohorts {
		add(cohort.Prefix, cohort.ASN)
	}
	for prefix, asn := range added {
		p.Bindings = append(p.Bindings, passiveasn.Binding{Prefix: prefix, ASN: asn})
	}
	slices.SortFunc(p.Bindings, func(a, b passiveasn.Binding) int { return a.Prefix.Addr().Compare(b.Prefix.Addr()) })
	return p, p.ExpiresAt.After(now)
}

func minPassiveASNTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func passiveASNServingReady(proofs map[string]passiveASNDNSProof, policies []platformconfig.DNSClientPolicy, a model.PlatformConsumerAssignment, node string) bool {
	// Do not send subnet-dependent answers unless the configured independent
	// authorities have each replay-verified a classified query on their current
	// selected pod and release.
	if len(policies) < 2 || len(policies) != len(proofs) {
		return false
	}
	seen := make(map[string]bool, len(policies))
	for _, policy := range policies {
		proof := proofs[policy.NodeID]
		if !proof.Verified || seen[policy.NodeID] {
			return false
		}
		seen[policy.NodeID] = true
	}
	current := proofs[node].Publication
	return seen[node] && current.ArtifactID == a.ArtifactID && current.Digest == a.ContentHash &&
		current.ReleaseSetID == a.ReleaseSetID && current.FencingToken == a.FencingToken
}
