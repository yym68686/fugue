package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

// recordLegacyConsumerFacts projects existing edge/DNS heartbeats into the
// runtime-facts read model. It never marks a consumer identity as trusted and
// never promotes a release; it only records the generation and probe state the
// process actually reported.
func (s *Server) recordLegacyConsumerFacts(consumerNodeID, edgeGroupID, routeGeneration, dnsGeneration, caddyGeneration, lkgGeneration string, healthy bool, tlsStatus string) {
	if s == nil || s.store == nil || strings.TrimSpace(consumerNodeID) == "" {
		return
	}
	sets, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ScopeKey: "global", Limit: 200})
	if err != nil {
		return
	}
	now := time.Now().UTC()
	for _, set := range sets {
		if set.ScopeKey != "global" {
			continue
		}
		for _, expected := range set.Consumers {
			if expected.NodeID != consumerNodeID || (edgeGroupID != "" && strings.HasPrefix(expected.FailureDomain, "edge-group:") && !strings.EqualFold(strings.TrimPrefix(expected.FailureDomain, "edge-group:"), edgeGroupID)) {
				continue
			}
			actual := routeGeneration
			probe := ""
			if expected.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
				actual = dnsGeneration
				if healthy {
					probe = model.PlatformConsumerProbeStatusPassed
				}
			} else if expected.ArtifactKind == model.PlatformArtifactKindCaddyRouteConfig {
				actual = caddyGeneration
				if strings.EqualFold(strings.TrimSpace(tlsStatus), "ready") || strings.EqualFold(strings.TrimSpace(tlsStatus), "active") {
					probe = model.PlatformConsumerProbeStatusPassed
				}
			} else if healthy {
				probe = model.PlatformConsumerProbeStatusPassed
			}
			apply := ""
			if strings.TrimSpace(actual) != "" {
				apply = model.PlatformConsumerApplyStatusApplied
			}
			evidence := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%s|%t", expected.ConsumerID, set.ExpectedGeneration, actual, lkgGeneration, healthy)))
			_, _ = s.store.UpsertPlatformConsumerHeartbeat(model.PlatformConsumerHeartbeatRequest{
				ConsumerID: expected.ConsumerID, Component: expected.Component, NodeID: expected.NodeID,
				ArtifactKind: expected.ArtifactKind, ScopeKey: set.ScopeKey, ReleaseSetID: set.ReleaseSetID,
				ExpectedConsumerSetID: set.ID, Sequence: now.UnixNano(), EvidenceHash: "sha256:" + hex.EncodeToString(evidence[:]),
				DesiredGeneration: set.ExpectedGeneration, ActualGeneration: strings.TrimSpace(actual), LKGGeneration: strings.TrimSpace(lkgGeneration),
				ApplyStatus: apply, ProbeStatus: probe, ServingLKG: strings.TrimSpace(actual) != "" && strings.TrimSpace(actual) == strings.TrimSpace(lkgGeneration),
			})
		}
	}
}
