package platformcontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

type ExpectedConsumerTopology struct {
	EdgeNodes    []model.EdgeNode
	DNSNodes     []model.DNSNode
	NodeUpdaters []model.NodeUpdater
	Runtimes     []model.Runtime
}

type ExpectedConsumerSetBuildRequest struct {
	ReleaseSetID      string
	ArtifactReleaseID string
	ArtifactKind      string
	Scope             model.PlatformArtifactScope
	ScopeKey          string
	Generation        string
	Revision          int64
	PreparedAt        time.Time
	Topology          ExpectedConsumerTopology
}

func BuildExpectedConsumerSet(req ExpectedConsumerSetBuildRequest) (model.PlatformExpectedConsumerSet, error) {
	now := req.PreparedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	kind := normalizeExpectedConsumerArtifactKind(req.ArtifactKind)
	generation := strings.TrimSpace(req.Generation)
	scopeKey := strings.TrimSpace(strings.ToLower(req.ScopeKey))
	if scopeKey == "" {
		scopeKey = "global"
	}
	if kind == "" || generation == "" {
		return model.PlatformExpectedConsumerSet{}, fmt.Errorf("artifact kind and generation are required")
	}
	if req.Revision <= 0 {
		req.Revision = 1
	}

	components := expectedComponentsForArtifact(kind)
	consumers := make([]model.PlatformExpectedConsumer, 0)
	for _, component := range components {
		switch component {
		case model.PlatformConsumerComponentEdgeWorker, model.PlatformConsumerComponentCaddyEdgeFront:
			for _, node := range req.Topology.EdgeNodes {
				if !expectedEdgeNodeMatchesScope(node, req.Scope) {
					continue
				}
				consumers = append(consumers, expectedEdgeConsumer(component, node, kind, scopeKey, generation, now))
			}
		case model.PlatformConsumerComponentDNSServer:
			for _, node := range req.Topology.DNSNodes {
				if !expectedDNSNodeMatchesScope(node, req.Scope) {
					continue
				}
				consumers = append(consumers, expectedDNSConsumer(node, kind, scopeKey, generation, now))
			}
		case model.PlatformConsumerComponentNodeGuardian:
			for _, updater := range req.Topology.NodeUpdaters {
				if !expectedNodeUpdaterMatchesScope(updater, req.Scope) {
					continue
				}
				consumers = append(consumers, expectedNodeGuardianConsumer(updater, kind, scopeKey, generation, now))
			}
		case model.PlatformConsumerComponentRuntimeAgent:
			for _, runtimeObj := range req.Topology.Runtimes {
				if !expectedRuntimeMatchesScope(runtimeObj, req.Scope) {
					continue
				}
				consumers = append(consumers, expectedRuntimeConsumer(runtimeObj, kind, scopeKey, generation, now))
			}
		}
	}

	consumers = deduplicateAndSortExpectedConsumers(consumers)
	required, optional := 0, 0
	heartbeatDeadline := now
	convergenceDeadline := now
	for _, consumer := range consumers {
		if consumer.Required {
			required++
		} else {
			optional++
		}
		if consumer.HeartbeatDeadline.After(heartbeatDeadline) {
			heartbeatDeadline = consumer.HeartbeatDeadline
		}
		if consumer.ConvergenceDeadline.After(convergenceDeadline) {
			convergenceDeadline = consumer.ConvergenceDeadline
		}
	}
	topologyRevision := expectedConsumerTopologyRevision(kind, scopeKey, consumers)
	idSeed := strings.Join([]string{req.ReleaseSetID, req.ArtifactReleaseID, kind, scopeKey, generation, topologyRevision, strconv.FormatInt(req.Revision, 10)}, "|")
	idHash := sha256.Sum256([]byte(idSeed))

	return model.PlatformExpectedConsumerSet{
		ID:                  "expectedconsumerset_" + hex.EncodeToString(idHash[:8]),
		ReleaseSetID:        strings.TrimSpace(req.ReleaseSetID),
		ArtifactReleaseID:   strings.TrimSpace(req.ArtifactReleaseID),
		ArtifactKind:        kind,
		Scope:               req.Scope,
		ScopeKey:            scopeKey,
		ExpectedGeneration:  generation,
		TopologyRevision:    topologyRevision,
		Revision:            req.Revision,
		RequiresConsumers:   len(components) > 0,
		RequiredCardinality: required,
		OptionalCardinality: optional,
		HeartbeatDeadline:   heartbeatDeadline,
		ConvergenceDeadline: convergenceDeadline,
		Consumers:           consumers,
		CreatedAt:           now,
		UpdatedAt:           now,
	}, nil
}

func EvaluateConsumerConvergence(set model.PlatformExpectedConsumerSet, observed []model.PlatformConsumerInstance, now time.Time) model.PlatformConsumerConvergenceStatus {
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	status := model.PlatformConsumerConvergenceStatus{
		ExpectedConsumerSetID: set.ID,
		ArtifactKind:          set.ArtifactKind,
		ScopeKey:              set.ScopeKey,
		ExpectedGeneration:    set.ExpectedGeneration,
		State:                 model.InvariantEvidenceStatePass,
		RequiredExpected:      set.RequiredCardinality,
		OptionalExpected:      set.OptionalCardinality,
		Assessments:           []model.PlatformConsumerEvidenceAssessment{},
		EvaluatedAt:           now,
	}

	observedByID := make(map[string]model.PlatformConsumerInstance, len(observed))
	for _, consumer := range observed {
		id := strings.TrimSpace(consumer.ConsumerID)
		if id == "" {
			continue
		}
		if previous, ok := observedByID[id]; !ok || consumer.LastHeartbeatAt.After(previous.LastHeartbeatAt) {
			observedByID[id] = consumer
		}
	}
	expectedIDs := make(map[string]struct{}, len(set.Consumers))
	for _, expected := range set.Consumers {
		expectedIDs[expected.ConsumerID] = struct{}{}
		consumer, found := observedByID[expected.ConsumerID]
		assessment := assessExpectedConsumer(expected, consumer, found, now)
		status.Assessments = append(status.Assessments, assessment)
		if expected.Required && found {
			status.RequiredObserved++
		}
		if expected.Required && assessment.State == model.InvariantEvidenceStatePass {
			status.RequiredPassing++
		}
		if expected.Required {
			status.State = aggregateEvidenceState(status.State, assessment.State)
		}
	}
	for id := range observedByID {
		if _, ok := expectedIDs[id]; !ok {
			status.UnexpectedConsumers = append(status.UnexpectedConsumers, id)
		}
	}
	sort.Strings(status.UnexpectedConsumers)
	if len(status.UnexpectedConsumers) > 0 {
		status.State = aggregateEvidenceState(status.State, model.InvariantEvidenceStateFail)
	}
	if set.RequiresConsumers && set.RequiredCardinality == 0 {
		status.State = aggregateEvidenceState(status.State, model.InvariantEvidenceStateUnknown)
	}
	status.Pass = status.State == model.InvariantEvidenceStatePass && status.RequiredPassing == status.RequiredExpected
	return status
}

func assessExpectedConsumer(expected model.PlatformExpectedConsumer, observed model.PlatformConsumerInstance, found bool, now time.Time) model.PlatformConsumerEvidenceAssessment {
	assessment := model.PlatformConsumerEvidenceAssessment{
		ConsumerID:  expected.ConsumerID,
		Component:   expected.Component,
		NodeID:      expected.NodeID,
		Required:    expected.Required,
		State:       model.InvariantEvidenceStatePass,
		Expected:    expected,
		EvaluatedAt: now,
	}
	if !found {
		assessment.State = model.InvariantEvidenceStateUnknown
		assessment.Reasons = []string{"consumer heartbeat is missing"}
		return assessment
	}
	copy := observed
	assessment.Observed = &copy
	assessment.ObservedAt = &copy.LastHeartbeatAt
	freshness := time.Duration(expected.HeartbeatFreshnessSeconds) * time.Second
	if freshness <= 0 {
		freshness = 90 * time.Second
	}
	freshUntil := observed.LastHeartbeatAt.Add(freshness)
	assessment.FreshUntil = &freshUntil
	if observed.LastHeartbeatAt.IsZero() {
		assessment.State = model.InvariantEvidenceStateUnknown
		assessment.Reasons = append(assessment.Reasons, "heartbeat observed_at is missing")
	} else if now.After(freshUntil) {
		assessment.State = model.InvariantEvidenceStateStale
		assessment.Reasons = append(assessment.Reasons, "consumer heartbeat is stale")
	}
	if strings.TrimSpace(observed.ConsumerID) != expected.ConsumerID ||
		strings.TrimSpace(observed.Component) != expected.Component ||
		strings.TrimSpace(observed.NodeID) != expected.NodeID ||
		strings.TrimSpace(strings.ToLower(observed.ArtifactKind)) != strings.TrimSpace(strings.ToLower(expected.ArtifactKind)) ||
		strings.TrimSpace(strings.ToLower(observed.ScopeKey)) != strings.TrimSpace(strings.ToLower(expected.ScopeKey)) {
		assessment.State = model.InvariantEvidenceStateFail
		assessment.Reasons = append(assessment.Reasons, "consumer identity does not match the expected component, node, artifact, or scope")
	}
	if observed.LKGExpired {
		assessment.State = model.InvariantEvidenceStateFail
		assessment.Reasons = append(assessment.Reasons, "consumer reports an expired LKG")
	}
	if strings.TrimSpace(observed.DesiredGeneration) == "" || strings.TrimSpace(observed.ActualGeneration) == "" || strings.TrimSpace(observed.LKGGeneration) == "" {
		assessment.State = aggregateEvidenceState(assessment.State, model.InvariantEvidenceStateUnknown)
		assessment.Reasons = append(assessment.Reasons, "desired, actual, and LKG generations must all be reported")
	} else {
		if observed.DesiredGeneration != expected.ExpectedGeneration || observed.ActualGeneration != expected.ExpectedGeneration {
			assessment.State = model.InvariantEvidenceStateFail
			assessment.Reasons = append(assessment.Reasons, "desired or actual generation does not match the expected generation")
		}
	}
	if strings.TrimSpace(strings.ToLower(observed.ApplyStatus)) != model.PlatformConsumerApplyStatusApplied {
		assessment.State = consumerStatusEvidenceState(assessment.State, observed.ApplyStatus)
		assessment.Reasons = append(assessment.Reasons, "apply status is not applied")
	}
	if strings.TrimSpace(strings.ToLower(observed.ProbeStatus)) != model.PlatformConsumerProbeStatusPassed {
		assessment.State = consumerStatusEvidenceState(assessment.State, observed.ProbeStatus)
		assessment.Reasons = append(assessment.Reasons, "probe status is not passed")
	}
	if !acceptedVersion(observed.ProtocolVersion, expected.ExpectedProtocolVersion, expected.AcceptedProtocolVersions) {
		assessment.State = consumerVersionEvidenceState(assessment.State, observed.ProtocolVersion)
		assessment.Reasons = append(assessment.Reasons, "consumer protocol version is missing or incompatible")
	}
	if !acceptedVersion(observed.SchemaVersion, expected.ExpectedSchemaVersion, expected.AcceptedSchemaVersions) {
		assessment.State = consumerVersionEvidenceState(assessment.State, observed.SchemaVersion)
		assessment.Reasons = append(assessment.Reasons, "consumer schema version is missing or incompatible")
	}
	for _, capability := range expected.CompatibilityCapabilities {
		if !containsStringFold(observed.CompatibilityCapabilities, capability) {
			assessment.State = model.InvariantEvidenceStateFail
			assessment.Reasons = append(assessment.Reasons, "consumer compatibility capability is missing: "+capability)
		}
	}
	return assessment
}

func expectedComponentsForArtifact(kind string) []string {
	switch kind {
	case model.PlatformArtifactKindEdgeRouteBundle:
		return []string{model.PlatformConsumerComponentEdgeWorker, model.PlatformConsumerComponentCaddyEdgeFront}
	case model.PlatformArtifactKindDNSAnswerBundle:
		return []string{model.PlatformConsumerComponentDNSServer}
	case model.PlatformArtifactKindCaddyRouteConfig:
		return []string{model.PlatformConsumerComponentCaddyEdgeFront}
	case model.PlatformArtifactKindDiscoveryBundle, model.PlatformArtifactKindNodeDesiredState, model.PlatformArtifactKindNodeGuardianPolicy:
		return []string{model.PlatformConsumerComponentNodeGuardian}
	case model.PlatformArtifactKindRuntimePlacementPlan, model.PlatformArtifactKindRuntimeContinuityPlan:
		return []string{model.PlatformConsumerComponentRuntimeAgent}
	case model.PlatformArtifactKindEdgeRankingPolicy, model.PlatformArtifactKindTrafficSafetyPolicy:
		return []string{model.PlatformConsumerComponentEdgeWorker, model.PlatformConsumerComponentDNSServer}
	default:
		return nil
	}
}

func normalizeExpectedConsumerArtifactKind(kind string) string {
	kind = strings.TrimSpace(strings.ToLower(kind))
	for _, known := range []string{
		model.PlatformArtifactKindEdgeRouteBundle,
		model.PlatformArtifactKindDNSAnswerBundle,
		model.PlatformArtifactKindCaddyRouteConfig,
		model.PlatformArtifactKindDiscoveryBundle,
		model.PlatformArtifactKindNodeDesiredState,
		model.PlatformArtifactKindRuntimePlacementPlan,
		model.PlatformArtifactKindRuntimeContinuityPlan,
		model.PlatformArtifactKindNodeGuardianPolicy,
		model.PlatformArtifactKindReleaseGuardPolicy,
		model.PlatformArtifactKindEdgeRankingPolicy,
		model.PlatformArtifactKindTrafficSafetyPolicy,
		model.PlatformArtifactKindSubsystemFailureContracts,
		model.PlatformArtifactKindGatePolicyRegistry,
		model.PlatformArtifactKindAutomaticActionContracts,
	} {
		if kind == known {
			return kind
		}
	}
	return ""
}

func expectedEdgeConsumer(component string, node model.EdgeNode, artifactKind, scopeKey, generation string, now time.Time) model.PlatformExpectedConsumer {
	nodeID := strings.TrimSpace(node.ID)
	failureDomain := firstNonEmptyExpected("edge-group:"+strings.TrimSpace(node.EdgeGroupID), "country:"+strings.ToLower(strings.TrimSpace(node.Country)), "region:"+strings.ToLower(strings.TrimSpace(node.Region)), "node:"+nodeID)
	return expectedConsumer(component, nodeID, artifactKind, scopeKey, generation, failureDomain, firstNonEmptyExpected(strings.TrimSpace(node.EdgeGroupID), "edge"), !node.Draining, 90*time.Second, now)
}

func expectedDNSConsumer(node model.DNSNode, artifactKind, scopeKey, generation string, now time.Time) model.PlatformExpectedConsumer {
	nodeID := strings.TrimSpace(node.ID)
	failureDomain := firstNonEmptyExpected("edge-group:"+strings.TrimSpace(node.EdgeGroupID), "node:"+firstNonEmptyExpected(strings.TrimSpace(node.PhysicalNodeID), nodeID))
	cohort := firstNonEmptyExpected(strings.TrimSpace(node.Zone), strings.TrimSpace(node.EdgeGroupID), "dns")
	return expectedConsumer(model.PlatformConsumerComponentDNSServer, nodeID, artifactKind, scopeKey, generation, failureDomain, cohort, true, 90*time.Second, now)
}

func expectedNodeGuardianConsumer(updater model.NodeUpdater, artifactKind, scopeKey, generation string, now time.Time) model.PlatformExpectedConsumer {
	nodeID := firstNonEmptyExpected(strings.TrimSpace(updater.ClusterNodeName), strings.TrimSpace(updater.MachineID), strings.TrimSpace(updater.ID))
	failureDomain := topologyFailureDomain(updater.Labels, nodeID)
	cohort := firstNonEmptyExpected(labelValue(updater.Labels, "fugue.io/cohort"), "node-guardian")
	required := strings.TrimSpace(strings.ToLower(updater.Status)) != model.NodeUpdaterStatusRevoked
	return expectedConsumer(model.PlatformConsumerComponentNodeGuardian, nodeID, artifactKind, scopeKey, generation, failureDomain, cohort, required, 2*time.Minute, now)
}

func expectedRuntimeConsumer(runtimeObj model.Runtime, artifactKind, scopeKey, generation string, now time.Time) model.PlatformExpectedConsumer {
	nodeID := firstNonEmptyExpected(strings.TrimSpace(runtimeObj.ClusterNodeName), strings.TrimSpace(runtimeObj.ID))
	failureDomain := topologyFailureDomain(runtimeObj.Labels, nodeID)
	cohort := firstNonEmptyExpected(labelValue(runtimeObj.Labels, "fugue.io/cohort"), "runtime-agent")
	required := strings.TrimSpace(strings.ToLower(runtimeObj.Status)) == model.RuntimeStatusActive
	return expectedConsumer(model.PlatformConsumerComponentRuntimeAgent, nodeID, artifactKind, scopeKey, generation, failureDomain, cohort, required, 2*time.Minute, now)
}

func expectedConsumer(component, nodeID, artifactKind, scopeKey, generation, failureDomain, cohort string, required bool, freshness time.Duration, now time.Time) model.PlatformExpectedConsumer {
	return model.PlatformExpectedConsumer{
		ConsumerID:                component + ":" + nodeID,
		Component:                 component,
		NodeID:                    nodeID,
		ArtifactKind:              artifactKind,
		ScopeKey:                  scopeKey,
		FailureDomain:             failureDomain,
		Cohort:                    cohort,
		Required:                  required,
		ExpectedProtocolVersion:   model.PlatformConsumerProtocolVersionV1,
		AcceptedProtocolVersions:  []string{model.PlatformConsumerProtocolVersionV1},
		ExpectedSchemaVersion:     model.PlatformConsumerSchemaVersionV1,
		AcceptedSchemaVersions:    []string{model.PlatformConsumerSchemaVersionV1},
		ExpectedGeneration:        generation,
		HeartbeatFreshnessSeconds: int(freshness / time.Second),
		HeartbeatDeadline:         now.Add(freshness),
		ConvergenceDeadline:       now.Add(5 * time.Minute),
	}
}

func deduplicateAndSortExpectedConsumers(consumers []model.PlatformExpectedConsumer) []model.PlatformExpectedConsumer {
	byID := make(map[string]model.PlatformExpectedConsumer, len(consumers))
	for _, consumer := range consumers {
		if strings.TrimSpace(consumer.NodeID) == "" || strings.TrimSpace(consumer.ConsumerID) == "" {
			continue
		}
		if previous, ok := byID[consumer.ConsumerID]; !ok || (!previous.Required && consumer.Required) {
			byID[consumer.ConsumerID] = consumer
		}
	}
	out := make([]model.PlatformExpectedConsumer, 0, len(byID))
	for _, consumer := range byID {
		out = append(out, consumer)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ConsumerID < out[j].ConsumerID
	})
	return out
}

func expectedConsumerTopologyRevision(kind, scopeKey string, consumers []model.PlatformExpectedConsumer) string {
	parts := []string{kind, scopeKey}
	for _, consumer := range consumers {
		parts = append(parts, strings.Join([]string{
			consumer.ConsumerID,
			consumer.Component,
			consumer.NodeID,
			consumer.ArtifactKind,
			consumer.ScopeKey,
			consumer.FailureDomain,
			consumer.Cohort,
			strconv.FormatBool(consumer.Required),
			consumer.ExpectedProtocolVersion,
			consumer.ExpectedSchemaVersion,
		}, "\x00"))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return "topology_" + hex.EncodeToString(sum[:12])
}

func aggregateEvidenceState(current, next string) string {
	priority := map[string]int{
		model.InvariantEvidenceStatePass:    0,
		model.InvariantEvidenceStateUnknown: 1,
		model.InvariantEvidenceStateStale:   2,
		model.InvariantEvidenceStateFail:    3,
	}
	if priority[next] > priority[current] {
		return next
	}
	return current
}

func consumerStatusEvidenceState(current, value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" || value == model.InvariantEvidenceStateUnknown {
		return aggregateEvidenceState(current, model.InvariantEvidenceStateUnknown)
	}
	if value == model.InvariantEvidenceStateStale {
		return aggregateEvidenceState(current, model.InvariantEvidenceStateStale)
	}
	return aggregateEvidenceState(current, model.InvariantEvidenceStateFail)
}

func consumerVersionEvidenceState(current, value string) string {
	if strings.TrimSpace(value) == "" {
		return aggregateEvidenceState(current, model.InvariantEvidenceStateUnknown)
	}
	return aggregateEvidenceState(current, model.InvariantEvidenceStateFail)
}

func acceptedVersion(actual, expected string, accepted []string) bool {
	actual = strings.TrimSpace(strings.ToLower(actual))
	if actual == "" {
		return false
	}
	if len(accepted) == 0 {
		return actual == strings.TrimSpace(strings.ToLower(expected))
	}
	return containsStringFold(accepted, actual)
}

func containsStringFold(values []string, wanted string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(wanted)) {
			return true
		}
	}
	return false
}

func expectedEdgeNodeMatchesScope(node model.EdgeNode, scope model.PlatformArtifactScope) bool {
	return expectedTopologyScopeMatches(scope, node.ID, node.EdgeGroupID, node.Country, node.Region)
}

func expectedDNSNodeMatchesScope(node model.DNSNode, scope model.PlatformArtifactScope) bool {
	return expectedTopologyScopeMatches(scope, firstNonEmptyExpected(node.PhysicalNodeID, node.ID), node.EdgeGroupID, "", "")
}

func expectedNodeUpdaterMatchesScope(updater model.NodeUpdater, scope model.PlatformArtifactScope) bool {
	nodeID := firstNonEmptyExpected(updater.ClusterNodeName, updater.MachineID, updater.ID)
	return expectedTopologyScopeMatches(scope, nodeID, "", labelValue(updater.Labels, "topology.kubernetes.io/country"), labelValue(updater.Labels, "topology.kubernetes.io/region"))
}

func expectedRuntimeMatchesScope(runtimeObj model.Runtime, scope model.PlatformArtifactScope) bool {
	nodeID := firstNonEmptyExpected(runtimeObj.ClusterNodeName, runtimeObj.ID)
	return expectedTopologyScopeMatches(scope, nodeID, "", labelValue(runtimeObj.Labels, "topology.kubernetes.io/country"), labelValue(runtimeObj.Labels, "topology.kubernetes.io/region"))
}

func expectedTopologyScopeMatches(scope model.PlatformArtifactScope, nodeID, edgeGroupID, country, region string) bool {
	if value := strings.TrimSpace(scope.NodeID); value != "" && !strings.EqualFold(value, strings.TrimSpace(nodeID)) {
		return false
	}
	if value := strings.TrimSpace(scope.EdgeID); value != "" && !strings.EqualFold(value, strings.TrimSpace(nodeID)) {
		return false
	}
	if value := strings.TrimSpace(scope.EdgeGroupID); value != "" && !strings.EqualFold(value, strings.TrimSpace(edgeGroupID)) {
		return false
	}
	if value := strings.TrimSpace(scope.Country); value != "" && !strings.EqualFold(value, strings.TrimSpace(country)) {
		return false
	}
	if value := strings.TrimSpace(scope.Region); value != "" && !strings.EqualFold(value, strings.TrimSpace(region)) {
		return false
	}
	return true
}

func topologyFailureDomain(labels map[string]string, nodeID string) string {
	for _, key := range []string{"fugue.io/failure-domain", "topology.kubernetes.io/zone", "topology.kubernetes.io/region", "fugue.io/provider"} {
		if value := labelValue(labels, key); value != "" {
			return key + ":" + value
		}
	}
	return "node:" + strings.TrimSpace(nodeID)
}

func labelValue(labels map[string]string, key string) string {
	for current, value := range labels {
		if strings.EqualFold(strings.TrimSpace(current), key) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstNonEmptyExpected(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" && !strings.HasSuffix(value, ":") {
			return strings.TrimSpace(value)
		}
	}
	return "unknown"
}
