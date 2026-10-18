package api

import (
	"context"
	"fmt"
	"slices"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

const producerQualityPlanBytes = 8 << 20
const producerQualityPlanCount = 8

// This is a bounded observation work list, never a serving configuration or a
// cached authorization result. It retains no compiler snapshot or receipt body.
// Publication rebuilds current inputs and checks every receipt's context/expiry.
type producerQualityPlan struct {
	binding    string
	capturedAt time.Time
	expiresAt  time.Time
	jobs       []dynamicQualityJob
	contexts   map[string]string
	consumers  []platformconfig.DNSConsumerObservation
	bytes      int
}

func (s *Server) rememberProducerQualityPlan(policy platformproducer.Policy, projection platformIntentProjectionResponse) {
	query := projection.Policy.DNSQueryPolicy
	if projection.Intent.Scope != policy.TargetScope || projection.Policy.Scope != policy.TargetScope || query == nil || query.DynamicQuality == nil || projection.RuntimeSnapshot.CapturedAt == nil {
		return
	}
	jobs, _, err := dynamicQualityRoutes(projection, *query)
	if err != nil || len(jobs) == 0 || len(jobs) > 20000 {
		return
	}
	binding, err := platformconfig.Digest(policy)
	if err != nil {
		return
	}
	captured := *projection.RuntimeSnapshot.CapturedAt
	now := time.Now().UTC()
	expires := captured.Add(time.Duration(query.DynamicQuality.Policy.EvidenceMaxAgeSeconds) * time.Second)
	if captured.After(now) || !expires.After(now) {
		return
	}
	plan := &producerQualityPlan{binding: binding, capturedAt: captured, expiresAt: expires, jobs: jobs, contexts: map[string]string{}, bytes: 512}
	contextFor := newDynamicQualityContextReader(projection, jobs)
	for _, job := range jobs {
		contextDigest, err := contextFor(job)
		if err == nil {
			plan.contexts[job.Key] = contextDigest
		}
		// Jobs are newly derived, detached values; their slices are not taken from
		// the projection. Account for strings and slice storage before retention.
		plan.bytes += 512 + len(job.Key) + len(job.NodeID) + len(job.EvidenceHostname) + len(job.Route.Hostname) + len(contextDigest)
		for _, host := range job.EvidenceHostnames {
			plan.bytes += 16 + len(host)
		}
		for _, service := range job.EvidenceServices {
			plan.bytes += 48 + len(service.Hostname) + len(service.PathPrefix) + len(service.TrafficClass)
		}
	}
	for _, consumer := range projection.RuntimeSnapshot.DNSConsumers {
		consumer.A = append([]string(nil), consumer.A...)
		consumer.AAAA = append([]string(nil), consumer.AAAA...)
		plan.consumers = append(plan.consumers, consumer)
		plan.bytes += 256 + len(consumer.NodeID) + len(consumer.EdgeGroupID)
		for _, address := range consumer.A {
			plan.bytes += 16 + len(address)
		}
		for _, address := range consumer.AAAA {
			plan.bytes += 16 + len(address)
		}
	}
	if plan.bytes > producerQualityPlanBytes {
		return
	}
	s.dynamicQuality.mu.Lock()
	defer s.dynamicQuality.mu.Unlock()
	if s.dynamicQuality.producerPlans == nil {
		s.dynamicQuality.producerPlans = map[string]*producerQualityPlan{}
	}
	delete(s.dynamicQuality.producerPlans, policy.TargetScope)
	for {
		size := plan.bytes
		oldest := ""
		for scope, previous := range s.dynamicQuality.producerPlans {
			if !previous.expiresAt.After(now) {
				delete(s.dynamicQuality.producerPlans, scope)
				continue
			}
			size += previous.bytes
			if oldest == "" || previous.capturedAt.Before(s.dynamicQuality.producerPlans[oldest].capturedAt) {
				oldest = scope
			}
		}
		if size <= producerQualityPlanBytes && len(s.dynamicQuality.producerPlans) < producerQualityPlanCount {
			break
		}
		delete(s.dynamicQuality.producerPlans, oldest)
	}
	s.dynamicQuality.producerPlans[policy.TargetScope] = plan
}

func (s *Server) pendingProducerQualityPlan(policy platformproducer.Policy, now time.Time) *producerQualityPlan {
	binding, err := platformconfig.Digest(policy)
	if err != nil {
		return nil
	}
	s.dynamicQuality.mu.Lock()
	plan := s.dynamicQuality.producerPlans[policy.TargetScope]
	s.dynamicQuality.mu.Unlock()
	if plan == nil || plan.binding != binding || plan.capturedAt.After(now) || !plan.expiresAt.After(now) {
		return nil
	}
	return plan
}

// Advance observations during rollout waits without invoking the compiler's
// business-readiness or explicit publication gates. The plan comes only from a
// prior producer capture and is bounded by the signed evidence freshness limit.
// New configuration, missing/revoked artifacts and expired plans cannot reuse it.
func (s *Server) refreshPendingProducerQuality(ctx context.Context, policy platformproducer.Policy) error {
	if policy.DNSPolicyArtifactID == "" || policy.Serving == nil || policy.Serving.SinglePublication {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	a, err := s.store.GetPlatformArtifact(policy.DNSPolicyArtifactID)
	if err != nil {
		return err
	}
	if a.ID != policy.DNSPolicyArtifactID || a.ContentHash != policy.DNSPolicyDigest || a.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(a) != nil || !platformsafety.EvaluateArtifactIntegrity(a, s.bundleKeyring()).Pass {
		return fmt.Errorf("pending quality DNS policy reference is not trusted")
	}
	static, err := s.loadStaticPlatformIntent(policy.StaticIntentArtifactID, policy.StaticIntentDigest)
	if err != nil {
		return err
	}
	input, err := platformproducer.DecodeProjectionPolicy(a, static.Consumers, policy.HostedZoneTemplates)
	if err != nil {
		return err
	}
	if err = platformproducer.ValidatePinnedSources(policy, static, &input); err != nil {
		return err
	}
	if input.DNSQueryPolicy == nil || input.DNSQueryPolicy.DynamicQuality == nil {
		return nil
	}
	now := time.Now().UTC()
	plan := s.pendingProducerQualityPlan(policy, now)
	if plan == nil {
		return nil
	}
	quality := *input.DNSQueryPolicy.DynamicQuality
	var nodes []model.DNSNode
	if static.AuthorityCellID != "" {
		_, nodes, err = s.captureCellEndpoints(ctx, static)
	} else {
		nodes, err = s.store.ListDNSNodes("")
		if err == nil {
			var policies []model.ClusterNodePolicyStatus
			policies, err = s.loadClusterNodePolicyStatuses(ctx, platformProducerPrincipal())
			if err == nil {
				nodes = activeDNSNodesForPolicy(nodes, policies)
			}
		}
	}
	if err != nil {
		return err
	}
	// ObservedAt describes this new inventory read. Never renew the timestamp
	// of an old endpoint, or redirect a frozen job to a changed physical owner.
	consumers := freshProducerQualityConsumers(plan.consumers, nodes, time.Now().UTC())
	work := s.dynamicQuality.schedule(plan.jobs, quality.RefreshQueriesPerCycle, now)
	contextFor := func(job dynamicQualityJob) (string, error) {
		digest := plan.contexts[job.Key]
		if digest == "" {
			return "", fmt.Errorf("pending quality observation has no frozen context")
		}
		return digest, nil
	}
	completed, exhausted := s.refreshDynamicQualityBatch(ctx, work, quality, consumers, contextFor)
	if s.log != nil {
		s.log.Printf("platform producer pending quality refreshed scope=%s queries=%d scheduled=%d refreshed=%d budget_exhausted=%t", policy.TargetScope, len(plan.jobs), len(work), completed, exhausted)
	}
	return ctx.Err()
}

func freshProducerQualityConsumers(expected []platformconfig.DNSConsumerObservation, nodes []model.DNSNode, now time.Time) []platformconfig.DNSConsumerObservation {
	same := func(a, b platformconfig.DNSConsumerObservation) bool {
		return a.NodeID == b.NodeID && a.EdgeGroupID == b.EdgeGroupID && slices.Equal(a.A, b.A) && slices.Equal(a.AAAA, b.AAAA)
	}
	current := map[string]platformconfig.DNSConsumerObservation{}
	ambiguous := map[string]bool{}
	for _, node := range nodes {
		value := platformconfig.DNSConsumerObservation{NodeID: firstNonEmpty(node.PhysicalNodeID, node.ID), EdgeGroupID: node.EdgeGroupID, ObservedAt: now}
		if node.PublicIPv4 != "" {
			value.A = []string{node.PublicIPv4}
		}
		if node.PublicIPv6 != "" {
			value.AAAA = []string{node.PublicIPv6}
		}
		if previous, found := current[value.NodeID]; found && !same(previous, value) {
			ambiguous[value.NodeID] = true
		}
		current[value.NodeID] = value
	}
	result := []platformconfig.DNSConsumerObservation{}
	for _, before := range expected {
		if value, found := current[before.NodeID]; found && !ambiguous[before.NodeID] && same(before, value) {
			result = append(result, value)
		}
	}
	return result
}
