package api

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/routeprobe"
	"fugue/internal/store"
)

func (s *Server) readOfflineRecovery(ctx context.Context, hold platformproducer.OfflineEdge) (platformproducer.OfflineRecoveryObservation, error) {
	var o platformproducer.OfflineRecoveryObservation
	raw, err := s.store.ReadObservationCheckpoint(ctx, platformproducer.OfflineRecoveryKey(hold))
	if err != nil {
		return o, err
	}
	if len(raw) > 4096 || len(raw) > 0 && json.Unmarshal(raw, &o) != nil {
		return o, fmt.Errorf("recovery observation invalid")
	}
	return o, nil
}

// A re-enrolled Worker or a pinned release must prove its exact activation
// before a route-only check can admit it. Capacity rounds already bind code.
func requiresOfflineRouteCode(hold platformproducer.OfflineEdge) bool {
	return hold.Recovery != nil && hold.Recovery.Capacity == nil && (hold.WorkerEnrollment != nil || hold.Recovery.WorkerTarget != nil)
}

func (s *Server) readOfflineCode(ctx context.Context, hold platformproducer.OfflineEdge, node model.EdgeNode) (platformproducer.CapacityCodeIdentity, error) {
	target := hold.Recovery.WorkerTarget
	read := s.offlineCapacityCode
	if read == nil {
		read = s.readCapacityCode
		if target != nil {
			read = func(ctx context.Context, node model.EdgeNode) (platformproducer.CapacityCodeIdentity, error) {
				return s.readCapacityCodeIdentity(ctx, node, true)
			}
		}
	}
	code, err := read(ctx, node)
	if err != nil {
		return code, err
	}
	if !code.Valid() || hold.WorkerEnrollment != nil && code.NodeUID != hold.WorkerEnrollment.NodeUID {
		return code, codeObservationError("code_identity_conflict", "node_metadata")
	}
	if target != nil && !target.Matches(code) {
		return code, codeObservationError("code_not_current", "code_binding")
	}
	return code, nil
}

func (s *Server) offlineRecoveryReady(ctx context.Context, hold platformproducer.OfflineEdge, o platformproducer.OfflineRecoveryObservation, node model.EdgeNode, at time.Time) bool {
	if !o.Ready(hold, at) {
		return false
	}
	if !requiresOfflineRouteCode(hold) && hold.Recovery.WorkerTarget == nil {
		return true
	}
	expected := o.RouteCode
	if hold.Recovery.Capacity != nil && o.Capacity != nil {
		expected = &o.Capacity.Code
	}
	if expected == nil || !expected.Valid() || hold.Recovery.Capacity == nil && o.LastResult != "route_code_verified" || node.ID != hold.EdgeID ||
		node.EdgeGroupID != hold.EdgeGroupID || node.PublicIPv4 != hold.PublicIPv4 || node.Draining ||
		!edgeNodeRouteServingCapable(node, time.Now().UTC()) {
		return false
	}
	code, err := s.readOfflineCode(ctx, hold, node)
	return err == nil && code == *expected
}

// Only the independent recovery worker schedules probes. Preview/inventory requests only
// read checkpoints. Claiming before I/O bounds retries across process restarts.
func (s *Server) reconcileOfflineEdgeRecovery(ctx context.Context, policy platformproducer.Policy) error {
	if policy.TargetScope != "global" || policy.DNSPolicyArtifactID == "" {
		return nil
	}
	source, err := s.store.GetPlatformArtifact(policy.DNSPolicyArtifactID)
	if err != nil {
		return err
	}
	if source.ContentHash != policy.DNSPolicyDigest || source.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(source) != nil || !platformsafety.EvaluateArtifactIntegrity(source, s.bundleKeyring()).Pass {
		return fmt.Errorf("recovery input not trusted")
	}
	if source.Content["offline_edges"] == nil {
		return nil
	}
	static, err := s.loadStaticPlatformIntent(policy.StaticIntentArtifactID, policy.StaticIntentDigest)
	if err != nil {
		return err
	}
	input, err := platformproducer.DecodeProjectionPolicy(source, static.Consumers, policy.HostedZoneTemplates)
	if err != nil {
		return err
	}
	type task struct {
		hold        platformproducer.OfflineEdge
		observation platformproducer.OfflineRecoveryObservation
	}
	tasks := []task{}
	for _, hold := range input.OfflineEdges {
		if hold.Recovery == nil {
			continue
		}
		// The external observer owns interval claims for capacity rounds. A
		// cluster-local rejection must not reset its consecutive successes.
		if hold.Recovery.Capacity != nil && externalCapacityRecoveryEnabled() {
			continue
		}
		authorized := false
		for _, route := range static.Routes {
			if route.Hostname == hold.Recovery.Hostname && route.Status != model.EdgeRouteStatusDisabled && model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) && (hold.Recovery.Capacity == nil || hold.Recovery.Capacity.TrafficClass == "streaming") {
				authorized = true
			}
		}
		if !authorized {
			return fmt.Errorf("recovery probe is not an enabled pinned static route")
		}
		o, err := s.readOfflineRecovery(ctx, hold)
		if err != nil {
			if s.log != nil {
				s.log.Printf("offline recovery checkpoint unavailable edge_id=%s; retaining hold: %v", hold.EdgeID, err)
			}
			continue
		}
		tasks = append(tasks, task{hold, o})
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].observation.LastAttempt.Before(tasks[j].observation.LastAttempt) })
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	attempts := 0
	for _, task := range tasks {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		hold := task.hold
		now := time.Now().UTC()
		claimed := false
		previous := task.observation
		if !previous.LastAttempt.IsZero() && now.Sub(previous.LastAttempt) < time.Duration(hold.Recovery.ProbeIntervalSeconds)*time.Second {
			continue
		}
		locked, err := s.store.UpdateObservationCheckpoint(ctx, platformproducer.OfflineRecoveryKey(hold), func(raw []byte) ([]byte, error) {
			var current platformproducer.OfflineRecoveryObservation
			if len(raw) > 4096 || len(raw) > 0 && json.Unmarshal(raw, &current) != nil {
				return nil, fmt.Errorf("recovery observation invalid")
			}
			if !current.LastAttempt.IsZero() && now.Sub(current.LastAttempt) < time.Duration(hold.Recovery.ProbeIntervalSeconds)*time.Second {
				return nil, nil
			}
			previous = current
			current.LastAttempt = now
			claimed = true
			return json.Marshal(current)
		})
		if err != nil {
			return err
		}
		if !locked || !claimed {
			continue
		}
		probe := s.offlineEdgeRecoveryProbe
		if probe == nil {
			probe = routeprobe.Probe
		}
		node, _, nodeErr := s.store.GetEdgeNode(hold.EdgeID)
		passed := false
		var validUntil time.Time
		var capacity *platformproducer.CapacityRecoveryEvidence
		var routeCode *platformproducer.CapacityCodeIdentity
		result, codeDetail := "", ""
		var codePlan *model.EdgeCodeRecoveryPlan
		observedAt := time.Time{}
		if hold.Recovery.Capacity != nil {
			outcome := s.probeCapacityRecovery(ctx, hold, input.OfflineEdges)
			capacity, result, validUntil = outcome.evidence, outcome.reason, outcome.validUntil
			codeDetail, codePlan = outcome.codeDetail, outcome.codePlan
			passed = capacity != nil && result == "capacity_verified"
			if capacity != nil {
				observedAt = capacity.CheckedAt
			}
		} else if nodeErr == nil && node.EdgeGroupID == hold.EdgeGroupID && node.PublicIPv4 == hold.PublicIPv4 && edgeNodeRouteServingCapable(node, now) && !node.Draining {
			var before platformproducer.CapacityCodeIdentity
			var codeErr error
			if requiresOfflineRouteCode(hold) {
				before, codeErr = s.readOfflineCode(ctx, hold, node)
			}
			if codeErr == nil {
				proof, probeErr := probe(ctx, hold.Recovery.Hostname, hold.Recovery.Path, hold.PublicIPv4, "", time.Duration(hold.Recovery.ProbeTimeoutSeconds)*time.Second)
				checked := time.Now().UTC()
				passed = probeErr == nil && proof.EdgeID == hold.EdgeID && proof.GroupID == hold.EdgeGroupID && proof.State == "" && !proof.CheckedAt.Before(now) && !proof.CheckedAt.After(checked) && proof.ValidUntil.After(checked)
				validUntil = proof.ValidUntil
				if passed && requiresOfflineRouteCode(hold) {
					var after platformproducer.CapacityCodeIdentity
					after, codeErr = s.readOfflineCode(ctx, hold, node)
					if codeErr == nil && before != after {
						codeErr = codeObservationError("code_identity_conflict", "observation_changed")
					}
					if codeErr == nil {
						routeCode, result = &after, "route_code_verified"
					}
				}
			}
			if codeErr != nil {
				passed = false
				failure := capacityCodeFailureResult(codeErr)
				result, codeDetail, codePlan = failure.reason, failure.codeDetail, failure.codePlan
			}
		}
		completionCtx, finish := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		_, err = s.store.UpdateObservationCheckpoint(completionCtx, platformproducer.OfflineRecoveryKey(hold), func(raw []byte) ([]byte, error) {
			var current platformproducer.OfflineRecoveryObservation
			if json.Unmarshal(raw, &current) != nil || !current.LastAttempt.Equal(now) {
				return nil, fmt.Errorf("recovery attempt superseded")
			}
			if observedAt.IsZero() {
				observedAt = time.Now().UTC()
			}
			if requiresOfflineRouteCode(hold) && (previous.RouteCode == nil || routeCode == nil || *previous.RouteCode != *routeCode) {
				// Do not count successes from a different activation, or a
				// pre-upgrade route-only checkpoint without code evidence.
				previous = platformproducer.OfflineRecoveryObservation{}
			}
			observed := previous.RecordCapacity(hold, observedAt, passed, validUntil, capacity, result)
			observed.CodeDetail, observed.CodePlan = codeDetail, codePlan
			observed.RouteCode = routeCode
			return json.Marshal(observed)
		})
		finish()
		if err != nil {
			return err
		}
		attempts++
		if attempts >= 2 || hold.Recovery.Capacity != nil {
			break
		}
	}
	return nil
}

func (s *Server) projectDNSReadinessWithRecovery(ctx context.Context, result *platformIntentProjectionResponse, nodes []model.EdgeNode, at time.Time, input *platformproducer.ProjectionPolicyInput) error {
	return s.projectDNSReadinessWithRecoveryChecked(ctx, result, nodes, at, input, s.store.RetiredPlatformConsumerTargets)
}

func offlineEdgeRouteEnrollments(hold platformproducer.OfflineEdge) ([2]store.PlatformConsumerRetirementTarget, error) {
	enrollment := ""
	if hold.WorkerEnrollment != nil {
		enrollment = hold.WorkerEnrollment.ID
	}
	id, err := platformcontrol.EnrolledPlatformConsumerID(model.PlatformConsumerComponentEdgeWorker, hold.EdgeID, platformcontrol.ConsumerAuthorityID(hold.EdgeGroupID), "global", enrollment)
	if err != nil {
		return [2]store.PlatformConsumerRetirementTarget{}, err
	}
	// Global offline intent is shared by the DNS candidate and both
	// worker-owned route artifacts in its ReleaseSet.
	return [2]store.PlatformConsumerRetirementTarget{
		{ConsumerID: id, ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ScopeKey: "global"},
		{ConsumerID: id, ArtifactKind: model.PlatformArtifactKindCaddyRouteConfig, ScopeKey: "global"},
	}, nil
}

func (s *Server) projectDNSReadinessWithRecoveryChecked(ctx context.Context, result *platformIntentProjectionResponse, nodes []model.EdgeNode, at time.Time, input *platformproducer.ProjectionPolicyInput, lookup func(context.Context, []store.PlatformConsumerRetirementTarget) (map[store.PlatformConsumerRetirementTarget]bool, error)) error {
	if input == nil || len(input.OfflineEdges) == 0 {
		return projectDNSReadinessWithPolicy(result, nodes, at, input)
	}
	effective, observations, err := s.effectiveOfflineEdgeHolds(ctx, nodes, at, input.OfflineEdges, lookup)
	if err != nil {
		return err
	}
	copy := *input
	copy.OfflineEdges = effective
	if err := projectDNSReadinessWithPolicy(result, nodes, at, &copy); err != nil {
		return err
	}
	result.hostedAddressHolds = offlineHostedAddressHolds(effective)
	return setOfflineRecoveryFact(&result.RuntimeSnapshot, observations, at)
}

func (s *Server) effectiveOfflineEdgeHolds(ctx context.Context, nodes []model.EdgeNode, at time.Time, holds []platformproducer.OfflineEdge, lookup func(context.Context, []store.PlatformConsumerRetirementTarget) (map[store.PlatformConsumerRetirementTarget]bool, error)) ([]platformproducer.OfflineEdge, []platformproducer.OfflineRecoveryObservation, error) {
	if err := platformproducer.ValidateOfflineEdges(holds); err != nil {
		return nil, nil, err
	}
	observations := []platformproducer.OfflineRecoveryObservation{}
	var targets []store.PlatformConsumerRetirementTarget
	var inventory map[string]model.EdgeNode
	for _, hold := range holds {
		if hold.Recovery != nil {
			o, err := s.readOfflineRecovery(ctx, hold)
			if err != nil {
				// Missing/corrupt recovery facts cannot release a hold. They
				// also cannot stop publication to the remaining valid members.
				if s.log != nil {
					s.log.Printf("offline recovery observation unavailable edge_id=%s; retaining hold: %v", hold.EdgeID, err)
				}
				continue
			}
			if (requiresOfflineRouteCode(hold) || hold.Recovery.WorkerTarget != nil) && inventory == nil {
				inventory = make(map[string]model.EdgeNode, len(nodes))
				for _, node := range nodes {
					inventory[node.ID] = node
				}
			}
			if s.offlineRecoveryReady(ctx, hold, o, inventory[hold.EdgeID], at) {
				enrollments, err := offlineEdgeRouteEnrollments(hold)
				if err != nil {
					return nil, nil, err
				}
				targets = append(targets, enrollments[:]...)
				observations = append(observations, o)
			}
		}
	}
	if len(targets) > 0 {
		retired, err := lookup(ctx, targets)
		if err != nil {
			return nil, nil, fmt.Errorf("check permanent consumer retirement before offline recovery: %w", err)
		}
		// A fresh probe establishes readiness, not enrollment authority.
		// Omit ineligible evidence before signing so replay retains the
		// original offline hold, rather than editing a prepared topology.
		eligible := observations[:0]
		for i, o := range observations {
			blocked := false
			for _, target := range targets[2*i : 2*i+2] {
				if retired[target] {
					blocked = true
					if s.log != nil {
						s.log.Printf("offline hold retained: permanently retired consumer edge_id=%s consumer_id=%s artifact_kind=%s scope=%s", o.EdgeID, target.ConsumerID, target.ArtifactKind, target.ScopeKey)
					}
				}
			}
			if blocked {
				continue
			}
			eligible = append(eligible, o)
		}
		observations = eligible
	}
	effective, err := platformproducer.EffectiveOfflineHolds(nodes, holds, observations, at)
	return effective, observations, err
}

func offlineHostedAddressHolds(holds []platformproducer.OfflineEdge) []platformconfig.HostedAddressHold {
	var out []platformconfig.HostedAddressHold
	for _, h := range holds {
		out = append(out, platformconfig.HostedAddressHold{EdgeID: h.EdgeID, EdgeGroupID: h.EdgeGroupID, Address: h.PublicIPv4})
	}
	return out
}

func setOfflineRecoveryFact(snapshot *platformconfig.RuntimeSnapshot, observations []platformproducer.OfflineRecoveryObservation, at time.Time) error {
	if len(observations) > 0 {
		if snapshot.Facts == nil {
			snapshot.Facts = map[string]any{}
		}
		// JSON storage form is also the compiler form; this prevents replay drift.
		raw, _ := json.Marshal(map[string]any{"captured_at": at, "observations": observations})
		var fact map[string]any
		if err := json.Unmarshal(raw, &fact); err != nil {
			return err
		}
		snapshot.Facts[platformproducer.OfflineRecoveryFact] = fact
	}
	return nil
}

func pinOfflineRecoveryEvidence(parent *model.PlatformArtifact, snapshot platformconfig.RuntimeSnapshot) error {
	fact, ok := snapshot.Facts[platformproducer.OfflineRecoveryFact]
	if !ok {
		return nil
	}
	raw, err := json.Marshal(fact)
	if err != nil || len(raw) > platformproducer.MaxOfflineRecoveryEvidenceBytes {
		return fmt.Errorf("recovery evidence exceeds bound")
	}
	parent.Metadata[platformproducer.OfflineRecoveryMetadata] = string(raw)
	return nil
}
