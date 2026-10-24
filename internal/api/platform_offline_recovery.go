package api

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
	"fugue/internal/routeprobe"
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
		result := ""
		observedAt := time.Time{}
		if hold.Recovery.Capacity != nil {
			outcome := s.probeCapacityRecovery(ctx, hold, input.OfflineEdges)
			capacity, result, validUntil = outcome.evidence, outcome.reason, outcome.validUntil
			passed = capacity != nil && result == "capacity_verified"
			if capacity != nil {
				observedAt = capacity.CheckedAt
			}
		} else if nodeErr == nil && node.EdgeGroupID == hold.EdgeGroupID && node.PublicIPv4 == hold.PublicIPv4 && edgeNodeRouteServingCapable(node, now) && !node.Draining {
			proof, probeErr := probe(ctx, hold.Recovery.Hostname, hold.Recovery.Path, hold.PublicIPv4, "", time.Duration(hold.Recovery.ProbeTimeoutSeconds)*time.Second)
			checked := time.Now().UTC()
			passed = probeErr == nil && proof.EdgeID == hold.EdgeID && proof.GroupID == hold.EdgeGroupID && proof.State == "" && !proof.CheckedAt.Before(now) && !proof.CheckedAt.After(checked) && proof.ValidUntil.After(checked)
			validUntil = proof.ValidUntil
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
			observed := previous.RecordCapacity(hold, observedAt, passed, validUntil, capacity, result)
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
	if input == nil || len(input.OfflineEdges) == 0 {
		return projectDNSReadinessWithPolicy(result, nodes, at, input)
	}
	observations := []platformproducer.OfflineRecoveryObservation{}
	for _, hold := range input.OfflineEdges {
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
			if o.Ready(hold, at) {
				observations = append(observations, o)
			}
		}
	}
	effective, err := platformproducer.EffectiveOfflineHolds(nodes, input.OfflineEdges, observations, at)
	if err != nil {
		return err
	}
	copy := *input
	copy.OfflineEdges = effective
	if err := projectDNSReadinessWithPolicy(result, nodes, at, &copy); err != nil {
		return err
	}
	if len(observations) > 0 {
		if result.RuntimeSnapshot.Facts == nil {
			result.RuntimeSnapshot.Facts = map[string]any{}
		}
		// JSON storage form is also the compiler form; this prevents replay drift.
		raw, _ := json.Marshal(map[string]any{"captured_at": at, "observations": observations})
		var fact map[string]any
		if err := json.Unmarshal(raw, &fact); err != nil {
			return err
		}
		result.RuntimeSnapshot.Facts[platformproducer.OfflineRecoveryFact] = fact
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
